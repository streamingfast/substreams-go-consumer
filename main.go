package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/cli"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	"github.com/streamingfast/substreams/client"
	"github.com/streamingfast/substreams/manifest"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"go.uber.org/zap"
)

// Commit sha1 value, injected via go build `ldflags` at build time
var commit = ""

// Version value, injected via go build `ldflags` at build time
var version = "dev"

// Date value, injected via go build `ldflags` at build time
var date = ""

var zlog, tracer = logging.ApplicationLogger("consumer", "github.com/streamingfast/substreams-consumer",
	logging.WithConsoleToStderr(),
)

func main() {
	Run(
		"consume <endpoint> <manifest> <module> [<start>:<stop>]",
		"Consumes the given Substreams manifest against the endpoint optionally within a range of blocks",
		Execute(run),
		Description(`
			Consumes a Substreams forever.

			The <endpoint> argument must always have its port defined.
		`),
		Example(`
			consume mainnet.eth.streamingfast.io:443 ethereum-network-v1-v0.1.0.spkg graph_out +1000
		`),
		ConfigureViper("CONSUMER"),
		RangeArgs(3, 4),
		Flags(func(flags *pflag.FlagSet) {
			flags.StringP("api-token", "a", "", "API Token to use for Substreams authentication, SUBSTREAMS_API_TOKEN is automatically checked also")
			flags.BoolP("backprocess", "b", false, cli.FlagDescription(`
				Enforces backprocessing of a Substreams by overriding start block to be head block of the chain. *Important* this enforces a block
				that might be far away from module's start block which means it will take quite a time before completing. Also, setting this overrides
				any manually start block specified.
			`))
			flags.BoolP("insecure", "k", false, "Skip certificate validation on GRPC connection")
			flags.BoolP("plaintext", "p", false, "Establish GRPC connection in plaintext")
			flags.DurationP("frequency", "f", 15*time.Second, "At which interval of time we should print statistics locally extracted from Prometheus")
			flags.BoolP("clean", "c", false, "Do not read existing state from cursor state file and start from scratch instead")
			flags.String("state-store", "./state.yaml", "Output path where to store latest received cursor, if empty, cursor will not be persisted")
		}),
		PersistentFlags(func(flags *pflag.FlagSet) {
			flags.String("metrics-listen-addr", ":9102", "If non-empty, the process will listen on this address to server Prometheus metrics")
			flags.String("pprof-listen-addr", "localhost:6060", "If non-empty, the process will listen on this address for pprof analysis (see https://golang.org/pkg/net/http/pprof/)")
		}),
		AfterAllHook(func(_ *cobra.Command) {
			setup(zlog, viper.GetString("global-metrics-listen-addr"), viper.GetString("global-pprof-listen-addr"))
		}),
	)
}

func run(cmd *cobra.Command, args []string) error {
	app := shutter.New()

	ctx, cancelApp := context.WithCancel(cmd.Context())
	app.OnTerminating(func(_ error) {
		cancelApp()
	})

	endpoint := args[0]
	manifestPath := args[1]
	moduleName := args[2]
	blockRange := ""
	if len(args) > 3 {
		blockRange = args[3]
	}

	backprocess := viper.GetBool("backprocess")
	cleanState := viper.GetBool("clean")
	stateStorePath := viper.GetString("state-store")

	zlog.Info("consuming substreams",
		zap.String("endpoint", endpoint),
		zap.String("manifest_path", manifestPath),
		zap.String("module_name", moduleName),
		zap.String("block_range", blockRange),
		zap.Bool("backprocess", backprocess),
		zap.Bool("clean_state", cleanState),
		zap.String("cursor_store_path", stateStorePath),
	)

	manifestReader := manifest.NewReader(manifestPath)
	pkg, err := manifestReader.Read()
	cli.NoError(err, "Read Substreams manifest")

	graph, err := manifest.NewModuleGraph(pkg.Modules.Modules)
	cli.NoError(err, "Create Substreams module graph")

	module, err := graph.Module(moduleName)
	cli.NoError(err, "Unable to get module")

	moduleOutput := (*ModuleOutput)(module.Output)

	if backprocess && module.GetKindMap() != nil {
		return fmt.Errorf("module %q is of type 'Mapper', it's invalid to enforce backprocessing (-b) of a mapper module, only store module can be backprocessed", module.Name)
	}

	startBlock, stopBlock, err := readBlockRange(module, blockRange)
	cli.NoError(err, "Unable to read block range")

	zlog.Info("resolved block range", zap.Int64("start_block", startBlock), zap.Uint64("stop_block", stopBlock))

	apiToken := readAPIToken()

	substreamsClientConfig := client.NewSubstreamsClientConfig(
		endpoint,
		apiToken,
		viper.GetBool("insecure"),
		viper.GetBool("plaintext"),
	)

	ssClient, connClose, callOpts, err := client.NewSubstreamsClient(substreamsClientConfig)
	cli.NoError(err, "Unable to create substreams client")
	defer connClose()

	firehoseClient, firehoseClose, err := NewFirehoseClient(&FirehoseClientConfig{Endpoint: endpoint, JWT: apiToken, Insecure: viper.GetBool("insecure"), PlainText: viper.GetBool("plaintext")})
	cli.NoError(err, "Unable to create firehose client")
	defer firehoseClose()

	headFetcher := NewHeadFetcher(firehoseClient)
	app.OnTerminating(func(_ error) { headFetcher.Close() })
	headFetcher.OnTerminated(func(err error) { app.Shutdown(err) })

	stats := NewStats(stopBlock, headFetcher)
	app.OnTerminating(func(_ error) { stats.Close() })
	stats.OnTerminated(func(err error) { app.Shutdown(err) })

	activeCursor := ""
	activeBlock := bstream.BlockRefEmpty
	backprocessingCompleted := false
	headBlockReached := false

	stateStore := NewStateStore(stateStorePath, func() (string, bstream.BlockRef, bool, bool) {
		return activeCursor, activeBlock, backprocessingCompleted, headBlockReached
	})
	app.OnTerminating(func(_ error) { stateStore.Close() })
	stateStore.OnTerminated(func(err error) { app.Shutdown(err) })

	if !cleanState {
		activeCursor, activeBlock, err = stateStore.Read()
		cli.NoError(err, "Unable to read state store")
	} else {
		if backprocess {
			zlog.Info("backprocessing enforced, retrieving head block from endpoint")
			err := headFetcher.Init(ctx)
			cli.NoError(err, "Unable to retrieved head block")

			headBlock, found := headFetcher.Current()
			cli.Ensure(found, "Head block should be set at that point")

			zlog.Info("overidding start block since backprocessing is enforced", zap.Int64("actual_start_block", startBlock), zap.Uint64("new_start_block", headBlock.Num()))
			startBlock = int64(headBlock.Num())
		}
	}

	recordEntityChange := moduleOutput.TypeName() == "proto:network.types.v1.EntitiesChanges"

	zlog.Info("client configured",
		zap.Stringer("module_output", moduleOutput),
		zap.Bool("record_entity_change", recordEntityChange),
		zap.Int64("start_block", startBlock),
		zap.Uint64("stop_block", stopBlock),
		zap.Stringer("active_block", activeBlock),
		zap.String("active_cursor", activeCursor),
	)

	stats.Start(viper.GetDuration("frequency"))
	headFetcher.Start(1 * time.Minute)
	stateStore.Start(30 * time.Second)

	// We will wait at max approximatively 5m before diying
	backOff := backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 15), ctx)

	for {
		var lastErr error

		req := &pbsubstreams.Request{
			StartBlockNum: startBlock,
			StopBlockNum:  stopBlock,
			StartCursor:   activeCursor,
			ForkSteps:     []pbsubstreams.ForkStep{pbsubstreams.ForkStep_STEP_NEW, pbsubstreams.ForkStep_STEP_UNDO},
			Modules:       pkg.Modules,
			OutputModules: []string{moduleName},
		}

		err = pbsubstreams.ValidateRequest(req)
		cli.NoError(err, "Invalid built Substreams request")

		zlog.Info("connecting...")
		cli, err := ssClient.Blocks(ctx, req, callOpts...)

		if err != nil {
			lastErr = fmt.Errorf("call sf.substreams.v1.Stream/Blocks: %w", err)
		} else {
			zlog.Info("connected")
			checkFirstBlock := true

			for {
				resp, err := cli.Recv()
				if err != nil {
					if errors.Is(err, context.Canceled) {
						zlog.Debug("context cancelled, terminating work")
						break
					}

					if err == io.EOF {
						zlog.Info("completed")
						return nil
					}

					lastErr = err
					break
				}

				if resp != nil {
					backOff.Reset()

					MessageSizeBytes.AddInt(proto.Size(resp))

					if progress := resp.GetProgress(); progress != nil {
						processProgressMessage(progress)
						continue
					}

					if data := resp.GetData(); data != nil {
						block := bstream.NewBlockRef(data.Clock.Id, data.Clock.Number)

						if checkFirstBlock {
							if !bstream.EqualsBlockRefs(activeBlock, bstream.BlockRefEmpty) {
								zlog.Info("checking first received block",
									zap.Stringer("block_at_cursor", activeBlock),
									zap.Stringer("first_block", block),
								)

								// Correct check would be using parent/child relationship, if the clock had
								// information about the parent block right there, we could validate that active block
								// is actually the parent of first received block. For now, let's ensure we have a following
								// block (will not work on network's that can skip block's num like NEAR or Solana).
								if block.Num()-1 != activeBlock.Num() {
									app.Shutdown(fmt.Errorf("block continuity on first block after restarting from cursor does not follow"))
									break
								}
							}

							checkFirstBlock = false
						}

						processDataMessage(data, graph, recordEntityChange)

						stats.lastBlock = block

						activeCursor = data.Cursor
						activeBlock = block
						backprocessingCompleted = true

						chainHeadBlock, found := headFetcher.Current()
						if found && data.Clock.Number >= chainHeadBlock.Num() {
							headBlockReached = true
						}

						continue
					}

					UnknownMessageCount.Inc()
				}
			}
		}

		if app.IsTerminating() {
			break
		}

		if lastErr != nil {
			SubstreamsErrorCount.Inc()
			zlog.Error("substreams encountered an error", zap.Error(lastErr))

			sleepFor := backOff.NextBackOff()
			if sleepFor == backoff.Stop {
				zlog.Info("backoff requested to stop retries")
				return lastErr
			}

			zlog.Info("sleeping before re-connecting", zap.Duration("sleep", sleepFor))
			time.Sleep(sleepFor)
		}
	}

	<-app.Terminated()
	return app.Err()
}

func processProgressMessage(progress *pbsubstreams.ModulesProgress) {
	if tracer.Enabled() {
		zlog.Debug("progress message received", zap.Reflect("progress", progress))
	}

	for _, module := range progress.Modules {
		ProgressMessageCount.Inc(module.Name)

		if processedRanges := module.GetProcessedRanges(); processedRanges != nil {
			latestEndBlock := uint64(0)
			for _, processedRange := range processedRanges.ProcessedRanges {
				if processedRange.EndBlock > latestEndBlock {
					latestEndBlock = processedRange.EndBlock
				}
			}

			ModuleProgressBlock.SetUint64(latestEndBlock, module.Name)
		}
	}
}

func processDataMessage(data *pbsubstreams.BlockScopedData, graph *manifest.ModuleGraph, recordEntityChange bool) {
	if tracer.Enabled() {
		zlog.Debug("data message received", zap.Reflect("data", data))
	}

	BackprocessingCompletion.SetUint64(1)
	HeadBlockNumber.SetUint64(data.Clock.Number)
	HeadBlockTime.SetBlockTime(data.Clock.Timestamp.AsTime())

	if data.Step == pbsubstreams.ForkStep_STEP_NEW {
		StepNewCount.Inc()
	} else if data.Step == pbsubstreams.ForkStep_STEP_UNDO {
		StepUndoCount.Inc()
	}

	for _, output := range data.Outputs {
		DataMessageCount.Inc(output.Name)

		if data := output.GetData(); data != nil {
			OutputMapperCount.Inc(output.Name)
			OutputMapperSizeBytes.AddInt(proto.Size(output), output.Name)

			if recordEntityChange && output.Name == "graph_out" {
				if output, found := moduleOutputType(output, graph); found && output.TypeName() == "proto:network.types.v1.EntitiesChanges" {
					// FIXME: Do we want to actually decode the type to get out the amount of data extracted?
				}
			}

			continue
		}

		if storeDeltas := output.GetStoreDeltas(); storeDeltas != nil {
			OutputStoreDeltasCount.AddInt(len(storeDeltas.Deltas), output.Name)
			OutputStoreDeltaSizeBytes.AddInt(proto.Size(output), output.Name)
		}
	}
}

func moduleOutputType(output *pbsubstreams.ModuleOutput, graph *manifest.ModuleGraph) (moduleOutput *ModuleOutput, found bool) {
	module, err := graph.Module(output.Name)
	if err != nil {
		// There is only one kind of error in the `Module` implementation, when the module is not found, hopefully it stays
		// like that forever!
		return nil, false
	}

	return (*ModuleOutput)(module.Output), module.Output != nil
}

func readAPIToken() string {
	apiToken := viper.GetString("api-token")
	if apiToken != "" {
		return apiToken
	}

	apiToken = os.Getenv("SUBSTREAMS_API_TOKEN")
	if apiToken != "" {
		return apiToken
	}

	return os.Getenv("SF_API_TOKEN")
}

func readBlockRange(module *pbsubstreams.Module, input string) (start int64, stop uint64, err error) {
	if input == "" {
		input = "-1"
	}

	before, after, found := strings.Cut(input, ":")

	beforeRelative := strings.HasPrefix(before, "+")
	beforeInt64, err := strconv.ParseInt(strings.TrimPrefix(before, "+"), 0, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid block number value %q: %w", before, err)
	}

	afterRelative := false
	afterInt64 := int64(0)
	if found {
		afterRelative = strings.HasPrefix(after, "+")
		afterInt64, err = strconv.ParseInt(after, 0, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid block number value %q: %w", after, err)
		}
	}

	// If there is no `:` we assume it's a stop block value right away
	if !found {
		start = int64(module.InitialBlock)
		stop = uint64(resolveBlockNumber(beforeInt64, 0, beforeRelative, uint64(start)))
	} else {
		start = resolveBlockNumber(beforeInt64, int64(module.InitialBlock), beforeRelative, module.InitialBlock)
		stop = uint64(resolveBlockNumber(afterInt64, 0, afterRelative, uint64(start)))
	}

	return
}

func resolveBlockNumber(value int64, ifMinus1 int64, relative bool, against uint64) int64 {
	if !relative {
		if value < 0 {
			return ifMinus1
		}

		return value
	}

	return int64(against) + value
}

func readStopBlockFlag(cmd *cobra.Command, startBlock int64, flagName string) (uint64, error) {
	val, err := cmd.Flags().GetString(flagName)
	if err != nil {
		panic(fmt.Sprintf("flags: couldn't find flag %q", flagName))
	}

	isRelative := strings.HasPrefix(val, "+")
	if isRelative {
		if startBlock == -1 {
			return 0, fmt.Errorf("relative end block is supported only with an absolute start block")
		}

		val = strings.TrimPrefix(val, "+")
	}

	endBlock, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("end block is invalid: %w", err)
	}

	if isRelative {
		return uint64(startBlock) + endBlock, nil
	}

	return endBlock, nil
}

func computeVersionString(version, commit, date string) string {
	var labels []string
	if len(commit) >= 7 {
		labels = append(labels, fmt.Sprintf("Commit %s", commit[0:7]))
	}

	if date != "" {
		labels = append(labels, fmt.Sprintf("Built %s", date))
	}

	if len(labels) == 0 {
		return version
	}

	return fmt.Sprintf("%s (%s)", version, strings.Join(labels, ", "))
}
