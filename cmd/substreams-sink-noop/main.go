package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"hash"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	pbbmsrv "github.com/streamingfast/blockmeta-service/server/pb/sf/blockmeta/v2"
	"github.com/streamingfast/blockmeta-service/server/pb/sf/blockmeta/v2/pbbmsrvconnect"
	"gopkg.in/yaml.v3"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/cli"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams/client"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/zap"
)

var zlog, tracer = logging.ApplicationLogger("sink-noop", "github.com/streamingfast/substreams-sink-noop",
	logging.WithConsoleToStderr(),
)

func main() {
	Run(
		"substreams-sink-noop <endpoint> <manifest> <module> [<start>:<stop>]",
		"Consumes the given Substreams manifest against the endpoint optionally within a range of blocks",
		Execute(run),
		Description(`
			Consumes a Substreams forever.

			The <endpoint> argument must always have its port defined.
		`),
		Example(`
			substreams-sink-noop mainnet.eth.streamingfast.io:443 ethereum-network-v1-v0.1.0.spkg graph_out +1000
		`),
		ConfigureViper("SINK_NOOP"),
		RangeArgs(3, 4),
		Flags(func(flags *pflag.FlagSet) {
			sink.AddFlagsToSet(flags, sink.FlagIgnore(sink.FlagIrreversibleOnly))

			frequencyDefault := 15 * time.Second
			if zlog.Core().Enabled(zap.DebugLevel) {
				frequencyDefault = 5 * time.Second
			}

			flags.BoolP("clean", "c", false, "Do not read existing state from cursor state file and start from scratch instead")
			flags.DurationP("frequency", "f", frequencyDefault, "At which interval of time we should print statistics locally extracted from Prometheus")
			flags.String("state-store", "./state.yaml", "Output path where to store latest received cursor, if empty, cursor will not be persisted")
			flags.String("api-listen-addr", ":8080", "Rest API to manage deployment")
			flags.Uint64("print-output-data-hash-interval", 0, "If non-zero, will hash the output for quickly comparing for differences")
			flags.Uint64("follow-head-substreams-segment", 1000, "")
			flags.String("follow-head-blockmeta-url", "", "Block meta URL to follow head block, when provided, the sink enable the follow head mode (if block range not provided)")
			flags.Bool("follow-head-insecure", false, "Skip tls verification when connecting to blockmeta service")
			flags.Uint64("follow-head-reversible-segment", 100, "Segment size for reversible block")
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

const ApiKeyHeader = "x-api-key"

func run(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	endpoint := args[0]
	manifestPath := args[1]
	moduleName := args[2]
	blockRangeArg := ""
	if len(args) > 3 {
		blockRangeArg = args[3]
	}

	var err error
	blockmetaUrl := sflags.MustGetString(cmd, "follow-head-blockmeta-url")
	substreamsSegmentSize := sflags.MustGetUint64(cmd, "follow-head-substreams-segment")
	reversibleSegmentSize := sflags.MustGetUint64(cmd, "follow-head-reversible-segment")
	var blockmetaClient pbbmsrvconnect.BlockClient
	var apiKey string

	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: sflags.MustGetBool(cmd, "follow-head-insecure")},
	}}

	if blockmetaUrl != "" {
		blockmetaClient = pbbmsrvconnect.NewBlockClient(client, blockmetaUrl)
		apiKey = os.Getenv("SUBSTREAMS_API_KEY")
		if apiKey == "" {
			return fmt.Errorf("missing SUBSTREAMS_API_KEY environment variable to use blockmeta service")
		}
	}

	signalHandler, isSignaled, _ := cli.SetupSignalHandler(0*time.Second, zlog)
	sessionCounter := uint64(0)
	stateStorePath := sflags.MustGetString(cmd, "state-store")
	var sleepingDuration time.Duration
	retryCounter := uint64(0)
	for {
		if blockmetaClient != nil {
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-signalHandler:
					return nil
				case <-time.After(sleepingDuration):
				}

				sleepingDuration = 5 * time.Second
				headBlockNum, err := fetchHeadBlockNum(ctx, blockmetaClient, apiKey)
				if err != nil {
					return fmt.Errorf("fetching head block: %w", err)
				}

				blockRangeArg, err = computeBlockRangeFromHead(reversibleSegmentSize, substreamsSegmentSize, blockRangeArg, headBlockNum)
				if err != nil {
					return fmt.Errorf("computing block range from head: %w", err)
				}

				startBlockString := strings.Split(blockRangeArg, ":")[0]
				startBlock, err := strconv.Atoi(startBlockString)
				if err != nil {
					return fmt.Errorf("converting start block to integer: %w", err)
				}

				computedEndBlock := strings.Split(blockRangeArg, ":")[1]
				endBlock, err := strconv.Atoi(computedEndBlock)
				if err != nil {
					return fmt.Errorf("converting start block to integer: %w", err)
				}

				cursorExisting, extractedBlockNumber, err := readBlockNumFromCursor(stateStorePath)
				if err != nil {
					return fmt.Errorf("reading start block from state path: %w", err)
				}

				if cursorExisting {
					startBlock = int(extractedBlockNumber)
				}

				if startBlock < endBlock-1 {
					break
				}
				if retryCounter%6 == 0 {
					zlog.Info("waiting for head to reach next threshold", zap.Uint64("target", uint64(startBlock)+substreamsSegmentSize+reversibleSegmentSize), zap.Uint64("current_head", headBlockNum))
				}

				retryCounter += 1
			}
		}

		zlog.Info("starting sink session", zap.Uint64("session_counter", sessionCounter))
		err = runSink(cmd, blockRangeArg, endpoint, manifestPath, moduleName, zlog, tracer, signalHandler, stateStorePath)
		if err != nil {
			return err
		}

		if blockmetaClient == nil {
			return nil
		}

		if isSignaled.Load() {
			return nil
		}

		sessionCounter += 1
		zlog.Info("sleeping until next session", zap.Uint64("session_counter", sessionCounter))
	}
}

func readBlockNumFromCursor(stateStorePath string) (cursorExisting bool, startBlock uint64, err error) {
	content, err := os.ReadFile(stateStorePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, 0, nil
		}
		return false, 0, fmt.Errorf("reading cursor state file: %w", err)
	}

	state := syncState{}
	if err = yaml.Unmarshal(content, &state); err != nil {
		return false, 0, fmt.Errorf("unmarshal state file %q: %w", stateStorePath, err)
	}

	return true, state.Block.Number, nil
}
func runSink(cmd *cobra.Command, blockRangeArg string, endpoint string, manifestPath string, moduleName string, zlog *zap.Logger, tracer logging.Tracer, signalHandler <-chan os.Signal, stateStorePath string) error {
	app := shutter.New()
	ctx, cancelApp := context.WithCancel(cmd.Context())
	app.OnTerminating(func(_ error) {
		cancelApp()
	})

	baseSinker, err := sink.NewFromViper(cmd, sink.IgnoreOutputModuleType, endpoint, manifestPath, moduleName, blockRangeArg, zlog, tracer,
		sink.WithBlockDataBuffer(0),
	)
	cli.NoError(err, "Unable to create sinker")

	sinker := &Sinker{Sinker: baseSinker}

	if outputInterval := sflags.MustGetUint64(cmd, "print-output-data-hash-interval"); outputInterval != 0 {
		sinker.outputDataHash = newDataHasher(outputInterval)
	}

	apiListenAddr := sflags.MustGetString(cmd, "api-listen-addr")
	cleanState := sflags.MustGetBool(cmd, "clean")
	blockRange := sinker.BlockRange()

	managementApi := NewManager(apiListenAddr)

	zlog.Info("start new substreams consumption session",
		zap.String("substreams_endpoint", endpoint),
		zap.String("manifest_path", manifestPath),
		zap.String("module_name", moduleName),
		zap.Stringer("block_range", blockRange),
		zap.String("cursor_store_path", stateStorePath),
		zap.String("manage_listen_addr", apiListenAddr),
	)

	headTrackerClient, headTrackerConnClose, headTrackerCallOpts, headTrackerHeaders, err := client.NewSubstreamsClient(sinker.ClientConfig())
	cli.NoError(err, "Unable to create head tracker client")
	defer headTrackerConnClose()

	headFetcher := NewHeadTracker(headTrackerClient, headTrackerCallOpts, headTrackerHeaders)
	app.OnTerminating(func(_ error) { headFetcher.Close() })
	headFetcher.OnTerminated(func(err error) {
		app.Shutdown(err)
	})

	sinker.headFetcher = headFetcher

	stopBlock := uint64(0)
	if blockRange != nil && blockRange.EndBlock() != nil {
		stopBlock = *blockRange.EndBlock()
	}

	stats := NewStats(stopBlock, headFetcher)
	app.OnTerminating(func(_ error) { stats.Close() })
	stats.OnTerminated(func(err error) {
		app.Shutdown(err)
	})

	stateStore := NewStateStore(stateStorePath, func() (*sink.Cursor, bool, bool) {
		return sinker.activeCursor, sinker.backprocessingCompleted, sinker.headBlockReachedMetric
	})
	app.OnTerminating(func(_ error) { stateStore.Close() })
	stateStore.OnTerminated(func(err error) {
		app.Shutdown(err)
	})

	managementApi.OnTerminated(func(err error) {
		app.Shutdown(err)
	})
	app.OnTerminating(func(_ error) {
		if managementApi.shouldResetState {
			if err := stateStore.Delete(); err != nil {
				zlog.Warn("failed to delete state store", zap.Error(err))
			}
		}
	})
	go managementApi.Launch()

	if !cleanState {
		cursor, _, err := stateStore.Read()
		cli.NoError(err, "Unable to read state store")
		sinker.activeCursor = sink.MustNewCursor(cursor)
	}

	zlog.Info("client configured",
		zap.String("output_module_name", moduleName),
		zap.Stringer("active_block", sinker.activeCursor.Block()),
		zap.String("active_cursor", sinker.activeCursor.String()),
	)

	stats.Start(sflags.MustGetDuration(cmd, "frequency"))
	headFetcher.Start()
	stateStore.Start(30 * time.Second)

	app.OnTerminating(func(_ error) { sinker.Shutdown(nil) })
	sinker.OnTerminating(func(err error) {
		app.Shutdown(err)
	})

	go sinker.Run(ctx)

	select {
	case <-signalHandler:
		go app.Shutdown(nil)
	case <-app.Terminating():
		zlog.Info("run terminating", zap.Bool("with_error", app.Err() != nil))
	}

	zlog.Info("waiting for run termination")
	select {
	case <-app.Terminated():
		return app.Err()
	case <-time.After(30 * time.Second):
		zlog.Warn("application did not terminate within 30s")
		return app.Err()
	}
}

func fetchHeadBlockNum(ctx context.Context, blockmetaClient pbbmsrvconnect.BlockClient, apiKey string) (uint64, error) {
	request := connect.NewRequest(&pbbmsrv.Empty{})
	request.Header().Set(ApiKeyHeader, apiKey)

	headBlock, err := blockmetaClient.Head(ctx, request)
	if err != nil {
		return 0, fmt.Errorf("requesting head block to blockmeta service: %w", err)
	}

	return headBlock.Msg.Num, nil
}
func computeBlockRangeFromHead(reversibleSegmentSize uint64, substreamsSegmentSize uint64, blockRangeArg string, headBlock uint64) (string, error) {
	computedEndBlock := ((headBlock - reversibleSegmentSize) / substreamsSegmentSize) * substreamsSegmentSize
	blockRangeArray := strings.Split(blockRangeArg, ":")
	if len(blockRangeArray) != 2 {
		return "", fmt.Errorf("invalid block range format")
	}

	//The computed block range replace the end block by a computed one
	return blockRangeArray[0] + ":" + strconv.FormatUint(computedEndBlock, 10), nil
}

type Sinker struct {
	*sink.Sinker

	headFetcher *HeadTracker

	activeCursor            *sink.Cursor
	headBlockReachedMetric  bool
	outputDataHash          *dataHasher
	backprocessingCompleted bool
}

func (s *Sinker) Run(ctx context.Context) {
	s.Sinker.Run(ctx, s.activeCursor, s)
}

func (s *Sinker) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	if tracer.Enabled() {
		zlog.Debug("data message received", zap.Reflect("data", data))
	}

	block := bstream.NewBlockRef(data.Clock.Id, data.Clock.Number)

	s.activeCursor = cursor
	s.backprocessingCompleted = true

	chainHeadBlock, found := s.headFetcher.Current()
	if found && block.Num() >= chainHeadBlock.Num() {
		s.headBlockReachedMetric = true
		HeadBlockReached.SetUint64(1)
	}

	if s.outputDataHash != nil {
		s.outputDataHash.process(data)
	}

	return nil
}

func (s *Sinker) HandleBlockUndoSignal(ctx context.Context, undoSignal *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	s.activeCursor = cursor

	return nil
}

func newDataHasher(size uint64) *dataHasher {
	return &dataHasher{
		size: size,
	}
}

type dataHasher struct {
	currentRange *bstream.Range
	size         uint64
	data         hash.Hash
}

func (h *dataHasher) process(d *pbsubstreamsrpc.BlockScopedData) {
	if h.currentRange == nil {
		h.currentRange, _ = bstream.NewRangeContaining(d.Clock.Number, h.size) // the only error case is if h.size is empty
		h.data = sha256.New()
	} else if !h.currentRange.Contains(d.Clock.Number) {
		fmt.Println(h.currentRange, fmt.Sprintf("%x", h.data.Sum(nil)))
		h.currentRange = h.currentRange.Next(h.size)
		h.data = sha256.New()
	}
	h.data.Write([]byte(d.Output.Name + d.Output.MapOutput.TypeUrl))
	h.data.Write(d.Output.MapOutput.Value)

}
