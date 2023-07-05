package main

import (
	"context"
	"time"

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
			flags.String("distinct-firehose-endpoint", "", "If not empty, will use this address for firehose request instead of using the same as substreams")
			flags.String("state-store", "./state.yaml", "Output path where to store latest received cursor, if empty, cursor will not be persisted")
			flags.String("api-listen-addr", ":8080", "Rest API to manage deployment")
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
	blockRangeArg := ""
	if len(args) > 3 {
		blockRangeArg = args[3]
	}

	baseSinker, err := sink.NewFromViper(cmd, sink.IgnoreOutputModuleType, endpoint, manifestPath, moduleName, blockRangeArg, zlog, tracer,
		sink.WithBlockDataBuffer(0),
	)
	cli.NoError(err, "Unable to create sinker")

	sinker := &Sinker{Sinker: baseSinker}

	apiListenAddr := sflags.MustGetString(cmd, "api-listen-addr")
	cleanState := sflags.MustGetBool(cmd, "clean")
	stateStorePath := sflags.MustGetString(cmd, "state-store")
	blockRange := sinker.BlockRange()

	firehoseEndpoint := sflags.MustGetString(cmd, "distinct-firehose-endpoint")
	if firehoseEndpoint == "" {
		firehoseEndpoint = endpoint
	}

	zlog.Info("consuming substreams",
		zap.String("substreams_endpoint", endpoint),
		zap.String("firehose_endpoint", firehoseEndpoint),
		zap.String("manifest_path", manifestPath),
		zap.String("module_name", moduleName),
		zap.Stringer("block_range", blockRange),
		zap.String("cursor_store_path", stateStorePath),
		zap.String("manage_listen_addr", apiListenAddr),
	)

	firehoseConfig := &FirehoseClientConfig{JWT: sinker.ApiToken()}
	_, firehoseConfig.PlainText, firehoseConfig.Insecure = sinker.EndpointConfig()
	firehoseConfig.Endpoint = firehoseEndpoint

	firehoseClient, firehoseClose, err := NewFirehoseClient(firehoseConfig)
	cli.NoError(err, "Unable to create firehose client")
	defer firehoseClose()

	headFetcher := NewHeadFetcher(firehoseClient)
	app.OnTerminating(func(_ error) { headFetcher.Close() })
	headFetcher.OnTerminated(func(err error) { app.Shutdown(err) })

	sinker.headFetcher = headFetcher

	stopBlock := uint64(0)
	if blockRange != nil && blockRange.EndBlock() != nil {
		stopBlock = *blockRange.EndBlock()
	}

	stats := NewStats(stopBlock, headFetcher)
	app.OnTerminating(func(_ error) { stats.Close() })
	stats.OnTerminated(func(err error) { app.Shutdown(err) })

	stateStore := NewStateStore(stateStorePath, func() (*sink.Cursor, bool, bool) {
		return sinker.activeCursor, sinker.backprocessingCompleted, sinker.headBlockReached
	})
	app.OnTerminating(func(_ error) { stateStore.Close() })
	stateStore.OnTerminated(func(err error) { app.Shutdown(err) })

	managementApi := NewManager(apiListenAddr)
	managementApi.OnTerminated(func(err error) { app.Shutdown(err) })
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
	headFetcher.Start(1 * time.Minute)
	stateStore.Start(30 * time.Second)

	app.OnTerminating(func(_ error) { sinker.Shutdown(nil) })
	sinker.OnTerminating(func(err error) {
		app.Shutdown(err)
	})

	go sinker.Run(ctx)

	zlog.Info("ready, waiting for signal to quit")

	signalHandler, isSignaled, _ := cli.SetupSignalHandler(0*time.Second, zlog)
	select {
	case <-signalHandler:
		go app.Shutdown(nil)
		break
	case <-app.Terminating():
		zlog.Info("run terminating", zap.Bool("from_signal", isSignaled.Load()), zap.Bool("with_error", app.Err() != nil))
		break
	}

	zlog.Info("waiting for run termination")
	select {
	case <-app.Terminated():
	case <-time.After(30 * time.Second):
		zlog.Warn("application did not terminate within 30s")
	}

	if err := app.Err(); err != nil {
		return err
	}

	zlog.Info("run terminated gracefully")
	return nil
}

type Sinker struct {
	*sink.Sinker

	headFetcher *HeadFetcher

	activeCursor            *sink.Cursor
	headBlockReached        bool
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
		s.headBlockReached = true
		HeadBlockReached.SetUint64(1)
	}

	return nil
}

func (s *Sinker) HandleBlockUndoSignal(ctx context.Context, undoSignal *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	s.activeCursor = cursor

	return nil
}
