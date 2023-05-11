package main

import (
	"net/http"

	"github.com/streamingfast/dmetrics"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
)

var metrics = dmetrics.NewSet()

var ChainHeadBlockNumber = metrics.NewGauge("substreams_sink_noop_chain_head_block_number", "The latest block of the chain as reported by Firehose endpoint")
var BackprocessingCompletion = metrics.NewGauge("substreams_sink_noop_backprocessing_completion", "Determines if backprocessing is completed, which is if we receive a first data message")
var HeadBlockReached = metrics.NewGauge("substreams_sink_noop_head_block_reached", "Determines if head block was reached at some point, once set it will not change anymore however on restart, there might be a delay before it's set back to 1")

func setup(logger *zap.Logger, metricsListenAddr string, pprofListenAddr string) {
	if metricsListenAddr != "" {
		sink.RegisterMetrics()
		metrics.Register()

		go dmetrics.Serve(metricsListenAddr)
	}

	if pprofListenAddr != "" {
		go func() {
			err := http.ListenAndServe(pprofListenAddr, nil)
			if err != nil {
				logger.Debug("unable to start profiling server", zap.Error(err), zap.String("listen_addr", pprofListenAddr))
			}
		}()
	}
}
