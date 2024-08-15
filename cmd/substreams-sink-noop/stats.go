package main

import (
	"time"

	sink "github.com/streamingfast/substreams-sink"

	"github.com/streamingfast/dmetrics"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

var ProcessedBlockCount = metrics.NewGauge("processed_block_count", "The number of processed block")

type Stats struct {
	*shutter.Shutter

	headFetcher *HeadTracker
	stopBlock   uint64

	processedBlockRate *dmetrics.AvgRatePromGauge

	backprocessingCompletion *dmetrics.ValueFromMetric
	headBlockReached         *dmetrics.ValueFromMetric
	fetchCursor              func() *sink.Cursor
}

func NewStats(stopBlock uint64, headFetcher *HeadTracker, fetchCursor func() *sink.Cursor) *Stats {
	return &Stats{
		Shutter:                  shutter.New(),
		stopBlock:                stopBlock,
		fetchCursor:              fetchCursor,
		headFetcher:              headFetcher,
		backprocessingCompletion: dmetrics.NewValueFromMetric(BackprocessingCompletion, "completion"),
		headBlockReached:         dmetrics.NewValueFromMetric(HeadBlockReached, "reached"),
		processedBlockRate:       dmetrics.MustNewAvgRateFromPromGauge(ProcessedBlockCount, 1*time.Second, 30*time.Second, "processed_block_sec"),
	}
}

func (s *Stats) Start(each time.Duration) {
	zlog.Info("starting stats service", zap.Duration("runs_each", each))

	if s.IsTerminating() || s.IsTerminated() {
		panic("already shutdown, refusing to start again")
	}

	go func() {
		ticker := time.NewTicker(each)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.LogNow()
			case <-s.Terminating():
				return
			}
		}
	}()
}

func (s *Stats) LogNow() {
	// Logging fields order is important as it affects the final rendering, we carefully ordered
	// them so the development logs looks nicer.
	fields := []zap.Field{}
	headBlock, headBlockFound := s.headFetcher.Current()

	cursor := s.fetchCursor()

	if headBlockFound {
		fields = append(fields, zap.Stringer("head_block", headBlock))
	}

	fields = append(fields,
		zap.String("last_cursor", cursor.Block().String()),
		zap.Bool("back_processing_completed", s.backprocessingCompletion.ValueUint() > 0),
		zap.Bool("head_block_reached", s.headBlockReached.ValueUint() > 0),
		zap.Float64("avg_blocks_sec", s.processedBlockRate.Rate()),
	)

	zlog.Info("substreams sink noop stats", fields...)
}

func (s *Stats) Close() {
	s.LogNow()

	s.Shutdown(nil)
}
