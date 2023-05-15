package main

import (
	"strconv"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dmetrics"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type Stats struct {
	*shutter.Shutter

	headFetcher *HeadFetcher
	lastBlock   bstream.BlockRef
	stopBlock   uint64

	backprocessingCompletion *dmetrics.ValueFromMetric
	headBlockReached         *dmetrics.ValueFromMetric
}

func NewStats(stopBlock uint64, headFetcher *HeadFetcher) *Stats {
	return &Stats{
		Shutter:   shutter.New(),
		stopBlock: stopBlock,

		headFetcher:              headFetcher,
		backprocessingCompletion: dmetrics.NewValueFromMetric(BackprocessingCompletion, "completion"),
		headBlockReached:         dmetrics.NewValueFromMetric(HeadBlockReached, "reached"),
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

	if s.lastBlock != nil {
		toBlockNum := s.stopBlock
		if s.stopBlock == 0 && headBlockFound {
			toBlockNum = headBlock.Num()
		}

		var blockDiff = "<N/A>"
		if toBlockNum > s.lastBlock.Num() {
			blockDiff = strconv.FormatUint(toBlockNum-s.lastBlock.Num(), 10)
		}

		fields = append(fields, zap.String("missing_block", blockDiff))
	}

	if headBlockFound {
		fields = append(fields, zap.Stringer("head_block", headBlock))
	}

	fields = append(fields,
		zap.Bool("backprocessing_completed", s.backprocessingCompletion.ValueUint() > 0),
		zap.Bool("head_block_reached", s.headBlockReached.ValueUint() > 0),
	)

	zlog.Info("substreams sink noop stats", fields...)
}

func (s *Stats) Close() {
	s.LogNow()

	s.Shutdown(nil)
}
