package main

import (
	"strconv"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type Stats struct {
	*shutter.Shutter

	headFetcher *HeadFetcher
	lastBlock   bstream.BlockRef
	stopBlock   uint64

	backprocessingCompletion *ValueFromGauge
	headBlockReached         *ValueFromGauge

	dataMsgRate     *RateFromCounter
	progressMsgRate *RateFromCounter
}

func NewStats(stopBlock uint64, headFetcher *HeadFetcher) *Stats {
	return &Stats{
		Shutter:   shutter.New(),
		stopBlock: stopBlock,

		headFetcher: headFetcher,

		backprocessingCompletion: NewValueFromGauge(BackprocessingCompletion, "completion"),
		headBlockReached:         NewValueFromGauge(HeadBlockReached, "reached"),

		dataMsgRate:     NewPerSecondRateFromCounter(DataMessageCount, "msg"),
		progressMsgRate: NewPerSecondRateFromCounter(ProgressMessageCount, "msg"),
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
				// Logging fields order is important as it affects the final rendering, we carefully ordered
				// them so the development logs looks nicer.
				fields := []zap.Field{
					zap.Stringer("data_msg_rate", s.dataMsgRate),
					zap.Stringer("progress_msg_rate", s.progressMsgRate),
				}

				headBlock, headBlockFound := s.headFetcher.Current()

				if s.lastBlock == nil {
					fields = append(fields, zap.String("last_block", "None"))
				} else {
					fields = append(fields, zap.Stringer("last_block", s.lastBlock))

					toBlockNum := s.stopBlock
					if s.stopBlock == 0 && headBlockFound {
						toBlockNum = headBlock.Num()
					}

					var blockDiff = "<N/A>"
					if toBlockNum > s.lastBlock.Num() {
						blockDiff = strconv.FormatUint(toBlockNum-s.lastBlock.Num(), 10)
					}

					zap.String("missing_block", blockDiff)
				}

				if headBlockFound {
					fields = append(fields, zap.Stringer("head_block", headBlock))
				}

				fields = append(fields,
					zap.Bool("backprocessing_completed", s.backprocessingCompletion.ValueUint() > 0),
					zap.Bool("head_block_reached", s.headBlockReached.ValueUint() > 0),
				)

				zlog.Info("substreams consumer stats", fields...)
			case <-s.Terminating():
				break
			}
		}
	}()
}

func (s *Stats) Close() {
	s.Shutdown(nil)
}
