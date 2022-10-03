package main

import (
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type Stats struct {
	*shutter.Shutter

	lastBlock bstream.BlockRef
	stopBlock uint64

	backprocessingCompletion *ValueFromGauge
	dataMsgRate              *RateFromCounter
	progressMsgRate          *RateFromCounter
}

func NewStats(stopBlock uint64) *Stats {
	return &Stats{
		Shutter:   shutter.New(),
		stopBlock: stopBlock,

		backprocessingCompletion: NewValueFromGauge(BackprocessingCompletion, "completion"),
		dataMsgRate:              NewPerSecondRateFromCounter(DataMessageCount, "msg"),
		progressMsgRate:          NewPerSecondRateFromCounter(ProgressMessageCount, "msg"),
	}
}

func (s *Stats) Start(each time.Duration) {
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

				if s.stopBlock != 0 {
				}

				if s.lastBlock == nil {
					fields = append(fields, zap.String("last_block", "None"))
				} else {
					fields = append(fields,
						zap.Uint64("missing_block", s.stopBlock-s.lastBlock.Num()),
						zap.Stringer("last_block", s.lastBlock),
					)
				}

				fields = append(fields,
					zap.Bool("backprocessing_completed", s.backprocessingCompletion.ValueUint() > 0),
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
