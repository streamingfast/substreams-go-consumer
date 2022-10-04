package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/streamingfast/bstream"
	pbtransform "github.com/streamingfast/firehose-ethereum/types/pb/sf/ethereum/transform/v1"
	pbeth "github.com/streamingfast/firehose-ethereum/types/pb/sf/ethereum/type/v2"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v2"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
)

type HeadFetcher struct {
	*shutter.Shutter
	client pbfirehose.StreamClient
	value  AtomicValue[bstream.BlockRef]
}

func NewHeadFetcher(client pbfirehose.StreamClient) *HeadFetcher {
	return &HeadFetcher{
		Shutter: shutter.New(),
		client:  client,
	}
}

func (s *HeadFetcher) Init(ctx context.Context) error {
	headBlock, err := s.FetchHeadBlockWithRetries(ctx)
	if err != nil {
		return err
	}

	if headBlock != nil {
		s.value.Store(headBlock)
	}

	return nil
}

func (s *HeadFetcher) Start(refreshEach time.Duration) {
	zlog.Info("starting head fetcher service", zap.Duration("refreshes_each", refreshEach))

	if s.IsTerminating() || s.IsTerminated() {
		panic("already shutdown, refusing to start again")
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.OnTerminating(func(error) { cancel() })

	go func() {
		if s.value.Load() == nil {
			if err := s.Init(ctx); err != nil {
				zlog.Error("unable to fetch head block with retries, head block will be nil until then")
			}
		}

		ticker := time.NewTicker(refreshEach)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				headBlock, err := s.FetchHeadBlockWithRetries(ctx)
				if err != nil {
					zlog.Error("unable to fetch head block with retries, head block will be nil until then")
					continue
				}

				s.value.Store(headBlock)
			case <-s.Terminating():
				break
			}
		}
	}()
}

func (s *HeadFetcher) FetchHeadBlockWithRetries(ctx context.Context) (bstream.BlockRef, error) {
	transform, err := anypb.New(&pbtransform.LightBlock{})
	if err != nil {
		return bstream.BlockRefEmpty, fmt.Errorf("light block transform to any: should never happen, message used here is always transformable to *anypb.Any")
	}

	maxTimeCtx, cancelTimeout := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelTimeout()

	fetchCtx, cancelFetch := context.WithCancel(maxTimeCtx)
	defer cancelFetch()

	retryWaitTime := time.Duration(0)
	var lastErr error

	for {
		if retryWaitTime != 0 {
			if errors.Is(lastErr, context.Canceled) || errors.Is(lastErr, context.DeadlineExceeded) {
				return bstream.BlockRefEmpty, fetchCtx.Err()
			}

			FirehoseErrorCount.Inc()
			zlog.Error("retrying after error with delay before retry", zap.Duration("delay", retryWaitTime), zap.Error(err))
			time.Sleep(retryWaitTime)
		} else {
			retryWaitTime = 5 * time.Second
		}

		stream, err := s.client.Blocks(fetchCtx, &pbfirehose.Request{
			StartBlockNum:   -1,
			FinalBlocksOnly: false,
			Cursor:          "",
			Transforms:      []*anypb.Any{transform},
		})
		if err != nil {
			lastErr = fmt.Errorf("firehose stream blocks: %w", err)
			continue
		}

		for {
			response, err := stream.Recv()
			if err != nil {
				lastErr = fmt.Errorf("firehose receive block: %w", err)
				break
			}

			if response.Step == pbfirehose.ForkStep_STEP_NEW {
				// FIXME: Works only on Ethereum models!
				var block pbeth.Block
				if err := response.Block.UnmarshalTo(&block); err != nil {
					// No retry there is something fishy so we return right away
					return bstream.BlockRefEmpty, fmt.Errorf("unable to read Ethereum block: %w", err)
				}

				return bstream.NewBlockRef(hex.EncodeToString(block.Hash), block.Number), err
			}
		}
	}
}

func (s *HeadFetcher) Current() (ref bstream.BlockRef, found bool) {
	ref = s.value.Load()
	found = ref != nil && !bstream.EqualsBlockRefs(ref, bstream.BlockRefEmpty)
	return
}

func (s *HeadFetcher) Close() {
	s.Shutdown(nil)
}

type AtomicValue[T any] struct {
	value atomic.Value
}

func (v *AtomicValue[T]) Load() T {
	if v := v.value.Load(); v == nil {
		var t T
		return t
	} else {
		return v.(T)
	}
}

func (v *AtomicValue[T]) Store(val T) {
	v.value.Store(val)
}

func (v *AtomicValue[T]) Swap(new T) (old T) {
	if v := v.value.Swap(new); v == nil {
		var t T
		return t
	} else {
		return v.(T)
	}
}
