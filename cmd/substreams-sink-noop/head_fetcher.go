package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dgrpc"
	pbtransform "github.com/streamingfast/firehose-ethereum/types/pb/sf/ethereum/transform/v1"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v2"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
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
	headBlock, err := s.FetchHeadBlock(ctx)
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
			if err := s.Init(ctx); err != nil && !isCanceledError(err) {
				zlog.Error("unable to fetch head block with retries, head block will be nil until then")
			}
		}

		ticker := time.NewTicker(refreshEach)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				headBlock, err := s.FetchHeadBlock(ctx)
				if err != nil && !isCanceledError(err) {
					zlog.Error("unable to fetch head block with retries, head block will be nil until then")
					continue
				}

				// Could be a canceled error, in which case we must not store the headBlock since it's nil
				if err == nil {
					s.value.Store(headBlock)
				}
			case <-s.Terminating():
				return
			}
		}
	}()
}

// FetchHeadBlock retrieves the head block from a Firehose endpoint and handles retry using an exponential backoff
// algorithm that is going to stop when current retry delay >60s which takes around 120s.
func (s *HeadFetcher) FetchHeadBlock(ctx context.Context) (ref bstream.BlockRef, err error) {
	operation := func() (opErr error) {
		ref, opErr = s.fetchHeadBlock(ctx)
		return opErr
	}

	err = backoff.RetryNotify(
		operation,
		backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 12), ctx),
		func(err error, delay time.Duration) {
			zlog.Error("retrying after error with delay before retry", zap.Duration("delay", delay), zap.Error(err))
		},
	)

	// Only when non-0 we assume the chain block head is right (some chain could have a valid block at 0, but
	// it's not super important for us).
	if err == nil && ref.Num() != 0 {
		ChainHeadBlockNumber.SetUint64(ref.Num())
	}

	return ref, err
}

func (s *HeadFetcher) fetchHeadBlock(ctx context.Context) (ref bstream.BlockRef, err error) {
	// FIXME: Transform per network would be required
	transform, err := anypb.New(&pbtransform.HeaderOnly{})
	if err != nil {
		return ref, fmt.Errorf("header only transform to any: should never happen, message used here is always transformable to *anypb.Any")
	}

	fetchCtx, cancelFetch := context.WithCancel(ctx)
	defer cancelFetch()

	stream, err := s.client.Blocks(fetchCtx, &pbfirehose.Request{
		StartBlockNum:   -1,
		FinalBlocksOnly: false,
		Cursor:          "",
		Transforms:      []*anypb.Any{transform},
	})
	if err != nil {
		return ref, fmt.Errorf("firehose stream blocks: %w", err)
	}

	for {
		response, err := stream.Recv()
		if err != nil {
			return ref, fmt.Errorf("firehose receive block: %w", err)
		}

		if response.Step == pbfirehose.ForkStep_STEP_NEW {
			cursor, err := bstream.CursorFromOpaque(response.Cursor)
			if err != nil {
				return ref, fmt.Errorf("invalid received cursor (maybe you have an outdated 'github.com/streamingfast/bstream' dependency?): %w", err)
			}

			return bstream.NewBlockRef(cursor.Block.ID(), cursor.Block.Num()), nil
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

func isCanceledError(err error) bool {
	return errors.Is(err, context.Canceled) || dgrpc.IsGRPCErrorCode(err, codes.Canceled)
}
