package main

import (
	_ "embed"

	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/derr"
	"github.com/streamingfast/dgrpc"
	"github.com/streamingfast/shutter"
	sinknoop "github.com/streamingfast/substreams-sink-noop"
	"github.com/streamingfast/substreams/client"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

type HeadTracker struct {
	*shutter.Shutter
	client   pbsubstreamsrpc.StreamClient
	callOpts []grpc.CallOption
	headers  client.Headers
	value    AtomicValue[bstream.BlockRef]
}

func NewHeadTracker(client pbsubstreamsrpc.StreamClient, callOpts []grpc.CallOption, headers client.Headers) *HeadTracker {
	return &HeadTracker{
		Shutter:  shutter.New(),
		client:   client,
		callOpts: callOpts,
		headers:  headers,
	}
}

func (s *HeadTracker) Start() {
	zlog.Info("starting head tracker service")

	if s.IsTerminating() || s.IsTerminated() {
		panic("already shutdown, refusing to start again")
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.OnTerminating(func(error) { cancel() })

	go func() {
		activeCursor := ""
		backOff := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
		receivedMessage := false

		for {
			var err error
			activeCursor, receivedMessage, err = s.streamHead(ctx, activeCursor)
			if ctx.Err() != nil {
				// Nothing to do, context canceled or timeout
				return
			}

			// If we received at least one message, we must reset the backoff
			if receivedMessage {
				backOff.Reset()
			}

			if err != nil {
				if errors.Is(err, io.EOF) {
					panic(fmt.Errorf("substreams head tracker should never end"))
				}

				var retryableError *derr.RetryableError
				if errors.As(err, &retryableError) {
					sleepFor := backOff.NextBackOff()
					if sleepFor == backoff.Stop {
						panic(fmt.Errorf("substreams head tracker backOff should never stop"))
					}

					time.Sleep(sleepFor)
				} else {
					zlog.Error("substreams head tracker failed", zap.Error(err))
					return
				}
			}
		}
	}()
}

func (s *HeadTracker) Current() (ref bstream.BlockRef, found bool) {
	ref = s.value.Load()
	found = ref != nil && !bstream.EqualsBlockRefs(ref, bstream.BlockRefEmpty)
	return
}

func (s *HeadTracker) Close() {
	s.Shutdown(nil)
}

func (s *HeadTracker) streamHead(ctx context.Context, activeCursor string) (string, bool, error) {
	receivedMessage := false

	req, err := sinknoop.AddHeadTrackerManifestToSubstreamsRequest(&pbsubstreamsrpc.Request{
		StartBlockNum:  -1,
		StopBlockNum:   0,
		StartCursor:    activeCursor,
		ProductionMode: true,
	})
	if err != nil {
		return activeCursor, receivedMessage, fmt.Errorf("add head tracker manifest to substreams request: %w", err)
	}

	if s.headers.IsSet() {
		ctx = metadata.AppendToOutgoingContext(ctx, s.headers.ToArray()...)
	}

	stream, err := s.client.Blocks(ctx, req, s.callOpts...)
	if err != nil {
		return activeCursor, receivedMessage, retryable(fmt.Errorf("call sf.substreams.rpc.v2.Stream/Blocks: %w", err))
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return activeCursor, receivedMessage, err
			}

			// Unauthanticated and canceled are not retryable
			if dgrpcError := dgrpc.AsGRPCError(err); dgrpcError != nil {
				switch dgrpcError.Code() {
				case codes.Unauthenticated, codes.Canceled:
					return activeCursor, receivedMessage, fmt.Errorf("stream failure: %w", err)
				}
			}

			return activeCursor, receivedMessage, retryable(err)
		}

		receivedMessage = true

		switch r := resp.Message.(type) {
		case *pbsubstreamsrpc.Response_BlockScopedData:
			s.value.Store(bstream.NewBlockRef(r.BlockScopedData.Clock.Id, r.BlockScopedData.Clock.Number))
			activeCursor = r.BlockScopedData.Cursor

		case *pbsubstreamsrpc.Response_BlockUndoSignal:
			undoSignal := r.BlockUndoSignal

			s.value.Store(bstream.NewBlockRef(undoSignal.LastValidBlock.Id, undoSignal.LastValidBlock.Number))
			activeCursor = undoSignal.LastValidCursor
		}
	}
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

func retryable(err error) error {
	return derr.NewRetryableError(err)
}
