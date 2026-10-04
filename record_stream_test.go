package weir

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
)

func TestReadStreamLargeDuplexCallConsumesIncrementally(t *testing.T) {
	peer := &clientTestPeer{mode: "large_duplex"}
	client := clientTestConnection(t, peer)
	adapter := &Document{MediaType: "application/octet-stream", Data: make([]byte, 1<<20)}
	produced, consumed := 0, uint64(0)
	var responseBytes uint64
	options := ReadStreamOptions{StoreName: "records"}
	options.Next = func(context.Context) (*ReadRequest, error) {
		if produced == 48 {
			return nil, io.EOF
		}
		produced++
		request := &ReadRequest{Resource: "records/s:key", AdapterOptions: adapter}
		return request, nil
	}
	options.Consume = func(_ context.Context, index uint64, result *ReadResult) error {
		consumed++
		if index != consumed || result.Document == nil || len(result.Document.Data) != 1<<20 {
			return errors.New("large response index or document mismatch")
		}
		responseBytes += uint64(len(result.Document.Data))
		return nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	if err := ReadStream(ctx, client, options); err != nil {
		t.Fatal(err)
	}
	if produced != 48 || consumed != 48 || responseBytes <= 32<<20 || peer.streams.Load() != 1 || peer.frames.Load() < 2 || peer.maxBytes.Load() > protocol.MaxRecordFrameBytes {
		t.Fatal("large call was truncated, buffered as one frame or split into RPCs", produced, consumed, responseBytes, peer.streams.Load(), peer.frames.Load(), peer.maxBytes.Load())
	}
}

func TestMutateStreamPausedConsumerBoundsProducerAndCancels(t *testing.T) {
	peer := &clientTestPeer{mode: "typed_mutation"}
	client := clientTestConnection(t, peer)
	document := &Document{MediaType: "application/octet-stream", Data: make([]byte, protocol.MaxDocument)}
	var produced atomic.Int64
	started := make(chan struct{})
	options := MutateStreamOptions{StoreName: "records"}
	options.Next = func(ctx context.Context) (*MutateRequest, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		produced.Add(1)
		request := &MutateRequest{Resource: "records/s:key", Action: MutationPut, Document: document}
		return request, nil
	}
	options.Consume = func(ctx context.Context, index uint64, result *MutationResult) error {
		if index != 1 || result.GetOutcome() != MutationApplied {
			return errors.New("invalid first acknowledgement")
		}
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- MutateStream(ctx, client, options) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("consumer did not start")
	}
	// Two 5MiB frames hold two 2MiB documents each. Size-based splitting may
	// inspect one additional item, but a paused consumer must stop the producer.
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case err := <-done:
		t.Fatal("stream finished while its consumer was paused", err)
	case <-timer.C:
	}
	if count := produced.Load(); count > 5 || peer.frames.Load() > 2 || peer.maxBytes.Load() > protocol.MaxRecordFrameBytes {
		t.Fatal("paused consumer allowed unbounded input", count, peer.frames.Load(), peer.maxBytes.Load())
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled stream succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled stream did not join its sender")
	}
}

func TestRecordStreamRejectsDuplicateAndExcessIndexesKeepsPrefix(t *testing.T) {
	for _, mode := range []string{"duplicate_index", "extra_result"} {
		t.Run(mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: mode}
			client := clientTestConnection(t, peer)
			options := ReadOptions{StoreName: "records", Requests: []*ReadRequest{clientTestReadRequest(), clientTestReadRequest()}}
			results, err := Read(t.Context(), client, options)
			if err == nil || len(results) != 2 || results[0] == nil {
				t.Fatal("malformed index succeeded or erased confirmed prefix", results, err)
			}
			if mode == "duplicate_index" && results[1] != nil {
				t.Fatal("duplicate index acknowledged another input", results)
			}
		})
	}
}

func TestReadStreamProducerFailureKeepsEarlierAcknowledgements(t *testing.T) {
	peer := &clientTestPeer{mode: "batch_read_order"}
	client := clientTestConnection(t, peer)
	confirmed := make(chan struct{})
	produced, consumed := 0, uint64(0)
	options := ReadStreamOptions{StoreName: "records"}
	options.Next = func(ctx context.Context) (*ReadRequest, error) {
		if produced == protocol.MaxRecordFrameItems {
			select {
			case <-confirmed:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			request := &ReadRequest{Resource: "bad//resource"}
			return request, nil
		}
		produced++
		return clientTestReadRequest(), nil
	}
	options.Consume = func(_ context.Context, index uint64, result *ReadResult) error {
		consumed++
		if index != consumed || result == nil {
			return errors.New("invalid earlier acknowledgement")
		}
		if index == protocol.MaxRecordFrameItems {
			close(confirmed)
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	err := ReadStream(ctx, client, options)
	if err == nil || !strings.Contains(err.Error(), "request 1025") || consumed != protocol.MaxRecordFrameItems || peer.streams.Load() != 1 || peer.received.Load() != protocol.MaxRecordFrameItems {
		t.Fatal("producer failure replayed requests or discarded earlier evidence", err, consumed, peer.streams.Load(), peer.received.Load())
	}
}

func TestReadStreamCancellationJoinsBlockedProducer(t *testing.T) {
	peer := &clientTestPeer{}
	client := clientTestConnection(t, peer)
	started, stopped := make(chan struct{}), make(chan struct{})
	options := ReadStreamOptions{StoreName: "records"}
	options.Next = func(ctx context.Context) (*ReadRequest, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return nil, ctx.Err()
	}
	options.Consume = func(context.Context, uint64, *ReadResult) error {
		return errors.New("no input was produced")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ReadStream(ctx, client, options) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("producer did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled producer succeeded")
		}
		select {
		case <-stopped:
		default:
			t.Fatal("stream returned before producer finished")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked producer was not cancelled and joined")
	}
}

func TestReadStreamRejectsPrematureSuccessAndJoinsSender(t *testing.T) {
	peer := &clientTestPeer{mode: "early_eof"}
	client := clientTestConnection(t, peer)
	options := ReadStreamOptions{StoreName: "records"}
	options.Next = func(context.Context) (*ReadRequest, error) { return clientTestReadRequest(), nil }
	options.Consume = func(context.Context, uint64, *ReadResult) error { return nil }
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := ReadStream(ctx, client, options); err == nil {
		t.Fatal("premature server EOF accepted an unfinished producer")
	}
}
