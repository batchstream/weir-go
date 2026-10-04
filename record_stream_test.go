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
	produced, consumed := 0, uint64(0)
	var responseBytes uint64
	options := ReadStreamOptions{StoreName: "records"}
	options.Next = func(context.Context) (*ReadRequest, error) {
		if produced == 48 {
			return nil, io.EOF
		}
		produced++
		request := &ReadRequest{Resource: "records/s:key"}
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
	if produced != 48 || consumed != 48 || responseBytes <= 32<<20 || peer.streams.Load() != 1 || peer.maxBytes.Load() > protocol.MaxExecuteRequestBytes {
		t.Fatal("large call was truncated or split into RPCs", produced, consumed, responseBytes, peer.streams.Load(), peer.maxBytes.Load())
	}
}

func TestMutateStreamPausedConsumerBoundsProducerAndCancels(t *testing.T) {
	peer := &clientTestPeer{mode: "typed_mutation"}
	client := clientTestConnection(t, peer)
	document := &Document{ContentType: "application/octet-stream", Data: make([]byte, protocol.MaxDocument)}
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
	// At most three encoded 2MiB inputs fit the 8MiB window. The producer
	// may yield one unsent item before its size causes it to wait for credit.
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case err := <-done:
		t.Fatal("stream finished while its consumer was paused", err)
	case <-timer.C:
	}
	if count := produced.Load(); count > 4 || peer.received.Load() > 3 || peer.maxBytes.Load() > protocol.MaxExecuteRequestBytes {
		t.Fatal("paused consumer allowed unbounded input", count, peer.received.Load(), peer.maxBytes.Load())
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
		if produced == 64 {
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
		if index == 64 {
			close(confirmed)
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	err := ReadStream(ctx, client, options)
	if err == nil || !strings.Contains(err.Error(), "request 65") || consumed != 64 || peer.streams.Load() != 1 || peer.received.Load() != 64 {
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

func TestRecordStreamSendsBeforeProducerHasItsNextItem(t *testing.T) {
	for _, kind := range []string{"read", "mutate"} {
		t.Run(kind, func(t *testing.T) {
			peer := &clientTestPeer{mode: "typed_mutation"}
			client := clientTestConnection(t, peer)
			firstConsumed := make(chan struct{})
			produced, consumed := 0, uint64(0)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			var err error
			if kind == "read" {
				options := ReadStreamOptions{StoreName: "records"}
				options.Next = func(ctx context.Context) (*ReadRequest, error) {
					if produced == 2 {
						return nil, io.EOF
					}
					if produced == 1 {
						select {
						case <-firstConsumed:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					produced++
					return clientTestReadRequest(), nil
				}
				options.Consume = func(_ context.Context, index uint64, result *ReadResult) error {
					consumed++
					if index != consumed || result.Document == nil {
						return errors.New("unexpected read acknowledgement")
					}
					if index == 1 {
						close(firstConsumed)
					}
					return nil
				}
				err = ReadStream(ctx, client, options)
			} else {
				options := MutateStreamOptions{StoreName: "records"}
				options.Next = func(ctx context.Context) (*MutateRequest, error) {
					if produced == 2 {
						return nil, io.EOF
					}
					if produced == 1 {
						select {
						case <-firstConsumed:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					produced++
					request := &MutateRequest{Resource: "records/s:key", Action: MutationDelete}
					return request, nil
				}
				options.Consume = func(_ context.Context, index uint64, result *MutationResult) error {
					consumed++
					if index != consumed || result.Outcome != MutationApplied {
						return errors.New("unexpected mutation acknowledgement")
					}
					if index == 1 {
						close(firstConsumed)
					}
					return nil
				}
				err = MutateStream(ctx, client, options)
			}
			if err != nil || produced != 2 || consumed != 2 || peer.received.Load() != 2 || peer.streams.Load() != 1 {
				t.Fatal("stream waited for a batch or EOF before sending its first item", err, produced, consumed, peer.received.Load(), peer.streams.Load())
			}
		})
	}
}

func TestReadStreamPausedConsumerBoundsItemWindow(t *testing.T) {
	peer := &clientTestPeer{mode: "read_missing"}
	client := clientTestConnection(t, peer)
	var produced atomic.Int64
	started := make(chan struct{})
	options := ReadStreamOptions{StoreName: "records"}
	options.Next = func(context.Context) (*ReadRequest, error) {
		produced.Add(1)
		return clientTestReadRequest(), nil
	}
	options.Consume = func(ctx context.Context, index uint64, result *ReadResult) error {
		if index != 1 || !result.Missing {
			return errors.New("unexpected first result")
		}
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ReadStream(ctx, client, options) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("consumer did not begin")
	}
	deadline := time.Now().Add(time.Second)
	for produced.Load() < recordWindowItems && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if count := produced.Load(); count != recordWindowItems {
		t.Fatal("paused consumer did not stop the producer at the item window", count)
	}
	time.Sleep(20 * time.Millisecond)
	if produced.Load() != recordWindowItems || peer.received.Load() > recordWindowItems {
		t.Fatal("paused consumer allowed more items than the window", produced.Load(), peer.received.Load())
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled stream succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("item-credit wait did not cancel and join")
	}
}
