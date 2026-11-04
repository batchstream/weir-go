package weir

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	"google.golang.org/grpc/metadata"
)

func TestNextLargeDuplexCallConsumesIncrementally(t *testing.T) {
	peer := &clientTestPeer{mode: "large_duplex"}
	client := clientTestClient(t, peer)
	produced, consumed := 0, 0
	var responseBytes uint64
	next := func(context.Context) (*ReadRequest, error) {
		if produced == 48 {
			return nil, io.EOF
		}
		produced++
		request := &ReadRequest{Resource: "records/s:key"}
		return request, nil
	}
	consume := func(_ context.Context, _ int, result *ReadResult) error {
		consumed++
		if result.Document == nil || len(result.Document.Data) != 1<<20 {
			return errors.New("large response document mismatch")
		}
		responseBytes += uint64(len(result.Document.Data))
		return nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	_, err := client.Read(ctx, "records", WithNext(next), WithReadConsume(consume))
	if err != nil {
		t.Fatal(err)
	}
	if produced != 48 || consumed != 48 || responseBytes <= 32<<20 || peer.streams.Load() != 1 || peer.maxBytes.Load() > int64(protocol.MaxTransportMessageBytes) {
		t.Fatal("large call was truncated or split into RPCs", produced, consumed, responseBytes, peer.streams.Load(), peer.maxBytes.Load())
	}
}

func TestPausedConsumerBoundsProducedBytesAndCancels(t *testing.T) {
	peer := &clientTestPeer{mode: "typed_mutation"}
	client := clientTestClient(t, peer)
	document := &Document{ContentType: "application/octet-stream", Data: make([]byte, (2 << 20))}
	var produced atomic.Int64
	started := make(chan struct{})
	next := func(ctx context.Context) (*MutateRequest, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		count := produced.Add(1)
		resource := "records/s:" + strings.Repeat("k", int(count))
		request := &MutateRequest{Resource: resource, Action: MutationPut, Document: document}
		return request, nil
	}
	once := sync.Once{}
	consume := func(ctx context.Context, _ int, result *MutationResult) error {
		if result.GetOutcome() != MutationApplied {
			return errors.New("invalid first acknowledgement")
		}
		once.Do(func() { close(started) })
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.Mutate(ctx, "records", WithNext(next), WithMutateConsume(consume))
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("consumer did not start")
	}
	// At most three encoded 2MiB inputs fit the 8MiB window. The producer may
	// yield one unsent item before its size causes it to wait for credit.
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case err := <-done:
		t.Fatal("call finished while its consumer was paused", err)
	case <-timer.C:
	}
	if count := produced.Load(); count > 5 || peer.received.Load() > 4 || peer.maxBytes.Load() > int64(protocol.MaxTransportMessageBytes) {
		t.Fatal("paused consumer allowed unbounded input", count, peer.received.Load(), peer.maxBytes.Load())
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled call succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled call did not join its sender")
	}
}

func TestCallRejectsDuplicateIndexAndKeepsEarlierResult(t *testing.T) {
	peer := &clientTestPeer{mode: "duplicate_index"}
	client := clientTestClient(t, peer)
	results, err := client.Read(t.Context(), "records", WithRequests(clientTestReadRequest(), clientTestReadRequest()))
	if err == nil || len(results) != 2 || results[0] == nil {
		t.Fatal("a duplicate index succeeded or erased a confirmed result", results, err)
	}
	if results[1] != nil {
		t.Fatal("a duplicate index acknowledged another input", results)
	}
}

// A call that has one result per input is complete, so what its stream does next
// is not its failure, as with TestReadSucceedsOnceEveryResultArrived.
func TestExtraResultAfterEveryInputsResultKeepsTheCall(t *testing.T) {
	peer := &clientTestPeer{mode: "extra_result"}
	client := clientTestClient(t, peer)
	results, err := client.Read(t.Context(), "records", WithRequests(clientTestReadRequest(), clientTestReadRequest()))
	if err != nil || len(results) != 2 || results[0] == nil || results[1] == nil {
		t.Fatal("an extra result changed a complete call", results, err)
	}
}

func TestProducerFailureKeepsEarlierAcknowledgements(t *testing.T) {
	peer := &clientTestPeer{mode: "batch_read_order"}
	client := clientTestClient(t, peer)
	confirmed := make(chan struct{})
	produced, consumed := 0, 0
	next := func(ctx context.Context) (*ReadRequest, error) {
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
	consume := func(_ context.Context, _ int, result *ReadResult) error {
		consumed++
		if result == nil {
			return errors.New("invalid earlier acknowledgement")
		}
		if consumed == 64 {
			close(confirmed)
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, err := client.Read(ctx, "records", WithNext(next), WithReadConsume(consume))
	if err == nil || !strings.Contains(err.Error(), "request 65") || consumed != 64 || peer.streams.Load() != 1 || peer.received.Load() != 64 {
		t.Fatal("producer failure replayed requests or discarded earlier evidence", err, consumed, peer.streams.Load(), peer.received.Load())
	}
}

func TestCancellationJoinsBlockedProducer(t *testing.T) {
	peer := &clientTestPeer{}
	client := clientTestClient(t, peer)
	started, stopped := make(chan struct{}), make(chan struct{})
	next := func(ctx context.Context) (*ReadRequest, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return nil, ctx.Err()
	}
	consume := func(context.Context, int, *ReadResult) error {
		return errors.New("no input was produced")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.Read(ctx, "records", WithNext(next), WithReadConsume(consume))
		done <- err
	}()
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
			t.Fatal("call returned before producer finished")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked producer was not cancelled and joined")
	}
}

func TestCallRejectsPrematureSuccessAndJoinsSender(t *testing.T) {
	peer := &clientTestPeer{mode: "early_eof"}
	client := clientTestClient(t, peer)
	next := func(context.Context) (*ReadRequest, error) { return clientTestReadRequest(), nil }
	consume := func(context.Context, int, *ReadResult) error { return nil }
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, err := client.Read(ctx, "records", WithNext(next), WithReadConsume(consume))
	if err == nil {
		t.Fatal("premature server EOF accepted an unfinished producer")
	}
}

func TestProducedRecordIsSentBeforeTheNextIsProduced(t *testing.T) {
	for _, kind := range []string{"read", "mutate"} {
		t.Run(kind, func(t *testing.T) {
			peer := &clientTestPeer{mode: "typed_mutation"}
			client := clientTestClient(t, peer)
			firstConsumed := make(chan struct{})
			produced, consumed := 0, 0
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			var err error
			wait := func(ctx context.Context) error {
				if produced != 1 {
					return nil
				}
				select {
				case <-firstConsumed:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			confirm := func(index int) {
				if consumed++; index == 0 {
					close(firstConsumed)
				}
			}
			if kind == "read" {
				next := func(ctx context.Context) (*ReadRequest, error) {
					if produced == 2 {
						return nil, io.EOF
					}
					if err := wait(ctx); err != nil {
						return nil, err
					}
					produced++
					return clientTestReadRequest(), nil
				}
				consume := func(_ context.Context, index int, result *ReadResult) error {
					if result.Document == nil {
						return errors.New("unexpected read acknowledgement")
					}
					confirm(index)
					return nil
				}
				_, err = client.Read(ctx, "records", WithNext(next), WithReadConsume(consume))
			} else {
				next := func(ctx context.Context) (*MutateRequest, error) {
					if produced == 2 {
						return nil, io.EOF
					}
					if err := wait(ctx); err != nil {
						return nil, err
					}
					produced++
					return deleteRequest("records/s:key"), nil
				}
				consume := func(_ context.Context, index int, result *MutationResult) error {
					if result.Outcome != MutationApplied {
						return errors.New("unexpected mutation acknowledgement")
					}
					confirm(index)
					return nil
				}
				_, err = client.Mutate(ctx, "records", WithNext(next), WithMutateConsume(consume))
			}
			if err != nil || produced != 2 || consumed != 2 || peer.received.Load() != 2 || peer.streams.Load() != 1 {
				t.Fatal("call waited for a batch or EOF before sending its first item", err, produced, consumed, peer.received.Load(), peer.streams.Load())
			}
		})
	}
}

func TestPausedConsumerBoundsItemWindow(t *testing.T) {
	peer := &clientTestPeer{mode: "read_missing"}
	client := clientTestClient(t, peer)
	var produced atomic.Int64
	started := make(chan struct{})
	next := func(context.Context) (*ReadRequest, error) {
		produced.Add(1)
		return clientTestReadRequest(), nil
	}
	consume := func(ctx context.Context, _ int, result *ReadResult) error {
		if !result.Missing {
			return errors.New("unexpected first result")
		}
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.Read(ctx, "records", WithNext(next), WithReadConsume(consume))
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
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
			t.Fatal("cancelled call succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("item-credit wait did not cancel and join")
	}
}

// readsAfter produces reads of the given resources, then io.EOF.
func readsAfter(resources ...string) func(context.Context) (*ReadRequest, error) {
	position := 0
	next := func(context.Context) (*ReadRequest, error) {
		if position == len(resources) {
			return nil, io.EOF
		}
		request := &ReadRequest{Resource: resources[position]}
		position++
		return request, nil
	}
	return next
}

func TestRequestsGoBeforeNextAndKeepOneIndexSequence(t *testing.T) {
	peer := newRefusingPeer()
	client := refusingClient(t, peer)
	first := putRequest("records/s:same", `{"n":1}`)
	second := putRequest("records/s:same", `{"n":2}`)
	produced := false
	next := func(context.Context) (*MutateRequest, error) {
		if produced {
			return nil, io.EOF
		}
		produced = true
		return putRequest("records/s:same", `{"n":3}`), nil
	}
	var indexes []int
	consume := func(_ context.Context, index int, result *MutationResult) error {
		if result.GetOutcome() != MutationApplied {
			t.Error(result)
		}
		indexes = append(indexes, index)
		return nil
	}
	results, err := client.Mutate(t.Context(), "records", WithRequests(first, second), WithNext(next), WithMutateConsume(consume))
	if err != nil || results != nil {
		t.Fatal(results, err)
	}
	// One resource runs in input order across both sources.
	if applied := peer.drain(); !slices.Equal(applied, []string{`{"n":1}`, `{"n":2}`, `{"n":3}`}) {
		t.Fatal("a resource's records did not keep input order across requests and Next", applied)
	}
	if !slices.Equal(indexes, []int{0, 1, 2}) {
		t.Fatal("the index did not continue from requests into Next", indexes)
	}
}

func TestNextStartsWithoutWaitingForResultsOfRequests(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	laneTestReady(t, client, peer)
	nextCalled := make(chan struct{})
	once := sync.Once{}
	next := func(ctx context.Context) (*MutateRequest, error) {
		once.Do(func() { close(nextCalled) })
		return nil, io.EOF
	}
	held := putRequest("records/s:held", `{}`)
	done := make(chan error, 1)
	go func() {
		_, err := client.Mutate(t.Context(), "records", WithRequests(held), WithNext(next))
		done <- err
	}()
	select {
	case <-nextCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("Next waited for the result of a held request")
	}
	close(peer.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestNextWithoutConsumeCollectsEveryResult(t *testing.T) {
	peer := &clientTestPeer{mode: "read_missing"}
	client := clientTestClient(t, peer)
	results, err := client.Read(t.Context(), "records", WithRequests(clientTestReadRequest()), WithNext(readsAfter("records/s:a", "records/s:b")))
	if err != nil || len(results) != 3 {
		t.Fatal(results, err)
	}
	for index, result := range results {
		if result == nil || !result.Missing {
			t.Fatal(index, result)
		}
	}
}

func TestNextWithoutConsumeKeepsUnconfirmedPositions(t *testing.T) {
	peer := &clientTestPeer{mode: "batch_partial"}
	client := clientTestClient(t, peer)
	produced := 0
	next := func(context.Context) (*MutateRequest, error) {
		if produced == 2 {
			return nil, io.EOF
		}
		produced++
		return deleteRequest("records/s:" + strings.Repeat("k", produced)), nil
	}
	results, err := client.Mutate(t.Context(), "records", WithNext(next))
	if err == nil || len(results) == 0 || results[0].GetOutcome() != MutationApplied {
		t.Fatal(results, err)
	}
	// Every produced record has a position, so one without a result stays visible.
	if len(results) < 2 || results[len(results)-1] != nil {
		t.Fatal("a record without a result lost its position", results)
	}
}

func TestInvalidRequestRejectsTheCallBeforeNext(t *testing.T) {
	peer := &clientTestPeer{}
	client := clientTestClient(t, peer)
	called := false
	next := func(context.Context) (*ReadRequest, error) {
		called = true
		return nil, io.EOF
	}
	invalid := &ReadRequest{Resource: "bad//resource"}
	results, err := client.Read(t.Context(), "records", WithRequests(clientTestReadRequest(), invalid), WithNext(next))
	if err == nil || len(results) != 2 || results[0] != nil || results[1] != nil || called || peer.streams.Load() != 0 {
		t.Fatal("an invalid request did not reject the call", results, err, called, peer.streams.Load())
	}
}

func TestRequestsWithConsumeCollectNothing(t *testing.T) {
	peer := &clientTestPeer{mode: "read_missing"}
	client := clientTestClient(t, peer)
	seen := make(map[int]bool)
	consume := func(_ context.Context, index int, result *ReadResult) error {
		if !result.Missing {
			t.Error(index, result)
		}
		seen[index] = true
		return nil
	}
	results, err := client.Read(t.Context(), "records", WithRequests(clientTestReadRequest(), clientTestReadRequest()), WithReadConsume(consume))
	if err != nil || results != nil || !seen[0] || !seen[1] || len(seen) != 2 {
		t.Fatal(results, err, seen)
	}
}

func TestOptionsCombine(t *testing.T) {
	peer := &clientTestPeer{mode: "read_missing"}
	client := clientTestClient(t, peer)
	var absent ReadOption
	results, err := client.Read(t.Context(), "records", absent, WithRequests(clientTestReadRequest()), WithRequests(clientTestReadRequest()), WithNext(readsAfter("records/s:ignored")), WithNext(readsAfter()))
	if err != nil || len(results) != 2 || peer.received.Load() != 2 {
		t.Fatal("options did not add requests or override Next", results, err, peer.received.Load())
	}
}

func TestSingleRecordSharesAStreamAndOtherCallsOwnOne(t *testing.T) {
	peer := &countsMessages{}
	address := serveForTest(t, peer)
	client, err := New(address)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	own := func() int {
		client.lanes.mu.Lock()
		defer client.lanes.mu.Unlock()
		count := 0
		for key := range client.lanes.groups {
			if key.owner != 0 {
				count++
			}
		}
		return count
	}
	shared := func() int {
		client.lanes.mu.Lock()
		defer client.lanes.mu.Unlock()
		key := laneKey{store: "records"}
		if group := client.lanes.groups[key]; group != nil {
			return len(group.lanes)
		}
		return 0
	}
	for range 3 {
		if _, err := mutateOne(t.Context(), client, putRequest("records/s:a", `{}`)); err != nil {
			t.Fatal(err)
		}
	}
	if shared() != 1 || own() != 0 {
		t.Fatal("single records did not share one stream", shared(), own())
	}
	// A call with a callback owns a stream, which it releases at its end.
	consume := func(context.Context, int, *MutationResult) error {
		if own() != 1 {
			t.Error("a call with a callback does not own a stream")
		}
		return nil
	}
	record := putRequest("records/s:b", `{}`)
	_, err = client.Mutate(t.Context(), "records", WithRequests(record), WithMutateConsume(consume))
	if err != nil {
		t.Fatal(err)
	}
}

func TestSingleRecordWithOutgoingMetadataSendsIt(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	ctx := metadata.AppendToOutgoingContext(t.Context(), "x-request", "seven")
	if _, err := mutateOne(ctx, client, putRequest("records/s:a", `{}`)); err != nil {
		t.Fatal(err)
	}
	incoming, _ := peer.outgoing.Load().(metadata.MD)
	if values := incoming.Get("x-request"); len(values) != 1 || values[0] != "seven" {
		t.Fatal("a single record dropped its caller's metadata", incoming)
	}
}
