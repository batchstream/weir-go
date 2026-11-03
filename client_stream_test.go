package weir

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// heldStream is a MutateStream of a Client that a test feeds record by record.
type heldStream struct {
	input    chan string
	produced atomic.Int64
	mu       sync.Mutex
	results  map[uint64]*MutationResult
	done     chan error
}

func openHeldStream(ctx context.Context, client *Client) *heldStream {
	held := &heldStream{input: make(chan string), results: make(map[uint64]*MutationResult), done: make(chan error, 1)}
	options := MutateStreamOptions{StoreName: "records"}
	options.Next = func(ctx context.Context) (*MutateRequest, error) {
		select {
		case resource, ok := <-held.input:
			if !ok {
				return nil, io.EOF
			}
			held.produced.Add(1)
			return putRequest(resource, `{}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	options.Consume = func(_ context.Context, index uint64, result *MutationResult) error {
		held.mu.Lock()
		defer held.mu.Unlock()
		held.results[index] = result
		return nil
	}
	go func() { held.done <- client.MutateStream(ctx, options) }()
	return held
}

// confirmed waits until count results arrived and returns those not APPLIED.
func (h *heldStream) confirmed(t *testing.T, count int) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		h.mu.Lock()
		arrived, other := len(h.results), 0
		for _, result := range h.results {
			if result.GetOutcome() != MutationApplied {
				other++
			}
		}
		h.mu.Unlock()
		if arrived >= count {
			return other
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d results arrived, want %d", arrived, count)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// finish ends the input and returns the stream's error.
func (h *heldStream) finish(t *testing.T) error {
	t.Helper()
	close(h.input)
	select {
	case err := <-h.done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("the stream did not end")
		return nil
	}
}

// laneTestHolder returns the peer holding the stream's records and the other.
func laneTestHolder(t *testing.T, before []int64, first, second *laneTestPeer) (*laneTestPeer, *laneTestPeer, int) {
	t.Helper()
	opened, otherOpened := first.streams.Load()-before[0], second.streams.Load()-before[1]
	switch {
	case opened == 1 && otherOpened == 0:
		return first, second, 0
	case opened == 0 && otherOpened == 1:
		return second, first, 1
	}
	t.Fatalf("a stream opened %d and %d streams, want one", opened, otherOpened)
	return nil, nil, 0
}

func waitForHalfClose(t *testing.T, peer *laneTestPeer, count int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for peer.halfClose.Load() < count {
		if time.Now().After(deadline) {
			t.Fatal("the stream on the server it left did not half-close", peer.halfClose.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestClientStreamLeavesAServerThatStopsResolving(t *testing.T) {
	first, second := newLaneTestPeer(), newLaneTestPeer()
	client, servers := laneTestResolved(t, first, second)
	before := laneTestReady(t, client, first, second)
	held := openHeldStream(t.Context(), client)
	held.input <- "records/s:a"
	held.confirmed(t, 1)
	holder, other, position := laneTestHolder(t, before, first, second)
	opened := other.streams.Load()
	servers.resolve(servers.addresses[1-position])
	for index := range 20 {
		held.input <- fmt.Sprintf("records/s:next-%d", index)
	}
	if refused := held.confirmed(t, 21); refused != 0 {
		t.Fatal("records were refused while the server left", refused)
	}
	if other.streams.Load() != opened+1 {
		t.Fatal("the stream did not move to the remaining server", other.streams.Load(), opened)
	}
	if err := held.finish(t); err != nil {
		t.Fatal(err)
	}
	waitForHalfClose(t, holder, 1)
}

func TestClientStreamMovesOffADrainingServerWithoutFailingARecord(t *testing.T) {
	first, second := newLaneTestPeer(), newLaneTestPeer()
	client, _ := laneTestResolved(t, first, second)
	client.resends = newResendBudget()
	before := laneTestReady(t, client, first, second)
	held := openHeldStream(t.Context(), client)
	held.input <- "records/s:a"
	held.confirmed(t, 1)
	holder, other, _ := laneTestHolder(t, before, first, second)
	opened := other.streams.Load()
	holder.draining.Store(true)
	for index := range 40 {
		held.input <- fmt.Sprintf("records/s:next-%d", index)
	}
	if refused := held.confirmed(t, 41); refused != 0 {
		t.Fatal("a refusal reached the consumer", refused)
	}
	if other.streams.Load() != opened+1 {
		t.Fatal("the stream did not move to the other server", other.streams.Load(), opened)
	}
	if err := held.finish(t); err != nil {
		t.Fatal(err)
	}
}

func TestClientStreamMovesWhenItsStreamRetires(t *testing.T) {
	peer := newLaneTestPeer()
	client, _ := laneTestResolved(t, peer)
	before := laneTestReady(t, client, peer)[0]
	held := openHeldStream(t.Context(), client)
	held.input <- "records/s:a"
	held.confirmed(t, 1)
	pool := client.lanes
	pool.mu.Lock()
	for key, group := range pool.groups {
		if key.owner == 0 {
			continue
		}
		for _, stream := range group.lanes {
			stream.expires = time.Now().Add(-time.Second)
		}
	}
	pool.mu.Unlock()
	held.input <- "records/s:b"
	held.confirmed(t, 2)
	if opened := peer.streams.Load() - before; opened != 2 {
		t.Fatal("a retired stream was not replaced", opened)
	}
	waitForHalfClose(t, peer, 1)
	if err := held.finish(t); err != nil {
		t.Fatal(err)
	}
}

func TestClientStreamRunsEachResourceInInputOrder(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	laneTestReady(t, client, peer)
	for len(peer.arrived) > 0 {
		<-peer.arrived
	}
	held := openHeldStream(t.Context(), client)
	held.input <- "records/s:held"
	if arrived := <-peer.arrived; arrived != "records/s:held" {
		t.Fatal(arrived)
	}
	// The second record of the resource waits; another resource passes it.
	held.input <- "records/s:held"
	held.input <- "records/s:other"
	if arrived := <-peer.arrived; arrived != "records/s:other" {
		t.Fatal("a record of the running resource was sent before its turn", arrived)
	}
	held.confirmed(t, 1)
	select {
	case arrived := <-peer.arrived:
		t.Fatal("a record of the running resource was sent before its turn", arrived)
	case <-time.After(50 * time.Millisecond):
	}
	close(peer.release)
	if arrived := <-peer.arrived; arrived != "records/s:held" {
		t.Fatal(arrived)
	}
	held.confirmed(t, 3)
	if err := held.finish(t); err != nil {
		t.Fatal(err)
	}
}

func TestClientStreamUsesOneStreamAndBoundsItsWindow(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	before := laneTestReady(t, client, peer)[0]
	held := openHeldStream(t.Context(), client)
	feed := make(chan error, 1)
	go func() {
		for index := range recordWindowItems + 8 {
			select {
			case held.input <- fmt.Sprintf("records/s:held-%d", index):
			case <-t.Context().Done():
				feed <- t.Context().Err()
				return
			}
		}
		feed <- nil
	}()
	deadline := time.Now().Add(5 * time.Second)
	for peer.held.Load() < recordWindowItems {
		if time.Now().After(deadline) {
			t.Fatal("the window did not fill", peer.held.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if sent := peer.held.Load(); sent != recordWindowItems {
		t.Fatal("more records than the window were sent", sent)
	}
	if opened := peer.streams.Load() - before; opened != 1 {
		t.Fatal("a stream at a full window opened", opened, "streams")
	}
	close(peer.release)
	if err := <-feed; err != nil {
		t.Fatal(err)
	}
	held.confirmed(t, recordWindowItems+8)
	if err := held.finish(t); err != nil {
		t.Fatal(err)
	}
	if opened := peer.streams.Load() - before; opened != 1 {
		t.Fatal("the stream opened", opened, "streams")
	}
}

func TestClientStreamPausedConsumerStopsTheProducer(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	release := make(chan struct{})
	var produced atomic.Int64
	options := MutateStreamOptions{StoreName: "records"}
	options.Next = func(context.Context) (*MutateRequest, error) {
		if produced.Add(1) > 2*recordWindowItems {
			return nil, io.EOF
		}
		return putRequest(fmt.Sprintf("records/s:%d", produced.Load()), `{}`), nil
	}
	options.Consume = func(ctx context.Context, _ uint64, _ *MutationResult) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() { done <- client.MutateStream(t.Context(), options) }()
	time.Sleep(100 * time.Millisecond)
	// The window's records plus the one the producer holds waiting for room.
	if count := produced.Load(); count > recordWindowItems+1 {
		t.Fatal("a paused consumer did not stop the producer", count)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestClientStreamFailureKeepsWhatWasConfirmedAndResendsNothing(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	client.resends = newResendBudget()
	laneTestReady(t, client, peer)
	for len(peer.arrived) > 0 {
		<-peer.arrived
	}
	held := openHeldStream(t.Context(), client)
	held.input <- "records/s:a"
	held.confirmed(t, 1)
	held.input <- "records/s:broken"
	select {
	case err := <-held.done:
		if status.Code(err) != codes.Unavailable {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the stream did not fail")
	}
	held.mu.Lock()
	defer held.mu.Unlock()
	if len(held.results) != 1 || held.results[1] == nil {
		t.Fatal("confirmed results differ", held.results)
	}
	var sent []string
	for len(peer.arrived) > 0 {
		sent = append(sent, <-peer.arrived)
	}
	if !slices.Equal(sent, []string{"records/s:a", "records/s:broken"}) {
		t.Fatal("a record whose outcome is unknown was sent again", sent)
	}
}

func TestClientStreamReportsItsCallbacksAndInputs(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	failure := errors.New("application failure")
	one := func() func(context.Context) (*MutateRequest, error) {
		sent := false
		return func(context.Context) (*MutateRequest, error) {
			if sent {
				return nil, io.EOF
			}
			sent = true
			return putRequest("records/s:a", `{}`), nil
		}
	}
	consume := func(context.Context, uint64, *MutationResult) error { return nil }
	tests := []struct {
		name    string
		options MutateStreamOptions
		want    string
	}{
		{name: "no callbacks", options: MutateStreamOptions{StoreName: "records"}, want: "requires a producer"},
		{name: "invalid store", options: MutateStreamOptions{StoreName: "", Next: one(), Consume: consume}, want: "valid Store"},
		{name: "empty input", options: MutateStreamOptions{StoreName: "records", Consume: consume, Next: func(context.Context) (*MutateRequest, error) { return nil, io.EOF }}, want: "at least one record"},
		{name: "producer error", options: MutateStreamOptions{StoreName: "records", Consume: consume, Next: func(context.Context) (*MutateRequest, error) { return nil, failure }}, want: "request 1: application failure"},
		{name: "invalid request", options: MutateStreamOptions{StoreName: "records", Consume: consume, Next: func(context.Context) (*MutateRequest, error) { return &MutateRequest{}, nil }}, want: "request 1"},
		{name: "consumer error", options: MutateStreamOptions{StoreName: "records", Next: one(), Consume: func(context.Context, uint64, *MutationResult) error { return failure }}, want: "application failure"},
	}
	for _, test := range tests {
		err := client.MutateStream(t.Context(), test.options)
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("%s: err=%v, want %q", test.name, err, test.want)
		}
	}
}

func TestClientStreamEndsWithItsCallerContext(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	held := openHeldStream(ctx, client)
	held.input <- "records/s:held"
	for peer.held.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-held.done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceling did not end the stream")
	}
}

func TestClientStreamReleasesItsGroupAndKeepsSharedStreamsApart(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	laneTestReady(t, client, peer)
	held := openHeldStream(t.Context(), client)
	held.input <- "records/s:a"
	held.confirmed(t, 1)
	owned := func() int {
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
	if owned() != 1 {
		t.Fatal("a stream has no group of its own")
	}
	if err := held.finish(t); err != nil {
		t.Fatal(err)
	}
	if owned() != 0 {
		t.Fatal("a finished stream kept its group")
	}
	waitForHalfClose(t, peer, 1)
}

func TestClientStreamSendsOutgoingMetadata(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	ctx := metadata.AppendToOutgoingContext(t.Context(), "x-request", "seven")
	held := openHeldStream(ctx, client)
	held.input <- "records/s:a"
	held.confirmed(t, 1)
	incoming, _ := peer.outgoing.Load().(metadata.MD)
	if values := incoming.Get("x-request"); len(values) != 1 || values[0] != "seven" {
		t.Fatal("the stream did not send its caller's metadata", incoming)
	}
	if err := held.finish(t); err != nil {
		t.Fatal(err)
	}
}

func TestClientStreamSendsALargeRecordAlone(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	before := laneTestReady(t, client, peer)[0]
	large := strings.Repeat("x", laneRecordBytes+1)
	sent := 0
	options := MutateStreamOptions{StoreName: "records"}
	options.Next = func(context.Context) (*MutateRequest, error) {
		if sent == 2 {
			return nil, io.EOF
		}
		sent++
		return putRequest(fmt.Sprintf("records/s:%d", sent), `{"padding":"`+large+`"}`), nil
	}
	results := 0
	options.Consume = func(_ context.Context, _ uint64, result *MutationResult) error {
		if result.GetOutcome() != MutationApplied {
			t.Error(result)
		}
		results++
		return nil
	}
	if err := client.MutateStream(t.Context(), options); err != nil || results != 2 {
		t.Fatal(err, results)
	}
	if opened := peer.streams.Load() - before; opened != 2 {
		t.Fatal("two large records opened", opened, "streams")
	}
}
