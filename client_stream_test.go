package weir

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
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
	expireOwnStreams(client)
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

// expireOwnStreams makes every record stream's gRPC stream retire at its next record.
func expireOwnStreams(client *Client) {
	pool := client.lanes
	pool.mu.Lock()
	defer pool.mu.Unlock()
	for key, group := range pool.groups {
		if key.owner == 0 {
			continue
		}
		for _, stream := range group.lanes {
			stream.expires = time.Now().Add(-time.Second)
		}
	}
}

func TestRefusedStreamNeedsTheServersDetail(t *testing.T) {
	if failure := refusedStream(refusedStreamError("draining")); failure.GetCode() != FailureUnavailable || failure.GetMessage() != "draining" {
		t.Fatal(failure)
	}
	for _, err := range []error{nil, errors.New("local"), status.Error(codes.Unavailable, "draining"), status.Error(codes.Internal, "other")} {
		if failure := refusedStream(err); failure != nil {
			t.Fatal(err, failure)
		}
	}
}

func TestClientStreamMovesOnWhenAServerRefusesItsNewStream(t *testing.T) {
	first, second := newLaneTestPeer(), newLaneTestPeer()
	client, _ := laneTestResolved(t, first, second)
	client.resends = newResendBudget()
	before := laneTestReady(t, client, first, second)
	held := openHeldStream(t.Context(), client)
	held.input <- "records/s:a"
	held.confirmed(t, 1)
	_, other, _ := laneTestHolder(t, before, first, second)
	other.rejecting.Store(true)
	const rotations = 32
	for rotation := range rotations {
		expireOwnStreams(client)
		held.input <- "records/s:next"
		if refused := held.confirmed(t, rotation+2); refused != 0 {
			t.Fatal("a refused stream's record reached the consumer", refused)
		}
	}
	if err := held.finish(t); err != nil {
		t.Fatal(err)
	}
}

func TestLoneServerRefusingStreamsReportsItsRecordsNotStarted(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	client.resends = newResendBudget()
	peer.rejecting.Store(true)
	held := openHeldStream(t.Context(), client)
	held.input <- "records/s:a"
	if refused := held.confirmed(t, 1); refused != 1 {
		t.Fatal("the refusal did not reach the consumer", refused)
	}
	held.mu.Lock()
	result := held.results[1]
	held.mu.Unlock()
	if result.GetOutcome() != MutationNotStarted || result.GetFailure().GetCode() != FailureUnavailable {
		t.Fatal(result)
	}
	peer.rejecting.Store(false)
	held.input <- "records/s:b"
	held.confirmed(t, 2)
	if err := held.finish(t); err != nil {
		t.Fatal(err)
	}
	peer.rejecting.Store(true)
	put, err := lanePut(t.Context(), client, "records/s:c", 16)
	if err != nil || put.GetOutcome() != MutationNotStarted {
		t.Fatal(put, err)
	}
	large, err := lanePut(t.Context(), client, "records/s:large", laneRecordBytes)
	if err != nil || large.GetOutcome() != MutationNotStarted {
		t.Fatal("a refused stream of a large record's own", large, err)
	}
}

// endsUnmarked ends each stream with UNAVAILABLE alone, as a lost connection does.
type endsUnmarked struct{}

func (endsUnmarked) Execute(grpc.BidiStreamingServer[pb.ExecuteRequests, pb.ExecuteResponses]) error {
	return status.Error(codes.Unavailable, "connection lost")
}

func TestUnavailableWithoutTheDetailStaysAnError(t *testing.T) {
	client, err := New(serveForTest(t, endsUnmarked{}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	result, err := lanePut(t.Context(), client, "records/s:a", 16)
	if status.Code(err) != codes.Unavailable || result != nil {
		t.Fatal(result, err)
	}
	large, err := lanePut(t.Context(), client, "records/s:large", laneRecordBytes)
	if status.Code(err) != codes.Unavailable || large != nil {
		t.Fatal(large, err)
	}
}

// answerThenBreak answers records without one of them and ends its stream.
type answerThenBreak struct {
	want    int
	dropped int
}

func (p *answerThenBreak) Execute(packed grpc.BidiStreamingServer[pb.ExecuteRequests, pb.ExecuteResponses]) error {
	var indexes []uint64
	for len(indexes) < p.want {
		message, err := packed.Recv()
		if err != nil {
			return err
		}
		for _, request := range message.Requests {
			if request.CancelIndex == 0 {
				indexes = append(indexes, request.Index)
			}
		}
	}
	applied := protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
	response := &pb.ExecuteResponses{}
	for _, index := range indexes[:len(indexes)-p.dropped] {
		value := &pb.Event_MutationResult{MutationResult: applied}
		event := &pb.Event{Value: value}
		answer := &pb.ExecuteResponse{Index: index, Event: event}
		response.Responses = append(response.Responses, answer)
	}
	if err := packed.Send(response); err != nil {
		return err
	}
	return status.Error(codes.Unavailable, "server stopped")
}

func serveForTest(t *testing.T, peer pb.StoreServiceServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterStoreServiceServer(server, peer)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

func TestClientMutateKeepsResultsReceivedBeforeTheStreamBroke(t *testing.T) {
	const count = 16
	peer := &answerThenBreak{want: count, dropped: 1}
	client, err := New(serveForTest(t, peer))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	options := MutateOptions{StoreName: "records"}
	for index := range count {
		options.Requests = append(options.Requests, putRequest(fmt.Sprintf("records/s:%d", index), `{}`))
	}
	for range 50 {
		results, err := client.Mutate(t.Context(), options)
		if status.Code(err) != codes.Unavailable {
			t.Fatal(err)
		}
		kept := 0
		for _, result := range results {
			if result != nil {
				kept++
			}
		}
		if kept != count-1 {
			t.Fatal("results the server confirmed were lost", kept)
		}
	}
}

func TestClientStreamConsumesAResultWaitingWhenTheStreamBreaks(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	input := make(chan string)
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	var mu sync.Mutex
	seen := make(map[uint64]bool)
	options := MutateStreamOptions{StoreName: "records"}
	options.Next = func(ctx context.Context) (*MutateRequest, error) {
		select {
		case resource := <-input:
			return putRequest(resource, `{}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	options.Consume = func(ctx context.Context, index uint64, _ *MutationResult) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		mu.Lock()
		seen[index] = true
		mu.Unlock()
		entered <- struct{}{}
		<-release
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- client.MutateStream(t.Context(), options) }()
	input <- "records/s:one"
	<-entered
	input <- "records/s:two"
	time.Sleep(50 * time.Millisecond)
	input <- "records/s:broken"
	time.Sleep(50 * time.Millisecond)
	close(release)
	if err := <-done; status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	if !seen[1] || !seen[2] {
		t.Fatal("a result received before the break did not reach Consume", seen)
	}
}

func TestClientStreamStopsConsumingOnceConsumeFails(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	failure := errors.New("application failure")
	sent := 0
	calls := 0
	options := MutateStreamOptions{StoreName: "records"}
	options.Next = func(context.Context) (*MutateRequest, error) {
		if sent == recordWindowItems {
			return nil, io.EOF
		}
		sent++
		return putRequest(fmt.Sprintf("records/s:%d", sent), `{}`), nil
	}
	options.Consume = func(context.Context, uint64, *MutationResult) error {
		calls++
		return failure
	}
	if err := client.MutateStream(t.Context(), options); !errors.Is(err, failure) || calls != 1 {
		t.Fatal(err, calls)
	}
}

// answerThenFail confirms each record and ends the stream with an error.
type answerThenFail struct{}

func (answerThenFail) Execute(packed grpc.BidiStreamingServer[pb.ExecuteRequests, pb.ExecuteResponses]) error {
	message, err := packed.Recv()
	if err != nil {
		return err
	}
	applied := protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
	value := &pb.Event_MutationResult{MutationResult: applied}
	event := &pb.Event{Value: value}
	answer := &pb.ExecuteResponse{Index: message.Requests[0].Index, Event: event}
	response := &pb.ExecuteResponses{Responses: []*pb.ExecuteResponse{answer}}
	if err := packed.Send(response); err != nil {
		return err
	}
	return status.Error(codes.Internal, "final status")
}

func TestLargeRecordKeepsItsResultWhenItsStreamEndsWithAnError(t *testing.T) {
	client, err := New(serveForTest(t, answerThenFail{}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	large := putRequest("records/s:large", `{"padding":"`+strings.Repeat("x", laneRecordBytes+1)+`"}`)
	options := MutateOptions{StoreName: "records", Requests: []*MutateRequest{large}}
	results, err := client.Mutate(t.Context(), options)
	if status.Code(err) != codes.Internal || results[0].GetOutcome() != MutationApplied {
		t.Fatal(results, err)
	}
}

// Under the race detector this shows that a canceled call stops reading its
// request before returning, as a caller may then reuse its bytes.
func TestCanceledMutateReturnsAfterItsWriterStoppedReading(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	buffer := []byte(`{"padding":"` + strings.Repeat("x", 256<<10) + `"}`)
	document := &Document{ContentType: "application/json", Data: buffer}
	request := &MutateRequest{Resource: "records/s:reuse", Action: MutationPut, Document: document}
	options := MutateOptions{StoreName: "records", Requests: []*MutateRequest{request}}
	deadline := time.Now().Add(time.Second)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		ctx, cancel := context.WithCancel(t.Context())
		timer := time.AfterFunc(time.Duration(rand.N(60))*time.Microsecond, cancel)
		_, _ = client.Mutate(ctx, options)
		timer.Stop()
		cancel()
		for index := 12; index < len(buffer)-2; index += 4096 {
			buffer[index] = byte('a' + attempt%26)
		}
	}
}

// countsMessages answers every record at once and records each message's size.
type countsMessages struct {
	mu       sync.Mutex
	messages []int
}

func (p *countsMessages) Execute(packed grpc.BidiStreamingServer[pb.ExecuteRequests, pb.ExecuteResponses]) error {
	for {
		message, err := packed.Recv()
		if err != nil {
			return nil
		}
		p.mu.Lock()
		p.messages = append(p.messages, len(message.Requests))
		p.mu.Unlock()
		response := &pb.ExecuteResponses{}
		for _, request := range message.Requests {
			event := &pb.Event{}
			if request.GetCommand().GetRead() != nil {
				document := &pb.Document{ContentType: "application/json", Data: []byte(`{}`)}
				event.Value = &pb.Event_ReadResult{ReadResult: protocol.ReadDocument(document)}
			} else {
				applied := protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
				event.Value = &pb.Event_MutationResult{MutationResult: applied}
			}
			answer := &pb.ExecuteResponse{Index: request.Index, Event: event}
			response.Responses = append(response.Responses, answer)
		}
		if err := packed.Send(response); err != nil {
			return err
		}
	}
}

func (p *countsMessages) take() []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	taken := p.messages
	p.messages = nil
	return taken
}

func TestClientBatchLeavesInOneMessage(t *testing.T) {
	peer := &countsMessages{}
	client, err := New(serveForTest(t, peer))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	mutations := MutateOptions{StoreName: "records"}
	reads := ReadOptions{StoreName: "records"}
	for index := range recordWindowItems {
		resource := fmt.Sprintf("records/s:%d", index)
		mutations.Requests = append(mutations.Requests, putRequest(resource, `{}`))
		reads.Requests = append(reads.Requests, &ReadRequest{Resource: resource})
	}
	for range 200 {
		if _, err := client.Mutate(t.Context(), mutations); err != nil {
			t.Fatal(err)
		}
		if messages := peer.take(); len(messages) != 1 || messages[0] != recordWindowItems {
			t.Fatal("a batch of mutations was split", messages)
		}
		if _, err := client.Read(t.Context(), reads); err != nil {
			t.Fatal(err)
		}
		if messages := peer.take(); len(messages) != 1 || messages[0] != recordWindowItems {
			t.Fatal("a batch of reads was split", messages)
		}
	}
}

func TestClientBatchBeyondItsWindowSendsTheRestAsResultsArrive(t *testing.T) {
	peer := &countsMessages{}
	client, err := New(serveForTest(t, peer))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	const count = 3*recordWindowItems + 1
	options := MutateOptions{StoreName: "records"}
	for index := range count {
		options.Requests = append(options.Requests, putRequest(fmt.Sprintf("records/s:%d", index), `{}`))
	}
	results, err := client.Mutate(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range results {
		if result.GetOutcome() != MutationApplied {
			t.Fatal(index, result)
		}
	}
	messages := peer.take()
	total := 0
	for _, size := range messages {
		total += size
	}
	if messages[0] != recordWindowItems || total != count {
		t.Fatal(messages)
	}
}

func TestClientBatchOfOneResourceKeepsItsOrder(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	const count = 8
	options := MutateOptions{StoreName: "records"}
	for range count {
		options.Requests = append(options.Requests, putRequest("records/s:same", `{}`))
	}
	results, err := client.Mutate(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range results {
		if result.GetOutcome() != MutationApplied {
			t.Fatal(index, result)
		}
	}
}

func TestCanceledBatchEndsWithItsRecordsUnsent(t *testing.T) {
	peer := &countsMessages{}
	client, err := New(serveForTest(t, peer))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	options := MutateOptions{StoreName: "records", Requests: []*MutateRequest{putRequest("records/s:a", `{}`)}}
	if _, err := client.Mutate(ctx, options); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestCloseEndsAStreamWaitingForItsProducer(t *testing.T) {
	client, err := New(serveForTest(t, &countsMessages{}))
	if err != nil {
		t.Fatal(err)
	}
	producing := make(chan struct{})
	options := MutateStreamOptions{StoreName: "records"}
	options.Next = func(ctx context.Context) (*MutateRequest, error) {
		close(producing)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	options.Consume = func(context.Context, uint64, *MutationResult) error { return nil }
	done := make(chan error, 1)
	go func() { done <- client.MutateStream(t.Context(), options) }()
	<-producing
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if status.Code(err) != codes.Canceled {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close left the stream running")
	}
}

func TestWaitReturnsAResultThatAlreadyArrived(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	stream := &lane{wake: make(chan struct{}, 1)}
	for range 200 {
		pending := &laneCall{done: make(chan laneOutcome, 1)}
		pending.done <- laneOutcome{event: &pb.Event{}}
		if outcome := stream.wait(ctx, pending); outcome.event == nil {
			t.Fatal("a result that already arrived was dropped", outcome)
		}
	}
}
