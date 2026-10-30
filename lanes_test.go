package weir

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
)

// laneTestPeer applies every mutation at once except the held resource, whose
// result waits for release or cancellation. It ends the stream on the broken
// resource, stops reading it on the stalled one and refuses the refused one as
// a draining server does.
type laneTestPeer struct {
	streams   atomic.Int64
	halfClose atomic.Int64
	timeouts  chan uint64
	arrived   chan string
	canceled  chan uint64
	release   chan struct{}
	stalled   chan struct{}
	stall     sync.Once
	// draining refuses every record, as a draining server does.
	draining atomic.Bool
	// rejecting refuses new streams, as a draining server does before its
	// clients learn to stop opening them.
	rejecting atomic.Bool
}

func newLaneTestPeer() *laneTestPeer {
	peer := &laneTestPeer{timeouts: make(chan uint64, 64), arrived: make(chan string, 64), canceled: make(chan uint64, 64), release: make(chan struct{}), stalled: make(chan struct{})}
	return peer
}

func (p *laneTestPeer) Execute(stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse]) error {
	if p.rejecting.Load() {
		return status.Error(codes.Unavailable, "draining")
	}
	p.streams.Add(1)
	var send sync.Mutex
	// Held results finish before a clean half-close; an error ends them.
	var held sync.WaitGroup
	var mu sync.Mutex
	cancels := make(map[uint64]chan struct{})
	applied := protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
	respond := func(index uint64, result *pb.MutationResult) {
		value := &pb.Event_MutationResult{MutationResult: result}
		event := &pb.Event{Value: value}
		response := &pb.ExecuteResponse{Index: index, Event: event}
		send.Lock()
		defer send.Unlock()
		_ = stream.Send(response)
	}
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			held.Wait()
			p.halfClose.Add(1)
			return nil
		}
		if err != nil {
			return err
		}
		if err := protocol.ValidateExecuteRequest(request); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		if request.CancelIndex != 0 {
			p.canceled <- request.CancelIndex
			mu.Lock()
			if canceled := cancels[request.CancelIndex]; canceled != nil {
				close(canceled)
				delete(cancels, request.CancelIndex)
			}
			mu.Unlock()
			continue
		}
		resource := request.Command.GetMutate().GetResource()
		select {
		case p.timeouts <- request.TimeoutMicros:
		default:
		}
		select {
		case p.arrived <- resource:
		default:
		}
		if p.draining.Load() {
			resource = "records/s:refused"
		}
		switch resource {
		case "records/s:broken":
			return status.Error(codes.Unavailable, "stream broke")
		case "records/s:stalled":
			p.stall.Do(func() { close(p.stalled) })
			<-stream.Context().Done()
			return stream.Context().Err()
		case "records/s:refused":
			failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, "store draining")
			respond(request.Index, protocol.Mutation(pb.MutationOutcome_NOT_STARTED, failure))
		case "records/s:held":
			index := request.Index
			canceled := make(chan struct{})
			mu.Lock()
			cancels[index] = canceled
			mu.Unlock()
			held.Go(func() {
				result := applied
				select {
				case <-p.release:
				case <-canceled:
					failure := protocol.Fail(pb.FailureCode_CANCELLED, "record canceled")
					result = protocol.Mutation(pb.MutationOutcome_NOT_STARTED, failure)
				case <-stream.Context().Done():
					return
				}
				respond(index, result)
			})
		default:
			respond(request.Index, applied)
		}
	}
}

func laneTestClient(t *testing.T, peers ...*laneTestPeer) *Client {
	t.Helper()
	builder := manual.NewBuilderWithScheme("weirlanes")
	addresses := make([]resolver.Address, len(peers))
	for i, peer := range peers {
		address := laneTestListen(t, peer)
		addresses[i] = resolver.Address{Addr: address}
	}
	state := resolver.State{Addresses: addresses}
	builder.InitialState(state)
	options := append(connectionOptions(), grpc.WithResolvers(builder))
	connection, err := grpc.NewClient("weirlanes:///records", options...)
	if err != nil {
		t.Fatal(err)
	}
	store := pb.NewStoreServiceClient(connection)
	client := &Client{connection: connection, store: store, lanes: newLanePool(store)}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func laneTestListen(t *testing.T, peer *laneTestPeer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(protocol.MaxTransportMessageBytes), grpc.MaxSendMsgSize(protocol.MaxTransportMessageBytes))
	pb.RegisterStoreServiceServer(server, peer)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-done })
	return listener.Addr().String()
}

// laneTestReady waits until calls on streams of their own reach every peer,
// so shared streams opened afterwards can use every connection. It returns
// each peer's stream count at that point.
func laneTestReady(t *testing.T, client *Client, peers ...*laneTestPeer) []int64 {
	t.Helper()
	document := &Document{ContentType: "application/json", Data: []byte(`{}`)}
	request := &WriteRequest{Resource: "records/s:ready", Document: document}
	options := WriteOptions{StoreName: "records", Request: request}
	for range 200 {
		if _, err := Put(t.Context(), client.store, options); err != nil {
			t.Fatal(err)
		}
		counts := make([]int64, len(peers))
		ready := true
		for i, peer := range peers {
			counts[i] = peer.streams.Load()
			ready = ready && counts[i] != 0
		}
		if ready {
			return counts
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("not every server became reachable")
	return nil
}

func lanePut(ctx context.Context, client *Client, resource string, size int) (*MutationResult, error) {
	document := &Document{ContentType: "application/json", Data: []byte(`{"padding":"` + strings.Repeat("x", size) + `"}`)}
	request := &WriteRequest{Resource: resource, Document: document}
	options := WriteOptions{StoreName: "records", Request: request}
	return client.Put(ctx, options)
}

func TestConcurrentSingleRecordCallsShareABoundedSetOfStreams(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	var calls sync.WaitGroup
	var failed atomic.Int64
	for worker := range 256 {
		calls.Go(func() {
			for range 20 {
				result, err := lanePut(t.Context(), client, fmt.Sprintf("records/s:%d", worker), 16)
				if err != nil || result.GetOutcome() != MutationApplied {
					failed.Add(1)
				}
			}
		})
	}
	calls.Wait()
	if failed.Load() != 0 {
		t.Fatal("shared calls failed", failed.Load())
	}
	if streams := peer.streams.Load(); streams < 2 || streams > maxLanes {
		t.Fatal("5120 calls did not share a bounded set of streams", streams)
	}
}

func TestSharedStreamsSpreadConcurrentCallsAcrossServers(t *testing.T) {
	first, second := newLaneTestPeer(), newLaneTestPeer()
	client := laneTestClient(t, first, second)
	before := laneTestReady(t, client, first, second)
	var calls sync.WaitGroup
	for worker := range 128 {
		calls.Go(func() {
			for range 10 {
				_, err := lanePut(t.Context(), client, fmt.Sprintf("records/s:%d", worker), 16)
				if err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	calls.Wait()
	if first.streams.Load() == before[0] || second.streams.Load() == before[1] {
		t.Fatal("shared streams stayed on one server", first.streams.Load(), second.streams.Load())
	}
}

func TestCanceledSharedCallCancelsItsRecordWithoutDisturbingTheStream(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	ctx, cancel := context.WithCancel(t.Context())
	held := make(chan error, 1)
	go func() {
		_, err := lanePut(ctx, client, "records/s:held", 16)
		held <- err
	}()
	if resource := <-peer.arrived; resource != "records/s:held" {
		t.Fatal("unexpected first record", resource)
	}
	cancel()
	if err := <-held; !errors.Is(err, context.Canceled) {
		t.Fatal("canceled call kept waiting", err)
	}
	if index := <-peer.canceled; index != 1 {
		t.Fatal("canceled call did not cancel its record", index)
	}
	result, err := lanePut(t.Context(), client, "records/s:other", 16)
	if err != nil || result.GetOutcome() != MutationApplied || peer.streams.Load() != 1 {
		t.Fatal("canceled call disturbed its shared stream", result, err, peer.streams.Load())
	}
	// The canceled record's own result frees its stream slot.
	deadline := time.Now().Add(5 * time.Second)
	for laneTestLoad(client) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("canceled record kept its stream slot", laneTestLoad(client))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func laneTestLoad(client *Client) int64 {
	client.lanes.mu.Lock()
	defer client.lanes.mu.Unlock()
	var load int64
	for _, group := range client.lanes.groups {
		for _, open := range group.lanes {
			load += open.load.Load()
		}
	}
	return load
}

func TestSharedCallsCarryTheirDeadline(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := lanePut(ctx, client, "records/s:bounded", 16)
	if err != nil {
		t.Fatal(err)
	}
	if timeout := <-peer.timeouts; timeout == 0 || timeout > uint64(time.Second/time.Microsecond) {
		t.Fatal("record did not carry its caller's remaining time", timeout)
	}
	_, err = lanePut(t.Context(), client, "records/s:unbounded", 16)
	if err != nil {
		t.Fatal(err)
	}
	if timeout := <-peer.timeouts; timeout != 0 {
		t.Fatal("unbounded caller received a record timeout", timeout)
	}
}

func TestLargeRecordsUseAStreamOfTheirOwn(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	for _, size := range []int{16, laneRecordBytes, 16} {
		_, err := lanePut(t.Context(), client, "records/s:sized", size)
		if err != nil {
			t.Fatal(err)
		}
	}
	if peer.streams.Load() != 2 {
		t.Fatal("large record shared a stream or small ones did not", peer.streams.Load())
	}
}

func TestBrokenSharedStreamFailsItsRecordsAndTheNextCallReconnects(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	held := make(chan error, 1)
	go func() {
		_, err := lanePut(t.Context(), client, "records/s:held", 16)
		held <- err
	}()
	<-peer.arrived
	_, err := lanePut(t.Context(), client, "records/s:broken", 16)
	if status.Code(err) != codes.Unavailable {
		t.Fatal("broken stream did not fail its record", err)
	}
	if err := <-held; status.Code(err) != codes.Unavailable {
		t.Fatal("outstanding record survived its broken stream", err)
	}
	result, err := lanePut(t.Context(), client, "records/s:after", 16)
	if err != nil || result.GetOutcome() != MutationApplied || peer.streams.Load() != 2 {
		t.Fatal("next call did not open a new stream", result, err, peer.streams.Load())
	}
}

func TestExpiredSharedStreamDrainsAndHalfCloses(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	_, err := lanePut(t.Context(), client, "records/s:first", 16)
	if err != nil {
		t.Fatal(err)
	}
	client.lanes.mu.Lock()
	for _, group := range client.lanes.groups {
		for _, open := range group.lanes {
			open.expires = time.Now().Add(-time.Second)
		}
	}
	client.lanes.mu.Unlock()
	_, err = lanePut(t.Context(), client, "records/s:second", 16)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for peer.halfClose.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("expired stream was not half-closed", peer.streams.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if peer.streams.Load() != 2 {
		t.Fatal("expired stream was not replaced", peer.streams.Load())
	}
}

func TestCloseEndsSharedCalls(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	held := make(chan error, 1)
	go func() {
		_, err := lanePut(t.Context(), client, "records/s:held", 16)
		held <- err
	}()
	<-peer.arrived
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-held; status.Code(err) != codes.Canceled {
		t.Fatal("Close left a shared call waiting", err)
	}
}

func TestStalledSharedStreamReleasesCallersAtTheirDeadline(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	stalled, stop := context.WithTimeout(t.Context(), 100*time.Millisecond)
	_, err := lanePut(stalled, client, "records/s:stalled", 16)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("stalled record outlived its deadline", err)
	}
	// A large record fills the flow-control window of the unread stream.
	var calls sync.WaitGroup
	late := make(chan time.Duration, 8)
	for _, size := range []int{900 << 10, 16, 16, 16, 16} {
		calls.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			started := time.Now()
			_, err := lanePut(ctx, client, "records/s:behind", size)
			if elapsed := time.Since(started); elapsed > time.Second || !errors.Is(err, context.DeadlineExceeded) {
				late <- elapsed
			}
		})
	}
	calls.Wait()
	close(late)
	for elapsed := range late {
		t.Fatal("a call behind a stalled stream outlived its deadline", elapsed)
	}
}

func TestRefusingStreamKeepsCarryingCallsWhenNoOtherCan(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	peer.draining.Store(true)
	// The first refusal opens one fresh stream; once it refuses too, both
	// stay open rather than each call opening a stream of its own.
	for range 20 {
		refused, err := lanePut(t.Context(), client, "records/s:refused", 16)
		if err != nil || refused.GetOutcome() != MutationNotStarted || refused.GetFailure().GetCode() != FailureUnavailable {
			t.Fatal("refused record lost its evidence", refused, err)
		}
	}
	if peer.streams.Load() != 2 || peer.halfClose.Load() != 0 {
		t.Fatal("refusals churned streams", peer.streams.Load(), peer.halfClose.Load())
	}
}

func TestLoneStreamMovesOffADrainingServerAtOnce(t *testing.T) {
	first, second := newLaneTestPeer(), newLaneTestPeer()
	client := laneTestClient(t, first, second)
	before := laneTestReady(t, client, first, second)
	if _, err := lanePut(t.Context(), client, "records/s:first", 16); err != nil {
		t.Fatal(err)
	}
	draining := first
	if second.streams.Load() != before[1] {
		draining = second
	}
	draining.draining.Store(true)
	refused := 0
	for range 20 {
		result, err := lanePut(t.Context(), client, "records/s:next", 16)
		if err != nil {
			t.Fatal(err)
		}
		if result.GetOutcome() != MutationApplied {
			refused++
		}
	}
	// Only the record that revealed the drain is refused.
	if refused != 1 {
		t.Fatal("calls stayed on the draining server", refused)
	}
	deadline := time.Now().Add(2 * time.Second)
	for draining.halfClose.Load() != draining.streams.Load() {
		if time.Now().After(deadline) {
			t.Fatal("stream to the draining server stayed open", draining.halfClose.Load(), draining.streams.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRejectedProbeDoesNotHoldCallsOnADrainingServer(t *testing.T) {
	first, second := newLaneTestPeer(), newLaneTestPeer()
	client := laneTestClient(t, first, second)
	before := laneTestReady(t, client, first, second)
	if _, err := lanePut(t.Context(), client, "records/s:first", 16); err != nil {
		t.Fatal(err)
	}
	draining, healthy := first, second
	if second.streams.Load() != before[1] {
		draining, healthy = second, first
	}
	draining.draining.Store(true)
	draining.rejecting.Store(true)
	// A stream of its own moves round-robin so the first probe reaches the
	// draining server.
	document := &Document{ContentType: "application/json", Data: []byte(`{}`)}
	request := &WriteRequest{Resource: "records/s:turn", Document: document}
	options := WriteOptions{StoreName: "records", Request: request}
	turned := healthy.streams.Load()
	if _, err := Put(t.Context(), client.store, options); err != nil || healthy.streams.Load() != turned+1 {
		t.Fatal("round-robin did not turn to the healthy server", err)
	}
	applied, refused, failed := 0, 0, 0
	for range 20 {
		result, err := lanePut(t.Context(), client, "records/s:next", 16)
		switch {
		case err != nil:
			failed++
		case result.GetOutcome() == MutationApplied:
			applied++
		default:
			refused++
		}
	}
	// One record reveals the drain and at most one rides the rejected probe.
	if refused != 1 || failed > 1 || applied < 18 {
		t.Fatal("a rejected probe held calls on the draining server", applied, refused, failed)
	}
}

func TestOnlyServerRefusalsCoolAStream(t *testing.T) {
	mutation := func(outcome pb.MutationOutcome, code pb.FailureCode) *pb.Event {
		value := &pb.Event_MutationResult{MutationResult: protocol.Mutation(outcome, protocol.Fail(code, "refused"))}
		event := &pb.Event{Value: value}
		return event
	}
	read := func(code pb.FailureCode) *pb.Event {
		value := &pb.Event_ReadResult{ReadResult: protocol.ReadFailure(protocol.Fail(code, "refused"))}
		event := &pb.Event{Value: value}
		return event
	}
	for name, want := range map[string]struct {
		event   *pb.Event
		refuses bool
	}{
		"draining mutation":   {mutation(pb.MutationOutcome_NOT_STARTED, pb.FailureCode_UNAVAILABLE), true},
		"overloaded mutation": {mutation(pb.MutationOutcome_NOT_STARTED, pb.FailureCode_RESOURCE_EXHAUSTED), true},
		"uncertain mutation":  {mutation(pb.MutationOutcome_UNKNOWN, pb.FailureCode_UNAVAILABLE), false},
		"unavailable read":    {read(pb.FailureCode_UNAVAILABLE), true},
		"oversized read":      {read(pb.FailureCode_RESOURCE_EXHAUSTED), false},
	} {
		if refusesWork(want.event) != want.refuses {
			t.Error(name, "classified wrongly")
		}
	}
}

func TestSharedAndOwnStreamCallsReportTheirDeadlineAlike(t *testing.T) {
	peer := newLaneTestPeer()
	client := laneTestClient(t, peer)
	defer close(peer.release)
	// The large record uses a stream of its own.
	for _, size := range []int{16, laneRecordBytes} {
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		_, err := lanePut(ctx, client, "records/s:held", size)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("deadline reported differently by stream path", size, err)
		}
	}
}

func TestNewRecordsLeaveADrainingServer(t *testing.T) {
	draining, healthy := newLaneTestPeer(), newLaneTestPeer()
	client := laneTestClient(t, draining, healthy)
	run := func(workers, calls int) (applied, refused int64) {
		var running sync.WaitGroup
		var appliedCount, refusedCount atomic.Int64
		for worker := range workers {
			running.Go(func() {
				for range calls {
					result, err := lanePut(t.Context(), client, fmt.Sprintf("records/s:%d", worker), 16)
					switch {
					case err != nil:
						t.Error(err)
						return
					case result.GetOutcome() == MutationApplied:
						appliedCount.Add(1)
					default:
						refusedCount.Add(1)
					}
				}
			})
		}
		running.Wait()
		return appliedCount.Load(), refusedCount.Load()
	}
	before := laneTestReady(t, client, draining, healthy)
	run(64, 10)
	if draining.streams.Load() == before[0] || healthy.streams.Load() == before[1] {
		t.Fatal("shared streams did not reach both servers", draining.streams.Load(), healthy.streams.Load())
	}
	// Few enough callers that the open streams have room for every record.
	draining.draining.Store(true)
	if applied, refused := run(8, 20); applied+refused != 8*20 {
		t.Fatal("records were lost or resent", applied, refused)
	}
	if applied, refused := run(8, 20); refused != 0 || applied != 8*20 {
		t.Fatal("new records still reached the draining server", applied, refused)
	}
	deadline := time.Now().Add(2 * time.Second)
	for draining.halfClose.Load() != draining.streams.Load() {
		if time.Now().After(deadline) {
			t.Fatal("streams to the draining server stayed open", draining.halfClose.Load(), draining.streams.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
