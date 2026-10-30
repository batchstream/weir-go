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
// result waits for release or cancellation, and ends the stream on the broken
// resource.
type laneTestPeer struct {
	streams   atomic.Int64
	halfClose atomic.Int64
	timeouts  chan uint64
	arrived   chan string
	canceled  chan uint64
	release   chan struct{}
}

func newLaneTestPeer() *laneTestPeer {
	peer := &laneTestPeer{timeouts: make(chan uint64, 64), arrived: make(chan string, 64), canceled: make(chan uint64, 64), release: make(chan struct{})}
	return peer
}

func (p *laneTestPeer) Execute(stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse]) error {
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
		switch resource {
		case "records/s:broken":
			return status.Error(codes.Unavailable, "stream broke")
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
	var calls sync.WaitGroup
	for worker := range 128 {
		calls.Go(func() {
			for range 10 {
				if _, err := lanePut(t.Context(), client, fmt.Sprintf("records/s:%d", worker), 16); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	calls.Wait()
	if first.streams.Load() == 0 || second.streams.Load() == 0 {
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
	if _, err := lanePut(ctx, client, "records/s:bounded", 16); err != nil {
		t.Fatal(err)
	}
	if timeout := <-peer.timeouts; timeout == 0 || timeout > uint64(time.Second/time.Microsecond) {
		t.Fatal("record did not carry its caller's remaining time", timeout)
	}
	if _, err := lanePut(t.Context(), client, "records/s:unbounded", 16); err != nil {
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
		if _, err := lanePut(t.Context(), client, "records/s:sized", size); err != nil {
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
	if _, err := lanePut(t.Context(), client, "records/s:broken", 16); status.Code(err) != codes.Unavailable {
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
	if _, err := lanePut(t.Context(), client, "records/s:first", 16); err != nil {
		t.Fatal(err)
	}
	client.lanes.mu.Lock()
	for _, group := range client.lanes.groups {
		for _, open := range group.lanes {
			open.expires = time.Now().Add(-time.Second)
		}
	}
	client.lanes.mu.Unlock()
	if _, err := lanePut(t.Context(), client, "records/s:second", 16); err != nil {
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
