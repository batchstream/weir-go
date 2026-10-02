package weir

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type refreshControlCall struct {
	store   string
	phase   int
	release chan struct{}
	done    chan struct{}
	err     error
}

type refreshControlPeer struct {
	pb.UnimplementedStoreServiceServer
	business  *clientTestPeer
	mu        sync.Mutex
	phase     int
	automatic bool
	records   map[string]*pb.ResolveStoreResponse
	started   chan *refreshControlCall
}

func (p *refreshControlPeer) Execute(stream pb.StoreService_ExecuteServer) error {
	return p.business.Execute(stream)
}

func (p *refreshControlPeer) ResolveStore(ctx context.Context, request *pb.ResolveStoreRequest) (*pb.ResolveStoreResponse, error) {
	p.mu.Lock()
	phase, automatic := p.phase, p.automatic
	response := p.records[request.StoreName]
	p.mu.Unlock()
	call := &refreshControlCall{store: request.StoreName, phase: phase, release: make(chan struct{}), done: make(chan struct{})}
	defer close(call.done)
	select {
	case p.started <- call:
	case <-ctx.Done():
		call.err = ctx.Err()
		return nil, call.err
	}
	if !automatic {
		select {
		case <-call.release:
		case <-ctx.Done():
			call.err = ctx.Err()
			return nil, call.err
		}
	}
	if response == nil {
		call.err = status.Error(codes.Unavailable, "directory not learned")
		return nil, call.err
	}
	return response, nil
}

func (p *refreshControlPeer) configure(phase int, automatic bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.phase, p.automatic = phase, automatic
}

type refreshControlRun struct {
	timeout time.Duration
	done    chan struct{}
	err     error
}

type refreshControlFixture struct {
	client  *Client
	peer    *refreshControlPeer
	rounds  chan *refreshControlRun
	initial time.Time
}

// Fairness only depends on refresh admission and the previous attempt state.
// Prepare every real connection before starting the cache clock or any round;
// real ingress limits and continuous business traffic are tested by Weir.
func newRefreshControlFixture(t *testing.T, names []string) *refreshControlFixture {
	t.Helper()
	business := &clientTestPeer{mode: "normal"}
	peer := &refreshControlPeer{business: business, records: make(map[string]*pb.ResolveStoreResponse), started: make(chan *refreshControlCall, 64)}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterStoreServiceServer(server, peer)
	serverDone := make(chan struct{})
	go func() { _ = server.Serve(listener); close(serverDone) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-serverDone })
	address := listener.Addr().String()
	options := OpenOptions{Seed: address, Stores: names, ResolveTimeout: 8 * time.Second}
	lifetime, cancel := context.WithCancel(t.Context())
	client := &Client{options: options, stores: make(map[string]*storeChannel), ctx: lifetime, cancel: cancel, done: make(chan struct{})}
	ready, readyCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer readyCancel()
	peer.mu.Lock()
	for _, name := range names {
		channel, err := client.newChannel(ready, []string{address})
		if err != nil {
			client.closeChannels()
			cancel()
			peer.mu.Unlock()
			t.Fatal(err)
		}
		if err := waitReady(ready, channel.connection); err != nil {
			_ = channel.connection.Close()
			client.closeChannels()
			cancel()
			peer.mu.Unlock()
			t.Fatal(err)
		}
		peer.records[name] = discoveryRecord(name, address)
		entry := &storeChannel{channel: channel, endpoints: []string{address}, ttl: 30 * time.Second}
		client.stores[name] = entry
	}
	peer.mu.Unlock()
	initial := time.Now()
	for _, entry := range client.stores {
		entry.expires, entry.attempt = initial.Add(30*time.Second), initial
	}
	fixture := &refreshControlFixture{client: client, peer: peer, rounds: make(chan *refreshControlRun), initial: initial}
	// Drive the production round explicitly, with a caller deadline shorter
	// than its per-RPC budget. A deadline ends a held round before any retry can
	// turn it into a throughput-dependent number of admitted Stores.
	go func() {
		defer close(client.done)
		for {
			select {
			case <-lifetime.Done():
				return
			case run := <-fixture.rounds:
				bounded, boundedCancel := context.WithTimeout(lifetime, run.timeout)
				runner := &Client{options: client.options, stores: client.stores, ctx: bounded}
				runner.refreshStores()
				run.err = bounded.Err()
				boundedCancel()
				close(run.done)
			}
		}
	}()
	t.Cleanup(func() { _ = client.Close() })
	return fixture
}

func (f *refreshControlFixture) start(t *testing.T, phase int, timeout time.Duration) *refreshControlRun {
	t.Helper()
	f.peer.configure(phase, false)
	run := &refreshControlRun{timeout: timeout, done: make(chan struct{})}
	select {
	case f.rounds <- run:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh worker did not accept a round")
	}
	return run
}

func (f *refreshControlFixture) next(t *testing.T, phase int) *refreshControlCall {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case call := <-f.peer.started:
			if call.phase == phase {
				return call
			}
		case <-timer.C:
			t.Fatal("refresh did not reach the controlled ResolveStore")
		}
	}
}

func waitRefreshControlRun(t *testing.T, run *refreshControlRun) {
	t.Helper()
	select {
	case <-run.done:
	case <-time.After(10 * time.Second):
		t.Fatal("refresh round did not finish or join its workers")
	}
}

func (f *refreshControlFixture) attempted() map[string]bool {
	attempted := make(map[string]bool)
	for name, entry := range f.client.stores {
		entry.mu.RLock()
		attempted[name] = entry.attempt.After(f.initial)
		entry.mu.RUnlock()
	}
	return attempted
}

func TestRefreshPrioritizesUnrenewedStoresAfterRoundDeadline(t *testing.T) {
	names := make([]string, 16)
	for i := range names {
		names[i] = fmt.Sprintf("records-%02d", i)
	}
	fixture := newRefreshControlFixture(t, names)
	first := fixture.start(t, 1, 2*time.Second)
	firstCalls := []*refreshControlCall{fixture.next(t, 1), fixture.next(t, 1)}
	waitRefreshControlRun(t, first)
	if !errors.Is(first.err, context.DeadlineExceeded) {
		t.Fatalf("held round did not end at its deadline: %v", first.err)
	}
	attempted := fixture.attempted()
	count := 0
	for _, started := range attempted {
		if started {
			count++
		}
	}
	if count != len(firstCalls) || firstCalls[0].store == firstCalls[1].store || !attempted[firstCalls[0].store] || !attempted[firstCalls[1].store] {
		t.Fatalf("canceled waiting jobs advanced without admission: attempts=%v", attempted)
	}
	second := fixture.start(t, 2, 10*time.Second)
	secondCalls := []*refreshControlCall{fixture.next(t, 2), fixture.next(t, 2)}
	for _, call := range secondCalls {
		if attempted[call.store] {
			t.Fatalf("previously attempted Store %s preceded the unattempted tail", call.store)
		}
	}
	fixture.peer.configure(2, true)
	for _, call := range secondCalls {
		close(call.release)
	}
	waitRefreshControlRun(t, second)
	for name, entry := range fixture.client.stores {
		entry.mu.RLock()
		refreshed := entry.expires.After(fixture.initial.Add(30 * time.Second))
		entry.mu.RUnlock()
		if !refreshed {
			t.Fatalf("Store %s was not renewed after the truncated round", name)
		}
		discoveryRead(t, fixture.client, name)
	}
	third := fixture.start(t, 3, 10*time.Second)
	thirdCalls := []*refreshControlCall{fixture.next(t, 3), fixture.next(t, 3)}
	closed := make(chan error, 1)
	go func() { closed <- fixture.client.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not cancel the held refresh round")
	}
	select {
	case <-third.done:
	default:
		t.Fatal("Close returned before the active refresh round joined")
	}
	if !errors.Is(third.err, context.Canceled) {
		t.Fatalf("Close waited for the round deadline instead of canceling: %v", third.err)
	}
	for _, call := range thirdCalls {
		select {
		case <-call.done:
			if !errors.Is(call.err, context.Canceled) {
				t.Fatalf("Close did not cancel active ResolveStore: %v", call.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("ResolveStore handler did not observe Close cancellation")
		}
	}
}

func TestRefreshFailuresCannotStarveHealthyTail(t *testing.T) {
	abandoned := []string{"abandoned-a", "abandoned-b", "abandoned-c", "abandoned-d"}
	healthy := []string{"records-a", "records-b", "records-c", "records-d"}
	names := append(abandoned, healthy...)
	fixture := newRefreshControlFixture(t, names)
	fixture.peer.mu.Lock()
	for _, name := range abandoned {
		fixture.peer.records[name] = nil
		fixture.client.stores[name].expires = time.Time{}
	}
	fixture.peer.mu.Unlock()
	previous := make(map[string]bool)
	renewed := false
	for phase := 1; phase <= 3; phase++ {
		run := fixture.start(t, phase, 2*time.Second)
		calls := []*refreshControlCall{fixture.next(t, phase), fixture.next(t, phase)}
		for _, call := range calls {
			if previous[call.store] {
				t.Fatalf("failed Store did not yield to unattempted Stores: %s", call.store)
			}
			previous[call.store] = true
			for _, name := range healthy {
				if call.store == name {
					renewed = true
				}
			}
		}
		if renewed {
			fixture.peer.configure(phase, true)
			for _, call := range calls {
				close(call.release)
			}
		}
		waitRefreshControlRun(t, run)
		if renewed {
			if run.err != nil {
				t.Fatalf("released round did not finish: %v", run.err)
			}
			break
		}
		if !errors.Is(run.err, context.DeadlineExceeded) {
			t.Fatalf("unavailable sources did not truncate the round: %v", run.err)
		}
		attempted := fixture.attempted()
		for name := range previous {
			if !attempted[name] {
				t.Fatalf("failed Store %s did not advance its attempt", name)
			}
		}
	}
	if !renewed {
		t.Fatal("expired failing sources repeatedly preceded the healthy tail")
	}
	for _, name := range healthy {
		entry := fixture.client.stores[name]
		entry.mu.RLock()
		refreshed := entry.err == nil && entry.expires.After(fixture.initial.Add(30*time.Second))
		entry.mu.RUnlock()
		if !refreshed {
			t.Fatalf("healthy Store %s was not renewed despite admitted control RPC", name)
		}
		discoveryRead(t, fixture.client, name)
	}
}
