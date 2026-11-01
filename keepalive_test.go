package weir

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/resolver/manual"
)

// laneTestFreezer forwards connections to a server until frozen. A frozen
// connection reads and drops what the client sends and answers nothing, as a
// hung server or a broken network path would, so only the client can end it.
type laneTestFreezer struct {
	target string
	frozen atomic.Bool
	mu     sync.Mutex
	// closed is when the client ended a connection that was frozen.
	closed []time.Time
}

func laneTestFreeze(t *testing.T, target string) (*laneTestFreezer, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	freezer := &laneTestFreezer{target: target}
	var conns sync.WaitGroup
	t.Cleanup(func() { _ = listener.Close(); conns.Wait() })
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			conns.Go(func() { freezer.forward(t, client) })
		}
	}()
	return freezer, listener.Addr().String()
}

func (f *laneTestFreezer) forward(t *testing.T, client net.Conn) {
	defer client.Close()
	server, err := net.Dial("tcp", f.target)
	if err != nil {
		return
	}
	defer server.Close()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	go func() {
		buffer := make([]byte, 32<<10)
		for {
			n, err := server.Read(buffer)
			if err != nil {
				_ = client.Close()
				return
			}
			if !f.frozen.Load() {
				_, _ = client.Write(buffer[:n])
			}
		}
	}()
	buffer := make([]byte, 32<<10)
	for {
		n, err := client.Read(buffer)
		if err != nil {
			if f.frozen.Load() {
				f.mu.Lock()
				f.closed = append(f.closed, time.Now())
				f.mu.Unlock()
			}
			return
		}
		if !f.frozen.Load() {
			_, _ = server.Write(buffer[:n])
		}
	}
}

func (f *laneTestFreezer) firstClose() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.closed) == 0 {
		return time.Time{}
	}
	return f.closed[0]
}

// A server that resolves and whose connection looked ready, but that answers
// nothing, must not keep taking every other call: the Client's pings end the
// connection and calls move to the server that answers.
func TestCallsLeaveAServerThatStopsAnswering(t *testing.T) {
	healthy, hung := newLaneTestPeer(), newLaneTestPeer()
	servers := &laneTestServers{resolver: manual.NewBuilderWithScheme("weirlanes"), endpoints: &endpoints{}}
	servers.addresses = append(servers.addresses, laneTestListen(t, healthy))
	freezer, frozen := laneTestFreeze(t, laneTestListen(t, hung))
	servers.addresses = append(servers.addresses, frozen)
	client := laneTestDial(t, servers, nil)
	laneTestReady(t, client, healthy, hung)

	document := &Document{ContentType: "application/json", Data: []byte(`{}`)}
	write := func(timeout time.Duration) error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		first := &MutateRequest{Resource: "records/s:first", Action: MutationPut, Document: document}
		second := &MutateRequest{Resource: "records/s:second", Action: MutationPut, Document: document}
		options := MutateOptions{StoreName: "records", Requests: []*MutateRequest{first, second}}
		_, err := client.Mutate(ctx, options)
		return err
	}
	frozenAt := time.Now()
	freezer.frozen.Store(true)
	limit := keepaliveTime + keepaliveTimeout + 5*time.Second
	for freezer.firstClose().IsZero() {
		if time.Since(frozenAt) > limit {
			t.Fatalf("the Client kept its connection to a server that answered nothing for %v", limit)
		}
		_ = write(200 * time.Millisecond)
	}
	if waited := freezer.firstClose().Sub(frozenAt); waited < keepaliveTime {
		t.Fatalf("the connection ended %v after the server stopped answering, before any ping was due", waited)
	}
	for range 50 {
		if err := write(time.Second); err != nil {
			t.Fatalf("a call after the hung server's connection ended: %v", err)
		}
	}
}
