package weir

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-go/internal/testutil/testdns"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type discoveryPeer struct {
	pb.UnimplementedStoreServiceServer
	business   *clientTestPeer
	executions atomic.Int64
	resolves   atomic.Int64
	mu         sync.Mutex
	records    map[string]*pb.ResolveStoreResponse
	err        error
	delay      time.Duration
	delays     map[string]time.Duration
}

func (p *discoveryPeer) ResolveStore(ctx context.Context, request *pb.ResolveStoreRequest) (*pb.ResolveStoreResponse, error) {
	p.resolves.Add(1)
	p.mu.Lock()
	err, delay := p.err, p.delay
	if override, exists := p.delays[request.StoreName]; exists {
		delay = override
	}
	var response *pb.ResolveStoreResponse
	if record := p.records[request.StoreName]; record != nil {
		response = proto.Clone(record).(*pb.ResolveStoreResponse)
	}
	p.mu.Unlock()
	if delay != 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, status.Error(codes.Unavailable, "directory not learned")
	}
	return response, nil
}

func (p *discoveryPeer) Read(ctx context.Context, request *pb.ReadBatchRequest) (*pb.ReadBatchResponse, error) {
	p.executions.Add(1)
	if p.business == nil {
		return nil, status.Error(codes.FailedPrecondition, "initialization node received business traffic")
	}
	return p.business.Read(ctx, request)
}
func (p *discoveryPeer) Mutate(ctx context.Context, request *pb.MutateBatchRequest) (*pb.MutateBatchResponse, error) {
	p.executions.Add(1)
	if p.business == nil {
		return nil, status.Error(codes.FailedPrecondition, "initialization node received business traffic")
	}
	return p.business.Mutate(ctx, request)
}
func (p *discoveryPeer) Execute(request *pb.ExecuteRequest, stream pb.StoreService_ExecuteServer) error {
	p.executions.Add(1)
	if p.business == nil {
		return status.Error(codes.FailedPrecondition, "initialization node received business traffic")
	}
	return p.business.Execute(request, stream)
}

func (p *discoveryPeer) set(store string, response *pb.ResolveStoreResponse) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.records == nil {
		p.records = make(map[string]*pb.ResolveStoreResponse)
	}
	p.records[store] = response
}

type discoveryListener struct {
	address string
	stop    func()
}

func listenDiscovery(t *testing.T, peer *discoveryPeer, address string) *discoveryListener {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(10 << 20))
	pb.RegisterStoreServiceServer(server, peer)
	done := make(chan struct{})
	go func() { _ = server.Serve(listener); close(done) }()
	var once sync.Once
	fixture := &discoveryListener{address: listener.Addr().String()}
	fixture.stop = func() {
		once.Do(func() { server.Stop(); _ = listener.Close(); <-done })
	}
	t.Cleanup(fixture.stop)
	return fixture
}

func discoveryRecord(store string, endpoints ...string) *pb.ResolveStoreResponse {
	response := &pb.ResolveStoreResponse{StoreName: store, Endpoints: endpoints, CacheTtlMs: 30000}
	return response
}

func openDiscovery(t *testing.T, options OpenOptions) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	client, err := Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func discoveryRead(t *testing.T, client *Client, store string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	options := ReadOneOptions{StoreName: store, Request: clientTestReadRequest()}
	result, err := client.ReadOne(ctx, options)
	if err != nil || result == nil || result.Document == nil {
		t.Fatalf("direct Store read: result=%v error=%v", result, err)
	}
}

func TestResolveStorePublicResponseContract(t *testing.T) {
	response := &pb.ResolveStoreResponse{}
	descriptor := response.ProtoReflect().Descriptor()
	fields := descriptor.Fields()
	names := []protoreflect.Name{"store_name", "endpoints", "cache_ttl_ms"}
	if fields.Len() != len(names) {
		t.Fatalf("public resolution exposes %d fields; only Store name, endpoints and cache TTL are allowed", fields.Len())
	}
	for i, name := range names {
		field := fields.ByName(name)
		if field == nil || field.Number() != protoreflect.FieldNumber(i+1) {
			t.Fatalf("public resolution field %s changed its wire identity", name)
		}
	}
	file := descriptor.ParentFile()
	if file.Messages().ByName("NodeAnnouncement") != nil || file.Services().ByName("PeerDiscoveryService") != nil {
		t.Fatal("public Store protocol contains internal peer discovery metadata")
	}
}

func TestOpenResolvesMultipleStoresAndBalancesDirectStreams(t *testing.T) {
	firstBusiness := &clientTestPeer{mode: "normal"}
	first := &discoveryPeer{business: firstBusiness}
	firstListener := listenDiscovery(t, first, "127.0.0.1:0")
	secondBusiness := &clientTestPeer{mode: "normal"}
	second := &discoveryPeer{business: secondBusiness}
	secondListener := listenDiscovery(t, second, "127.0.0.1:0")
	seed := &discoveryPeer{}
	seed.set("records", discoveryRecord("records", firstListener.address, secondListener.address))
	seed.set("other", discoveryRecord("other", secondListener.address))
	seedListener := listenDiscovery(t, seed, "127.0.0.1:0")
	options := OpenOptions{Seed: seedListener.address, Stores: []string{"records", "other"}}
	client := openDiscovery(t, options)
	for range 20 {
		discoveryRead(t, client, "records")
	}
	discoveryRead(t, client, "other")
	if first.executions.Load() == 0 || second.executions.Load() == 0 || seed.executions.Load() != 0 {
		t.Fatalf("business must balance direct replicas: first=%d second=%d seed=%d", first.executions.Load(), second.executions.Load(), seed.executions.Load())
	}
	if first.resolves.Load() != 0 || second.resolves.Load() != 0 {
		t.Fatal("business replicas received initialization traffic")
	}
	unknown := ReadOneOptions{StoreName: "uninitialized", Request: clientTestReadRequest()}
	if _, err := client.ReadOne(t.Context(), unknown); err == nil {
		t.Fatal("uninitialized Store accepted")
	}
}

func TestClientDNSDiscoversScaleAndDrainsRetiredReplica(t *testing.T) {
	dns := testdns.Start(t)
	firstBusiness := &clientTestPeer{mode: "normal"}
	first := &discoveryPeer{business: firstBusiness}
	firstListener := listenDiscovery(t, first, "127.0.0.1:0")
	_, port, _ := net.SplitHostPort(firstListener.address)
	secondBusiness := &clientTestPeer{mode: "normal"}
	second := &discoveryPeer{business: secondBusiness}
	secondListener := listenDiscovery(t, second, net.JoinHostPort("::1", port))
	_ = secondListener
	name := "store.replicas.test"
	ipv4 := netip.MustParseAddr("127.0.0.1")
	ipv6 := netip.MustParseAddr("::1")
	answer := testdns.Answer{Addresses: []netip.Addr{ipv4}}
	dns.Set(name, answer)
	seed := &discoveryPeer{}
	seed.set("records", discoveryRecord("records", net.JoinHostPort(name, port)))
	seedListener := listenDiscovery(t, seed, "127.0.0.1:0")
	options := OpenOptions{Seed: seedListener.address, Stores: []string{"records"}, Resolver: dns.Resolver(), RefreshInterval: 50 * time.Millisecond, ResolveTimeout: time.Second}
	client := openDiscovery(t, options)
	discoveryRead(t, client, "records")
	answer.Addresses = []netip.Addr{ipv4, ipv6}
	dns.Set(name, answer)
	deadline := time.Now().Add(3 * time.Second)
	for second.executions.Load() == 0 && time.Now().Before(deadline) {
		discoveryRead(t, client, "records")
		time.Sleep(10 * time.Millisecond)
	}
	if second.executions.Load() == 0 {
		t.Fatal("healthy connection prevented discovery of scaled replica")
	}
	// Existing Store replicas remain discoverable while their directory lease
	// is valid, even when the initialization Service loses its only endpoint.
	seedListener.stop()
	answer.Addresses = []netip.Addr{ipv6}
	dns.Set(name, answer)
	time.Sleep(200 * time.Millisecond)
	before := first.executions.Load()
	for range 12 {
		discoveryRead(t, client, "records")
	}
	if first.executions.Load() != before || seed.executions.Load() != 0 {
		t.Fatal("retired DNS replica or seed still receives business RPCs")
	}
	if dns.Queries.Load() < 4 {
		t.Fatal("business DNS was not proactively refreshed")
	}
}

func TestClientRefreshChangesEndpointsWithoutReplayingActiveStream(t *testing.T) {
	blocked := &clientTestPeer{mode: "blocked_receive", canceled: make(chan struct{})}
	first := &discoveryPeer{business: blocked}
	firstListener := listenDiscovery(t, first, "127.0.0.1:0")
	secondBusiness := &clientTestPeer{mode: "normal"}
	second := &discoveryPeer{business: secondBusiness}
	secondListener := listenDiscovery(t, second, "127.0.0.1:0")
	seed := &discoveryPeer{}
	seed.set("records", discoveryRecord("records", firstListener.address))
	seedListener := listenDiscovery(t, seed, "127.0.0.1:0")
	options := OpenOptions{Seed: seedListener.address, Stores: []string{"records"}, RefreshInterval: 50 * time.Millisecond}
	client := openDiscovery(t, options)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		options := ReadOneOptions{StoreName: "records", Request: clientTestReadRequest()}
		_, err := client.ReadOne(ctx, options)
		finished <- err
	}()
	deadline := time.Now().Add(time.Second)
	for first.executions.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if first.executions.Load() != 1 {
		t.Fatal("initial finite stream did not start")
	}
	response := discoveryRecord("records", secondListener.address)
	seed.set("records", response)
	time.Sleep(200 * time.Millisecond)
	discoveryRead(t, client, "records")
	select {
	case err := <-finished:
		t.Fatalf("endpoint migration interrupted active stream: %v", err)
	case <-blocked.canceled:
		t.Fatal("removed endpoint canceled an active stream")
	default:
	}
	if first.executions.Load() != 1 || second.executions.Load() != 1 {
		t.Fatal("active stream was replayed after directory migration")
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("active stream failed to cancel")
	}
}

func TestClientLeaseExpiresAndConflictInvalidates(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprint(conflict), func(t *testing.T) {
			business := &clientTestPeer{mode: "normal"}
			target := &discoveryPeer{business: business}
			targetListener := listenDiscovery(t, target, "127.0.0.1:0")
			seed := &discoveryPeer{}
			response := discoveryRecord("records", targetListener.address)
			response.CacheTtlMs = 300
			seed.set("records", response)
			seedListener := listenDiscovery(t, seed, "127.0.0.1:0")
			options := OpenOptions{Seed: seedListener.address, Stores: []string{"records"}, RefreshInterval: 50 * time.Millisecond, ResolveTimeout: 100 * time.Millisecond}
			client := openDiscovery(t, options)
			seed.mu.Lock()
			if conflict {
				seed.err = status.Error(codes.FailedPrecondition, "two owning groups")
			} else {
				seed.err = status.Error(codes.Unavailable, "temporarily unavailable")
			}
			seed.mu.Unlock()
			if !conflict {
				discoveryRead(t, client, "records")
			}
			wait := 150 * time.Millisecond
			if !conflict {
				wait = 400 * time.Millisecond
			}
			time.Sleep(wait)
			before := target.executions.Load()
			optionsRecord := ReadOneOptions{StoreName: "records", Request: clientTestReadRequest()}
			if _, err := client.ReadOne(t.Context(), optionsRecord); err == nil {
				t.Fatal("expired or conflicting mapping accepted business request")
			}
			if target.executions.Load() != before {
				t.Fatal("rejected request reached previous Store owner")
			}
		})
	}
}

func TestOpenValidatesWholeMappingAndBoundsDeadTargets(t *testing.T) {
	business := &clientTestPeer{mode: "normal"}
	target := &discoveryPeer{business: business}
	targetListener := listenDiscovery(t, target, "127.0.0.1:0")
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddress := dead.Addr().String()
	_ = dead.Close()
	seed := &discoveryPeer{}
	seedListener := listenDiscovery(t, seed, "127.0.0.1:0")
	for _, invalid := range []string{"wrong-store", "bad-target", "duplicate-target", "empty-ttl", "overflow-ttl", "unknown-field", "dead-only"} {
		t.Run(invalid, func(t *testing.T) {
			response := discoveryRecord("records", targetListener.address)
			switch invalid {
			case "wrong-store":
				response.StoreName = "other"
			case "bad-target":
				response.Endpoints = []string{"http://arbitrary.example:7447"}
			case "duplicate-target":
				response.Endpoints = []string{targetListener.address, targetListener.address}
			case "empty-ttl":
				response.CacheTtlMs = 0
			case "overflow-ttl":
				response.CacheTtlMs = ^uint64(0)
			case "unknown-field":
				response.ProtoReflect().SetUnknown([]byte{0x22, 0x01, 0x00})
			case "dead-only":
				response.Endpoints = []string{deadAddress}
			}
			seed.set("records", response)
			options := OpenOptions{Seed: seedListener.address, Stores: []string{"records"}, ResolveTimeout: 100 * time.Millisecond}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			started := time.Now()
			client, err := Open(ctx, options)
			if err == nil {
				_ = client.Close()
				t.Fatal("invalid or dead-only directory accepted")
			}
			if time.Since(started) > time.Second {
				t.Fatal("initialization exceeded caller deadline")
			}
		})
	}
	seed.set("records", discoveryRecord("records", deadAddress, targetListener.address))
	options := OpenOptions{Seed: seedListener.address, Stores: []string{"records"}, ResolveTimeout: 100 * time.Millisecond}
	client := openDiscovery(t, options)
	discoveryRead(t, client, "records")
	if seed.executions.Load() != 0 {
		t.Fatal("initialization sent a business request")
	}
}

func TestOpenCancellationAndCloseJoinDiscovery(t *testing.T) {
	dns := testdns.Start(t)
	answer := testdns.Answer{Drop: true}
	dns.Set("unreachable.seed.test", answer)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	options := OpenOptions{Seed: "unreachable.seed.test:7447", Stores: []string{"records"}, Resolver: dns.Resolver()}
	started := time.Now()
	if client, err := Open(ctx, options); err == nil {
		_ = client.Close()
		t.Fatal("dropped DNS initialized")
	}
	if time.Since(started) > time.Second {
		t.Fatal("initial DNS ignored caller cancellation")
	}
	business := &clientTestPeer{mode: "normal"}
	target := &discoveryPeer{business: business}
	targetListener := listenDiscovery(t, target, "127.0.0.1:0")
	_, port, _ := net.SplitHostPort(targetListener.address)
	answer = testdns.Answer{Addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")}}
	dns.Set("live.store.test", answer)
	seed := &discoveryPeer{}
	seed.set("records", discoveryRecord("records", "live.store.test:"+port))
	seedListener := listenDiscovery(t, seed, "127.0.0.1:0")
	options = OpenOptions{Seed: seedListener.address, Stores: []string{"records"}, Resolver: dns.Resolver(), RefreshInterval: 50 * time.Millisecond, ResolveTimeout: time.Second}
	client := openDiscovery(t, options)
	answer.Drop = true
	dns.Set("live.store.test", answer)
	queries := dns.Queries.Load()
	deadline := time.Now().Add(time.Second)
	for dns.Queries.Load() == queries && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	started = time.Now()
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("Close did not cancel and join in-flight DNS")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	record := ReadOneOptions{StoreName: "records", Request: clientTestReadRequest()}
	if _, err := client.ReadOne(t.Context(), record); !errors.Is(err, ErrClosed) {
		t.Fatal("closed client admitted request", err)
	}
	queries = dns.Queries.Load()
	time.Sleep(100 * time.Millisecond)
	if dns.Queries.Load() != queries {
		t.Fatal("discovery queried DNS after Close returned")
	}
}

func TestDiscoveredWriteLossIsNeverReplayed(t *testing.T) {
	business := &clientTestPeer{mode: "write_reply_loss"}
	target := &discoveryPeer{business: business}
	targetListener := listenDiscovery(t, target, "127.0.0.1:0")
	seed := &discoveryPeer{}
	seed.set("records", discoveryRecord("records", targetListener.address))
	seedListener := listenDiscovery(t, seed, "127.0.0.1:0")
	options := OpenOptions{Seed: seedListener.address, Stores: []string{"records"}}
	client := openDiscovery(t, options)
	document := &Document{MediaType: "application/octet-stream", Data: []byte("one write")}
	request := &WriteRequest{Resource: "records/s:key", Document: document}
	record := WriteOptions{StoreName: "records", Request: request}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	result, err := client.Put(ctx, record)
	if err == nil || result != nil || target.executions.Load() != 1 || seed.executions.Load() != 0 {
		t.Fatalf("write evidence/replay: result=%v error=%v direct=%d seed=%d", result, err, target.executions.Load(), seed.executions.Load())
	}
}
