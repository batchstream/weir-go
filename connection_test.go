package weir

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/test/bufconn"
)

func TestNewRequiresHostAndPort(t *testing.T) {
	for _, address := range []string{"", "weir", "127.0.0.1", ":7447", "weir:", "weir:0", "weir:65536", "weir:+1", "http://weir:7447", "dns:///weir:7447", "a b:7447", "weir..local:7447", "[::1]"} {
		if client, err := New(address); err == nil {
			_ = client.Close()
			t.Fatal("New accepted an address without an explicit host and port", address)
		}
	}
	for _, address := range []string{"weir:7447", "weir.default.svc.cluster.local.:7447", "127.0.0.1:7447", "[::1]:7447", "weir_headless:1"} {
		client, err := New(address)
		if err != nil {
			t.Fatal("New rejected a host:port address", address, err)
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// A resolver that returns several servers and its own service config must not
// change the client's balancing or turn on retries.
func TestConnectionSpreadsCallsAndIgnoresResolverServiceConfig(t *testing.T) {
	first, second := &clientTestPeer{}, &clientTestPeer{}
	addresses := []resolver.Address{{Addr: clientTestAddress(t, first)}, {Addr: clientTestAddress(t, second)}}
	builder := manual.NewBuilderWithScheme("weirtest")
	builder.BuildCallback = func(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) {
		config := cc.ParseServiceConfig(`{"loadBalancingConfig":[{"pick_first":{}}],"methodConfig":[{"name":[{}],"retryPolicy":{"maxAttempts":5,"initialBackoff":"0.01s","maxBackoff":"0.01s","backoffMultiplier":1,"retryableStatusCodes":["UNAVAILABLE"]}}]}`)
		if config.Err != nil {
			t.Error(config.Err)
		}
		state := resolver.State{Addresses: addresses, ServiceConfig: config}
		builder.InitialState(state)
	}
	options := connectionOptions(nil, builder)
	connection, err := grpc.NewClient("weirtest:///records", options...)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	// A stream of its own is balanced on each call.
	client := &Client{store: pb.NewStoreServiceClient(connection)}
	command, _, err := readCommand(clientTestReadRequest())
	if err != nil {
		t.Fatal(err)
	}
	calls := int64(0)
	deadline := time.Now().Add(5 * time.Second)
	for first.streams.Load() == 0 || second.streams.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("calls did not reach both servers", first.streams.Load(), second.streams.Load())
		}
		_, err := readKind.alone(t.Context(), client, "records", command)
		if err != nil {
			t.Fatal(err)
		}
		calls++
	}
	if first.streams.Load()+second.streams.Load() != calls {
		t.Fatal("calls were not sent exactly once", first.streams.Load(), second.streams.Load(), calls)
	}
}

// sharedAndOwnCalls sends a call on a shared stream and one on a stream of its own.
func sharedAndOwnCalls(t *testing.T, client *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if _, err := mutateOne(ctx, client, putRequest("records/s:a", `{}`)); err != nil {
		t.Fatal("shared stream:", err)
	}
	results, err := client.Mutate(ctx, "records", WithRequests(putRequest("records/s:a", `{}`), putRequest("records/s:b", `{}`)))
	if err != nil || results[1].GetOutcome() != MutationApplied {
		t.Fatal("own stream:", results, err)
	}
}

func TestDialOptionsReachAServerThroughACustomDialer(t *testing.T) {
	memory := bufconn.Listen(1 << 20)
	directory, err := os.MkdirTemp("", "weir")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socket := filepath.Join(directory, "weir.sock")
	unix, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	dialers := map[string]struct {
		listener net.Listener
		dial     func(context.Context, string) (net.Conn, error)
	}{
		"in memory": {listener: memory, dial: func(ctx context.Context, _ string) (net.Conn, error) { return memory.DialContext(ctx) }},
		"unix socket": {listener: unix, dial: func(ctx context.Context, _ string) (net.Conn, error) {
			dialer := net.Dialer{}
			return dialer.DialContext(ctx, "unix", socket)
		}},
	}
	for name, test := range dialers {
		t.Run(name, func(t *testing.T) {
			server := grpc.NewServer()
			peer := &countsMessages{}
			pb.RegisterStoreServiceServer(server, peer)
			go func() { _ = server.Serve(test.listener) }()
			t.Cleanup(server.Stop)
			// The address only labels the server the dialer reaches.
			client, err := New("127.0.0.1:1", nil, WithDialOptions(grpc.WithContextDialer(test.dial)))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			sharedAndOwnCalls(t, client)
		})
	}
}

func TestDialOptionsAddAnInterceptor(t *testing.T) {
	var calls atomic.Int64
	interceptor := func(ctx context.Context, desc *grpc.StreamDesc, connection *grpc.ClientConn, method string, streamer grpc.Streamer, options ...grpc.CallOption) (grpc.ClientStream, error) {
		calls.Add(1)
		return streamer(ctx, desc, connection, method, options...)
	}
	option := WithDialOptions(grpc.WithStreamInterceptor(interceptor))
	peer := &countsMessages{}
	client, err := New(serveForTest(t, peer), option)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	sharedAndOwnCalls(t, client)
	if calls.Load() == 0 {
		t.Fatal("the interceptor did not run")
	}
}

func TestDialOptionsCannotReplaceWhatAClientReliesOn(t *testing.T) {
	// Neither a resolver that never answers nor another balancer may take over.
	other := manual.NewBuilderWithScheme(endpointScheme)
	balancer := grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`)
	peer := &countsMessages{}
	client, err := New(serveForTest(t, peer), WithDialOptions(grpc.WithResolvers(other), balancer))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	sharedAndOwnCalls(t, client)
	client.lanes.endpoints.mu.Lock()
	ready := len(client.lanes.endpoints.ready)
	client.lanes.endpoints.mu.Unlock()
	if ready == 0 {
		t.Fatal("another balancer replaced the Client's: it knows no ready server")
	}
}

// tlsTestServer serves TLS for a certificate of one name and returns the pool that
// trusts it and an address through localhost.
func tlsTestServer(t *testing.T, name string) (*x509.CertPool, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	subject := pkix.Name{CommonName: name}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: subject, DNSNames: []string{name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(certificate)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	chain := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	config := &tls.Config{Certificates: []tls.Certificate{chain}}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(config)))
	peer := &countsMessages{}
	pb.RegisterStoreServiceServer(server, peer)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return pool, net.JoinHostPort("localhost", port)
}

func TestDialOptionsReplaceCredentialsAndTLSChecksTheAddressName(t *testing.T) {
	pool, address := tlsTestServer(t, "localhost")
	trusted := grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(pool, ""))
	client, err := New(address, WithDialOptions(trusted))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	sharedAndOwnCalls(t, client)

	otherPool, otherAddress := tlsTestServer(t, "other.example")
	mismatched := grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(otherPool, ""))
	other, err := New(otherAddress, WithDialOptions(mismatched))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if _, err := mutateOne(ctx, other, putRequest("records/s:a", `{}`)); err == nil {
		t.Fatal("a certificate for another name was accepted")
	}
}
