package weir

import (
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
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
// change the client's round-robin balancing or turn on retries.
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
	options := append(connectionOptions(), grpc.WithResolvers(builder))
	connection, err := grpc.NewClient("weirtest:///records", options...)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{connection: connection, store: pb.NewStoreServiceClient(connection)}
	defer client.Close()
	for range 40 {
		request := clientTestReadRequest()
		readOptions := ReadOneOptions{StoreName: "records", Request: request}
		if _, err := client.ReadOne(t.Context(), readOptions); err != nil {
			t.Fatal(err)
		}
	}
	if first.streams.Load() == 0 || second.streams.Load() == 0 || first.streams.Load()+second.streams.Load() != 40 {
		t.Fatal("calls were not spread across both servers exactly once", first.streams.Load(), second.streams.Load())
	}
}
