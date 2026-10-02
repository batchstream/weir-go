package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
)

type uncertainServer struct {
	ownerFixture
	mu      sync.Mutex
	puts    map[string]int
	deletes int
}

func (s *uncertainServer) mutate(_ context.Context, req *pb.MutateRequest) (*pb.MutationResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := &pb.MutationResult{Outcome: pb.MutationOutcome_APPLIED}
	if req.GetPut() != nil {
		s.puts[req.Resource]++
		result.Outcome = pb.MutationOutcome_UNKNOWN
		result.Failure = &pb.Failure{Code: pb.FailureCode_UNAVAILABLE, Message: "lost acknowledgement"}
	}
	if req.GetDelete() != nil {
		s.deletes++
	}
	return result, nil
}

func TestUnknownStopsWorkWithoutReplayOrCleanup(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	backend := &uncertainServer{puts: make(map[string]int)}
	backend.endpoint = listener.Addr().String()
	pb.RegisterStoreServiceServer(server, backend)
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	cfg := options{Address: listener.Addr().String(), Mongo: "weir://mongo/test/records", Search: "weir://search/records", RunID: "weir-soak-negative", Duration: 2 * time.Second, MaxP99: time.Second, Workers: 2, Rate: 100}
	var output bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := run(ctx, cfg, &output); err == nil {
		t.Fatal("unknown write passed qualification")
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.puts) == 0 || backend.deletes != 0 {
		t.Fatal("uncertain records were not retained", backend.puts, backend.deletes)
	}
	for key, count := range backend.puts {
		if count != 1 {
			t.Fatal("write was replayed", key, count)
		}
	}
	if !strings.Contains(output.String(), `"kind":"failed"`) || !strings.Contains(output.String(), `"unknown":true`) || strings.Contains(output.String(), `"kind":"passed"`) {
		t.Fatal("failure evidence missing", output.String())
	}
}

func TestMutationEvidenceIsIncludedInFailureAccounting(t *testing.T) {
	unknown := &mutationAuditError{Outcome: pb.MutationOutcome_UNKNOWN, Cause: errors.New("uncertain")}
	if !unknownWrite(unknown) {
		t.Fatal("UNKNOWN was omitted")
	}
	if unknownWrite(errors.New("ordinary read failure")) {
		t.Fatal("read error became unknown mutation")
	}
	acknowledged := &mutationAuditError{Outcome: pb.MutationOutcome_APPLIED, Cause: errors.New("response interrupted")}
	if unknownWrite(acknowledged) {
		t.Fatal("APPLIED was downgraded after transport failure")
	}
}

func TestHistogramUsesUpperBoundAndOwnershipIsExplicit(t *testing.T) {
	var h histogram
	for range 99 {
		h.add(time.Millisecond)
	}
	h.add(250 * time.Millisecond)
	if h.p99() != time.Millisecond {
		t.Fatal(h.p99())
	}
	h.add(250 * time.Millisecond)
	if h.p99() != 500*time.Millisecond {
		t.Fatal(h.p99())
	}
	h = histogram{}
	h.add(11 * time.Second)
	if h.Overflow != 1 || h.p99() <= 11*time.Second {
		t.Fatal("histogram underreported an overflow", h)
	}
	cfg := options{}
	if err := validate(cfg); err == nil {
		t.Fatal("zero configuration could contact a service")
	}
}
