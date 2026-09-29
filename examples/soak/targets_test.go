package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"google.golang.org/grpc"
)

func TestFixedTargetValidation(t *testing.T) {
	invalid := []options{
		{},
		{Address: "service:7447", Targets: "127.0.0.1:7447", Workers: 6},
		{Targets: "127.0.0.1:7447,", Workers: 6},
		{Targets: "service:7447", Workers: 6},
		{Targets: "127.0.0.1", Workers: 6},
		{Targets: "127.0.0.1:0", Workers: 6},
		{Targets: "127.0.0.1:65536", Workers: 6},
		{Targets: "127.0.0.1:+7447", Workers: 6},
		{Targets: "0.0.0.0:7447", Workers: 6},
		{Targets: "224.0.0.1:7447", Workers: 6},
		{Targets: "[fe80::1%zone]:7447", Workers: 6},
		{Targets: "127.0.0.1:7447,[::ffff:127.0.0.1]:7447,127.0.0.3:7447", Workers: 6},
		{Targets: "127.0.0.1:7447,127.0.0.2:7447", Workers: 4},
		{Targets: "127.0.0.1:7447,127.0.0.2:7447,127.0.0.3:7447", Workers: 2},
		{Targets: "127.0.0.1:7447,127.0.0.2:7447,127.0.0.3:7447", Workers: 8},
	}
	for i, cfg := range invalid {
		if _, err := targetAddresses(cfg); err == nil {
			t.Fatalf("accepted invalid configuration %d: %+v", i, cfg)
		}
	}
	legacy := options{Address: "service.namespace.svc:7447", Workers: 6}
	addresses, err := targetAddresses(legacy)
	if err != nil || len(addresses) != 1 || addresses[0] != legacy.Address {
		t.Fatal("single Service target changed", addresses, err)
	}
	fixed := options{Targets: "[2001:db8::1]:7447,127.0.0.2:7447,127.0.0.3:7447", Workers: 6}
	if addresses, err := targetAddresses(fixed); err != nil || len(addresses) != 3 {
		t.Fatal("valid fixed targets rejected", addresses, err)
	}
}

type targetReceipt struct {
	target   int
	resource string
}

type fixedTargetServer struct {
	pb.UnimplementedWeirServer
	number  int
	created chan<- targetReceipt
}

func (s *fixedTargetServer) Mutate(ctx context.Context, req *pb.MutateRequest) (*pb.MutationResult, error) {
	if req.GetCreate() != nil {
		receipt := targetReceipt{target: s.number, resource: req.Resource}
		s.created <- receipt
	}
	if req.GetPut() != nil {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	result := &pb.MutationResult{Outcome: pb.MutationOutcome_APPLIED}
	return result, nil
}

func TestEachFixedTargetReceivesBothBackendsAndIsAudited(t *testing.T) {
	created := make(chan targetReceipt, 6)
	addresses := make([]string, 3)
	for i := range addresses {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addresses[i] = listener.Addr().String()
		server := grpc.NewServer()
		backend := &fixedTargetServer{number: i, created: created}
		pb.RegisterWeirServer(server, backend)
		go server.Serve(listener)
		t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	}
	cfg := options{Targets: strings.Join(addresses, ","), Mongo: "weir://mongo/test/records", Search: "weir://search/records", RunID: "weir-soak-fixed", Duration: 2 * time.Minute, MaxP99: time.Second, Workers: 6, Rate: 5}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var output bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, &output) }()
	seen := make(map[string]bool)
	for range 6 {
		select {
		case receipt := <-created:
			var number int
			key := strings.Split(receipt.resource, "/s:")[1]
			if _, err := fmt.Sscanf(key, "weir-soak-fixed-%02d", &number); err != nil {
				t.Fatal(err)
			}
			backend := "mongo"
			if number%2 == 1 {
				backend = "search"
			}
			if receipt.target != number%3 || !strings.HasPrefix(receipt.resource, "weir://"+backend+"/") || seen[key] {
				t.Fatal("worker did not use its fixed target/backend", receipt, number)
			}
			seen[key] = true
		case <-ctx.Done():
			t.Fatal("not all fixed targets received their workers")
		}
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("interrupted qualification passed")
	}
	var start struct {
		Targets []string `json:"targets"`
		Workers []struct {
			Worker  int    `json:"worker"`
			Backend string `json:"backend"`
			Target  string `json:"target"`
		} `json:"worker_targets"`
	}
	if err := json.NewDecoder(&output).Decode(&start); err != nil {
		t.Fatal(err)
	}
	if len(start.Targets) != 3 || len(start.Workers) != 6 {
		t.Fatal("missing target audit", start)
	}
	for i, worker := range start.Workers {
		backend := "mongo"
		if i%2 == 1 {
			backend = "search"
		}
		if worker.Worker != i || worker.Backend != backend || worker.Target != addresses[i%3] {
			t.Fatal("incorrect target audit", worker)
		}
	}
}
