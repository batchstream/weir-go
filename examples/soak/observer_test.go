package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"google.golang.org/grpc"
)

func observerFiles(t *testing.T) options {
	t.Helper()
	dir := t.TempDir()
	cfg := options{ObserverStatus: filepath.Join(dir, "observer.status.json"), ObserverHeartbeat: filepath.Join(dir, "observer.ready")}
	if err := os.WriteFile(cfg.ObserverHeartbeat, []byte("ready"), 0600); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func publishStatus(filename string, data []byte) error {
	if err := os.WriteFile(filename+".tmp", data, 0600); err != nil {
		return err
	}
	return os.Rename(filename+".tmp", filename)
}

func TestObserverRejectsTerminalAndMalformedStatus(t *testing.T) {
	for _, data := range []string{`{"passed":false,"error":"pod restarted"}`, `{"passed":true}`, `{}`, `{"passed":"false"}`, `{"passed":false`, `{"passed":false} {}`} {
		t.Run(data, func(t *testing.T) {
			cfg := observerFiles(t)
			if err := publishStatus(cfg.ObserverStatus, []byte(data)); err != nil {
				t.Fatal(err)
			}
			if _, err := awaitObserver(context.Background(), cfg); err == nil {
				t.Fatal("terminal or malformed observer status accepted")
			}
		})
	}
}

func TestObserverHeartbeatUsesMonotonicElapsedAndFailsClosed(t *testing.T) {
	cfg := observerFiles(t)
	future := time.Now().Add(24 * time.Hour)
	if err := os.Chtimes(cfg.ObserverHeartbeat, future, future); err != nil {
		t.Fatal(err)
	}
	guard, err := awaitObserver(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := guard.check(guard.lastChange.Add(observerStaleAfter + time.Nanosecond)); err == nil {
		t.Fatal("future file time extended the freshness interval")
	}
	changed := future.Add(-48 * time.Hour)
	if err := os.Chtimes(cfg.ObserverHeartbeat, changed, changed); err != nil {
		t.Fatal(err)
	}
	now := guard.lastChange.Add(100 * time.Second)
	if err := guard.check(now); err != nil {
		t.Fatal(err)
	}
	if !guard.lastChange.Equal(now) {
		t.Fatal("changed heartbeat did not refresh monotonic deadline")
	}
	if err := guard.check(now.Add(observerStaleAfter + time.Nanosecond)); err == nil {
		t.Fatal("unchanged heartbeat did not expire")
	}
	if err := os.Remove(cfg.ObserverHeartbeat); err != nil {
		t.Fatal(err)
	}
	if err := guard.check(now); err == nil {
		t.Fatal("missing runtime heartbeat ignored")
	}
	if err := os.Mkdir(cfg.ObserverHeartbeat, 0700); err != nil {
		t.Fatal(err)
	}
	if err := guard.check(now); err == nil {
		t.Fatal("non-file heartbeat accepted")
	}
}

func TestObserverReadinessIsBoundedAndAudited(t *testing.T) {
	cfg := observerFiles(t)
	if err := os.Remove(cfg.ObserverHeartbeat); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	var output bytes.Buffer
	err := run(ctx, cfg, &output)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(output.String(), `"kind":"failed"`) {
		t.Fatal("missing observer readiness was not a bounded audited failure", err, output.String())
	}
	if err := os.Mkdir(cfg.ObserverStatus, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := awaitObserver(context.Background(), cfg); err == nil {
		t.Fatal("unreadable status ignored")
	}
}

type blockedWriteServer struct {
	pb.UnimplementedWeirServer
	mu      sync.Mutex
	puts    map[string]int
	deletes int
	started chan struct{}
}

func (s *blockedWriteServer) Mutate(ctx context.Context, req *pb.MutateRequest) (*pb.MutationResult, error) {
	s.mu.Lock()
	if req.GetPut() != nil {
		s.puts[req.Resource]++
		select {
		case s.started <- struct{}{}:
		default:
		}
		s.mu.Unlock()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if req.GetDelete() != nil {
		s.deletes++
	}
	s.mu.Unlock()
	result := &pb.MutationResult{Outcome: pb.MutationOutcome_APPLIED}
	return result, nil
}

func TestObserverFailureCancelsWorkAndRetainsUncertainRecords(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	backend := &blockedWriteServer{puts: make(map[string]int), started: make(chan struct{}, 1)}
	pb.RegisterWeirServer(server, backend)
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	cfg := observerFiles(t)
	cfg.Address, cfg.Mongo, cfg.Search = listener.Addr().String(), "weir://mongo/test/records", "weir://search/records"
	cfg.RunID, cfg.Duration, cfg.MaxP99, cfg.Workers, cfg.Rate = "weir-soak-observer-negative", 2*time.Minute, time.Second, 2, 100
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	published := make(chan error, 1)
	go func() {
		select {
		case <-backend.started:
			published <- publishStatus(cfg.ObserverStatus, []byte(`{"passed":false,"error":"pod restarted"}`))
		case <-ctx.Done():
			published <- ctx.Err()
		}
	}()
	var output bytes.Buffer
	err = run(ctx, cfg, &output)
	if err == nil || !strings.Contains(err.Error(), "observer failed: pod restarted") {
		t.Fatal("observer did not cancel the run", err, output.String())
	}
	if err := <-published; err != nil {
		t.Fatal(err)
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
	var last map[string]any
	decoder := json.NewDecoder(&output)
	for decoder.More() {
		if err := decoder.Decode(&last); err != nil {
			t.Fatal(err)
		}
	}
	if last["kind"] != "failed" || last["unknown"].(float64) < 1 {
		t.Fatal("missing terminal failure/uncertainty evidence", last)
	}
}
