package weir

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fixtureServer struct {
	pb.UnimplementedWeirServer
	mode  string
	calls atomic.Int32
}

func newFixture(t *testing.T, mode string) (*Client, *fixtureServer) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	fixture := &fixtureServer{mode: mode}
	pb.RegisterWeirServer(server, fixture)
	go server.Serve(listener)
	conn, err := grpc.NewClient("passthrough:///fixture",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(),
		grpc.WithDefaultCallOptions(grpc.MaxRetryRPCBufferSize(0), grpc.MaxCallRecvMsgSize(MaxFrameBytes), grpc.MaxCallSendMsgSize(MaxFrameBytes)))
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{conn: conn, rpc: pb.NewWeirClient(conn), timeout: time.Second}
	t.Cleanup(func() { conn.Close(); server.Stop(); listener.Close() })
	return client, fixture
}

func readRequest() *pb.ReadRequest {
	req := &pb.ReadRequest{Resource: "weir://mongo/db/records/s:test"}
	return req
}
func mutationRequest() *pb.MutateRequest {
	doc := &pb.Document{MediaType: MediaTypeJSON, Data: []byte(`{"n":1}`)}
	action := &pb.MutateRequest_Put{Put: doc}
	req := &pb.MutateRequest{Resource: readRequest().Resource, Action: action}
	return req
}
func missingResult() *pb.ReadResult {
	empty := &pb.Empty{}
	variant := &pb.ReadResult_Missing{Missing: empty}
	result := &pb.ReadResult{Result: variant}
	return result
}
func (s *fixtureServer) Read(ctx context.Context, _ *pb.ReadRequest) (*pb.ReadResult, error) {
	s.calls.Add(1)
	switch s.mode {
	case "failure":
		failure := &pb.Failure{Code: pb.FailureCode_UNAVAILABLE, Message: "fixture"}
		variant := &pb.ReadResult_Failure{Failure: failure}
		result := &pb.ReadResult{Result: variant}
		return result, nil
	case "malformed":
		result := &pb.ReadResult{}
		return result, nil
	case "deadline":
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	case "unavailable":
		return nil, status.Error(codes.Unavailable, "fixture")
	default:
		return missingResult(), nil
	}
}
func (s *fixtureServer) Mutate(_ context.Context, _ *pb.MutateRequest) (*pb.MutationResult, error) {
	s.calls.Add(1)
	result := &pb.MutationResult{Outcome: pb.MutationOutcome_APPLIED}
	switch s.mode {
	case "unavailable":
		return nil, status.Error(codes.Unavailable, "after write")
	case "malformed":
		result.Outcome = pb.MutationOutcome_MUTATION_OUTCOME_UNSPECIFIED
	case "noop":
		result.Outcome = pb.MutationOutcome_NOT_APPLIED
	case "failure":
		result.Outcome = pb.MutationOutcome_NOT_APPLIED
		result.Failure = &pb.Failure{Code: pb.FailureCode_PRECONDITION_FAILED}
	case "applied-failure":
		result.Failure = &pb.Failure{Code: pb.FailureCode_UNAVAILABLE}
	case "unknown":
		result.Outcome = pb.MutationOutcome_UNKNOWN
	}
	return result, nil
}

func TestReadAndMutationOutcomes(t *testing.T) {
	for _, mode := range []string{"ok", "noop", "failure", "applied-failure", "unknown", "malformed", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			client, fixture := newFixture(t, mode)
			result, err := client.Mutate(context.Background(), mutationRequest())
			if fixture.calls.Load() != 1 {
				t.Fatal("mutation replayed")
			}
			switch mode {
			case "ok", "noop":
				if err != nil {
					t.Fatal(err)
				}
			case "failure", "applied-failure":
				var failure *FailureError
				if !errors.As(err, &failure) {
					t.Fatalf("missing application failure: %v", err)
				}
				if mode == "applied-failure" && result.Outcome != pb.MutationOutcome_APPLIED {
					t.Fatal("lost known APPLIED outcome")
				}
			default:
				var mutation *MutationError
				if !errors.As(err, &mutation) || result.Outcome != pb.MutationOutcome_UNKNOWN {
					t.Fatalf("want UNKNOWN, got %v / %v", result, err)
				}
			}
		})
	}
	for _, mode := range []string{"ok", "failure", "malformed", "unavailable"} {
		t.Run("read-"+mode, func(t *testing.T) {
			client, fixture := newFixture(t, mode)
			result, err := client.Read(context.Background(), readRequest())
			if fixture.calls.Load() != 1 {
				t.Fatal("Read replayed")
			}
			if mode == "ok" && (err != nil || result.GetMissing() == nil) {
				t.Fatalf("missing is success: %v", err)
			}
			if mode != "ok" && err == nil {
				t.Fatal("missing error")
			}
		})
	}
}

func TestLocalValidationAndDeadline(t *testing.T) {
	client, fixture := newFixture(t, "deadline")
	result, err := client.Mutate(context.Background(), nil)
	if err == nil || result.Outcome != pb.MutationOutcome_NOT_STARTED || fixture.calls.Load() != 0 {
		t.Fatal("invalid mutation dispatched")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := client.Read(ctx, readRequest()); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("context lost: %v", err)
	}
	client.timeout = 20 * time.Millisecond
	if _, err := client.Read(context.Background(), readRequest()); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("default budget lost: %v", err)
	}
}

func TestResource(t *testing.T) {
	resource, err := Resource("mongo", "db", "records", "s:a/ 中?")
	if err != nil || resource != "weir://mongo/db/records/s:a%2F%20%E4%B8%AD%3F" {
		t.Fatalf("resource: %q %v", resource, err)
	}
	for _, raw := range []string{"weir://Mongo/x", "weir://mongo/..", "weir://mongo/%2e", "weir://mongo/%2f", "weir://mongo/%00", "weir://mongo/a?b", "weir://mongo/a//b"} {
		if _, _, err := parseResource(raw); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}

func (s *fixtureServer) Bulk(stream grpc.BidiStreamingServer[pb.BulkRequestFrame, pb.BulkResponseFrame]) error {
	s.calls.Add(1)
	if _, err := stream.Recv(); err != nil {
		return err
	}
	var ops []*pb.BulkOperation
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		ops = append(ops, frame.GetOperation())
	}
	if s.mode == "missing-end" {
		return nil
	}
	for i := len(ops) - 1; i >= 0; i-- {
		op := ops[i]
		result := &pb.BulkResult{Index: op.Index}
		if op.GetRead() != nil {
			read := missingResult()
			if s.mode == "result-limit" {
				document := &pb.Document{MediaType: MediaTypeJSON, Data: make([]byte, MaxDocumentBytes)}
				variant := &pb.ReadResult_Document{Document: document}
				read = &pb.ReadResult{Result: variant}
			}
			variant := &pb.BulkResult_Read{Read: read}
			result.Result = variant
		} else {
			mutation := &pb.MutationResult{Outcome: pb.MutationOutcome_APPLIED}
			if s.mode == "operation-failure" {
				mutation.Outcome = pb.MutationOutcome_NOT_APPLIED
				mutation.Failure = &pb.Failure{Code: pb.FailureCode_PRECONDITION_FAILED}
			}
			variant := &pb.BulkResult_Mutation{Mutation: mutation}
			result.Result = variant
		}
		if s.mode == "range" {
			result.Index = 999
		}
		if s.mode == "wrong-kind" {
			variant := &pb.BulkResult_Read{Read: missingResult()}
			result.Result = variant
		}
		variant := &pb.BulkResponseFrame_Result{Result: result}
		frame := &pb.BulkResponseFrame{Frame: variant}
		if err := stream.Send(frame); err != nil {
			return err
		}
		if s.mode == "duplicate" {
			if err := stream.Send(frame); err != nil {
				return err
			}
		}
		if s.mode == "partial" {
			return status.Error(codes.Unavailable, "lost connection")
		}
	}
	end := &pb.BulkEnd{ReceivedCount: uint64(len(ops)), ResultCount: uint64(len(ops))}
	if s.mode == "count" {
		end.ResultCount++
	}
	variant := &pb.BulkResponseFrame_End{End: end}
	frame := &pb.BulkResponseFrame{Frame: variant}
	if err := stream.Send(frame); err != nil {
		return err
	}
	if s.mode == "duplicate-end" {
		return stream.Send(frame)
	}
	if s.mode == "after-end" {
		result := &pb.BulkResult{Index: 0}
		read := &pb.BulkResult_Read{Read: missingResult()}
		result.Result = read
		variant := &pb.BulkResponseFrame_Result{Result: result}
		frame := &pb.BulkResponseFrame{Frame: variant}
		return stream.Send(frame)
	}
	if s.mode == "status-after-end" {
		return status.Error(codes.Unavailable, "after End")
	}
	return nil
}

func TestBulkCompletionAndPartialResults(t *testing.T) {
	for _, mode := range []string{"ok", "operation-failure", "missing-end", "duplicate", "range", "wrong-kind", "partial", "count", "duplicate-end", "after-end", "status-after-end"} {
		t.Run(mode, func(t *testing.T) {
			client, fixture := newFixture(t, mode)
			operations := []Operation{{Read: readRequest()}, {Mutate: mutationRequest()}}
			results, err := client.Bulk(context.Background(), "weir://mongo", operations)
			if fixture.calls.Load() != 1 {
				t.Fatal("Bulk replayed")
			}
			if mode == "ok" {
				if err != nil || len(results) != 2 || results[0].Index != 0 || results[1].Index != 1 {
					t.Fatalf("bad ordered results: %v %v", results, err)
				}
				return
			}
			var batch *BatchError
			if !errors.As(err, &batch) {
				t.Fatalf("want BatchError: %v", err)
			}
			if mode == "operation-failure" && (batch.Cause != nil || batch.Failures[1] == nil) {
				t.Fatalf("bad operation failure: %v", batch)
			}
			if mode == "partial" && (results[0] != nil || results[1] == nil) {
				t.Fatal("partial evidence lost")
			}
		})
	}
}

func TestBulkValidatesAllBeforeSending(t *testing.T) {
	client, fixture := newFixture(t, "ok")
	invalid := mutationRequest()
	invalid.Resource = "weir://other/db/records/s:test"
	operations := []Operation{{Mutate: mutationRequest()}, {Mutate: invalid}}
	if _, err := client.Bulk(context.Background(), "weir://mongo", operations); err == nil || fixture.calls.Load() != 0 {
		t.Fatal("partial batch dispatched before validation")
	}
	operations = make([]Operation, MaxBulkOperations+1)
	if _, err := client.Bulk(context.Background(), "weir://mongo", operations); err == nil {
		t.Fatal("unbounded batch accepted")
	}
}

func ExampleResource() {
	resource, _ := Resource("mongo", "orders", "records", "s:order/42")
	fmt.Println(resource)
	// Output: weir://mongo/orders/records/s:order%2F42
}

func TestBulkResultMemoryBound(t *testing.T) {
	client, _ := newFixture(t, "result-limit")
	operations := make([]Operation, 34)
	for index := range operations {
		operations[index].Read = readRequest()
	}
	results, err := client.Bulk(context.Background(), "weir://mongo", operations)
	var batch *BatchError
	if !errors.As(err, &batch) || batch.Cause == nil {
		t.Fatalf("missing memory bound error: %v", err)
	}
	retained := 0
	for _, result := range results {
		if result != nil {
			retained++
		}
	}
	if retained == 0 || retained >= 32 {
		t.Fatalf("retained %d oversized results", retained)
	}
}
