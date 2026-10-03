package weir

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protodelim"
)

type clientTestPeer struct {
	pb.UnimplementedStoreServiceServer
	mode      string
	completed atomic.Int64
	streams   atomic.Int64
	received  atomic.Int64
	commands  chan *pb.Command
	canceled  chan struct{}
}

func (p *clientTestPeer) Execute(stream pb.StoreService_ExecuteServer) error {
	p.streams.Add(1)
	if strings.HasPrefix(p.mode, "batch_") {
		return p.batchExecute(stream)
	}
	if strings.HasPrefix(p.mode, "scan_") {
		return p.scanExecute(stream)
	}
	if strings.HasPrefix(p.mode, "native_") {
		return p.nativeExecute(stream)
	}
	if p.mode == "reject_early" {
		return status.Error(codes.InvalidArgument, "fixture header rejection")
	}
	if p.mode == "early_eof" {
		return nil
	}
	if p.mode == "blocked_receive" {
		<-stream.Context().Done()
		close(p.canceled)
		return stream.Context().Err()
	}
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if p.canceled != nil {
				close(p.canceled)
			}
			return err
		}
		p.received.Add(1)
		if p.commands != nil {
			command, err := protocol.DecodeCommand(request.CommandPayload)
			if err != nil {
				return err
			}
			p.commands <- command
		}
		document := &pb.Document{MediaType: "application/octet-stream", Data: bytes.Repeat([]byte{37}, 257<<10)}
		readValue := &pb.ReadResult_Document{Document: document}
		read := &pb.ReadResult{Result: readValue}
		if p.mode == "read_missing" {
			empty := &pb.Empty{}
			read.Result = &pb.ReadResult_Missing{Missing: empty}
		}
		if p.mode == "read_failure" {
			failure := &pb.Failure{Code: pb.FailureCode_PERMISSION_DENIED, Message: "fixture backend denied read"}
			read.Result = &pb.ReadResult_Failure{Failure: failure}
		}
		resultValue := &pb.Result_Read{Read: read}
		result := &pb.Result{Index: request.RequestId, Result: resultValue}
		value := &pb.Event_Result{Result: result}
		event := &pb.Event{Version: 1, Value: value}
		if p.mode == "write_reply_loss" || p.mode == "write_end_failure" || p.mode == "typed_mutation" {
			mutation := &pb.MutationResult{Outcome: pb.MutationOutcome_APPLIED}
			result.Result = &pb.Result_Mutation{Mutation: mutation}
		}
		if p.mode == "invalid_event" {
			event.Version = 2
		}
		var encoded bytes.Buffer
		if _, err := protodelim.MarshalTo(&encoded, event); err != nil {
			return err
		}
		raw := encoded.Bytes()
		if p.mode == "incomplete_event" {
			raw = raw[:3]
		}
		id := request.RequestId
		if p.mode == "unknown_id" {
			id++
		}
		for len(raw) > 0 {
			size := min(len(raw), 19<<10)
			response := &pb.ExecuteResponse{RequestId: id, EventFragment: raw[:size]}
			if err := stream.Send(response); err != nil {
				return err
			}
			raw = raw[size:]
		}
		if p.mode == "write_reply_loss" {
			return status.Error(codes.Unavailable, "lost terminal after acknowledged write")
		}
		if p.mode == "missing_end" {
			return nil
		}
		end := &pb.ExecuteResponse{RequestId: id, RequestComplete: true}
		if err := stream.Send(end); err != nil {
			return err
		}
		p.completed.Add(1)
		if p.mode == "non_ok_after_end" || p.mode == "write_end_failure" {
			return status.Error(codes.Unavailable, "fixture failure after business completion")
		}
		if p.mode == "early_eof_after_result" {
			return nil
		}
	}
}

func clientTestConnection(t *testing.T, peer *clientTestPeer) pb.StoreServiceClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(10 << 20))
	pb.RegisterStoreServiceServer(server, peer)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(),
		grpc.WithStaticConnWindowSize(65535), grpc.WithStaticStreamWindowSize(65535),
	}
	connection, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return pb.NewStoreServiceClient(connection)
}

func clientTestReadRequest() *ReadRequest {
	request := &ReadRequest{Resource: "records/s:key"}
	return request
}

func clientTestRead() *Command { return NewReadCommand(clientTestReadRequest()) }

func TestExecuteConsumesFragmentedFiniteBatch(t *testing.T) {
	peer := &clientTestPeer{mode: "normal"}
	client := clientTestConnection(t, peer)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	produced, consumed := 0, 0
	opts := ExecuteOptions{StoreName: "records"}
	opts.Produce = func(context.Context) (*Command, error) {
		if produced == 25 {
			return nil, io.EOF
		}
		produced++
		return clientTestRead(), nil
	}
	opts.Consume = func(_ context.Context, id uint64, event *Event) error {
		data := event.GetResult().GetRead().GetDocument().GetData()
		if id != uint64(consumed+1) || len(data) != 257<<10 || !bytes.Equal(data, bytes.Repeat([]byte{37}, len(data))) {
			return errors.New("fragmented result corrupt, truncated or miscorrelated")
		}
		consumed++
		return nil
	}
	if err := Execute(ctx, client, opts); err != nil {
		t.Fatal(err)
	}
	if produced != 25 || consumed != 25 || peer.completed.Load() != 25 {
		t.Fatal("finite Execute failed to half-close and drain", produced, consumed, peer.completed.Load())
	}
}

func TestReadRejectsIncompleteAndInvalidResponses(t *testing.T) {
	for _, mode := range []string{"unknown_id", "invalid_event", "incomplete_event", "missing_end", "non_ok_after_end"} {
		t.Run(mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: mode}
			client := clientTestConnection(t, peer)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			opts := ReadOneOptions{StoreName: "records", Request: clientTestReadRequest()}
			result, err := ReadOne(ctx, client, opts)
			if err == nil {
				t.Fatal("invalid or incomplete RPC reported success", mode)
			}
			if mode == "missing_end" || mode == "non_ok_after_end" {
				if len(result.GetDocument().GetData()) != 257<<10 {
					t.Fatal("complete validated business evidence was discarded", mode)
				}
			} else if result != nil {
				t.Fatal("unvalidated response exposed a result", mode)
			}
			if mode == "non_ok_after_end" && !strings.Contains(err.Error(), "indeterminate") {
				t.Fatal("non-OK transport terminal lost uncertainty semantics", err)
			}
		})
	}
}

func TestExecuteProducerErrorCancelsAndJoins(t *testing.T) {
	peer := &clientTestPeer{mode: "normal", canceled: make(chan struct{})}
	client := clientTestConnection(t, peer)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	failure := errors.New("producer source failed")
	opts := ExecuteOptions{StoreName: "records"}
	opts.Produce = func(context.Context) (*Command, error) { return nil, failure }
	opts.Consume = func(context.Context, uint64, *Event) error { return nil }
	if err := Execute(ctx, client, opts); !errors.Is(err, failure) {
		t.Fatal("producer failure was masked", err)
	}
}

func TestExecuteConsumerErrorJoinsBlockedProducer(t *testing.T) {
	peer := &clientTestPeer{mode: "normal"}
	client := clientTestConnection(t, peer)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	failure := errors.New("consumer failed")
	produced := false
	joined := make(chan struct{})
	opts := ExecuteOptions{StoreName: "records"}
	opts.Produce = func(ctx context.Context) (*Command, error) {
		if !produced {
			produced = true
			return clientTestRead(), nil
		}
		<-ctx.Done()
		close(joined)
		return nil, ctx.Err()
	}
	opts.Consume = func(context.Context, uint64, *Event) error { return failure }
	if err := Execute(ctx, client, opts); !errors.Is(err, failure) {
		t.Fatal("consumer failure was masked", err)
	}
	select {
	case <-joined:
	default:
		t.Fatal("Execute returned before its canceled producer exited")
	}
}

func TestExecuteCanceledBlockedSendReleasesProducer(t *testing.T) {
	peer := &clientTestPeer{mode: "blocked_receive", canceled: make(chan struct{})}
	client := clientTestConnection(t, peer)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	document := &pb.Document{MediaType: "application/octet-stream", Data: make([]byte, 2<<20)}
	action := &pb.MutateRequest_Put{Put: document}
	mutation := &pb.MutateRequest{Resource: "records/s:key", Action: action}
	value := &pb.Command_Mutate{Mutate: mutation}
	wire := &pb.Command{Version: 1, Operation: value}
	call := &Command{wire: wire}
	var active atomic.Int64
	opts := ExecuteOptions{StoreName: "records"}
	opts.Produce = func(context.Context) (*Command, error) {
		active.Add(1)
		defer active.Add(-1)
		return call, nil
	}
	opts.Consume = func(context.Context, uint64, *Event) error { return nil }
	started := time.Now()
	if err := Execute(ctx, client, opts); err == nil {
		t.Fatal("blocked transport completed after cancellation")
	}
	if active.Load() != 0 || time.Since(started) > time.Second {
		t.Fatal("canceled blocked send left active producer", active.Load(), time.Since(started))
	}
}

func TestExecuteEarlyEOFJoinsWithoutWaitingForCallerDeadline(t *testing.T) {
	for _, mode := range []string{"early_eof", "early_eof_after_result"} {
		t.Run(mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: mode}
			client := clientTestConnection(t, peer)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			produced := false
			joined := make(chan struct{})
			opts := ExecuteOptions{StoreName: "records"}
			opts.Produce = func(ctx context.Context) (*Command, error) {
				if mode == "early_eof_after_result" && !produced {
					produced = true
					return clientTestRead(), nil
				}
				<-ctx.Done()
				close(joined)
				return nil, ctx.Err()
			}
			opts.Consume = func(context.Context, uint64, *Event) error { return nil }
			started := time.Now()
			if err := Execute(ctx, client, opts); err == nil {
				t.Fatal("downstream early EOF was reported as finite batch success")
			}
			if time.Since(started) > 500*time.Millisecond {
				t.Fatal("early EOF stalled while joining an uncanceled producer", time.Since(started))
			}
			select {
			case <-joined:
			default:
				t.Fatal("early EOF did not join producer")
			}
		})
	}
}

func TestExecuteCompleteRequiresEmptyTransportEnd(t *testing.T) {
	for _, mode := range []string{"normal", "missing_end", "non_ok_after_end"} {
		t.Run(mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: mode}
			client := clientTestConnection(t, peer)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			produced, consumed, completed := false, 0, 0
			opts := ExecuteOptions{StoreName: "records"}
			opts.Produce = func(context.Context) (*Command, error) {
				if produced {
					return nil, io.EOF
				}
				produced = true
				return clientTestRead(), nil
			}
			opts.Consume = func(context.Context, uint64, *Event) error { consumed++; return nil }
			opts.Complete = func(_ context.Context, id uint64) error {
				if consumed != 1 || id != 1 {
					return errors.New("Complete fired before validated business result")
				}
				completed++
				return nil
			}
			err := Execute(ctx, client, opts)
			if mode == "normal" && err != nil || mode != "normal" && err == nil {
				t.Fatal("unexpected finite RPC status", mode, err)
			}
			wantCompleted := 1
			if mode == "missing_end" {
				wantCompleted = 0
			}
			if consumed != 1 || completed != wantCompleted {
				t.Fatal("business and transport completion were conflated", mode, consumed, completed)
			}
		})
	}
}

func TestPutPreservesAppliedEvidenceWithRPCError(t *testing.T) {
	for _, mode := range []string{"write_reply_loss", "write_end_failure"} {
		t.Run(mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: mode}
			client := clientTestConnection(t, peer)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			document := &Document{MediaType: "application/json", Data: []byte(`{"n":1}`)}
			request := &WriteRequest{Resource: "records/s:key", Document: document}
			opts := WriteOptions{StoreName: "records", Request: request}
			result, err := Put(ctx, client, opts)
			if status.Code(err) != codes.Unavailable || result.GetOutcome() != MutationApplied {
				t.Fatal("lost RPC terminal erased validated write evidence or reported success", result, err)
			}
		})
	}
}

func TestExecuteEarlyRejectionPreservesAuthoritativeStatus(t *testing.T) {
	peer := &clientTestPeer{mode: "reject_early"}
	client := clientTestConnection(t, peer)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	opts := ExecuteOptions{StoreName: "records"}
	opts.Produce = func(context.Context) (*Command, error) {
		// Bounded source work can finish after the peer rejects RPC headers.
		// A subsequent Send EOF must not mask the authoritative receive status.
		time.Sleep(20 * time.Millisecond)
		return clientTestRead(), nil
	}
	opts.Consume = func(context.Context, uint64, *Event) error { return errors.New("rejected RPC returned a result") }
	if err := Execute(ctx, client, opts); status.Code(err) != codes.InvalidArgument {
		t.Fatal("Send EOF or cancellation replaced peer rejection", err)
	}
}

func TestExecuteOversizedInputDoesNotAllocateEncodedCopy(t *testing.T) {
	peer := &clientTestPeer{mode: "normal"}
	client := clientTestConnection(t, peer)
	body := make([]byte, protocol.MaxPayload+1)
	descriptor := &pb.Document{MediaType: "application/vnd.weir.search-http.v1+protobuf"}
	open := &pb.NativeOpen{Resource: "records", Descriptor_: descriptor}
	native := &pb.NativeRequest{Open: open, Body: body}
	variant := &pb.Command_Native{Native: native}
	wire := &pb.Command{Version: 1, Operation: variant}
	call := &Command{wire: wire}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	opts := ExecuteOptions{StoreName: "records"}
	opts.Produce = func(context.Context) (*Command, error) { return call, nil }
	opts.Consume = func(context.Context, uint64, *Event) error {
		return errors.New("oversized input returned a response")
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := Execute(ctx, client, opts)
	runtime.ReadMemStats(&after)
	if err == nil || peer.completed.Load() != 0 {
		t.Fatal("oversized input was transmitted", err, peer.completed.Load())
	}
	if after.TotalAlloc-before.TotalAlloc >= uint64(protocol.MaxPayload) {
		t.Fatal("SDK encoded an oversized input before rejecting it", after.TotalAlloc-before.TotalAlloc)
	}
}

func (p *clientTestPeer) scanExecute(stream pb.StoreService_ExecuteServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		return errors.New("expected scan half close")
	}
	send := func(event *pb.Event) error {
		var raw bytes.Buffer
		if _, err := protodelim.MarshalTo(&raw, event); err != nil {
			return err
		}
		frame := &pb.ExecuteResponse{RequestId: request.RequestId, EventFragment: raw.Bytes()}
		return stream.Send(frame)
	}
	count := uint64(1)
	if p.mode == "scan_overbound" {
		count = 2
	}
	for i := uint64(0); i < count; i++ {
		document := &pb.Document{MediaType: "application/json", Data: []byte(`{"n":1}`)}
		value := &pb.Event_Document{Document: document}
		event := &pb.Event{Version: 1, Value: value}
		if err := send(event); err != nil {
			return err
		}
	}
	if p.mode == "scan_missing_business_end" {
		return nil
	}
	end := &pb.ScanEnd{DocumentCount: count, NextContinuationToken: []byte("checkpoint")}
	if p.mode == "scan_business_failure" {
		end.Failure = &pb.Failure{Code: pb.FailureCode_UNAVAILABLE, Message: "backend failed the scan"}
		end.NextContinuationToken = nil
	}
	if p.mode == "scan_invalid_end" {
		end.Exhausted = true
	}
	value := &pb.Event_ScanEnd{ScanEnd: end}
	event := &pb.Event{Version: 1, Value: value}
	if err := send(event); err != nil {
		return err
	}
	if p.mode == "scan_missing_request_end" {
		return nil
	}
	frame := &pb.ExecuteResponse{RequestId: request.RequestId, RequestComplete: true}
	if err := stream.Send(frame); err != nil {
		return err
	}
	if p.mode == "scan_non_ok" {
		return status.Error(codes.Unavailable, "fixture status after page")
	}
	return nil
}

func TestScanCommitsOnlyCompleteBoundedPage(t *testing.T) {
	for _, mode := range []string{"scan_normal", "scan_missing_business_end", "scan_missing_request_end", "scan_non_ok", "scan_overbound", "scan_invalid_end", "scan_consumer_failure"} {
		t.Run(mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: mode}
			client := clientTestConnection(t, peer)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			request := &pb.ScanRequest{Resource: "records", PageSize: 1}
			consumed := 0
			opts := ScanOptions{StoreName: "search", Request: request}
			opts.Consume = func(context.Context, *pb.Document) error {
				consumed++
				if mode == "scan_consumer_failure" {
					return errors.New("fixture consumer failed")
				}
				return nil
			}
			end, err := Scan(ctx, client, opts)
			if mode == "scan_normal" {
				if err != nil || end == nil || end.DocumentCount != 1 || string(end.NextContinuationToken) != "checkpoint" || end.Exhausted || consumed != 1 {
					t.Fatal("complete page was not committed", end, err, consumed)
				}
			} else if err == nil || end != nil {
				t.Fatal("incomplete or invalid page exposed checkpoint", end, err)
			}
		})
	}
}
