package weir

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type clientTestPeer struct {
	mode      string
	streams   atomic.Int64
	maxBytes  atomic.Int64
	received  atomic.Int64
	mutations chan *pb.MutateRequest
	canceled  chan struct{}
}

func (p *clientTestPeer) ResolveStore(context.Context, *pb.ResolveStoreRequest) (*pb.ResolveStoreResponse, error) {
	return nil, status.Error(codes.Unimplemented, "fixture serves business requests only")
}

func (p *clientTestPeer) Execute(stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse]) error {
	p.streams.Add(1)
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			if p.mode == "batch_read_final_error" || p.mode == "read_unknown_fields_final_error" {
				return status.Error(codes.Unavailable, "lost final status")
			}
			return nil
		}
		if err != nil {
			return err
		}
		if err := protocol.ValidateExecuteRequest(request); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		requestBytes := int64(proto.Size(request))
		for previous := p.maxBytes.Load(); requestBytes > previous; previous = p.maxBytes.Load() {
			if p.maxBytes.CompareAndSwap(previous, requestBytes) {
				break
			}
		}
		if p.mode == "early_eof" {
			return nil
		}
		switch {
		case request.Command.GetRead() != nil:
			if err := p.readRecord(request, stream); err != nil {
				return err
			}
		case request.Command.GetMutate() != nil:
			if err := p.mutationRecord(request, stream); err != nil {
				return err
			}
		default:
			p.received.Add(1)
			if _, err := stream.Recv(); err != io.EOF {
				return status.Error(codes.InvalidArgument, "single request did not close its input")
			}
			return p.nativeExecute(stream)
		}
	}
}

func (p *clientTestPeer) readRecord(request *pb.ExecuteRequest, stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse]) error {
	p.received.Add(1)
	if p.mode == "blocked_receive" {
		<-stream.Context().Done()
		close(p.canceled)
		return stream.Context().Err()
	}
	index := request.Index
	document := &Document{ContentType: "application/octet-stream", Data: bytes.Repeat([]byte{37}, 257<<10)}
	if p.mode == "large_duplex" {
		document.Data = bytes.Repeat([]byte{37}, 1<<20)
	}
	if strings.HasPrefix(p.mode, "batch_") {
		document.ContentType, document.Data = "application/json", []byte(fmt.Sprintf(`{"id":%d}`, index))
	}
	read := protocol.ReadDocument(document)
	if p.mode == "read_missing" || strings.HasPrefix(p.mode, "batch_") && index == 2 {
		read = protocol.Missing()
	}
	if p.mode == "read_failure" || strings.HasPrefix(p.mode, "batch_") && index == 3 {
		read = protocol.ReadFailure(protocol.Fail(FailurePermissionDenied, "denied"))
	}
	if p.mode == "read_target_missing" {
		read = protocol.ReadFailure(protocol.Fail(FailureTargetNotFound, "backend target missing"))
	}
	switch p.mode {
	case "invalid_result":
		read = &pb.ReadResult{}
	case "unknown_read_variant":
		read = &pb.ReadResult{}
		addResponseFields(read)
	case "read_unknown_failure":
		read = protocol.ReadFailure(protocol.Fail(FailureCode(127), "undeclared failure"))
	case "read_negative_failure":
		read = protocol.ReadFailure(protocol.Fail(FailureCode(-1), "invalid failure"))
	case "read_unknown_missing":
		read = protocol.Missing()
		addResponseFields(read.GetMissing())
	case "missing_result":
		return nil
	}
	value := &pb.Event_ReadResult{ReadResult: read}
	event := &pb.Event{Value: value}
	response := &pb.ExecuteResponse{Index: index, Event: event}
	if strings.HasPrefix(p.mode, "read_unknown_") && p.mode != "read_unknown_failure" {
		addResponseFields(response, event, read)
		if read.GetDocument() != nil {
			addResponseFields(read.GetDocument())
		}
	}
	switch p.mode {
	case "zero_index":
		response.Index = 0
	case "skipped_index":
		response.Index++
	case "wrong_kind":
		response.Event.Value = &pb.Event_MutationResult{MutationResult: protocol.Mutation(MutationApplied, nil)}
	case "unknown_event_variant":
		response.Event.Value = nil
		addResponseFields(event)
	}
	if err := stream.Send(response); err != nil {
		return err
	}
	if p.mode == "duplicate_index" {
		return stream.Send(response)
	}
	if p.mode == "extra_result" && index == 2 {
		response.Index++
		return stream.Send(response)
	}
	return nil
}

func (p *clientTestPeer) mutationRecord(request *pb.ExecuteRequest, stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse]) error {
	p.received.Add(1)
	if p.mode == "blocked_mutation" {
		<-stream.Context().Done()
		close(p.canceled)
		return stream.Context().Err()
	}
	if p.mutations != nil {
		p.mutations <- request.Command.GetMutate()
	}
	if p.mode == "write_reply_loss" {
		return status.Error(codes.Unavailable, "applied but response lost")
	}
	result := protocol.Mutation(MutationApplied, nil)
	if strings.HasPrefix(p.mode, "batch_") && request.Index == 2 {
		result = protocol.Mutation(MutationNotApplied, protocol.Fail(FailureConflict, "conflict"))
	}
	switch p.mode {
	case "invalid_outcome":
		result.Outcome = MutationUnknown
	case "unknown_outcome":
		result.Outcome = MutationOutcome(127)
		result.Failure = protocol.Fail(FailureUnavailable, "unrecognized application evidence")
	case "negative_failure":
		result.Failure = protocol.Fail(FailureCode(-1), "invalid failure")
	case "applied_failure", "applied_known_failure", "applied_known_failure_later_error":
		result.Failure = protocol.Fail(FailureUnavailable, "replica acknowledgement failed")
	case "applied_unknown_failure", "applied_unknown_failure_later_error":
		result.Failure = protocol.Fail(FailureCode(127), "undeclared acknowledgement failure")
	}
	if p.mode == "applied_then_unknown_code" && request.Index == 2 {
		result.Failure = protocol.Fail(FailureCode(127), "undeclared failure")
	}
	value := &pb.Event_MutationResult{MutationResult: result}
	event := &pb.Event{Value: value}
	response := &pb.ExecuteResponse{Index: request.Index, Event: event}
	if p.mode == "applied_then_unknown_fields" && request.Index == 2 {
		addResponseFields(response)
	}
	if strings.HasPrefix(p.mode, "applied_unknown_failure") {
		addResponseFields(response, event, result, result.Failure)
	}
	if err := stream.Send(response); err != nil {
		return err
	}
	if p.mode == "batch_partial" || p.mode == "applied_unknown_failure_later_error" || p.mode == "applied_known_failure_later_error" {
		return status.Error(codes.Unavailable, "later mutation response lost")
	}
	return nil
}

// Field 127 is undeclared in the response schema. These
// bytes travel through the real protobuf codec before SDK validation.
func addResponseFields(messages ...proto.Message) {
	for _, message := range messages {
		message.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 1})
	}
}

func clientTestConnection(t *testing.T, peer *clientTestPeer) pb.StoreServiceClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(protocol.MaxTransportMessageBytes), grpc.MaxSendMsgSize(protocol.MaxTransportMessageBytes))
	pb.RegisterStoreServiceServer(server, peer)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	connection, err := Dial("passthrough:///" + listener.Addr().String())
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

func TestReadRejectsMalformedStreamWithoutEvidence(t *testing.T) {
	for _, mode := range []string{"invalid_result", "unknown_read_variant", "unknown_event_variant", "read_negative_failure", "missing_result", "zero_index", "skipped_index", "wrong_kind"} {
		t.Run(mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: mode}
			client := clientTestConnection(t, peer)
			options := ReadOneOptions{StoreName: "records", Request: clientTestReadRequest()}
			result, err := ReadOne(t.Context(), client, options)
			if err == nil || result != nil {
				t.Fatal("unvalidated evidence exposed", result, err)
			}
		})
	}
}

func TestReadRejectsUnknownFieldsAndFailureCodes(t *testing.T) {
	for _, mode := range []string{"read_unknown_fields", "read_unknown_fields_final_error", "read_unknown_missing", "read_unknown_failure"} {
		t.Run(mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: mode}
			client := clientTestConnection(t, peer)
			options := ReadOneOptions{StoreName: "records", Request: clientTestReadRequest()}
			result, err := ReadOne(t.Context(), client, options)
			if err == nil || result != nil {
				t.Fatal("unknown response accepted as Read evidence", result, err)
			}
			if peer.streams.Load() != 1 || peer.received.Load() != 1 {
				t.Fatal("malformed response caused read replay")
			}
		})
	}
}

func TestMutationKeepsAppliedEvidenceWithKnownFailureAndLaterError(t *testing.T) {
	for _, mode := range []string{"applied_known_failure", "applied_known_failure_later_error"} {
		t.Run(mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: mode}
			client := clientTestConnection(t, peer)
			request := &MutateRequest{Resource: "records/s:key", Action: MutationDelete}
			options := MutateOptions{StoreName: "records", Requests: []*MutateRequest{request, request}}
			results, err := Mutate(t.Context(), client, options)
			if len(results) != 2 || results[0].GetOutcome() != MutationApplied || results[0].GetFailure().GetCode() != FailureUnavailable {
				t.Fatal("confirmed application evidence lost", results, err)
			}
			if mode == "applied_known_failure_later_error" {
				if status.Code(err) != codes.Unavailable || results[1] != nil || peer.received.Load() != 1 {
					t.Fatal("later failure acknowledged or replayed another mutation", results, err, peer.received.Load())
				}
			} else if err != nil || results[1].GetOutcome() != MutationApplied || results[1].GetFailure().GetCode() != FailureUnavailable || peer.received.Load() != 2 {
				t.Fatal("known acknowledgement failure rejected", results, err)
			}
			if peer.streams.Load() != 1 {
				t.Fatal("mutation replayed")
			}
		})
	}
}

func TestMutationRetainsConfirmedPrefixBeforeUnknownResponse(t *testing.T) {
	for _, mode := range []string{"applied_then_unknown_fields", "applied_then_unknown_code"} {
		t.Run(mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: mode}
			client := clientTestConnection(t, peer)
			request := &MutateRequest{Resource: "records/s:key", Action: MutationDelete}
			options := MutateOptions{StoreName: "records", Requests: []*MutateRequest{request, request}}
			results, err := Mutate(t.Context(), client, options)
			if err == nil || len(results) != 2 || results[0].GetOutcome() != MutationApplied || results[1] != nil {
				t.Fatal("malformed later response changed confirmed prefix", results, err)
			}
			if peer.streams.Load() != 1 || peer.received.Load() != 2 {
				t.Fatal("malformed mutation was replayed", peer.streams.Load(), peer.received.Load())
			}
		})
	}
}

func TestPutLostResponseHasNoAcknowledgementOrReplay(t *testing.T) {
	peer := &clientTestPeer{mode: "write_reply_loss"}
	client := clientTestConnection(t, peer)
	document := &Document{ContentType: "application/json", Data: []byte(`{}`)}
	request := &WriteRequest{Resource: "records/s:key", Document: document}
	options := WriteOptions{StoreName: "records", Request: request}
	result, err := Put(t.Context(), client, options)
	if status.Code(err) != codes.Unavailable || result != nil || peer.streams.Load() != 1 || peer.received.Load() != 1 {
		t.Fatal("lost response confirmed or replayed mutation", result, err, peer.streams.Load(), peer.received.Load())
	}
}

func TestMutationRejectsInvalidOutcomeAndKeepsAppliedFailure(t *testing.T) {
	for _, mode := range []string{"invalid_outcome", "unknown_outcome", "negative_failure", "applied_failure", "applied_unknown_failure", "applied_unknown_failure_later_error"} {
		t.Run(mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: mode}
			client := clientTestConnection(t, peer)
			request := &DeleteRequest{Resource: "records/s:key"}
			options := DeleteOptions{StoreName: "records", Request: request}
			result, err := Delete(t.Context(), client, options)
			if mode != "applied_failure" {
				if err == nil || result != nil {
					t.Fatal("unqualified UNKNOWN accepted", result, err)
				}
			} else if err != nil || result.GetOutcome() != MutationApplied || result.GetFailure().GetCode() != FailureUnavailable {
				t.Fatal("APPLIED backend evidence lost", result, err)
			}
		})
	}
}

func TestMutationCancellationStopsCallWithoutReplay(t *testing.T) {
	peer := &clientTestPeer{mode: "blocked_mutation", canceled: make(chan struct{})}
	client := clientTestConnection(t, peer)
	request := &DeleteRequest{Resource: "records/s:key"}
	options := DeleteOptions{StoreName: "records", Request: request}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	result, err := Delete(ctx, client, options)
	if err == nil || result != nil {
		t.Fatal("blocked mutation succeeded", result, err)
	}
	select {
	case <-peer.canceled:
	case <-time.After(time.Second):
		t.Fatal("backend context was not canceled")
	}
	if peer.received.Load() != 1 {
		t.Fatal("canceled mutation replayed")
	}
}
