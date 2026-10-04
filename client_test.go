package weir

import (
	"bytes"
	"context"
	"errors"
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
	pb.UnimplementedStoreServiceServer
	mode      string
	streams   atomic.Int64
	maxBytes  atomic.Int64
	received  atomic.Int64
	mutations chan *pb.MutateRequest
	canceled  chan struct{}
}

func (p *clientTestPeer) Execute(stream pb.StoreService_ExecuteServer) error {
	p.streams.Add(1)
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			if p.mode == "batch_read_final_error" {
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
		if p.mode == "reject_early" {
			return status.Error(codes.InvalidArgument, "fixture rejection")
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
			if request.Command.GetNative() != nil {
				return p.nativeExecute(stream)
			}
			return p.scanExecute(stream)
		}
	}
}

func (p *clientTestPeer) readRecord(request *pb.ExecuteRequest, stream pb.StoreService_ExecuteServer) error {
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
	switch p.mode {
	case "invalid_result":
		read = &pb.ReadResult{}
	case "unknown_fields":
		read.ProtoReflect().SetUnknown([]byte{0x20, 1})
	case "missing_result":
		return nil
	}
	value := &pb.Event_ReadResult{ReadResult: read}
	event := &pb.Event{Value: value}
	response := &pb.ExecuteResponse{Index: index, Event: event}
	switch p.mode {
	case "zero_index":
		response.Index = 0
	case "skipped_index":
		response.Index++
	case "wrong_kind":
		response.Event.Value = &pb.Event_MutationResult{MutationResult: protocol.Mutation(MutationApplied, nil)}
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

func (p *clientTestPeer) mutationRecord(request *pb.ExecuteRequest, stream pb.StoreService_ExecuteServer) error {
	p.received.Add(1)
	if p.mode == "blocked_mutation" {
		<-stream.Context().Done()
		close(p.canceled)
		return stream.Context().Err()
	}
	if p.mutations != nil {
		p.mutations <- request.Command.GetMutate()
	}
	if p.mode == "write_reply_loss" || p.mode == "write_end_failure" {
		return status.Error(codes.Unavailable, "applied but response lost")
	}
	result := protocol.Mutation(MutationApplied, nil)
	if strings.HasPrefix(p.mode, "batch_") && request.Index == 2 {
		result = protocol.Mutation(MutationNotApplied, protocol.Fail(FailureConflict, "conflict"))
	}
	switch p.mode {
	case "invalid_outcome":
		result.Outcome = MutationUnknown
	case "applied_failure":
		result.Failure = protocol.Fail(FailureUnavailable, "replica acknowledgement failed")
	}
	value := &pb.Event_MutationResult{MutationResult: result}
	event := &pb.Event{Value: value}
	response := &pb.ExecuteResponse{Index: request.Index, Event: event}
	if err := stream.Send(response); err != nil {
		return err
	}
	if p.mode == "batch_partial" {
		return status.Error(codes.Unavailable, "later mutation response lost")
	}
	return nil
}

func clientTestConnection(t *testing.T, peer *clientTestPeer) pb.StoreServiceClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(protocol.MaxExecuteRequestBytes), grpc.MaxSendMsgSize(protocol.MaxExecuteResponseBytes))
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
	for _, mode := range []string{"invalid_result", "unknown_fields", "missing_result", "zero_index", "skipped_index", "wrong_kind"} {
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
	for _, mode := range []string{"invalid_outcome", "applied_failure"} {
		t.Run(mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: mode}
			client := clientTestConnection(t, peer)
			request := &DeleteRequest{Resource: "records/s:key"}
			options := DeleteOptions{StoreName: "records", Request: request}
			result, err := Delete(t.Context(), client, options)
			if mode == "invalid_outcome" {
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

func (p *clientTestPeer) scanExecute(stream pb.StoreService_ExecuteServer) error {
	count := uint64(1)
	if p.mode == "scan_overbound" {
		count = 2
	}
	for range count {
		document := &Document{ContentType: "application/json", Data: []byte(`{"n":1}`)}
		value := &pb.Event_Document{Document: document}
		event := &pb.Event{Value: value}
		frame := &pb.ExecuteResponse{Index: 1, Event: event}
		if p.mode == "scan_unknown_fields" {
			event.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 1})
		}
		if err := stream.Send(frame); err != nil {
			return err
		}
	}
	if p.mode == "scan_missing_business_end" {
		return nil
	}
	end := &ScanEnd{DocumentCount: count, NextContinuationToken: []byte("checkpoint")}
	if p.mode == "scan_business_failure" {
		end.Failure = protocol.Fail(FailureUnavailable, "backend failed the Scan")
		end.NextContinuationToken = nil
	}
	if p.mode == "scan_invalid_end" {
		end.Exhausted = true
	}
	if p.mode == "scan_bad_count" {
		end.DocumentCount++
	}
	value := &pb.Event_ScanEnd{ScanEnd: end}
	event := &pb.Event{Value: value}
	frame := &pb.ExecuteResponse{Index: 1, Event: event}
	if err := stream.Send(frame); err != nil {
		return err
	}
	if p.mode == "scan_after_end" {
		return stream.Send(frame)
	}
	if p.mode == "scan_non_ok" {
		return status.Error(codes.Unavailable, "lost final status")
	}
	return nil
}

func TestScanCommitsOnlyCompleteBoundedPage(t *testing.T) {
	for _, mode := range []string{"scan_normal", "scan_missing_business_end", "scan_non_ok", "scan_overbound", "scan_invalid_end", "scan_bad_count", "scan_unknown_fields", "scan_after_end", "scan_consumer_failure"} {
		t.Run(mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: mode}
			client := clientTestConnection(t, peer)
			request := &ScanRequest{Resource: "records", PageSize: 1}
			consumed := 0
			options := ScanOptions{StoreName: "search", Request: request}
			options.Consume = func(context.Context, *Document) error {
				consumed++
				if mode == "scan_consumer_failure" {
					return errors.New("consumer failed")
				}
				return nil
			}
			end, err := Scan(t.Context(), client, options)
			if mode == "scan_normal" {
				if err != nil || end.GetDocumentCount() != 1 || string(end.NextContinuationToken) != "checkpoint" || consumed != 1 {
					t.Fatal("complete page lost", end, err, consumed)
				}
			} else if err == nil || end != nil {
				t.Fatal("invalid page exposed checkpoint", end, err)
			}
		})
	}
}
