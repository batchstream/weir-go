package weir

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
)

type clientTestPeer struct {
	pb.UnimplementedStoreServiceServer
	mode      string
	streams   atomic.Int64
	received  atomic.Int64
	mutations chan *pb.MutateRequest
	canceled  chan struct{}
}

func (p *clientTestPeer) Read(ctx context.Context, request *pb.ReadBatchRequest) (*pb.ReadBatchResponse, error) {
	p.streams.Add(1)
	p.received.Add(int64(len(request.Requests)))
	if p.mode == "blocked_receive" {
		<-ctx.Done()
		close(p.canceled)
		return nil, ctx.Err()
	}
	response := &pb.ReadBatchResponse{}
	for index := range request.Requests {
		document := &Document{MediaType: "application/octet-stream", Data: bytes.Repeat([]byte{37}, 257<<10)}
		if strings.HasPrefix(p.mode, "batch_") {
			document.MediaType, document.Data = "application/json", []byte(fmt.Sprintf(`{"id":%d}`, index+1))
		}
		read := protocol.ReadDocument(document)
		if p.mode == "read_missing" || strings.HasPrefix(p.mode, "batch_") && index == 1 {
			read = protocol.Missing()
		}
		if p.mode == "read_failure" || strings.HasPrefix(p.mode, "batch_") && index == 2 {
			read = protocol.ReadFailure(protocol.Fail(FailurePermissionDenied, "denied"))
		}
		response.Results = append(response.Results, read)
	}
	switch p.mode {
	case "invalid_result":
		response.Results[0] = &pb.ReadResult{}
	case "unknown_fields":
		response.Results[0].ProtoReflect().SetUnknown([]byte{0x20, 1})
	case "missing_result":
		response.Results = response.Results[:len(response.Results)-1]
	case "extra_result":
		response.Results = append(response.Results, protocol.Missing())
	case "batch_read_final_error":
		return nil, status.Error(codes.Unavailable, "lost response")
	}
	return response, nil
}

func (p *clientTestPeer) Mutate(ctx context.Context, request *pb.MutateBatchRequest) (*pb.MutateBatchResponse, error) {
	p.streams.Add(1)
	p.received.Add(int64(len(request.Requests)))
	response := &pb.MutateBatchResponse{}
	for index, item := range request.Requests {
		if p.mutations != nil {
			p.mutations <- item
		}
		result := protocol.Mutation(MutationApplied, nil)
		if strings.HasPrefix(p.mode, "batch_") && index == 1 {
			result = protocol.Mutation(MutationNotApplied, protocol.Fail(FailureConflict, "conflict"))
		}
		response.Results = append(response.Results, result)
	}
	switch p.mode {
	case "batch_partial", "write_reply_loss", "write_end_failure":
		return nil, status.Error(codes.Unavailable, "applied but response lost")
	case "invalid_outcome":
		response.Results[0].Outcome = MutationUnknown
	case "applied_failure":
		response.Results[0].Failure = protocol.Fail(FailureUnavailable, "replica acknowledgement failed")
	case "blocked_mutation":
		<-ctx.Done()
		close(p.canceled)
		return nil, ctx.Err()
	}
	return response, nil
}

func (p *clientTestPeer) Execute(request *pb.ExecuteRequest, stream pb.StoreService_ExecuteServer) error {
	p.streams.Add(1)
	p.received.Add(1)
	if p.mode == "reject_early" {
		return status.Error(codes.InvalidArgument, "fixture rejection")
	}
	if request.Command.GetNative() != nil {
		return p.nativeExecute(stream)
	}
	return p.scanExecute(stream)
}

func clientTestConnection(t *testing.T, peer *clientTestPeer) pb.StoreServiceClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(protocol.MaxBatchRequestBytes), grpc.MaxSendMsgSize(protocol.MaxBatchResponseBytes))
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

func TestReadRejectsMalformedUnaryResponseWithoutEvidence(t *testing.T) {
	for _, mode := range []string{"invalid_result", "unknown_fields", "missing_result", "extra_result"} {
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
	document := &Document{MediaType: "application/json", Data: []byte(`{}`)}
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
		document := &Document{MediaType: "application/json", Data: []byte(`{"n":1}`)}
		value := &pb.Event_Document{Document: document}
		event := &pb.Event{Version: 1, Value: value}
		frame := &pb.ExecuteResponse{Event: event}
		if p.mode == "scan_unknown_fields" {
			event.ProtoReflect().SetUnknown([]byte{0x10, 1})
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
	event := &pb.Event{Version: 1, Value: value}
	frame := &pb.ExecuteResponse{Event: event}
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
