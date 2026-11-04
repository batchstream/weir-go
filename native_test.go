package weir

import (
	"context"
	"errors"
	"testing"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (p *clientTestPeer) nativeExecute(stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse]) error {
	metadata := &Document{ContentType: "application/vnd.example.response", Data: []byte{0xff, 0}}
	head := &pb.NativeHead{Metadata: metadata, BodyContentType: "application/octet-stream"}
	if p.mode == "native_invalid_metadata" {
		head.Metadata.ContentType = "invalid"
	}
	if p.mode == "native_no_metadata" {
		head.Metadata = nil
	}
	if p.mode == "native_http_error" {
		head.Metadata.ContentType = "application/http"
		head.Metadata.Data = []byte("HTTP/1.1 404 Not Found\r\nX-Example: value\r\n\r\n")
	}
	headValue := &pb.Event_Head{Head: head}
	headEvent := &pb.Event{Value: headValue}
	chunkValue := &pb.Event_Chunk{Chunk: []byte("native body bytes")}
	chunkEvent := &pb.Event{Value: chunkValue}
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	if p.mode == "native_not_started" || p.mode == "native_not_started_after_head" || p.mode == "native_not_started_after_chunk" {
		end.Completion = NativeNotStarted
		end.Failure = &Failure{Code: FailureUnavailable, Message: "not started"}
	}
	if p.mode == "native_incomplete" || p.mode == "native_incomplete_without_head" {
		end.Completion = NativeResponseIncomplete
		end.Failure = &Failure{Code: FailureUnavailable, Message: "response incomplete"}
	}
	if p.mode == "native_unknown_failure" {
		end.Completion = NativeResponseIncomplete
		end.Failure = &Failure{Code: FailureCode(127), Message: "future response failure"}
	}
	if p.mode == "native_unknown_completion" {
		end.Completion = NativeCompletion(127)
		end.Failure = &Failure{Code: FailureUnavailable, Message: "unrecognized response evidence"}
	}
	endValue := &pb.Event_NativeEnd{NativeEnd: end}
	endEvent := &pb.Event{Value: endValue}
	events := []*pb.Event{headEvent, chunkEvent, endEvent}
	switch p.mode {
	case "native_empty_chunk":
		emptyValue := &pb.Event_Chunk{}
		emptyEvent := &pb.Event{Value: emptyValue}
		events = []*pb.Event{headEvent, emptyEvent, endEvent}
	case "native_empty_response", "native_not_started_after_head":
		events = []*pb.Event{headEvent, endEvent}
	case "native_not_started", "native_incomplete_without_head", "native_complete_without_head":
		events = []*pb.Event{endEvent}
	case "native_duplicate_head":
		events = []*pb.Event{headEvent, headEvent, chunkEvent, endEvent}
	case "native_chunk_before_head":
		events = []*pb.Event{chunkEvent, headEvent, endEvent}
	case "native_missing_end":
		events = []*pb.Event{headEvent, chunkEvent}
	case "native_after_end":
		events = []*pb.Event{headEvent, chunkEvent, endEvent, chunkEvent}
	case "native_wrong_kind":
		readValue := &pb.Event_ReadResult{ReadResult: protocol.Missing()}
		readEvent := &pb.Event{Value: readValue}
		events = []*pb.Event{readEvent}
	}
	for _, event := range events {
		response := &pb.ExecuteResponse{Index: 1, Event: event}
		if p.mode == "native_wrong_index" {
			response.Index = 2
		}
		if p.mode == "native_unknown_fields" || p.mode == "native_unknown_fields_final_error" {
			addResponseFields(response, event, head, metadata, end)
		}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
	if p.mode == "native_final_status_error" || p.mode == "native_unknown_fields_final_error" {
		return status.Error(codes.Unavailable, "lost Native final status")
	}
	return nil
}

func clientTestNativeRequest() *NativeRequest {
	document := &Document{ContentType: "application/vnd.example.request", Data: []byte{1}}
	request := &NativeRequest{Resource: "records", Request: document}
	return request
}

func TestNativeConsumesBytesAndPreservesResponseEvidence(t *testing.T) {
	cases := []struct {
		mode       string
		completion NativeCompletion
		chunks     int
		response   bool
		failure    bool
		rpcError   bool
	}{
		{mode: "native_normal", completion: NativeResponseComplete, chunks: 1, response: true},
		{mode: "native_no_metadata", completion: NativeResponseComplete, chunks: 1, response: true},
		{mode: "native_empty_response", completion: NativeResponseComplete, response: true},
		{mode: "native_http_error", completion: NativeResponseComplete, chunks: 1, response: true},
		{mode: "native_not_started", completion: NativeNotStarted, failure: true},
		{mode: "native_incomplete", completion: NativeResponseIncomplete, chunks: 1, response: true, failure: true},
		{mode: "native_incomplete_without_head", completion: NativeResponseIncomplete, failure: true},
		{mode: "native_final_status_error", completion: NativeResponseComplete, chunks: 1, response: true, rpcError: true},
		{mode: "native_after_end", completion: NativeResponseComplete, chunks: 1, response: true, rpcError: true},
		{mode: "native_consumer_chunk_error", completion: NativeCompletionUnconfirmed, chunks: 1, response: true, rpcError: true},
	}
	for _, test := range cases {
		t.Run(test.mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: test.mode}
			client := clientTestClient(t, peer)
			chunks := 0
			failure := errors.New("consumer rejected body")
			consume := func(_ context.Context, response *NativeResponse, data []byte) error {
				chunks++
				if string(data) != "native body bytes" {
					t.Error("body bytes changed", response, data)
				}
				if test.mode == "native_http_error" {
					httpResponse, err := ParseHTTPNativeResponse(response)
					if err != nil || httpResponse.StatusCode != 404 {
						t.Error("complete native HTTP failure lost metadata", response, err)
					}
				} else if test.mode == "native_no_metadata" {
					if response.Metadata != nil {
						t.Error("absent metadata changed", response)
					}
				} else if response.Metadata.ContentType != "application/vnd.example.response" {
					t.Error("opaque metadata changed", response)
				}
				if test.mode == "native_consumer_chunk_error" {
					return failure
				}
				return nil
			}
			result, err := client.Native(t.Context(), "records", clientTestNativeRequest(), WithChunks(consume))
			if result == nil || result.Completion != test.completion || (result.Response != nil) != test.response || (result.Failure != nil) != test.failure || chunks != test.chunks || (err != nil) != test.rpcError {
				t.Fatal("Native conflated body, completion and RPC evidence", result, err, chunks)
			}
			if test.mode == "native_consumer_chunk_error" && !errors.Is(err, failure) {
				t.Fatal("consumer cause lost", err)
			}
			if peer.received.Load() != 1 {
				t.Fatal("Native request replayed", peer.received.Load())
			}
		})
	}
}

func TestNativeSendsBackendRootRequest(t *testing.T) {
	peer := &clientTestPeer{mode: "native_no_metadata"}
	client := clientTestClient(t, peer)
	document := &Document{ContentType: "application/http", Data: []byte("GET /_cluster/health HTTP/1.1\r\n\r\n")}
	request := &NativeRequest{Request: document}
	result, err := client.Native(t.Context(), "search", request)
	if err != nil || result == nil || result.Completion != NativeResponseComplete || peer.streams.Load() != 1 || peer.received.Load() != 1 {
		t.Fatal("Native backend root request failed", result, err, peer.streams.Load(), peer.received.Load())
	}
}

func TestNativeRejectsContradictoryOrMalformedResponseEvidence(t *testing.T) {
	for _, mode := range []string{"native_empty_chunk", "native_not_started_after_head", "native_not_started_after_chunk", "native_duplicate_head", "native_chunk_before_head", "native_complete_without_head", "native_invalid_metadata", "native_unknown_completion", "native_unknown_fields", "native_unknown_fields_final_error", "native_unknown_failure", "native_missing_end", "native_wrong_kind", "native_wrong_index"} {
		t.Run(mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: mode}
			client := clientTestClient(t, peer)
			chunks := 0
			consume := func(context.Context, *NativeResponse, []byte) error { chunks++; return nil }
			result, err := client.Native(t.Context(), "records", clientTestNativeRequest(), WithChunks(consume))
			if err == nil || result != nil && result.Completion != NativeCompletionUnconfirmed {
				t.Fatal("invalid completion evidence exposed", result, err)
			}
			expectedChunks := 0
			if mode == "native_not_started_after_chunk" || mode == "native_unknown_completion" || mode == "native_unknown_failure" || mode == "native_missing_end" {
				expectedChunks = 1
			}
			if chunks != expectedChunks {
				t.Fatal("malformed body exposed", chunks)
			}
		})
	}
}

func TestNativeRejectsInvalidRequestEnvelopeBeforeRPC(t *testing.T) {
	invalid := &Document{ContentType: "invalid"}
	for _, request := range []*NativeRequest{
		{Resource: "records"},
		{Resource: "records", Request: invalid},
	} {
		peer := &clientTestPeer{}
		client := clientTestClient(t, peer)
		if result, err := client.Native(t.Context(), "records", request); err == nil || result != nil {
			t.Fatal("invalid native request envelope accepted", result, err)
		}
		if peer.streams.Load() != 0 {
			t.Fatal("invalid request opened an RPC")
		}
	}
}

func TestNativeCollectsTheBodyUnlessChunksConsumeIt(t *testing.T) {
	peer := &clientTestPeer{mode: "native_normal"}
	client := clientTestClient(t, peer)
	result, err := client.Native(t.Context(), "records", clientTestNativeRequest())
	if err != nil || string(result.Body) != "native body bytes" || result.Completion != NativeResponseComplete {
		t.Fatal("the body was not collected", result, err)
	}
	var chunks [][]byte
	consume := func(_ context.Context, _ *NativeResponse, chunk []byte) error {
		chunks = append(chunks, chunk)
		return nil
	}
	result, err = client.Native(t.Context(), "records", clientTestNativeRequest(), WithChunks(consume))
	if err != nil || result.Body != nil || len(chunks) != 1 || string(chunks[0]) != "native body bytes" {
		t.Fatal("a consumed body was also collected", result, chunks, err)
	}
}

func TestNativeKeepsTheBodyThatArrivedBeforeAnError(t *testing.T) {
	peer := &clientTestPeer{mode: "native_final_status_error"}
	client := clientTestClient(t, peer)
	result, err := client.Native(t.Context(), "records", clientTestNativeRequest())
	if status.Code(err) != codes.Unavailable || result == nil || string(result.Body) != "native body bytes" {
		t.Fatal("a later error lost the body", result, err)
	}
}

func TestNativeRequiresARequest(t *testing.T) {
	peer := &clientTestPeer{}
	client := clientTestClient(t, peer)
	if result, err := client.Native(t.Context(), "records", nil); err == nil || result != nil || peer.streams.Load() != 0 {
		t.Fatal("a call without a request was accepted", result, err)
	}
}
