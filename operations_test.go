package weir

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReadPreservesMissingAndBackendFailure(t *testing.T) {
	for _, mode := range []string{"read_missing", "read_failure", "read_target_missing"} {
		t.Run(mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: mode}
			client := clientTestConnection(t, peer)
			options := ReadOneOptions{StoreName: "records", Request: clientTestReadRequest()}
			result, err := ReadOne(t.Context(), client, options)
			if err != nil || result == nil || result.Document != nil {
				t.Fatal("Read lost backend result", result, err)
			}
			if mode == "read_missing" && (!result.Missing || result.Failure != nil) {
				t.Fatal("Read changed missing into failure", result)
			}
			if mode == "read_failure" && (result.Missing || result.Failure.GetCode() != FailurePermissionDenied) {
				t.Fatal("Read lost failure evidence", result)
			}
			if mode == "read_target_missing" && (result.Missing || result.Failure.GetCode() != FailureTargetNotFound) {
				t.Fatal("missing backend target became successful document absence", result)
			}
		})
	}
}

func TestScanPreservesBackendFailureWithoutCheckpoint(t *testing.T) {
	peer := &clientTestPeer{mode: "scan_business_failure"}
	client := clientTestConnection(t, peer)
	request := &ScanRequest{Resource: "records", PageSize: 1}
	options := ScanOptions{StoreName: "records", Request: request}
	options.Consume = func(context.Context, *Document) error { return nil }
	end, err := Scan(t.Context(), client, options)
	if err != nil || end.GetFailure().GetCode() != FailureUnavailable || end.Exhausted || len(end.NextContinuationToken) != 0 {
		t.Fatal("Scan lost backend failure or exposed a checkpoint", end, err)
	}
}

func TestTypedMutationsSelectBackendOperation(t *testing.T) {
	peer := &clientTestPeer{mode: "typed_mutation", mutations: make(chan *pb.MutateRequest, 5)}
	client := clientTestConnection(t, peer)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	document := &Document{ContentType: "application/json", Data: []byte(`{"n":1}`)}
	request := &WriteRequest{Resource: "records/s:key", Document: document}
	write := WriteOptions{StoreName: "records", Request: request}
	for _, action := range []string{"create", "put", "replace", "delete", "transform"} {
		var result *MutationResult
		var err error
		switch action {
		case "create":
			result, err = Create(ctx, client, write)
		case "put":
			result, err = Put(ctx, client, write)
		case "replace":
			result, err = Replace(ctx, client, write)
		case "delete":
			request := &DeleteRequest{Resource: "records/s:key"}
			options := DeleteOptions{StoreName: "records", Request: request}
			result, err = Delete(ctx, client, options)
		case "transform":
			program := &LuaTransform{Source: []byte("return doc")}
			request := &AtomicTransformRequest{Resource: "records/s:key", Lua: program}
			options := AtomicTransformOptions{StoreName: "records", Request: request}
			result, err = AtomicTransform(ctx, client, options)
		}
		if err != nil || result.GetOutcome() != MutationApplied {
			t.Fatal(action, result, err)
		}
		mutation := <-peer.mutations
		if mutation.Resource != request.Resource {
			t.Fatal("typed operation lost its resource", action, mutation)
		}
		selected := false
		switch action {
		case "create":
			selected = mutation.GetCreate() != nil
		case "put":
			selected = mutation.GetPut() != nil
		case "replace":
			selected = mutation.GetReplace() != nil
		case "delete":
			selected = mutation.GetDelete() != nil
		case "transform":
			selected = mutation.GetAtomicTransform().GetLua() != nil
		}
		if !selected {
			t.Fatal("SDK executed a different mutation", action, mutation)
		}
	}
	if peer.received.Load() != 5 {
		t.Fatal("typed mutations were replayed", peer.received.Load())
	}
}

func TestAtomicTransformRejectsAmbiguousFormsBeforeBusinessSend(t *testing.T) {
	for _, both := range []bool{false, true} {
		t.Run(map[bool]string{false: "neither", true: "both"}[both], func(t *testing.T) {
			peer := &clientTestPeer{mode: "typed_mutation"}
			client := clientTestConnection(t, peer)
			request := &AtomicTransformRequest{Resource: "records/s:key"}
			if both {
				request.Lua = &LuaTransform{Source: []byte("return doc")}
				request.BackendExpression = &Document{ContentType: "application/json", Data: []byte(`{}`)}
			}
			options := AtomicTransformOptions{StoreName: "records", Request: request}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if result, err := AtomicTransform(ctx, client, options); err == nil || result != nil {
				t.Fatal("invalid transform was not rejected", result, err)
			}
			if peer.received.Load() != 0 {
				t.Fatal("invalid transform sent business data", peer.received.Load())
			}
		})
	}
}

func (p *clientTestPeer) nativeExecute(stream pb.StoreService_ExecuteServer) error {
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
	chunkValue := &pb.Event_Chunk{Chunk: []byte("bounded native bytes")}
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
	}
	for _, event := range events {
		response := &pb.ExecuteResponse{Index: 1, Event: event}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
	if p.mode == "native_final_status_error" {
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
		{mode: "native_consumer_chunk_error", completion: NativeCompletionUnconfirmed, chunks: 1, response: true, rpcError: true},
	}
	for _, test := range cases {
		t.Run(test.mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: test.mode}
			client := clientTestConnection(t, peer)
			options := NativeOptions{StoreName: "records", Request: clientTestNativeRequest()}
			chunks := 0
			failure := errors.New("consumer rejected body")
			options.Consume = func(_ context.Context, response *NativeResponse, data []byte) error {
				chunks++
				if string(data) != "bounded native bytes" {
					t.Fatal("body bytes changed", response, data)
				}
				if test.mode == "native_http_error" {
					httpResponse, err := ParseHTTPNativeResponse(response)
					if err != nil || httpResponse.StatusCode != 404 {
						t.Fatal("complete native HTTP failure lost metadata", response, err)
					}
				} else if test.mode == "native_no_metadata" {
					if response.Metadata != nil {
						t.Fatal("absent metadata changed", response)
					}
				} else if response.Metadata.ContentType != "application/vnd.example.response" {
					t.Fatal("opaque metadata changed", response)
				}
				if test.mode == "native_consumer_chunk_error" {
					return failure
				}
				return nil
			}
			result, err := Native(t.Context(), client, options)
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

func TestNativeRejectsContradictoryOrMalformedResponseEvidence(t *testing.T) {
	for _, mode := range []string{"native_empty_chunk", "native_not_started_after_head", "native_not_started_after_chunk", "native_duplicate_head", "native_chunk_before_head", "native_complete_without_head", "native_invalid_metadata"} {
		t.Run(mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: mode}
			client := clientTestConnection(t, peer)
			options := NativeOptions{StoreName: "records", Request: clientTestNativeRequest()}
			chunks := 0
			options.Consume = func(context.Context, *NativeResponse, []byte) error { chunks++; return nil }
			result, err := Native(t.Context(), client, options)
			if err == nil || result != nil && result.Completion != NativeCompletionUnconfirmed {
				t.Fatal("invalid completion evidence exposed", result, err)
			}
			expectedChunks := 0
			if mode == "native_not_started_after_chunk" {
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
		client := clientTestConnection(t, peer)
		options := NativeOptions{StoreName: "records", Request: request}
		options.Consume = func(context.Context, *NativeResponse, []byte) error { return nil }
		if result, err := Native(t.Context(), client, options); err == nil || result != nil {
			t.Fatal("invalid native request envelope accepted", result, err)
		}
		if peer.streams.Load() != 0 {
			t.Fatal("invalid request opened an RPC")
		}
	}
}
