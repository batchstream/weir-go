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
	for _, mode := range []string{"read_missing", "read_failure"} {
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
			program := &ProgramTransform{Runtime: "lua.v1", Source: []byte("return doc")}
			request := &AtomicTransformRequest{Resource: "records/s:key", Program: program}
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
			selected = mutation.GetAtomicTransform().GetProgram() != nil
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
				request.Program = &ProgramTransform{Runtime: "lua.v1", Source: []byte("return doc")}
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
	head := &pb.NativeHead{BodyContentType: "application/octet-stream"}
	headValue := &pb.Event_Head{Head: head}
	headEvent := &pb.Event{Value: headValue}
	emptyValue := &pb.Event_Chunk{}
	emptyEvent := &pb.Event{Value: emptyValue}
	chunkValue := &pb.Event_Chunk{Chunk: []byte("bounded native bytes")}
	chunkEvent := &pb.Event{Value: chunkValue}
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	endValue := &pb.Event_NativeEnd{NativeEnd: end}
	endEvent := &pb.Event{Value: endValue}
	events := []*pb.Event{headEvent, chunkEvent, endEvent}
	if p.mode == "native_empty_chunk" {
		events = []*pb.Event{headEvent, emptyEvent, endEvent}
	}
	for _, event := range events {
		frame := &pb.ExecuteResponse{Index: 1, Event: event}
		if err := stream.Send(frame); err != nil {
			return err
		}
	}
	if p.mode == "native_final_status_error" {
		return status.Error(codes.Unavailable, "lost Native final status")
	}
	return nil
}

func TestNativeIncrementalEventsPreserveTerminalEvidence(t *testing.T) {
	for _, mode := range []string{"native_normal", "native_final_status_error", "native_consumer_terminal_error"} {
		t.Run(mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: mode}
			client := clientTestConnection(t, peer)
			descriptor := &Document{ContentType: "application/vnd.weir.search-http.v1+protobuf"}
			request := &NativeRequest{Resource: "records", Descriptor: descriptor}
			chunks, heads, terminals, bytes := 0, 0, 0, 0
			failure := errors.New("terminal consumer rejected output")
			options := NativeOptions{StoreName: "records", Request: request}
			options.Consume = func(_ context.Context, event *Event) error {
				if event.Head != nil {
					heads++
				}
				if event.Chunk != nil {
					chunks++
					bytes += len(event.Chunk)
				}
				if event.NativeEnd != nil {
					terminals++
					if mode == "native_consumer_terminal_error" {
						return failure
					}
				}
				return nil
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			end, err := Native(ctx, client, options)
			if end.GetCompletion() != NativeResponseComplete || heads != 1 || chunks != 1 || terminals != 1 || bytes != len("bounded native bytes") {
				t.Fatal("Native incremental evidence was lost", end, err, heads, chunks, terminals, bytes)
			}
			if mode == "native_normal" && err != nil || mode != "native_normal" && err == nil {
				t.Fatal("Native conflated evidence and RPC success", err)
			}
			if mode == "native_consumer_terminal_error" && !errors.Is(err, failure) {
				t.Fatal("Native lost callback error", err)
			}
			if peer.received.Load() != 1 {
				t.Fatal("Native request was replayed", peer.received.Load())
			}
		})
	}
}

func TestNativeRejectsEmptyWireChunk(t *testing.T) {
	peer := &clientTestPeer{mode: "native_empty_chunk"}
	client := clientTestConnection(t, peer)
	descriptor := &Document{ContentType: "application/vnd.weir.search-http.v1+protobuf"}
	request := &NativeRequest{Resource: "records", Descriptor: descriptor}
	chunks := 0
	options := NativeOptions{StoreName: "records", Request: request}
	options.Consume = func(_ context.Context, event *Event) error {
		if event.Chunk != nil {
			chunks++
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	end, err := Native(ctx, client, options)
	if err == nil || end != nil || chunks != 0 {
		t.Fatal("invalid empty wire chunk was exposed", end, err, chunks)
	}
}
