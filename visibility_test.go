package weir

import (
	"context"
	"errors"
	"io"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func TestMutateStreamPreservesPerRecordVisibility(t *testing.T) {
	peer := &clientTestPeer{mode: "typed_mutation", mutations: make(chan *pb.MutateRequest, 2)}
	client := clientTestConnection(t, peer)
	expression := &Document{ContentType: "application/json", Data: []byte(`{"doc":{"n":1}}`)}
	first := &MutateRequest{Resource: "records/s:first", Action: MutationAtomicTransform, BackendExpression: expression, WaitUntilVisible: true}
	second := &MutateRequest{Resource: "records/s:second", Action: MutationDelete}
	requests := []*MutateRequest{first, second}
	produced, consumed := 0, uint64(0)
	options := MutateStreamOptions{StoreName: "records"}
	options.Next = func(context.Context) (*MutateRequest, error) {
		if produced == len(requests) {
			return nil, io.EOF
		}
		request := requests[produced]
		produced++
		return request, nil
	}
	options.Consume = func(_ context.Context, index uint64, result *MutationResult) error {
		consumed++
		if index != consumed || result.GetOutcome() != MutationApplied || result.Failure != nil {
			return errors.New("mutation evidence was lost or reordered")
		}
		return nil
	}
	if err := MutateStream(t.Context(), client, options); err != nil {
		t.Fatal(err)
	}
	for _, request := range requests {
		mutation := <-peer.mutations
		if mutation.Resource != request.Resource || mutation.WaitUntilVisible != request.WaitUntilVisible {
			t.Fatal("stream changed per-record visibility semantics", mutation, request)
		}
	}
	if peer.streams.Load() != 1 || peer.received.Load() != 2 || consumed != 2 {
		t.Fatal("visibility requests were split, replayed or left unconfirmed", peer.streams.Load(), peer.received.Load(), consumed)
	}
}

func TestNativeBackendRootUsesExecuteV2(t *testing.T) {
	peer := &clientTestPeer{mode: "native_no_metadata"}
	client := clientTestConnection(t, peer)
	document := &Document{ContentType: "application/http", Data: []byte("GET /_cluster/health HTTP/1.1\r\n\r\n")}
	request := &NativeRequest{Request: document}
	options := NativeOptions{StoreName: "search", Request: request}
	options.Consume = func(context.Context, *NativeResponse, []byte) error { return nil }
	result, err := Native(t.Context(), client, options)
	if err != nil || result == nil || result.Completion != NativeResponseComplete || peer.streams.Load() != 1 || peer.received.Load() != 1 {
		t.Fatal("Native backend root did not reach ExecuteV2", result, err, peer.streams.Load(), peer.received.Load())
	}
}
