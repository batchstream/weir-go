package weir

import (
	"context"
	"errors"
	"io"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/protobuf/proto"
)

func TestMutateStreamPreservesPerRecordVisibility(t *testing.T) {
	peer := &clientTestPeer{mode: "typed_mutation", mutations: make(chan *pb.MutateRequest, 2)}
	client := clientTestConnection(t, peer)
	expression := &Document{ContentType: "application/json", Data: []byte(`{"doc":{"n":1}}`)}
	completion := &MutationOptions{Visibility: MutationQueryVisible}
	first := &MutateRequest{Resource: "records/s:first", Action: MutationAtomicTransform, BackendExpression: expression, Options: completion}
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
		if mutation.Resource != request.Resource || !proto.Equal(mutation.Options, request.Options) {
			t.Fatal("stream changed complete per-record options", mutation, request)
		}
	}
	if peer.streams.Load() != 1 || peer.received.Load() != 2 || consumed != 2 {
		t.Fatal("visibility requests were split, replayed or left unconfirmed", peer.streams.Load(), peer.received.Load(), consumed)
	}
}

func TestNativeBackendRootUsesExecute(t *testing.T) {
	peer := &clientTestPeer{mode: "native_no_metadata"}
	client := clientTestConnection(t, peer)
	document := &Document{ContentType: "application/http", Data: []byte("GET /_cluster/health HTTP/1.1\r\n\r\n")}
	request := &NativeRequest{Request: document}
	options := NativeOptions{StoreName: "search", Request: request}
	options.Consume = func(context.Context, *NativeResponse, []byte) error { return nil }
	result, err := Native(t.Context(), client, options)
	if err != nil || result == nil || result.Completion != NativeResponseComplete || peer.streams.Load() != 1 || peer.received.Load() != 1 {
		t.Fatal("Native backend root did not reach Execute", result, err, peer.streams.Load(), peer.received.Load())
	}
}

func TestMutationOptionsRejectInvalidBatchBeforeRPC(t *testing.T) {
	unknown := &MutationOptions{Visibility: MutationQueryVisible}
	unknown.ProtoReflect().SetUnknown([]byte{0x10, 1})
	negative := &MutationOptions{Visibility: MutationVisibility(-1)}
	undeclared := &MutationOptions{Visibility: MutationVisibility(2)}
	for _, completion := range []*MutationOptions{negative, undeclared, unknown} {
		peer := &clientTestPeer{mode: "typed_mutation"}
		client := clientTestConnection(t, peer)
		first := &MutateRequest{Resource: "records/s:first", Action: MutationDelete}
		second := &MutateRequest{Resource: "records/s:second", Action: MutationDelete, Options: completion}
		options := MutateOptions{StoreName: "records", Requests: []*MutateRequest{first, second}}
		results, err := Mutate(t.Context(), client, options)
		if err == nil || results != nil || peer.streams.Load() != 0 || peer.received.Load() != 0 {
			t.Fatal("invalid completion option sent part of a batch", completion, results, err, peer.streams.Load(), peer.received.Load())
		}
	}
}

func TestMutateStreamInvalidOptionsKeepConfirmedPrefix(t *testing.T) {
	peer := &clientTestPeer{mode: "typed_mutation"}
	client := clientTestConnection(t, peer)
	completion := &MutationOptions{Visibility: MutationVisibility(2)}
	confirmed := make(chan struct{})
	produced, consumed := 0, uint64(0)
	options := MutateStreamOptions{StoreName: "records"}
	options.Next = func(ctx context.Context) (*MutateRequest, error) {
		produced++
		request := &MutateRequest{Resource: "records/s:key", Action: MutationDelete}
		if produced == 1 {
			return request, nil
		}
		if produced > 2 {
			return nil, io.EOF
		}
		select {
		case <-confirmed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		request.Options = completion
		return request, nil
	}
	options.Consume = func(_ context.Context, index uint64, result *MutationResult) error {
		consumed++
		if index != 1 || result.GetOutcome() != MutationApplied || result.Failure != nil {
			return errors.New("confirmed mutation changed")
		}
		close(confirmed)
		return nil
	}
	err := MutateStream(t.Context(), client, options)
	if err == nil || consumed != 1 || peer.streams.Load() != 1 || peer.received.Load() != 1 {
		t.Fatal("invalid later option lost evidence, reached the backend or caused replay", err, consumed, peer.streams.Load(), peer.received.Load())
	}
}
