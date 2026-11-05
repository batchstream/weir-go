package weir

import (
	"context"
	"errors"
	"io"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func TestQueryVisibleAppliesToEveryRecordOfACall(t *testing.T) {
	peer := &clientTestPeer{mode: "typed_mutation", mutations: make(chan *pb.MutateRequest, 2)}
	client := clientTestClient(t, peer)
	expression := &Document{ContentType: "application/json", Data: []byte(`{"doc":{"n":1}}`)}
	first := &MutateRequest{Resource: "records/s:first", Action: MutationAtomicTransform, BackendExpression: expression}
	second := deleteRequest("records/s:second")
	produced := false
	next := func(context.Context) (*MutateRequest, error) {
		if produced {
			return nil, io.EOF
		}
		produced = true
		return second, nil
	}
	consumed := 0
	consume := func(_ context.Context, _ int, result *MutationResult) error {
		consumed++
		if result.GetOutcome() != MutationApplied || result.Failure != nil {
			return errors.New("mutation evidence was lost")
		}
		return nil
	}
	_, err := client.Mutate(t.Context(), "records", WithRequests(first), WithNext(next), WithMutateConsume(consume), WithQueryVisible())
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		mutation := <-peer.mutations
		if mutation.GetOptions().GetVisibility() != pb.MutationVisibility_QUERY_VISIBLE {
			t.Fatal("a record lost the call's visibility", mutation)
		}
	}
	if peer.streams.Load() != 1 || peer.received.Load() != 2 || consumed != 2 {
		t.Fatal("visibility requests were split, replayed or left unconfirmed", peer.streams.Load(), peer.received.Load(), consumed)
	}
}

func TestMutationsWaitForApplicationByDefault(t *testing.T) {
	peer := &clientTestPeer{mode: "typed_mutation", mutations: make(chan *pb.MutateRequest, 1)}
	client := clientTestClient(t, peer)
	if _, err := mutateOne(t.Context(), client, deleteRequest("records/s:key")); err != nil {
		t.Fatal(err)
	}
	if mutation := <-peer.mutations; mutation.Options != nil {
		t.Fatal("a mutation sent options nobody asked for", mutation)
	}
}

func TestProducedInvalidMutationKeepsConfirmedPrefix(t *testing.T) {
	peer := &clientTestPeer{mode: "typed_mutation"}
	client := clientTestClient(t, peer)
	confirmed := make(chan struct{})
	produced, consumed := 0, 0
	next := func(ctx context.Context) (*MutateRequest, error) {
		produced++
		if produced == 1 {
			return deleteRequest("records/s:key"), nil
		}
		select {
		case <-confirmed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		invalid := &MutateRequest{Resource: "records/s:key", Action: MutationAction(99)}
		return invalid, nil
	}
	consume := func(_ context.Context, index int, result *MutationResult) error {
		consumed++
		if index != 0 || result.GetOutcome() != MutationApplied || result.Failure != nil {
			return errors.New("confirmed mutation changed")
		}
		close(confirmed)
		return nil
	}
	_, err := client.Mutate(t.Context(), "records", WithNext(next), WithMutateConsume(consume))
	if err == nil || consumed != 1 || peer.streams.Load() != 1 || peer.received.Load() != 1 {
		t.Fatal("invalid later request lost evidence, reached the backend or caused replay", err, consumed, peer.streams.Load(), peer.received.Load())
	}
}
