package weir

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func TestReadKeepsInputOrderAndIndividualFailures(t *testing.T) {
	peer := &clientTestPeer{mode: "batch_read_order"}
	client := clientTestConnection(t, peer)
	options := ReadOptions{StoreName: "records"}
	for range 25 {
		// Repeated resources retain distinct result positions.
		request := &ReadRequest{Resource: "records/s:repeated"}
		options.Requests = append(options.Requests, request)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	results, err := Read(ctx, client, options)
	if err != nil || len(results) != len(options.Requests) {
		t.Fatal("batch failed", len(results), err)
	}
	for index, result := range results {
		switch index {
		case 1:
			if result == nil || !result.Missing || result.Failure != nil {
				t.Fatal("missing result lost", result)
			}
		case 2:
			if result.Failure.GetCode() != FailurePermissionDenied {
				t.Fatal("individual backend failure lost", result)
			}
		default:
			expected := fmt.Sprintf(`{"id":%d}`, index+1)
			if string(result.Document.GetData()) != expected {
				t.Fatal("result associated with wrong input", index, result)
			}
		}
	}
	if peer.streams.Load() != 1 || peer.received.Load() != 25 {
		t.Fatal("batch opened multiple RPCs or replayed", peer.streams.Load(), peer.received.Load())
	}
}

func TestMutateBatchSelectsOperationsAndPreservesOrder(t *testing.T) {
	peer := &clientTestPeer{mode: "batch_mutate_order", mutations: make(chan *pb.MutateRequest, 5)}
	client := clientTestConnection(t, peer)
	document := &Document{ContentType: "application/json", Data: []byte(`{"n":1}`)}
	program := &LuaTransform{Source: []byte("return doc")}
	options := MutateOptions{StoreName: "records"}
	for _, action := range []MutationAction{MutationCreate, MutationPut, MutationReplace, MutationDelete, MutationAtomicTransform} {
		request := &MutateRequest{Resource: "records/s:key", Action: action}
		if action == MutationAtomicTransform {
			request.Lua = program
		} else if action != MutationDelete {
			request.Document = document
		}
		options.Requests = append(options.Requests, request)
	}
	results, err := Mutate(t.Context(), client, options)
	if err != nil || len(results) != 5 {
		t.Fatal("mutation batch failed", results, err)
	}
	for index, request := range options.Requests {
		mutation := <-peer.mutations
		selected := request.Action == MutationCreate && mutation.GetCreate() != nil ||
			request.Action == MutationPut && mutation.GetPut() != nil ||
			request.Action == MutationReplace && mutation.GetReplace() != nil ||
			request.Action == MutationDelete && mutation.GetDelete() != nil ||
			request.Action == MutationAtomicTransform && mutation.GetAtomicTransform().GetLua() != nil
		if !selected {
			t.Fatal("wrong business operation", index, mutation)
		}
		if index == 1 {
			if results[index].GetOutcome() != MutationNotApplied || results[index].GetFailure().GetCode() != FailureConflict {
				t.Fatal("individual mutation failure lost", results[index])
			}
		} else if results[index].GetOutcome() != MutationApplied {
			t.Fatal("mutation result reordered", index, results[index])
		}
	}
	if peer.streams.Load() != 1 || peer.received.Load() != 5 {
		t.Fatal("mutation batch opened multiple RPCs or replayed", peer.streams.Load(), peer.received.Load())
	}
}

func TestBatchPreflightRejectsEntireInputBeforeRPC(t *testing.T) {
	peer := &clientTestPeer{mode: "batch_read_order"}
	client := clientTestConnection(t, peer)
	for _, resource := range []string{"", "weir://other/records/s:key", "/records/s:key", "records/%6B", "records/.."} {
		request := &ReadRequest{Resource: resource}
		options := ReadOptions{StoreName: "records", Requests: []*ReadRequest{clientTestReadRequest(), request}}
		if results, err := Read(t.Context(), client, options); err == nil || results != nil {
			t.Fatal("invalid batch was not rejected", resource, results, err)
		}
	}
	for _, requests := range [][]*ReadRequest{nil, {}, {clientTestReadRequest(), nil}} {
		options := ReadOptions{StoreName: "records", Requests: requests}
		if results, err := Read(t.Context(), client, options); err == nil || results != nil {
			t.Fatal("empty or nil read accepted", results, err)
		}
	}
	options := ReadOptions{StoreName: "invalid/store", Requests: []*ReadRequest{clientTestReadRequest()}}
	if results, err := Read(t.Context(), client, options); err == nil || results != nil {
		t.Fatal("invalid store accepted", results, err)
	}
	options.StoreName = "records"
	options.Requests = nil
	for range 12 {
		options.Requests = append(options.Requests, clientTestReadRequest())
	}
	options.Requests = append(options.Requests, nil)
	if results, err := Read(t.Context(), client, options); err == nil || results != nil {
		t.Fatal("late invalid input sent earlier valid requests", results, err)
	}
	document := &Document{ContentType: "application/json", Data: []byte(`{}`)}
	valid := &MutateRequest{Resource: "records/s:first", Action: MutationPut, Document: document}
	for _, invalid := range []*MutateRequest{
		nil,
		{Resource: "records/s:key", Action: MutationPut},
		{Resource: "records/s:key", Action: MutationDelete, Document: document},
		{Resource: "weir://other/records/s:key", Action: MutationPut, Document: document},
		{Resource: "records/s:key", Action: MutationAtomicTransform},
		{Resource: "records/s:key", Action: MutationAction(99)},
	} {
		mutation := MutateOptions{StoreName: "records", Requests: []*MutateRequest{valid, invalid}}
		if results, err := Mutate(t.Context(), client, mutation); err == nil || results != nil {
			t.Fatal("invalid mutation sent earlier valid item", results, err)
		}
	}
	if peer.streams.Load() != 0 || peer.received.Load() != 0 {
		t.Fatal("preflight opened RPC", peer.streams.Load(), peer.received.Load())
	}
}

func TestBatchRejectsEncodedInputBoundsBeforeRPC(t *testing.T) {
	peer := &clientTestPeer{mode: "typed_mutation"}
	client := clientTestConnection(t, peer)
	mutations := MutateOptions{StoreName: "records"}
	oversized := &Document{ContentType: "application/octet-stream", Data: make([]byte, protocol.MaxCommandBytes+1)}
	request := &MutateRequest{Resource: "records/s:key", Action: MutationPut, Document: oversized}
	mutations.Requests = []*MutateRequest{request}
	if results, err := Mutate(t.Context(), client, mutations); err == nil || results != nil {
		t.Fatal("oversized command accepted", len(results), err)
	}
	if peer.streams.Load() != 0 || peer.received.Load() != 0 {
		t.Fatal("oversized batch opened RPC", peer.streams.Load(), peer.received.Load())
	}
}

func TestMutateStreamFailurePreservesConfirmedPrefix(t *testing.T) {
	peer := &clientTestPeer{mode: "batch_partial"}
	client := clientTestConnection(t, peer)
	document := &Document{ContentType: "application/json", Data: []byte(`{}`)}
	first := &MutateRequest{Resource: "records/s:first", Action: MutationPut, Document: document}
	second := &MutateRequest{Resource: "records/s:second", Action: MutationDelete}
	options := MutateOptions{StoreName: "records", Requests: []*MutateRequest{first, second}}
	results, err := Mutate(t.Context(), client, options)
	if err == nil || len(results) != 2 || results[0].GetOutcome() != MutationApplied || results[1] != nil {
		t.Fatal("stream failure lost confirmed prefix or acknowledged missing result", results, err)
	}
	if peer.streams.Load() != 1 || peer.received.Load() != 1 {
		t.Fatal("uncertain mutation replayed", peer.streams.Load(), peer.received.Load())
	}
}

func TestReadStreamFinalFailurePreservesConfirmedResults(t *testing.T) {
	peer := &clientTestPeer{mode: "batch_read_final_error"}
	client := clientTestConnection(t, peer)
	options := ReadOptions{StoreName: "records", Requests: []*ReadRequest{clientTestReadRequest(), clientTestReadRequest(), clientTestReadRequest()}}
	results, err := Read(t.Context(), client, options)
	if err == nil || len(results) != 3 || results[0] == nil || results[1] == nil || results[2] == nil {
		t.Fatal("final transport error or confirmed results lost", results, err)
	}
	if peer.streams.Load() != 1 || peer.received.Load() != 3 {
		t.Fatal("read batch replayed", peer.streams.Load(), peer.received.Load())
	}
}

func TestClientBatchPreflightBeforeStoreLookup(t *testing.T) {
	client := &Client{}
	options := ReadOptions{StoreName: "records"}
	_, err := client.Read(t.Context(), options)
	if err == nil || !strings.Contains(err.Error(), "nonempty requests") {
		t.Fatal("Store lookup happened before input validation", err)
	}
	mutation := MutateOptions{StoreName: "records"}
	_, err = client.Mutate(t.Context(), mutation)
	if err == nil || !strings.Contains(err.Error(), "nonempty requests") {
		t.Fatal("Store lookup happened before mutation validation", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	options.Requests = []*ReadRequest{clientTestReadRequest()}
	if _, err := client.Read(ctx, options); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled batch did not stop before routing", err)
	}
}

func TestClientBatchesRouteDirectlyToOneInitializedStore(t *testing.T) {
	business := &clientTestPeer{mode: "batch_mixed"}
	owner := &discoveryPeer{business: business}
	ownerListener := listenDiscovery(t, owner, "127.0.0.1:0")
	seed := &discoveryPeer{}
	seed.set("records", discoveryRecord("records", ownerListener.address))
	seedListener := listenDiscovery(t, seed, "127.0.0.1:0")
	openOptions := OpenOptions{Seed: seedListener.address, Stores: []string{"records"}}
	client := openDiscovery(t, openOptions)
	read := ReadOptions{StoreName: "records", Requests: []*ReadRequest{clientTestReadRequest(), clientTestReadRequest()}}
	readResults, err := client.Read(t.Context(), read)
	if err != nil || len(readResults) != 2 || readResults[0] == nil || !readResults[1].Missing {
		t.Fatal("direct read batch failed", readResults, err)
	}
	document := &Document{ContentType: "application/json", Data: []byte(`{}`)}
	first := &MutateRequest{Resource: "records/s:first", Action: MutationPut, Document: document}
	second := &MutateRequest{Resource: "records/s:second", Action: MutationDelete}
	mutation := MutateOptions{StoreName: "records", Requests: []*MutateRequest{first, second}}
	mutationResults, err := client.Mutate(t.Context(), mutation)
	if err != nil || len(mutationResults) != 2 || mutationResults[0].GetOutcome() != MutationApplied || mutationResults[1].GetFailure().GetCode() != FailureConflict {
		t.Fatal("direct mutation batch failed", mutationResults, err)
	}
	if seed.resolves.Load() != 1 || seed.executions.Load() != 0 || owner.resolves.Load() != 0 || owner.executions.Load() != 2 {
		t.Fatal("batch repeated resolution, forwarded business traffic, or split RPCs", seed.resolves.Load(), seed.executions.Load(), owner.resolves.Load(), owner.executions.Load())
	}
}

func TestMutateBatchLargeDocumentsUseOneRPC(t *testing.T) {
	peer := &clientTestPeer{mode: "typed_mutation"}
	client := clientTestConnection(t, peer)
	document := &Document{ContentType: "application/octet-stream", Data: make([]byte, protocol.MaxDocument)}
	options := MutateOptions{StoreName: "records"}
	for range 20 {
		request := &MutateRequest{Resource: "records/s:key", Action: MutationPut, Document: document}
		options.Requests = append(options.Requests, request)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	results, err := Mutate(ctx, client, options)
	if err != nil || len(results) != 20 {
		t.Fatal("large typed batch failed", len(results), err)
	}
	for _, result := range results {
		if result.GetOutcome() != MutationApplied {
			t.Fatal("large mutation lost acknowledgement", result)
		}
	}
	if peer.streams.Load() != 1 || peer.received.Load() != 20 || peer.maxBytes.Load() > protocol.MaxExecuteRequestBytes {
		t.Fatal("large batch opened multiple RPCs", peer.streams.Load(), peer.received.Load())
	}
}

func TestReadAcceptsLargeRequestSequence(t *testing.T) {
	peer := &clientTestPeer{mode: "batch_read_order"}
	client := clientTestConnection(t, peer)
	options := ReadOptions{StoreName: "records"}
	for range 3073 {
		options.Requests = append(options.Requests, clientTestReadRequest())
	}
	results, err := Read(t.Context(), client, options)
	if err != nil || len(results) != 3073 || peer.streams.Load() != 1 || peer.received.Load() != 3073 {
		t.Fatal("bounded large batch was split or limited by count", len(results), err, peer.streams.Load(), peer.received.Load())
	}
}
