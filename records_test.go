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
	"google.golang.org/grpc"
)

func TestReadPreservesMissingAndBackendFailure(t *testing.T) {
	for _, mode := range []string{"read_missing", "read_failure", "read_target_missing"} {
		t.Run(mode, func(t *testing.T) {
			peer := &clientTestPeer{mode: mode}
			client := clientTestClient(t, peer)
			result, err := readOne(t.Context(), client, clientTestReadRequest())
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

func TestMutationActionsSelectBackendOperation(t *testing.T) {
	for _, visible := range []bool{false, true} {
		t.Run(fmt.Sprintf("visible=%t", visible), func(t *testing.T) {
			peer := &clientTestPeer{mode: "typed_mutation", mutations: make(chan *pb.MutateRequest, 5)}
			client := clientTestClient(t, peer)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			document := &Document{ContentType: "application/json", Data: []byte(`{"n":1}`)}
			program := &LuaTransform{Source: []byte("return function(current, incoming) return current end")}
			want := pb.MutationVisibility_APPLICATION_ACKNOWLEDGED
			var options []MutateOption
			if visible {
				want = pb.MutationVisibility_QUERY_VISIBLE
				options = append(options, WithQueryVisible())
			}
			for _, action := range []MutationAction{MutationCreate, MutationPut, MutationReplace, MutationDelete, MutationAtomicTransform} {
				request := &MutateRequest{Resource: "records/s:key", Action: action}
				switch action {
				case MutationDelete:
				case MutationAtomicTransform:
					request.Lua = program
				default:
					request.Document = document
				}
				result, err := mutateOne(ctx, client, request, options...)
				if err != nil || result.GetOutcome() != MutationApplied {
					t.Fatal(action, result, err)
				}
				mutation := <-peer.mutations
				if mutation.Resource != request.Resource || mutation.GetOptions().GetVisibility() != want {
					t.Fatal("a mutation lost its resource or visibility", action, mutation)
				}
				selected := action == MutationCreate && mutation.GetCreate() != nil ||
					action == MutationPut && mutation.GetPut() != nil ||
					action == MutationReplace && mutation.GetReplace() != nil ||
					action == MutationDelete && mutation.GetDelete() != nil ||
					action == MutationAtomicTransform && mutation.GetAtomicTransform().GetLua() != nil
				if !selected {
					t.Fatal("SDK executed a different mutation", action, mutation)
				}
			}
			if peer.received.Load() != 5 {
				t.Fatal("mutations were replayed", peer.received.Load())
			}
		})
	}
}

func TestAtomicTransformRejectsAmbiguousFormsBeforeBusinessSend(t *testing.T) {
	for _, both := range []bool{false, true} {
		t.Run(map[bool]string{false: "neither", true: "both"}[both], func(t *testing.T) {
			peer := &clientTestPeer{mode: "typed_mutation"}
			client := clientTestClient(t, peer)
			request := &MutateRequest{Resource: "records/s:key", Action: MutationAtomicTransform}
			if both {
				request.Lua = &LuaTransform{Source: []byte("return function(current, incoming) return current end")}
				request.BackendExpression = &Document{ContentType: "application/json", Data: []byte(`{}`)}
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if result, err := mutateOne(ctx, client, request); err == nil || result != nil {
				t.Fatal("invalid transform was not rejected", result, err)
			}
			if peer.received.Load() != 0 {
				t.Fatal("invalid transform sent business data", peer.received.Load())
			}
		})
	}
}

func TestReadKeepsInputOrderAndIndividualFailures(t *testing.T) {
	peer := &clientTestPeer{mode: "batch_read_order"}
	client := clientTestClient(t, peer)
	var requests []*ReadRequest
	for range 25 {
		// Repeated resources retain distinct result positions.
		request := &ReadRequest{Resource: "records/s:repeated"}
		requests = append(requests, request)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	results, err := client.Read(ctx, "records", WithRequests(requests...))
	if err != nil || len(results) != len(requests) {
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
	client := clientTestClient(t, peer)
	document := &Document{ContentType: "application/json", Data: []byte(`{"n":1}`)}
	program := &LuaTransform{Source: []byte("return function(current, incoming) return current end")}
	var requests []*MutateRequest
	for _, action := range []MutationAction{MutationCreate, MutationPut, MutationReplace, MutationDelete, MutationAtomicTransform} {
		request := &MutateRequest{Resource: "records/s:key", Action: action}
		if action == MutationAtomicTransform {
			request.Lua = program
		} else if action != MutationDelete {
			request.Document = document
		}
		requests = append(requests, request)
	}
	results, err := client.Mutate(t.Context(), "records", WithRequests(requests...))
	if err != nil || len(results) != 5 {
		t.Fatal("mutation batch failed", results, err)
	}
	for index, request := range requests {
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
			if results[index].GetOutcome() != MutationNotApplied || results[index].GetFailure().GetCode() != FailurePreconditionFailed {
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

// allNil reports whether every result of a rejected call stayed unconfirmed.
func allNil[R comparable](results []R, length int) bool {
	var none R
	if len(results) != length {
		return false
	}
	for _, result := range results {
		if result != none {
			return false
		}
	}
	return true
}

func TestBatchPreflightRejectsEntireInputBeforeRPC(t *testing.T) {
	peer := &clientTestPeer{mode: "batch_read_order"}
	client := clientTestClient(t, peer)
	for _, resource := range []string{"", "weir://other/records/s:key", "/records/s:key", "records/%6B", "records/.."} {
		request := &ReadRequest{Resource: resource}
		results, err := client.Read(t.Context(), "records", WithRequests(clientTestReadRequest(), request))
		if err == nil || !allNil(results, 2) {
			t.Fatal("invalid batch was not rejected", resource, results, err)
		}
	}
	if results, err := client.Read(t.Context(), "records"); err == nil || len(results) != 0 {
		t.Fatal("a call without requests was accepted", results, err)
	}
	if results, err := client.Read(t.Context(), "records", WithRequests[*ReadRequest]()); err == nil || len(results) != 0 {
		t.Fatal("an empty read was accepted", results, err)
	}
	if results, err := client.Read(t.Context(), "records", WithRequests(clientTestReadRequest(), nil)); err == nil || !allNil(results, 2) {
		t.Fatal("a nil read was accepted", results, err)
	}
	if results, err := client.Read(t.Context(), "invalid/store", WithRequests(clientTestReadRequest())); err == nil || !allNil(results, 1) {
		t.Fatal("invalid store accepted", results, err)
	}
	var requests []*ReadRequest
	for range 12 {
		requests = append(requests, clientTestReadRequest())
	}
	requests = append(requests, nil)
	if results, err := client.Read(t.Context(), "records", WithRequests(requests...)); err == nil || !allNil(results, 13) {
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
		results, err := client.Mutate(t.Context(), "records", WithRequests(valid, invalid))
		if err == nil || !allNil(results, 2) {
			t.Fatal("invalid mutation sent earlier valid item", results, err)
		}
	}
	if peer.streams.Load() != 0 || peer.received.Load() != 0 {
		t.Fatal("preflight opened RPC", peer.streams.Load(), peer.received.Load())
	}
}

func TestBatchAcceptsLargeRecord(t *testing.T) {
	peer := &clientTestPeer{mode: "typed_mutation"}
	client := clientTestClient(t, peer)
	large := &Document{ContentType: "application/octet-stream", Data: make([]byte, 16<<20)}
	request := &MutateRequest{Resource: "records/s:key", Action: MutationPut, Document: large}
	if result, err := mutateOne(t.Context(), client, request); err != nil || result.Outcome != MutationApplied || result.Failure != nil {
		t.Fatal("large record rejected", result, err)
	}
	if peer.streams.Load() != 1 || peer.received.Load() != 1 {
		t.Fatal("large record did not use one RPC", peer.streams.Load(), peer.received.Load())
	}
}

func TestMutateFailurePreservesConfirmedPrefix(t *testing.T) {
	peer := &clientTestPeer{mode: "batch_partial"}
	client := clientTestClient(t, peer)
	document := &Document{ContentType: "application/json", Data: []byte(`{}`)}
	first := &MutateRequest{Resource: "records/s:first", Action: MutationPut, Document: document}
	second := deleteRequest("records/s:second")
	results, err := client.Mutate(t.Context(), "records", WithRequests(first, second))
	if err == nil || len(results) != 2 || results[0].GetOutcome() != MutationApplied || results[1] != nil {
		t.Fatal("stream failure lost confirmed prefix or acknowledged missing result", results, err)
	}
	if peer.streams.Load() != 1 || peer.received.Load() != 1 {
		t.Fatal("uncertain mutation replayed", peer.streams.Load(), peer.received.Load())
	}
}

func TestReadSucceedsOnceEveryResultArrived(t *testing.T) {
	peer := &clientTestPeer{mode: "batch_read_final_error"}
	client := clientTestClient(t, peer)
	results, err := client.Read(t.Context(), "records", WithRequests(clientTestReadRequest(), clientTestReadRequest(), clientTestReadRequest()))
	if err != nil || len(results) != 3 || results[0] == nil || results[1] == nil || results[2] == nil {
		t.Fatal("a call with every result failed or lost one", results, err)
	}
	if peer.streams.Load() != 1 || peer.received.Load() != 3 {
		t.Fatal("read batch replayed", peer.streams.Load(), peer.received.Load())
	}
}

func TestClientBatchPreflightBeforeRPC(t *testing.T) {
	client := &Client{}
	_, err := client.Read(t.Context(), "records")
	if err == nil || !strings.Contains(err.Error(), "requires a Store and requests") {
		t.Fatal("RPC started before input validation", err)
	}
	_, err = client.Mutate(t.Context(), "records")
	if err == nil || !strings.Contains(err.Error(), "requires a Store and requests") {
		t.Fatal("RPC started before mutation validation", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := client.Read(ctx, "records", WithRequests(clientTestReadRequest())); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled batch did not stop before its RPC", err)
	}
}

func TestClientSendsBatchesToItsAddress(t *testing.T) {
	peer := &clientTestPeer{mode: "batch_mixed"}
	client, err := New(clientTestAddress(t, peer))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reads := []*ReadRequest{clientTestReadRequest(), clientTestReadRequest()}
	readResults, err := client.Read(t.Context(), "records", WithRequests(reads...))
	// The peer answers by wire index; records of a Client's stream need not be
	// sent in input order unless they share a resource.
	if err != nil || len(readResults) != 2 || readResults[0] == nil || readResults[1] == nil || readResults[0].Missing == readResults[1].Missing {
		t.Fatal("read batch failed", readResults, err)
	}
	document := &Document{ContentType: "application/json", Data: []byte(`{}`)}
	first := &MutateRequest{Resource: "records/s:first", Action: MutationPut, Document: document}
	second := deleteRequest("records/s:second")
	mutationResults, err := client.Mutate(t.Context(), "records", WithRequests(first, second))
	if err != nil || len(mutationResults) != 2 || mutationResults[0] == nil || mutationResults[1] == nil || (mutationResults[0].GetFailure() == nil) == (mutationResults[1].GetFailure() == nil) {
		t.Fatal("mutation batch failed", mutationResults, err)
	}
	if peer.streams.Load() != 2 {
		t.Fatal("batches were split or replayed", peer.streams.Load())
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	closed, err := client.Read(t.Context(), "records", WithRequests(reads...))
	if !errors.Is(err, ErrClosed) || !allNil(closed, 2) {
		t.Fatal("closed client accepted a call", closed, err)
	}
}

func TestLargeDocumentsEachUseAStreamOfTheirOwn(t *testing.T) {
	peer := &clientTestPeer{mode: "typed_mutation"}
	client := clientTestClient(t, peer)
	document := &Document{ContentType: "application/octet-stream", Data: make([]byte, (2 << 20))}
	var requests []*MutateRequest
	for range 20 {
		request := &MutateRequest{Resource: "records/s:key", Action: MutationPut, Document: document}
		requests = append(requests, request)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	results, err := client.Mutate(ctx, "records", WithRequests(requests...))
	if err != nil || len(results) != 20 {
		t.Fatal("large typed batch failed", len(results), err)
	}
	for _, result := range results {
		if result.GetOutcome() != MutationApplied {
			t.Fatal("large mutation lost acknowledgement", result)
		}
	}
	if peer.streams.Load() != 20 || peer.received.Load() != 20 || peer.maxBytes.Load() > int64(protocol.MaxTransportMessageBytes) {
		t.Fatal("large records did not each use a stream", peer.streams.Load(), peer.received.Load())
	}
}

func TestReadAcceptsLargeRequestSequence(t *testing.T) {
	peer := &clientTestPeer{mode: "batch_read_order"}
	client := clientTestClient(t, peer)
	var requests []*ReadRequest
	for range 5000 {
		requests = append(requests, clientTestReadRequest())
	}
	results, err := client.Read(t.Context(), "records", WithRequests(requests...))
	if err != nil || len(results) != 5000 || peer.streams.Load() != 1 || peer.received.Load() != 5000 {
		t.Fatal("large batch was split or limited by count", len(results), err, peer.streams.Load(), peer.received.Load())
	}
}

// streamRefuser turns every stream away before reading it.
type streamRefuser struct{}

func (streamRefuser) Execute(grpc.BidiStreamingServer[pb.ExecuteRequests, pb.ExecuteResponses]) error {
	return refusedStreamError("draining")
}

func TestARefusedStreamOfItsOwnIsTheRecordsRefusal(t *testing.T) {
	client, err := New(serveForTest(t, streamRefuser{}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	readCommandToSend, _, err := readCommand(clientTestReadRequest())
	if err != nil {
		t.Fatal(err)
	}
	read, err := readKind.alone(t.Context(), client, "records", readCommandToSend)
	if err != nil || read.Failure.GetCode() != FailureUnavailable {
		t.Fatal("a refused read lost its refusal", read, err)
	}
	mutateCommandToSend, _, err := mutateCommand(deleteRequest("records/s:key"), nil)
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := mutateKind.alone(t.Context(), client, "records", mutateCommandToSend)
	if err != nil || mutation.GetOutcome() != MutationNotStarted || mutation.GetFailure().GetCode() != FailureUnavailable {
		t.Fatal("a refused mutation lost its refusal", mutation, err)
	}
}
