package weir

import (
	"context"
	"io"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
)

// refusingPeer refuses the first refusals[resource] sends of a resource as a
// draining server does, then fails conflicts[resource] mutations as lost to
// concurrent writers, then answers results[resource] or applies the mutation
// and reports a read missing. It counts every send and records the documents
// of applied mutations in order.
type refusingPeer struct {
	mu        sync.Mutex
	refusals  map[string]int
	conflicts map[string]int
	results   map[string]*pb.MutationResult
	sends     map[string]int
	applied   []string
}

func newRefusingPeer() *refusingPeer {
	peer := &refusingPeer{
		refusals:  make(map[string]int),
		conflicts: make(map[string]int),
		results:   make(map[string]*pb.MutationResult),
		sends:     make(map[string]int),
	}
	return peer
}

func (p *refusingPeer) Execute(packed grpc.BidiStreamingServer[pb.ExecuteRequests, pb.ExecuteResponses]) error {
	stream := &recordPeer{BidiStreamingServer: packed}
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if request.CancelIndex != 0 {
			continue
		}
		event := p.answer(request.Command)
		response := &pb.ExecuteResponse{Index: request.Index, Event: event}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
}

func (p *refusingPeer) answer(command *pb.Command) *pb.Event {
	read := command.GetRead()
	mutation := command.GetMutate()
	resource := mutation.GetResource()
	if read != nil {
		resource = read.GetResource()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sends[resource]++
	refused := p.refusals[resource] > 0
	if refused {
		p.refusals[resource]--
	}
	failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, "store draining")
	if read != nil {
		result := protocol.Missing()
		if refused {
			result = protocol.ReadFailure(failure)
		}
		value := &pb.Event_ReadResult{ReadResult: result}
		event := &pb.Event{Value: value}
		return event
	}
	result := protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
	switch {
	case refused:
		result = protocol.Mutation(pb.MutationOutcome_NOT_STARTED, failure)
	case p.conflicts[resource] > 0:
		p.conflicts[resource]--
		conflict := protocol.Fail(pb.FailureCode_CONFLICT, "record changed during every attempt")
		result = protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, conflict)
	case p.results[resource] != nil:
		result = p.results[resource]
	default:
		p.applied = append(p.applied, string(mutation.GetPut().GetData()))
	}
	value := &pb.Event_MutationResult{MutationResult: result}
	event := &pb.Event{Value: value}
	return event
}

func (p *refusingPeer) refuse(resource string, sends int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.refusals[resource] = sends
}

func (p *refusingPeer) sent(resource string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sends[resource]
}

// drain returns the documents applied since the last call.
func (p *refusingPeer) drain() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	applied := p.applied
	p.applied = nil
	return applied
}

// refusingClient serves peer on loopback and returns a Client made by New.
func refusingClient(t *testing.T, peer *refusingPeer) *Client {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterStoreServiceServer(server, peer)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-done })
	client, err := New(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func putRequest(resource, data string) *MutateRequest {
	document := &Document{ContentType: "application/json", Data: []byte(data)}
	request := &MutateRequest{Resource: resource, Action: MutationPut, Document: document}
	return request
}

func refusingPut(ctx context.Context, client *Client, resource string) (*MutationResult, error) {
	return mutateOne(ctx, client, putRequest(resource, `{}`))
}

func TestClientResendsRecordsAServerRefusedBeforeTheyBegan(t *testing.T) {
	peer := newRefusingPeer()
	peer.refuse("records/s:write", 2)
	peer.refuse("records/s:read", 1)
	client := refusingClient(t, peer)
	result, err := refusingPut(t.Context(), client, "records/s:write")
	if err != nil || result.GetOutcome() != MutationApplied || peer.sent("records/s:write") != 3 {
		t.Fatal("refused write was not resent until applied", result, err, peer.sent("records/s:write"))
	}
	request := &ReadRequest{Resource: "records/s:read"}
	read, err := readOne(t.Context(), client, request)
	if err != nil || !read.Missing || peer.sent("records/s:read") != 2 {
		t.Fatal("refused read was not resent", read, err, peer.sent("records/s:read"))
	}
}

func TestClientResendsMutationsThatLostToConcurrentWriters(t *testing.T) {
	peer := newRefusingPeer()
	peer.conflicts["records/s:hot"] = 2
	peer.conflicts["records/s:hotter"] = 10
	client := refusingClient(t, peer)
	result, err := refusingPut(t.Context(), client, "records/s:hot")
	if err != nil || result.GetOutcome() != MutationApplied || peer.sent("records/s:hot") != 3 {
		t.Fatal("conflicted write was not resent until applied", result, err, peer.sent("records/s:hot"))
	}
	result, err = refusingPut(t.Context(), client, "records/s:hotter")
	if err != nil || result.GetOutcome() != MutationNotApplied || result.GetFailure().GetCode() != FailureConflict {
		t.Fatal("last conflict lost its evidence", result, err)
	}
	if peer.sent("records/s:hotter") != resendAttempts {
		t.Fatal("unexpected sends", peer.sent("records/s:hotter"))
	}
	// A call of several records resends a conflicted one before the next
	// mutation of its resource.
	peer.conflicts["records/s:x"] = 1
	first := putRequest("records/s:x", `{"n":1}`)
	later := putRequest("records/s:x", `{"n":2}`)
	peer.drain()
	results, err := client.Mutate(t.Context(), "records", WithRequests(first, later))
	if err != nil || results[0].GetOutcome() != MutationApplied || results[1].GetOutcome() != MutationApplied {
		t.Fatal("conflicted write in a call was not resent", results, err)
	}
	if applied := peer.drain(); !slices.Equal(applied, []string{`{"n":1}`, `{"n":2}`}) {
		t.Fatal("resent write applied out of order", applied)
	}
}

func TestConflictsNeverSpendTheResendsOfRefusals(t *testing.T) {
	peer := newRefusingPeer()
	client := refusingClient(t, peer)
	for client.resends.spend(resendConflict) {
	}
	peer.conflicts["records/s:hot"] = 1
	result, err := refusingPut(t.Context(), client, "records/s:hot")
	if err != nil || result.GetFailure().GetCode() != FailureConflict || peer.sent("records/s:hot") != 1 {
		t.Fatal("conflict resent beyond its budget", result, err, peer.sent("records/s:hot"))
	}
	peer.refuse("records/s:cold", 1)
	result, err = refusingPut(t.Context(), client, "records/s:cold")
	if err != nil || result.GetOutcome() != MutationApplied || peer.sent("records/s:cold") != 2 {
		t.Fatal("refusal not resent after conflicts used their budget", result, err, peer.sent("records/s:cold"))
	}
}

func TestClientStopsResendingAfterFourSends(t *testing.T) {
	peer := newRefusingPeer()
	peer.refuse("records/s:write", 10)
	client := refusingClient(t, peer)
	result, err := refusingPut(t.Context(), client, "records/s:write")
	if err != nil || result.GetOutcome() != MutationNotStarted || result.GetFailure().GetCode() != FailureUnavailable {
		t.Fatal("last refusal lost its evidence", result, err)
	}
	if peer.sent("records/s:write") != resendAttempts {
		t.Fatal("unexpected sends", peer.sent("records/s:write"))
	}
}

func TestResendsNeverOutlastTheCallerDeadline(t *testing.T) {
	peer := newRefusingPeer()
	peer.refuse("records/s:write", 10)
	client := refusingClient(t, peer)
	if _, err := refusingPut(t.Context(), client, "records/s:warm"); err != nil {
		t.Fatal(err)
	}
	// The second send waits at most 50ms and leaves at least 50ms for itself;
	// a third would need 150ms.
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Millisecond)
	defer cancel()
	result, err := refusingPut(ctx, client, "records/s:write")
	if err != nil || result.GetOutcome() != MutationNotStarted {
		t.Fatal("refusal within the deadline was not returned", result, err)
	}
	if ctx.Err() != nil || peer.sent("records/s:write") != 2 {
		t.Fatal("resends did not fit the deadline", ctx.Err(), peer.sent("records/s:write"))
	}
}

func TestOnlyRefusalsBeforeTheRecordBeganAreResent(t *testing.T) {
	peer := newRefusingPeer()
	unavailable := protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend unavailable")
	canceled := protocol.Fail(pb.FailureCode_CANCELLED, "record canceled")
	peer.results["records/s:unknown"] = protocol.Mutation(pb.MutationOutcome_UNKNOWN, unavailable)
	peer.results["records/s:not-applied"] = protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, unavailable)
	peer.results["records/s:canceled"] = protocol.Mutation(pb.MutationOutcome_NOT_STARTED, canceled)
	client := refusingClient(t, peer)
	for resource, want := range peer.results {
		result, err := refusingPut(t.Context(), client, resource)
		if err != nil || result.GetOutcome() != want.GetOutcome() || peer.sent(resource) != 1 {
			t.Fatal("result was resent or changed", resource, result, err, peer.sent(resource))
		}
	}
}

func TestMutateResendsRefusedRecordsWithoutReorderingAResource(t *testing.T) {
	peer := newRefusingPeer()
	client := refusingClient(t, peer)
	mutate := func(requests ...*MutateRequest) []*MutationResult {
		results, err := client.Mutate(t.Context(), "records", WithRequests(requests...))
		if err != nil {
			t.Fatal(err)
		}
		return results
	}
	// The later write of x waits until its refused earlier write was sent again.
	peer.refuse("records/s:x", 1)
	first := putRequest("records/s:x", `{"n":1}`)
	other := putRequest("records/s:y", `{"n":2}`)
	later := putRequest("records/s:x", `{"n":3}`)
	results := mutate(first, other, later)
	if results[0].GetOutcome() != MutationApplied || results[1].GetOutcome() != MutationApplied || results[2].GetOutcome() != MutationApplied {
		t.Fatal("a refused write was not resent", results)
	}
	applied := slices.DeleteFunc(peer.drain(), func(document string) bool { return document == `{"n":2}` })
	if !slices.Equal(applied, []string{`{"n":1}`, `{"n":3}`}) {
		t.Fatal("a resource's writes applied out of order", applied)
	}
	// The first two sends of z are refused; both writes still apply in order.
	peer.refuse("records/s:z", 2)
	first = putRequest("records/s:z", `{"n":4}`)
	later = putRequest("records/s:z", `{"n":5}`)
	results = mutate(first, later)
	if results[0].GetOutcome() != MutationApplied || results[1].GetOutcome() != MutationApplied {
		t.Fatal("refused writes were not resent", results)
	}
	if applied := peer.drain(); !slices.Equal(applied, []string{`{"n":4}`, `{"n":5}`}) {
		t.Fatal("resent writes applied out of order", applied)
	}
}

func TestReadResendsRefusedReads(t *testing.T) {
	peer := newRefusingPeer()
	peer.refuse("records/s:a", 1)
	client := refusingClient(t, peer)
	first := &ReadRequest{Resource: "records/s:a"}
	second := &ReadRequest{Resource: "records/s:b"}
	results, err := client.Read(t.Context(), "records", WithRequests(first, second))
	if err != nil || !results[0].Missing || !results[1].Missing {
		t.Fatal("refused read was not resent", results, err)
	}
	if peer.sent("records/s:a") != 2 || peer.sent("records/s:b") != 1 {
		t.Fatal("unexpected sends", peer.sent("records/s:a"), peer.sent("records/s:b"))
	}
}

func TestResendBudgetAllowsATenthOfFirstSendsPlusABurst(t *testing.T) {
	budget := newResendBudget()
	spent := func(attempts int) int {
		covered := 0
		for range attempts {
			if budget.spend(resendRefusal) {
				covered++
			}
		}
		return covered
	}
	if covered := spent(resendBurst + 50); covered != resendBurst {
		t.Fatal("burst not bounded", covered)
	}
	if !budget.spend(resendConflict) {
		t.Fatal("refusals spent the conflict budget")
	}
	budget.earn(25)
	if covered := spent(5); covered != 2 {
		t.Fatal("first sends did not earn a tenth of a resend each", covered)
	}
	budget.earn(1 << 20)
	if covered := spent(1 << 20); covered != resendBurst {
		t.Fatal("credit grew past the burst", covered)
	}
}

func TestRefusalsAreThoseBeforeARecordBegan(t *testing.T) {
	unavailable := protocol.Fail(pb.FailureCode_UNAVAILABLE, "store draining")
	exhausted := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "result exceeds the message size limit")
	invalid := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid record")
	conflict := protocol.Fail(pb.FailureCode_CONFLICT, "record changed during every attempt")
	refusedRead := &ReadResult{Failure: unavailable}
	exhaustedRead := &ReadResult{Failure: exhausted}
	invalidRead := &ReadResult{Failure: invalid}
	missingRead := &ReadResult{Missing: true}
	refusedNative := &NativeResult{Completion: NativeNotStarted, Failure: unavailable}
	answeredNative := &NativeResult{Response: &NativeResponse{}, Completion: NativeNotStarted, Failure: unavailable}
	incompleteNative := &NativeResult{Completion: NativeResponseIncomplete, Failure: unavailable}
	cases := []struct {
		name    string
		refused bool
		want    bool
	}{
		{"draining mutation", mutationRefused(protocol.Mutation(pb.MutationOutcome_NOT_STARTED, unavailable)), true},
		{"exhausted mutation", mutationRefused(protocol.Mutation(pb.MutationOutcome_NOT_STARTED, exhausted)), false},
		{"invalid mutation", mutationRefused(protocol.Mutation(pb.MutationOutcome_NOT_STARTED, invalid)), false},
		{"unknown mutation", mutationRefused(protocol.Mutation(pb.MutationOutcome_UNKNOWN, unavailable)), false},
		{"missing mutation result", mutationRefused(nil), false},
		{"conflicted mutation", mutationRefused(protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, conflict)), false},
		{"draining mutation resent as a refusal", mutationResend(protocol.Mutation(pb.MutationOutcome_NOT_STARTED, unavailable)) == resendRefusal, true},
		{"conflicted mutation resent as a conflict", mutationResend(protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, conflict)) == resendConflict, true},
		{"unknown conflicted mutation", mutationResend(protocol.Mutation(pb.MutationOutcome_UNKNOWN, conflict)) != noResend, false},
		{"not applied mutation", mutationResend(protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, invalid)) != noResend, false},
		{"missing mutation result resent", mutationResend(nil) != noResend, false},
		{"refused read", readRefused(refusedRead), true},
		{"exhausted read", readRefused(exhaustedRead), false},
		{"invalid read", readRefused(invalidRead), false},
		{"missing read", readRefused(missingRead), false},
		{"refused Native", nativeRefused(refusedNative), true},
		{"Native with a response", nativeRefused(answeredNative), false},
		{"incomplete Native", nativeRefused(incompleteNative), false},
	}
	for _, test := range cases {
		if test.refused != test.want {
			t.Error(test.name, test.refused)
		}
	}
}
