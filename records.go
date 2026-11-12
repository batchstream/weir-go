package weir

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Read sends the reads of WithRequests and then of WithNext, and returns one
// result per read in input order. A later error preserves confirmed results, and
// unconfirmed positions stay nil. With WithReadConsume results go to its callback
// and Read returns nil results. It needs at least one read, or returns an error.
//
// A single read without WithNext, WithReadConsume or outgoing gRPC metadata shares
// long-lived streams with concurrent calls. Any other Read uses a gRPC stream of
// its own that follows the Client's servers. A read a server refused is sent
// again; nothing else is.
func (c *Client) Read(ctx context.Context, store string, options ...ReadOption) ([]*ReadResult, error) {
	config := applyOptions(options)
	call := recordCall[*ReadRequest, *ReadResult]{
		kind:     readKind,
		store:    store,
		requests: config.requests,
		next:     config.next,
		consume:  config.consumeRead,
		command:  readCommand,
	}
	return runRecords(ctx, c, call)
}

// Mutate sends the mutations of WithRequests and then of WithNext, and returns
// one result per mutation in input order. Mutations of one resource run in input
// order, each after the previous one's result, including after a failure; those
// of different resources may run concurrently. It is not a transaction. A later
// error preserves confirmed results, and unconfirmed positions stay nil and may
// have applied. With WithMutateConsume results go to its callback and Mutate
// returns nil results. It needs at least one mutation, or returns an error.
//
// A single mutation without WithNext, WithMutateConsume or outgoing gRPC metadata
// shares long-lived streams with concurrent calls. Any other Mutate uses a gRPC
// stream of its own that follows the Client's servers. A mutation a server refused,
// or one that lost to concurrent writers, is sent again; nothing else is.
func (c *Client) Mutate(ctx context.Context, store string, options ...MutateOption) ([]*MutationResult, error) {
	config := applyOptions(options)
	var completion *pb.MutationOptions
	if config.queryVisible {
		completion = &pb.MutationOptions{Visibility: pb.MutationVisibility_QUERY_VISIBLE}
	}
	command := func(request *MutateRequest) (*pb.Command, string, error) {
		return mutateCommand(request, completion)
	}
	call := recordCall[*MutateRequest, *MutationResult]{
		kind:     mutateKind,
		store:    store,
		requests: config.requests,
		next:     config.next,
		consume:  config.consumeMutate,
		command:  command,
	}
	return runRecords(ctx, c, call)
}

// recordKind holds what differs between the results of reads and of mutations.
type recordKind[R comparable] struct {
	name string
	read bool
	// result returns an event's result, or the zero value for another event.
	result func(*pb.Event) R
	// refused returns the result of a record a server turned away.
	refused func(*pb.Failure) R
	// resend returns why the Client sends a result's record again: a server
	// may accept it later, or a mutation lost to concurrent writers.
	resend func(R) resendCause
}

var readKind = recordKind[*ReadResult]{name: "Read", read: true, result: readEventResult, refused: readResultOfRefusal, resend: onRefusal(readRefused)}

var mutateKind = recordKind[*MutationResult]{name: "Mutate", result: mutationEventResult, refused: mutationResultOfRefusal, resend: mutationResend}

func readEventResult(event *pb.Event) *ReadResult {
	wire := event.GetReadResult()
	if wire == nil {
		return nil
	}
	return readResult(wire)
}

func readResultOfRefusal(failure *pb.Failure) *ReadResult {
	result := &ReadResult{Failure: failure}
	return result
}

func mutationEventResult(event *pb.Event) *MutationResult {
	return event.GetMutationResult()
}

func mutationResultOfRefusal(failure *pb.Failure) *MutationResult {
	return protocol.Mutation(pb.MutationOutcome_NOT_STARTED, failure)
}

// recordCall describes one Read or Mutate of a Client.
type recordCall[Q any, R comparable] struct {
	kind     recordKind[R]
	store    string
	requests []Q
	next     func(context.Context) (Q, error)
	consume  func(context.Context, int, R) error
	// command returns a validated command and, when it must keep input order,
	// its resource.
	command func(Q) (*pb.Command, string, error)
}

func runRecords[Q any, R comparable](ctx context.Context, c *Client, call recordCall[Q, R]) ([]R, error) {
	var results []R
	if call.consume == nil {
		results = make([]R, len(call.requests))
	}
	if _, err := c.storeClient(); err != nil {
		return results, err
	}
	last, err := call.validate(ctx)
	if err != nil {
		return results, err
	}
	if call.shares(ctx) {
		key := laneKey{store: call.store, read: call.kind.read}
		send := call.sender(c, key)
		once := func() (R, error) { return send(ctx, last, nil) }
		results[0], err = withResends(ctx, c, call.kind.resend, once)
		return results, err
	}
	produced := 0
	position := 0
	next := func(ctx context.Context) (*pb.Command, string, error) {
		var request Q
		switch {
		case position < len(call.requests):
			request = call.requests[position]
			position++
		case call.next != nil:
			var err error
			request, err = call.next(ctx)
			if err != nil {
				return nil, "", err
			}
		default:
			return nil, "", io.EOF
		}
		command, resource, err := call.command(request)
		if err == nil {
			produced++
		}
		return command, resource, err
	}
	consume := call.consume
	if consume == nil {
		consume = func(_ context.Context, index int, result R) error {
			results = growResults(results, index+1)
			results[index] = result
			return nil
		}
	}
	key := c.lanes.ownKey(call.store, call.kind.read)
	// Next may block, so only requests it cannot follow wait to share a message.
	stream := clientStreamOptions[R]{key: key, known: call.next == nil, next: next, send: call.sender(c, key), resend: call.kind.resend, consume: consume}
	err = runClientStream(ctx, c, stream)
	if call.consume == nil {
		results = growResults(results, produced)
	}
	return results, err
}

// growResults extends results to a length, leaving the new positions unconfirmed.
func growResults[R any](results []R, length int) []R {
	for len(results) < length {
		var unconfirmed R
		results = append(results, unconfirmed)
	}
	return results
}

// validate checks the store and every held request before anything is sent. It
// returns the last request's command, which a single shared record sends.
func (call recordCall[Q, R]) validate(ctx context.Context) (*pb.Command, error) {
	if !protocol.ValidStoreName(call.store) || (len(call.requests) == 0 && call.next == nil) {
		return nil, fmt.Errorf("%s requires a Store and requests or a producer", call.kind.name)
	}
	var command *pb.Command
	for index, request := range call.requests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var err error
		command, _, err = call.command(request)
		if err != nil {
			return nil, fmt.Errorf("%s request %d: %w", call.kind.name, index, err)
		}
	}
	return command, nil
}

// shares reports a call of one record, which needs no stream of its own. A call
// with outgoing metadata keeps one, because shared streams belong to no caller.
func (call recordCall[Q, R]) shares(ctx context.Context) bool {
	if len(call.requests) != 1 || call.next != nil || call.consume != nil {
		return false
	}
	_, outgoing := metadata.FromOutgoingContext(ctx)
	return !outgoing
}

// sender returns a function that sends a command on a stream of the key's group,
// or on a gRPC stream of its own when none takes it, unless it is already queued.
func (call recordCall[Q, R]) sender(c *Client, key laneKey) func(context.Context, *pb.Command, *queuedRecord) (R, error) {
	send := func(ctx context.Context, command *pb.Command, queued *queuedRecord) (R, error) {
		var none R
		event, shared, err := c.lanes.finish(ctx, key, command, queued)
		if !shared {
			return call.kind.alone(ctx, c, call.store, command)
		}
		if err != nil {
			return none, callError(ctx, err)
		}
		return call.kind.result(event), nil
	}
	return send
}

// alone sends one command on a gRPC stream of its own. A refused stream is the
// record's refusal, like one on a shared stream.
func (kind recordKind[R]) alone(ctx context.Context, c *Client, store string, command *pb.Command) (R, error) {
	var none, result R
	consume := func(_ context.Context, event *pb.Event) (bool, error) {
		result = kind.result(event)
		if result == none {
			return false, fmt.Errorf("unexpected event in %s stream", kind.name)
		}
		return true, nil
	}
	request := &pb.ExecuteRequest{StoreName: store, Index: 1, Command: command}
	err := execute(ctx, c.store, request, consume)
	if failure := refusedStream(err); failure != nil && result == none {
		return kind.refused(failure), nil
	}
	return result, callError(ctx, err)
}

// refusedStream returns the Failure a server sends when it turns a stream away
// before reading any of it, which proves that nothing on the stream took
// effect. A lost connection is UNAVAILABLE too, but has no such Failure.
func refusedStream(err error) *pb.Failure {
	if status.Code(err) != codes.Unavailable {
		return nil
	}
	for _, detail := range status.Convert(err).Details() {
		if failure, ok := detail.(*pb.Failure); ok {
			return failure
		}
	}
	return nil
}

// readCommand validates one read and wraps it for sending. Reads keep no order.
func readCommand(request *ReadRequest) (*pb.Command, string, error) {
	if err := protocol.ValidateReadRequest(request); err != nil {
		return nil, "", err
	}
	operation := &pb.Command_Read{Read: request}
	command := &pb.Command{Operation: operation}
	return command, "", nil
}

// mutateCommand validates one mutation and wraps it for sending. Mutations keep
// the input order of their resource.
func mutateCommand(input *MutateRequest, completion *pb.MutationOptions) (*pb.Command, string, error) {
	request, err := wireMutation(input, completion)
	if err != nil {
		return nil, "", err
	}
	if err := protocol.ValidateMutationRequest(request); err != nil {
		return nil, "", err
	}
	operation := &pb.Command_Mutate{Mutate: request}
	command := &pb.Command{Operation: operation}
	return command, request.GetResource(), nil
}

func readResult(wire *pb.ReadResult) *ReadResult {
	result := &ReadResult{Document: wire.GetDocument(), Missing: wire.GetMissing() != nil, Failure: wire.GetFailure()}
	return result
}

func wireMutation(request *MutateRequest, completion *pb.MutationOptions) (*pb.MutateRequest, error) {
	if request == nil {
		return nil, errors.New("missing mutation")
	}
	mutation := &pb.MutateRequest{Resource: request.Resource, Options: completion}
	switch request.Action {
	case MutationCreate, MutationPut, MutationReplace:
		if request.Document == nil || request.Lua != nil || request.BackendExpression != nil {
			return nil, errors.New("write requires only a document")
		}
		switch request.Action {
		case MutationCreate:
			mutation.Action = &pb.MutateRequest_Create{Create: request.Document}
		case MutationPut:
			mutation.Action = &pb.MutateRequest_Put{Put: request.Document}
		case MutationReplace:
			mutation.Action = &pb.MutateRequest_Replace{Replace: request.Document}
		}
	case MutationDelete:
		if request.Document != nil || request.Lua != nil || request.BackendExpression != nil {
			return nil, errors.New("delete accepts no document or transform")
		}
		empty := &pb.Empty{}
		mutation.Action = &pb.MutateRequest_Delete{Delete: empty}
	case MutationAtomicTransform:
		if request.Document != nil || (request.Lua == nil) == (request.BackendExpression == nil) {
			return nil, errors.New("AtomicTransform requires exactly one Lua or backend expression")
		}
		transform := &pb.Transform{}
		if request.Lua != nil {
			transform.Form = &pb.Transform_Lua{Lua: request.Lua}
		} else {
			transform.Form = &pb.Transform_BackendExpression{BackendExpression: request.BackendExpression}
		}
		mutation.Action = &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	default:
		return nil, errors.New("unknown mutation action")
	}
	return mutation, nil
}
