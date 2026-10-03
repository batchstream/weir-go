package weir

import (
	"context"
	"errors"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

// Leaf DTOs preserve document bytes and backend evidence without exposing any
// transport framing, version field, or protobuf operation selection.
type Document = pb.Document
type Failure = pb.Failure
type FailureCode = pb.FailureCode
type MutationResult = pb.MutationResult
type MutationOutcome = pb.MutationOutcome
type ProgramTransform = pb.ProgramTransform
type ReadRequest = pb.ReadRequest
type ScanRequest = pb.ScanRequest
type ScanEnd = pb.ScanEnd
type NativeHead = pb.NativeHead
type NativeEnd = pb.NativeEnd
type NativeCompletion = pb.NativeCompletion

const (
	MutationNotStarted = pb.MutationOutcome_NOT_STARTED
	MutationNotApplied = pb.MutationOutcome_NOT_APPLIED
	MutationApplied    = pb.MutationOutcome_APPLIED
	MutationUnknown    = pb.MutationOutcome_UNKNOWN

	NativeNotStarted         = pb.NativeCompletion_NATIVE_NOT_STARTED
	NativeResponseComplete   = pb.NativeCompletion_RESPONSE_COMPLETE
	NativeResponseIncomplete = pb.NativeCompletion_RESPONSE_INCOMPLETE

	FailureInvalidArgument    = pb.FailureCode_INVALID_ARGUMENT
	FailureUnauthenticated    = pb.FailureCode_UNAUTHENTICATED
	FailurePermissionDenied   = pb.FailureCode_PERMISSION_DENIED
	FailureNotFound           = pb.FailureCode_NOT_FOUND
	FailurePreconditionFailed = pb.FailureCode_PRECONDITION_FAILED
	FailureConflict           = pb.FailureCode_CONFLICT
	FailureUnsupported        = pb.FailureCode_UNSUPPORTED
	FailureResourceExhausted  = pb.FailureCode_RESOURCE_EXHAUSTED
	FailureUnavailable        = pb.FailureCode_UNAVAILABLE
	FailureCancelled          = pb.FailureCode_CANCELLED
	FailureDeadlineExceeded   = pb.FailureCode_DEADLINE_EXCEEDED
	FailureInternal           = pb.FailureCode_INTERNAL
)

// EncodeSegment encodes one decoded resource path segment, including slashes.
func EncodeSegment(segment string) string { return protocol.EncodeSegment(segment) }

type WriteRequest struct {
	Resource       string
	Document       *Document
	AdapterOptions *Document
}

type DeleteRequest struct {
	Resource       string
	AdapterOptions *Document
}

// AtomicTransformRequest requires exactly one of Program and BackendExpression.
type AtomicTransformRequest struct {
	Resource          string
	Program           *ProgramTransform
	BackendExpression *Document
	AdapterOptions    *Document
}

type NativeRequest struct {
	Resource      string
	Descriptor    *Document
	BodyMediaType string
	Body          []byte
}

// ReadOneOptions describes a single read. Use ReadOptions to share one RPC across
// multiple independent requests in the same Store.
type ReadOneOptions struct {
	StoreName string
	Request   *ReadRequest
}

// ReadOptions describes a finite read batch addressed to one Store. Resources
// are canonical paths relative to StoreName. Results have the input order. Do
// not mutate requests or their document bytes until the call returns.
type ReadOptions struct {
	StoreName string
	Requests  []*ReadRequest
}

// MutationAction selects one business operation without a protobuf oneof.
type MutationAction uint8

const (
	MutationCreate MutationAction = iota + 1
	MutationPut
	MutationReplace
	MutationDelete
	MutationAtomicTransform
)

// MutateRequest describes one mutation. Create, Put and Replace require Document;
// Delete accepts no payload; AtomicTransform requires Program or BackendExpression.
type MutateRequest struct {
	Resource          string
	Action            MutationAction
	Document          *Document
	Program           *ProgramTransform
	BackendExpression *Document
	AdapterOptions    *Document
}

// MutateOptions describes independent mutations addressed to one Store. It is
// not a transaction; use separate calls for operations that depend on each other.
// Do not mutate requests or their document bytes until the call returns.
type MutateOptions struct {
	StoreName string
	Requests  []*MutateRequest
}

type WriteOptions struct {
	StoreName string
	Request   *WriteRequest
}

type DeleteOptions struct {
	StoreName string
	Request   *DeleteRequest
}

type AtomicTransformOptions struct {
	StoreName string
	Request   *AtomicTransformRequest
}

// Scan executes one finite page. Consume must finish before the next document.
// A transport failure returns no checkpoint, even after a ScanEnd was observed.
type ScanOptions struct {
	StoreName string
	Request   *ScanRequest
	Consume   func(context.Context, *Document) error
}

// Native consumes bounded response events incrementally. A terminal NativeEnd
// remains evidence when a later transport failure is returned alongside it.
type NativeOptions struct {
	StoreName string
	Request   *NativeRequest
	Consume   func(context.Context, *Event) error
}

type ReadResult struct {
	Document *Document
	Missing  bool
	Failure  *Failure
}

func (r *ReadResult) GetDocument() *Document {
	if r == nil {
		return nil
	}
	return r.Document
}
func (r *ReadResult) GetMissing() bool { return r != nil && r.Missing }
func (r *ReadResult) GetFailure() *Failure {
	if r == nil {
		return nil
	}
	return r.Failure
}

type Result struct {
	Index    uint64
	Read     *ReadResult
	Mutation *MutationResult
}

func (r *Result) GetIndex() uint64 {
	if r == nil {
		return 0
	}
	return r.Index
}
func (r *Result) GetRead() *ReadResult {
	if r == nil {
		return nil
	}
	return r.Read
}
func (r *Result) GetMutation() *MutationResult {
	if r == nil {
		return nil
	}
	return r.Mutation
}

// Event contains exactly one validated business value. Native chunks are
// nonempty; a nil Chunk means a different event.
type Event struct {
	Result    *Result
	Document  *Document
	Head      *NativeHead
	Chunk     []byte
	ScanEnd   *ScanEnd
	NativeEnd *NativeEnd
}

func (e *Event) GetResult() *Result {
	if e == nil {
		return nil
	}
	return e.Result
}
func (e *Event) GetDocument() *Document {
	if e == nil {
		return nil
	}
	return e.Document
}
func (e *Event) GetHead() *NativeHead {
	if e == nil {
		return nil
	}
	return e.Head
}
func (e *Event) GetChunk() []byte {
	if e == nil {
		return nil
	}
	return e.Chunk
}
func (e *Event) GetScanEnd() *ScanEnd {
	if e == nil {
		return nil
	}
	return e.ScanEnd
}
func (e *Event) GetNativeEnd() *NativeEnd {
	if e == nil {
		return nil
	}
	return e.NativeEnd
}

func businessEvent(wire *pb.Event) *Event {
	event := &Event{}
	switch value := wire.Value.(type) {
	case *pb.Event_Result:
		result := &Result{Index: value.Result.Index, Mutation: value.Result.GetMutation()}
		if read := value.Result.GetRead(); read != nil {
			result.Read = &ReadResult{Document: read.GetDocument(), Missing: read.GetMissing() != nil, Failure: read.GetFailure()}
		}
		event.Result = result
	case *pb.Event_Document:
		event.Document = value.Document
	case *pb.Event_Head:
		event.Head = value.Head
	case *pb.Event_Chunk:
		event.Chunk = value.Chunk
	case *pb.Event_ScanEnd:
		event.ScanEnd = value.ScanEnd
	case *pb.Event_NativeEnd:
		event.NativeEnd = value.NativeEnd
	}
	return event
}

// Command is one business request in a finite Execute batch. Construct it with
// NewReadCommand, NewPutCommand, or another operation-specific constructor.
// Execute validates each command before sending it. Do not mutate its request
// or document bytes while execution runs.
type Command struct {
	wire    *pb.Command
	err     error
	payload []byte
	kind    string
}

func NewReadCommand(request *ReadRequest) *Command {
	variant := &pb.Command_Read{Read: request}
	wire := &pb.Command{Version: 1, Operation: variant}
	command := &Command{wire: wire}
	return command
}

func NewCreateCommand(request *WriteRequest) *Command {
	mutation := &pb.MutateRequest{}
	if request != nil {
		variant := &pb.MutateRequest_Create{Create: request.Document}
		mutation.Resource, mutation.AdapterOptions, mutation.Action = request.Resource, request.AdapterOptions, variant
	}
	return mutationCommand(mutation)
}

func NewPutCommand(request *WriteRequest) *Command {
	mutation := &pb.MutateRequest{}
	if request != nil {
		variant := &pb.MutateRequest_Put{Put: request.Document}
		mutation.Resource, mutation.AdapterOptions, mutation.Action = request.Resource, request.AdapterOptions, variant
	}
	return mutationCommand(mutation)
}

func NewReplaceCommand(request *WriteRequest) *Command {
	mutation := &pb.MutateRequest{}
	if request != nil {
		variant := &pb.MutateRequest_Replace{Replace: request.Document}
		mutation.Resource, mutation.AdapterOptions, mutation.Action = request.Resource, request.AdapterOptions, variant
	}
	return mutationCommand(mutation)
}

func NewDeleteCommand(request *DeleteRequest) *Command {
	mutation := &pb.MutateRequest{}
	if request != nil {
		empty := &pb.Empty{}
		variant := &pb.MutateRequest_Delete{Delete: empty}
		mutation.Resource, mutation.AdapterOptions, mutation.Action = request.Resource, request.AdapterOptions, variant
	}
	return mutationCommand(mutation)
}

func NewAtomicTransformCommand(request *AtomicTransformRequest) *Command {
	if request == nil || (request.Program == nil) == (request.BackendExpression == nil) {
		command := &Command{err: errors.New("AtomicTransform requires exactly one program or backend expression")}
		return command
	}
	mutation := &pb.MutateRequest{}
	if request != nil {
		transform := &pb.Transform{}
		if request.Program != nil && request.BackendExpression == nil {
			transform.Form = &pb.Transform_Program{Program: request.Program}
		} else if request.BackendExpression != nil && request.Program == nil {
			transform.Form = &pb.Transform_BackendExpression{BackendExpression: request.BackendExpression}
		}
		variant := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
		mutation.Resource, mutation.AdapterOptions, mutation.Action = request.Resource, request.AdapterOptions, variant
	}
	return mutationCommand(mutation)
}

func mutationCommand(mutation *pb.MutateRequest) *Command {
	variant := &pb.Command_Mutate{Mutate: mutation}
	wire := &pb.Command{Version: 1, Operation: variant}
	command := &Command{wire: wire}
	return command
}

func NewScanCommand(request *ScanRequest) *Command {
	variant := &pb.Command_Scan{Scan: request}
	wire := &pb.Command{Version: 1, Operation: variant}
	command := &Command{wire: wire}
	return command
}

func NewNativeCommand(request *NativeRequest) *Command {
	var native *pb.NativeRequest
	if request != nil {
		open := &pb.NativeOpen{Resource: request.Resource, Descriptor_: request.Descriptor, BodyMediaType: request.BodyMediaType}
		native = &pb.NativeRequest{Open: open, Body: request.Body}
	}
	variant := &pb.Command_Native{Native: native}
	wire := &pb.Command{Version: 1, Operation: variant}
	command := &Command{wire: wire}
	return command
}
