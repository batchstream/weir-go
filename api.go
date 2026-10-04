package weir

import (
	"context"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

// Leaf DTOs preserve document bytes and backend evidence without exposing any
// transport framing or protobuf operation selection.
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
	Resource string
	Document *Document
}

type DeleteRequest struct {
	Resource string
}

// AtomicTransformRequest requires exactly one of Program and BackendExpression.
type AtomicTransformRequest struct {
	Resource          string
	Program           *ProgramTransform
	BackendExpression *Document
}

type NativeRequest struct {
	Resource        string
	Descriptor      *Document
	BodyContentType string
	Body            []byte
}

// ReadOneOptions describes a single read. Use ReadOptions to share one RPC across
// multiple independent requests in the same Store.
type ReadOneOptions struct {
	StoreName string
	Request   *ReadRequest
}

// ReadOptions describes a finite read sequence addressed to one Store. Resources
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
}

// MutateOptions describes independent mutations addressed to one Store. It is
// not a transaction. Same-resource requests execute in input order; different
// resources may execute concurrently. Cross-RPC ordering follows the database.
// Do not mutate requests or their document bytes until the call returns.
type MutateOptions struct {
	StoreName string
	Requests  []*MutateRequest
}

// ReadStreamOptions produces a finite sequence without retaining every input or
// result. Next returns io.EOF after its final item. Items are validated before
// they are sent; an invalid later item does not undo earlier requests.
// Consume receives each confirmed result in ordinal order, starting at 1.
// Next and Consume run concurrently; synchronize any shared application state.
// Both callbacks must honor their context and return promptly. Keep yielded
// requests and document bytes immutable until the corresponding Consume call.
type ReadStreamOptions struct {
	StoreName string
	Next      func(context.Context) (*ReadRequest, error)
	Consume   func(context.Context, uint64, *ReadResult) error
}

// MutateStreamOptions incrementally produces mutations to one Store. Next
// returns io.EOF after the final item. Consume receives confirmed results in
// ordinal order; a later error does not revoke those acknowledgements. Items
// not consumed remain unacknowledged and must never be automatically replayed.
// Next and Consume run concurrently; synchronize any shared application state.
// Both callbacks must honor their context and return promptly. Keep yielded
// requests and document bytes immutable until the corresponding Consume call.
type MutateStreamOptions struct {
	StoreName string
	Next      func(context.Context) (*MutateRequest, error)
	Consume   func(context.Context, uint64, *MutationResult) error
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

// Event contains exactly one validated Native response value. Chunks are
// nonempty; a nil Chunk means a different event.
type Event struct {
	Head      *NativeHead
	Chunk     []byte
	NativeEnd *NativeEnd
}
