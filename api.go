package weir

import (
	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

// Leaf DTOs preserve document bytes and backend evidence without exposing any
// transport framing or protobuf operation selection.
type Document = pb.Document
type Failure = pb.Failure

// FailureCode identifies a declared operation failure. Only known nonzero codes
// are accepted. A Client sends a record again only for FailureUnavailable on
// a record that never began, or FailureConflict on a mutation NOT_APPLIED.
type FailureCode = pb.FailureCode
type MutationResult = pb.MutationResult
type MutationOutcome = pb.MutationOutcome

type LuaTransform = pb.LuaTransform
type ReadRequest = pb.ReadRequest

// NativeRequest carries an opaque adapter-owned document. Keep its bytes
// immutable until Native returns. Empty request data is permitted. Resource is
// an optional target prefix; empty selects the backend root when supported.
type NativeRequest = pb.NativeRequest
type NativeResponse = pb.NativeHead
type NativeCompletion = pb.NativeCompletion

const (
	MutationNotStarted = pb.MutationOutcome_NOT_STARTED
	MutationNotApplied = pb.MutationOutcome_NOT_APPLIED
	MutationApplied    = pb.MutationOutcome_APPLIED
	MutationUnknown    = pb.MutationOutcome_UNKNOWN

	NativeCompletionUnconfirmed = pb.NativeCompletion_NATIVE_COMPLETION_UNSPECIFIED
	NativeNotStarted            = pb.NativeCompletion_NATIVE_NOT_STARTED
	NativeResponseComplete      = pb.NativeCompletion_RESPONSE_COMPLETE
	NativeResponseIncomplete    = pb.NativeCompletion_RESPONSE_INCOMPLETE

	FailureInvalidArgument    = pb.FailureCode_INVALID_ARGUMENT
	FailureUnauthenticated    = pb.FailureCode_UNAUTHENTICATED
	FailurePermissionDenied   = pb.FailureCode_PERMISSION_DENIED
	FailureTargetNotFound     = pb.FailureCode_TARGET_NOT_FOUND
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

// NativeResult retains validated response and completion evidence. Completion is
// NativeCompletionUnconfirmed until a terminal acknowledgement arrives; Response
// may already be available. An RPC error never revokes a confirmed terminal.
// Completion describes response transport, not whether backend writes applied.
// Body holds the response body of a call without WithChunks, as far as it arrived.
type NativeResult struct {
	Response   *NativeResponse
	Body       []byte
	Completion NativeCompletion
	Failure    *Failure
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
// Delete accepts no payload; AtomicTransform requires Lua or BackendExpression.
type MutateRequest struct {
	Resource          string
	Action            MutationAction
	Document          *Document
	Lua               *LuaTransform
	BackendExpression *Document
}

// ReadResult contains exactly one of Document, Missing, or Failure. Missing
// confirms document absence after a successful read. FailureTargetNotFound
// reports a missing adapter-owned target, never a missing document.
type ReadResult struct {
	Document *Document
	Missing  bool
	Failure  *Failure
}
