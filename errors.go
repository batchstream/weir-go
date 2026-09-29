package weir

import (
	"fmt"
	pb "github.com/batchstream/weir/api/weir/v1"
)

// ProtocolError means the peer reply cannot establish valid completion evidence.
type ProtocolError struct{ Message string }

func (e *ProtocolError) Error() string { return "weir: invalid response: " + e.Message }
func protocolError(message string) error {
	err := &ProtocolError{Message: message}
	return err
}

// FailureError preserves the server's application failure code and message.
type FailureError struct{ Failure *pb.Failure }

func (e *FailureError) Error() string {
	return fmt.Sprintf("weir: %s: %s", e.Failure.Code, e.Failure.Message)
}
func failureError(f *pb.Failure) error {
	if f == nil {
		return nil
	}
	if f.Code < pb.FailureCode_INVALID_ARGUMENT || f.Code > pb.FailureCode_INTERNAL || len(f.Message) > 1024 {
		return protocolError("invalid failure envelope")
	}
	err := &FailureError{Failure: f}
	return err
}

// MutationError always preserves a mutation outcome. UNKNOWN requires external
// reconciliation; it is never a signal to replay a request.
type MutationError struct {
	Outcome pb.MutationOutcome
	Cause   error
}

func (e *MutationError) Error() string {
	return fmt.Sprintf("weir: mutation %s: %v", e.Outcome, e.Cause)
}
func (e *MutationError) Unwrap() error { return e.Cause }

// BatchError includes per-index failures and an optional whole-stream cause.
// When Cause is non-nil, nil entries in the returned results have no terminal
// evidence. Treat every such mutation as UNKNOWN, including unreported sends.
type BatchError struct {
	Failures map[int]error
	Cause    error
}

func (e *BatchError) Error() string {
	return fmt.Sprintf("weir: bulk: %d operation failures; stream error: %v", len(e.Failures), e.Cause)
}
func (e *BatchError) Unwrap() error { return e.Cause }

func readError(result *pb.ReadResult) error {
	if result == nil {
		return protocolError("missing Read result")
	}
	switch value := result.Result.(type) {
	case *pb.ReadResult_Document:
		if validateDocument(value.Document) != nil {
			return protocolError("invalid Read document")
		}
	case *pb.ReadResult_Missing:
		if value.Missing == nil {
			return protocolError("nil missing marker")
		}
	case *pb.ReadResult_Failure:
		if value.Failure == nil {
			return protocolError("nil Read failure")
		}
		return failureError(value.Failure)
	default:
		return protocolError("missing Read variant")
	}
	return nil
}

func mutationError(result *pb.MutationResult) error {
	if result == nil || result.Outcome < pb.MutationOutcome_NOT_STARTED || result.Outcome > pb.MutationOutcome_UNKNOWN {
		return protocolError("invalid mutation outcome")
	}
	cause := failureError(result.Failure)
	if _, invalid := cause.(*ProtocolError); invalid {
		return cause
	}
	if cause == nil && (result.Outcome == pb.MutationOutcome_APPLIED || result.Outcome == pb.MutationOutcome_NOT_APPLIED) {
		return nil
	}
	if cause == nil {
		cause = fmt.Errorf("no successful mutation acknowledgement")
	}
	err := &MutationError{Outcome: result.Outcome, Cause: cause}
	return err
}
