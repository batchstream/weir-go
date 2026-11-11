package weir

import (
	"errors"
	"fmt"
)

// ErrNotFound reports a record that does not exist. Read reports one as
// ReadResult.Missing; the mongodb and search packages return ErrNotFound.
var ErrNotFound = errors.New("weir: record not found")

// ErrInterrupted marks a request cut short by an outage after it may have
// begun: the connection to Weir broke, a mutation ended UNKNOWN with
// UNAVAILABLE, or a Native request that began ended with UNAVAILABLE or
// without its end. Whether the backend ran it is unknown, so
// send it again only when repeating it is safe, as it is for a read. A request
// a server refused before it began is not interrupted: it took no effect, and
// the Client already sent it again. Neither is a request that failed for a
// reason of its own, or whose context ended.
var ErrInterrupted = errors.New("weir: interrupted")

// Error is a Failure Weir reported for one record or Native request.
type Error struct {
	// Outcome is the outcome of a mutation, and zero for a read or Native
	// request.
	Outcome MutationOutcome
	// Failure is nil when the request has no result or its Native response
	// stopped early without one.
	Failure *Failure

	reason      string
	interrupted bool
}

func (e *Error) Error() string {
	text := "weir"
	if e.Outcome != 0 {
		text += ": mutation " + e.Outcome.String()
	}
	if e.Failure != nil {
		text += fmt.Sprintf(": %s: %s", e.Failure.GetCode(), e.Failure.GetMessage())
	}
	if e.reason != "" {
		text += ": " + e.reason
	}
	return text
}

// Unwrap returns ErrInterrupted for an interrupted request.
func (e *Error) Unwrap() error {
	if e.interrupted {
		return ErrInterrupted
	}
	return nil
}

// CodeOf returns the FailureCode of the first *Error in err's chain, or zero
// when there is none or it has no Failure.
func CodeOf(err error) FailureCode {
	var failed *Error
	if !errors.As(err, &failed) {
		return 0
	}
	return failed.Failure.GetCode()
}

// MutationErr returns nil for an APPLIED result without a Failure, and an
// *Error otherwise. APPLIED with a Failure has applied, so never send it again.
// A nil result, a position a broken call left unconfirmed, may have applied.
func MutationErr(result *MutationResult) error {
	if result == nil {
		failed := &Error{Outcome: MutationUnknown, reason: "without a result", interrupted: true}
		return failed
	}
	outcome := result.GetOutcome()
	if outcome == MutationApplied && result.GetFailure() == nil {
		return nil
	}
	interrupted := outcome == MutationUnknown && result.GetFailure().GetCode() == FailureUnavailable
	failed := &Error{Outcome: outcome, Failure: result.GetFailure(), interrupted: interrupted}
	return failed
}

// Err returns nil for a read that returned a document or confirmed it
// missing, and an *Error for a read that failed or has no result.
func (r *ReadResult) Err() error {
	if r == nil {
		failed := &Error{reason: "read without a result", interrupted: true}
		return failed
	}
	if r.Failure != nil {
		failed := &Error{Failure: r.Failure}
		return failed
	}
	return nil
}

// Err returns nil for a Native request whose response arrived complete, and an
// *Error otherwise. The response's own status, such as an HTTP 404 or a
// MongoDB reply with ok: 0, is data for the caller and never an Err.
func (r *NativeResult) Err() error {
	if r == nil {
		failed := &Error{reason: "native request without a result", interrupted: true}
		return failed
	}
	if r.Failure != nil {
		interrupted := r.Completion != NativeNotStarted && r.Failure.GetCode() == FailureUnavailable
		failed := &Error{Failure: r.Failure, interrupted: interrupted}
		return failed
	}
	// A terminal that is not complete carries a Failure, so this response
	// stopped without one.
	if r.Completion != NativeResponseComplete {
		failed := &Error{reason: "native response without its end", interrupted: true}
		return failed
	}
	return nil
}
