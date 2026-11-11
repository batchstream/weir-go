package weir

import (
	"context"
	"math/rand/v2"
	"sync/atomic"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

// A Client sends a record again when a server refused it before it began, as
// a draining or memory-pressured server or an unavailable backend does: a
// mutation NOT_STARTED, a failed read or a Native NOT_STARTED whose Failure is
// UNAVAILABLE. It also sends again a mutation NOT_APPLIED with CONFLICT, which
// lost every compare-and-swap or transaction attempt Weir made against
// concurrent writers of its record. Such a record took no effect, so sending
// it again cannot apply it twice. RESOURCE_EXHAUSTED reports a budget of the
// request itself, which a resend would exceed again. Every other result
// reaches the caller as it came.
const (
	// resendAttempts bounds the sends of one record, the first included.
	resendAttempts = 4
	// resendBackoff is the longest wait before the first resend; it doubles
	// for each later one and is jittered down to half.
	resendBackoff = 50 * time.Millisecond
	// Resends across a Client stay within one per resendShare first sends,
	// plus a burst of resendBurst, so a server that keeps refusing never
	// receives several times the traffic it refuses.
	resendShare = 10
	resendBurst = 100
)

// resendCause tells why a record may be sent again. Each cause spends a budget
// of its own, so records that keep losing to concurrent writers never use up
// the resends of records a draining server refused.
type resendCause int

const (
	// noResend marks a result that reaches the caller as it came.
	noResend resendCause = iota
	// resendRefusal marks a record a server refused before it began.
	resendRefusal
	// resendConflict marks a mutation that lost to concurrent writers.
	resendConflict
	resendCauses
)

// resendBudget holds resend credit for each cause; one resend costs
// resendShare credit of its cause and every first send earns one of each.
type resendBudget struct {
	credit [resendCauses]atomic.Int64
}

func newResendBudget() *resendBudget {
	budget := &resendBudget{}
	for cause := range budget.credit {
		budget.credit[cause].Store(resendShare * resendBurst)
	}
	return budget
}

func (b *resendBudget) earn(credits int64) {
	for cause := range b.credit {
		b.refund(resendCause(cause), credits)
	}
}

// refund adds credit to one cause, up to the burst.
func (b *resendBudget) refund(cause resendCause, credits int64) {
	credit := &b.credit[cause]
	for {
		current := credit.Load()
		next := min(current+credits, resendShare*resendBurst)
		if next == current || credit.CompareAndSwap(current, next) {
			return
		}
	}
}

// spend takes the credit for one resend of a cause and reports whether there
// was enough.
func (b *resendBudget) spend(cause resendCause) bool {
	credit := &b.credit[cause]
	for {
		current := credit.Load()
		if current < resendShare {
			return false
		}
		if credit.CompareAndSwap(current, current-resendShare) {
			return true
		}
	}
}

// resend reports whether a record may be sent again for a cause as the given
// attempt, after waiting its backoff. It may not once the attempts or the
// cause's budget run out, or when the caller's deadline would not leave the
// longest wait again for the send, which could otherwise turn a result that
// took no effect into a deadline error that may have taken effect.
func (c *Client) resend(ctx context.Context, attempt int, cause resendCause) bool {
	if c.resends == nil || attempt > resendAttempts || !c.resends.spend(cause) {
		return false
	}
	longest := resendBackoff << (attempt - 2)
	wait := longest/2 + rand.N(longest/2+1)
	if deadline, bounded := ctx.Deadline(); bounded && time.Until(deadline) <= wait+longest {
		c.resends.refund(cause, resendShare)
		return false
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		c.resends.refund(cause, resendShare)
		return false
	}
}

// earn credits the budget for records sent for the first time.
func (c *Client) earn() {
	if c.resends != nil {
		c.resends.earn(1)
	}
}

// withResends sends one record until its result calls for no resend, the
// attempts or budget run out, or ctx ends, and returns the last outcome.
func withResends[T any](ctx context.Context, c *Client, cause func(T) resendCause, send func() (T, error)) (T, error) {
	c.earn()
	for attempt := 2; ; attempt++ {
		result, err := send()
		if err != nil {
			return result, err
		}
		resent := cause(result)
		if resent == noResend || !c.resend(ctx, attempt, resent) {
			return result, nil
		}
	}
}

// onRefusal resends, as refusals, the results refused reports.
func onRefusal[T any](refused func(T) bool) func(T) resendCause {
	cause := func(result T) resendCause {
		if refused(result) {
			return resendRefusal
		}
		return noResend
	}
	return cause
}

// refusal reports a Failure with which a server turns away work it may take
// later.
func refusal(failure *pb.Failure) bool {
	return failure.GetCode() == pb.FailureCode_UNAVAILABLE
}

func mutationRefused(result *MutationResult) bool {
	return result.GetOutcome() == pb.MutationOutcome_NOT_STARTED && refusal(result.GetFailure())
}

// mutationResend returns why a mutation that took no effect may apply when sent
// again: a server refused it, or it lost to concurrent writers.
func mutationResend(result *MutationResult) resendCause {
	switch {
	case mutationRefused(result):
		return resendRefusal
	case result.GetOutcome() == pb.MutationOutcome_NOT_APPLIED && result.GetFailure().GetCode() == pb.FailureCode_CONFLICT:
		return resendConflict
	}
	return noResend
}

func readRefused(result *ReadResult) bool {
	return result != nil && refusal(result.Failure)
}

func nativeRefused(result *NativeResult) bool {
	return result != nil && result.Completion == NativeNotStarted && result.Response == nil && refusal(result.Failure)
}
