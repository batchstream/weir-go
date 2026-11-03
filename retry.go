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
// UNAVAILABLE. Such a record took no effect, so sending it again cannot apply
// it twice. RESOURCE_EXHAUSTED reports a budget of the request itself, which a
// resend would exceed again. Every other result reaches the caller as it came.
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

// resendBudget holds resend credit; one resend costs resendShare credit and
// every first send earns one.
type resendBudget struct {
	credit atomic.Int64
}

func newResendBudget() *resendBudget {
	budget := &resendBudget{}
	budget.credit.Store(resendShare * resendBurst)
	return budget
}

func (b *resendBudget) earn(credits int64) {
	for {
		credit := b.credit.Load()
		next := min(credit+credits, resendShare*resendBurst)
		if next == credit || b.credit.CompareAndSwap(credit, next) {
			return
		}
	}
}

// spend takes the credit for one resend and reports whether there was enough.
func (b *resendBudget) spend() bool {
	for {
		credit := b.credit.Load()
		if credit < resendShare {
			return false
		}
		if b.credit.CompareAndSwap(credit, credit-resendShare) {
			return true
		}
	}
}

// resend reports whether a refused record may be sent as the given attempt,
// after waiting its backoff. It may not once the attempts or the budget run
// out, or when the caller's deadline would not leave the longest wait again
// for the send, which could otherwise turn a refusal into a deadline error
// that may have taken effect.
func (c *Client) resend(ctx context.Context, attempt int) bool {
	if c.resends == nil || attempt > resendAttempts || !c.resends.spend() {
		return false
	}
	longest := resendBackoff << (attempt - 2)
	wait := longest/2 + rand.N(longest/2+1)
	if deadline, bounded := ctx.Deadline(); bounded && time.Until(deadline) <= wait+longest {
		c.resends.earn(resendShare)
		return false
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		c.resends.earn(resendShare)
		return false
	}
}

// earn credits the budget for records sent for the first time.
func (c *Client) earn() {
	if c.resends != nil {
		c.resends.earn(1)
	}
}

// resendRefused sends one record until a server does not refuse it, the
// attempts or budget run out, or ctx ends, and returns the last outcome.
func resendRefused[T any](ctx context.Context, c *Client, refused func(T) bool, send func() (T, error)) (T, error) {
	c.earn()
	for attempt := 2; ; attempt++ {
		result, err := send()
		if err != nil || !refused(result) || !c.resend(ctx, attempt) {
			return result, err
		}
	}
}

// refusal reports a Failure with which a server turns away work it may take
// later.
func refusal(failure *pb.Failure) bool {
	return failure.GetCode() == pb.FailureCode_UNAVAILABLE
}

func mutationRefused(result *MutationResult) bool {
	return result.GetOutcome() == pb.MutationOutcome_NOT_STARTED && refusal(result.GetFailure())
}

func readRefused(result *ReadResult) bool {
	return result != nil && refusal(result.Failure)
}

func nativeRefused(result *NativeResult) bool {
	return result != nil && result.Completion == NativeNotStarted && result.Response == nil && refusal(result.Failure)
}
