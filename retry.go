package weir

import (
	"context"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

// A Client sends a record again when a server refused it before it began, as
// a draining or memory-pressured server or an unavailable backend does: a
// mutation NOT_STARTED, a failed read or a Native NOT_STARTED whose Failure is
// UNAVAILABLE or RESOURCE_EXHAUSTED. Such a record took no effect, so sending
// it again cannot apply it twice. Every other result reaches the caller as it
// came.
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

func (b *resendBudget) earn(records int) {
	for {
		credit := b.credit.Load()
		next := min(credit+int64(records), resendShare*resendBurst)
		if next == credit || b.credit.CompareAndSwap(credit, next) {
			return
		}
	}
}

// spend takes credit for up to records resends and returns how many it covers.
func (b *resendBudget) spend(records int) int {
	for {
		credit := b.credit.Load()
		covered := min(int64(records), credit/resendShare)
		if covered == 0 || b.credit.CompareAndSwap(credit, credit-covered*resendShare) {
			return int(covered)
		}
	}
}

// resend reports how many of records refused records may be sent as the
// given attempt, after waiting its backoff. It covers none once the attempts
// or the budget run out, or when the caller's context would end first.
func (c *Client) resend(ctx context.Context, attempt, records int) int {
	if c.resends == nil || attempt > resendAttempts || records == 0 {
		return 0
	}
	covered := c.resends.spend(records)
	if covered == 0 {
		return 0
	}
	longest := resendBackoff << (attempt - 2)
	wait := longest/2 + rand.N(longest/2+1)
	if deadline, bounded := ctx.Deadline(); bounded && time.Until(deadline) <= wait {
		c.resends.earn(covered * resendShare)
		return 0
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return covered
	case <-ctx.Done():
		c.resends.earn(covered * resendShare)
		return 0
	}
}

// earn credits the budget for records sent for the first time.
func (c *Client) earn(records int) {
	if c.resends != nil {
		c.resends.earn(records)
	}
}

// resendRefused sends one record until a server does not refuse it, the
// attempts or budget run out, or ctx ends, and returns the last outcome.
func resendRefused[T any](ctx context.Context, c *Client, refused func(T) bool, send func() (T, error)) (T, error) {
	c.earn(1)
	for attempt := 2; ; attempt++ {
		result, err := send()
		if err != nil || !refused(result) || c.resend(ctx, attempt, 1) == 0 {
			return result, err
		}
	}
}

// resendBatch sends the positions refused returns again, in order, until none
// is left, the attempts or budget run out, or a resend fails. It sends a
// prefix when the budget covers only part of them.
func (c *Client) resendBatch(ctx context.Context, refused func() []int, send func([]int) error) error {
	for attempt := 2; ; attempt++ {
		positions := refused()
		positions = positions[:c.resend(ctx, attempt, len(positions))]
		if len(positions) == 0 {
			return nil
		}
		if err := send(positions); err != nil {
			return err
		}
	}
}

// refusal reports a Failure with which a server turns away work before it
// begins.
func refusal(failure *pb.Failure) bool {
	code := failure.GetCode()
	return code == pb.FailureCode_UNAVAILABLE || code == pb.FailureCode_RESOURCE_EXHAUSTED
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

// resendableMutations returns, in input order, the refused mutations that can
// be sent again without reordering a resource: those whose later requests for
// the same resource were all refused too.
func resendableMutations(requests []*MutateRequest, results []*MutationResult) []int {
	settled := make(map[string]bool)
	var positions []int
	for i := len(requests) - 1; i >= 0; i-- {
		resource := requests[i].Resource
		if !mutationRefused(results[i]) {
			settled[resource] = true
			continue
		}
		if !settled[resource] {
			positions = append(positions, i)
		}
	}
	slices.Reverse(positions)
	return positions
}

func resendableReads(results []*ReadResult) []int {
	var positions []int
	for i, result := range results {
		if readRefused(result) {
			positions = append(positions, i)
		}
	}
	return positions
}
