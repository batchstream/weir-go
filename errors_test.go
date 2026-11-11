package weir

import (
	"context"
	"errors"
	"testing"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMutationErrAcceptsOnlyAppliedWithoutFailure(t *testing.T) {
	unavailable := protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend unavailable")
	precondition := protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "record exists")
	deadline := protocol.Fail(pb.FailureCode_DEADLINE_EXCEEDED, "deadline")
	cases := []struct {
		name        string
		result      *MutationResult
		ok          bool
		code        FailureCode
		interrupted bool
	}{
		{name: "applied", result: protocol.Mutation(pb.MutationOutcome_APPLIED, nil), ok: true},
		{name: "applied without visibility", result: protocol.Mutation(pb.MutationOutcome_APPLIED, unavailable), code: FailureUnavailable},
		{name: "not applied", result: protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, precondition), code: FailurePreconditionFailed},
		{name: "unknown after an outage", result: protocol.Mutation(pb.MutationOutcome_UNKNOWN, unavailable), code: FailureUnavailable, interrupted: true},
		{name: "unknown at the deadline", result: protocol.Mutation(pb.MutationOutcome_UNKNOWN, deadline), code: FailureDeadlineExceeded},
		{name: "no result", interrupted: true},
	}
	for _, test := range cases {
		err := MutationErr(test.result)
		if (err == nil) != test.ok || CodeOf(err) != test.code || errors.Is(err, ErrInterrupted) != test.interrupted {
			t.Error(test.name, err)
		}
		var failed *Error
		if err != nil && (!errors.As(err, &failed) || failed.Outcome != test.result.GetOutcome() && test.result != nil) {
			t.Error(test.name, "lost its outcome", err)
		}
	}
	text := MutationErr(protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, precondition)).Error()
	if text != "weir: mutation NOT_APPLIED: PRECONDITION_FAILED: record exists" {
		t.Error(text)
	}
}

func TestReadAndNativeErrs(t *testing.T) {
	unavailable := protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend unavailable")
	invalid := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid")
	head := &NativeResponse{}
	found := &ReadResult{Document: &Document{}}
	missing := &ReadResult{Missing: true}
	failedRead := &ReadResult{Failure: unavailable}
	var noRead *ReadResult
	complete := &NativeResult{Response: head, Completion: NativeResponseComplete}
	refused := &NativeResult{Completion: NativeNotStarted, Failure: unavailable}
	lost := &NativeResult{Response: head, Completion: NativeResponseIncomplete, Failure: unavailable}
	invalidNative := &NativeResult{Completion: NativeNotStarted, Failure: invalid}
	endless := &NativeResult{Response: head}
	var noNative *NativeResult
	cases := []struct {
		name        string
		err         error
		ok          bool
		code        FailureCode
		interrupted bool
	}{
		{name: "found read", err: found.Err(), ok: true},
		{name: "missing read", err: missing.Err(), ok: true},
		{name: "failed read", err: failedRead.Err(), code: FailureUnavailable},
		{name: "no read result", err: noRead.Err(), interrupted: true},
		{name: "complete Native", err: complete.Err(), ok: true},
		{name: "refused Native", err: refused.Err(), code: FailureUnavailable},
		{name: "Native that lost its backend", err: lost.Err(), code: FailureUnavailable, interrupted: true},
		{name: "invalid Native", err: invalidNative.Err(), code: FailureInvalidArgument},
		{name: "Native without its end", err: endless.Err(), interrupted: true},
		{name: "no Native result", err: noNative.Err(), interrupted: true},
	}
	for _, test := range cases {
		if (test.err == nil) != test.ok || CodeOf(test.err) != test.code || errors.Is(test.err, ErrInterrupted) != test.interrupted {
			t.Error(test.name, test.err)
		}
	}
	if CodeOf(errors.New("other")) != 0 || CodeOf(nil) != 0 {
		t.Error("code of an error without a Failure")
	}
}

func TestLostConnectionIsInterruptedAndKeepsItsStatus(t *testing.T) {
	lost := status.Error(codes.Unavailable, "connection lost")
	err := callError(t.Context(), lost)
	if !errors.Is(err, ErrInterrupted) || status.Code(err) != codes.Unavailable {
		t.Fatal("lost connection not marked", err)
	}
	other := status.Error(codes.Internal, "server failed")
	if err := callError(t.Context(), other); errors.Is(err, ErrInterrupted) {
		t.Fatal("another status was marked", err)
	}
	ended, cancel := context.WithCancel(t.Context())
	cancel()
	if err := callError(ended, lost); err != context.Canceled {
		t.Fatal("an ended context was marked", err)
	}
}
