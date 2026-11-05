package weir

import (
	"context"
	"slices"

	"google.golang.org/grpc"
)

// ClientOption configures a Client.
type ClientOption func(*clientOptions)

type clientOptions struct {
	dial []grpc.DialOption
}

// WithDialOptions adds gRPC options to a Client's connection, such as a stats
// handler, an interceptor or a custom dialer. They can replace its defaults for
// transport credentials, keepalive and message sizes. They cannot change what a
// Client relies on: gRPC never retries, service configs have no effect, and its
// own resolver and balancer follow its servers. Several WithDialOptions add their
// options in order.
func WithDialOptions(options ...grpc.DialOption) ClientOption {
	option := func(config *clientOptions) { config.dial = append(config.dial, options...) }
	return option
}

// Option configures one Read or Mutate call whose requests have type Q.
type Option[Q any] func(*callOptions[Q])

// ReadOption configures a Read.
type ReadOption = Option[*ReadRequest]

// MutateOption configures a Mutate.
type MutateOption = Option[*MutateRequest]

// NativeOption configures a Native call.
type NativeOption func(*nativeOptions)

type callOptions[Q any] struct {
	requests      []Q
	next          func(context.Context) (Q, error)
	consumeRead   func(context.Context, int, *ReadResult) error
	consumeMutate func(context.Context, int, *MutationResult) error
	queryVisible  bool
}

type nativeOptions struct {
	chunks func(context.Context, *NativeResponse, []byte) error
}

// WithRequests adds requests the call already holds. They are all validated
// before anything is sent, so an invalid one rejects the whole call and WithNext
// is never called. Several WithRequests add their requests in order. Keep the
// requests and their document bytes unchanged until the call returns, and after
// a call its context ended do not reuse them at all: a single record's shared
// stream may still be reading them.
func WithRequests[Q any](requests ...Q) Option[Q] {
	option := func(options *callOptions[Q]) {
		if options.requests == nil {
			options.requests = slices.Clip(requests)
			return
		}
		options.requests = append(options.requests, requests...)
	}
	return option
}

// WithNext produces requests one at a time, after those of WithRequests have
// entered the window of 32 records and 8 MiB, without waiting for their results.
// Next returns io.EOF after the last request. Each request is validated as it is
// produced. An invalid request or another error from Next ends the input there:
// the records before it still finish, and the call then returns that error.
// Next runs concurrently with a consume callback, so synchronize what they share.
// It must honor its context, and return promptly once that ends. Keep a produced
// request and its document bytes unchanged until the call returns, or with a
// consume callback until its result arrives there.
//
// Without a consume callback, results accumulate until the call returns, and
// memory grows with the number of records.
func WithNext[Q any](next func(ctx context.Context) (Q, error)) Option[Q] {
	option := func(options *callOptions[Q]) { options.next = next }
	return option
}

// WithReadConsume receives each read's result as its record completes, instead
// of the call collecting them, and Read returns nil results. The index is the
// record's position in the input, from 0. Results delivered before a later error
// remain valid. Calls run one at a time and hold back the input when slow; an
// error from consume ends the call. It must honor its context, as a call does not
// return before it does.
func WithReadConsume(consume func(ctx context.Context, index int, result *ReadResult) error) ReadOption {
	option := func(options *callOptions[*ReadRequest]) { options.consumeRead = consume }
	return option
}

// WithMutateConsume receives each mutation's result as its record completes,
// instead of the call collecting them, and Mutate returns nil results. The index
// is the record's position in the input, from 0. Each result is an individual
// acknowledgement that a later error does not revoke; records without a result
// may have applied. Calls run one at a time and hold back the input when slow; an
// error from consume ends the call. It must honor its context, as a call does not
// return before it does.
func WithMutateConsume(consume func(ctx context.Context, index int, result *MutationResult) error) MutateOption {
	option := func(options *callOptions[*MutateRequest]) { options.consumeMutate = consume }
	return option
}

// WithQueryVisible requires every mutation of the call to be visible to later
// normal queries before an APPLIED result without Failure. APPLIED with Failure
// confirms application only. Without it, a mutation completes on application
// acknowledgement, with no separate visibility guarantee.
func WithQueryVisible() MutateOption {
	option := func(options *callOptions[*MutateRequest]) { options.queryVisible = true }
	return option
}

// WithChunks receives a Native response's body incrementally, instead of the call
// collecting it into NativeResult.Body. Each nonempty chunk finishes before the
// next one, and consume must honor its context. An empty response invokes no
// callback. Do not mutate the response metadata, which stays attached to the
// result.
func WithChunks(consume func(ctx context.Context, response *NativeResponse, chunk []byte) error) NativeOption {
	option := func(options *nativeOptions) { options.chunks = consume }
	return option
}

func applyOptions[Q any](options []Option[Q]) callOptions[Q] {
	applied := callOptions[Q]{}
	for _, option := range options {
		if option != nil {
			option(&applied)
		}
	}
	return applied
}
