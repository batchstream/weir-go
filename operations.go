package weir

import (
	"context"
	"errors"
	"io"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

// Read executes one read without replay. A validated result remains evidence if
// a later frame or final RPC status is lost, and is returned with that error.
func Read(ctx context.Context, client pb.StoreServiceClient, options ReadOptions) (*ReadResult, error) {
	result, err := executeResult(ctx, client, options.StoreName, NewReadCommand(options.Request))
	if result == nil {
		return nil, err
	}
	return result.Read, err
}

// Create writes only when the resource does not exist. The MutationResult is
// backend evidence; an APPLIED acknowledgement remains valid with a transport
// error. Missing acknowledgements are indeterminate and are never replayed.
func Create(ctx context.Context, client pb.StoreServiceClient, options WriteOptions) (*MutationResult, error) {
	return executeMutation(ctx, client, options.StoreName, NewCreateCommand(options.Request))
}

// Put creates or replaces one resource without replaying a failed request.
func Put(ctx context.Context, client pb.StoreServiceClient, options WriteOptions) (*MutationResult, error) {
	return executeMutation(ctx, client, options.StoreName, NewPutCommand(options.Request))
}

// Replace writes only when the resource already exists.
func Replace(ctx context.Context, client pb.StoreServiceClient, options WriteOptions) (*MutationResult, error) {
	return executeMutation(ctx, client, options.StoreName, NewReplaceCommand(options.Request))
}

func Delete(ctx context.Context, client pb.StoreServiceClient, options DeleteOptions) (*MutationResult, error) {
	return executeMutation(ctx, client, options.StoreName, NewDeleteCommand(options.Request))
}

func AtomicTransform(ctx context.Context, client pb.StoreServiceClient, options AtomicTransformOptions) (*MutationResult, error) {
	return executeMutation(ctx, client, options.StoreName, NewAtomicTransformCommand(options.Request))
}

func executeMutation(ctx context.Context, client pb.StoreServiceClient, store string, command *Command) (*MutationResult, error) {
	result, err := executeResult(ctx, client, store, command)
	if result == nil {
		return nil, err
	}
	return result.Mutation, err
}

func executeResult(ctx context.Context, client pb.StoreServiceClient, store string, command *Command) (*Result, error) {
	produced := false
	var result *Result
	batch := ExecuteOptions{StoreName: store}
	batch.Produce = func(context.Context) (*Command, error) {
		if produced {
			return nil, io.EOF
		}
		produced = true
		return command, nil
	}
	batch.Consume = func(_ context.Context, _ uint64, event *Event) error { result = event.Result; return nil }
	err := Execute(ctx, client, batch)
	if err != nil {
		return result, err
	}
	if result == nil {
		return nil, errors.New("missing business result")
	}
	return result, nil
}

// Scan consumes one finite page incrementally. Only final RPC OK exposes its
// next checkpoint; retrying a failed page may redeliver documents and must use
// the previous token. The caller owns deduplication and output commits.
func Scan(ctx context.Context, client pb.StoreServiceClient, options ScanOptions) (*ScanEnd, error) {
	if options.Request == nil || options.Consume == nil {
		return nil, errors.New("Scan requires a request and document consumer")
	}
	command := NewScanCommand(options.Request)
	produced := false
	var end *ScanEnd
	batch := ExecuteOptions{StoreName: options.StoreName}
	batch.Produce = func(context.Context) (*Command, error) {
		if produced {
			return nil, io.EOF
		}
		produced = true
		return command, nil
	}
	batch.Consume = func(ctx context.Context, _ uint64, event *Event) error {
		if event.Document != nil {
			return options.Consume(ctx, event.Document)
		}
		end = event.ScanEnd
		return nil
	}
	if err := Execute(ctx, client, batch); err != nil {
		return nil, err
	}
	if end == nil {
		return nil, errors.New("missing Scan completion")
	}
	return end, nil
}

// Native consumes sequential head/chunk/end events without collecting a whole
// response. A backend error is native response data, not a mutation outcome.
func Native(ctx context.Context, client pb.StoreServiceClient, options NativeOptions) (*NativeEnd, error) {
	if options.Request == nil || options.Consume == nil {
		return nil, errors.New("Native requires a request and event consumer")
	}
	command := NewNativeCommand(options.Request)
	produced := false
	var end *NativeEnd
	batch := ExecuteOptions{StoreName: options.StoreName}
	batch.Produce = func(context.Context) (*Command, error) {
		if produced {
			return nil, io.EOF
		}
		produced = true
		return command, nil
	}
	batch.Consume = func(ctx context.Context, _ uint64, event *Event) error {
		if event.NativeEnd != nil {
			end = event.NativeEnd
		}
		return options.Consume(ctx, event)
	}
	err := Execute(ctx, client, batch)
	if err != nil {
		return end, err
	}
	if end == nil {
		return nil, errors.New("missing Native completion")
	}
	return end, nil
}
