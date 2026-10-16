package weir

import (
	"context"
	"errors"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

// ReadOne sends one read without replay.
func ReadOne(ctx context.Context, client pb.StoreServiceClient, options ReadOneOptions) (*ReadResult, error) {
	batch := ReadOptions{StoreName: options.StoreName, Requests: []*ReadRequest{options.Request}}
	results, err := Read(ctx, client, batch)
	if len(results) == 0 {
		return nil, err
	}
	return results[0], err
}

func Create(ctx context.Context, client pb.StoreServiceClient, options WriteOptions) (*MutationResult, error) {
	return writeOne(ctx, client, options, MutationCreate)
}
func Put(ctx context.Context, client pb.StoreServiceClient, options WriteOptions) (*MutationResult, error) {
	return writeOne(ctx, client, options, MutationPut)
}
func Replace(ctx context.Context, client pb.StoreServiceClient, options WriteOptions) (*MutationResult, error) {
	return writeOne(ctx, client, options, MutationReplace)
}

func writeOne(ctx context.Context, client pb.StoreServiceClient, options WriteOptions, action MutationAction) (*MutationResult, error) {
	var request *MutateRequest
	if options.Request != nil {
		request = &MutateRequest{Resource: options.Request.Resource, Action: action, Document: options.Request.Document, Options: options.Request.Options}
	}
	batch := MutateOptions{StoreName: options.StoreName, Requests: []*MutateRequest{request}}
	return mutateOne(ctx, client, batch)
}

func Delete(ctx context.Context, client pb.StoreServiceClient, options DeleteOptions) (*MutationResult, error) {
	var request *MutateRequest
	if options.Request != nil {
		request = &MutateRequest{Resource: options.Request.Resource, Action: MutationDelete, Options: options.Request.Options}
	}
	batch := MutateOptions{StoreName: options.StoreName, Requests: []*MutateRequest{request}}
	return mutateOne(ctx, client, batch)
}

func AtomicTransform(ctx context.Context, client pb.StoreServiceClient, options AtomicTransformOptions) (*MutationResult, error) {
	var request *MutateRequest
	if options.Request != nil {
		request = &MutateRequest{Resource: options.Request.Resource, Action: MutationAtomicTransform, Lua: options.Request.Lua, BackendExpression: options.Request.BackendExpression, Options: options.Request.Options}
	}
	batch := MutateOptions{StoreName: options.StoreName, Requests: []*MutateRequest{request}}
	return mutateOne(ctx, client, batch)
}

func mutateOne(ctx context.Context, client pb.StoreServiceClient, options MutateOptions) (*MutationResult, error) {
	results, err := Mutate(ctx, client, options)
	if len(results) == 0 {
		return nil, err
	}
	return results[0], err
}

// Native consumes response bytes incrementally. Backend errors remain native
// response data. A validated result remains evidence alongside later RPC errors.
func Native(ctx context.Context, client pb.StoreServiceClient, options NativeOptions) (*NativeResult, error) {
	if options.Request == nil || options.Consume == nil {
		return nil, errors.New("Native requires a request and byte consumer")
	}
	variant := &pb.Command_Native{Native: options.Request}
	command := &pb.Command{Operation: variant}
	request := &pb.ExecuteRequest{StoreName: options.StoreName, Index: 1, Command: command}
	var result *NativeResult
	consume := func(ctx context.Context, wire *pb.Event) error {
		switch value := wire.Value.(type) {
		case *pb.Event_Head:
			result = &NativeResult{Response: value.Head}
		case *pb.Event_Chunk:
			return options.Consume(ctx, result.Response, value.Chunk)
		case *pb.Event_NativeEnd:
			if result == nil {
				result = &NativeResult{}
			}
			result.Completion = value.NativeEnd.Completion
			result.Failure = value.NativeEnd.Failure
		}
		return nil
	}
	err := execute(ctx, client, request, consume)
	return result, err
}
