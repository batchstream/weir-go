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
		request = &MutateRequest{Resource: options.Request.Resource, Action: action, Document: options.Request.Document}
	}
	batch := MutateOptions{StoreName: options.StoreName, Requests: []*MutateRequest{request}}
	return mutateOne(ctx, client, batch)
}

func Delete(ctx context.Context, client pb.StoreServiceClient, options DeleteOptions) (*MutationResult, error) {
	var request *MutateRequest
	if options.Request != nil {
		request = &MutateRequest{Resource: options.Request.Resource, Action: MutationDelete}
	}
	batch := MutateOptions{StoreName: options.StoreName, Requests: []*MutateRequest{request}}
	return mutateOne(ctx, client, batch)
}

func AtomicTransform(ctx context.Context, client pb.StoreServiceClient, options AtomicTransformOptions) (*MutationResult, error) {
	var request *MutateRequest
	if options.Request != nil {
		request = &MutateRequest{Resource: options.Request.Resource, Action: MutationAtomicTransform, Program: options.Request.Program, BackendExpression: options.Request.BackendExpression}
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

// Scan consumes one finite page incrementally. Only final RPC OK exposes its
// next checkpoint; retrying a failed page may redeliver documents and must use
// the previous token. The caller owns deduplication and output commits.
func Scan(ctx context.Context, client pb.StoreServiceClient, options ScanOptions) (*ScanEnd, error) {
	if options.Request == nil || options.Consume == nil {
		return nil, errors.New("Scan requires a request and document consumer")
	}
	variant := &pb.Command_Scan{Scan: options.Request}
	command := &pb.Command{Operation: variant}
	request := &pb.ExecuteRequest{StoreName: options.StoreName, Index: 1, Command: command}
	var end *ScanEnd
	consume := func(ctx context.Context, event *pb.Event) error {
		if document := event.GetDocument(); document != nil {
			return options.Consume(ctx, document)
		}
		end = event.GetScanEnd()
		return nil
	}
	err := execute(ctx, client, request, consume)
	if err != nil {
		return nil, err
	}
	return end, nil
}

// Native consumes sequential head/chunk/end events without collecting a whole
// response. A backend error is native response data, not a mutation outcome.
func Native(ctx context.Context, client pb.StoreServiceClient, options NativeOptions) (*NativeEnd, error) {
	if options.Request == nil || options.Consume == nil {
		return nil, errors.New("Native requires a request and event consumer")
	}
	open := &pb.NativeOpen{Resource: options.Request.Resource, Descriptor_: options.Request.Descriptor, BodyContentType: options.Request.BodyContentType}
	native := &pb.NativeRequest{Open: open, Body: options.Request.Body}
	variant := &pb.Command_Native{Native: native}
	command := &pb.Command{Operation: variant}
	request := &pb.ExecuteRequest{StoreName: options.StoreName, Index: 1, Command: command}
	var end *NativeEnd
	consume := func(ctx context.Context, wire *pb.Event) error {
		event := &Event{Head: wire.GetHead(), Chunk: wire.GetChunk(), NativeEnd: wire.GetNativeEnd()}
		if event.NativeEnd != nil {
			end = event.NativeEnd
		}
		return options.Consume(ctx, event)
	}
	err := execute(ctx, client, request, consume)
	if err != nil {
		return end, err
	}
	return end, nil
}
