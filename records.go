package weir

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

// Read validates every input before opening one bounded Execute stream. Results
// retain input order. A later stream error preserves already confirmed items;
// unconfirmed positions remain nil. Requests are never automatically replayed.
func Read(ctx context.Context, client pb.StoreServiceClient, options ReadOptions) ([]*ReadResult, error) {
	if err := validateReads(ctx, options); err != nil {
		return nil, err
	}
	return readRecords(ctx, client, options)
}

// Mutate validates every input before opening one bounded Execute stream.
// Same-resource mutations execute in input order, including after item failures.
// A later stream error preserves confirmed results; nil positions remain
// unacknowledged and may have applied. The call is not a transaction or replayed.
func Mutate(ctx context.Context, client pb.StoreServiceClient, options MutateOptions) ([]*MutationResult, error) {
	if err := validateMutations(ctx, options); err != nil {
		return nil, err
	}
	return mutateRecords(ctx, client, options)
}

func (c *Client) Read(ctx context.Context, options ReadOptions) ([]*ReadResult, error) {
	if err := validateReads(ctx, options); err != nil {
		return nil, err
	}
	client, err := c.storeClient(options.StoreName)
	if err != nil {
		return nil, err
	}
	return readRecords(ctx, client, options)
}

func (c *Client) Mutate(ctx context.Context, options MutateOptions) ([]*MutationResult, error) {
	if err := validateMutations(ctx, options); err != nil {
		return nil, err
	}
	client, err := c.storeClient(options.StoreName)
	if err != nil {
		return nil, err
	}
	return mutateRecords(ctx, client, options)
}

func validateReads(ctx context.Context, options ReadOptions) error {
	if !protocol.ValidStoreName(options.StoreName) || len(options.Requests) == 0 {
		return errors.New("Read requires a Store and nonempty requests")
	}
	for index, item := range options.Requests {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := protocol.ValidateReadRequest(item); err != nil {
			return fmt.Errorf("Read request %d: %w", index+1, err)
		}
	}
	return nil
}

func validateMutations(ctx context.Context, options MutateOptions) error {
	if !protocol.ValidStoreName(options.StoreName) || len(options.Requests) == 0 {
		return errors.New("Mutate requires a Store and nonempty requests")
	}
	for index, item := range options.Requests {
		if err := ctx.Err(); err != nil {
			return err
		}
		mutation, err := wireMutation(item)
		if err != nil {
			return fmt.Errorf("Mutate request %d: %w", index+1, err)
		}
		if err := protocol.ValidateMutationRequest(mutation); err != nil {
			return fmt.Errorf("Mutate request %d: %w", index+1, err)
		}
	}
	return nil
}

func readRecords(ctx context.Context, client pb.StoreServiceClient, options ReadOptions) ([]*ReadResult, error) {
	results := make([]*ReadResult, len(options.Requests))
	position := 0
	streamOptions := ReadStreamOptions{StoreName: options.StoreName}
	streamOptions.Next = func(context.Context) (*ReadRequest, error) {
		if position == len(options.Requests) {
			return nil, io.EOF
		}
		item := options.Requests[position]
		position++
		return item, nil
	}
	streamOptions.Consume = func(_ context.Context, index uint64, result *ReadResult) error {
		results[index-1] = result
		return nil
	}
	err := ReadStream(ctx, client, streamOptions)
	return results, err
}

func mutateRecords(ctx context.Context, client pb.StoreServiceClient, options MutateOptions) ([]*MutationResult, error) {
	results := make([]*MutationResult, len(options.Requests))
	position := 0
	streamOptions := MutateStreamOptions{StoreName: options.StoreName}
	streamOptions.Next = func(context.Context) (*MutateRequest, error) {
		if position == len(options.Requests) {
			return nil, io.EOF
		}
		item := options.Requests[position]
		position++
		return item, nil
	}
	streamOptions.Consume = func(_ context.Context, index uint64, result *MutationResult) error {
		results[index-1] = result
		return nil
	}
	err := MutateStream(ctx, client, streamOptions)
	return results, err
}

func wireMutation(request *MutateRequest) (*pb.MutateRequest, error) {
	if request == nil {
		return nil, errors.New("missing mutation")
	}
	mutation := &pb.MutateRequest{Resource: request.Resource}
	switch request.Action {
	case MutationCreate, MutationPut, MutationReplace:
		if request.Document == nil || request.Program != nil || request.BackendExpression != nil {
			return nil, errors.New("write requires only a document")
		}
		switch request.Action {
		case MutationCreate:
			mutation.Action = &pb.MutateRequest_Create{Create: request.Document}
		case MutationPut:
			mutation.Action = &pb.MutateRequest_Put{Put: request.Document}
		case MutationReplace:
			mutation.Action = &pb.MutateRequest_Replace{Replace: request.Document}
		}
	case MutationDelete:
		if request.Document != nil || request.Program != nil || request.BackendExpression != nil {
			return nil, errors.New("delete accepts no document or transform")
		}
		empty := &pb.Empty{}
		mutation.Action = &pb.MutateRequest_Delete{Delete: empty}
	case MutationAtomicTransform:
		if request.Document != nil || (request.Program == nil) == (request.BackendExpression == nil) {
			return nil, errors.New("AtomicTransform requires exactly one program or backend expression")
		}
		transform := &pb.Transform{}
		if request.Program != nil {
			transform.Form = &pb.Transform_Program{Program: request.Program}
		} else {
			transform.Form = &pb.Transform_BackendExpression{BackendExpression: request.BackendExpression}
		}
		mutation.Action = &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	default:
		return nil, errors.New("unknown mutation action")
	}
	return mutation, nil
}
