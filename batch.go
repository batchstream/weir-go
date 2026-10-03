package weir

import (
	"context"
	"errors"
	"fmt"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
)

const MaxBatchRequestBytes = protocol.MaxBatchRequestBytes
const MaxBatchResponseBytes = protocol.MaxBatchResponseBytes

// Read validates the entire input and sends one typed unary batch. Results have
// input order. A failed RPC yields no confirmed results and is never replayed.
func Read(ctx context.Context, client pb.StoreServiceClient, options ReadOptions) ([]*ReadResult, error) {
	request, err := prepareReads(ctx, options)
	if err != nil {
		return nil, err
	}
	return readBatch(ctx, client, request)
}

// Mutate executes one unary batch without retry. Same-resource mutations run in
// input order, including after item failures; different resources may run in
// parallel. The batch is not a transaction. A failed RPC leaves every submitted
// mutation unacknowledged, so any may have applied.
func Mutate(ctx context.Context, client pb.StoreServiceClient, options MutateOptions) ([]*MutationResult, error) {
	request, err := prepareMutations(ctx, options)
	if err != nil {
		return nil, err
	}
	return mutationBatch(ctx, client, request)
}

func (c *Client) Read(ctx context.Context, options ReadOptions) ([]*ReadResult, error) {
	request, err := prepareReads(ctx, options)
	if err != nil {
		return nil, err
	}
	client, err := c.storeClient(options.StoreName)
	if err != nil {
		return nil, err
	}
	return readBatch(ctx, client, request)
}

func (c *Client) Mutate(ctx context.Context, options MutateOptions) ([]*MutationResult, error) {
	request, err := prepareMutations(ctx, options)
	if err != nil {
		return nil, err
	}
	client, err := c.storeClient(options.StoreName)
	if err != nil {
		return nil, err
	}
	return mutationBatch(ctx, client, request)
}

func prepareReads(ctx context.Context, options ReadOptions) (*pb.ReadBatchRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	request := &pb.ReadBatchRequest{StoreName: options.StoreName, Requests: options.Requests}
	if err := protocol.ValidateReadBatchRequest(request); err != nil {
		return nil, err
	}
	return request, nil
}

func prepareMutations(ctx context.Context, options MutateOptions) (*pb.MutateBatchRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	request := &pb.MutateBatchRequest{StoreName: options.StoreName, Requests: make([]*pb.MutateRequest, len(options.Requests))}
	for index, item := range options.Requests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		mutation, err := wireMutation(item)
		if err != nil {
			return nil, fmt.Errorf("Mutate request %d: %w", index, err)
		}
		request.Requests[index] = mutation
	}
	if err := protocol.ValidateMutateBatchRequest(request); err != nil {
		return nil, err
	}
	return request, nil
}

func wireMutation(request *MutateRequest) (*pb.MutateRequest, error) {
	if request == nil {
		return nil, errors.New("missing mutation")
	}
	mutation := &pb.MutateRequest{Resource: request.Resource, AdapterOptions: request.AdapterOptions}
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

func readBatch(ctx context.Context, client pb.StoreServiceClient, request *pb.ReadBatchRequest) ([]*ReadResult, error) {
	if client == nil {
		return nil, errors.New("Read requires a client")
	}
	results := make([]*ReadResult, len(request.Requests))
	codec := batchResponseCodec{results: len(request.Requests)}
	response, err := client.Read(ctx, request, grpc.ForceCodecV2(codec), grpc.MaxCallSendMsgSize(MaxBatchRequestBytes), grpc.MaxCallRecvMsgSize(MaxBatchResponseBytes), grpc.MaxRetryRPCBufferSize(0))
	if err != nil {
		return results, err
	}
	if err := protocol.ValidateReadBatchResponse(response, len(request.Requests)); err != nil {
		return results, err
	}
	for index, item := range response.Results {
		result := &ReadResult{Document: item.GetDocument(), Missing: item.GetMissing() != nil, Failure: item.GetFailure()}
		results[index] = result
	}
	return results, nil
}

func mutationBatch(ctx context.Context, client pb.StoreServiceClient, request *pb.MutateBatchRequest) ([]*MutationResult, error) {
	if client == nil {
		return nil, errors.New("Mutate requires a client")
	}
	results := make([]*MutationResult, len(request.Requests))
	codec := batchResponseCodec{results: len(request.Requests)}
	response, err := client.Mutate(ctx, request, grpc.ForceCodecV2(codec), grpc.MaxCallSendMsgSize(MaxBatchRequestBytes), grpc.MaxCallRecvMsgSize(MaxBatchResponseBytes), grpc.MaxRetryRPCBufferSize(0))
	if err != nil {
		return results, fmt.Errorf("Mutate RPC failed; every submitted mutation is unacknowledged: %w", err)
	}
	if err := protocol.ValidateMutateBatchResponse(response, len(request.Requests)); err != nil {
		return results, fmt.Errorf("Mutate response invalid; every submitted mutation is unacknowledged: %w", err)
	}
	return response.Results, nil
}
