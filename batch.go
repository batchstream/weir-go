package weir

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/protobuf/proto"
)

// MaxBatchRequests bounds retained results in Read and Mutate. Use Execute for
// larger workloads that consume their results incrementally.
const MaxBatchRequests = 128

// MaxBatchInputBytes bounds the total encoded commands retained by preflight.
const MaxBatchInputBytes = 32 << 20

// Read sends all requests over one finite Execute RPC. The entire input is
// validated before opening the RPC. Results have the input order; a nil entry
// has no validated backend evidence. Results remain available with a later
// transport error. A missing resource or backend failure is an individual result.
func Read(ctx context.Context, client pb.StoreServiceClient, options ReadOptions) ([]*ReadResult, error) {
	commands, err := prepareReads(ctx, options)
	if err != nil {
		return nil, err
	}
	return readBatch(ctx, client, options.StoreName, commands)
}

// Mutate sends independent mutations over one finite Execute RPC without retry.
// All requests are validated before any is sent. Results have the input order;
// nil entries are unacknowledged and may have been applied. An APPLIED result
// remains backend evidence when a later transport error is returned with it.
// The batch is not atomic and does not impose ordering between its mutations.
func Mutate(ctx context.Context, client pb.StoreServiceClient, options MutateOptions) ([]*MutationResult, error) {
	commands, err := prepareMutations(ctx, options)
	if err != nil {
		return nil, err
	}
	return mutationBatch(ctx, client, options.StoreName, commands)
}

func (c *Client) Read(ctx context.Context, options ReadOptions) ([]*ReadResult, error) {
	commands, err := prepareReads(ctx, options)
	if err != nil {
		return nil, err
	}
	client, err := c.storeClient(options.StoreName)
	if err != nil {
		return nil, err
	}
	return readBatch(ctx, client, options.StoreName, commands)
}

func (c *Client) Mutate(ctx context.Context, options MutateOptions) ([]*MutationResult, error) {
	commands, err := prepareMutations(ctx, options)
	if err != nil {
		return nil, err
	}
	client, err := c.storeClient(options.StoreName)
	if err != nil {
		return nil, err
	}
	return mutationBatch(ctx, client, options.StoreName, commands)
}

func prepareReads(ctx context.Context, options ReadOptions) ([]*Command, error) {
	if !protocol.ValidStoreName(options.StoreName) || len(options.Requests) == 0 || len(options.Requests) > MaxBatchRequests {
		return nil, fmt.Errorf("Read requires a valid Store name and 1-%d nonempty requests", MaxBatchRequests)
	}
	commands := make([]*Command, len(options.Requests))
	remaining := MaxBatchInputBytes
	for index, request := range options.Requests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		command := NewReadCommand(request)
		if err := validateBatchCommand(options.StoreName, command, remaining); err != nil {
			return nil, fmt.Errorf("Read request %d: %w", index, err)
		}
		remaining -= len(command.payload)
		commands[index] = command
	}
	return commands, nil
}

func prepareMutations(ctx context.Context, options MutateOptions) ([]*Command, error) {
	if !protocol.ValidStoreName(options.StoreName) || len(options.Requests) == 0 || len(options.Requests) > MaxBatchRequests {
		return nil, fmt.Errorf("Mutate requires a valid Store name and 1-%d nonempty requests", MaxBatchRequests)
	}
	commands := make([]*Command, len(options.Requests))
	remaining := MaxBatchInputBytes
	for index, request := range options.Requests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		command, err := batchMutationCommand(request)
		if err != nil {
			return nil, fmt.Errorf("Mutate request %d: %w", index, err)
		}
		if err := validateBatchCommand(options.StoreName, command, remaining); err != nil {
			return nil, fmt.Errorf("Mutate request %d: %w", index, err)
		}
		remaining -= len(command.payload)
		commands[index] = command
	}
	return commands, nil
}

func batchMutationCommand(request *MutateRequest) (*Command, error) {
	if request == nil {
		return nil, errors.New("missing mutation")
	}
	switch request.Action {
	case MutationCreate, MutationPut, MutationReplace:
		if request.Document == nil || request.Program != nil || request.BackendExpression != nil {
			return nil, errors.New("write requires only a document")
		}
		write := &WriteRequest{Resource: request.Resource, Document: request.Document, AdapterOptions: request.AdapterOptions}
		switch request.Action {
		case MutationCreate:
			return NewCreateCommand(write), nil
		case MutationPut:
			return NewPutCommand(write), nil
		default:
			return NewReplaceCommand(write), nil
		}
	case MutationDelete:
		if request.Document != nil || request.Program != nil || request.BackendExpression != nil {
			return nil, errors.New("delete accepts no document or transform")
		}
		remove := &DeleteRequest{Resource: request.Resource, AdapterOptions: request.AdapterOptions}
		return NewDeleteCommand(remove), nil
	case MutationAtomicTransform:
		if request.Document != nil || (request.Program == nil) == (request.BackendExpression == nil) {
			return nil, errors.New("AtomicTransform requires exactly one program or backend expression")
		}
		transform := &AtomicTransformRequest{Resource: request.Resource, Program: request.Program, BackendExpression: request.BackendExpression, AdapterOptions: request.AdapterOptions}
		return NewAtomicTransformCommand(transform), nil
	default:
		return nil, errors.New("unknown mutation action")
	}
}

func validateBatchCommand(store string, command *Command, remaining int) error {
	if command == nil || command.wire == nil || command.wire.Version != 1 || command.wire.Operation == nil {
		return errors.New("invalid Command")
	}
	if command.err != nil {
		return command.err
	}
	encodedSize := proto.Size(command.wire)
	if encodedSize > protocol.MaxPayload || encodedSize > remaining {
		return errors.New("command exceeds payload or total batch input bound")
	}
	data, err := proto.Marshal(command.wire)
	if err != nil {
		return err
	}
	decoded, err := protocol.DecodeCommand(data)
	if err != nil {
		return err
	}
	operation := &pb.Operation{}
	if read := decoded.GetRead(); read != nil {
		read.Resource = "weir://" + store + "/" + read.Resource
		variant := &pb.Operation_Read{Read: read}
		operation.Operation = variant
	} else if mutation := decoded.GetMutate(); mutation != nil {
		mutation.Resource = "weir://" + store + "/" + mutation.Resource
		variant := &pb.Operation_Mutate{Mutate: mutation}
		operation.Operation = variant
	} else {
		return errors.New("batch requires read or mutation commands")
	}
	if failure := protocol.Validate(operation, store); failure != nil {
		return errors.New(failure.Message)
	}
	command.payload = data
	command.kind = commandKind(decoded)
	return nil
}

func readBatch(ctx context.Context, client pb.StoreServiceClient, store string, commands []*Command) ([]*ReadResult, error) {
	results := make([]*ReadResult, len(commands))
	batch := batchOptions(store, commands)
	batch.Consume = func(_ context.Context, id uint64, event *Event) error {
		results[id-1] = event.Result.Read
		return nil
	}
	err := Execute(ctx, client, batch)
	return results, err
}

func mutationBatch(ctx context.Context, client pb.StoreServiceClient, store string, commands []*Command) ([]*MutationResult, error) {
	results := make([]*MutationResult, len(commands))
	batch := batchOptions(store, commands)
	batch.Consume = func(_ context.Context, id uint64, event *Event) error {
		results[id-1] = event.Result.Mutation
		return nil
	}
	err := Execute(ctx, client, batch)
	return results, err
}

func batchOptions(store string, commands []*Command) ExecuteOptions {
	next := 0
	batch := ExecuteOptions{StoreName: store}
	batch.Produce = func(context.Context) (*Command, error) {
		if next == len(commands) {
			return nil, io.EOF
		}
		command := commands[next]
		next++
		return command, nil
	}
	return batch
}
