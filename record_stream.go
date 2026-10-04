package weir

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync/atomic"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// ReadStream produces and consumes a finite read sequence using bounded frames.
// Confirmed callbacks remain valid when a later transport or producer fails.
func ReadStream(ctx context.Context, client pb.StoreServiceClient, options ReadStreamOptions) error {
	if options.Next == nil || options.Consume == nil {
		return errors.New("ReadStream requires a producer and consumer")
	}
	source := recordSource{store: options.StoreName, nextRead: options.Next}
	consume := func(ctx context.Context, response *pb.ExecuteResponse) error {
		wire := response.Event.GetReadResult()
		if wire == nil {
			return errors.New("unexpected event in Read stream")
		}
		result := &ReadResult{Document: wire.GetDocument(), Missing: wire.GetMissing() != nil, Failure: wire.GetFailure()}
		return options.Consume(ctx, response.Index, result)
	}
	optionsInternal := recordStreamOptions{source: &source, consume: consume}
	return recordStream(ctx, client, optionsInternal)
}

// MutateStream never replays a request. Every consumed result is an individual
// acknowledgement; later failures leave other items unacknowledged.
func MutateStream(ctx context.Context, client pb.StoreServiceClient, options MutateStreamOptions) error {
	if options.Next == nil || options.Consume == nil {
		return errors.New("MutateStream requires a producer and consumer")
	}
	source := recordSource{store: options.StoreName, nextMutation: options.Next}
	consume := func(ctx context.Context, response *pb.ExecuteResponse) error {
		result := response.Event.GetMutationResult()
		if result == nil {
			return errors.New("unexpected event in Mutate stream")
		}
		return options.Consume(ctx, response.Index, result)
	}
	optionsInternal := recordStreamOptions{source: &source, consume: consume}
	return recordStream(ctx, client, optionsInternal)
}

func (c *Client) ReadStream(ctx context.Context, options ReadStreamOptions) error {
	client, err := c.storeClient(options.StoreName)
	if err != nil {
		return err
	}
	return ReadStream(ctx, client, options)
}

func (c *Client) MutateStream(ctx context.Context, options MutateStreamOptions) error {
	client, err := c.storeClient(options.StoreName)
	if err != nil {
		return err
	}
	return MutateStream(ctx, client, options)
}

type recordSource struct {
	store           string
	nextRead        func(context.Context) (*ReadRequest, error)
	nextMutation    func(context.Context) (*MutateRequest, error)
	pendingRead     *pb.ReadRequest
	pendingMutation *pb.MutateRequest
	index           uint64
	eof             bool
}

func (source *recordSource) next(ctx context.Context) (*pb.ReadRequest, *pb.MutateRequest, error) {
	if source.pendingRead != nil || source.pendingMutation != nil {
		read, mutation := source.pendingRead, source.pendingMutation
		source.pendingRead, source.pendingMutation = nil, nil
		return read, mutation, nil
	}
	if source.nextRead != nil {
		read, err := source.nextRead(ctx)
		if err != nil {
			return nil, nil, err
		}
		if err := protocol.ValidateReadRequest(read); err != nil {
			return nil, nil, err
		}
		return read, nil, nil
	}
	input, err := source.nextMutation(ctx)
	if err != nil {
		return nil, nil, err
	}
	mutation, err := wireMutation(input)
	if err != nil {
		return nil, nil, err
	}
	if err := protocol.ValidateMutationRequest(mutation); err != nil {
		return nil, nil, err
	}
	return nil, mutation, nil
}

func (source *recordSource) frame(ctx context.Context) (*pb.ExecuteRequest, error) {
	if source.eof {
		return nil, io.EOF
	}
	command := &pb.Command{}
	readBatch := &pb.ReadBatch{}
	mutationBatch := &pb.MutationBatch{}
	if source.nextRead != nil {
		command.Operation = &pb.Command_Read{Read: readBatch}
	} else {
		command.Operation = &pb.Command_Mutate{Mutate: mutationBatch}
	}
	count, batchBytes := 0, 0
	for count < protocol.MaxRecordFrameItems {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		read, mutation, err := source.next(ctx)
		if errors.Is(err, io.EOF) {
			source.eof = true
			break
		}
		if err != nil {
			return nil, fmt.Errorf("request %d: %w", source.index+uint64(count)+1, err)
		}
		itemBytes := 0
		if read != nil {
			itemBytes = proto.Size(read)
		} else {
			itemBytes = proto.Size(mutation)
		}
		nextBytes := batchBytes + protowire.SizeTag(1) + protowire.SizeBytes(itemBytes)
		commandBytes := protowire.SizeTag(1) + protowire.SizeBytes(nextBytes)
		if commandBytes > protocol.MaxRecordFrameBytes {
			if count == 0 {
				return nil, errors.New("record exceeds frame byte budget")
			}
			source.pendingRead, source.pendingMutation = read, mutation
			break
		}
		if source.index >= math.MaxUint64-uint64(count)-1 {
			return nil, errors.New("record ordinal exhausted")
		}
		if read != nil {
			readBatch.Requests = append(readBatch.Requests, read)
		} else {
			mutationBatch.Requests = append(mutationBatch.Requests, mutation)
		}
		count++
		batchBytes = nextBytes
	}
	if count == 0 {
		return nil, io.EOF
	}
	frame := &pb.ExecuteRequest{StoreName: source.store, Index: source.index + 1, Command: command}
	source.index += uint64(count)
	return frame, nil
}

type recordStreamOptions struct {
	source  *recordSource
	consume func(context.Context, *pb.ExecuteResponse) error
}

type recordSendResult struct {
	count uint64
	err   error
}

func recordStream(ctx context.Context, client pb.StoreServiceClient, options recordStreamOptions) error {
	if client == nil || !protocol.ValidStoreName(options.source.store) {
		return errors.New("Execute requires a client and valid Store")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Execute(ctx, grpc.MaxCallSendMsgSize(protocol.MaxExecuteRequestBytes), grpc.MaxCallRecvMsgSize(protocol.MaxExecuteResponseBytes), grpc.MaxRetryRPCBufferSize(0))
	if err != nil {
		return err
	}
	// Calling Context commits this attempt, disabling transparent write replay.
	_ = stream.Context()
	credits := make(chan struct{}, 2)
	ends := make(chan uint64, cap(credits))
	done := make(chan recordSendResult, 1)
	var submitted atomic.Uint64
	var finishedInput atomic.Bool
	go func() {
		result := recordSendResult{}
		defer func() {
			// Local producer/send errors need not yield a peer response. EOF
			// is different: Recv must expose the peer's final status.
			if result.err != nil && !errors.Is(result.err, io.EOF) {
				cancel()
			}
			done <- result
		}()
		for {
			select {
			case credits <- struct{}{}:
			case <-ctx.Done():
				result.err = ctx.Err()
				return
			}
			frame, err := options.source.frame(ctx)
			if errors.Is(err, io.EOF) {
				<-credits
				if result.count == 0 {
					result.err = errors.New("Execute requires at least one record")
					return
				}
				finishedInput.Store(true)
				result.err = stream.CloseSend()
				return
			}
			if err != nil {
				result.err = err
				return
			}
			result.count = options.source.index
			ends <- result.count
			submitted.Store(result.count)
			if err := stream.Send(frame); err != nil {
				result.err = err
				return
			}
		}
	}()
	var received, frameEnd uint64
	var receiveErr error
	for {
		response, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if !finishedInput.Load() {
					receiveErr = errors.New("Execute ended before input was closed")
				}
			} else {
				receiveErr = err
			}
			break
		}
		if err := protocol.ValidateExecuteResponse(response); err != nil {
			receiveErr = err
			break
		}
		if response.Index != received+1 || response.Index > submitted.Load() {
			receiveErr = errors.New("Execute response ordinal is missing, duplicated or unexpected")
			break
		}
		if frameEnd == 0 {
			frameEnd = <-ends
		}
		if err := options.consume(ctx, response); err != nil {
			receiveErr = err
			break
		}
		received++
		if received == frameEnd {
			<-credits
			frameEnd = 0
		}
	}
	if receiveErr != nil {
		cancel()
	}
	sent := <-done
	if sent.err != nil && !errors.Is(sent.err, io.EOF) {
		if receiveErr == nil {
			return sent.err
		}
		return errors.Join(receiveErr, sent.err)
	}
	if receiveErr != nil {
		return receiveErr
	}
	if sent.err != nil || received != sent.count {
		return fmt.Errorf("Execute incomplete: confirmed %d of %d records", received, sent.count)
	}
	return nil
}
