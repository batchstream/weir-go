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
	"google.golang.org/protobuf/proto"
)

// ReadStream sends each produced read immediately and consumes indexed results.
// Confirmed callbacks remain valid when a later transport or producer fails.
func ReadStream(ctx context.Context, client pb.StoreServiceClient, options ReadStreamOptions) error {
	if options.Next == nil || options.Consume == nil {
		return errors.New("ReadStream requires a producer and consumer")
	}
	next := func(ctx context.Context) (*pb.Command, error) {
		request, err := options.Next(ctx)
		if err != nil {
			return nil, err
		}
		if err := protocol.ValidateReadRequest(request); err != nil {
			return nil, err
		}
		operation := &pb.Command_Read{Read: request}
		command := &pb.Command{Operation: operation}
		return command, nil
	}
	consume := func(ctx context.Context, response *pb.ExecuteResponse) error {
		wire := response.Event.GetReadResult()
		if wire == nil {
			return errors.New("unexpected event in Read stream")
		}
		result := &ReadResult{Document: wire.GetDocument(), Missing: wire.GetMissing() != nil, Failure: wire.GetFailure()}
		return options.Consume(ctx, response.Index, result)
	}
	optionsInternal := recordStreamOptions{store: options.StoreName, next: next, consume: consume}
	return recordStream(ctx, client, optionsInternal)
}

// MutateStream never replays a request. Every consumed result is an individual
// acknowledgement; later failures leave other items unacknowledged.
func MutateStream(ctx context.Context, client pb.StoreServiceClient, options MutateStreamOptions) error {
	if options.Next == nil || options.Consume == nil {
		return errors.New("MutateStream requires a producer and consumer")
	}
	next := func(ctx context.Context) (*pb.Command, error) {
		input, err := options.Next(ctx)
		if err != nil {
			return nil, err
		}
		request, err := wireMutation(input)
		if err != nil {
			return nil, err
		}
		if err := protocol.ValidateMutationRequest(request); err != nil {
			return nil, err
		}
		operation := &pb.Command_Mutate{Mutate: request}
		command := &pb.Command{Operation: operation}
		return command, nil
	}
	consume := func(ctx context.Context, response *pb.ExecuteResponse) error {
		result := response.Event.GetMutationResult()
		if result == nil {
			return errors.New("unexpected event in Mutate stream")
		}
		return options.Consume(ctx, response.Index, result)
	}
	optionsInternal := recordStreamOptions{store: options.StoreName, next: next, consume: consume}
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

type recordStreamOptions struct {
	store   string
	next    func(context.Context) (*pb.Command, error)
	consume func(context.Context, *pb.ExecuteResponse) error
}

// These SDK backpressure bounds are independent of the public wire contract.
const recordWindowItems = 32
const recordWindowBytes = 8 << 20

type recordSendResult struct {
	count uint64
	err   error
}

func recordStream(ctx context.Context, client pb.StoreServiceClient, options recordStreamOptions) error {
	if client == nil || !protocol.ValidStoreName(options.store) {
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
	sizes := make(chan int, recordWindowItems)
	released := make(chan int, recordWindowItems)
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
		inflight, bytes := 0, 0
		for {
			// Stop producing when the item window fills. A size-dependent wait
			// below can retain only the current unsent item.
			for inflight == recordWindowItems {
				select {
				case size := <-released:
					inflight--
					bytes -= size
				case <-ctx.Done():
					result.err = ctx.Err()
					return
				}
			}
			if err := ctx.Err(); err != nil {
				result.err = err
				return
			}
			command, err := options.next(ctx)
			if errors.Is(err, io.EOF) {
				if result.count == 0 {
					result.err = errors.New("Execute requires at least one record")
					return
				}
				finishedInput.Store(true)
				result.err = stream.CloseSend()
				return
			}
			if err != nil {
				result.err = fmt.Errorf("request %d: %w", result.count+1, err)
				return
			}
			if result.count == math.MaxUint64-1 {
				result.err = errors.New("record ordinal exhausted")
				return
			}
			request := &pb.ExecuteRequest{StoreName: options.store, Index: result.count + 1, Command: command}
			size := proto.Size(request)
			if size > protocol.MaxExecuteRequestBytes {
				result.err = errors.New("record exceeds request byte bound")
				return
			}
			// A legal item larger than the byte window proceeds alone. Ordinary
			// items wait for individual acknowledgements to return byte credit.
			for inflight > 0 && size > recordWindowBytes-bytes {
				select {
				case credit := <-released:
					inflight--
					bytes -= credit
				case <-ctx.Done():
					result.err = ctx.Err()
					return
				}
			}
			if err := ctx.Err(); err != nil {
				result.err = err
				return
			}
			// Register before Send: Recv can observe the response before Send
			// returns. Only the receiver publishes acknowledged byte credit.
			sizes <- size
			inflight++
			bytes += size
			result.count = request.Index
			submitted.Store(result.count)
			if err := stream.Send(request); err != nil {
				result.err = err
				return
			}
		}
	}()
	var received uint64
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
		if err := options.consume(ctx, response); err != nil {
			receiveErr = err
			break
		}
		received++
		released <- <-sizes
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
