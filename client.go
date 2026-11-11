// Package weir sends typed business requests to the Stores hosted behind one
// Weir application address. A Client sends a record again only when it took
// no effect: a server refused it before it began, or a mutation lost to
// concurrent writers. Nothing that may have taken effect is resent.
package weir

import (
	"context"
	"errors"
	"io"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
)

// execute sends one request on a gRPC stream of its own and passes its events to
// consume, which reports the terminal one, then awaits the final gRPC status.
func execute(ctx context.Context, client pb.StoreServiceClient, request *pb.ExecuteRequest, consume func(context.Context, *pb.Event) (bool, error)) (resultErr error) {
	if client == nil || consume == nil {
		return errors.New("Execute requires a client and consumer")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := protocol.ValidateExecuteRequest(request); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Execute(ctx, grpc.MaxCallSendMsgSize(protocol.MaxTransportMessageBytes), grpc.MaxCallRecvMsgSize(protocol.MaxTransportMessageBytes), grpc.MaxRetryRPCBufferSize(0))
	if err != nil {
		return err
	}
	// Commit this attempt before consuming any event; grpc must not transparently
	// replay a request that could mutate its backend.
	_ = stream.Context()
	done := make(chan struct{})
	var sendErr error
	message := &pb.ExecuteRequests{Requests: []*pb.ExecuteRequest{request}}
	go func() {
		defer close(done)
		sendErr = stream.Send(message)
		if sendErr == nil {
			sendErr = stream.CloseSend()
		}
		// An EOF send error carries the peer's final status through Recv. Other
		// errors may be local, so stop a receive that cannot make progress.
		if sendErr != nil && !errors.Is(sendErr, io.EOF) {
			cancel()
		}
	}()
	defer func() {
		cancel()
		<-done
		if resultErr == nil {
			resultErr = sendErr
		} else if sendErr != nil && !errors.Is(sendErr, io.EOF) {
			resultErr = errors.Join(resultErr, sendErr)
		}
	}()
	terminal := false
	for {
		message, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if !terminal {
				return errors.New("Execute ended without a terminal business event")
			}
			// A peer can finish before Send returns to its caller. Await the
			// actual Send/CloseSend outcome before accepting its final OK.
			<-done
			return nil
		}
		if err != nil {
			return err
		}
		if err := protocol.ValidateExecuteResponses(message); err != nil {
			return err
		}
		for _, response := range message.Responses {
			if terminal {
				return errors.New("Execute event after terminal business event")
			}
			if err := protocol.ValidateExecuteResponse(response); err != nil {
				return err
			}
			if response.Index != 1 {
				return errors.New("unexpected Execute response ordinal")
			}
			terminal, err = consume(ctx, response.Event)
			if err != nil {
				return err
			}
		}
	}
}
