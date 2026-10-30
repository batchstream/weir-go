// Package weir sends typed business requests to the Stores hosted behind one
// Weir application address. Business RPCs are never automatically replayed.
package weir

import (
	"context"
	"errors"
	"io"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
)

// execute consumes one Native request incrementally through its terminal event
// and final gRPC status.
func execute(ctx context.Context, client pb.StoreServiceClient, request *pb.ExecuteRequest, consume func(context.Context, *pb.Event) error) (resultErr error) {
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
	// replay a Native request that could mutate its backend.
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
	state := nativeStreamState{}
	for {
		message, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if !state.terminal {
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
			if err := state.consume(ctx, response, consume); err != nil {
				return err
			}
		}
	}
}

type nativeStreamState struct {
	terminal bool
	head     bool
}

// consume checks one Native response in stream order and passes its event on.
func (s *nativeStreamState) consume(ctx context.Context, response *pb.ExecuteResponse, consume func(context.Context, *pb.Event) error) error {
	if s.terminal {
		return errors.New("Execute event after terminal business event")
	}
	if err := protocol.ValidateExecuteResponse(response); err != nil {
		return err
	}
	if response.Index != 1 {
		return errors.New("unexpected Native response ordinal")
	}
	if err := s.validate(response.Event); err != nil {
		return err
	}
	return consume(ctx, response.Event)
}

func (s *nativeStreamState) validate(event *pb.Event) error {
	switch value := event.Value.(type) {
	case *pb.Event_Head:
		if s.head {
			return errors.New("invalid Native response head")
		}
		s.head = true
	case *pb.Event_Chunk:
		if !s.head {
			return errors.New("Native chunk before head")
		}
	case *pb.Event_NativeEnd:
		if value.NativeEnd.Completion == NativeResponseComplete && !s.head || value.NativeEnd.Completion == NativeNotStarted && s.head {
			return errors.New("invalid Native completion")
		}
		s.terminal = true
	default:
		return errors.New("unexpected Native event")
	}
	return nil
}
