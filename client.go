// Package weir initializes Store destinations and sends typed business requests
// directly to their owners. Business RPCs are never automatically replayed.
package weir

import (
	"context"
	"errors"
	"io"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
)

// execute consumes one finite Scan or Native request incrementally. Scan callers
// expose a checkpoint only after the terminal event and final gRPC OK.
func execute(ctx context.Context, client pb.StoreServiceClient, request *pb.ExecuteRequest, consume func(context.Context, *pb.Event) error) error {
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
	stream, err := client.Execute(ctx, request, grpc.MaxCallSendMsgSize(protocol.MaxExecuteRequestBytes), grpc.MaxCallRecvMsgSize(protocol.MaxExecuteResponseBytes), grpc.MaxRetryRPCBufferSize(0))
	if err != nil {
		return err
	}
	// Commit this attempt before consuming any event; grpc must not transparently
	// replay a Native request that could mutate its backend.
	_ = stream.Context()
	state := streamState{}
	if scan := request.Command.GetScan(); scan != nil {
		state.kind = "scan"
		state.pageSize = protocol.ScanPageSize(scan)
	} else {
		state.kind = "native"
	}
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if !state.terminal {
				return errors.New("Execute ended without a terminal business event")
			}
			return nil
		}
		if err != nil {
			return err
		}
		if state.terminal {
			return errors.New("Execute event after terminal business event")
		}
		if err := protocol.ValidateExecuteResponse(response); err != nil {
			return err
		}
		if err := state.validate(response.Event); err != nil {
			return err
		}
		if err := consume(ctx, response.Event); err != nil {
			return err
		}
	}
}

type streamState struct {
	kind      string
	terminal  bool
	head      bool
	documents uint64
	pageSize  uint64
}

func (s *streamState) validate(event *pb.Event) error {
	switch value := event.Value.(type) {
	case *pb.Event_Document:
		if s.kind != "scan" {
			return errors.New("unexpected Scan document")
		}
		s.documents++
		if s.documents > s.pageSize {
			return errors.New("Scan exceeded requested page size")
		}
	case *pb.Event_ScanEnd:
		if s.kind != "scan" || value.ScanEnd.DocumentCount != s.documents {
			return errors.New("Scan document count does not match completion")
		}
		s.terminal = true
	case *pb.Event_Head:
		if s.kind != "native" || s.head {
			return errors.New("invalid Native response head")
		}
		s.head = true
	case *pb.Event_Chunk:
		if s.kind != "native" || !s.head {
			return errors.New("Native chunk before head")
		}
	case *pb.Event_NativeEnd:
		if s.kind != "native" || value.NativeEnd.Completion == NativeResponseComplete && !s.head {
			return errors.New("invalid Native completion")
		}
		s.terminal = true
	default:
		return errors.New("unexpected Execute event")
	}
	return nil
}
