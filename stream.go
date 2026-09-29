package weir

import (
	"context"
	"fmt"
	"io"

	pb "github.com/batchstream/weir/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Scan streams documents to consume without retaining them. A callback error
// cancels the RPC. Callbacks run synchronously and must return promptly; context
// cancellation cannot interrupt arbitrary user callback code. Consumed documents
// are partial results until matching End, no failure, and final gRPC OK.
func (c *Client) Scan(ctx context.Context, req *pb.ScanRequest, consume func(*pb.Document) error) (*pb.ScanEnd, error) {
	if req == nil || consume == nil || proto.Size(req) > MaxFrameBytes {
		return nil, fmt.Errorf("weir: invalid Scan request or callback")
	}
	if err := validateRecord(req.Resource); err != nil {
		return nil, err
	}
	if req.ReadMediaType != "" && !validateMedia(req.ReadMediaType) {
		return nil, fmt.Errorf("weir: invalid Scan media type")
	}
	if req.Selector != nil {
		if err := validateDocument(req.Selector); err != nil {
			return nil, err
		}
		if len(req.Selector.Data) > 16<<10 {
			return nil, fmt.Errorf("weir: Scan selector exceeds 16 KiB")
		}
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	stream, err := c.rpc.Scan(ctx, req)
	if err != nil {
		return nil, err
	}
	var end *pb.ScanEnd
	var count uint64
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return end, err
		}
		if end != nil {
			return end, protocolError("Scan frame after End")
		}
		if doc := frame.GetDocument(); doc != nil {
			if validateDocument(doc) != nil {
				return nil, protocolError("invalid Scan document")
			}
			if err := consume(doc); err != nil {
				return nil, err
			}
			count++
		} else if frame.GetEnd() != nil {
			end = frame.GetEnd()
		} else {
			return nil, protocolError("missing Scan frame variant")
		}
	}
	if end == nil || end.DocumentCount != count {
		return end, protocolError("missing Scan End or mismatched document count")
	}
	return end, failureError(end.Failure)
}

// NativeRequest supplies a bounded in-memory upload. The SDK never reads an
// arbitrary Reader in a goroutine, so cancellation always joins its own sender.
// Chunk boundaries are transport details and carry no record framing semantics.
type NativeRequest struct {
	Open *pb.NativeOpen
	Body []byte
}

// NativeResult is response transport evidence, not a mutation acknowledgement.
// Even RESPONSE_COMPLETE can contain a native database error. Interpret Head and
// body with the selected adapter protocol. Missing End implies unknown effects.
type NativeResult struct {
	Head  *pb.NativeHead
	End   *pb.NativeEnd
	Bytes uint64
}

// Native concurrently uploads and consumes response chunks. consume is required
// even for empty bodies and follows the same cancellation contract as Scan.
// An error never implies that a native write was not applied; do not replay it.
func (c *Client) Native(ctx context.Context, req NativeRequest, consume func([]byte) error) (*NativeResult, error) {
	open := req.Open
	if open == nil || open.Descriptor_ == nil || len(open.Descriptor_.Data) > 64<<10 || proto.Size(open) > (64<<10)+4096+256 || len(req.Body) > MaxNativeBodyBytes || consume == nil {
		return nil, fmt.Errorf("weir: invalid Native request, callback, or byte limit")
	}
	if _, _, err := parseResource(open.Resource); err != nil {
		return nil, err
	}
	if !validateMedia(open.Descriptor_.MediaType) || open.BodyMediaType != "" && !validateMedia(open.BodyMediaType) {
		return nil, fmt.Errorf("weir: invalid Native media type")
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	stream, err := c.rpc.Native(ctx)
	if err != nil {
		return nil, err
	}
	sent := make(chan error, 1)
	go func() { sent <- sendNative(stream, req) }()
	defer func() { cancel(); <-sent }()
	result := &NativeResult{}
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return result, err
		}
		if result.End != nil {
			return result, protocolError("Native frame after End")
		}
		switch value := frame.Frame.(type) {
		case *pb.NativeResponseFrame_Head:
			if result.Head != nil || value.Head == nil {
				return result, protocolError("duplicate or nil Native Head")
			}
			if value.Head.Metadata != nil {
				if validateDocument(value.Head.Metadata) != nil {
					return result, protocolError("invalid Native metadata")
				}
			}
			if value.Head.BodyMediaType != "" && !validateMedia(value.Head.BodyMediaType) {
				return result, protocolError("invalid Native response media type")
			}
			result.Head = value.Head
		case *pb.NativeResponseFrame_Chunk:
			if result.Head == nil || len(value.Chunk) == 0 || len(value.Chunk) > NativeChunkBytes {
				return result, protocolError("Native chunk before Head or invalid chunk size")
			}
			if err := consume(value.Chunk); err != nil {
				return result, err
			}
			result.Bytes += uint64(len(value.Chunk))
		case *pb.NativeResponseFrame_End:
			if value.End == nil {
				return result, protocolError("nil Native End")
			}
			result.End = value.End
		default:
			return result, protocolError("missing Native frame variant")
		}
	}
	if result.End == nil {
		return result, protocolError("missing Native End; effects unknown")
	}
	switch result.End.Completion {
	case pb.NativeCompletion_RESPONSE_COMPLETE:
		if result.Head == nil || result.End.Failure != nil {
			return result, protocolError("invalid complete Native response")
		}
		return result, nil
	case pb.NativeCompletion_NATIVE_NOT_STARTED, pb.NativeCompletion_RESPONSE_INCOMPLETE:
		if result.End.Failure == nil {
			return result, protocolError("Native failure lacks failure envelope")
		}
		if result.End.Completion == pb.NativeCompletion_NATIVE_NOT_STARTED && result.Head != nil {
			return result, protocolError("Native NOT_STARTED after Head")
		}
		return result, failureError(result.End.Failure)
	default:
		return result, protocolError("unknown Native completion")
	}
}

func sendNative(stream grpc.BidiStreamingClient[pb.NativeRequestFrame, pb.NativeResponseFrame], req NativeRequest) error {
	variant := &pb.NativeRequestFrame_Open{Open: req.Open}
	frame := &pb.NativeRequestFrame{Frame: variant}
	if err := stream.Send(frame); err != nil {
		return err
	}
	for body := req.Body; len(body) > 0; {
		count := min(len(body), NativeChunkBytes)
		variant := &pb.NativeRequestFrame_Chunk{Chunk: body[:count]}
		frame := &pb.NativeRequestFrame{Frame: variant}
		if err := stream.Send(frame); err != nil {
			return err
		}
		body = body[count:]
	}
	return stream.CloseSend()
}
