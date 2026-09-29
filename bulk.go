package weir

import (
	"context"
	"fmt"
	"io"

	pb "github.com/batchstream/weir/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Operation contains exactly one Read or Mutate. Bulk assigns consecutive indexes
// by slice position; it does not split requests across streams.
type Operation struct {
	Read   *pb.ReadRequest
	Mutate *pb.MutateRequest
}

// Bulk validates every envelope before sending, runs upload/download concurrently,
// and returns results in input order, even if the server sends them out of order.
// It accepts at most 1024 operations / 8 MiB of encoded input. Semantic backend
// validation still happens per operation on the server: Bulk is not atomic.
// Successful stream completion needs one unique result per input, matching End
// counts, and final gRPC OK. Results received before a failure remain available.
func (c *Client) Bulk(ctx context.Context, store string, operations []Operation) ([]*pb.BulkResult, error) {
	name, segments, err := parseResource(store)
	if err != nil || segments != 0 {
		return nil, fmt.Errorf("weir: Bulk requires a Store URI")
	}
	if len(operations) > MaxBulkOperations {
		return nil, fmt.Errorf("weir: Bulk exceeds %d operations", MaxBulkOperations)
	}
	wire := make([]*pb.BulkOperation, len(operations))
	total := 0
	for index, op := range operations {
		var resource string
		item := &pb.BulkOperation{Index: uint64(index)}
		switch {
		case op.Read != nil && op.Mutate == nil:
			err = validateRead(op.Read)
			resource = op.Read.Resource
			variant := &pb.BulkOperation_Read{Read: op.Read}
			item.Operation = variant
		case op.Mutate != nil && op.Read == nil:
			err = validateMutation(op.Mutate)
			resource = op.Mutate.Resource
			variant := &pb.BulkOperation_Mutate{Mutate: op.Mutate}
			item.Operation = variant
		default:
			err = fmt.Errorf("operation needs exactly one Read or Mutate")
		}
		actual, _, _ := parseResource(resource)
		if err != nil {
			return nil, fmt.Errorf("weir: Bulk index %d: %w", index, err)
		}
		if actual != name {
			return nil, fmt.Errorf("weir: Bulk index %d uses another Store", index)
		}
		variant := &pb.BulkRequestFrame_Operation{Operation: item}
		frame := &pb.BulkRequestFrame{Frame: variant}
		size := proto.Size(frame)
		total += size
		if size > MaxFrameBytes || total > MaxBulkBytes {
			return nil, fmt.Errorf("weir: Bulk exceeds frame or total byte limit")
		}
		wire[index] = item
	}
	results := make([]*pb.BulkResult, len(wire))
	if len(wire) == 0 {
		return results, nil
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	stream, err := c.rpc.Bulk(ctx)
	if err != nil {
		failure := &BatchError{Cause: err}
		return results, failure
	}
	sent := make(chan error, 1)
	go func() { sent <- sendBulk(stream, store, wire) }()
	defer func() { cancel(); <-sent }()
	batch := &BatchError{Failures: make(map[int]error)}
	var end *pb.BulkEnd
	count := uint64(0)
	resultBytes := 0
	for {
		frame, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			batch.Cause = recvErr
			break
		}
		if end != nil {
			batch.Cause = protocolError("Bulk frame after End")
			break
		}
		if result := frame.GetResult(); result != nil {
			resultBytes += proto.Size(result)
			if resultBytes > MaxBulkBytes {
				batch.Cause = fmt.Errorf("weir: Bulk results exceed %d bytes", MaxBulkBytes)
				break
			}
			if result.Index >= uint64(len(wire)) || results[result.Index] != nil {
				batch.Cause = protocolError("duplicate or out-of-range Bulk index")
				break
			}
			index := int(result.Index)
			var resultErr error
			if operations[index].Read != nil {
				resultErr = readError(result.GetRead())
			} else {
				resultErr = mutationError(result.GetMutation())
			}
			if _, invalid := resultErr.(*ProtocolError); invalid {
				batch.Cause = resultErr
				break
			}
			results[index] = result
			count++
			if resultErr != nil {
				batch.Failures[index] = resultErr
			}
		} else if frame.GetEnd() != nil {
			end = frame.GetEnd()
		} else {
			batch.Cause = protocolError("missing Bulk frame variant")
			break
		}
	}
	if batch.Cause == nil && (end == nil || end.ReceivedCount != uint64(len(wire)) || end.ResultCount != count || count != uint64(len(wire))) {
		batch.Cause = protocolError("missing Bulk End or mismatched counts")
	}
	// End counts plus all indexed results prove receipt. A late Send EOF is not
	// grounds to discard that evidence; deferred cancellation joins the sender.
	if batch.Cause != nil || len(batch.Failures) > 0 {
		return results, batch
	}
	return results, nil
}

func sendBulk(stream grpc.BidiStreamingClient[pb.BulkRequestFrame, pb.BulkResponseFrame], store string, operations []*pb.BulkOperation) error {
	open := &pb.BulkOpen{Store: store}
	variant := &pb.BulkRequestFrame_Open{Open: open}
	frame := &pb.BulkRequestFrame{Frame: variant}
	if err := stream.Send(frame); err != nil {
		return err
	}
	for _, op := range operations {
		variant := &pb.BulkRequestFrame_Operation{Operation: op}
		frame := &pb.BulkRequestFrame{Frame: variant}
		if err := stream.Send(frame); err != nil {
			return err
		}
	}
	return stream.CloseSend()
}
