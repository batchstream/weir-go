// Package weir runs finite Execute batches with bounded parallel sending
// and receiving. Callbacks must honor their context and release each Event before
// returning. Consume exposes incremental Events; Complete observes the validated
// request end. Execute still requires final gRPC OK for whole-batch success. A
// transport error cannot invalidate an acknowledged write or prove rollback.
package weir

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type Options struct {
	StoreName string
	// Produce returns io.EOF after the finite batch. Only one Produce call runs at
	// a time and at most eight calls are retained, regardless of total batch size.
	Produce func(context.Context) (*pb.Call, error)
	// Consume receives one complete bounded business Event, not a transport chunk.
	// A scan produces a bounded page; native exchanges produce sequential Events.
	Consume func(context.Context, uint64, *pb.Event) error
	// Complete runs after one request has a validated terminal business Event and
	// empty end frame. Other requests and the RPC can still fail later.
	Complete func(context.Context, uint64) error
}

type pending struct {
	bytes          int
	kind           string
	data           []byte
	terminal, head bool
	documents      uint64
	pageSize       uint64
}

type ledger struct {
	sync.Mutex
	entries         map[uint64]*pending
	bytes, reserved int
	inputEnded      bool
	changed         chan struct{}
}

func (l *ledger) notify() { close(l.changed); l.changed = make(chan struct{}) }

func (l *ledger) reserve(ctx context.Context) error {
	for {
		l.Lock()
		if l.reserved < protocol.RouteOutstanding {
			l.reserved++
			l.Unlock()
			return nil
		}
		changed := l.changed
		l.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (l *ledger) register(ctx context.Context, id uint64, entry *pending) error {
	for {
		l.Lock()
		if l.bytes+entry.bytes <= protocol.RouteBytes {
			l.entries[id] = entry
			l.bytes += entry.bytes
			l.Unlock()
			return nil
		}
		changed := l.changed
		l.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Execute opens exactly one Execute RPC, half-closes after Produce finishes and drains
// all responses. It does not retry. Reuse the supplied gRPC connection across calls.
func Execute(ctx context.Context, client pb.StoreServiceClient, opts Options) error {
	if client == nil || opts.Produce == nil || opts.Consume == nil {
		return errors.New("Execute requires client, producer and consumer")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Execute(ctx, grpc.MaxCallSendMsgSize(protocol.MaxFrame), grpc.MaxCallRecvMsgSize(protocol.MaxResponse), grpc.MaxRetryRPCBufferSize(0))
	if err != nil {
		return err
	}
	// Commit this attempt before sending any potentially executable request.
	_ = stream.Context()
	l := &ledger{entries: make(map[uint64]*pending), changed: make(chan struct{})}
	sent := make(chan error, 1)
	go func() {
		err := produce(ctx, stream, opts, l)
		sent <- err
		if err != nil && !errors.Is(err, io.EOF) {
			cancel()
		}
	}()
	var senderErr error
	joined := false
	join := func() error {
		if !joined {
			senderErr = <-sent
			joined = true
		}
		return senderErr
	}
	defer func() { cancel(); _ = join() }()
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			l.Lock()
			remaining := len(l.entries)
			inputEnded := l.inputEnded
			l.Unlock()
			if !inputEnded {
				cancel()
				_ = join()
				return errors.New("Execute ended before input half-close; uncompleted writes are indeterminate")
			}
			if remaining != 0 {
				return errors.New("Execute ended with incomplete requests; uncompleted writes are indeterminate")
			}
			return join()
		}
		if err != nil {
			cancel()
			if sendErr := join(); sendErr != nil && !errors.Is(sendErr, context.Canceled) && !errors.Is(sendErr, io.EOF) {
				return sendErr
			}
			return fmt.Errorf("Execute interrupted; uncompleted writes are indeterminate: %w", err)
		}
		if err := protocol.ValidateExecuteResponse(frame); err != nil {
			return err
		}
		l.Lock()
		entry := l.entries[frame.RequestId]
		l.Unlock()
		if entry == nil {
			return errors.New("Execute response has unknown or completed ID")
		}
		if frame.RequestComplete {
			if len(entry.data) != 0 || !entry.terminal {
				return errors.New("Execute end before complete business result")
			}
			if opts.Complete != nil {
				if err := opts.Complete(ctx, frame.RequestId); err != nil {
					return err
				}
			}
			l.Lock()
			delete(l.entries, frame.RequestId)
			l.bytes -= entry.bytes
			l.reserved--
			l.notify()
			l.Unlock()
			continue
		}
		if entry.terminal {
			return errors.New("Execute data after terminal business result")
		}
		if len(entry.data)+len(frame.EventFragment) > protocol.MaxEvent+10+protocol.NativeChunk {
			return errors.New("Execute event exceeds bounded decode workspace")
		}
		entry.data = append(entry.data, frame.EventFragment...)
		for len(entry.data) > 0 {
			length, n := binary.Uvarint(entry.data)
			if n == 0 {
				if len(entry.data) >= 10 {
					return errors.New("invalid Event length")
				}
				break
			}
			if n < 0 || length == 0 || length > protocol.MaxEvent {
				return errors.New("invalid Event length")
			}
			if uint64(len(entry.data)-n) < length {
				break
			}
			event := &pb.Event{}
			if err := proto.Unmarshal(entry.data[n:n+int(length)], event); err != nil {
				return err
			}
			// The same strict recursive unknown-field and size rules apply both ways.
			if err := protocol.ValidateEvent(event); err != nil {
				return err
			}
			if err := validateEvent(entry, frame.RequestId, event); err != nil {
				return err
			}
			if err := opts.Consume(ctx, frame.RequestId, event); err != nil {
				return err
			}
			consumed := n + int(length)
			if consumed == len(entry.data) {
				entry.data = nil
			} else {
				entry.data = entry.data[consumed:]
			}
		}
	}
}

func produce(ctx context.Context, stream grpc.BidiStreamingClient[pb.ExecuteRequest, pb.ExecuteResponse], opts Options, l *ledger) error {
	var id uint64
	for {
		if err := l.reserve(ctx); err != nil {
			return err
		}
		call, err := opts.Produce(ctx)
		if errors.Is(err, io.EOF) {
			l.Lock()
			l.reserved--
			l.inputEnded = true
			l.notify()
			l.Unlock()
			return stream.CloseSend()
		}
		if err != nil {
			return err
		}
		if id == ^uint64(0) {
			return errors.New("Execute ID space exhausted")
		}
		id++
		if call == nil || call.Version != 1 || call.Operation == nil || proto.Size(call) > protocol.MaxPayload {
			return errors.New("invalid Call")
		}
		data, err := proto.Marshal(call)
		if err != nil {
			return err
		}
		if _, err := protocol.DecodeCall(data); err != nil {
			return err
		}
		request := &pb.ExecuteRequest{RequestId: id, StoreName: opts.StoreName, CallPayload: data}
		if err := protocol.ValidateExecuteRequest(request, opts.StoreName, id-1); err != nil {
			return err
		}
		entry := &pending{bytes: len(data), kind: callKind(call)}
		if request := call.GetScan(); request != nil {
			if request.PageSize > protocol.MaxScanPageSize || len(request.ContinuationToken) > protocol.MaxScanToken {
				return errors.New("Scan page size or continuation exceeds bound")
			}
			entry.pageSize = protocol.ScanPageSize(request)
		}
		if err := l.register(ctx, id, entry); err != nil {
			return err
		}
		if err := stream.Send(request); err != nil {
			return err
		}
	}
}

func callKind(call *pb.Call) string {
	switch call.Operation.(type) {
	case *pb.Call_Read:
		return "read"
	case *pb.Call_Mutate:
		return "mutate"
	case *pb.Call_Scan:
		return "scan"
	case *pb.Call_Native:
		return "native"
	}
	return ""
}

func validateEvent(p *pending, id uint64, e *pb.Event) error {
	if p.terminal {
		return errors.New("Event after terminal result")
	}
	switch value := e.Value.(type) {
	case *pb.Event_Result:
		r := value.Result
		if r == nil || r.Index != id || p.kind == "read" && r.GetRead() == nil || p.kind == "mutate" && r.GetMutation() == nil || p.kind != "read" && p.kind != "mutate" {
			return errors.New("invalid record result")
		}
		if p.kind == "read" && r.GetRead().Result == nil {
			return errors.New("missing read result")
		}
		if p.kind == "mutate" && (r.GetMutation().Outcome < pb.MutationOutcome_NOT_STARTED || r.GetMutation().Outcome > pb.MutationOutcome_UNKNOWN) {
			return errors.New("invalid write outcome")
		}
		p.terminal = true
	case *pb.Event_Document:
		if p.kind != "scan" || value.Document == nil || len(value.Document.Data) > protocol.MaxDocument {
			return errors.New("unexpected scan document")
		}
		p.documents++
		if p.documents > p.pageSize {
			return errors.New("Scan exceeded requested page size")
		}
	case *pb.Event_ScanEnd:
		if p.kind != "scan" || value.ScanEnd == nil || value.ScanEnd.DocumentCount != p.documents {
			return errors.New("invalid scan completion")
		}
		p.terminal = true
	case *pb.Event_Head:
		if p.kind != "native" || p.head || value.Head == nil {
			return errors.New("invalid native response head")
		}
		p.head = true
	case *pb.Event_Chunk:
		if p.kind != "native" || !p.head {
			return errors.New("native chunk before head")
		}
	case *pb.Event_NativeEnd:
		if p.kind != "native" || value.NativeEnd == nil || value.NativeEnd.Completion == pb.NativeCompletion_NATIVE_COMPLETION_UNSPECIFIED {
			return errors.New("invalid native completion")
		}
		if value.NativeEnd.Completion == pb.NativeCompletion_RESPONSE_COMPLETE && !p.head {
			return errors.New("complete native response without head")
		}
		p.terminal = true
	default:
		return errors.New("missing Event value")
	}
	return nil
}

// Record executes one read or mutation. It collects its single bounded result.
// Use Execute for batches, scans and native streaming results. It never retries.
// A transport error can accompany a validated business result. Check the error
// for RPC completion and retain the result as backend evidence; APPLIED is not
// invalidated by a missing end frame or a later non-OK RPC status.
type RecordOptions struct {
	StoreName string
	Call      *pb.Call
}

func Record(ctx context.Context, client pb.StoreServiceClient, opts RecordOptions) (*pb.Result, error) {
	if opts.Call == nil || opts.Call.GetRead() == nil && opts.Call.GetMutate() == nil {
		return nil, errors.New("Record requires a read or mutation Call")
	}
	produced := false
	var result *pb.Result
	batch := Options{StoreName: opts.StoreName}
	batch.Produce = func(context.Context) (*pb.Call, error) {
		if produced {
			return nil, io.EOF
		}
		produced = true
		return opts.Call, nil
	}
	batch.Consume = func(_ context.Context, _ uint64, e *pb.Event) error { result = e.GetResult(); return nil }
	if err := Execute(ctx, client, batch); err != nil {
		return result, err
	}
	if result == nil {
		return nil, errors.New("missing record result")
	}
	return result, nil
}

// ScanPage executes one finite page and consumes each document incrementally.
// Only a successful Execute end and final gRPC OK expose its next checkpoint.
// Retrying an interrupted page uses the previous checkpoint and may redeliver
// documents; the caller owns deduplication and committing consumed output.
type ScanPageOptions struct {
	StoreName string
	Request   *pb.ScanRequest
	Consume   func(context.Context, *pb.Document) error
}

func ScanPage(ctx context.Context, client pb.StoreServiceClient, opts ScanPageOptions) (*pb.ScanEnd, error) {
	if opts.Request == nil || opts.Consume == nil {
		return nil, errors.New("ScanPage requires a request and document consumer")
	}
	variant := &pb.Call_Scan{Scan: opts.Request}
	call := &pb.Call{Version: 1, Operation: variant}
	produced := false
	var end *pb.ScanEnd
	batch := Options{StoreName: opts.StoreName}
	batch.Produce = func(context.Context) (*pb.Call, error) {
		if produced {
			return nil, io.EOF
		}
		produced = true
		return call, nil
	}
	batch.Consume = func(ctx context.Context, _ uint64, event *pb.Event) error {
		if document := event.GetDocument(); document != nil {
			return opts.Consume(ctx, document)
		}
		end = event.GetScanEnd()
		return nil
	}
	if err := Execute(ctx, client, batch); err != nil {
		return nil, err
	}
	if end == nil {
		return nil, errors.New("missing Scan completion")
	}
	return end, nil
}
