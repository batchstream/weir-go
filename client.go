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

type ExecuteOptions struct {
	StoreName string
	// Produce returns io.EOF after the finite batch. Only one Produce wireCommand runs at
	// a time and at most eight commands are retained, regardless of total batch size.
	Produce func(context.Context) (*Command, error)
	// Consume receives one complete bounded business Event, not a transport chunk.
	// A scan produces a bounded page; native exchanges produce sequential Events.
	Consume func(context.Context, uint64, *Event) error
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
func Execute(ctx context.Context, client pb.StoreServiceClient, opts ExecuteOptions) error {
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
			if err := opts.Consume(ctx, frame.RequestId, businessEvent(event)); err != nil {
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

func produce(ctx context.Context, stream grpc.BidiStreamingClient[pb.ExecuteRequest, pb.ExecuteResponse], opts ExecuteOptions, l *ledger) error {
	var id uint64
	for {
		if err := l.reserve(ctx); err != nil {
			return err
		}
		command, err := opts.Produce(ctx)
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
		if command == nil {
			return errors.New("invalid Command")
		}
		if command.err != nil {
			return command.err
		}
		wireCommand := command.wire
		if wireCommand == nil || wireCommand.Version != 1 || wireCommand.Operation == nil || proto.Size(wireCommand) > protocol.MaxPayload {
			return errors.New("invalid Command")
		}
		data, err := proto.Marshal(wireCommand)
		if err != nil {
			return err
		}
		if _, err := protocol.DecodeCommand(data); err != nil {
			return err
		}
		request := &pb.ExecuteRequest{RequestId: id, StoreName: opts.StoreName, CommandPayload: data}
		if err := protocol.ValidateExecuteRequest(request, opts.StoreName, id-1); err != nil {
			return err
		}
		entry := &pending{bytes: len(data), kind: commandKind(wireCommand)}
		if request := wireCommand.GetScan(); request != nil {
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

func commandKind(wireCommand *pb.Command) string {
	switch wireCommand.Operation.(type) {
	case *pb.Command_Read:
		return "read"
	case *pb.Command_Mutate:
		return "mutate"
	case *pb.Command_Scan:
		return "scan"
	case *pb.Command_Native:
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
