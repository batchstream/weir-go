package weir

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
)

// This fixture delivers a request and a complete response before allowing Send
// to return. It forces the legitimate gap between wire delivery and bookkeeping.
type completionClient struct {
	pb.StoreServiceClient
	stream *completionStream
}

func (client *completionClient) Execute(ctx context.Context, _ ...grpc.CallOption) (pb.StoreService_ExecuteClient, error) {
	client.stream.ctx = ctx
	return client.stream, nil
}

type completionStream struct {
	grpc.ClientStream
	ctx       context.Context
	mode      string
	delivered chan struct{}
	eofSeen   chan struct{}
	release   chan struct{}
	closed    chan struct{}
	sendErr   error
	stage     int
}

func (stream *completionStream) Context() context.Context { return stream.ctx }

func (stream *completionStream) Send(*pb.ExecuteRequest) error {
	close(stream.delivered)
	if stream.sendErr != nil {
		return stream.sendErr
	}
	select {
	case <-stream.release:
		return nil
	case <-stream.ctx.Done():
		return stream.ctx.Err()
	}
}

func (stream *completionStream) CloseSend() error {
	close(stream.closed)
	return nil
}

func (stream *completionStream) Recv() (*pb.ExecuteResponse, error) {
	<-stream.delivered
	if stream.sendErr != nil {
		<-stream.ctx.Done()
		return nil, stream.ctx.Err()
	}
	var event *pb.Event
	switch stream.mode {
	case "scan":
		if stream.stage == 0 {
			end := &pb.ScanEnd{Exhausted: true}
			value := &pb.Event_ScanEnd{ScanEnd: end}
			event = &pb.Event{Value: value}
		}
	case "native":
		if stream.stage == 0 {
			head := &pb.NativeHead{}
			value := &pb.Event_Head{Head: head}
			event = &pb.Event{Value: value}
		} else if stream.stage == 1 {
			end := &pb.NativeEnd{Completion: NativeResponseComplete}
			value := &pb.Event_NativeEnd{NativeEnd: end}
			event = &pb.Event{Value: value}
		}
	}
	stream.stage++
	if event == nil {
		close(stream.eofSeen)
		return nil, io.EOF
	}
	response := &pb.ExecuteResponse{Index: 1, Event: event}
	return response, nil
}

func TestSingleExecuteWaitsForSendAfterTerminalEOF(t *testing.T) {
	for _, mode := range []string{"scan", "native"} {
		t.Run(mode, func(t *testing.T) {
			stream := &completionStream{mode: mode, delivered: make(chan struct{}), eofSeen: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
			client := &completionClient{stream: stream}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if mode == "scan" {
					request := &ScanRequest{Resource: "records"}
					options := ScanOptions{StoreName: "records", Request: request}
					options.Consume = func(context.Context, *Document) error { return nil }
					_, err := Scan(ctx, client, options)
					done <- err
				} else {
					descriptor := &Document{ContentType: "application/octet-stream"}
					request := &NativeRequest{Resource: "records", Descriptor: descriptor}
					options := NativeOptions{StoreName: "records", Request: request}
					options.Consume = func(context.Context, *Event) error { return nil }
					_, err := Native(ctx, client, options)
					done <- err
				}
			}()
			select {
			case <-stream.eofSeen:
			case <-ctx.Done():
				t.Fatal("fixture did not complete before Send returned")
			}
			select {
			case err := <-done:
				t.Fatal("terminal EOF bypassed sender completion", err)
			case <-stream.ctx.Done():
				t.Fatal("legitimate EOF cancelled the unfinished sender", stream.ctx.Err())
			case <-time.After(50 * time.Millisecond):
			}
			close(stream.release)
			select {
			case err := <-done:
				if err != nil {
					t.Fatal("valid completion raced sender bookkeeping", err)
				}
				select {
				case <-stream.closed:
				default:
					t.Fatal("completion did not wait for CloseSend")
				}
			case <-ctx.Done():
				t.Fatal("sender completion was not joined")
			}
		})
	}
}

func TestLocalSendFailureCancelsReceiveAndJoins(t *testing.T) {
	for _, mode := range []string{"scan", "read"} {
		t.Run(mode, func(t *testing.T) {
			failure := errors.New("local send failed")
			stream := &completionStream{mode: mode, delivered: make(chan struct{}), sendErr: failure}
			client := &completionClient{stream: stream}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			var err error
			if mode == "scan" {
				request := &ScanRequest{Resource: "records"}
				options := ScanOptions{StoreName: "records", Request: request}
				options.Consume = func(context.Context, *Document) error { return nil }
				_, err = Scan(ctx, client, options)
			} else {
				options := ReadOptions{StoreName: "records", Requests: []*ReadRequest{clientTestReadRequest()}}
				_, err = Read(ctx, client, options)
			}
			if !errors.Is(err, failure) || ctx.Err() != nil {
				t.Fatal("local Send failure was lost or receive needed caller deadline", err, ctx.Err())
			}
		})
	}
}

func TestNativeRejectsInvalidBodyContentTypeBeforeRPC(t *testing.T) {
	peer := &clientTestPeer{}
	client := clientTestConnection(t, peer)
	descriptor := &Document{ContentType: "application/octet-stream"}
	request := &NativeRequest{Resource: "records", Descriptor: descriptor, BodyContentType: string([]byte{0xff})}
	options := NativeOptions{StoreName: "records", Request: request}
	options.Consume = func(context.Context, *Event) error { return nil }
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	end, err := Native(ctx, client, options)
	if err == nil || end != nil || ctx.Err() != nil || peer.received.Load() != 0 {
		t.Fatal("local protobuf error waited for a response or sent malformed input", end, err, ctx.Err(), peer.received.Load())
	}
}
