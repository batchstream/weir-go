// Package storetest serves a fake Weir to the tests of the mongodb and search
// packages.
package storetest

import (
	"io"
	"net"
	"sync"
	"testing"

	weir "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
)

// Handler answers one command sent to a Store with the events Weir would send.
type Handler func(store string, command *pb.Command) []*pb.Event

type server struct {
	mu      sync.Mutex
	handler Handler
}

func (s *server) Execute(stream grpc.BidiStreamingServer[pb.ExecuteRequests, pb.ExecuteResponses]) error {
	// A stream may name its Store only in its first request.
	store := ""
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		for _, request := range message.Requests {
			if request.CancelIndex != 0 {
				continue
			}
			if request.StoreName != "" {
				store = request.StoreName
			}
			s.mu.Lock()
			events := s.handler(store, request.Command)
			s.mu.Unlock()
			for _, event := range events {
				response := &pb.ExecuteResponse{Index: request.Index, Event: event}
				responses := &pb.ExecuteResponses{Responses: []*pb.ExecuteResponse{response}}
				if err := stream.Send(responses); err != nil {
					return err
				}
			}
		}
	}
}

// Client serves handler on loopback until the test ends and returns a Client
// of it. The handler runs for one command at a time.
func Client(t testing.TB, handler Handler) *weir.Client {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fake := &server{handler: handler}
	service := grpc.NewServer()
	pb.RegisterStoreServiceServer(service, fake)
	done := make(chan error, 1)
	go func() { done <- service.Serve(listener) }()
	client, err := weir.New(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		service.Stop()
		<-done
	})
	return client
}

// Read is the event of a read that returned document.
func Read(document *pb.Document) []*pb.Event {
	result := &pb.ReadResult{Result: &pb.ReadResult_Document{Document: document}}
	return readEvent(result)
}

// Missing is the event of a read that found no record.
func Missing() []*pb.Event {
	missing := &pb.Empty{}
	result := &pb.ReadResult{Result: &pb.ReadResult_Missing{Missing: missing}}
	return readEvent(result)
}

// ReadFailure is the event of a failed read.
func ReadFailure(code pb.FailureCode) []*pb.Event {
	failure := &pb.Failure{Code: code, Message: "test failure"}
	result := &pb.ReadResult{Result: &pb.ReadResult_Failure{Failure: failure}}
	return readEvent(result)
}

func readEvent(result *pb.ReadResult) []*pb.Event {
	value := &pb.Event_ReadResult{ReadResult: result}
	event := &pb.Event{Value: value}
	return []*pb.Event{event}
}

// Applied is the event of an applied mutation.
func Applied() []*pb.Event {
	return Mutation(pb.MutationOutcome_APPLIED, 0)
}

// Mutation is the event of a mutation result, with a Failure unless code is
// zero.
func Mutation(outcome pb.MutationOutcome, code pb.FailureCode) []*pb.Event {
	result := &pb.MutationResult{Outcome: outcome}
	if code != 0 {
		result.Failure = &pb.Failure{Code: code, Message: "test failure"}
	}
	value := &pb.Event_MutationResult{MutationResult: result}
	event := &pb.Event{Value: value}
	return []*pb.Event{event}
}

// Native is the events of a complete Native response.
func Native(metadata *pb.Document, body []byte) []*pb.Event {
	events := NativeStarted(metadata, body)
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	value := &pb.Event_NativeEnd{NativeEnd: end}
	event := &pb.Event{Value: value}
	return append(events, event)
}

// NativeStarted is the events of a Native response that began, without its
// end.
func NativeStarted(metadata *pb.Document, body []byte) []*pb.Event {
	head := &pb.NativeHead{Metadata: metadata}
	headValue := &pb.Event_Head{Head: head}
	headEvent := &pb.Event{Value: headValue}
	events := []*pb.Event{headEvent}
	if len(body) > 0 {
		chunk := &pb.Event_Chunk{Chunk: body}
		chunkEvent := &pb.Event{Value: chunk}
		events = append(events, chunkEvent)
	}
	return events
}

// NativeIncomplete is the events of a Native response that began and stopped
// before its end with a Failure.
func NativeIncomplete(metadata *pb.Document, body []byte, code pb.FailureCode) []*pb.Event {
	events := NativeStarted(metadata, body)
	failure := &pb.Failure{Code: code, Message: "test failure"}
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_INCOMPLETE, Failure: failure}
	value := &pb.Event_NativeEnd{NativeEnd: end}
	event := &pb.Event{Value: value}
	return append(events, event)
}
