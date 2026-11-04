package weir

import (
	"context"
	"errors"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

// Native sends one backend command and returns its response. Backend errors
// remain native response data, and a validated result remains evidence alongside
// a later RPC error. The body is collected into NativeResult.Body unless WithChunks
// consumes it incrementally. The request is sent again only when a server refused
// it with NativeNotStarted before any response.
func (c *Client) Native(ctx context.Context, store string, request *NativeRequest, options ...NativeOption) (*NativeResult, error) {
	client, err := c.storeClient()
	if err != nil {
		return nil, err
	}
	config := nativeOptions{}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	call := nativeCall{store: store, request: request, chunks: config.chunks}
	send := func() (*NativeResult, error) { return call.send(ctx, client) }
	return resendRefused(ctx, c, nativeRefused, send)
}

// nativeCall describes one send of a Native request.
type nativeCall struct {
	store   string
	request *NativeRequest
	chunks  func(context.Context, *NativeResponse, []byte) error
}

func (n nativeCall) send(ctx context.Context, client pb.StoreServiceClient) (*NativeResult, error) {
	if n.request == nil {
		return nil, errors.New("Native requires a request")
	}
	variant := &pb.Command_Native{Native: n.request}
	command := &pb.Command{Operation: variant}
	request := &pb.ExecuteRequest{StoreName: n.store, Index: 1, Command: command}
	var result *NativeResult
	state := nativeStreamState{}
	consume := func(ctx context.Context, event *pb.Event) (bool, error) {
		if err := state.validate(event); err != nil {
			return false, err
		}
		switch value := event.Value.(type) {
		case *pb.Event_Head:
			result = &NativeResult{Response: value.Head}
		case *pb.Event_Chunk:
			if n.chunks != nil {
				return false, n.chunks(ctx, result.Response, value.Chunk)
			}
			result.Body = append(result.Body, value.Chunk...)
		case *pb.Event_NativeEnd:
			if result == nil {
				result = &NativeResult{}
			}
			result.Completion = value.NativeEnd.Completion
			result.Failure = value.NativeEnd.Failure
			return true, nil
		}
		return false, nil
	}
	err := execute(ctx, client, request, consume)
	return result, err
}

type nativeStreamState struct {
	head bool
}

// validate checks one Native event in stream order.
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
	default:
		return errors.New("unexpected Native event")
	}
	return nil
}
