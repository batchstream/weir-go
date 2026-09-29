package weir

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *fixtureServer) Scan(_ *pb.ScanRequest, stream grpc.ServerStreamingServer[pb.ScanResponseFrame]) error {
	s.calls.Add(1)
	doc := &pb.Document{MediaType: MediaTypeJSON, Data: []byte(`{"n":1}`)}
	variant := &pb.ScanResponseFrame_Document{Document: doc}
	frame := &pb.ScanResponseFrame{Frame: variant}
	if err := stream.Send(frame); err != nil {
		return err
	}
	if s.mode == "missing-end" {
		return nil
	}
	if s.mode == "wait" {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	end := &pb.ScanEnd{DocumentCount: 1}
	if s.mode == "count" {
		end.DocumentCount++
	}
	if s.mode == "failure" {
		end.Failure = &pb.Failure{Code: pb.FailureCode_UNAVAILABLE}
	}
	endVariant := &pb.ScanResponseFrame_End{End: end}
	endFrame := &pb.ScanResponseFrame{Frame: endVariant}
	if err := stream.Send(endFrame); err != nil {
		return err
	}
	if s.mode == "duplicate-end" {
		return stream.Send(endFrame)
	}
	if s.mode == "after-end" {
		return stream.Send(frame)
	}
	if s.mode == "status-after-end" {
		return status.Error(codes.Unavailable, "after End")
	}
	return nil
}

func TestScanCompletion(t *testing.T) {
	for _, mode := range []string{"ok", "count", "missing-end", "failure", "duplicate-end", "after-end", "status-after-end"} {
		t.Run(mode, func(t *testing.T) {
			client, fixture := newFixture(t, mode)
			req := &pb.ScanRequest{Resource: "weir://mongo/db/records"}
			count := 0
			_, err := client.Scan(context.Background(), req, func(doc *pb.Document) error { count++; return nil })
			if (err == nil) != (mode == "ok") || count != 1 || fixture.calls.Load() != 1 {
				t.Fatalf("count=%d calls=%d err=%v", count, fixture.calls.Load(), err)
			}
		})
	}
}

func TestScanCallbackCancels(t *testing.T) {
	client, _ := newFixture(t, "wait")
	req := &pb.ScanRequest{Resource: "weir://mongo/db/records"}
	stop := errors.New("consumer stopped")
	_, err := client.Scan(context.Background(), req, func(*pb.Document) error { return stop })
	if !errors.Is(err, stop) {
		t.Fatalf("callback error lost: %v", err)
	}
}

func nativeRequest() NativeRequest {
	descriptor := &pb.Document{MediaType: "application/vnd.weir.mongodb-command.v1+bson"}
	open := &pb.NativeOpen{Resource: "weir://mongo/db/records", Descriptor_: descriptor, BodyMediaType: MediaTypeBSON}
	req := NativeRequest{Open: open, Body: []byte("fixture")}
	return req
}

func (s *fixtureServer) Native(stream grpc.BidiStreamingServer[pb.NativeRequestFrame, pb.NativeResponseFrame]) error {
	s.calls.Add(1)
	if _, err := stream.Recv(); err != nil {
		return err
	}
	// early-return does not read input, exercising cancellation of a blocked sender.
	if s.mode != "early-return" {
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
		}
	}
	head := &pb.NativeHead{BodyMediaType: MediaTypeBSON}
	headVariant := &pb.NativeResponseFrame_Head{Head: head}
	headFrame := &pb.NativeResponseFrame{Frame: headVariant}
	chunkVariant := &pb.NativeResponseFrame_Chunk{Chunk: []byte("response")}
	chunkFrame := &pb.NativeResponseFrame{Frame: chunkVariant}
	if s.mode == "chunk-before-head" {
		return stream.Send(chunkFrame)
	}
	if s.mode != "no-head" && s.mode != "not-started" && s.mode != "early-return" {
		if err := stream.Send(headFrame); err != nil {
			return err
		}
		if s.mode == "duplicate-head" {
			return stream.Send(headFrame)
		}
		if err := stream.Send(chunkFrame); err != nil {
			return err
		}
	}
	if s.mode == "missing-end" {
		return nil
	}
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	if s.mode == "incomplete" {
		end.Completion = pb.NativeCompletion_RESPONSE_INCOMPLETE
		end.Failure = &pb.Failure{Code: pb.FailureCode_UNAVAILABLE}
	}
	if s.mode == "not-started" || s.mode == "early-return" {
		end.Completion = pb.NativeCompletion_NATIVE_NOT_STARTED
		end.Failure = &pb.Failure{Code: pb.FailureCode_INVALID_ARGUMENT}
	}
	if s.mode == "unspecified" {
		end.Completion = pb.NativeCompletion_NATIVE_COMPLETION_UNSPECIFIED
	}
	endVariant := &pb.NativeResponseFrame_End{End: end}
	endFrame := &pb.NativeResponseFrame{Frame: endVariant}
	if err := stream.Send(endFrame); err != nil {
		return err
	}
	if s.mode == "duplicate-end" {
		return stream.Send(endFrame)
	}
	if s.mode == "after-end" {
		return stream.Send(chunkFrame)
	}
	if s.mode == "status-after-end" {
		return status.Error(codes.Unavailable, "after End")
	}
	return nil
}

func TestNativeCompletion(t *testing.T) {
	for _, mode := range []string{"ok", "chunk-before-head", "duplicate-head", "no-head", "missing-end", "incomplete", "not-started", "unspecified", "duplicate-end", "after-end", "status-after-end"} {
		t.Run(mode, func(t *testing.T) {
			client, fixture := newFixture(t, mode)
			result, err := client.Native(context.Background(), nativeRequest(), func([]byte) error { return nil })
			if (err == nil) != (mode == "ok") || fixture.calls.Load() != 1 {
				t.Fatalf("result=%v err=%v calls=%d", result, err, fixture.calls.Load())
			}
		})
	}
}

func TestNativeJoinsBlockedUpload(t *testing.T) {
	client, _ := newFixture(t, "early-return")
	request := nativeRequest()
	request.Body = make([]byte, MaxNativeBodyBytes)
	start := time.Now()
	result, err := client.Native(context.Background(), request, func([]byte) error { return nil })
	if err == nil || result.End == nil || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("failed early completion/join: %v %v", result, err)
	}
}

func TestNativeCallbackError(t *testing.T) {
	client, _ := newFixture(t, "ok")
	stop := errors.New("consumer stopped")
	_, err := client.Native(context.Background(), nativeRequest(), func([]byte) error { return stop })
	if !errors.Is(err, stop) {
		t.Fatalf("callback error lost: %v", err)
	}
}
