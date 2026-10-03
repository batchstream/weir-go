package main

import (
	"context"
	"encoding/binary"
	"errors"
	"io"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type ownerFixture struct {
	pb.UnimplementedStoreServiceServer
	endpoint string
}

func (s *ownerFixture) ResolveStore(_ context.Context, request *pb.ResolveStoreRequest) (*pb.ResolveStoreResponse, error) {
	response := &pb.ResolveStoreResponse{StoreName: request.StoreName, Endpoints: []string{s.endpoint}, CacheTtlMs: 30000}
	return response, nil
}
func receiveMutation(stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse]) (*pb.ExecuteRequest, *pb.MutateRequest, error) {
	frame, err := stream.Recv()
	if err != nil {
		return nil, nil, err
	}
	call := &pb.Command{}
	if err := proto.Unmarshal(frame.CommandPayload, call); err != nil {
		return nil, nil, err
	}
	if call.GetMutate() == nil {
		return nil, nil, errors.New("fixture expects mutation")
	}
	return frame, call.GetMutate(), nil
}
func sendMutation(stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse], id uint64, mutation *pb.MutationResult) error {
	resultVariant := &pb.Result_Mutation{Mutation: mutation}
	result := &pb.Result{Index: id, Result: resultVariant}
	eventVariant := &pb.Event_Result{Result: result}
	event := &pb.Event{Version: 1, Value: eventVariant}
	raw, err := proto.Marshal(event)
	if err != nil {
		return err
	}
	data := binary.AppendUvarint(nil, uint64(len(raw)))
	data = append(data, raw...)
	frame := &pb.ExecuteResponse{RequestId: id, EventFragment: data}
	if err := stream.Send(frame); err != nil {
		return err
	}
	complete := &pb.ExecuteResponse{RequestId: id, RequestComplete: true}
	return stream.Send(complete)
}

func (s *uncertainServer) Execute(stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse]) error {
	for {
		frame, request, err := receiveMutation(stream)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		result, err := s.mutate(stream.Context(), request)
		if err != nil {
			return err
		}
		if err := sendMutation(stream, frame.RequestId, result); err != nil {
			return err
		}
	}
}
func (s *fixedTargetServer) Execute(stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse]) error {
	for {
		frame, request, err := receiveMutation(stream)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		result, err := s.mutate(stream.Context(), frame.StoreName, request)
		if err != nil {
			return err
		}
		if err := sendMutation(stream, frame.RequestId, result); err != nil {
			return err
		}
	}
}
func (s *blockedWriteServer) Execute(stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse]) error {
	for {
		frame, request, err := receiveMutation(stream)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		result, err := s.mutate(stream.Context(), request)
		if err != nil {
			return err
		}
		if err := sendMutation(stream, frame.RequestId, result); err != nil {
			return err
		}
	}
}
