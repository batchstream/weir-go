package main

import (
	"context"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

type ownerFixture struct {
	pb.UnimplementedStoreServiceServer
	endpoint string
}

func (s *ownerFixture) ResolveStore(_ context.Context, request *pb.ResolveStoreRequest) (*pb.ResolveStoreResponse, error) {
	response := &pb.ResolveStoreResponse{StoreName: request.StoreName, Endpoints: []string{s.endpoint}, CacheTtlMs: 30000}
	return response, nil
}
func (s *uncertainServer) Mutate(ctx context.Context, request *pb.MutateBatchRequest) (*pb.MutateBatchResponse, error) {
	response := &pb.MutateBatchResponse{}
	for _, item := range request.Requests {
		result, err := s.mutate(ctx, item)
		if err != nil {
			return nil, err
		}
		response.Results = append(response.Results, result)
	}
	return response, nil
}
func (s *fixedTargetServer) Mutate(ctx context.Context, request *pb.MutateBatchRequest) (*pb.MutateBatchResponse, error) {
	response := &pb.MutateBatchResponse{}
	for _, item := range request.Requests {
		result, err := s.mutate(ctx, request.StoreName, item)
		if err != nil {
			return nil, err
		}
		response.Results = append(response.Results, result)
	}
	return response, nil
}
func (s *blockedWriteServer) Mutate(ctx context.Context, request *pb.MutateBatchRequest) (*pb.MutateBatchResponse, error) {
	response := &pb.MutateBatchResponse{}
	for _, item := range request.Requests {
		result, err := s.mutate(ctx, item)
		if err != nil {
			return nil, err
		}
		response.Results = append(response.Results, result)
	}
	return response, nil
}
