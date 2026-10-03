package weir

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestBatchResponseRejectsAmplificationBeforeObjectConstruction(t *testing.T) {
	codec := batchResponseCodec{results: 2}
	for _, count := range []int{3, 200000} {
		for _, read := range []bool{true, false} {
			raw := bytes.Repeat([]byte{0x0a, 0}, count)
			input := mem.BufferSlice{mem.SliceBuffer(raw)}
			readResponse := &pb.ReadBatchResponse{}
			mutationResponse := &pb.MutateBatchResponse{}
			var response proto.Message = mutationResponse
			if read {
				response = readResponse
			}
			err := codec.Unmarshal(input, response)
			input.Free()
			if status.Code(err) != codes.ResourceExhausted || len(readResponse.Results) != 0 || len(mutationResponse.Results) != 0 {
				t.Fatal("oversupplied reply constructed result objects", count, read, err, response)
			}
		}
	}
	input := mem.BufferSlice{mem.SliceBuffer(make([]byte, MaxBatchResponseBytes+1))}
	response := &pb.ReadBatchResponse{}
	err := codec.Unmarshal(input, response)
	input.Free()
	if status.Code(err) != codes.ResourceExhausted || len(response.Results) != 0 {
		t.Fatal("oversized reply reached protobuf construction", err, response)
	}
}

func TestBatchResponseCodecPreservesLargeMatchedReplies(t *testing.T) {
	const count = 513
	codec := batchResponseCodec{results: count}
	reads := &pb.ReadBatchResponse{Results: make([]*pb.ReadResult, count)}
	mutations := &pb.MutateBatchResponse{Results: make([]*pb.MutationResult, count)}
	for index := range reads.Results {
		reads.Results[index] = protocol.Missing()
		mutations.Results[index] = protocol.Mutation(MutationApplied, nil)
	}
	for _, source := range []proto.Message{reads, mutations} {
		raw, err := proto.Marshal(source)
		if err != nil {
			t.Fatal(err)
		}
		var input mem.BufferSlice
		for index := range raw {
			buffer := mem.SliceBuffer(raw[index : index+1])
			input = append(input, buffer)
		}
		decoded := source.ProtoReflect().New().Interface()
		err = codec.Unmarshal(input, decoded)
		input.Free()
		if err != nil || !proto.Equal(source, decoded) {
			t.Fatal("matched large reply was capped or changed", err, decoded)
		}
	}
}

func TestBatchResponseCodecRejectsMalformedEnvelope(t *testing.T) {
	codec := batchResponseCodec{results: 2}
	for _, raw := range [][]byte{{0x80}, {0x08, 0}, {0x12, 0}, {0x0a, 0x80}, {0x0a, 2, 1}} {
		input := mem.BufferSlice{mem.SliceBuffer(raw)}
		response := &pb.ReadBatchResponse{}
		err := codec.Unmarshal(input, response)
		input.Free()
		if status.Code(err) != codes.InvalidArgument || len(response.Results) != 0 {
			t.Fatal("malformed envelope reached native decoder", err, response)
		}
	}
}

type amplifiedReplyCodec struct {
	raw []byte
}

func (amplifiedReplyCodec) Name() string { return "proto" }

func (codec amplifiedReplyCodec) Marshal(any) (mem.BufferSlice, error) {
	buffer := mem.SliceBuffer(codec.raw)
	encoded := mem.BufferSlice{buffer}
	return encoded, nil
}

func (amplifiedReplyCodec) Unmarshal(input mem.BufferSlice, value any) error {
	return encoding.GetCodecV2("proto").Unmarshal(input, value)
}

type amplifiedReplyPeer struct {
	pb.UnimplementedStoreServiceServer
	reads     atomic.Int64
	mutations atomic.Int64
	applied   atomic.Int64
}

func (peer *amplifiedReplyPeer) Read(context.Context, *pb.ReadBatchRequest) (*pb.ReadBatchResponse, error) {
	peer.reads.Add(1)
	response := &pb.ReadBatchResponse{}
	return response, nil
}

func (peer *amplifiedReplyPeer) Mutate(_ context.Context, request *pb.MutateBatchRequest) (*pb.MutateBatchResponse, error) {
	peer.mutations.Add(1)
	peer.applied.Add(int64(len(request.Requests)))
	response := &pb.MutateBatchResponse{}
	return response, nil
}

func TestUnaryBatchRejectsRawAmplifiedRepliesWithoutAcknowledgementOrReplay(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	codec := amplifiedReplyCodec{raw: bytes.Repeat([]byte{0x0a, 0}, 513)}
	server := grpc.NewServer(grpc.ForceServerCodecV2(codec))
	peer := &amplifiedReplyPeer{}
	pb.RegisterStoreServiceServer(server, peer)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	connection, err := Dial("passthrough:///" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := pb.NewStoreServiceClient(connection)
	read := ReadOptions{StoreName: "records", Requests: []*ReadRequest{clientTestReadRequest(), clientTestReadRequest()}}
	readResults, err := Read(t.Context(), client, read)
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "more results than submitted requests") || len(readResults) != 2 || readResults[0] != nil || readResults[1] != nil || peer.reads.Load() != 1 {
		t.Fatal("read raw amplification bypassed per-call codec", readResults, err, peer.reads.Load())
	}
	first := &MutateRequest{Resource: "records/s:first", Action: MutationDelete}
	second := &MutateRequest{Resource: "records/s:second", Action: MutationDelete}
	mutation := MutateOptions{StoreName: "records", Requests: []*MutateRequest{first, second}}
	mutationResults, err := Mutate(t.Context(), client, mutation)
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "more results than submitted requests") || len(mutationResults) != 2 || mutationResults[0] != nil || mutationResults[1] != nil || peer.mutations.Load() != 1 || peer.applied.Load() != 2 {
		t.Fatal("mutation reply invented acknowledgement or replayed applied records", mutationResults, err, peer.mutations.Load(), peer.applied.Load())
	}
}
