package weir

import (
	"bufio"
	"encoding/binary"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
)

// batchResponseCodec bounds native protobuf construction by this call's input
// count. A small reply cannot allocate an unrelated number of result objects.
type batchResponseCodec struct {
	results int
}

func (batchResponseCodec) Name() string { return "proto" }

func (batchResponseCodec) Marshal(value any) (mem.BufferSlice, error) {
	return encoding.GetCodecV2("proto").Marshal(value)
}

func (codec batchResponseCodec) Unmarshal(data mem.BufferSlice, value any) error {
	if data.Len() > MaxBatchResponseBytes {
		return status.Error(codes.ResourceExhausted, "batch response exceeds protobuf byte budget")
	}
	source := data.Reader()
	defer source.Close()
	reader := bufio.NewReader(source)
	results := 0
	for {
		tag, err := binary.ReadUvarint(reader)
		if err == io.EOF {
			break
		}
		if err != nil || tag != 0x0a {
			return status.Error(codes.InvalidArgument, "invalid batch response framing")
		}
		length, err := binary.ReadUvarint(reader)
		if err != nil || length > uint64(data.Len()) {
			return status.Error(codes.InvalidArgument, "invalid batch result length")
		}
		results++
		if results > codec.results {
			return status.Error(codes.ResourceExhausted, "batch response contains more results than submitted requests")
		}
		if _, err := reader.Discard(int(length)); err != nil {
			return status.Error(codes.InvalidArgument, "truncated batch result")
		}
	}
	return encoding.GetCodecV2("proto").Unmarshal(data, value)
}
