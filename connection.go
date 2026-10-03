package weir

import (
	"github.com/batchstream/weir-protocol/api/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Dial creates a low-level connection to a known Store's plaintext listener.
// Use Open to discover Store destinations through an initialization endpoint.
// Reuse the connection across calls and close it after callers finish.
func Dial(address string) (*grpc.ClientConn, error) {
	return grpc.NewClient(address, connectionOptions()...)
}

func connectionOptions() []grpc.DialOption {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(),
		grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(),
		grpc.WithMaxHeaderListSize(16 << 10),
		grpc.WithDefaultCallOptions(grpc.MaxRetryRPCBufferSize(0), grpc.MaxCallSendMsgSize(protocol.MaxBatchRequestBytes), grpc.MaxCallRecvMsgSize(protocol.MaxBatchResponseBytes)),
	}
	return options
}
