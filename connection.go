package weir

import (
	"github.com/batchstream/weir/api/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Dial creates a low-level connection to a known Store's plaintext listener.
// Use Open to discover Store destinations through an initialization endpoint.
// Static windows and bounded buffers limit transport workspace. Close the
// returned connection after all finite batches finish.
func Dial(address string) (*grpc.ClientConn, error) {
	return grpc.NewClient(address, connectionOptions()...)
}

func connectionOptions() []grpc.DialOption {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(),
		grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(),
		grpc.WithStaticStreamWindowSize(65535), grpc.WithStaticConnWindowSize(65535),
		grpc.WithReadBufferSize(16 << 10), grpc.WithWriteBufferSize(16 << 10), grpc.WithMaxHeaderListSize(16 << 10),
		grpc.WithDefaultCallOptions(grpc.MaxRetryRPCBufferSize(0), grpc.MaxCallSendMsgSize(protocol.MaxFrame), grpc.MaxCallRecvMsgSize(protocol.MaxResponse)),
	}
	return options
}
