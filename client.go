// Package weir provides a bounded, non-replaying client for the Weir data plane.
// The public wire types come from github.com/batchstream/weir/api/weir/v1.
package weir

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	MaxDocumentBytes   = 256 << 10
	MaxFrameBytes      = 300 << 10
	NativeChunkBytes   = 64 << 10
	MaxBulkOperations  = 1024
	MaxBulkBytes       = 8 << 20
	MaxNativeBodyBytes = 8 << 20
	MediaTypeJSON      = "application/json"
	MediaTypeBSON      = "application/bson"
)

// Options configures a client. Its zero value uses verified TLS and a 30-second
// call timeout. Plaintext must be explicitly enabled for an isolated network.
type Options struct {
	Plaintext bool
	TLSConfig *tls.Config
	Timeout   time.Duration
}

// Client owns one gRPC connection and is safe for concurrent use. Requests and
// their payloads must not be mutated until the call returns. Close after use.
type Client struct {
	conn    *grpc.ClientConn
	rpc     pb.WeirClient
	timeout time.Duration
}

// New creates a lazy connection to an application listener. TLSConfig is cloned.
// It disables configured retries and retry buffering. No SDK operation retries,
// fails over, restarts a stream, or sends peer-only hop metadata.
func New(target string, opts Options) (*Client, error) {
	if target == "" || opts.Timeout < 0 || opts.Plaintext && opts.TLSConfig != nil {
		return nil, fmt.Errorf("weir: invalid client options")
	}
	if opts.Timeout == 0 {
		opts.Timeout = 30 * time.Second
	}
	var transport credentials.TransportCredentials
	if opts.Plaintext {
		transport = insecure.NewCredentials()
	} else {
		config := opts.TLSConfig
		if config == nil {
			config = &tls.Config{MinVersion: tls.VersionTLS12}
		} else {
			config = config.Clone()
			if config.InsecureSkipVerify {
				return nil, fmt.Errorf("weir: TLS verification cannot be disabled")
			}
			if config.MinVersion < tls.VersionTLS12 {
				config.MinVersion = tls.VersionTLS12
			}
		}
		transport = credentials.NewTLS(config)
	}
	dial := []grpc.DialOption{
		grpc.WithTransportCredentials(transport),
		grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxFrameBytes),
			grpc.MaxCallSendMsgSize(MaxFrameBytes), grpc.MaxRetryRPCBufferSize(0),
			grpc.WaitForReady(false)),
	}
	conn, err := grpc.NewClient(target, dial...)
	if err != nil {
		return nil, err
	}
	client := &Client{conn: conn, rpc: pb.NewWeirClient(conn), timeout: opts.Timeout}
	return client, nil
}

func (c *Client) Close() error { return c.conn.Close() }

// Read returns a missing result without error. Server failures are FailureError.
func (c *Client) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResult, error) {
	if err := validateRead(req); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	result, err := c.rpc.Read(ctx, req)
	if err != nil {
		return nil, err
	}
	return result, readError(result)
}

// Mutate always returns an outcome. Local validation returns NOT_STARTED;
// transport errors or malformed replies return UNKNOWN and must not be replayed.
// A valid server failure is exposed through MutationError and FailureError.
func (c *Client) Mutate(ctx context.Context, req *pb.MutateRequest) (*pb.MutationResult, error) {
	if err := validateMutation(req); err != nil {
		result := &pb.MutationResult{Outcome: pb.MutationOutcome_NOT_STARTED}
		failure := &MutationError{Outcome: result.Outcome, Cause: err}
		return result, failure
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	result, err := c.rpc.Mutate(ctx, req)
	if err == nil {
		err = mutationError(result)
		if _, invalid := err.(*ProtocolError); !invalid {
			return result, err
		}
	}
	result = &pb.MutationResult{Outcome: pb.MutationOutcome_UNKNOWN}
	failure := &MutationError{Outcome: result.Outcome, Cause: err}
	return result, failure
}
