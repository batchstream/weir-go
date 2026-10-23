package weir

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/credentials/insecure"
)

var ErrClosed = errors.New("Weir client is closed")

// Client sends business requests to the Weir servers behind one application
// address. Every server behind the address must host the Stores the client
// uses. Reuse a Client across goroutines and close it after callers finish.
// It never replays a business request.
type Client struct {
	connection *grpc.ClientConn
	store      pb.StoreServiceClient
	closed     atomic.Bool
}

// New returns a Client for a Weir application address such as "weir:7447".
// It connects on first use and reconnects after failures. A DNS name that
// resolves to several servers spreads calls across them.
func New(address string) (*Client, error) {
	connection, err := Dial(address)
	if err != nil {
		return nil, err
	}
	client := &Client{connection: connection, store: pb.NewStoreServiceClient(connection)}
	return client, nil
}

// Dial creates a low-level connection for the package-level operation
// functions. The caller owns its lifetime.
func Dial(address string) (*grpc.ClientConn, error) {
	return grpc.NewClient(address, connectionOptions()...)
}

func connectionOptions() []grpc.DialOption {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(),
		grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`),
		grpc.WithMaxHeaderListSize(^uint32(0)),
		grpc.WithDefaultCallOptions(grpc.MaxRetryRPCBufferSize(0), grpc.MaxCallSendMsgSize(protocol.MaxTransportMessageBytes), grpc.MaxCallRecvMsgSize(protocol.MaxTransportMessageBytes)),
	}
	return options
}

func (c *Client) storeClient() (pb.StoreServiceClient, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	return c.store, nil
}

func (c *Client) ReadOne(ctx context.Context, options ReadOneOptions) (*ReadResult, error) {
	client, err := c.storeClient()
	if err != nil {
		return nil, err
	}
	return ReadOne(ctx, client, options)
}

func (c *Client) Create(ctx context.Context, options WriteOptions) (*MutationResult, error) {
	client, err := c.storeClient()
	if err != nil {
		return nil, err
	}
	return Create(ctx, client, options)
}

func (c *Client) Put(ctx context.Context, options WriteOptions) (*MutationResult, error) {
	client, err := c.storeClient()
	if err != nil {
		return nil, err
	}
	return Put(ctx, client, options)
}

func (c *Client) Replace(ctx context.Context, options WriteOptions) (*MutationResult, error) {
	client, err := c.storeClient()
	if err != nil {
		return nil, err
	}
	return Replace(ctx, client, options)
}

func (c *Client) Delete(ctx context.Context, options DeleteOptions) (*MutationResult, error) {
	client, err := c.storeClient()
	if err != nil {
		return nil, err
	}
	return Delete(ctx, client, options)
}

func (c *Client) AtomicTransform(ctx context.Context, options AtomicTransformOptions) (*MutationResult, error) {
	client, err := c.storeClient()
	if err != nil {
		return nil, err
	}
	return AtomicTransform(ctx, client, options)
}

func (c *Client) Native(ctx context.Context, options NativeOptions) (*NativeResult, error) {
	client, err := c.storeClient()
	if err != nil {
		return nil, err
	}
	return Native(ctx, client, options)
}

// Close cancels active requests and closes the connection.
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	return c.connection.Close()
}
