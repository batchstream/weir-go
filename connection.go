package weir

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/credentials/insecure"
)

// ErrClosed reports a call started after Close. Calls already running when
// Close is called end with the gRPC cancellation error instead.
var ErrClosed = errors.New("Weir client is closed")

// Client sends business requests to the Weir servers behind one application
// address. Every server behind the address must host the Stores the client
// uses. Reuse a Client across goroutines and close it after callers finish.
// It never replays a business request.
type Client struct {
	connection *grpc.ClientConn
	store      pb.StoreServiceClient
	closed     atomic.Bool
	closeOnce  sync.Once
	closeErr   error
}

// New returns a Client for a Weir application address such as "weir:7447".
// The address is a host name or IP literal with an explicit port. The Client
// connects on first use and reconnects after failures. A DNS name that
// resolves to several servers spreads calls across them.
func New(address string) (*Client, error) {
	if err := validateAddress(address); err != nil {
		return nil, err
	}
	connection, err := Dial(address)
	if err != nil {
		return nil, err
	}
	client := &Client{connection: connection, store: pb.NewStoreServiceClient(connection)}
	return client, nil
}

// Dial creates a low-level connection to any gRPC target for the
// package-level operation functions. The caller owns target validation and
// the connection's lifetime.
func Dial(target string) (*grpc.ClientConn, error) {
	return grpc.NewClient(target, connectionOptions()...)
}

func validateAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	number, portErr := strconv.Atoi(port)
	valid := err == nil && portErr == nil && strings.Trim(port, "0123456789") == "" &&
		number >= 1 && number <= 65535 && (net.ParseIP(host) != nil || validHostname(host))
	if !valid {
		return fmt.Errorf("Weir address %q must be host:port", address)
	}
	return nil
}

func validHostname(host string) bool {
	if host == "" || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(strings.TrimSuffix(host, "."), ".") {
		if label == "" || len(label) > 63 || strings.Trim(label, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_") != "" {
			return false
		}
	}
	return true
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

// Close cancels active requests and closes the connection. Concurrent and
// repeated calls wait for the first and return its result.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.closeErr = c.connection.Close()
	})
	return c.closeErr
}
