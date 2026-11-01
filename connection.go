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
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// ErrClosed reports a call started after Close. Calls already running when
// Close is called end with the gRPC cancellation error instead.
var ErrClosed = errors.New("Weir client is closed")

// Client sends business requests to the Weir servers behind one application
// address. Every server behind the address must host the Stores the client
// uses. Reuse a Client across goroutines and close it after callers finish.
// It sends a record again only when a server refused it before it began, and
// never one that may have taken effect; streams are never resent.
type Client struct {
	connection *grpc.ClientConn
	store      pb.StoreServiceClient
	lanes      *lanePool
	resends    *resendBudget
	closed     atomic.Bool
	closeOnce  sync.Once
	closeErr   error
}

// Service configs of a Client's own connection and of Dial's.
const (
	endpointConfig   = `{"loadBalancingConfig":[{"weir_endpoints":{}}]}`
	roundRobinConfig = `{"loadBalancingConfig":[{"round_robin":{}}]}`
)

// A connection pings a server it has heard nothing from for keepaliveTime and
// closes when keepaliveTimeout passes without an answer, so calls leave a
// server they can no longer reach even while its address still resolves and
// its connection looked ready. Weir servers accept pings this often.
const (
	keepaliveTime    = 10 * time.Second
	keepaliveTimeout = 5 * time.Second
)

// New returns a Client for a Weir application address such as "weir:7447".
// The address is a host name or IP literal with an explicit port. The Client
// connects on first use and reconnects after failures. A DNS name that
// resolves to several servers, such as a headless Service, spreads streams
// across them. The Client looks the name up every five seconds: a server
// that joins takes its share of streams, and one that stops resolving gets
// no new records while its streams finish the ones they carry.
func New(address string) (*Client, error) {
	if err := validateAddress(address); err != nil {
		return nil, err
	}
	servers := &endpoints{}
	builder := &endpointResolverBuilder{endpoints: servers, lookup: net.DefaultResolver.LookupHost, interval: resolveInterval, spacing: resolveSpacing}
	options := append(connectionOptions(endpointConfig), grpc.WithResolvers(builder))
	connection, err := grpc.NewClient(endpointScheme+":///"+address, options...)
	if err != nil {
		return nil, err
	}
	store := pb.NewStoreServiceClient(connection)
	lanes := newLanePool(store, servers)
	servers.onLeave(lanes.leave)
	client := &Client{connection: connection, store: store, lanes: lanes, resends: newResendBudget()}
	return client, nil
}

// Dial creates a low-level connection to any gRPC target for the
// package-level operation functions. The caller owns target validation and
// the connection's lifetime.
func Dial(target string) (*grpc.ClientConn, error) {
	return grpc.NewClient(target, connectionOptions(roundRobinConfig)...)
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

func connectionOptions(serviceConfig string) []grpc.DialOption {
	pings := keepalive.ClientParameters{Time: keepaliveTime, Timeout: keepaliveTimeout, PermitWithoutStream: true}
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(),
		grpc.WithKeepaliveParams(pings),
		grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(),
		grpc.WithDefaultServiceConfig(serviceConfig),
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

// ReadOne and the other single-record Client methods share a bounded set of
// long-lived streams with concurrent calls of this Client. A canceled call
// stops waiting and cancels only its own record, which may still take effect
// if it is already executing. A record a server refused before it began is
// sent again, up to four sends within the caller's deadline.
func (c *Client) ReadOne(ctx context.Context, options ReadOneOptions) (*ReadResult, error) {
	client, err := c.storeClient()
	if err != nil {
		return nil, err
	}
	batch := ReadOptions{StoreName: options.StoreName, Requests: []*ReadRequest{options.Request}}
	if err := validateReads(ctx, batch); err != nil {
		return nil, err
	}
	operation := &pb.Command_Read{Read: options.Request}
	command := &pb.Command{Operation: operation}
	key := laneKey{store: options.StoreName, read: true}
	send := func() (*ReadResult, error) {
		event, shared, err := c.lanes.execute(ctx, key, command)
		if !shared {
			result, err := ReadOne(ctx, client, options)
			return result, callError(ctx, err)
		}
		if err != nil {
			return nil, err
		}
		return readResult(event.GetReadResult()), nil
	}
	return resendRefused(ctx, c, readRefused, send)
}

func (c *Client) Create(ctx context.Context, options WriteOptions) (*MutationResult, error) {
	request := writeRequest(options.Request, MutationCreate)
	return c.mutateOne(ctx, options.StoreName, request)
}

func (c *Client) Put(ctx context.Context, options WriteOptions) (*MutationResult, error) {
	request := writeRequest(options.Request, MutationPut)
	return c.mutateOne(ctx, options.StoreName, request)
}

func (c *Client) Replace(ctx context.Context, options WriteOptions) (*MutationResult, error) {
	request := writeRequest(options.Request, MutationReplace)
	return c.mutateOne(ctx, options.StoreName, request)
}

func (c *Client) Delete(ctx context.Context, options DeleteOptions) (*MutationResult, error) {
	request := deleteRequest(options.Request)
	return c.mutateOne(ctx, options.StoreName, request)
}

func (c *Client) AtomicTransform(ctx context.Context, options AtomicTransformOptions) (*MutationResult, error) {
	request := transformRequest(options.Request)
	return c.mutateOne(ctx, options.StoreName, request)
}

func (c *Client) mutateOne(ctx context.Context, store string, request *MutateRequest) (*MutationResult, error) {
	client, err := c.storeClient()
	if err != nil {
		return nil, err
	}
	batch := MutateOptions{StoreName: store, Requests: []*MutateRequest{request}}
	if err := validateMutations(ctx, batch); err != nil {
		return nil, err
	}
	mutation, err := wireMutation(request)
	if err != nil {
		return nil, err
	}
	operation := &pb.Command_Mutate{Mutate: mutation}
	command := &pb.Command{Operation: operation}
	key := laneKey{store: store}
	send := func() (*MutationResult, error) {
		event, shared, err := c.lanes.execute(ctx, key, command)
		if !shared {
			result, err := mutateOne(ctx, client, batch)
			return result, callError(ctx, err)
		}
		if err != nil {
			return nil, err
		}
		return event.GetMutationResult(), nil
	}
	return resendRefused(ctx, c, mutationRefused, send)
}

// callError reports a call that ended with its context as the context's error,
// whether it used a shared stream or a stream of its own. The server enforces
// the propagated deadline too, so its status can arrive before ctx expires.
func callError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if _, bounded := ctx.Deadline(); bounded && status.Code(err) == codes.DeadlineExceeded {
		return context.DeadlineExceeded
	}
	return err
}

// Native sends the request again only when a server refused it with
// NativeNotStarted before any response.
func (c *Client) Native(ctx context.Context, options NativeOptions) (*NativeResult, error) {
	client, err := c.storeClient()
	if err != nil {
		return nil, err
	}
	send := func() (*NativeResult, error) { return Native(ctx, client, options) }
	return resendRefused(ctx, c, nativeRefused, send)
}

// Close cancels active requests and closes the connection. Concurrent and
// repeated calls wait for the first and return its result.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.lanes.close()
		c.closeErr = c.connection.Close()
	})
	return c.closeErr
}
