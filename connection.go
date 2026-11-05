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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
)

// ErrClosed reports a call started after Close. Calls already running when
// Close is called end with the gRPC cancellation error instead.
var ErrClosed = errors.New("Weir client is closed")

// Client sends business requests to the Weir servers behind one application
// address. Every server behind the address must host the Stores the client
// uses. Reuse a Client across goroutines and close it after callers finish.
// It sends a record again only when a server refused it before it began, and
// never one that may have taken effect.
type Client struct {
	connection *grpc.ClientConn
	store      pb.StoreServiceClient
	lanes      *lanePool
	resends    *resendBudget
	closed     atomic.Bool
	closeOnce  sync.Once
	closeErr   error
}

// endpointConfig is the service config of a Client's connection.
const endpointConfig = `{"loadBalancingConfig":[{"weir_endpoints":{}}]}`

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
func New(address string, options ...ClientOption) (*Client, error) {
	if err := validateAddress(address); err != nil {
		return nil, err
	}
	config := clientOptions{}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	servers := &endpoints{}
	builder := &endpointResolverBuilder{endpoints: servers, lookup: net.DefaultResolver.LookupHost, interval: resolveInterval, spacing: resolveSpacing}
	connection, err := grpc.NewClient(endpointScheme+":///"+address, connectionOptions(config.dial, builder)...)
	if err != nil {
		return nil, err
	}
	store := pb.NewStoreServiceClient(connection)
	lanes := newLanePool(store, servers)
	servers.onLeave(lanes.leave)
	client := &Client{connection: connection, store: store, lanes: lanes, resends: newResendBudget()}
	return client, nil
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

// connectionOptions orders a connection's options so that custom ones can replace
// the credentials and keepalive but not what a Client relies on. gRPC takes the
// first resolver of a scheme and the last credentials, keepalive and service
// config. Each stream sets its message sizes itself, which outranks these.
func connectionOptions(custom []grpc.DialOption, servers resolver.Builder) []grpc.DialOption {
	pings := keepalive.ClientParameters{Time: keepaliveTime, Timeout: keepaliveTimeout, PermitWithoutStream: true}
	options := []grpc.DialOption{
		grpc.WithResolvers(servers),
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(),
		grpc.WithKeepaliveParams(pings),
		grpc.WithMaxHeaderListSize(^uint32(0)),
		grpc.WithDefaultCallOptions(grpc.MaxRetryRPCBufferSize(0), grpc.MaxCallSendMsgSize(protocol.MaxTransportMessageBytes), grpc.MaxCallRecvMsgSize(protocol.MaxTransportMessageBytes)),
	}
	options = append(options, custom...)
	options = append(options, grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithDefaultServiceConfig(endpointConfig))
	return options
}

func (c *Client) storeClient() (pb.StoreServiceClient, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	return c.store, nil
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
