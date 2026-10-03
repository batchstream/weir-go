package weir

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/batchstream/weir-protocol/api/netlimit"
	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
)

const maxClientStores = 16
const maxClientAddresses = 64

var ErrClosed = errors.New("Weir client is closed")

// OpenOptions identifies an initialization endpoint and the Stores required by
// this client. Addresses are IP or DNS host:port pairs on Weir's plaintext
// application listener. RefreshInterval defaults to 30 seconds; ResolveTimeout
// bounds each discovery/DNS/connection operation and defaults to two seconds.
// Resolver is an optional standard DNS transport, useful for isolated networks.
type OpenOptions struct {
	Seed            string
	Stores          []string
	RefreshInterval time.Duration
	ResolveTimeout  time.Duration
	Resolver        *net.Resolver
}

// Client initializes Store destinations through ResolveStore and sends each finite
// business RPC directly to the discovered Store's replicas. It has no URI
// affinity and never replays a business request. Close joins its refresh worker.
type Client struct {
	options OpenOptions
	stores  map[string]*storeChannel
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	mu      sync.RWMutex
	closed  bool
	once    sync.Once
}

type storeChannel struct {
	channel   *addressChannel
	mu        sync.RWMutex
	endpoints []string
	expires   time.Time
	ttl       time.Duration
	err       error
	attempt   time.Time
}

type addressChannel struct {
	connection *grpc.ClientConn
	resolver   *manual.Resolver
	addresses  []string
}

type refreshRound struct {
	ctx       context.Context
	preferred *addressChannel
	mu        sync.Mutex
	seed      *addressChannel
	seedTried bool
	err       error
	control   chan struct{}
}

// Timer callbacks can publish cancellation after the deadline has passed.
// gRPC rejects elapsed deadlines synchronously, so admission must do the same.
func (r *refreshRound) contextError(now time.Time) error {
	if err := r.ctx.Err(); err != nil {
		return err
	}
	if deadline, bounded := r.ctx.Deadline(); bounded && !now.Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

type refreshJob struct {
	store   string
	attempt time.Time
}

// Open completes initialization before returning: every requested Store has a
// valid ResolveStore response and at least one ready direct business connection.
// The opening context does not own the returned client's lifetime.
func Open(ctx context.Context, options OpenOptions) (*Client, error) {
	seed, err := protocol.CanonicalEndpoint(options.Seed)
	if err != nil {
		return nil, fmt.Errorf("initialization address: %w", err)
	}
	if len(options.Stores) < 1 || len(options.Stores) > maxClientStores {
		return nil, errors.New("initialization requires 1-16 Stores")
	}
	options.Seed = seed
	options.Stores = slices.Clone(options.Stores)
	slices.Sort(options.Stores)
	for i, store := range options.Stores {
		if !protocol.ValidStoreName(store) || i > 0 && options.Stores[i-1] == store {
			return nil, errors.New("invalid or duplicate initialization Store")
		}
	}
	if options.RefreshInterval == 0 {
		options.RefreshInterval = 30 * time.Second
	}
	if options.ResolveTimeout == 0 {
		options.ResolveTimeout = 2 * time.Second
	}
	if options.RefreshInterval < 50*time.Millisecond || options.RefreshInterval > time.Hour || options.ResolveTimeout < time.Millisecond || options.ResolveTimeout > time.Minute {
		return nil, errors.New("invalid discovery interval or timeout")
	}
	if _, bounded := ctx.Deadline(); !bounded {
		var openCancel context.CancelFunc
		ctx, openCancel = context.WithTimeout(ctx, options.ResolveTimeout)
		defer openCancel()
	}
	lifetime, cancel := context.WithCancel(context.Background())
	client := &Client{options: options, stores: make(map[string]*storeChannel), ctx: lifetime, cancel: cancel, done: make(chan struct{})}
	seedChannel, err := client.newChannel(ctx, []string{seed})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("initialization connection: %w", err)
	}
	defer seedChannel.connection.Close()
	// Finish discovery before opening business channels so a server can honor
	// its physical connection limit without needing an extra bootstrap slot.
	for _, store := range options.Stores {
		response, resolveErr := client.initialResolve(ctx, seedChannel, store)
		if resolveErr != nil {
			client.closeChannels()
			cancel()
			return nil, resolveErr
		}
		resolvedAt := time.Now()
		ttl := time.Duration(response.CacheTtlMs) * time.Millisecond
		entry := &storeChannel{endpoints: slices.Clone(response.Endpoints), expires: resolvedAt.Add(ttl), ttl: ttl, attempt: resolvedAt}
		client.stores[store] = entry
	}
	_ = seedChannel.connection.Close()
	for _, store := range options.Stores {
		entry := client.stores[store]
		channel, channelErr := client.newChannel(ctx, entry.endpoints)
		if channelErr != nil {
			client.closeChannels()
			cancel()
			return nil, fmt.Errorf("initialize Store %s: %w", store, channelErr)
		}
		ready, readyCancel := context.WithTimeout(ctx, options.ResolveTimeout)
		readyErr := waitReady(ready, channel.connection)
		readyCancel()
		if readyErr != nil {
			_ = channel.connection.Close()
			client.closeChannels()
			cancel()
			return nil, fmt.Errorf("initialize Store %s: %w", store, readyErr)
		}
		entry.channel = channel
	}
	for _, store := range options.Stores {
		if _, err := client.storeClient(store); err != nil {
			client.closeChannels()
			cancel()
			return nil, err
		}
	}
	go client.refresh()
	return client, nil
}

func (c *Client) initialResolve(ctx context.Context, seed *addressChannel, store string) (*pb.ResolveStoreResponse, error) {
	for {
		response, err := c.resolve(ctx, seed, store)
		if err == nil || status.Code(err) != codes.Unavailable || ctx.Err() != nil {
			return response, err
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func waitReady(ctx context.Context, connection *grpc.ClientConn) error {
	connection.Connect()
	for {
		state := connection.GetState()
		if state == connectivity.Ready {
			return ctx.Err()
		}
		if state == connectivity.Shutdown {
			return ErrClosed
		}
		if !connection.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
}

func (c *Client) resolve(ctx context.Context, seed *addressChannel, store string) (*pb.ResolveStoreResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.options.ResolveTimeout)
	defer cancel()
	request := &pb.ResolveStoreRequest{StoreName: store}
	client := pb.NewStoreServiceClient(seed.connection)
	started := time.Now()
	response, err := client.ResolveStore(ctx, request, grpc.WaitForReady(true))
	if err != nil {
		return nil, fmt.Errorf("resolve Store %s: %w", store, err)
	}
	if response == nil || response.StoreName != store || response.CacheTtlMs == 0 || response.CacheTtlMs > uint64(protocol.MaxDiscoveryCacheTTL/time.Millisecond) || len(response.ProtoReflect().GetUnknown()) != 0 {
		return nil, fmt.Errorf("resolve Store %s: invalid directory response", store)
	}
	endpoints, err := protocol.CanonicalEndpoints(response.Endpoints)
	if err != nil {
		return nil, fmt.Errorf("resolve Store %s: %w", store, err)
	}
	response.Endpoints = endpoints
	// Start the lease before the RPC so time spent in transport, DNS and
	// connection setup can never extend the owner's advertised cache lifetime.
	elapsed := uint64((time.Since(started) + time.Millisecond - 1) / time.Millisecond)
	if elapsed >= response.CacheTtlMs {
		return nil, status.Error(codes.Unavailable, "directory response expired during ResolveStore")
	}
	response.CacheTtlMs -= elapsed
	return response, nil
}

func (c *Client) newChannel(ctx context.Context, endpoints []string) (*addressChannel, error) {
	ctx, cancel := context.WithTimeout(ctx, c.options.ResolveTimeout)
	defer cancel()
	addresses, err := physicalAddresses(ctx, c.options.Resolver, endpoints)
	if err != nil {
		return nil, err
	}
	builder := manual.NewBuilderWithScheme("weir-discovery")
	state := addressState(addresses)
	builder.InitialState(state)
	options := connectionOptions()
	options = append(options, grpc.WithResolvers(builder), grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`), grpc.WithIdleTimeout(0))
	connection, err := grpc.NewClient("weir-discovery:///replicas", options...)
	if err != nil {
		return nil, err
	}
	channel := &addressChannel{connection: connection, resolver: builder, addresses: addresses}
	connection.Connect()
	return channel, nil
}

func physicalAddresses(ctx context.Context, dns *net.Resolver, endpoints []string) ([]string, error) {
	addresses := make(map[string]struct{})
	for _, target := range endpoints {
		host, port, err := net.SplitHostPort(target)
		if err != nil {
			return nil, err
		}
		ips := []string{host}
		if _, parseErr := netip.ParseAddr(host); parseErr != nil {
			ips, err = netlimit.LookupHostLimit(ctx, dns, host, maxClientAddresses)
			if err != nil {
				return nil, fmt.Errorf("resolve business address %s: %w", host, err)
			}
		}
		for _, ip := range ips {
			address := net.JoinHostPort(ip, port)
			addresses[address] = struct{}{}
			if len(addresses) > maxClientAddresses {
				return nil, errors.New("Store exceeds 64 physical addresses")
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make([]string, 0, len(addresses))
	for address := range addresses {
		result = append(result, address)
	}
	slices.Sort(result)
	return result, nil
}

func addressState(addresses []string) resolver.State {
	state := resolver.State{Addresses: make([]resolver.Address, 0, len(addresses))}
	for _, address := range addresses {
		endpoint := resolver.Address{Addr: address}
		state.Addresses = append(state.Addresses, endpoint)
	}
	return state
}

func (a *addressChannel) update(addresses []string) {
	if slices.Equal(a.addresses, addresses) {
		return
	}
	a.addresses = slices.Clone(addresses)
	state := addressState(addresses)
	a.resolver.UpdateState(state)
}

func (c *Client) refresh() {
	defer close(c.done)
	for {
		interval := c.options.RefreshInterval
		for _, entry := range c.stores {
			entry.mu.RLock()
			interval = min(interval, entry.ttl/3)
			entry.mu.RUnlock()
		}
		interval = max(interval, 50*time.Millisecond)
		timer := time.NewTimer(interval)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		c.refreshStores()
	}
}

func (c *Client) refreshStores() {
	ctx, cancel := context.WithTimeout(c.ctx, c.options.ResolveTimeout)
	defer cancel()
	var preferred *addressChannel
	for _, store := range c.options.Stores {
		channel := c.stores[store].channel
		if channel.connection.GetState() == connectivity.Ready {
			preferred = channel
			break
		}
	}
	round := &refreshRound{ctx: ctx, preferred: preferred, control: make(chan struct{}, 2)}
	defer func() {
		if round.seed != nil {
			_ = round.seed.connection.Close()
		}
	}()
	ordered := make([]refreshJob, 0, len(c.options.Stores))
	for _, store := range c.options.Stores {
		entry := c.stores[store]
		entry.mu.RLock()
		job := refreshJob{store: store, attempt: entry.attempt}
		entry.mu.RUnlock()
		ordered = append(ordered, job)
	}
	// A bounded round can end before every Store is reached. Failed attempts
	// also advance, so unavailable Stores cannot permanently starve the tail.
	slices.SortFunc(ordered, func(a, b refreshJob) int {
		if order := a.attempt.Compare(b.attempt); order != 0 {
			return order
		}
		return strings.Compare(a.store, b.store)
	})
	jobs := make(chan string, len(ordered))
	for _, job := range ordered {
		jobs <- job.store
	}
	close(jobs)
	var workers sync.WaitGroup
	for range min(4, len(c.options.Stores)) {
		workers.Go(func() {
			for store := range jobs {
				if round.contextError(time.Now()) != nil {
					return
				}
				c.refreshStore(round, store)
			}
		})
	}
	workers.Wait()
}

func (c *Client) refreshResolve(round *refreshRound, store string) (*pb.ResolveStoreResponse, error) {
	if err := round.contextError(time.Now()); err != nil {
		return nil, err
	}
	select {
	case <-round.ctx.Done():
		return nil, round.ctx.Err()
	case round.control <- struct{}{}:
	}
	defer func() { <-round.control }()
	if err := round.contextError(time.Now()); err != nil {
		return nil, err
	}
	entry := c.stores[store]
	entry.mu.Lock()
	started := time.Now()
	if err := round.contextError(started); err != nil {
		entry.mu.Unlock()
		return nil, err
	}
	entry.attempt = started
	entry.mu.Unlock()
	if round.preferred != nil {
		resolve, resolveCancel := context.WithTimeout(round.ctx, max(c.options.ResolveTimeout/4, time.Millisecond))
		response, err := c.resolve(resolve, round.preferred, store)
		resolveCancel()
		if err == nil || status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
			return response, err
		}
		if err := round.contextError(time.Now()); err != nil {
			return nil, err
		}
	}
	// Any application connection can ResolveStore. Only borrow it: closing it here
	// would interrupt active finite business streams. A failed directory lookup
	// falls back to one temporary initialization channel shared by this round.
	round.mu.Lock()
	if err := round.contextError(time.Now()); err != nil {
		round.mu.Unlock()
		return nil, err
	}
	if !round.seedTried {
		round.seedTried = true
		seedContext, seedCancel := context.WithTimeout(round.ctx, max(c.options.ResolveTimeout/4, time.Millisecond))
		round.seed, round.err = c.newChannel(seedContext, []string{c.options.Seed})
		seedCancel()
	}
	seed, err := round.seed, round.err
	round.mu.Unlock()
	if err != nil {
		return nil, err
	}
	resolve, resolveCancel := context.WithTimeout(round.ctx, max(c.options.ResolveTimeout/4, time.Millisecond))
	defer resolveCancel()
	return c.resolve(resolve, seed, store)
}

func (c *Client) refreshStore(round *refreshRound, store string) {
	if round.contextError(time.Now()) != nil {
		return
	}
	entry := c.stores[store]
	entry.mu.RLock()
	endpoints := slices.Clone(entry.endpoints)
	valid := time.Now().Before(entry.expires)
	entry.mu.RUnlock()
	var addresses []string
	var lookupErr error
	// Refresh cached business DNS before a seed RPC can consume the round's
	// deadline. Discovery outages must not hide scaled replicas of a valid Store.
	if valid {
		started := time.Now()
		lookup, lookupCancel := context.WithTimeout(round.ctx, max(c.options.ResolveTimeout/4, time.Millisecond))
		addresses, lookupErr = physicalAddresses(lookup, c.options.Resolver, endpoints)
		lookupCancel()
		if lookupErr == nil {
			entry.mu.Lock()
			entry.channel.update(addresses)
			entry.mu.Unlock()
		} else {
			entry.mu.Lock()
			if round.contextError(time.Now()) == nil {
				entry.attempt = started
			}
			entry.mu.Unlock()
		}
	}
	response, err := c.refreshResolve(round, store)
	if err != nil {
		entry.mu.Lock()
		entry.err = err
		if status.Code(err) == codes.FailedPrecondition || status.Code(err) == codes.NotFound || status.Code(err) == codes.InvalidArgument || !time.Now().Before(entry.expires) {
			entry.expires = time.Time{}
			entry.channel.update(nil)
		}
		entry.mu.Unlock()
		return
	}
	resolvedAt := time.Now()
	if !valid || lookupErr != nil || !slices.Equal(endpoints, response.Endpoints) {
		addresses, lookupErr = physicalAddresses(round.ctx, c.options.Resolver, response.Endpoints)
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if lookupErr != nil {
		entry.err = lookupErr
		entry.expires = time.Time{}
		entry.channel.update(nil)
		return
	}
	entry.channel.update(addresses)
	entry.endpoints = slices.Clone(response.Endpoints)
	entry.ttl = time.Duration(response.CacheTtlMs) * time.Millisecond
	entry.expires = resolvedAt.Add(entry.ttl)
	entry.err = nil
}

func (c *Client) storeClient(store string) (pb.StoreServiceClient, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, ErrClosed
	}
	entry := c.stores[store]
	if entry == nil {
		return nil, fmt.Errorf("Store %s was not initialized", store)
	}
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	if !time.Now().Before(entry.expires) {
		return nil, fmt.Errorf("Store %s discovery is unavailable or expired: %v", store, entry.err)
	}
	return pb.NewStoreServiceClient(entry.channel.connection), nil
}

func (c *Client) Execute(ctx context.Context, options ExecuteOptions) error {
	client, err := c.storeClient(options.StoreName)
	if err != nil {
		return err
	}
	return Execute(ctx, client, options)
}

func (c *Client) Read(ctx context.Context, options ReadOptions) (*ReadResult, error) {
	client, err := c.storeClient(options.StoreName)
	if err != nil {
		return nil, err
	}
	return Read(ctx, client, options)
}

func (c *Client) Create(ctx context.Context, options WriteOptions) (*MutationResult, error) {
	client, err := c.storeClient(options.StoreName)
	if err != nil {
		return nil, err
	}
	return Create(ctx, client, options)
}

func (c *Client) Put(ctx context.Context, options WriteOptions) (*MutationResult, error) {
	client, err := c.storeClient(options.StoreName)
	if err != nil {
		return nil, err
	}
	return Put(ctx, client, options)
}

func (c *Client) Replace(ctx context.Context, options WriteOptions) (*MutationResult, error) {
	client, err := c.storeClient(options.StoreName)
	if err != nil {
		return nil, err
	}
	return Replace(ctx, client, options)
}

func (c *Client) Delete(ctx context.Context, options DeleteOptions) (*MutationResult, error) {
	client, err := c.storeClient(options.StoreName)
	if err != nil {
		return nil, err
	}
	return Delete(ctx, client, options)
}

func (c *Client) AtomicTransform(ctx context.Context, options AtomicTransformOptions) (*MutationResult, error) {
	client, err := c.storeClient(options.StoreName)
	if err != nil {
		return nil, err
	}
	return AtomicTransform(ctx, client, options)
}

func (c *Client) Scan(ctx context.Context, options ScanOptions) (*ScanEnd, error) {
	client, err := c.storeClient(options.StoreName)
	if err != nil {
		return nil, err
	}
	return Scan(ctx, client, options)
}

func (c *Client) Native(ctx context.Context, options NativeOptions) (*NativeEnd, error) {
	client, err := c.storeClient(options.StoreName)
	if err != nil {
		return nil, err
	}
	return Native(ctx, client, options)
}

func (c *Client) closeChannels() {
	for _, entry := range c.stores {
		if entry.channel != nil {
			_ = entry.channel.connection.Close()
		}
	}
}

// Close cancels active requests, stops discovery and waits for owned DNS I/O.
func (c *Client) Close() error {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.cancel()
		c.closeChannels()
		<-c.done
	})
	return nil
}
