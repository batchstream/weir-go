package weir

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
)

// A Client follows the servers its address resolves to, so replicas that are
// added or removed, such as a headless Service's Pods, gain or lose traffic
// within seconds rather than at the server's connection age.
const (
	// resolveInterval is how often the address is looked up again.
	resolveInterval = 5 * time.Second
	// resolveSpacing bounds the lookups that connection failures request.
	resolveSpacing = time.Second
	// endpointScheme is the resolver scheme of a Client's own connection.
	endpointScheme = "weir"
	// endpointBalancer spreads streams across ready servers and lets a shared
	// stream ask for a particular one.
	endpointBalancer = "weir_endpoints"
)

func init() {
	builder := endpointBalancerBuilder{}
	balancer.Register(builder)
}

// endpoints is a Client's view of its servers: the ones the address resolves
// to and the ones with a ready connection.
type endpoints struct {
	mu       sync.Mutex
	resolved []string
	ready    []string
	// leave is told which servers stopped resolving.
	leave func(gone []string)
}

type endpointKey int

const (
	// endpointsAttribute carries a Client's endpoints from its resolver to its
	// balancer.
	endpointsAttribute endpointKey = iota
	// endpointPreference carries the server a shared stream asks for.
	endpointPreference
)

func (e *endpoints) onLeave(leave func(gone []string)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.leave = leave
}

// resolve records the resolved servers and reports the ones that left.
func (e *endpoints) resolve(addresses []string) {
	e.mu.Lock()
	var gone []string
	for _, address := range e.resolved {
		if !slices.Contains(addresses, address) {
			gone = append(gone, address)
		}
	}
	e.resolved = addresses
	leave := e.leave
	e.mu.Unlock()
	if len(gone) != 0 && leave != nil {
		leave(gone)
	}
}

func (e *endpoints) setReady(addresses []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ready = addresses
}

// usable returns, sorted, the servers that resolve and have a ready connection.
func (e *endpoints) usable() []string {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	usable := make([]string, 0, len(e.ready))
	for _, address := range e.ready {
		if slices.Contains(e.resolved, address) {
			usable = append(usable, address)
		}
	}
	return usable
}

// resolves reports whether the address still resolves to the server.
func (e *endpoints) resolves(address string) bool {
	if e == nil || address == "" {
		return true
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Contains(e.resolved, address)
}

type endpointBalancerBuilder struct{}

func (endpointBalancerBuilder) Name() string { return endpointBalancer }

func (endpointBalancerBuilder) Build(cc balancer.ClientConn, options balancer.BuildOptions) balancer.Balancer {
	conn := &endpointConn{ClientConn: cc}
	config := base.Config{}
	pickers := endpointPickerBuilder{}
	inner := base.NewBalancerBuilder(endpointBalancer, pickers, config).Build(conn, options)
	built := &endpointBalancerState{Balancer: inner, conn: conn}
	return built
}

// endpointBalancerState hands the resolved and ready servers to the Client's
// endpoints around the base balancer.
type endpointBalancerState struct {
	balancer.Balancer
	conn *endpointConn
}

func (b *endpointBalancerState) UpdateClientConnState(state balancer.ClientConnState) error {
	if found, ok := state.ResolverState.Attributes.Value(endpointsAttribute).(*endpoints); ok {
		b.conn.endpoints.Store(found)
	}
	err := b.Balancer.UpdateClientConnState(state)
	// The base balancer has dropped removed servers from its picker by now, so
	// streams that leave them cannot be reopened there.
	if found := b.conn.endpoints.Load(); found != nil {
		addresses := make([]string, 0, len(state.ResolverState.Addresses))
		for _, address := range state.ResolverState.Addresses {
			addresses = append(addresses, address.Addr)
		}
		slices.Sort(addresses)
		found.resolve(slices.Compact(addresses))
	}
	return err
}

// endpointConn reports each new picker's ready servers once the picker is in
// use, so a stream never asks for a server the picker lacks.
type endpointConn struct {
	balancer.ClientConn
	endpoints atomic.Pointer[endpoints]
}

func (c *endpointConn) UpdateState(state balancer.State) {
	c.ClientConn.UpdateState(state)
	if found := c.endpoints.Load(); found != nil {
		var ready []string
		if picker, ok := state.Picker.(*endpointPicker); ok {
			ready = picker.addresses
		}
		found.setReady(ready)
	}
}

type endpointPickerBuilder struct{}

func (endpointPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	picker := &endpointPicker{byAddress: make(map[string]balancer.SubConn, len(info.ReadySCs))}
	for subConn, connection := range info.ReadySCs {
		picker.byAddress[connection.Address.Addr] = subConn
		picker.addresses = append(picker.addresses, connection.Address.Addr)
	}
	slices.Sort(picker.addresses)
	// Clients start at different servers, as round_robin does.
	picker.next.Store(rand.Uint32())
	return picker
}

// endpointPicker takes ready servers in turn unless a stream asks for one.
type endpointPicker struct {
	addresses []string
	byAddress map[string]balancer.SubConn
	next      atomic.Uint32
}

func (p *endpointPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if address, ok := info.Ctx.Value(endpointPreference).(string); ok {
		if subConn := p.byAddress[address]; subConn != nil {
			preferred := balancer.PickResult{SubConn: subConn}
			return preferred, nil
		}
	}
	index := p.next.Add(1) % uint32(len(p.addresses))
	result := balancer.PickResult{SubConn: p.byAddress[p.addresses[index]]}
	return result, nil
}

// endpointResolverBuilder looks a Client's address up every interval and when
// a connection fails, at most once per spacing.
type endpointResolverBuilder struct {
	endpoints         *endpoints
	lookup            func(ctx context.Context, host string) ([]string, error)
	interval, spacing time.Duration
}

func (b *endpointResolverBuilder) Scheme() string { return endpointScheme }

func (b *endpointResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	host, port, err := net.SplitHostPort(target.Endpoint())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	built := &endpointResolver{builder: b, cc: cc, host: host, port: port, ctx: ctx, cancel: cancel, now: make(chan struct{}, 1), done: make(chan struct{})}
	go built.run()
	return built, nil
}

type endpointResolver struct {
	builder    *endpointResolverBuilder
	cc         resolver.ClientConn
	host, port string
	ctx        context.Context
	cancel     context.CancelFunc
	now        chan struct{}
	done       chan struct{}
}

var errNoServers = errors.New("Weir address resolves to no servers")

func (r *endpointResolver) run() {
	defer close(r.done)
	var current []string
	var last time.Time
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.now:
			if wait := time.Until(last.Add(r.builder.spacing)); wait > 0 {
				timer.Reset(wait)
				continue
			}
		case <-timer.C:
		}
		last = time.Now()
		addresses, err := r.lookup()
		switch {
		case err == nil && !slices.Equal(addresses, current):
			state := resolver.State{Addresses: make([]resolver.Address, len(addresses)), Attributes: attributes.New(endpointsAttribute, r.builder.endpoints)}
			for i, address := range addresses {
				state.Addresses[i] = resolver.Address{Addr: address}
			}
			if r.cc.UpdateState(state) == nil {
				current = addresses
			}
		case err != nil && current == nil:
			r.cc.ReportError(err)
		}
		// A failed lookup keeps the last servers rather than dropping them.
		timer.Reset(r.builder.interval)
	}
}

// lookup returns the sorted host:port of every server the address resolves to.
func (r *endpointResolver) lookup() ([]string, error) {
	ctx, cancel := context.WithTimeout(r.ctx, r.builder.interval)
	defer cancel()
	hosts, err := r.builder.lookup(ctx, r.host)
	if err != nil {
		return nil, err
	}
	if len(hosts) == 0 {
		return nil, errNoServers
	}
	addresses := make([]string, len(hosts))
	for i, host := range hosts {
		// A stream's peer address is canonical, so IP literals must be too.
		if ip, err := netip.ParseAddr(host); err == nil {
			host = ip.Unmap().String()
		}
		addresses[i] = net.JoinHostPort(host, r.port)
	}
	slices.Sort(addresses)
	return slices.Compact(addresses), nil
}

func (r *endpointResolver) ResolveNow(resolver.ResolveNowOptions) {
	select {
	case r.now <- struct{}{}:
	default:
	}
}

func (r *endpointResolver) Close() {
	r.cancel()
	<-r.done
}
