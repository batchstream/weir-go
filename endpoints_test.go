package weir

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc/resolver"
)

type resolverTestConn struct {
	resolver.ClientConn
	states chan resolver.State
	errs   chan error
}

func (c *resolverTestConn) UpdateState(state resolver.State) error {
	c.states <- state
	return nil
}

func (c *resolverTestConn) ReportError(err error) { c.errs <- err }

type resolverTestAnswer struct {
	hosts []string
	err   error
}

func TestResolverReportsChangedServersAndKeepsThemWhenLookupsFail(t *testing.T) {
	answers := make(chan resolverTestAnswer)
	lookup := func(ctx context.Context, host string) ([]string, error) {
		if host != "weir.test" {
			t.Error("looked up", host)
		}
		select {
		case answer := <-answers:
			return answer.hosts, answer.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	servers := &endpoints{}
	builder := &endpointResolverBuilder{endpoints: servers, lookup: lookup, interval: time.Hour, spacing: time.Millisecond}
	conn := &resolverTestConn{states: make(chan resolver.State, 8), errs: make(chan error, 8)}
	target, err := url.Parse("weir:///weir.test:7447")
	if err != nil {
		t.Fatal(err)
	}
	resolverTarget := resolver.Target{URL: *target}
	buildOptions := resolver.BuildOptions{}
	built, err := builder.Build(resolverTarget, conn, buildOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer built.Close()
	now := resolver.ResolveNowOptions{}
	answer := func(hosts []string, err error) {
		reply := resolverTestAnswer{hosts: hosts, err: err}
		answers <- reply
		built.ResolveNow(now)
	}
	failed := errors.New("lookup failed")
	// A failure before any servers resolve is reported.
	answer(nil, failed)
	if reported := <-conn.errs; !errors.Is(reported, failed) {
		t.Fatal("first failed lookup was not reported", reported)
	}
	answer([]string{"10.0.0.2", "10.0.0.1", "10.0.0.1"}, nil)
	state := <-conn.states
	addresses := make([]string, 0, len(state.Addresses))
	for _, address := range state.Addresses {
		addresses = append(addresses, address.Addr)
	}
	if !slices.Equal(addresses, []string{"10.0.0.1:7447", "10.0.0.2:7447"}) || state.Attributes.Value(endpointsAttribute) != servers {
		t.Fatal("resolved servers were not sorted, deduplicated and tagged", addresses, state.Attributes)
	}
	// Neither the same servers nor a later failure changes the state.
	answer([]string{"10.0.0.1", "10.0.0.2"}, nil)
	answer(nil, failed)
	answer(nil, nil)
	// IP literals take the canonical form a stream's peer address has.
	answer([]string{"::ffff:10.0.0.3", "2001:DB8:0::1"}, nil)
	state = <-conn.states
	addresses = addresses[:0]
	for _, address := range state.Addresses {
		addresses = append(addresses, address.Addr)
	}
	if !slices.Equal(addresses, []string{"10.0.0.3:7447", "[2001:db8::1]:7447"}) {
		t.Fatal("changed servers were not reported in canonical form", addresses)
	}
	if len(conn.errs) != 0 {
		t.Fatal("a failed lookup was reported after servers resolved", <-conn.errs)
	}
}
