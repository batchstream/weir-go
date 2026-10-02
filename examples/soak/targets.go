package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/batchstream/weir/api/netlimit"
	"github.com/batchstream/weir/api/protocol"
	pb "github.com/batchstream/weir/api/weir/v1"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"
)

func targetAddresses(cfg options) ([]string, error) {
	if (cfg.Address == "") == (cfg.Targets == "") {
		return nil, errors.New("supply exactly one of address or targets")
	}
	if cfg.Targets == "" {
		address, err := protocol.CanonicalEndpoint(cfg.Address)
		if err != nil {
			return nil, err
		}
		return []string{address}, nil
	}
	targets := strings.Split(cfg.Targets, ",")
	seen := make(map[string]bool)
	for i, target := range targets {
		host, portText, err := net.SplitHostPort(target)
		if err != nil {
			return nil, fmt.Errorf("target %d must be a literal Pod IP:port", i)
		}
		ip, err := netip.ParseAddr(host)
		port, portErr := strconv.Atoi(portText)
		ip = ip.Unmap()
		if err != nil || ip.Zone() != "" || ip.IsUnspecified() || ip.IsMulticast() || portErr != nil || port < 1 || port > 65535 || strconv.Itoa(port) != portText {
			return nil, fmt.Errorf("target %d must be a routable literal IP and canonical port", i)
		}
		target = net.JoinHostPort(ip.Unmap().String(), portText)
		if seen[target] {
			return nil, errors.New("targets must not contain duplicates")
		}
		seen[target], targets[i] = true, target
	}
	// Worker parity selects the backend; modulo selects its fixed target.
	if len(targets)%2 == 0 || cfg.Workers < 2*len(targets) || cfg.Workers%(2*len(targets)) != 0 {
		return nil, errors.New("targets require an odd count and workers divisible by twice that count for equal backend coverage")
	}
	return targets, nil
}

// verifyOwner checks discovery but keeps business traffic on the supplied
// connection. Fixed targets must be actual owners, not arbitrary seed nodes.
func verifyOwner(ctx context.Context, client pb.StoreServiceClient, target, store string) error {
	request := &pb.ResolveStoreRequest{StoreName: store}
	response, err := client.ResolveStore(ctx, request)
	if err != nil {
		return err
	}
	if response == nil || response.StoreName != store || response.CacheTtlMs == 0 || response.CacheTtlMs > uint64(protocol.MaxDiscoveryCacheTTL/time.Millisecond) || len(response.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("owner returned invalid Store discovery")
	}
	endpoints, err := protocol.CanonicalEndpoints(response.Endpoints)
	if err != nil || !slices.Equal(endpoints, response.Endpoints) {
		return errors.New("owner returned noncanonical Store endpoints")
	}
	targets, err := endpointAddresses(ctx, target)
	if err != nil {
		return err
	}
	allowed := make(map[string]bool)
	for _, endpoint := range endpoints {
		addresses, err := endpointAddresses(ctx, endpoint)
		if err != nil {
			return err
		}
		for _, address := range addresses {
			allowed[address] = true
		}
	}
	for _, address := range targets {
		if !allowed[address] {
			return errors.New("supplied business target does not own requested Store")
		}
	}
	return nil
}

func endpointAddresses(ctx context.Context, endpoint string) ([]string, error) {
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		return nil, err
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		return []string{net.JoinHostPort(ip.Unmap().String(), port)}, nil
	}
	addresses, err := netlimit.LookupHostLimit(ctx, nil, host, protocol.MaxDiscoveryEndpoints)
	if err != nil {
		return nil, err
	}
	for i, address := range addresses {
		addresses[i] = net.JoinHostPort(address, port)
	}
	return addresses, nil
}
