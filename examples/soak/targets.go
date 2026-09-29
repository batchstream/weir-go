package main

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
)

func targetAddresses(cfg options) ([]string, error) {
	if (cfg.Address == "") == (cfg.Targets == "") {
		return nil, errors.New("supply exactly one of address or targets")
	}
	if cfg.Targets == "" {
		return []string{cfg.Address}, nil
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
