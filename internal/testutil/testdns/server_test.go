package testdns

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

func TestListenTCPPortCollision(t *testing.T) {
	udp, blocker, err := listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	defer blocker.Close()
	address := udp.LocalAddr().String()
	// Release only our UDP socket. Our TCP listener keeps every candidate at
	// this explicit address in collision, without depending on ephemeral luck.
	if err := udp.Close(); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	failedUDP, failedTCP, err := listen(address)
	if failedUDP != nil {
		_ = failedUDP.Close()
	}
	if failedTCP != nil {
		_ = failedTCP.Close()
	}
	var op *net.OpError
	if failedUDP != nil || failedTCP != nil || !errors.As(err, &op) || op.Net != "tcp" {
		t.Fatalf("partial pair or wrong collision: udp=%v tcp=%v err=%v", failedUDP, failedTCP, err)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("after %d attempts", maxListenAttempts)) || time.Since(started) > time.Second {
		t.Fatal("collision retry was not bounded", err, time.Since(started))
	}
	// Rebinding UDP proves every failed attempt released its partial pair.
	rebound, err := net.ListenPacket("udp", address)
	if err != nil {
		t.Fatal("failed attempt leaked UDP", err)
	}
	defer rebound.Close()
	dialer := net.Dialer{Timeout: time.Second}
	client, err := dialer.DialContext(t.Context(), "tcp", address)
	if err != nil {
		t.Fatal("collision closed the existing TCP listener", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := blocker.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	accepted, err := blocker.Accept()
	if err != nil {
		t.Fatal(err)
	}
	_ = accepted.Close()
	_ = client.Close()
	_ = blocker.Close()
	_ = rebound.Close()
	recoveredUDP, recoveredTCP, err := listen(address)
	if err != nil {
		t.Fatal("released pair could not be acquired", err)
	}
	defer recoveredUDP.Close()
	defer recoveredTCP.Close()
	if recoveredUDP.LocalAddr().String() != address || recoveredTCP.Addr().String() != address {
		t.Fatal("pair did not retain the shared address")
	}
}

func TestListenUDPBindFailure(t *testing.T) {
	blocker, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	for _, address := range []string{blocker.LocalAddr().String(), "127.0.0.1:-1"} {
		t.Run(address, func(t *testing.T) {
			udp, tcp, err := listen(address)
			if udp != nil {
				_ = udp.Close()
			}
			if tcp != nil {
				_ = tcp.Close()
			}
			var op *net.OpError
			if udp != nil || tcp != nil || !errors.As(err, &op) || op.Net != "udp" || strings.Contains(err.Error(), "attempts") {
				t.Fatal("UDP bind failure was hidden or retried", err)
			}
		})
	}
}

func TestStartIndependentFixtures(t *testing.T) {
	var fixtures []*Server
	t.Run("concurrent", func(t *testing.T) {
		addresses := make(map[string]bool)
		for i := range 8 {
			s := Start(t)
			fixtures = append(fixtures, s)
			if addresses[s.Address] || s.udp.LocalAddr().String() != s.Address || s.tcp.Addr().String() != s.Address {
				t.Fatal("fixtures did not retain distinct complete pairs", s.Address)
			}
			addresses[s.Address] = true
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				t.Parallel()
				want := []netip.Addr{netip.MustParseAddr(fmt.Sprintf("127.0.0.%d", i+1)), netip.MustParseAddr("::1")}
				for _, tcp := range []bool{false, true} {
					answer := Answer{Addresses: want, TCP: tcp}
					s.Set("fixture.weir.test", answer)
					ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
					got, err := s.Resolver().LookupNetIP(ctx, "ip", "fixture.weir.test.")
					cancel()
					if err != nil || len(got) != 2 || !slices.Contains(got, want[0]) || !slices.Contains(got, want[1]) {
						t.Fatal("fixture answers crossed or lookup failed", got, err)
					}
				}
				if s.A.Load() != 3 || s.AAAA.Load() != 3 || s.TCP.Load() != 2 || s.Other.Load() != 0 {
					t.Fatal("real UDP A/AAAA and truncated TCP fallback not observed")
				}
			})
		}
	})
	for _, s := range fixtures {
		assertClosed(t, s)
	}
}

func TestStartCleanupJoinsBoundedWorkers(t *testing.T) {
	var s *Server
	var pending net.Conn
	var cleanupStarted time.Time
	t.Cleanup(func() {
		if pending != nil {
			_ = pending.Close()
		}
	})
	t.Run("active", func(t *testing.T) {
		s = Start(t)
		answer := Answer{Delay: time.Hour}
		s.Set("fixture.weir.test", answer)
		var err error
		pending, err = s.Dial(t.Context(), "tcp", "")
		if err != nil {
			t.Fatal(err)
		}
		await(t, func() bool { return len(s.slots) == 1 })
		udp, err := s.Dial(t.Context(), "udp", "")
		if err != nil {
			t.Fatal(err)
		}
		defer udp.Close()
		question := dnsmessage.Question{
			Name:  dnsmessage.MustNewName("fixture.weir.test."),
			Type:  dnsmessage.TypeA,
			Class: dnsmessage.ClassINET,
		}
		message := dnsmessage.Message{Questions: []dnsmessage.Question{question}}
		wire, err := message.Pack()
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i < cap(s.slots); i++ {
			if _, err := udp.Write(wire); err != nil {
				t.Fatal(err)
			}
			await(t, func() bool { return s.Active.Load() == int32(i) })
		}
		overflow, err := s.Dial(t.Context(), "tcp", "")
		if err != nil {
			t.Fatal(err)
		}
		defer overflow.Close()
		_ = overflow.SetReadDeadline(time.Now().Add(time.Second))
		var raw [1]byte
		if _, err := overflow.Read(raw[:]); err != io.EOF {
			t.Fatal("full fixture did not close excess connection", err)
		}
		if len(s.slots) != 64 || s.Peak.Load() != 63 {
			t.Fatal("fixture worker bound changed", len(s.slots), s.Peak.Load())
		}
		cleanupStarted = time.Now()
	})
	if s == nil || pending == nil {
		t.Fatal("fixture setup failed")
	}
	assertClosed(t, s)
	if time.Since(cleanupStarted) > time.Second {
		t.Fatal("cleanup did not promptly cancel delayed queries")
	}
	_ = pending.SetReadDeadline(time.Now().Add(time.Second))
	var raw [1]byte
	if _, err := pending.Read(raw[:]); err != io.EOF {
		t.Fatal("cleanup retained accepted TCP connection", err)
	}
}

func await(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("fixture state did not settle within 2s")
		}
		time.Sleep(time.Millisecond)
	}
}

func assertClosed(t *testing.T, s *Server) {
	t.Helper()
	s.mu.Lock()
	connections := len(s.conns)
	s.mu.Unlock()
	if s.ctx.Err() == nil || s.Active.Load() != 0 || len(s.slots) != 0 || connections != 0 {
		t.Fatal("cleanup retained fixture work", s.Active.Load(), len(s.slots), connections)
	}
	udp, tcp, err := listen(s.Address)
	if err != nil {
		t.Fatal("cleanup did not release both listener ports", err)
	}
	_ = udp.Close()
	_ = tcp.Close()
}
