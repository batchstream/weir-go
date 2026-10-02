// Package testdns serves only task-owned loopback DNS. It never edits host DNS.
package testdns

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

type Answer struct {
	Addresses []netip.Addr
	Code      dnsmessage.RCode
	Delay     time.Duration
	Drop      bool
	TCP       bool
}

type Server struct {
	Address             string
	udp                 net.PacketConn
	tcp                 net.Listener
	mu                  sync.Mutex
	answers             map[string]Answer
	conns               map[net.Conn]bool
	ctx                 context.Context
	cancel              context.CancelFunc
	workers             sync.WaitGroup
	slots               chan struct{}
	Queries             atomic.Int32
	A, AAAA, Other, TCP atomic.Int32
	Active, Peak        atomic.Int32
}

func Start(t *testing.T) *Server {
	t.Helper()
	udp, tcp, err := listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		Address: udp.LocalAddr().String(),
		udp:     udp,
		tcp:     tcp,
		answers: make(map[string]Answer),
		conns:   make(map[net.Conn]bool),
		ctx:     ctx,
		cancel:  cancel,
		slots:   make(chan struct{}, 64),
	}
	s.workers.Go(s.serveUDP)
	s.workers.Go(s.serveTCP)
	t.Cleanup(func() {
		cancel()
		_ = udp.Close()
		_ = tcp.Close()
		s.mu.Lock()
		for conn := range s.conns {
			_ = conn.Close()
		}
		s.mu.Unlock()
		s.workers.Wait()
		if s.Active.Load() != 0 {
			t.Error("DNS fixture leaked queries")
		}
	})
	return s
}

const maxListenAttempts = 16

// listen retains both sockets before publishing a shared DNS address. UDP's
// ephemeral port may already be occupied in TCP's independent port namespace.
func listen(address string) (net.PacketConn, net.Listener, error) {
	addressInUse := syscall.EADDRINUSE
	if runtime.GOOS == "windows" {
		// Winsock WSAEADDRINUSE differs from Go's synthetic Windows EADDRINUSE.
		addressInUse = syscall.Errno(10048)
	}
	var err error
	for range maxListenAttempts {
		var udp net.PacketConn
		udp, err = net.ListenPacket("udp", address)
		if err != nil {
			return nil, nil, err
		}
		var tcp net.Listener
		tcp, err = net.Listen("tcp", udp.LocalAddr().String())
		if err == nil {
			return udp, tcp, nil
		}
		_ = udp.Close()
		if !errors.Is(err, addressInUse) {
			return nil, nil, err
		}
	}
	return nil, nil, fmt.Errorf("bind DNS UDP/TCP after %d attempts: %w", maxListenAttempts, err)
}

func (s *Server) Set(name string, answer Answer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	answer.Addresses = append([]netip.Addr(nil), answer.Addresses...)
	s.answers[name+"."] = answer
}

func (s *Server) Resolver() *net.Resolver {
	r := &net.Resolver{PreferGo: true, StrictErrors: true, Dial: s.Dial}
	return r
}

func (s *Server) Dial(ctx context.Context, network, _ string) (net.Conn, error) {
	d := net.Dialer{Timeout: time.Second}
	return d.DialContext(ctx, network, s.Address)
}

func (s *Server) serveUDP() {
	for {
		raw := make([]byte, 2048)
		n, addr, err := s.udp.ReadFrom(raw)
		if err != nil {
			return
		}
		select {
		case s.slots <- struct{}{}:
		default:
			continue
		}
		s.workers.Go(func() {
			defer func() { <-s.slots }()
			response := s.reply(raw[:n], false)
			if response != nil {
				_, _ = s.udp.WriteTo(response, addr)
			}
		})
	}
}

func (s *Server) serveTCP() {
	for {
		conn, err := s.tcp.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.ctx.Err() != nil {
			s.mu.Unlock()
			_ = conn.Close()
			return
		}
		s.conns[conn] = true
		s.mu.Unlock()
		select {
		case s.slots <- struct{}{}:
		default:
			_ = conn.Close()
			s.mu.Lock()
			delete(s.conns, conn)
			s.mu.Unlock()
			continue
		}
		s.workers.Go(func() {
			defer func() {
				_ = conn.Close()
				s.mu.Lock()
				delete(s.conns, conn)
				s.mu.Unlock()
				<-s.slots
			}()
			_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
			var length [2]byte
			if _, err := io.ReadFull(conn, length[:]); err != nil {
				return
			}
			raw := make([]byte, binary.BigEndian.Uint16(length[:]))
			if _, err := io.ReadFull(conn, raw); err != nil {
				return
			}
			response := s.reply(raw, true)
			if response == nil {
				return
			}
			wire := binary.BigEndian.AppendUint16(nil, uint16(len(response)))
			wire = append(wire, response...)
			_, _ = conn.Write(wire)
		})
	}
}

func (s *Server) reply(raw []byte, tcp bool) []byte {
	var p dnsmessage.Parser
	header, err := p.Start(raw)
	if err != nil {
		return nil
	}
	q, err := p.Question()
	if err != nil {
		return nil
	}
	s.Queries.Add(1)
	if tcp {
		s.TCP.Add(1)
	}
	switch q.Type {
	case dnsmessage.TypeA:
		s.A.Add(1)
	case dnsmessage.TypeAAAA:
		s.AAAA.Add(1)
	default:
		s.Other.Add(1)
	}
	active := s.Active.Add(1)
	defer s.Active.Add(-1)
	for peak := s.Peak.Load(); active > peak && !s.Peak.CompareAndSwap(peak, active); peak = s.Peak.Load() {
	}
	s.mu.Lock()
	answer, ok := s.answers[q.Name.String()]
	s.mu.Unlock()
	if !ok {
		answer.Code = dnsmessage.RCodeNameError
	}
	if answer.Drop {
		return nil
	}
	if answer.Delay > 0 {
		timer := time.NewTimer(answer.Delay)
		defer timer.Stop()
		select {
		case <-s.ctx.Done():
			return nil
		case <-timer.C:
		}
	}
	head := dnsmessage.Header{
		ID:                 header.ID,
		Response:           true,
		Authoritative:      true,
		RecursionAvailable: true,
		RCode:              answer.Code,
		Truncated:          answer.TCP && !tcp,
	}
	b := dnsmessage.NewBuilder(nil, head)
	b.EnableCompression()
	if b.StartQuestions() != nil || b.Question(q) != nil || b.StartAnswers() != nil {
		return nil
	}
	if !head.Truncated && answer.Code == dnsmessage.RCodeSuccess {
		for _, ip := range answer.Addresses {
			h := dnsmessage.ResourceHeader{Name: q.Name, Class: dnsmessage.ClassINET, TTL: 1}
			if ip.Is4() && q.Type == dnsmessage.TypeA {
				a := dnsmessage.AResource{A: ip.As4()}
				if b.AResource(h, a) != nil {
					return nil
				}
			} else if ip.Is6() && q.Type == dnsmessage.TypeAAAA {
				a := dnsmessage.AAAAResource{AAAA: ip.As16()}
				if b.AAAAResource(h, a) != nil {
					return nil
				}
			}
		}
	}
	response, err := b.Finish()
	if err != nil {
		return nil
	}
	return response
}
