package weir

import (
	"context"
	"errors"
	"io"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Single-record calls of one Client share a bounded set of long-lived Execute
// streams per Store and operation kind. A stream returns results in completion
// order and fails only the record the server rejects, so one slow or invalid
// call does not hold back the others.
const (
	// laneRecords stays within the records a server reads ahead on one
	// stream, so a cancel request is never stuck behind a waiting record.
	laneRecords = protocol.RecordWindow
	// laneOpenAt opens another stream once every stream holds this many records.
	laneOpenAt = laneRecords / 2
	// maxLanes bounds the streams per Store and kind; calls beyond their
	// capacity use a stream of their own.
	maxLanes = 32
	// laneRecordBytes keeps large records from delaying a shared stream's sends.
	laneRecordBytes = 1 << 20
	// laneLifetime retires a stream well within the server's connection age
	// so connection rebalancing still reaches long-running clients.
	laneLifetime = time.Minute
	// laneIdle half-closes an unused stream before the server's stall timeout.
	laneIdle = 10 * time.Second
)

var errLaneClosed = errors.New("shared Execute stream closed")

type laneKey struct {
	store string
	read  bool
}

type lanePool struct {
	client pb.StoreServiceClient
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	groups map[laneKey]*laneGroup
}

type laneGroup struct {
	lanes []*lane
	// opening is closed when the stream being opened is ready or failed.
	opening chan struct{}
}

func newLanePool(client pb.StoreServiceClient) *lanePool {
	ctx, cancel := context.WithCancel(context.Background())
	pool := &lanePool{client: client, ctx: ctx, cancel: cancel, groups: make(map[laneKey]*laneGroup)}
	return pool
}

func (p *lanePool) close() { p.cancel() }

// laneCall is one record waiting for its result on a shared stream.
type laneCall struct {
	done chan laneOutcome
}

type laneOutcome struct {
	event *pb.Event
	err   error
}

type lane struct {
	pool    *lanePool
	key     laneKey
	stream  grpc.BidiStreamingClient[pb.ExecuteRequest, pb.ExecuteResponse]
	cancel  context.CancelFunc
	expires time.Time
	// load counts reserved and outstanding records. Only reserve, under the
	// pool lock, increases it.
	load atomic.Int64

	// send orders index assignment with Send; only the sender holds it.
	send  sync.Mutex
	index uint64

	mu          sync.Mutex
	outstanding map[uint64]*laneCall
	closing     bool
	halfClosed  bool
	err         error
	idle        *time.Timer
}

// execute sends one validated command on a shared stream when one has room and
// otherwise reports false so the caller can use a stream of its own. Callers
// validate first because an invalid request ends the stream for every record.
func (p *lanePool) execute(ctx context.Context, key laneKey, command *pb.Command) (*pb.Event, bool, error) {
	if proto.Size(command) > laneRecordBytes {
		return nil, false, nil
	}
	for {
		chosen, err := p.reserve(ctx, key)
		if err != nil {
			return nil, true, err
		}
		if chosen == nil {
			return nil, false, nil
		}
		event, retry, err := chosen.call(ctx, command)
		if !retry {
			return event, true, err
		}
	}
}

// reserve takes a record slot on the least-loaded stream. It opens another
// stream once every stream is half full, one at a time per Store and kind. A
// nil lane without error means every stream is full.
func (p *lanePool) reserve(ctx context.Context, key laneKey) (*lane, error) {
	for {
		p.mu.Lock()
		group := p.groups[key]
		if group == nil {
			group = &laneGroup{}
			p.groups[key] = group
		}
		now := time.Now()
		var chosen *lane
		var expired []*lane
		live := group.lanes[:0]
		for _, candidate := range group.lanes {
			if now.After(candidate.expires) {
				expired = append(expired, candidate)
				continue
			}
			live = append(live, candidate)
			if chosen == nil || candidate.load.Load() < chosen.load.Load() {
				chosen = candidate
			}
		}
		clear(group.lanes[len(live):])
		group.lanes = live
		load := int64(laneRecords)
		if chosen != nil {
			load = chosen.load.Load()
		}
		opening := group.opening
		open := load >= laneOpenAt && len(live) < maxLanes && opening == nil
		if open {
			group.opening = make(chan struct{})
		} else if load < laneRecords {
			chosen.load.Add(1)
		}
		p.mu.Unlock()
		for _, retired := range expired {
			retired.drain()
		}
		switch {
		case open:
			return p.open(ctx, key, group)
		case load < laneRecords:
			return chosen, nil
		case opening == nil:
			return nil, nil
		}
		select {
		case <-opening:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// open starts a stream for group and reserves its first record slot.
func (p *lanePool) open(ctx context.Context, key laneKey, group *laneGroup) (*lane, error) {
	laneCtx, cancel := context.WithCancel(p.ctx)
	// The opening caller's cancellation bounds connection establishment only.
	stop := context.AfterFunc(ctx, cancel)
	stream, err := p.client.Execute(laneCtx, grpc.MaxCallSendMsgSize(protocol.MaxTransportMessageBytes), grpc.MaxCallRecvMsgSize(protocol.MaxTransportMessageBytes), grpc.MaxRetryRPCBufferSize(0))
	if err == nil {
		// Calling Context commits this attempt, disabling transparent write replay.
		_ = stream.Context()
	}
	stopped := stop()
	var opened *lane
	if stopped && err == nil {
		opened = &lane{pool: p, key: key, stream: stream, cancel: cancel, expires: time.Now().Add(laneLifetime), outstanding: make(map[uint64]*laneCall)}
		opened.load.Store(1)
		go opened.receive()
	} else {
		cancel()
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = ctxErr
		}
	}
	p.mu.Lock()
	if opened != nil {
		group.lanes = append(group.lanes, opened)
	}
	close(group.opening)
	group.opening = nil
	p.mu.Unlock()
	return opened, err
}

// call sends one record on a reserved stream and waits for its result. retry
// reports that the stream closed before the record was sent.
func (l *lane) call(ctx context.Context, command *pb.Command) (*pb.Event, bool, error) {
	pending := &laneCall{done: make(chan laneOutcome, 1)}
	request := &pb.ExecuteRequest{StoreName: l.key.store, Command: command}
	if deadline, ok := ctx.Deadline(); ok {
		request.TimeoutMicros = protocol.TimeoutMicros(time.Until(deadline))
	}
	l.send.Lock()
	if err := ctx.Err(); err != nil {
		l.send.Unlock()
		l.release()
		return nil, false, err
	}
	l.mu.Lock()
	if l.closing || l.err != nil || l.index == math.MaxUint64-1 {
		l.mu.Unlock()
		l.send.Unlock()
		l.release()
		return nil, true, nil
	}
	l.index++
	request.Index = l.index
	// Register before Send: the result can arrive before Send returns.
	l.outstanding[request.Index] = pending
	if l.idle != nil {
		l.idle.Stop()
	}
	l.mu.Unlock()
	err := l.stream.Send(request)
	l.send.Unlock()
	if err != nil && !errors.Is(err, io.EOF) {
		// A local send failure may never surface through Recv.
		l.cancel()
	}
	select {
	case outcome := <-pending.done:
		return outcome.event, false, outcome.err
	case <-ctx.Done():
		l.abandon(request.Index)
		return nil, false, ctx.Err()
	}
}

// abandon asks the server to cancel a record whose caller stopped waiting.
// The record keeps its stream slot until the server reports it.
func (l *lane) abandon(index uint64) {
	request := &pb.ExecuteRequest{StoreName: l.key.store, CancelIndex: index}
	go func() {
		l.send.Lock()
		defer l.send.Unlock()
		l.mu.Lock()
		_, outstanding := l.outstanding[index]
		l.mu.Unlock()
		// A stream half-closes only once nothing is outstanding.
		if !outstanding {
			return
		}
		if err := l.stream.Send(request); err != nil && !errors.Is(err, io.EOF) {
			l.cancel()
		}
	}()
}

// release returns a reservation that never became an outstanding record.
func (l *lane) release() {
	l.load.Add(-1)
}

func (l *lane) receive() {
	var err error
	for {
		var response *pb.ExecuteResponse
		response, err = l.stream.Recv()
		if err != nil {
			break
		}
		err = l.deliver(response)
		if err != nil {
			l.cancel()
			break
		}
	}
	if errors.Is(err, io.EOF) {
		err = errLaneClosed
	}
	l.fail(err)
}

func (l *lane) deliver(response *pb.ExecuteResponse) error {
	if err := protocol.ValidateExecuteResponse(response); err != nil {
		return err
	}
	read := response.Event.GetReadResult() != nil
	if read != l.key.read || !read && response.Event.GetMutationResult() == nil {
		return errors.New("unexpected event on shared Execute stream")
	}
	l.mu.Lock()
	pending, ok := l.outstanding[response.Index]
	if !ok {
		l.mu.Unlock()
		return errors.New("Execute response ordinal is missing, duplicated or unexpected")
	}
	delete(l.outstanding, response.Index)
	l.load.Add(-1)
	if len(l.outstanding) == 0 {
		l.quietLocked()
	}
	l.mu.Unlock()
	outcome := laneOutcome{event: response.Event}
	pending.done <- outcome
	return nil
}

// quietLocked half-closes a retiring stream once it has nothing outstanding
// and schedules an idle one to do the same.
func (l *lane) quietLocked() {
	if l.closing {
		l.halfCloseLocked()
		return
	}
	if l.idle == nil {
		l.idle = time.AfterFunc(laneIdle, l.retire)
		return
	}
	l.idle.Reset(laneIdle)
}

// retire removes an idle stream from its pool and drains it.
func (l *lane) retire() {
	l.pool.remove(l)
	l.drain()
}

// drain stops new records on the stream and half-closes it after its
// outstanding records complete.
func (l *lane) drain() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closing = true
	if len(l.outstanding) == 0 {
		l.halfCloseLocked()
	}
}

func (l *lane) halfCloseLocked() {
	if l.halfClosed {
		return
	}
	l.halfClosed = true
	// CloseSend must not race a Send that already holds the send lock.
	go func() {
		l.send.Lock()
		defer l.send.Unlock()
		if err := l.stream.CloseSend(); err != nil {
			l.cancel()
		}
	}()
}

// fail ends every outstanding record with the stream's final error.
func (l *lane) fail(err error) {
	l.pool.remove(l)
	l.cancel()
	l.mu.Lock()
	l.err = err
	outstanding := l.outstanding
	l.outstanding = make(map[uint64]*laneCall)
	if l.idle != nil {
		l.idle.Stop()
	}
	l.mu.Unlock()
	for _, pending := range outstanding {
		l.load.Add(-1)
		outcome := laneOutcome{err: err}
		pending.done <- outcome
	}
}

func (p *lanePool) remove(l *lane) {
	p.mu.Lock()
	defer p.mu.Unlock()
	group := p.groups[l.key]
	if group == nil {
		return
	}
	for i, candidate := range group.lanes {
		if candidate == l {
			group.lanes = slices.Delete(group.lanes, i, i+1)
			return
		}
	}
}
