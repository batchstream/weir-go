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
	// maxLanes bounds the streams per Store and kind that take new records;
	// calls beyond their capacity use a stream of their own.
	maxLanes = 32
	// laneRecordBytes keeps large records from delaying a shared stream's sends.
	laneRecordBytes = 1 << 20
	// laneLifetime retires a stream well within the server's connection age
	// so connection rebalancing still reaches long-running clients.
	laneLifetime = time.Minute
	// laneIdle half-closes an unused stream before the server's stall timeout.
	laneIdle = 10 * time.Second
	// laneAttempts bounds how often a record that never reached the server
	// moves to another shared stream before it uses a stream of its own.
	laneAttempts = 3
	// laneCooldown keeps a stream whose server refused a record from taking
	// new records while any stream has not, so a draining server's stream
	// goes quiet instead of attracting the records it refuses at once, and
	// closes once quiet. When every stream is cooling, they keep carrying
	// records and at most one new stream opens per cooldown.
	laneCooldown = 10 * time.Second
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
	// probed is when a stream last opened while every stream was cooling.
	probed time.Time
}

func newLanePool(client pb.StoreServiceClient) *lanePool {
	ctx, cancel := context.WithCancel(context.Background())
	pool := &lanePool{client: client, ctx: ctx, cancel: cancel, groups: make(map[laneKey]*laneGroup)}
	return pool
}

func (p *lanePool) close() { p.cancel() }

type callState int

const (
	callQueued callState = iota
	callSent
	callAbandoned
	// callReturned is handed back to try another stream.
	callReturned
)

// laneCall is one record queued for, or outstanding on, a shared stream. Its
// state and index are guarded by the lane's mutex.
type laneCall struct {
	command  *pb.Command
	deadline time.Time
	done     chan laneOutcome
	state    callState
	index    uint64
}

type laneOutcome struct {
	event *pb.Event
	err   error
	// retry reports that the record never reached the server.
	retry bool
}

// lane is one shared stream. Callers queue records and wait with their own
// context; only the writer goroutine assigns indexes and sends, so a stalled
// stream never holds a caller past its deadline.
type lane struct {
	pool    *lanePool
	key     laneKey
	stream  grpc.BidiStreamingClient[pb.ExecuteRequest, pb.ExecuteResponse]
	ctx     context.Context
	cancel  context.CancelFunc
	expires time.Time
	// load counts reserved, queued and outstanding records. Only open, before
	// the stream is shared, and the pool, under its lock, increase it.
	load atomic.Int64
	// refused holds when the server last refused a record without starting it.
	refused atomic.Int64
	// wake asks the writer to send what is queued.
	wake chan struct{}

	mu          sync.Mutex
	queued      []*laneCall
	cancels     []uint64
	outstanding map[uint64]*laneCall
	// sending is the record whose Send is in progress; its outcome belongs
	// to the writer even if the stream fails meanwhile.
	sending    *laneCall
	index      uint64
	closing    bool
	halfClosed bool
	err        error
	idle       *time.Timer
}

// execute sends one validated command on a shared stream when one has room and
// otherwise reports false so the caller can use a stream of its own. Callers
// validate first because an invalid request ends the stream for every record.
func (p *lanePool) execute(ctx context.Context, key laneKey, command *pb.Command) (*pb.Event, bool, error) {
	if proto.Size(command) > laneRecordBytes {
		return nil, false, nil
	}
	for range laneAttempts {
		chosen, err := p.reserve(ctx, key)
		if err != nil {
			return nil, true, err
		}
		if chosen == nil {
			return nil, false, nil
		}
		outcome := chosen.call(ctx, command)
		if !outcome.retry {
			return outcome.event, true, outcome.err
		}
	}
	return nil, false, nil
}

// reserve takes a record slot on the least-loaded stream. It opens another
// stream once every stream is half full, one at a time per Store and kind. A
// nil lane without error means every stream is full.
func (p *lanePool) reserve(ctx context.Context, key laneKey) (*lane, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p.mu.Lock()
		group := p.groups[key]
		if group == nil {
			group = &laneGroup{}
			p.groups[key] = group
		}
		now := time.Now()
		var chosen, cooling *lane
		var expired []*lane
		live := group.lanes[:0]
		for _, candidate := range group.lanes {
			if now.After(candidate.expires) {
				expired = append(expired, candidate)
				continue
			}
			live = append(live, candidate)
			best := &chosen
			if candidate.cooling(now) {
				best = &cooling
			}
			if *best == nil || candidate.load.Load() < (*best).load.Load() {
				*best = candidate
			}
		}
		// Once another stream is healthy, quiet cooling streams close. Only
		// the pool, under its lock, increases load, so a quiet one stays quiet.
		if chosen != nil {
			healthy := live[:0]
			for _, candidate := range live {
				if candidate.cooling(now) && candidate.load.Load() == 0 {
					expired = append(expired, candidate)
					continue
				}
				healthy = append(healthy, candidate)
			}
			live = healthy
		}
		clear(group.lanes[len(live):])
		group.lanes = live
		load := int64(laneRecords)
		if chosen != nil {
			load = chosen.load.Load()
		}
		opening := group.opening
		// Without a healthy stream, a group opens a fresh one at once, but
		// while every stream is cooling only one per cooldown; with a healthy
		// stream, overflow uses streams of its own.
		probe := chosen == nil && (cooling == nil || now.Sub(group.probed) >= laneCooldown)
		open := (probe || chosen != nil && load >= laneOpenAt) && len(live) < maxLanes && opening == nil
		// Cooling streams take records only when no stream is healthy.
		if !open && chosen == nil && cooling != nil {
			chosen, load = cooling, cooling.load.Load()
		}
		if open {
			group.opening = make(chan struct{})
			if cooling != nil && chosen == nil {
				group.probed = now
			}
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
		now := time.Now()
		opened = &lane{pool: p, key: key, stream: stream, ctx: laneCtx, cancel: cancel, expires: now.Add(laneLifetime), wake: make(chan struct{}, 1), outstanding: make(map[uint64]*laneCall)}
		opened.load.Store(1)
		go opened.receive()
		go opened.write()
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

// call queues one record on a reserved stream and waits for its outcome.
func (l *lane) call(ctx context.Context, command *pb.Command) laneOutcome {
	pending := &laneCall{command: command, done: make(chan laneOutcome, 1)}
	pending.deadline, _ = ctx.Deadline()
	l.mu.Lock()
	if l.closing || l.err != nil {
		l.mu.Unlock()
		l.load.Add(-1)
		retry := laneOutcome{retry: true}
		return retry
	}
	l.queued = append(l.queued, pending)
	if l.idle != nil {
		l.idle.Stop()
	}
	l.mu.Unlock()
	l.signal()
	select {
	case outcome := <-pending.done:
		return outcome
	case <-ctx.Done():
		l.abandon(pending)
		abandoned := laneOutcome{err: ctx.Err()}
		return abandoned
	}
}

// abandon drops a record whose caller stopped waiting. A queued record is never
// sent; a sent one is canceled on the server and keeps its slot until the
// server reports it.
func (l *lane) abandon(call *laneCall) {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch call.state {
	case callQueued:
		call.state = callAbandoned
	case callSent:
		if l.outstanding[call.index] == call {
			l.cancels = append(l.cancels, call.index)
			l.signal()
		}
	}
}

func (l *lane) signal() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// write sends queued cancel requests and records, then half-closes the stream
// once it is closing and nothing is outstanding.
func (l *lane) write() {
	for {
		select {
		case <-l.wake:
		case <-l.ctx.Done():
			return
		}
		for l.writeNext() {
		}
	}
}

// writeNext sends one frame and reports whether more may be pending.
func (l *lane) writeNext() bool {
	l.mu.Lock()
	if l.err != nil {
		l.mu.Unlock()
		return false
	}
	if len(l.cancels) > 0 {
		index := l.cancels[0]
		l.cancels = slices.Delete(l.cancels, 0, 1)
		_, outstanding := l.outstanding[index]
		l.mu.Unlock()
		if outstanding {
			request := &pb.ExecuteRequest{StoreName: l.key.store, CancelIndex: index}
			err := l.stream.Send(request)
			l.sendFailed(err)
		}
		return true
	}
	if len(l.queued) > 0 {
		call := l.queued[0]
		l.queued = slices.Delete(l.queued, 0, 1)
		if call.state == callAbandoned {
			l.load.Add(-1)
			l.quietLocked()
			l.mu.Unlock()
			return true
		}
		l.index++
		call.state, call.index = callSent, l.index
		// Register before Send: the result can arrive before Send returns.
		l.outstanding[call.index] = call
		l.sending = call
		var returned []*laneCall
		if l.index == math.MaxUint64-1 {
			l.closing = true
			returned = l.returnQueuedLocked()
		}
		l.mu.Unlock()
		l.handBack(returned)
		request := &pb.ExecuteRequest{StoreName: l.key.store, Index: call.index, Command: call.command}
		if !call.deadline.IsZero() {
			request.TimeoutMicros = protocol.TimeoutMicros(time.Until(call.deadline))
		}
		err := l.stream.Send(request)
		l.sendFailed(err)
		l.sent(call, err)
		return true
	}
	if l.closing && len(l.outstanding) == 0 && !l.halfClosed {
		l.halfClosed = true
		unused := l.index == 0
		l.mu.Unlock()
		// A server rejects a stream that ends without a record.
		if unused {
			l.cancel()
		} else if err := l.stream.CloseSend(); err != nil {
			l.cancel()
		}
		return false
	}
	l.mu.Unlock()
	return false
}

// sendFailed ends the stream after a local send failure, which may never
// surface through Recv. io.EOF means Recv holds the stream's final status.
func (l *lane) sendFailed(err error) {
	if err != nil && !errors.Is(err, io.EOF) {
		l.cancel()
	}
}

// sent settles a record whose Send returned. A record whose Send failed never
// reached the server, so after io.EOF it may use another stream.
func (l *lane) sent(call *laneCall, err error) {
	l.mu.Lock()
	l.sending = nil
	if l.outstanding[call.index] != call {
		// Its result already arrived.
		l.mu.Unlock()
		return
	}
	var outcome laneOutcome
	switch {
	case errors.Is(err, io.EOF):
		outcome.retry = true
	case err != nil:
		outcome.err = err
	case l.err != nil:
		// The stream failed during Send and left this record to the writer.
		outcome.err = l.err
	default:
		l.mu.Unlock()
		return
	}
	delete(l.outstanding, call.index)
	l.load.Add(-1)
	l.mu.Unlock()
	call.done <- outcome
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
	// A refused record returns at once; without the cooldown its stream
	// would look least loaded and attract more records.
	if refusesWork(response.Event) {
		l.refused.Store(time.Now().UnixNano())
	}
	l.mu.Lock()
	pending, ok := l.outstanding[response.Index]
	if !ok {
		l.mu.Unlock()
		return errors.New("Execute response ordinal is missing, duplicated or unexpected")
	}
	delete(l.outstanding, response.Index)
	l.load.Add(-1)
	l.quietLocked()
	l.mu.Unlock()
	outcome := laneOutcome{event: response.Event}
	pending.done <- outcome
	return nil
}

// refusesWork reports a record the server did not start because it, or its
// backend, could not take it: a draining or overloaded server, or an
// unavailable backend.
func refusesWork(event *pb.Event) bool {
	failure := event.GetReadResult().GetFailure()
	if result := event.GetMutationResult(); result != nil {
		if result.GetOutcome() != pb.MutationOutcome_NOT_STARTED {
			return false
		}
		failure = result.GetFailure()
	}
	code := failure.GetCode()
	return code == pb.FailureCode_UNAVAILABLE || code == pb.FailureCode_RESOURCE_EXHAUSTED
}

// quietLocked lets a closing stream half-close once nothing is queued or
// outstanding, and schedules an idle one to retire.
func (l *lane) quietLocked() {
	if len(l.outstanding) != 0 || len(l.queued) != 0 {
		return
	}
	if l.closing {
		l.signal()
		return
	}
	if l.idle == nil {
		l.idle = time.AfterFunc(laneIdle, l.retire)
		return
	}
	l.idle.Reset(laneIdle)
}

// cooling reports whether the server refused a record within the cooldown.
func (l *lane) cooling(now time.Time) bool {
	return now.UnixNano()-l.refused.Load() < int64(laneCooldown)
}

// retire removes a stream from its pool and drains it.
func (l *lane) retire() {
	l.pool.remove(l)
	l.drain()
}

// drain stops new records on the stream, hands queued ones back for another
// stream and half-closes it after its outstanding records complete.
func (l *lane) drain() {
	l.mu.Lock()
	l.closing = true
	returned := l.returnQueuedLocked()
	l.mu.Unlock()
	l.signal()
	l.handBack(returned)
}

// returnQueuedLocked empties the queue, releases each queued slot and returns
// the calls still waiting, marked so that abandon leaves them alone.
func (l *lane) returnQueuedLocked() []*laneCall {
	var returned []*laneCall
	for _, call := range l.queued {
		l.load.Add(-1)
		if call.state == callQueued {
			call.state = callReturned
			returned = append(returned, call)
		}
	}
	l.queued = nil
	return returned
}

// handBack tells returned calls to try another stream.
func (l *lane) handBack(returned []*laneCall) {
	for _, call := range returned {
		retry := laneOutcome{retry: true}
		call.done <- retry
	}
}

// fail ends every outstanding record with the stream's final error and hands
// queued ones back for another stream.
func (l *lane) fail(err error) {
	l.pool.remove(l)
	l.cancel()
	l.mu.Lock()
	l.err = err
	outstanding := l.outstanding
	returned := l.returnQueuedLocked()
	l.outstanding = make(map[uint64]*laneCall)
	l.cancels = nil
	if sending := l.sending; sending != nil && outstanding[sending.index] == sending {
		delete(outstanding, sending.index)
		l.outstanding[sending.index] = sending
	}
	if l.idle != nil {
		l.idle.Stop()
	}
	l.mu.Unlock()
	for _, pending := range outstanding {
		l.load.Add(-1)
		outcome := laneOutcome{err: err}
		pending.done <- outcome
	}
	l.handBack(returned)
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
