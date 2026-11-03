package weir

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ReadStream sends each produced read once there is room and consumes results
// as they complete, over a gRPC stream of its own that follows the Client's
// servers: it moves to another server when its server stops resolving, refuses
// a read or reaches the stream lifetime. A read a server refused is sent again;
// nothing else is. Every result received before a later error is consumed.
func (c *Client) ReadStream(ctx context.Context, options ReadStreamOptions) error {
	return c.readStream(ctx, options, false)
}

func (c *Client) readStream(ctx context.Context, options ReadStreamOptions, known bool) error {
	if _, err := c.storeClient(); err != nil {
		return err
	}
	if options.Next == nil || options.Consume == nil {
		return errors.New("ReadStream requires a producer and consumer")
	}
	next := func(ctx context.Context) (*pb.Command, string, error) {
		request, err := options.Next(ctx)
		if err != nil {
			return nil, "", err
		}
		command, err := readCommand(request)
		return command, "", err
	}
	key := c.lanes.ownKey(options.StoreName, true)
	send := func(ctx context.Context, command *pb.Command, queued *queuedRecord) (*ReadResult, error) {
		event, shared, err := c.lanes.finish(ctx, key, command, queued)
		if !shared {
			return c.readAlone(ctx, options.StoreName, command.GetRead())
		}
		if err != nil {
			return nil, err
		}
		return readResult(event.GetReadResult()), nil
	}
	stream := clientStreamOptions[*ReadResult]{key: key, known: known, next: next, send: send, refused: readRefused, consume: options.Consume}
	return runClientStream(ctx, c, stream)
}

// MutateStream sends each produced mutation once there is room and consumes
// results as they complete, over a gRPC stream of its own that follows the
// Client's servers, as ReadStream does. Mutations of one resource run in input
// order, each after the previous one's result, including after a failure. A
// mutation a server refused is sent again; nothing else is. Each consumed result
// is an individual acknowledgement, and every result received before a later
// error is consumed; other mutations stay unacknowledged and may have applied.
func (c *Client) MutateStream(ctx context.Context, options MutateStreamOptions) error {
	return c.mutateStream(ctx, options, false)
}

func (c *Client) mutateStream(ctx context.Context, options MutateStreamOptions, known bool) error {
	if _, err := c.storeClient(); err != nil {
		return err
	}
	if options.Next == nil || options.Consume == nil {
		return errors.New("MutateStream requires a producer and consumer")
	}
	next := func(ctx context.Context) (*pb.Command, string, error) {
		request, err := options.Next(ctx)
		if err != nil {
			return nil, "", err
		}
		command, err := mutateCommand(request)
		return command, command.GetMutate().GetResource(), err
	}
	key := c.lanes.ownKey(options.StoreName, false)
	send := func(ctx context.Context, command *pb.Command, queued *queuedRecord) (*MutationResult, error) {
		event, shared, err := c.lanes.finish(ctx, key, command, queued)
		if !shared {
			return c.mutateAlone(ctx, options.StoreName, command)
		}
		if err != nil {
			return nil, err
		}
		return event.GetMutationResult(), nil
	}
	stream := clientStreamOptions[*MutationResult]{key: key, known: known, next: next, send: send, refused: mutationRefused, consume: options.Consume}
	return runClientStream(ctx, c, stream)
}

// clientStreamOptions describes one record stream of a Client.
type clientStreamOptions[R comparable] struct {
	// key names the shared-stream group only this record stream uses.
	key laneKey
	// known says next never blocks, as all its records already exist, so those
	// that fit the window leave in one message.
	known bool
	// next returns a validated command and, when it must keep input order, its
	// resource.
	next func(context.Context) (*pb.Command, string, error)
	// send returns the result of one command, sending it once unless it is
	// already queued.
	send    func(context.Context, *pb.Command, *queuedRecord) (R, error)
	refused func(R) bool
	consume func(context.Context, uint64, R) error
}

// streamRecord is one record produced for a stream.
type streamRecord struct {
	index    uint64
	size     int
	resource string
	command  *pb.Command
}

// streamOutcome holds a record's result, or nil without one.
type streamOutcome[R comparable] struct {
	record *streamRecord
	result R
}

// clientStream sends records over lanes and hands their results to its caller.
// It holds at most recordWindowItems records, sent or waiting for an earlier
// record of their resource, from the moment they are produced until they are
// consumed, so a slow consumer stops the producer.
type clientStream[R comparable] struct {
	client  *Client
	options clientStreamOptions[R]
	// caller is the caller's context, which Consume receives: an error that
	// stops sending does not stop the delivery of results already received.
	caller context.Context
	// ctx ends with the first error and stops sending.
	ctx    context.Context
	cancel context.CancelFunc
	// outcomes has room for every record the window allows.
	outcomes chan streamOutcome[R]
	// released returns consumed records' sizes to the producer.
	released chan int
	sends    sync.WaitGroup
	// unsent holds the streams with records the producer has not woken yet.
	unsent []*lane

	mu sync.Mutex
	// running holds the records waiting behind each resource's running record.
	running map[string][]*streamRecord
	failure error
}

func runClientStream[R comparable](ctx context.Context, c *Client, options clientStreamOptions[R]) error {
	if !protocol.ValidStoreName(options.key.store) {
		return errors.New("Execute requires a client and valid Store")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	sending, cancel := context.WithCancel(ctx)
	defer cancel()
	stream := &clientStream[R]{
		client:   c,
		options:  options,
		caller:   ctx,
		ctx:      sending,
		cancel:   cancel,
		outcomes: make(chan streamOutcome[R], recordWindowItems),
		released: make(chan int, recordWindowItems),
		running:  make(map[string][]*streamRecord),
	}
	stopWatching := context.AfterFunc(c.lanes.ctx, func() {
		stream.fail(status.Error(codes.Canceled, "client closed"))
	})
	defer stopWatching()
	go stream.produce()
	stream.consume()
	stream.mu.Lock()
	failure := stream.failure
	stream.mu.Unlock()
	// A failed stream may have left a record in Send, reading its caller's
	// document; only joining the writer lets the caller reuse it.
	c.lanes.forget(options.key, failure != nil)
	return failure
}

// fail keeps the stream's first error and stops sending. A record that fails
// cancels the ones behind it before they can start.
func (s *clientStream[R]) fail(err error) {
	s.mu.Lock()
	if s.failure == nil {
		s.failure = err
	}
	s.mu.Unlock()
	s.cancel()
}

// produce admits records until the input ends or fails, then closes outcomes
// once every admitted record has one.
func (s *clientStream[R]) produce() {
	defer func() {
		s.wakeWriters()
		s.sends.Wait()
		close(s.outcomes)
	}()
	var count uint64
	inflight, bytes := 0, 0
	// wait returns one consumed record's credit.
	wait := func() bool {
		s.wakeWriters()
		select {
		case size := <-s.released:
			inflight--
			bytes -= size
			return true
		case <-s.ctx.Done():
			s.fail(s.ctx.Err())
			return false
		}
	}
	for {
		for inflight == recordWindowItems {
			if !wait() {
				return
			}
		}
		if err := s.ctx.Err(); err != nil {
			s.fail(err)
			return
		}
		command, resource, err := s.options.next(s.ctx)
		if errors.Is(err, io.EOF) {
			if count == 0 {
				s.fail(errors.New("Execute requires at least one record"))
			}
			return
		}
		if err != nil {
			s.fail(fmt.Errorf("request %d: %w", count+1, err))
			return
		}
		size := proto.Size(command)
		// A legal item larger than the byte window proceeds alone.
		for inflight > 0 && size > recordWindowBytes-bytes {
			if !wait() {
				return
			}
		}
		count++
		inflight++
		bytes += size
		record := &streamRecord{index: count, size: size, resource: resource, command: command}
		s.admit(record)
	}
}

// admit starts a record, or queues it behind a running record of its resource.
func (s *clientStream[R]) admit(record *streamRecord) {
	if record.resource != "" {
		s.mu.Lock()
		waiting, running := s.running[record.resource]
		if running {
			s.running[record.resource] = append(waiting, record)
			s.mu.Unlock()
			return
		}
		s.running[record.resource] = nil
		s.mu.Unlock()
	}
	queued := s.client.lanes.enqueue(s.ctx, s.options.key, record.command)
	if queued.lane != nil && !slices.Contains(s.unsent, queued.lane) {
		s.unsent = append(s.unsent, queued.lane)
	}
	if !s.options.known {
		s.wakeWriters()
	}
	s.sends.Add(1)
	go s.run(record, queued)
}

// wakeWriters lets the streams send what the producer queued on them. Only the
// producer calls it.
func (s *clientStream[R]) wakeWriters() {
	for _, stream := range s.unsent {
		stream.signal()
	}
	s.unsent = s.unsent[:0]
}

// run sends a record and then those waiting behind it, in order.
// run sends a resource's records in input order, the first of them already
// queued.
func (s *clientStream[R]) run(record *streamRecord, queued *queuedRecord) {
	defer s.sends.Done()
	for record != nil {
		send := func() (R, error) {
			current := queued
			queued = nil
			return s.options.send(s.ctx, record.command, current)
		}
		result, err := resendRefused(s.ctx, s.client, s.options.refused, send)
		if err != nil {
			s.fail(err)
		}
		outcome := streamOutcome[R]{record: record, result: result}
		s.outcomes <- outcome
		record = s.following(record.resource)
	}
}

// following returns the record waiting behind a resource's finished record.
func (s *clientStream[R]) following(resource string) *streamRecord {
	if resource == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	waiting := s.running[resource]
	if len(waiting) == 0 {
		delete(s.running, resource)
		return nil
	}
	s.running[resource] = waiting[1:]
	return waiting[0]
}

// consume hands every received result to the caller, also after an error ended
// the sending, until Consume itself fails. A result can accompany the error that
// ended its own stream. The producer never holds more unread credit than its
// window, so released does not block.
func (s *clientStream[R]) consume() {
	var none R
	consuming := true
	for outcome := range s.outcomes {
		if !consuming || outcome.result == none {
			continue
		}
		if err := s.options.consume(s.caller, outcome.record.index, outcome.result); err != nil {
			s.fail(err)
			consuming = false
			continue
		}
		s.released <- outcome.record.size
	}
}
