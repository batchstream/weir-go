package weir

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/protobuf/proto"
)

// ReadStream sends each produced read as soon as there is room and consumes
// results as they complete, over a stream of the Client that follows its
// servers: it moves to another server when its server stops resolving, refuses
// a read or reaches the stream lifetime. A read a server refused is sent again;
// nothing else is. Results delivered before a later error remain evidence.
func (c *Client) ReadStream(ctx context.Context, options ReadStreamOptions) error {
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
	send := func(ctx context.Context, command *pb.Command) (*ReadResult, error) {
		request := command.GetRead()
		event, shared, err := c.lanes.execute(ctx, key, command)
		if !shared {
			alone := ReadOneOptions{StoreName: options.StoreName, Request: request}
			result, err := ReadOne(ctx, c.store, alone)
			return result, callError(ctx, err)
		}
		if err != nil {
			return nil, err
		}
		return readResult(event.GetReadResult()), nil
	}
	stream := clientStreamOptions[*ReadResult]{key: key, next: next, send: send, refused: readRefused, consume: options.Consume}
	return runClientStream(ctx, c, stream)
}

// MutateStream sends each produced mutation as soon as there is room and
// consumes results as they complete, over a stream of the Client that follows
// its servers, as ReadStream does. Mutations of one resource run in input
// order, each after the previous one's result, including after a failure. A
// mutation a server refused is sent again; nothing else is. Each consumed result
// is an individual acknowledgement; an error leaves other mutations
// unacknowledged, and they may have applied.
func (c *Client) MutateStream(ctx context.Context, options MutateStreamOptions) error {
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
	send := func(ctx context.Context, command *pb.Command) (*MutationResult, error) {
		event, shared, err := c.lanes.execute(ctx, key, command)
		if !shared {
			result, err := sendMutationAlone(ctx, c.store, options.StoreName, command)
			return result, callError(ctx, err)
		}
		if err != nil {
			return nil, err
		}
		return event.GetMutationResult(), nil
	}
	stream := clientStreamOptions[*MutationResult]{key: key, next: next, send: send, refused: mutationRefused, consume: options.Consume}
	return runClientStream(ctx, c, stream)
}

// clientStreamOptions describes one record stream of a Client.
type clientStreamOptions[R any] struct {
	// key names the shared-stream group only this record stream uses.
	key laneKey
	// next returns a validated command and, when it must keep input order, its
	// resource.
	next func(context.Context) (*pb.Command, string, error)
	// send sends one command once and returns its result.
	send    func(context.Context, *pb.Command) (R, error)
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

type streamOutcome[R any] struct {
	record *streamRecord
	result R
	err    error
}

// clientStream sends records over lanes and hands their results to its caller.
// It holds at most recordWindowItems records, sent or waiting for an earlier
// record of their resource, from the moment they are produced until they are
// consumed, so a slow consumer stops the producer.
type clientStream[R any] struct {
	client  *Client
	options clientStreamOptions[R]
	ctx     context.Context
	cancel  context.CancelFunc
	// outcomes has room for every record the window allows.
	outcomes chan streamOutcome[R]
	// released returns consumed records' sizes to the producer.
	released chan int
	sends    sync.WaitGroup

	mu sync.Mutex
	// running holds the records waiting behind each resource's running record.
	running map[string][]*streamRecord
	failure error
}

func runClientStream[R any](ctx context.Context, c *Client, options clientStreamOptions[R]) error {
	if !protocol.ValidStoreName(options.key.store) {
		return errors.New("Execute requires a client and valid Store")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	defer c.lanes.forget(options.key)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream := &clientStream[R]{
		client:   c,
		options:  options,
		ctx:      ctx,
		cancel:   cancel,
		outcomes: make(chan streamOutcome[R], recordWindowItems),
		released: make(chan int, recordWindowItems),
		running:  make(map[string][]*streamRecord),
	}
	go stream.produce()
	stream.consume()
	stream.mu.Lock()
	defer stream.mu.Unlock()
	return stream.failure
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

func (s *clientStream[R]) failed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failure != nil
}

// produce admits records until the input ends or fails, then closes outcomes
// once every admitted record has one.
func (s *clientStream[R]) produce() {
	defer func() {
		s.sends.Wait()
		close(s.outcomes)
	}()
	var count uint64
	inflight, bytes := 0, 0
	// wait returns one consumed record's credit.
	wait := func() bool {
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
	s.sends.Add(1)
	go s.run(record)
}

// run sends a record and then those waiting behind it, in order.
func (s *clientStream[R]) run(record *streamRecord) {
	defer s.sends.Done()
	for record != nil {
		send := func() (R, error) { return s.options.send(s.ctx, record.command) }
		result, err := resendRefused(s.ctx, s.client, s.options.refused, send)
		if err != nil {
			s.fail(err)
		}
		outcome := streamOutcome[R]{record: record, result: result, err: err}
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

// consume hands results to the caller until a failure, then discards the rest.
func (s *clientStream[R]) consume() {
	for outcome := range s.outcomes {
		if outcome.err != nil || s.failed() {
			continue
		}
		if err := s.options.consume(s.ctx, outcome.record.index, outcome.result); err != nil {
			s.fail(err)
			continue
		}
		s.released <- outcome.record.size
	}
}
