// Command soak runs an explicitly configured, non-retrying stability workload.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	weir "github.com/batchstream/weir-go"
	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type options struct {
	Address, Targets, Mongo, Search, RunID                 string
	ServerRevision, ImageDigest, ChartVersion, SDKRevision string
	ObserverStatus, ObserverHeartbeat                      string
	Duration, MaxP99                                       time.Duration
	Workers, Rate                                          int
}

type event struct {
	Kind       string `json:"kind"`
	Worker     int    `json:"worker"`
	Target     string `json:"target"`
	Backend    string `json:"backend"`
	Resource   string `json:"resource"`
	Sequence   int64  `json:"sequence"`
	DurationNS int64  `json:"duration_ns,omitempty"`
	Error      string `json:"error,omitempty"`
	Unknown    bool   `json:"unknown,omitempty"`
}

type mutationAuditError struct {
	Cause   error
	Outcome weir.MutationOutcome
}

func (e *mutationAuditError) Error() string {
	return fmt.Sprintf("mutation outcome %s: %v", e.Outcome, e.Cause)
}
func (e *mutationAuditError) Unwrap() error { return e.Cause }

type worker struct {
	client                                   pb.StoreServiceClient
	backend, collection, resource, id, store string
	number                                   int
	opts                                     options
}

type workerRun struct {
	ctx    context.Context
	cfg    options
	number int
	target string
	start  time.Time
	events chan<- event
}

type histogram struct {
	Counts   [12]uint64
	Total    uint64
	Overflow uint64
}

var buckets = [...]time.Duration{time.Millisecond, 2 * time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 10 * time.Second}

func (h *histogram) add(d time.Duration) {
	h.Total++
	for i, bound := range buckets {
		if d <= bound {
			h.Counts[i]++
			return
		}
	}
	h.Overflow++
}
func (h histogram) p99() time.Duration {
	if h.Total == 0 {
		return 0
	}
	target := (h.Total*99 + 99) / 100
	var seen uint64
	for i, n := range h.Counts {
		seen += n
		if seen >= target {
			return buckets[i]
		}
	}
	return time.Duration(1<<63 - 1)
}

func main() {
	cfg := options{}
	flag.StringVar(&cfg.Address, "address", "", "known owner application host:port; explicit isolated plaintext")
	flag.StringVar(&cfg.Targets, "targets", "", "comma-separated fixed Pod IP:port targets; mutually exclusive with address")
	flag.StringVar(&cfg.Mongo, "mongo-resource", "", "owned Mongo collection URI")
	flag.StringVar(&cfg.Search, "search-resource", "", "owned Search index URI")
	flag.StringVar(&cfg.RunID, "run-id", "", "unique weir-soak-* ownership prefix; never reuse")
	flag.StringVar(&cfg.ServerRevision, "server-revision", "", "exact server source SHA")
	flag.StringVar(&cfg.ImageDigest, "image-digest", "", "exact server OCI digest")
	flag.StringVar(&cfg.ChartVersion, "chart-version", "", "chart version or commit")
	flag.StringVar(&cfg.SDKRevision, "sdk-revision", "", "SDK version or commit")
	flag.StringVar(&cfg.ObserverStatus, "observer-status", "", "optional observer terminal JSON path; requires observer-heartbeat")
	flag.StringVar(&cfg.ObserverHeartbeat, "observer-heartbeat", "", "optional observer heartbeat path; requires observer-status")
	flag.DurationVar(&cfg.Duration, "duration", 0, "required observation duration, e.g. 24h")
	flag.DurationVar(&cfg.MaxP99, "max-p99", 500*time.Millisecond, "maximum cycle p99 bucket upper bound")
	flag.IntVar(&cfg.Workers, "workers", 6, "even worker count; half per backend")
	flag.IntVar(&cfg.Rate, "cycles-per-second", 5, "fixed scheduled rate per worker")
	flag.Parse()
	if err := validate(cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, cfg, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func validate(cfg options) error {
	if _, err := targetAddresses(cfg); err != nil {
		return err
	}
	if (cfg.ObserverStatus == "") != (cfg.ObserverHeartbeat == "") || (cfg.ObserverStatus != "" && cfg.ObserverStatus == cfg.ObserverHeartbeat) {
		return errors.New("observer-status and observer-heartbeat must be distinct paths supplied together")
	}
	_, mongoSegments, mongoErr := protocol.ParseResource(cfg.Mongo)
	_, searchSegments, searchErr := protocol.ParseResource(cfg.Search)
	if mongoErr != nil || searchErr != nil || len(mongoSegments) != 2 || len(searchSegments) != 1 {
		return errors.New("owned Mongo/Search collection URIs are required")
	}
	if !regexp.MustCompile(`^weir-soak-[a-z0-9-]{1,64}$`).MatchString(cfg.RunID) {
		return errors.New("run-id must be a unique weir-soak-* prefix")
	}
	if !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(cfg.ServerRevision) || !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(cfg.ImageDigest) || cfg.ChartVersion == "" || cfg.SDKRevision == "" {
		return errors.New("all artifact identity flags are required")
	}
	if cfg.Duration < time.Second || cfg.Duration > 7*24*time.Hour || cfg.MaxP99 <= 0 || cfg.MaxP99 > 2*time.Second || cfg.Workers < 2 || cfg.Workers > 64 || cfg.Workers%2 != 0 || cfg.Rate < 1 || cfg.Rate > 100 {
		return errors.New("invalid duration, latency bound, worker count or rate")
	}
	return nil
}
func run(parent context.Context, cfg options, out io.Writer) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	encoder := json.NewEncoder(out)
	var observer *observerGuard
	if cfg.ObserverStatus != "" {
		var err error
		observer, err = awaitObserver(ctx, cfg)
		if err != nil {
			report := map[string]any{"kind": "failed", "run_id": cfg.RunID, "elapsed_ns": 0, "cycles": 0, "failures": 1, "unknown": 0, "error": err.Error()}
			if writeErr := encoder.Encode(report); writeErr != nil {
				return errors.Join(err, writeErr)
			}
			return err
		}
	}
	targets, err := targetAddresses(cfg)
	if err != nil {
		return err
	}
	assignments := make([]map[string]any, cfg.Workers)
	for i := range assignments {
		backend := "mongo"
		if i%2 == 1 {
			backend = "search"
		}
		assignments[i] = map[string]any{"worker": i, "backend": backend, "target": targets[i%len(targets)]}
	}
	start := time.Now()
	identity := map[string]any{"kind": "start", "started_utc": start.UTC().Format(time.RFC3339Nano), "duration_ns": int64(cfg.Duration), "run_id": cfg.RunID, "workers": cfg.Workers, "cycles_per_second": cfg.Rate, "max_p99_ns": int64(cfg.MaxP99), "server_revision": cfg.ServerRevision, "image_digest": cfg.ImageDigest, "chart_version": cfg.ChartVersion, "sdk_revision": cfg.SDKRevision, "address": cfg.Address, "mongo_resource": cfg.Mongo, "search_resource": cfg.Search}
	identity["targets"], identity["worker_targets"] = targets, assignments
	if observer != nil {
		identity["observer_status"] = cfg.ObserverStatus
		identity["observer_heartbeat"] = cfg.ObserverHeartbeat
	}
	if err := encoder.Encode(identity); err != nil {
		return err
	}
	events := make(chan event, cfg.Workers*2)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		args := workerRun{ctx: ctx, cfg: cfg, number: i, target: targets[i%len(targets)], start: start, events: events}
		go func() { defer wg.Done(); execute(args) }()
	}
	go func() { wg.Wait(); close(events) }()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	var observerTicks <-chan time.Time
	if observer != nil {
		observerTicker := time.NewTicker(time.Second)
		defer observerTicker.Stop()
		observerTicks = observerTicker.C
	}
	var hist histogram
	var interval histogram
	var cycles, streams, failures, unknown uint64
	var firstError error
	emit := func(kind string) error {
		report := map[string]any{"kind": kind, "utc": time.Now().UTC().Format(time.RFC3339Nano), "elapsed_ns": int64(time.Since(start)), "cycles": cycles, "verified_mutations": cycles * 2, "verified_reads": cycles * 2, "stream_checks": streams, "failures": failures, "unknown": unknown, "cycle_p99_upper_ns": int64(hist.p99()), "interval_p99_upper_ns": int64(interval.p99()), "cycle_histogram": hist.Counts, "cycle_overflow": hist.Overflow, "run_id": cfg.RunID}
		if firstError != nil {
			report["error"] = firstError.Error()
		}
		return encoder.Encode(report)
	}
	checkObserver := func() {
		if observer == nil || firstError != nil {
			return
		}
		if err := observer.check(time.Now()); err != nil {
			firstError = err
			failures++
			cancel()
			report := map[string]any{"kind": "observer_failure", "run_id": cfg.RunID, "error": err.Error()}
			if writeErr := encoder.Encode(report); writeErr != nil {
				firstError = errors.Join(firstError, writeErr)
			}
		}
	}
	for {
		select {
		case <-observerTicks:
			checkObserver()
		case item, ok := <-events:
			if !ok {
				checkObserver()
				elapsed := time.Since(start)
				planned := uint64(cfg.Duration.Seconds() * float64(cfg.Rate*cfg.Workers))
				if firstError == nil && (parent.Err() != nil || elapsed < cfg.Duration || cycles*100 < planned*98 || hist.p99() > cfg.MaxP99 || interval.p99() > cfg.MaxP99) {
					firstError = fmt.Errorf("duration/rate/latency gate failed: elapsed=%s cycles=%d planned=%d p99=%s interrupted=%v", elapsed, cycles, planned, hist.p99(), parent.Err())
				}
				kind := "passed"
				if firstError != nil {
					kind = "failed"
				}
				if err := emit(kind); err != nil {
					return err
				}
				return firstError
			}
			if item.Error != "" {
				failures++
				if item.Unknown {
					unknown++
				}
				if firstError == nil {
					firstError = errors.New(item.Error)
					cancel()
				}
				if err := encoder.Encode(item); err != nil {
					firstError = err
					cancel()
				}
			} else if item.Kind == "cycle" {
				cycles++
				hist.add(time.Duration(item.DurationNS))
				interval.add(time.Duration(item.DurationNS))
			} else if item.Kind == "streams" {
				streams++
			} else if err := encoder.Encode(item); err != nil {
				firstError = err
				cancel()
			}
		case <-ticker.C:
			if firstError == nil && interval.p99() > cfg.MaxP99 {
				firstError = fmt.Errorf("minute cycle p99 %s exceeds %s", interval.p99(), cfg.MaxP99)
				failures++
				cancel()
			}
			if err := emit("progress"); err != nil {
				firstError = err
				cancel()
			}
			interval = histogram{}
		}
	}
}
func execute(args workerRun) {
	ctx, cfg, number, start, events := args.ctx, args.cfg, args.number, args.start, args.events
	backend, collection := "mongo", cfg.Mongo
	if number%2 == 1 {
		backend, collection = "search", cfg.Search
	}
	id := fmt.Sprintf("%s-%02d", cfg.RunID, number)
	store, _, parseErr := protocol.ParseResource(collection)
	relative := strings.TrimPrefix(collection, "weir://"+store+"/")
	w := worker{backend: backend, collection: relative, resource: relative + "/s:" + id, id: id, store: store, number: number, opts: cfg}
	report := func(kind string, sequence int64, d time.Duration, err error) {
		e := event{Kind: kind, Worker: number, Target: args.target, Backend: backend, Resource: "weir://" + w.store + "/" + w.resource, Sequence: sequence, DurationNS: int64(d)}
		if err != nil {
			e.Error = err.Error()
			e.Unknown = unknownWrite(err)
		}
		events <- e
	}
	if parseErr != nil {
		report("failure", 0, 0, parseErr)
		return
	}
	connection, err := weir.Dial(args.target)
	if err != nil {
		report("failure", 0, 0, err)
		return
	}
	defer connection.Close()
	w.client = pb.NewStoreServiceClient(connection)
	initialization, cancel := context.WithTimeout(ctx, 2*time.Second)
	err = verifyOwner(initialization, w.client, args.target, w.store)
	cancel()
	if err != nil {
		report("failure", 0, 0, err)
		return
	}
	create, err := w.mutation("create", 0)
	if err == nil {
		err = w.mutate(ctx, create)
	}
	if err != nil {
		report("failure", 0, 0, err)
		return
	}
	report("owned", 0, 0, nil)
	tick := time.NewTicker(time.Second / time.Duration(cfg.Rate))
	defer tick.Stop()
	sequence := int64(0)
	nextStreams := time.Now().Add(time.Minute)
	for time.Since(start) < cfg.Duration {
		select {
		case <-ctx.Done():
			report("failure", sequence, 0, ctx.Err())
			return
		case <-tick.C:
		}
		if time.Since(start) >= cfg.Duration {
			break
		}
		sequence++
		before := time.Now()
		if err := w.cycle(ctx, sequence); err != nil {
			report("failure", sequence, time.Since(before), err)
			return
		}
		report("cycle", sequence, time.Since(before), nil)
		if number < 2 && !time.Now().Before(nextStreams) {
			if err := w.streams(ctx); err != nil {
				report("failure", sequence, 0, err)
				return
			}
			report("streams", sequence, 0, nil)
			nextStreams = time.Now().Add(time.Minute)
		}
	}
	// Only a clean, completed worker deletes its own record. On any uncertainty
	// leave data for external reconciliation; there is no cleanup retry.
	if ctx.Err() != nil {
		report("failure", sequence, 0, ctx.Err())
		return
	}
	if number < 2 {
		if err := w.streams(ctx); err != nil {
			report("failure", sequence, 0, err)
			return
		}
		report("streams", sequence, 0, nil)
	}
	remove, _ := w.mutation("delete", 0)
	if err := w.mutate(ctx, remove); err != nil {
		report("failure", sequence, 0, err)
		return
	}
	read := &weir.ReadRequest{Resource: w.resource}
	result, err := w.read(ctx, read)
	if err == nil && !result.GetMissing() {
		err = errors.New("delete acknowledged but record remains")
	}
	if err != nil {
		report("failure", sequence, 0, err)
		return
	}
	report("cleaned", sequence, 0, nil)
}

func unknownWrite(err error) bool {
	var audited *mutationAuditError
	return errors.As(err, &audited) && audited.Outcome == weir.MutationUnknown
}

func (w worker) mutation(action string, n int64) (*weir.Command, error) {
	document := &weir.Document{MediaType: "application/json", Data: []byte(fmt.Sprintf(`{"n":%d}`, n))}
	if w.backend == "mongo" {
		value := bson.D{{Key: "_id", Value: w.id}, {Key: "n", Value: n}}
		raw, err := bson.Marshal(value)
		if err != nil {
			return nil, err
		}
		document.MediaType, document.Data = "application/bson", raw
	}
	request := &weir.WriteRequest{Resource: w.resource, Document: document}
	switch action {
	case "create":
		return weir.NewCreateCommand(request), nil
	case "put":
		return weir.NewPutCommand(request), nil
	case "replace":
		return weir.NewReplaceCommand(request), nil
	case "delete":
		request := &weir.DeleteRequest{Resource: w.resource}
		return weir.NewDeleteCommand(request), nil
	}
	return nil, errors.New("invalid workload action")
}
func (w worker) mutate(ctx context.Context, command *weir.Command) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	produced := false
	var mutation *weir.MutationResult
	options := weir.ExecuteOptions{StoreName: w.store}
	options.Produce = func(context.Context) (*weir.Command, error) {
		if produced {
			return nil, io.EOF
		}
		produced = true
		return command, nil
	}
	options.Consume = func(_ context.Context, _ uint64, event *weir.Event) error {
		mutation = event.GetResult().GetMutation()
		return nil
	}
	err := weir.Execute(ctx, w.client, options)
	if err != nil || mutation.GetOutcome() != weir.MutationApplied || mutation.GetFailure() != nil {
		outcome := weir.MutationUnknown
		if mutation != nil {
			outcome = mutation.Outcome
		}
		if err == nil {
			err = fmt.Errorf("unexpected mutation result: %v", mutation)
		}
		audited := &mutationAuditError{Cause: err, Outcome: outcome}
		return audited
	}
	return nil
}
func (w worker) read(ctx context.Context, request *weir.ReadRequest) (*weir.ReadResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	options := weir.ReadOptions{StoreName: w.store, Request: request}
	read, err := weir.Read(ctx, w.client, options)
	if err != nil {
		return read, err
	}
	if read == nil || read.GetFailure() != nil {
		return read, fmt.Errorf("unexpected read result: %v", read)
	}
	return read, nil
}

func (w worker) verify(doc *weir.Document, want int64) error {
	if doc == nil {
		return errors.New("acknowledged record missing")
	}
	var got int64
	if w.backend == "mongo" {
		raw := bson.Raw(doc.Data)
		if err := raw.Validate(); err != nil {
			return err
		}
		value, ok := raw.Lookup("n").Int64OK()
		if !ok {
			return errors.New("stored sequence is not int64")
		}
		got = value
	} else {
		var value struct {
			N int64 `json:"n"`
		}
		if err := json.Unmarshal(doc.Data, &value); err != nil {
			return err
		}
		got = value.N
	}
	if got != want {
		return fmt.Errorf("stored sequence %d differs from acknowledged %d", got, want)
	}
	return nil
}
func (w worker) cycle(ctx context.Context, n int64) error {
	put, err := w.mutation("put", n*2)
	if err != nil {
		return err
	}
	if err := w.mutate(ctx, put); err != nil {
		return err
	}
	read := &weir.ReadRequest{Resource: w.resource}
	result, err := w.read(ctx, read)
	if err != nil {
		return err
	}
	if err := w.verify(result.GetDocument(), n*2); err != nil {
		return err
	}
	replace, err := w.mutation("replace", n*2+1)
	if err != nil {
		return err
	}
	if err := w.mutate(ctx, replace); err != nil {
		return err
	}
	result, err = w.read(ctx, read)
	if err != nil {
		return err
	}
	return w.verify(result.GetDocument(), n*2+1)
}

func (w worker) streams(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	selector := &weir.Document{MediaType: "application/json", Data: []byte(`{"query":{"ids":{"values":["` + w.id + `"]}}}`)}
	if w.backend == "mongo" {
		filter := bson.D{{Key: "_id", Value: w.id}}
		value := bson.D{{Key: "filter", Value: filter}}
		raw, err := bson.Marshal(value)
		if err != nil {
			return err
		}
		selector.MediaType = "application/bson"
		selector.Data = raw
	}
	request := &weir.ScanRequest{Resource: w.collection, Selector: selector, PageSize: 1}
	count := 0
	pageOptions := weir.ScanOptions{StoreName: w.store, Request: request}
	pageOptions.Consume = func(context.Context, *weir.Document) error { count++; return nil }
	page, err := weir.Scan(ctx, w.client, pageOptions)
	if err != nil {
		return err
	}
	if page.GetFailure() != nil {
		return fmt.Errorf("Scan failed: %v", page.Failure)
	}
	if count != 1 {
		return fmt.Errorf("owned record Scan count=%d", count)
	}
	native := &weir.NativeRequest{Resource: w.collection}
	if w.backend == "mongo" {
		parts := strings.Split(w.collection, "/")
		query := bson.D{{Key: "_id", Value: w.id}}
		command := bson.D{{Key: "count", Value: parts[len(parts)-1]}, {Key: "query", Value: query}}
		raw, err := bson.Marshal(command)
		if err != nil {
			return err
		}
		native.Body = raw
		native.Descriptor = &weir.Document{MediaType: "application/vnd.weir.mongodb-command.v1+protobuf"}
		native.BodyMediaType = "application/bson"
	} else {
		descriptor := &weir.SearchHTTPRequest{Method: "GET", Path: "/_doc/" + w.id}
		encoded, err := weir.SearchHTTPDescriptor(descriptor)
		if err != nil {
			return err
		}
		native.Descriptor = encoded
	}
	var body []byte
	var head *weir.NativeHead
	nativeOptions := weir.NativeOptions{StoreName: w.store, Request: native}
	nativeOptions.Consume = func(_ context.Context, event *weir.Event) error {
		if value := event.GetHead(); value != nil {
			head = value
		}
		if len(body)+len(event.GetChunk()) > 1<<20 {
			return errors.New("native response exceeds workload bound")
		}
		body = append(body, event.GetChunk()...)
		return nil
	}
	terminal, err := weir.Native(ctx, w.client, nativeOptions)
	if err != nil {
		return err
	}
	if terminal.GetCompletion() != weir.NativeResponseComplete {
		return fmt.Errorf("native completion: %v", terminal)
	}

	if w.backend == "mongo" {
		raw := bson.Raw(body)
		if err := raw.Validate(); err != nil {
			return err
		}
		count, countOK := raw.Lookup("n").AsFloat64OK()
		ok, okType := raw.Lookup("ok").AsFloat64OK()
		if !countOK || count != 1 || !okType || ok != 1 {
			return errors.New("Native count differs from one owned record")
		}
	} else {
		if head == nil || head.Metadata == nil {
			return errors.New("Native Search metadata missing")
		}
		meta, err := weir.DecodeSearchHTTPResponse(head.Metadata)
		if err != nil {
			return err
		}
		if meta.StatusCode != 200 {
			return fmt.Errorf("Native HTTP status=%d", meta.StatusCode)
		}
	}
	return nil
}
