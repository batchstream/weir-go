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
	spb "github.com/batchstream/weir/api/weir/search/v1"
	pb "github.com/batchstream/weir/api/weir/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/protobuf/proto"
)

type options struct {
	Address, Mongo, Search, RunID                          string
	ServerRevision, ImageDigest, ChartVersion, SDKRevision string
	ObserverStatus, ObserverHeartbeat                      string
	Duration, MaxP99                                       time.Duration
	Workers, Rate                                          int
}

type event struct {
	Kind       string `json:"kind"`
	Worker     int    `json:"worker"`
	Backend    string `json:"backend"`
	Resource   string `json:"resource"`
	Sequence   int64  `json:"sequence"`
	DurationNS int64  `json:"duration_ns,omitempty"`
	Error      string `json:"error,omitempty"`
	Unknown    bool   `json:"unknown,omitempty"`
}

type bulkAuditError struct {
	Cause    error
	Evidence string
	Unknown  bool
}

func (e *bulkAuditError) Error() string {
	return fmt.Sprintf("Bulk terminal evidence %s: %v", e.Evidence, e.Cause)
}
func (e *bulkAuditError) Unwrap() error { return e.Cause }

type worker struct {
	client                                   *weir.Client
	backend, collection, resource, id, store string
	number                                   int
	opts                                     options
}

type workerRun struct {
	ctx    context.Context
	cfg    options
	number int
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
	flag.StringVar(&cfg.Address, "address", "", "application Service host:port; explicit isolated plaintext")
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
	if (cfg.ObserverStatus == "") != (cfg.ObserverHeartbeat == "") || (cfg.ObserverStatus != "" && cfg.ObserverStatus == cfg.ObserverHeartbeat) {
		return errors.New("observer-status and observer-heartbeat must be distinct paths supplied together")
	}
	if cfg.Address == "" || !strings.HasPrefix(cfg.Mongo, "weir://") || len(strings.Split(cfg.Mongo, "/")) != 5 || !strings.HasPrefix(cfg.Search, "weir://") || len(strings.Split(cfg.Search, "/")) != 4 || strings.HasSuffix(cfg.Mongo, "/") || strings.HasSuffix(cfg.Search, "/") {
		return errors.New("explicit address and owned Mongo/Search collection URIs are required")
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
	start := time.Now()
	identity := map[string]any{"kind": "start", "started_utc": start.UTC().Format(time.RFC3339Nano), "duration_ns": int64(cfg.Duration), "run_id": cfg.RunID, "workers": cfg.Workers, "cycles_per_second": cfg.Rate, "max_p99_ns": int64(cfg.MaxP99), "server_revision": cfg.ServerRevision, "image_digest": cfg.ImageDigest, "chart_version": cfg.ChartVersion, "sdk_revision": cfg.SDKRevision, "address": cfg.Address, "mongo_resource": cfg.Mongo, "search_resource": cfg.Search}
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
		args := workerRun{ctx: ctx, cfg: cfg, number: i, start: start, events: events}
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
	w := worker{backend: backend, collection: collection, resource: collection + "/s:" + id, id: id, store: "weir://" + strings.Split(strings.TrimPrefix(collection, "weir://"), "/")[0], number: number, opts: cfg}
	report := func(kind string, sequence int64, d time.Duration, err error) {
		e := event{Kind: kind, Worker: number, Backend: backend, Resource: w.resource, Sequence: sequence, DurationNS: int64(d)}
		if err != nil {
			e.Error = err.Error()
			e.Unknown = unknownWrite(err)
		}
		events <- e
	}
	clientOpts := weir.Options{Plaintext: true, Timeout: 2 * time.Second}
	var err error
	w.client, err = weir.New(cfg.Address, clientOpts)
	if err != nil {
		report("failure", 0, 0, err)
		return
	}
	defer w.client.Close()
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
	read := &pb.ReadRequest{Resource: w.resource}
	result, err := w.client.Read(ctx, read)
	if err == nil && result.GetMissing() == nil {
		err = errors.New("delete acknowledged but record remains")
	}
	if err != nil {
		report("failure", sequence, 0, err)
		return
	}
	report("cleaned", sequence, 0, nil)
}

func unknownWrite(err error) bool {
	var audited *bulkAuditError
	if errors.As(err, &audited) {
		return audited.Unknown
	}
	var mutation *weir.MutationError
	if errors.As(err, &mutation) && mutation.Outcome == pb.MutationOutcome_UNKNOWN {
		return true
	}
	var batch *weir.BatchError
	if errors.As(err, &batch) {
		if batch.Cause != nil {
			return true
		}
		for _, failure := range batch.Failures {
			if unknownWrite(failure) {
				return true
			}
		}
	}
	return false
}
func (w worker) mutation(action string, n int64) (*pb.MutateRequest, error) {
	doc := &pb.Document{MediaType: weir.MediaTypeJSON, Data: []byte(fmt.Sprintf(`{"n":%d}`, n))}
	if w.backend == "mongo" {
		value := bson.D{{Key: "_id", Value: w.id}, {Key: "n", Value: n}}
		raw, err := bson.Marshal(value)
		if err != nil {
			return nil, err
		}
		doc.MediaType = weir.MediaTypeBSON
		doc.Data = raw
	}
	request := &pb.MutateRequest{Resource: w.resource}
	switch action {
	case "create":
		request.Action = &pb.MutateRequest_Create{Create: doc}
	case "put":
		request.Action = &pb.MutateRequest_Put{Put: doc}
	case "replace":
		request.Action = &pb.MutateRequest_Replace{Replace: doc}
	case "delete":
		empty := &pb.Empty{}
		request.Action = &pb.MutateRequest_Delete{Delete: empty}
	default:
		return nil, errors.New("invalid workload action")
	}
	return request, nil
}
func (w worker) mutate(ctx context.Context, request *pb.MutateRequest) error {
	result, err := w.client.Mutate(ctx, request)
	if err != nil {
		return err
	}
	if result.Outcome != pb.MutationOutcome_APPLIED {
		return fmt.Errorf("unexpected mutation outcome %s", result.Outcome)
	}
	return nil
}
func (w worker) verify(doc *pb.Document, want int64) error {
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
	read := &pb.ReadRequest{Resource: w.resource}
	result, err := w.client.Read(ctx, read)
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
	mutation := weir.Operation{Mutate: replace}
	retrieval := weir.Operation{Read: read}
	operations := []weir.Operation{mutation, retrieval}
	results, err := w.client.Bulk(ctx, w.store, operations)
	if err != nil {
		failure := &bulkAuditError{Cause: err, Evidence: bulkEvidence(results), Unknown: unknownBulkMutation(results, err)}
		return failure
	}
	if len(results) != 2 || results[0].GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED {
		return errors.New("ordered Bulk mutation acknowledgement missing")
	}
	return w.verify(results[1].GetRead().GetDocument(), n*2+1)
}

func unknownBulkMutation(results []*pb.BulkResult, err error) bool {
	// The sole mutation in this workload is index zero. A valid APPLIED reply
	// remains conclusive even if the following Read or final transport fails.
	if len(results) > 0 && results[0].GetMutation() != nil {
		return results[0].GetMutation().Outcome == pb.MutationOutcome_UNKNOWN
	}
	var batch *weir.BatchError
	return errors.As(err, &batch)
}

func bulkEvidence(results []*pb.BulkResult) string {
	items := make([]string, 0, 2)
	for index := range 2 {
		state := "no-result"
		if index < len(results) && results[index] != nil {
			if mutation := results[index].GetMutation(); mutation != nil {
				state = mutation.Outcome.String()
				if mutation.Failure != nil {
					state += "/" + mutation.Failure.Code.String()
				}
			} else if read := results[index].GetRead(); read != nil {
				state = "read"
				if read.GetFailure() != nil {
					state += "/" + read.GetFailure().Code.String()
				}
			}
		}
		items = append(items, fmt.Sprintf("%d=%s", index, state))
	}
	return strings.Join(items, ",")
}
func (w worker) streams(ctx context.Context) error {
	selector := &pb.Document{MediaType: weir.MediaTypeJSON, Data: []byte(`{"query":{"ids":{"values":["` + w.id + `"]}}}`)}
	if w.backend == "mongo" {
		filter := bson.D{{Key: "_id", Value: w.id}}
		value := bson.D{{Key: "filter", Value: filter}}
		raw, err := bson.Marshal(value)
		if err != nil {
			return err
		}
		selector.MediaType = weir.MediaTypeBSON
		selector.Data = raw
	}
	request := &pb.ScanRequest{Resource: w.collection, Selector: selector, FetchItemsHint: 1}
	count := 0
	_, err := w.client.Scan(ctx, request, func(doc *pb.Document) error { count++; return nil })
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("owned record Scan count=%d", count)
	}
	open := &pb.NativeOpen{Resource: w.collection}
	native := weir.NativeRequest{Open: open}
	if w.backend == "mongo" {
		parts := strings.Split(w.collection, "/")
		query := bson.D{{Key: "_id", Value: w.id}}
		command := bson.D{{Key: "count", Value: parts[len(parts)-1]}, {Key: "query", Value: query}}
		raw, err := bson.Marshal(command)
		if err != nil {
			return err
		}
		native.Body = raw
		open.Descriptor_ = &pb.Document{MediaType: "application/vnd.weir.mongodb-command.v1+protobuf"}
		open.BodyMediaType = weir.MediaTypeBSON
	} else {
		descriptor := &spb.Request{Method: "GET", Path: "/_doc/" + w.id}
		raw, err := proto.Marshal(descriptor)
		if err != nil {
			return err
		}
		open.Descriptor_ = &pb.Document{MediaType: "application/vnd.weir.search-http.v1+protobuf", Data: raw}
	}
	var body []byte
	result, err := w.client.Native(ctx, native, func(chunk []byte) error {
		if len(body)+len(chunk) > 1<<20 {
			return errors.New("Native response exceeds workload bound")
		}
		body = append(body, chunk...)
		return nil
	})
	if err != nil {
		return err
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
		if result == nil || result.Head == nil || result.Head.Metadata == nil {
			return errors.New("Native Search metadata missing")
		}
		meta := &spb.Response{}
		if err := proto.Unmarshal(result.Head.Metadata.Data, meta); err != nil {
			return err
		}
		if meta.StatusCode != 200 {
			return fmt.Errorf("Native HTTP status=%d", meta.StatusCode)
		}
	}
	return nil
}
