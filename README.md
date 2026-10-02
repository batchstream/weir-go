# Weir Go SDK

Go client for [Weir](https://github.com/batchstream/weir). Requires Go 1.27.1.
Initialize through any application's `StoreService.ResolveStore` endpoint, then
send finite `StoreService.Execute` RPCs directly to each Store's replicas. Weir
does not relay business traffic. IP and DNS addresses work in Kubernetes and
other deployments; URI affinity is not implemented.

```sh
go get github.com/batchstream/weir-go@main
```

This source uses the current discovery/Execute protocol and replaces the older
five-RPC SDK API. Public schemas, generated protobuf types and shared
validation/DNS helpers are owned by the independent
[weir-protocol](https://github.com/batchstream/weir-protocol) module. Both Weir
and this SDK depend on that module; the protocol module depends on neither.
The SDK has no dependency on the Weir server module and does not generate schemas.
See `go.mod` for the exact protocol revision. This migration pins an upstream
commit and does not publish a new release. SDK releases require a stable,
unreplaced `weir-protocol` tag.

## Initialize and read

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
options := weir.OpenOptions{Seed: "127.0.0.1:7447", Stores: []string{"search"}}
client, err := weir.Open(ctx, options)
if err != nil { return err }
defer client.Close()

request := &pb.ReadRequest{Resource: "records/s:example"}
variant := &pb.Call_Read{Read: request}
call := &pb.Call{Version: 1, Operation: variant}
recordOptions := weir.RecordOptions{StoreName: "search", Call: call}
result, err := client.Record(ctx, recordOptions)
if err != nil { return err }
if result.GetRead().GetFailure() != nil {
    return fmt.Errorf("read failed: %v", result.GetRead().GetFailure())
}
fmt.Println("missing:", result.GetRead().GetMissing() != nil)
```

Import `weir "github.com/batchstream/weir-go"` and
`pb "github.com/batchstream/weir-protocol/api/weir/v1"`. `Open` completes initialization
and establishes a ready direct connection for every requested Store. Reuse the
client across goroutines and close it after callers finish. Its refresh worker
updates directory metadata and DNS replicas; opening context cancellation does
not own the returned client. Open accepts 1–16 Stores. DNS balances replicas
without Kubernetes APIs. Discovery can retry read-only initialization; business
requests are never replayed. A stale directory cache fails closed after its lease.

`Call` selects a read, mutation, finite scan page, or native exchange. A resource
is relative to `StoreName`; the Call carries no `weir://STORE/` prefix. Canonically
encode individual decoded path segments with `protocol.EncodeSegment` from
`github.com/batchstream/weir-protocol/api/protocol`. MongoDB paths are
`DATABASE/COLLECTION/KEY`; Search paths are `INDEX/KEY`. Mongo documents use
`application/bson` with the first `_id` matching the URI key. Search documents
use `application/json`. The SDK preserves document bytes and business outcomes;
inspect `Result.GetMutation()` or `Result.GetRead()` for backend failures.

## Finite execution and completion

`client.Execute(ctx, options)` executes one finite batch with named `Options`
fields `StoreName`, `Produce`, `Consume`, and `Complete`. `Produce` returns a versioned protobuf `Call`, then `io.EOF` when
input ends. `Consume` receives decoded Events incrementally. `Complete` runs once
a request has a validated terminal Event and empty `request_complete` frame.
Every request must complete and the RPC must finish with gRPC OK for Execute to
succeed. Consume and Complete callbacks must honor their context and return
promptly. Do not retain unbounded Events or mutate input while execution runs.

One Execute stream has a fixed Store. Requests may be in flight concurrently,
and completion order can differ from input order. An application requiring
read-after-write waits for Complete or ends one Record before the next call.
`Record` collects one read/mutation Result. It may return a validated result with
a transport error: an APPLIED acknowledgement remains evidence even if a later
frame or final status is lost. Incomplete writes are indeterminate; reconcile
through application knowledge and never automatically replay them.

`ScanPage` consumes each document and returns a finite page's ScanEnd. Its next
continuation token is usable only after a successful page and final gRPC OK. Keep
the previous token on interruption; retrying that page can repeat documents.
Native Calls use Execute and produce NativeHead, chunks, and NativeEnd. Native
completion describes transport evidence; a complete response can contain a
backend error that the caller must interpret.

The SDK keeps at most eight requests and 16 MiB of encoded pending input in flight.
Documents are bounded at 2 MiB, Calls at 9 MiB, event fragments at 64 KiB, scan pages
at 256 documents, and native responses are consumed incrementally. The server may
apply smaller Store budgets. Scan selectors and transform expressions have 16 KiB
bounds. Use caller deadlines; business operations have no implicit retry or
failover. `Dial` and the package-level Execute/Record/ScanPage functions expose a
low-level connection for an already known owner; `Dial` is lazy and does not
initialize or validate ownership.

The current application and peer listeners use plaintext gRPC. Restrict their
network access; the SDK only contacts application listeners. It reads no secret
or credential files and contains no backend provisioning or transaction API.

## Examples and tests

[read](examples/read/main.go) reads one record, [basic](examples/basic/main.go)
executes a finite batch, [scan](examples/scan/main.go) commits page checkpoints,
and [native](examples/native/main.go) consumes native responses. The explicit
[soak workload](examples/soak/README.md) audits fixed owner traffic and uncertainty.

```sh
python3 scripts/download_modules.py
GOWORK=off GOPROXY=off GOSUMDB=off go test -race -count=1 ./...
GOWORK=off GOPROXY=off GOSUMDB=off go vet ./...
GOWORK=off GOPROXY=off GOSUMDB=off python3 scripts/check_dependencies.py
```

Default tests use in-memory or loopback fixtures and never start databases or
contact production. The dependency check scans both packages and the complete
module graph, including test and unused requirements. It rejects the Weir server
module, protocol dependencies leading back to the SDK/server, local module
replacements and SDK-generated schemas. Real backend integration is an explicit opt-in:

```sh
WEIR_ADDRESS=127.0.0.1:7447 \
WEIR_MONGO_RESOURCE=weir://mongo/weir_acceptance/records \
WEIR_SEARCH_RESOURCE=weir://search/weir_acceptance \
go test -race -tags integration -run 'Test(Mongo|Search)Lifecycle' -v .
```

Create the database/collection and concrete Search index beforehand. Each enabled
backend owns one temporary record and checks Create, duplicate precondition,
Replace, Put, AtomicTransform, Read, a Complete-gated mixed Execute, ScanPage,
Native and Delete. Omit a backend resource variable to skip its test. Use dedicated
test datasets. The [qualification record](docs/qualification.md) distinguishes
historical release evidence from checks of the current protocol.

This repository follows the upstream project's current licensing status; no new
license grant is introduced here.
