# Weir Go SDK

Typed Go client for [Weir](https://github.com/batchstream/weir). Requires Go 1.27.1.
Initialize through any application node, then send business requests directly to
its Store replicas. Weir does not relay business traffic. IP and DNS endpoints
work in Kubernetes and other deployments; URI affinity is not implemented.

```sh
go get github.com/batchstream/weir-go@v0.3.0
```

The SDK depends on the stable `github.com/batchstream/weir-protocol v0.1.0`
release. The independent protocol repository owns public schemas, generated
protobuf types and shared validation/DNS helpers. Both Weir and this SDK consume
it; neither the protocol nor SDK module depends on the server. The SDK does not
generate schemas, and CI rejects first-party pseudo versions and module replacements.

## Initialize and read

Import only `weir "github.com/batchstream/weir-go"` for ordinary business calls:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
options := weir.OpenOptions{Seed: "127.0.0.1:7447", Stores: []string{"search"}}
client, err := weir.Open(ctx, options)
if err != nil { return err }
defer client.Close()

first := &weir.ReadRequest{Resource: "records/s:example"}
second := &weir.ReadRequest{Resource: "records/s:another"}
readOptions := weir.ReadOptions{StoreName: "search", Requests: []*weir.ReadRequest{first, second}}
results, err := client.Read(ctx, readOptions)
if err != nil { return fmt.Errorf("read RPC incomplete; preserve results %v: %w", results, err) }
for index, result := range results {
    if result.Failure != nil { return fmt.Errorf("read %d failed: %v", index, result.Failure) }
    fmt.Println(index, "missing:", result.Missing)
}
```

`Open` resolves ownership and establishes a ready direct connection for every
requested Store. Reuse the client across goroutines and close it after callers
finish. Its worker refreshes directory metadata and DNS replicas. The opening
context bounds initialization; it does not own the returned client. Open accepts
1–16 Stores. Discovery may retry read-only initialization; business requests are
never replayed. A stale directory cache fails closed when its lease expires.

Resources are relative to `StoreName`, without a `weir://STORE/` prefix.
`weir.EncodeSegment` canonically encodes each decoded path segment. MongoDB paths
are `DATABASE/COLLECTION/KEY`; Search paths are `INDEX/KEY`. Mongo documents use
`application/bson` with the first `_id` matching the URI key. Search documents
use `application/json`. The caller owns its document codec; the SDK preserves bytes.

## Business operations

Each method takes named options with `StoreName`. `Read` and `Mutate` accept
`Requests`; single-operation helpers accept `Request`:

| Method | Options / request | Result |
| --- | --- | --- |
| `Read` | `ReadOptions` / multiple `ReadRequest` | `[]*ReadResult`: Document, Missing, Failure |
| `Mutate` | `MutateOptions` / multiple `MutateRequest` | `[]*MutationResult` |
| `ReadOne` | `ReadOneOptions` / `ReadRequest` | `ReadResult` |
| `Create`, `Put`, `Replace` | `WriteOptions` / `WriteRequest` | `MutationResult` |
| `Delete` | `DeleteOptions` / `DeleteRequest` | `MutationResult` |
| `AtomicTransform` | `AtomicTransformOptions` / `AtomicTransformRequest` | `MutationResult` |
| `Scan` | `ScanOptions` / `ScanRequest`, document consumer | `ScanEnd` |
| `Native` | `NativeOptions` / `NativeRequest`, event consumer | `NativeEnd` |

`Read` and `Mutate` validate the complete input before selecting the Store
connection or opening their single Execute RPC. An empty batch, nil request,
malformed resource, unsupported action or invalid document envelope rejects the
entire call without sending earlier valid items. Every resource is relative to
one outer `StoreName`; full `weir://STORE/` URIs are rejected. Both methods preserve
input order even when responses arrive out of order or resources are repeated.
Individual backend failures stay in their result positions. With a transport
error, validated results remain evidence and nil positions are unacknowledged;
mutations at those positions may have been applied. The SDK never retries them.

```go
firstDoc := &weir.Document{MediaType: "application/json", Data: []byte(`{"n":1}`)}
secondDoc := &weir.Document{MediaType: "application/json", Data: []byte(`{"n":2}`)}
first := &weir.MutateRequest{Resource: "records/s:first", Action: weir.MutationPut, Document: firstDoc}
second := &weir.MutateRequest{Resource: "records/s:second", Action: weir.MutationPut, Document: secondDoc}
options := weir.MutateOptions{StoreName: "search", Requests: []*weir.MutateRequest{first, second}}
results, err := client.Mutate(ctx, options)
// Inspect every result's Outcome/Failure, even when err is nonnil. Never replay
// an unacknowledged mutation automatically.
```

`MutateRequest.Action` selects `MutationCreate`, `MutationPut`, `MutationReplace`,
`MutationDelete` or `MutationAtomicTransform`. Writes require Document, Delete
accepts no payload, and AtomicTransform requires exactly one Program or
BackendExpression. A batch is not a transaction and its items are independent;
use separate completed calls for dependent operations. Convenience batch methods
accept 1–128 requests (`MaxBatchRequests`) and at most 32 MiB of encoded input
(`MaxBatchInputBytes`). Preflight retains that bounded encoding and Execute reuses
it. Pending transport input still has the independent eight-request / 16 MiB
limit. A read batch retains its results, potentially 256 MiB of document bytes at
the maximum size, so choose smaller batches when documents are large. Use Execute
consumers for incremental processing of larger workloads.

`WriteRequest` contains Resource, Document and optional AdapterOptions. Create
requires absence; Replace requires an existing resource; Put creates or replaces.
AtomicTransform requires exactly one Program or BackendExpression. Its invalid
combinations are rejected before any business frame is sent, including in a batch.

Read and mutation results may accompany a transport error. Preserve their
validated backend evidence: `weir.MutationApplied` remains an acknowledgement if
a later frame or final status is lost. A missing acknowledgement is indeterminate;
reconcile through application knowledge and never automatically replay a write.
Backend failures remain in the result separately from RPC completion errors.

`Scan` executes one finite page and consumes documents incrementally. It returns
a checkpoint only after the matching document count, request completion frame
and final gRPC OK. A failed page returns no ScanEnd; keep the previous token.
Retrying that page may repeat documents, so the caller owns deduplication and
committing output together with its checkpoint. A completed RPC can still carry
`ScanEnd.Failure`, which has no continuation token.

`NativeRequest` contains Resource, Descriptor, BodyMediaType and a bounded Body.
Its consumer sees flat SDK Events containing Head, Chunk or NativeEnd. Chunks are
nonempty and consumed incrementally; the SDK does not collect a whole response.
A validated NativeEnd is returned with a later transport or consumer error.
Completion describes response transport evidence, not a normalized mutation
outcome. Interpret backend response status/body separately. For Search HTTP,
use `SearchHTTPRequest`, `SearchHTTPDescriptor` and `DecodeSearchHTTPResponse`
without importing generated protobuf packages. Mongo command bodies use BSON and
`MongoCommandMediaType` for the descriptor.

## Finite batches

`client.Execute(ctx, ExecuteOptions)` uses a fixed Store, with `Produce`, `Consume`
and optional `Complete` callbacks. Produce returns an opaque SDK Command, then
`io.EOF` when its finite input ends. Use `NewReadCommand`, `NewCreateCommand`,
`NewPutCommand`, `NewReplaceCommand`, `NewDeleteCommand`,
`NewAtomicTransformCommand`, `NewScanCommand` or `NewNativeCommand`; callers never
construct a wire version or protobuf oneof. Consume receives validated flat SDK
Events incrementally. Complete runs after one request's terminal business event
and empty completion frame. Every request must complete and the RPC must finish with gRPC OK
for the whole Execute to succeed. Backend failures remain business results.

Requests can be in flight concurrently and complete out of input order. Wait
for Complete, or finish one typed method, before dependent operations. This is
a framing barrier; check Outcome/Failure before depending on business success. Callbacks
must honor their context and return promptly. Do not retain unbounded events or
mutate requests/documents while execution runs. The SDK keeps at most eight
requests and 16 MiB of encoded pending input in flight. Documents are bounded at
2 MiB, Commands at 9 MiB, event fragments at 64 KiB, scan pages at 256 documents,
and selectors/transform expressions at 16 KiB. Servers may use smaller budgets.

Use caller deadlines. Business operations have no implicit retry or failover.
`Dial` and package-level operation functions provide an advanced fixed-owner
gRPC seam; the caller owns target validation and connection lifetime. That seam
can accept a generated StoreService client for raw protocol fixtures. Ordinary
applications use Open and the typed Client methods shown above.

## Examples and validation

[read](examples/read/main.go) performs a typed read, [basic](examples/basic/main.go)
mutates a document then reads multiple resources in a single typed batch, [scan](examples/scan/main.go)
commits finite-page checkpoints, and [native](examples/native/main.go) consumes
native responses. These examples use SDK request/result/event types. The explicit
[soak workload](examples/soak/README.md) audits fixed owners and write uncertainty;
its advanced owner check uses the public discovery transport on the same connection.

```sh
python3 scripts/download_modules.py
GOWORK=off GOPROXY=off GOSUMDB=off go test -race -count=1 ./...
GOWORK=off GOPROXY=off GOSUMDB=off go vet ./...
GOWORK=off GOPROXY=off GOSUMDB=off python3 scripts/check_dependencies.py
```

Default tests use owned loopback fixtures, without databases or production I/O.
CI checks typed operation dispatch, acknowledgements with transport errors,
malformed/empty native chunks, incremental consumption, scan checkpoints,
completion/cancellation, no replay, deadline admission, directory expiry/conflicts,
DNS refresh and fixed-owner workload routing. The full module graph and normal/
integration package closures must remain independent of the Weir server. Releases
require stable unreplaced dependencies and compile a separate ordinary consumer
that imports only this SDK for all business operations.

Real backend tests require an explicit tag and operator-provided fixtures:

```sh
WEIR_ADDRESS=127.0.0.1:7447 \
WEIR_MONGO_RESOURCE=weir://mongo/weir_acceptance/records \
WEIR_SEARCH_RESOURCE=weir://search/weir_acceptance \
go test -race -tags integration -run 'Test(Mongo|Search)Lifecycle' -v .
```

Pre-create disposable collections/indexes. Each backend owns one generated record
and checks the typed operations, Complete-gated mixed Execute, Scan and Native.
The [qualification record](docs/qualification.md) separates current protocol checks
from historical release/deployment evidence.

Application and peer listeners use plaintext gRPC; restrict their network access.
This repository follows the upstream project's current licensing status; no new
license grant is introduced here.
