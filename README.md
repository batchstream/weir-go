# Weir Go SDK

Typed Go client for [Weir](https://github.com/batchstream/weir). Requires Go 1.27.1.
Initialize through any application node, then send business requests directly to
its Store replicas. Weir does not relay business traffic. IP and DNS endpoints
work in Kubernetes and other deployments; URI affinity is not implemented.

```sh
go get github.com/batchstream/weir-go@v0.4.2
```

The SDK depends on the stable `github.com/batchstream/weir-protocol v0.2.1`
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
if err != nil { return fmt.Errorf("read RPC failed; unacknowledged results %v: %w", results, err) }
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
connection or opening their single typed unary RPC. An empty batch, nil request,
malformed resource, unsupported action or invalid document envelope rejects the
entire call without sending earlier valid items. Every resource is relative to
one outer `StoreName`; full `weir://STORE/` URIs are rejected. Results preserve
input order, including repeated resources. Individual backend failures stay in
their result positions. A failed RPC returns a result slice of the submitted
length with nil entries: the whole response is unacknowledged and any mutation
may have applied. The SDK never automatically replays business requests.
Before protobuf decoding, each batch reply is checked against the submitted
request count and the encoded byte budget. Malformed or excess results leave the
entire batch unacknowledged. The request count adds no separate batch-size cap.

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
BackendExpression. A batch is not a transaction. Mutations to the same resource
execute in input order, including after an item failure; different resources may
run concurrently. Across RPCs, ordering follows the database semantics.

There is no independent request-count or in-flight-item limit. Complete protobuf
request and response envelopes are each bounded at 32 MiB
(`MaxBatchRequestBytes`, `MaxBatchResponseBytes`). Documents are bounded at 2 MiB.
Use smaller batches when documents are large. The SDK validates every result and
requires exactly one result per input before exposing any unary batch evidence.

`WriteRequest` contains Resource, Document and optional AdapterOptions. Create
requires absence; Replace requires an existing resource; Put creates or replaces.
AtomicTransform requires exactly one Program or BackendExpression.

On a successful RPC, `weir.MutationApplied` may also carry a subsequent
acknowledgement failure. Inspect both Outcome and Failure. Other outcomes always
carry a Failure. A failed unary RPC provides no confirmed individual results;
reconcile through application knowledge and never automatically replay a write.

`Scan` executes one finite page and consumes documents incrementally. It returns
a checkpoint only after the matching document count, terminal ScanEnd
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

## Streaming and deadlines

Scan and Native each issue one server-streaming RPC with one typed request and
incrementally consume typed Events. Native chunks are bounded at 64 KiB, Scan
pages at 256 documents and selectors/transform expressions at 16 KiB. There are
no per-record Commands, request IDs, fragments, producer callbacks or completion
frames in the SDK API. Consumers must honor their context and return promptly.
Do not retain unbounded events or mutate requests/documents while a call runs.

Use caller deadlines. Business operations have no implicit retry or failover.
`Dial` uses gRPC's normal adaptive flow control and reusable connections. Package
operation functions accept a generated StoreService client for fixed-owner
connections and protocol fixtures; ordinary applications use Open and typed Client
methods. The caller owns target validation and connection lifetime for Dial.

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
