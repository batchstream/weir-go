# Weir Go SDK

Typed Go client for [Weir](https://github.com/batchstream/weir). Requires Go 1.27.1.
Initialize through any application node, then send business requests directly to
its Store replicas. Weir does not relay business traffic. IP and DNS endpoints
work in Kubernetes and other deployments.

```sh
go get github.com/batchstream/weir-go@v0.16.0
```

The SDK depends on the stable `github.com/batchstream/weir-protocol v0.14.0`
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
context bounds initialization; it does not own the returned client. Open accepts any nonempty set of distinct Stores. Discovery may retry read-only initialization; business requests are
never replayed. A stale directory cache fails closed when its lease expires. Refresh failures
preserve their error cause and gRPC classification; no live ownership announcement
means Unavailable, while conflicting owners mean FailedPrecondition.

Resources are canonical paths relative to `StoreName`.
`weir.EncodeSegment` canonically encodes each decoded path segment. MongoDB paths
are `DATABASE/COLLECTION/KEY`; Search paths are `INDEX/KEY`. Mongo documents use
`application/bson` with the first `_id` matching the resource key. Search documents
use `application/json`. The caller owns its document codec; the SDK preserves bytes.

## Business operations

Each method takes named options with `StoreName`. `Read` and `Mutate` accept
`Requests`; single-operation helpers accept `Request`:

| Method | Options / request | Result |
| --- | --- | --- |
| `Read` | `ReadOptions` / multiple `ReadRequest` | `[]*ReadResult`: Document, Missing, Failure |
| `Mutate` | `MutateOptions` / multiple `MutateRequest` | `[]*MutationResult` |
| `ReadStream` | `ReadStreamOptions` / producer and consumer | per-item `ReadResult` callbacks |
| `MutateStream` | `MutateStreamOptions` / producer and consumer | per-item `MutationResult` callbacks |
| `ReadOne` | `ReadOneOptions` / `ReadRequest` | `ReadResult` |
| `Create`, `Put`, `Replace` | `WriteOptions` / `WriteRequest` | `MutationResult` |
| `Delete` | `DeleteOptions` / `DeleteRequest` | `MutationResult` |
| `AtomicTransform` | `AtomicTransformOptions` / `AtomicTransformRequest` | `MutationResult` |
| `Native` | `NativeOptions` / typed `NativeRequest`, byte consumer | `NativeResult` |

`Read` and `Mutate` validate the complete input before selecting the Store
connection or opening one bidirectional Execute RPC. An empty input, nil request,
malformed resource, unsupported action or invalid document envelope rejects the
entire call without sending earlier valid items. Every resource is relative to
one `StoreName`. Each input is sent immediately as one ExecuteRequest, and results preserve
input order, including repeated resources. Individual backend failures stay in
their result positions. A later RPC failure preserves every validated result
already received; unconfirmed positions remain nil and any such mutation may
have applied. Never automatically replay those mutations.

```go
firstDoc := &weir.Document{ContentType: "application/json", Data: []byte(`{"n":1}`)}
secondDoc := &weir.Document{ContentType: "application/json", Data: []byte(`{"n":2}`)}
first := &weir.MutateRequest{Resource: "records/s:first", Action: weir.MutationPut, Document: firstDoc}
second := &weir.MutateRequest{Resource: "records/s:second", Action: weir.MutationPut, Document: secondDoc}
options := weir.MutateOptions{StoreName: "search", Requests: []*weir.MutateRequest{first, second}}
results, err := client.Mutate(ctx, options)
// Inspect every result's Outcome/Failure, even when err is nonnil. Never replay
// an unacknowledged mutation automatically.
```

`MutateRequest.Action` selects `MutationCreate`, `MutationPut`, `MutationReplace`,
`MutationDelete` or `MutationAtomicTransform`. Writes require Document, Delete
accepts no payload, and AtomicTransform requires exactly one Lua or
BackendExpression. A batch is not a transaction. Mutations to the same resource
execute in input order, including after an item failure; different resources may
run concurrently. Across RPCs, ordering follows the database semantics.

A logical call has no total byte or item limit. Each ExecuteRequest carries one
read or mutation and each response carries its indexed result. Backend protocols determine accepted document sizes. The SDK permits at most 32 unconsumed results and 8 MiB of
unacknowledged encoded input; a larger legal request uses the window alone.
Credit returns after each Consume callback finishes. A size-dependent wait can
hold one produced item that has not yet been sent. Sending and receiving run
concurrently so HTTP/2 flow control advances in both directions. The SDK never
waits for another input or EOF before sending the current item.
`Read` and `Mutate` intentionally accumulate results for their caller; use the
streaming interfaces to avoid retaining the full input and result sequence.

`WriteRequest` contains Resource and Document. Create requires absence; Replace
requires an existing resource; Put creates or replaces. AtomicTransform requires
exactly one Lua or BackendExpression. `MutateRequest`, `WriteRequest`,
`DeleteRequest` and `AtomicTransformRequest` share `Options *MutationOptions`.
Nil options or `MutationApplicationAcknowledged` (the zero Visibility value)
wait for application acknowledgement. Set Visibility to `MutationQueryVisible`
when subsequent normal queries must observe the mutation before the operation
succeeds.
An APPLIED result without Failure confirms that guarantee; APPLIED with Failure
confirms application only and must not be replayed automatically. This includes
Lua keep and missing-document deletes. Search adapters wait for refresh; backends
without deferred query visibility satisfy this when the mutation is acknowledged.
The SDK forwards the complete options message and rejects unknown option fields
or undeclared visibility values.
Keep options immutable until the call returns or its result is consumed.

```go
completion := &weir.MutationOptions{Visibility: weir.MutationQueryVisible}
request := &weir.WriteRequest{Resource: "records/s:first", Document: document, Options: completion}
options := weir.WriteOptions{StoreName: "search", Request: request}
result, err := client.Put(ctx, options)
```

`ReadResult` contains exactly one of Document, Missing, or Failure. Missing is a
successful read that confirmed document absence. `FailureTargetNotFound` means a
required backend collection or index is missing. A failure does not establish
that the requested document is absent. Individual Read/Mutate failures are result
data, even when the Go error is nil; inspect every result as well as the error.

`MutationApplied` means the requested semantics were satisfied. It includes a
successful Lua keep and deleting an already absent document without a database
write. APPLIED may carry a subsequent acknowledgement Failure. Other outcomes
always carry a Failure: NOT_STARTED means no backend mutation began, NOT_APPLIED
means absence of application was established, and UNKNOWN means available evidence
cannot establish whether it applied. A later RPC failure does not revoke any
confirmed item. Unconfirmed nil positions may have applied; reconcile them through
application knowledge and never automatically replay writes.

Requests and responses reject unknown protobuf fields recursively. FailureCode
accepts only declared nonzero values. The SDK validates required result variants,
completion evidence, document envelopes and indexes. A known Failure never
authorizes a retry or changes confirmed MutationApplied evidence.
A malformed later response preserves previously confirmed results.

### Lua transforms

`LuaTransform` accepts UTF-8 Source and optional Input. Source must return exactly
one function, called with ordinary Lua tables as `function(current, incoming)`.
Missing current or omitted Input is nil. Fields, arrays, comparisons and loops
use ordinary Lua syntax; `weir.null()` represents explicit document null.
For example, this merge can create a record on its first call:

```go
input := &weir.Document{ContentType: "application/json", Data: []byte(`{"state":"ready"}`)}
source := []byte(`return function(current, incoming)
    current = current or weir.object()
    for key, item in pairs(incoming or {}) do current[key] = item end
    return current
end`)
lua := &weir.LuaTransform{Source: source, Input: input}
request := &weir.AtomicTransformRequest{Resource: "records/s:first", Lua: lua}
options := weir.AtomicTransformOptions{StoreName: "search", Request: request}
result, err := client.AtomicTransform(ctx, options)
```

Mongo Input uses BSON. Lua source and document values have no Weir size, depth,
or node-count gate. The callback must return exactly one object to create/replace,
or `weir.keep()`, `weir.delete()` or `weir.reject(message)`. Reject returns
NOT_APPLIED with PRECONDITION_FAILED; nil, missing/multiple returns and scalar
results are errors. `weir.object()` and `weir.array()` distinguish empty containers.
Lua integers preserve 64 bits, and `weir.time.now()` returns one fixed UTC timestamp
per operation, including confirmed conflict retries. This function entry point,
return contract, helper behavior and document conversion follow the protocol's
Lua contract. See the server's
[Lua guide](https://github.com/batchstream/weir/blob/main/docs/lua.md).
Concurrent first creation can trigger a bounded fresh read/evaluation after a
confirmed conflict. An ambiguous write or commit acknowledgement never triggers
automatic replay.

### Native requests and responses

`NativeRequest` has Resource and a required Request Document. Its ContentType
identifies an adapter-owned format; Data carries opaque bytes and may be empty. The public protocol and SDK do not select or interpret backends.
A new Store can use its own content type without changing the schema or SDK.
Backend queries, counts and traversal use Native with caller-owned backend
commands, pagination and cleanup.
Native imposes no command, method, path or parameter allowlist. The backend
handles authorization and operation semantics. Resource is an optional canonical
prefix; empty addresses the backend root when supported. MongoDB accepts a
database target or an empty Resource with `$db` in its `application/bson` command
document. Search uses `application/http` and an optional resource prefix.

```go
document := &weir.Document{ContentType: "application/vnd.example.command", Data: commandBytes}
request := &weir.NativeRequest{Resource: "records", Request: document}
options := weir.NativeOptions{StoreName: "example", Request: request}
options.Consume = func(ctx context.Context, response *weir.NativeResponse, data []byte) error {
    return consumeNativeBytes(ctx, response, data)
}
result, err := client.Native(ctx, options)
```

Consume receives nonempty response chunks and finishes before the next chunk.
`NativeResult.Response` also retains metadata for an empty body when no callback
runs. Response.Metadata is an optional opaque Document;
Response.BodyContentType describes the separately streamed body. Metadata may
use a different content type from the request. Do not mutate response metadata
in a callback. The SDK hides the wire Head/Chunk/End phases.

For HTTP-backed adapters, `NewHTTPNativeRequest(resource, request)` consumes and
closes a standard `net/http.Request` body and serializes an `application/http`
Document. It emits an HTTP/1.1 origin-form target, exact Content-Length and no
default User-Agent. Methods, escaped paths, queries and application headers are
preserved. Pass an empty resource for backend-root operations. HTTP request bodies
and headers have no SDK size gate. Trailers are rejected. The Host header never
chooses the owning Store or backend destination.

```go
httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, "/_doc/example", nil)
if err != nil { return err }
request, err := weir.NewHTTPNativeRequest("records", httpRequest)
if err != nil { return err }
options := weir.NativeOptions{StoreName: "search", Request: request}
options.Consume = func(ctx context.Context, response *weir.NativeResponse, data []byte) error {
    return consumeNativeBytes(ctx, response, data)
}
result, err := client.Native(ctx, options)
```

`ParseHTTPNativeResponse(response)` optionally parses application/http metadata
containing a complete status line and CRLF header block, with no body bytes or
transfer framing. It returns HTTPNativeResponse with StatusCode and Headers;
response body bytes continue through Native's chunk consumer. These helpers are
ordinary HTTP codecs, independent of any Store type or protobuf backend fields.

NativeResult distinguishes response evidence from the Go error. A confirmed
NativeNotStarted means the backend operation did not start. NativeResponseIncomplete
means it started without a complete response and establishes no write outcome.
NativeCompletionUnconfirmed means no terminal was received; Response may still be
available. NativeResponseComplete means all response bytes arrived. Its status/body
can report a backend error such as HTTP 404 with no Go error; interpret that data
separately. A validated terminal is retained alongside a later RPC error. No Native
request is automatically replayed.

## Streaming and deadlines

All business operations use bidirectional Execute streams. Each stream addresses
one Store and one operation kind. Native sends one request, closes its input and
consumes response metadata and body chunks incrementally. Transport and backend
protocols govern representable messages and accepted data.

`ReadStream` and `MutateStream` take `Next` and `Consume` callbacks. `Next(ctx)`
returns one request, or `io.EOF` to end the finite input. `Consume(ctx, index,
result)` receives confirmed results in input order, with indexes starting at 1.
Inputs are validated as they are produced; an invalid later item does not undo
earlier requests. Stream success requires one result per submitted input and
final gRPC OK. Results delivered before a later producer, consumer or transport
error remain evidence. Next and Consume run concurrently; synchronize shared
application state. Callbacks must honor their context and return promptly;
the SDK cancels and joins its sender before returning. Keep yielded requests and
document bytes immutable until the corresponding Consume callback begins.

```go
position := 0
options := weir.ReadStreamOptions{StoreName: "search"}
options.Next = func(ctx context.Context) (*weir.ReadRequest, error) {
    if position == len(resourceNames) { return nil, io.EOF }
    request := &weir.ReadRequest{Resource: resourceNames[position]}
    position++
    return request, nil
}
options.Consume = func(ctx context.Context, index uint64, result *weir.ReadResult) error {
    if result.Failure != nil { return fmt.Errorf("read %d failed: %v", index, result.Failure) }
    if result.Missing { return nil }
    return processDocument(ctx, result.Document)
}
err := client.ReadStream(ctx, options)
```

Use streams for finite producer sequences. Consumers should commit or release
each result promptly and avoid retaining unbounded events.

Use caller deadlines. Business operations have no implicit retry or failover.
`Dial` uses gRPC's normal adaptive flow control and reusable connections. Package
operation functions accept a generated StoreService client for fixed-owner
connections and protocol fixtures; ordinary applications use Open and typed Client
methods. The caller owns target validation and connection lifetime for Dial.

## Examples and validation

[read](examples/read/main.go) performs a typed read, [basic](examples/basic/main.go)
mutates a document then reads multiple resources in one stream, and
[native](examples/native/main.go) consumes
native responses. These examples use SDK request and result types.

```sh
python3 scripts/download_modules.py
GOWORK=off GOPROXY=off GOSUMDB=off go test -race -count=1 ./...
GOWORK=off GOPROXY=off GOSUMDB=off go vet ./...
GOWORK=off GOPROXY=off GOSUMDB=off python3 scripts/check_dependencies.py
```

Default tests use owned loopback fixtures, without databases or production I/O.
CI checks typed operation dispatch, acknowledgements with transport errors,
malformed/empty native chunks, incremental consumption,
completion/cancellation, no replay, deadline admission, directory expiry/conflicts,
DNS refresh and direct Store routing. The full module graph and normal/
integration package closures must remain independent of the Weir server. Releases
require stable unreplaced dependencies and compile a separate ordinary consumer
that imports only this SDK for all business operations.

Real backend tests require an explicit tag and operator-provided fixtures:

```sh
WEIR_ADDRESS=127.0.0.1:7447 \
WEIR_MONGO_STORE=mongo \
WEIR_MONGO_RESOURCE=weir_example/records \
WEIR_SEARCH_STORE=search \
WEIR_SEARCH_RESOURCE=weir_example \
go test -race -tags integration -run 'Test(Mongo|Search)(Lifecycle|Streaming)' -v .
```

Pre-create disposable collections/indexes. Each test creates and cleans up only
its generated records. Lifecycle tests cover CRUD, backend expressions and
Native; streaming tests verify same-resource ordering through an individual
failure, Lua persistence, and repeated and missing reads. System integration tests,
backend fault tests and throughput benchmarks belong to the independent
[weir-tests](https://github.com/batchstream/weir-tests) repository.

Application and peer listeners use plaintext gRPC; restrict their network access.
