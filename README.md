# Weir Go SDK

Go client for the [Weir bounded record data plane](https://github.com/batchstream/weir).
Requires Go 1.27.1. The SDK supports all five RPCs: `Read`, `Mutate`, mixed `Bulk`,
server-streaming `Scan`, and duplex `Native`.

```sh
go get github.com/batchstream/weir-go@v0.1.0
```

The first release is being qualified together with Weir v0.1.0. Until that tag is
published, install an explicitly reviewed commit. See `go.mod` for the exact
server protocol revision. The module imports the server's generated public
`api/weir/v1` and `api/weir/search/v1` packages; it does not duplicate descriptors,
generate a second protocol, or depend on server internals. Patch updates preserve
the SDK API and wire semantics; breaking changes before v1 increment the minor
version. There are no local `replace` directives.

## Use

```go
package main

import (
    "context"
    "fmt"
    "time"

    weir "github.com/batchstream/weir-go"
    pb "github.com/batchstream/weir/api/weir/v1"
)

func main() {
    // Weir currently serves plaintext gRPC. Restrict it to an isolated network.
    // Zero-value options instead use verified TLS for a TLS-terminating proxy.
    options := weir.Options{Plaintext: true}
    client, err := weir.New("127.0.0.1:7447", options)
    if err != nil { panic(err) }
    defer client.Close()

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    resource, err := weir.Resource("search", "records", "s:order/42")
    if err != nil { panic(err) }
    request := &pb.ReadRequest{Resource: resource}
    result, err := client.Read(ctx, request)
    if err != nil { panic(err) }
    fmt.Println("missing:", result.GetMissing() != nil)
}
```

`Resource` takes decoded segments and produces canonical percent encoding.
MongoDB addresses are `weir://STORE/DATABASE/COLLECTION/KEY`; Search addresses
are `weir://STORE/INDEX/KEY`. `s:ID`, canonical `i:INTEGER`, and `oid:OBJECTID`
key support depends on the backend. The server validates backend names and keys.
MongoDB documents are opaque BSON and require the first `_id` to match the URI.
Search documents are opaque JSON. The client never re-encodes documents.

Build a mutation using the public protobuf oneof:

```go
document := &pb.Document{MediaType: weir.MediaTypeJSON, Data: []byte(`{"n":1}`)}
action := &pb.MutateRequest_Create{Create: document}
request := &pb.MutateRequest{Resource: resource, Action: action}
result, err := client.Mutate(ctx, request)
// Always retain result.Outcome, even when err != nil.
```

Put, Create, Replace, Delete and qualified backend-expression AtomicTransform
use their corresponding protobuf action. ProgramTransform is unsupported.
Missing reads and successful no-op mutations are ordinary successful results.
`FailureError` preserves the server's code/message; `MutationError` also carries
the outcome. Use `errors.As` to inspect these errors. A failure may accompany an
APPLIED result: the SDK preserves that positive acknowledgement.

## Completion and retry rules

The client never retries an operation, starts another endpoint attempt, or restarts
a stream. gRPC configured retries, service config, and retry buffering are disabled.
Calls have a default 30-second timeout, shortened by the caller's context deadline.
Set a positive `Options.Timeout` to change that ceiling. Connection establishment
is lazy; `New` does not prove a server is reachable.

- A mutation transport error or malformed response returns `UNKNOWN` plus an error.
  Reconcile through application knowledge; never automatically replay it. Local
  validation returns `NOT_STARTED`. A valid server result keeps its outcome.
- `Bulk(ctx, "weir://STORE", operations)` validates all input envelopes before any
  RPC. It assigns consecutive indexes and returns a slice in input order. Each
  operation has exactly one `Read` or `Mutate`. Backend semantics are validated
  per record on the server; Bulk is not a transaction.
- Bulk requires every unique result, matching received/result counts, an End,
  and final gRPC OK. A `BatchError` exposes indexed failures and a stream `Cause`.
  On interruption, valid results remain available and unreported entries are nil.
  Treat unreported mutations as UNKNOWN; the client cannot prove non-application.
- Scan invokes a document callback and validates End document count, optional
  failure and final gRPC OK. MongoDB selectors/results are native BSON. Search
  selectors are JSON; Scan documents are native hits including metadata, whereas
  Read returns `_source`. Search visibility follows the backend refresh interval.
- Native takes a `NativeRequest` with an Open descriptor and an in-memory body,
  sends chunks concurrently with receiving, and invokes a byte callback. It checks
  Head ordering, End completion and final gRPC OK. `NativeResult` is transport
  evidence only: `RESPONSE_COMPLETE` can contain a database error. Interpret the
  adapter's metadata/body. An incomplete response never proves a write failed.

Scan/Native callbacks are synchronous and must return promptly. Return an error
to cancel consumption. Consumed data is partial until the method succeeds. The SDK
cancels and joins its sender on every exit; it does not launch goroutines that read
from arbitrary user Readers. Request buffers must not be mutated until return.
The Client can be shared across goroutines; close it after users have finished.

## Bounds and deployment

Documents are at most 256 KiB; each gRPC frame is at most 300 KiB. A Bulk call
accepts at most 1,024 operations and 8 MiB encoded input, and retains at most
8 MiB encoded results. Exceeding the result budget returns partial evidence and
an error; choose smaller batches for large records. Calls are never automatically
split because cross-stream ordering would change. Native uploads are at most
8 MiB with 64 KiB chunks; Scan/Native responses are processed incrementally.
Backend-specific bounds may be smaller. Selectors and transform expressions
are at most 16 KiB. These bounds are qualified against the pinned protocol revision.

Current Weir application/peer listeners have no built-in authentication or TLS.
Use `Plaintext: true` only on a restricted private network, with the chart's
NetworkPolicy enforced. Never target the peer listener from this SDK. TLS options
support a verifying TLS proxy with cloned caller-supplied roots and client identity;
`InsecureSkipVerify` is rejected. The SDK reads no credential or configuration files.
No automatic production endpoint discovery, peer routing metadata, backend
provisioning, transaction API, or queueing is included.

## Validation

```sh
go mod download
GOPROXY=off GOSUMDB=off go test -race -count=1 ./...
GOPROXY=off GOSUMDB=off go vet ./...
```

Default tests use local gRPC fixtures (in-memory protocol tests and a loopback
workload failure test) and never start databases or contact external servers.
Tests cover malformed/truncated/duplicate/out-of-order results, final
non-OK status after End, partial batch evidence, cancellation, blocked Native
upload cleanup, prevalidation and no replay.

Real integration is an explicit opt-in and writes unique temporary records only:

```sh
WEIR_ADDRESS=127.0.0.1:7447 \
WEIR_MONGO_RESOURCE=weir://mongo/weir_acceptance/records \
WEIR_SEARCH_RESOURCE=weir://search/weir_acceptance \
go test -race -tags integration -run 'Test(Mongo|Search)Lifecycle' -v ./...
```

Provision the database/collection and concrete Search index beforehand. Each
enabled backend runs Create, duplicate-precondition, Replace, Put, AtomicTransform,
Read, ordered mixed Bulk, Scan, Native and Delete through the SDK. Omit either
resource variable to skip that backend. Only target a dedicated test dataset.
These tests are compatible with an installed Helm release through port-forward.

The [explicit stability workload](examples/soak/README.md) schedules bounded
record cycles, audits acknowledged values and mutation uncertainty, and emits
machine-readable progress/final evidence for a separately provisioned acceptance
Job. It never starts as part of default tests. Infrastructure observation and
persistent evidence storage remain part of the deployment qualification.

This repository follows the upstream project's current licensing status; no new
license grant is introduced here.
