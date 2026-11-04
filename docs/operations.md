# Business operations

How to read and write records, and how to interpret results.

## Connect and read

Import only `weir "github.com/batchstream/weir-go"` for ordinary business calls:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
client, err := weir.New("127.0.0.1:7447")
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

## Resources and documents

Resources are canonical paths relative to `StoreName`.
`weir.EncodeSegment` canonically encodes each decoded path segment. MongoDB paths
are `DATABASE/COLLECTION/KEY`; Search paths are `INDEX/KEY`. Mongo documents use
`application/bson` with the first `_id` matching the resource key. Search documents
use `application/json`. The caller owns its document codec; the SDK preserves bytes.

## Methods

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

## Read and Mutate

`Read` and `Mutate` validate the complete input before sending anything. An empty
input, nil request, malformed resource, unsupported action or invalid document
envelope rejects the entire call without sending earlier valid items. Every
resource is relative to one `StoreName`. Each input is sent as one
ExecuteRequest as soon as the window and an earlier mutation of its resource allow.
Results arrive as records complete and are returned in input order, including
repeated resources. Individual backend failures stay in their result positions. A later
RPC failure preserves every validated result already received; unconfirmed
positions remain nil and any such mutation may have applied. Never
automatically replay those mutations.

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

## Actions and ordering

`MutateRequest.Action` selects `MutationCreate`, `MutationPut`, `MutationReplace`,
`MutationDelete` or `MutationAtomicTransform`. Writes require Document, Delete
accepts no payload, and AtomicTransform requires exactly one Lua or
BackendExpression. A batch is not a transaction. Mutations to the same resource
execute in input order, including after an item failure; different resources may
run concurrently. Across RPCs, ordering follows the database semantics.

## Call size and flow control

A logical call has no total byte or item limit. Each ExecuteRequest carries one
read or mutation and each response carries its indexed result. Backend protocols determine accepted document sizes. The SDK permits at most 32 unconsumed results and 8 MiB of
unacknowledged encoded input; a larger legal request uses the window alone.
Credit returns after each Consume callback finishes. A size-dependent wait can
hold one produced item that has not yet been sent. Sending and receiving run
concurrently so HTTP/2 flow control advances in both directions. The SDK never
waits for another input or EOF before sending the current item.
`Read` and `Mutate` intentionally accumulate results for their caller; use the
streaming interfaces to avoid retaining the full input and result sequence.

## Write requests and visibility

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

## Read results

`ReadResult` contains exactly one of Document, Missing, or Failure. Missing is a
successful read that confirmed document absence. `FailureTargetNotFound` means a
required backend collection or index is missing. A failure does not establish
that the requested document is absent. Individual Read/Mutate failures are result
data, even when the Go error is nil; inspect every result as well as the error.

## Mutation outcomes

`MutationApplied` means the requested semantics were satisfied. It includes a
successful Lua keep and deleting an already absent document without a database
write. APPLIED may carry a subsequent acknowledgement Failure. Other outcomes
always carry a Failure: NOT_STARTED means no backend mutation began, NOT_APPLIED
means absence of application was established, and UNKNOWN means available evidence
cannot establish whether it applied. A later RPC failure does not revoke any
confirmed item. Unconfirmed nil positions may have applied; reconcile them through
application knowledge and never automatically replay writes.

## Response validation

Requests and responses reject unknown protobuf fields recursively. FailureCode
accepts only declared nonzero values. The SDK validates required result variants,
completion evidence, document envelopes and indexes. A Failure leads to a
resend only as described under
[Resending refused records](streaming.md#resending-refused-records), and never changes
confirmed MutationApplied evidence.
A malformed later response preserves previously confirmed results.
