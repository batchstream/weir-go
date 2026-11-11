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
results, err := client.Read(ctx, "search", weir.WithRequests(first, second))
if err != nil { return fmt.Errorf("read RPC failed; unacknowledged results %v: %w", results, err) }
for index, result := range results {
    if result.Failure != nil { return fmt.Errorf("read %d failed: %v", index, result.Failure) }
    fmt.Println(index, "missing:", result.Missing)
}
```

## Resources and documents

Resources are canonical paths relative to their Store.
`weir.EncodeSegment` canonically encodes each decoded path segment. MongoDB paths
are `DATABASE/COLLECTION/KEY`; Search paths are `INDEX/KEY`. Mongo documents use
`application/bson` with the first `_id` matching the resource key. Search documents
use `application/json`. The caller owns its document codec; the SDK preserves bytes.

## Methods

Each method takes a context and a Store, then options:

| Method | Options | Result |
| --- | --- | --- |
| `Read` | `WithRequests`, `WithNext`, `WithReadConsume` | `[]*ReadResult`: Document, Missing, Failure |
| `Mutate` | `WithRequests`, `WithNext`, `WithMutateConsume`, `WithQueryVisible` | `[]*MutationResult` |
| `Native` | `WithChunks`, after a required `NativeRequest` | `NativeResult` |

## Read and Mutate

`WithRequests` passes the records a call already holds, and `WithNext` a callback
that produces more, as described under
[Streaming and deadlines](streaming.md#withnext-and-consume-callbacks). The call
sends the requests first, then what Next produces, and needs at least one record.
Several `WithRequests` add their requests in order; a later `WithNext` or consume
callback replaces an earlier one.

A call validates every request of `WithRequests` before sending anything and
before calling Next. A nil request, malformed resource, unsupported action or
invalid document envelope rejects the entire call without sending earlier valid
items. Every resource is relative to the call's Store. Each input is sent as one
ExecuteRequest as soon as the window and an earlier mutation of its resource allow.

Without a consume callback, results return in input order, one per input,
including repeated resources. Individual backend failures stay in their result
positions. A later RPC failure preserves every validated result already received;
unconfirmed positions remain nil and any such mutation may have applied. Never
automatically replay those mutations.

```go
firstDoc := &weir.Document{ContentType: "application/json", Data: []byte(`{"n":1}`)}
secondDoc := &weir.Document{ContentType: "application/json", Data: []byte(`{"n":2}`)}
first := &weir.MutateRequest{Resource: "records/s:first", Action: weir.MutationPut, Document: firstDoc}
second := &weir.MutateRequest{Resource: "records/s:second", Action: weir.MutationPut, Document: secondDoc}
results, err := client.Mutate(ctx, "search", weir.WithRequests(first, second))
// Inspect every result's Outcome/Failure, even when err is nonnil. Never replay
// an unacknowledged mutation automatically.
```

Keep requests and their document bytes unchanged until the call returns. After a
call whose context ended, do not reuse them at all: a single record's shared
stream may still be reading them.

## Actions and ordering

`MutateRequest.Action` selects `MutationCreate`, `MutationPut`, `MutationReplace`,
`MutationDelete` or `MutationAtomicTransform`. Create requires absence, Replace
requires an existing resource and Put creates or replaces; all three require
Document. Delete accepts no payload, and AtomicTransform requires exactly one Lua
or BackendExpression. A call is not a transaction. Mutations to the same resource
execute in input order, including after an item failure; different resources may
run concurrently. Across RPCs, ordering follows the database semantics.

## Call size and flow control

A logical call has no total byte or item limit. Each ExecuteRequest carries one
read or mutation and each response carries its indexed result. Backend protocols determine accepted document sizes. The SDK permits at most 32 unconsumed results and 8 MiB of
unacknowledged encoded input; a larger legal request uses the window alone.
Credit returns after each Consume callback finishes. A size-dependent wait can
hold one produced item that has not yet been sent. Sending and receiving run
concurrently so HTTP/2 flow control advances in both directions. A call with
`WithNext` never waits for another input or EOF before sending the current item.
Without a consume callback a call accumulates results for its caller; use `WithNext`
and a consume callback to avoid retaining the full input and result sequence.

## Visibility

Without `WithQueryVisible` a mutation waits for application acknowledgement. Pass
`WithQueryVisible` when subsequent normal queries must observe the mutations of a
call before they succeed; it applies to every mutation of that call.
An APPLIED result without Failure confirms that guarantee; APPLIED with Failure
confirms application only and must not be replayed automatically. This includes
Lua keep and missing-document deletes. Search adapters wait for refresh; backends
without deferred query visibility satisfy this when the mutation is acknowledged.

```go
request := &weir.MutateRequest{Resource: "records/s:first", Action: weir.MutationPut, Document: document}
results, err := client.Mutate(ctx, "search", weir.WithRequests(request), weir.WithQueryVisible())
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

## Result errors

These turn results into Go errors, so a caller can return them with `err`:

| Function | Returns nil for | Otherwise |
| --- | --- | --- |
| `MutationErr(result)` | APPLIED without a Failure | `*Error` with Outcome and Failure |
| `result.Err()` on a `ReadResult` | A document or a confirmed missing record | `*Error` with the Failure |
| `result.Err()` on a `NativeResult` | A complete response, whatever its own status | `*Error` with the Failure, if any |

```go
results, err := client.Mutate(ctx, "search", weir.WithRequests(request))
if err != nil { return err }
if err := weir.MutationErr(results[0]); err != nil {
    if weir.CodeOf(err) == weir.FailurePreconditionFailed {
        // The record already exists.
    }
    return err
}
```

`CodeOf(err)` returns the FailureCode of an `*Error` anywhere in the chain, or
zero. `errors.Is(err, ErrInterrupted)` reports a request an outage cut short after
it may have begun: a lost connection, a mutation UNKNOWN with UNAVAILABLE, or a
Native request that began and ended with UNAVAILABLE or without its end. Send it
again only when repeating it is safe. `ErrNotFound` is what the
[mongodb](mongodb.md) and [search](search.md) packages return for a missing
record.

## Response validation

Requests and responses reject unknown protobuf fields recursively. FailureCode
accepts only declared nonzero values. The SDK validates required result variants,
completion evidence, document envelopes and indexes. A Failure leads to a
resend only as described under
[Resending records](streaming.md#resending-records), and never changes
confirmed MutationApplied evidence.
A malformed later response preserves previously confirmed results.
