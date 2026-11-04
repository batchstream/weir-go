# Streaming and deadlines

How the SDK uses gRPC streams, and when it sends a record again.

## Execute streams

All business operations use bidirectional Execute streams. Each stream addresses
one Store and one operation kind. Native sends one request, closes its input and
consumes response metadata and body chunks incrementally. Transport and backend
protocols govern representable messages and accepted data.

## ReadStream and MutateStream

`ReadStream` and `MutateStream` take `Next` and `Consume` callbacks. `Next(ctx)`
returns one request, or `io.EOF` to end the finite input. `Consume(ctx, index,
result)` receives each confirmed result as its record completes; the index,
starting at 1, identifies the input.
Inputs are validated as they are produced; an invalid later item does not undo
earlier requests. A Client's stream succeeds with one result per submitted input;
a package function's also requires a final gRPC OK. Results delivered before a
later producer, consumer or transport error remain evidence, and a Client's stream
goes on delivering results it already received after such an error, unless
Consume itself failed. Next and Consume run concurrently; synchronize shared
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

Consumers should commit or release each result promptly and avoid retaining
unbounded events.

## Read, Mutate, ReadStream and MutateStream

A Client's `Read`, `Mutate`, `ReadStream` and `MutateStream` each send their
records over a gRPC stream of their own, and replace it with one to another server
when its server stops resolving or refuses a record, and every minute, so a
call may stay open for as long as its producer has records, and an idle one holds
no gRPC stream after 10 seconds. A record over 1 MiB uses a further gRPC stream of
its own. `Read` and `Mutate` send the records that fit their window of 32 records
and 8 MiB together, in messages of up to 1 MiB. A stream's mutations of one resource run in input order,
each after the previous one's result, including after a failure; records of
different resources may arrive at the server in any order. A broken gRPC stream
ends the call with its error, and mutations without a result may have applied.
The package operation functions keep one gRPC stream for the whole call, end
with an error when its server goes away and leave following servers to their
caller.

## Shared streams of single-record calls

The single-record Client methods (`ReadOne`, `Create`, `Put`, `Replace`,
`Delete` and `AtomicTransform`) share long-lived streams with concurrent calls
of the same Client, so many goroutines in one process pay for a few gRPC streams
instead of one per call. A stream sends the records queued on it together in one
message of up to 1 MiB, and the server returns results that complete together
in one message, so a busy stream pays for a message per burst rather than per
record. Each Store and operation kind takes new records on at
most 32 streams of 32 outstanding records; another stream opens once every
stream is half full, and calls beyond that capacity, or with a record over 1
MiB, use a stream of their own. With fewer concurrent calls, one stream, and so
one server, carries a Store's calls until it retires. Results return as each
record completes, so a slow call does not hold back the others. A record the
server rejects fails alone, but a broken stream fails every record outstanding
on it. A call's deadline travels with its record, so the server drops it if it
is still queued when the deadline passes. Canceling a call stops waiting and
asks the server to cancel only that record; as with any canceled call, a record
already executing may still take effect. When the server refuses a record
without starting it, as a draining or overloaded server or an unavailable
backend does, the refusal returns to the caller. A server that turns a new gRPC
stream away before reading it, as a draining or overloaded one does, says so with
a status detail, and that refuses the stream's records alike. For 10 seconds afterwards the
stream takes new records only when no other stream can, and while another stream
does, it closes as soon as it falls quiet, so new records move to other servers
and a draining server's streams end. When every stream of a Store has refused,
a fresh stream looks for a server that is not refusing; once that stream is
refused too, as when the backend is unavailable everywhere, the refusing streams
keep carrying calls and the next fresh stream opens 10 seconds later.
Shared streams do not move to a server that refused a record, or rejected a
stream before answering one, within those 10 seconds. Streams retire after a minute, which lets the server's connection age
rebalance clients behind one virtual IP, and idle streams close after 10
seconds. Shared streams do not carry outgoing gRPC metadata from a call's
context; package operation functions keep one stream per call and send it.

## Dial and package functions

`Dial` uses gRPC's normal adaptive flow control and reusable connections. Package
operation functions accept a generated StoreService client for custom
connections and protocol fixtures; ordinary applications use New and typed Client
methods. `Dial` accepts any gRPC target; the caller owns its validation and the
connection lifetime.

## Resending refused records

A Client sends a record again when a server refused it before it began: a
`MutationNotStarted` mutation, a read Failure, or a `NativeNotStarted` without a
Response, whose Failure code is `FailureUnavailable`. A replica draining for a
rolling restart, a replica under memory pressure and an unavailable backend
refuse this way. Such a record took no effect, so sending it again cannot apply
it twice. `FailureResourceExhausted` reports that the request exceeded a budget
of its own, such as a result over the message size limit, which a resend would
exceed again. Every other result, including UNKNOWN, NOT_APPLIED, APPLIED with a
Failure and nil positions, and every Go error reach the caller unchanged.

A record is sent at most four times. The wait before each resend is jittered
between half and all of 50 ms, 100 ms and then 200 ms. A resend is made only
when the context deadline leaves that longest wait again for the send after it.
A resend that still outlasts the deadline ends with the deadline error, and that
record may have taken effect. Resends across a Client stay within one per ten
first sends plus a burst of 100, so a server that refuses everything never
receives several times its normal traffic. A record still refused at the end
keeps its NOT_STARTED evidence, and later mutations of its resource in the same
call still run after it. The caller may send it again, unless such a mutation
did not end NOT_STARTED; sending it then would apply it after that mutation. The
package operation functions never resend. Use caller deadlines; they bound
resends as well as calls.
