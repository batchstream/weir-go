# Native requests and responses

How to send a backend's own commands, such as queries, scans and counts.

## Requests

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
result, err := client.Native(ctx, "example", request)
// result.Body holds the response body, as far as it arrived.
```

## Responses

`NativeResult.Body` holds the response body, as far as it arrived. To avoid
retaining a large one, pass `WithChunks`: its callback receives nonempty response
chunks and finishes before the next chunk, and Body stays nil.
`NativeResult.Response` also retains metadata for an empty body when no callback
runs. Response.Metadata is an optional opaque Document;
Response.BodyContentType describes the separately streamed body. Metadata may
use a different content type from the request. Do not mutate response metadata
in a callback. The SDK hides the wire Head/Chunk/End phases.

## HTTP helpers

For HTTP-backed adapters, `NewHTTPNativeRequest(resource, request)` consumes and
closes a standard `net/http.Request` body and serializes an `application/http`
Document. It emits an HTTP/1.1 origin-form target, exact Content-Length and no
default User-Agent. Methods, escaped paths, queries and application headers are
preserved. Pass an empty resource for backend-root operations. HTTP request bodies
and headers have no SDK size gate. Trailers are rejected. The Host header never
chooses the Store or backend destination.

```go
httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, "/_doc/example", nil)
if err != nil { return err }
request, err := weir.NewHTTPNativeRequest("records", httpRequest)
if err != nil { return err }
consume := func(ctx context.Context, response *weir.NativeResponse, data []byte) error {
    return consumeNativeBytes(ctx, response, data)
}
result, err := client.Native(ctx, "search", request, weir.WithChunks(consume))
```

`ParseHTTPNativeResponse(response)` optionally parses application/http metadata
containing a complete status line and CRLF header block, with no body bytes or
transfer framing. It returns HTTPNativeResponse with StatusCode and Headers;
response body bytes stay in `Body` or go to the `WithChunks` callback. These helpers are
ordinary HTTP codecs, independent of any Store type or protobuf backend fields.

## Completion evidence

NativeResult distinguishes response evidence from the Go error. A confirmed
NativeNotStarted means the backend operation did not start. NativeResponseIncomplete
means it started without a complete response and establishes no write outcome.
NativeCompletionUnconfirmed means no terminal was received; Response may still be
available. NativeResponseComplete means all response bytes arrived. Its status/body
can report a backend error such as HTTP 404 with no Go error; interpret that data
separately. A validated terminal is retained alongside a later RPC error. A
Client sends a Native request again only after a refused NativeNotStarted without
a Response.
