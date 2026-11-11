# Search package

How to read and write Elasticsearch and OpenSearch documents with
`github.com/batchstream/weir-go/search`.

## Setup

A `Cluster` names a Weir Store whose backend is search. An `Index` adds an index
and the Go type of its documents. Both are plain values: build them once, copy
and share them freely.

```go
client, err := weir.New("127.0.0.1:7447")
if err != nil { return err }
defer client.Close()

cluster := search.Cluster{Client: client, Store: "search"}
products := search.Index[Product]{Cluster: cluster, Name: "products"}
```

| Field | Meaning |
| --- | --- |
| `Cluster.Store` | Name of a Weir Store whose backend is search |
| `Index.Name` | An index, or an alias of one index. It must exist before its first write. |
| `Index.QueryVisible` | Each write returns only once later searches see it |

Document IDs are strings. Documents are encoded with `encoding/json`.

## Records

| Method | Effect | Failure code when it does not apply |
| --- | --- | --- |
| `Get(ctx, id)` | Returns the document, or `weir.ErrNotFound` | |
| `GetMany(ctx, ids...)` | Returns the documents in one call, in order, nil for a missing one | |
| `Create(ctx, id, doc)` | Writes a document that must not exist | `FailurePreconditionFailed` |
| `Put(ctx, id, doc)` | Creates or replaces the document | |
| `Replace(ctx, id, doc)` | Replaces a document that must exist | `FailurePreconditionFailed` |
| `Delete(ctx, id)` | Deletes the document; a missing one succeeds | |
| `Update(ctx, id, fields)` | Merges fields into a document that must exist | `FailurePreconditionFailed` |
| `Transform(ctx, id, program, input)` | Runs a Lua program atomically against the document | `FailurePreconditionFailed` for `weir.reject` |

```go
lamp := &Product{Name: "Lamp", Price: 30}
err := products.Put(ctx, "p1", lamp)

price := map[string]any{"price": 25}
err = products.Update(ctx, "p1", price)

product, err := products.Get(ctx, "p1")
if errors.Is(err, weir.ErrNotFound) {
    // No such product.
}
```

`Transform` sends `input` as JSON, or no input when it is nil. See [Lua](lua.md)
for what the program may return.

## Searches

`Search` sends a search request body to the index and returns its hits. Each
`Hit` has the document's `ID`, `Score`, `Sort` values and the decoded `Document`.
`Sort` keeps each value's exact JSON, so pass it back as `search_after` to read
the next page. A status of 300 or more is a `*search.StatusError`.

```go
query := map[string]any{
    "query": map[string]any{"match": map[string]any{"name": "lamp"}},
    "sort":  []any{map[string]any{"price": "asc"}, map[string]any{"_id": "asc"}},
    "size":  100,
}
hits, err := products.Search(ctx, query)
if err != nil { return err }
for _, hit := range hits {
    fmt.Println(hit.ID, hit.Document.Name)
}
if len(hits) > 0 {
    query["search_after"] = hits[len(hits)-1].Sort
}
```

## HTTP requests

`Index.Do` sends any HTTP request with a path relative to the index, such as
`/_count` or `/_refresh`, and `Cluster.Do` one with a path from the cluster root,
such as `/_cluster/health`. A path may carry a query string. A `[]byte` or
`json.RawMessage` body is sent as it is, so it suits `_bulk`; any other body is
sent as JSON. The response comes back whatever its status: check `StatusCode`.

```go
count := map[string]any{"query": map[string]any{"term": map[string]any{"name": "lamp"}}}
response, err := products.Do(ctx, http.MethodPost, "/_count", count)
if err != nil { return err }
if response.StatusCode != http.StatusOK {
    return fmt.Errorf("count failed: %s", response.Body)
}
```

For headers of your own or a response body read in chunks, use
`weir.NewHTTPNativeRequest` and `Client.Native`; see [Native](native.md).

Weir never sends a request again on its own. A request a lost connection or an
unavailable backend cut short reports `weir.ErrInterrupted`, and may have run.
Send it again only when that is safe, as it is for a search.

## Errors

| Error | Meaning |
| --- | --- |
| `weir.ErrNotFound` | `Get` found no document |
| `*weir.Error` | A record failed. `weir.CodeOf(err)` gives its `FailureCode`, and `Outcome` tells whether a write applied. |
| `weir.ErrInterrupted` | The request may have run. Never send a write again without checking. |
| `*search.StatusError` | `Search` got a status of 300 or more |

The Client already sent again every write a server refused before it began and
every write that lost to concurrent writers of its record; see
[Streaming](streaming.md#resending-records).
