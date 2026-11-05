# Lua transforms

How to change a record with a Lua function through `MutationAtomicTransform`.

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
request := &weir.MutateRequest{Resource: "records/s:first", Action: weir.MutationAtomicTransform, Lua: lua}
results, err := client.Mutate(ctx, "search", weir.WithRequests(request))
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
