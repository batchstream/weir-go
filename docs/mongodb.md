# MongoDB package

How to read and write MongoDB documents with `github.com/batchstream/weir-go/mongodb`.

## Setup

A `DB` names a database behind a Weir Store whose backend is MongoDB. A
`Collection` adds a collection, the Go type of its documents and the type of
their `_id`. Both are plain values: build them once, copy and share them freely.

```go
client, err := weir.New("127.0.0.1:7447")
if err != nil { return err }
defer client.Close()

db := mongodb.DB{Client: client, Store: "mongo", Name: "shop"}
users := mongodb.Collection[string, User]{DB: db, Name: "users"}
```

| Field | Meaning |
| --- | --- |
| `DB.Store` | Name of a Weir Store whose backend is MongoDB |
| `DB.Name` | Database name |
| `Collection.Name` | Collection name. The collection must exist before its first write. |
| `Collection.QueryVisible` | Each write returns only once later queries see it |

The `_id` type is a string, an `int`, `int32` or `int64`, or a `bson.ObjectID`,
or a type defined on a string or integer. Documents are encoded with the
MongoDB Go driver's `bson` package.

## Records

| Method | Effect | Failure code when it does not apply |
| --- | --- | --- |
| `Get(ctx, id)` | Returns the document, or `weir.ErrNotFound` | |
| `GetMany(ctx, ids...)` | Returns the documents in one call, in order, nil for a missing one | |
| `Create(ctx, id, doc)` | Writes a document that must not exist | `FailurePreconditionFailed`, also for a unique index |
| `Put(ctx, id, doc)` | Creates or replaces the document | |
| `Replace(ctx, id, doc)` | Replaces a document that must exist | `FailurePreconditionFailed` |
| `Delete(ctx, id)` | Deletes the document; a missing one succeeds | |
| `Update(ctx, id, update)` | Applies `$set`, `$unset` and `$inc` to a document that must exist | `FailurePreconditionFailed` |
| `Transform(ctx, id, program, input)` | Runs a Lua program atomically against the document | `FailurePreconditionFailed` for `weir.reject` |

Writes take the `_id` separately. A document may leave out its `_id` field; one
it has must equal the id. The package puts the `_id` first, as Weir requires.

```go
ada := &User{Name: "Ada"}
err := users.Create(ctx, "ada", ada)
if weir.CodeOf(err) == weir.FailurePreconditionFailed {
    // Ada already exists.
}

visits := bson.D{{Key: "$inc", Value: bson.D{{Key: "visits", Value: 1}}}}
err = users.Update(ctx, "ada", visits)

user, err := users.Get(ctx, "ada")
if errors.Is(err, weir.ErrNotFound) {
    // No such user.
}
```

`Transform` sends `input` as BSON, or no input when it is nil. The program
returns one function, called as `function(current, incoming)`. See
[Lua](lua.md) for what it may return.

```go
const rename = `return function(current, incoming)
    if current == nil then return weir.reject("no such user") end
    current.name = incoming.name
    return current
end`
name := bson.D{{Key: "name", Value: "Ada L."}}
err := users.Transform(ctx, "ada", rename, name)
```

Concurrent calls on one Client share its streams, so many goroutines writing
single records still reach Weir in batches.

## Queries

`Find` yields the documents that match a filter, nil for all. Further options are
fields of MongoDB's `find` command, such as `sort`, `projection`, `skip`, `limit`
and `batchSize`. It follows the cursor with `getMore`, and kills the cursor when
the loop stops early. An error ends the loop.

```go
active := bson.D{{Key: "active", Value: true}}
newest := bson.E{Key: "sort", Value: bson.D{{Key: "_id", Value: -1}}}
for user, err := range users.Find(ctx, active, newest) {
    if err != nil { return err }
    fmt.Println(user.Name)
}
```

`Count` returns the number of matching documents. It counts them; for a cheap
estimate of a whole collection, run the `count` command with `DB.Command`.

## Commands

`DB.Command` runs any command in the database and returns its reply. A reply with
`ok: 0`, `writeErrors` or a `writeConcernError` comes back together with a
`*CommandError` holding its code, code name and message. `DB.Cursor` runs a
command that opens a cursor, such as `aggregate` or `listIndexes`, and yields its
documents the way `Find` does.

```go
create := bson.D{{Key: "create", Value: "users"}}
reply, err := db.Command(ctx, create)
var failed *mongodb.CommandError
if errors.As(err, &failed) && failed.Name == "NamespaceExists" {
    // The collection already exists.
}

pipeline := bson.A{bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$country"}}}}}
aggregate := bson.D{{Key: "aggregate", Value: "users"}, {Key: "pipeline", Value: pipeline}, {Key: "cursor", Value: bson.D{}}}
for document, err := range db.Cursor(ctx, aggregate) {
    if err != nil { return err }
    fmt.Println(document)
}
```

Weir never sends a command again on its own. A command a lost connection or an
unavailable backend cut short reports `weir.ErrInterrupted`, and may have run.
Send it again only when that is safe, as it is for a read.

## Errors

| Error | Meaning |
| --- | --- |
| `weir.ErrNotFound` | `Get` found no document |
| `*weir.Error` | A record failed. `weir.CodeOf(err)` gives its `FailureCode`, and `Outcome` tells whether a write applied. |
| `weir.ErrInterrupted` | The request may have run. Never send a write again without checking. |
| `*mongodb.CommandError` | MongoDB rejected a command or one of its writes |

The Client already sent again every write a server refused before it began and
every write that lost to concurrent writers of its record; see
[Streaming](streaming.md#resending-records).
