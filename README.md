# Weir Go SDK

[![CI](https://github.com/batchstream/weir-go/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/batchstream/weir-go/actions/workflows/ci.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/batchstream/weir-go.svg)](https://pkg.go.dev/github.com/batchstream/weir-go) [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Typed Go client for [Weir](https://github.com/batchstream/weir).

## Overview

### Packages

| Package | Use |
| --- | --- |
| `github.com/batchstream/weir-go` | The client: `Read`, `Mutate` and `Native` for any Store |
| `github.com/batchstream/weir-go/mongodb` | Typed MongoDB collections, queries and commands |
| `github.com/batchstream/weir-go/search` | Typed Elasticsearch and OpenSearch indexes, searches and HTTP requests |

### Terms

| Term | Meaning |
| --- | --- |
| Store | A named backend connection on the Weir server. Every request names a Store. |
| Resource | The path of one record in a Store. |

### Methods

| Method | Use |
| --- | --- |
| `Read(ctx, store, options...)` | Read records. Results return in input order. |
| `Mutate(ctx, store, options...)` | Write records. Not a transaction. |
| `Native(ctx, store, request, options...)` | Send a backend's own command. Use it for queries, scans and counts. |

### Options

| Option | Applies to | Effect |
| --- | --- | --- |
| `WithDialOptions` | `New` | gRPC options for the connection, such as an interceptor or a custom dialer. |
| `WithRequests` | `Read`, `Mutate` | The records to send. |
| `WithNext` | `Read`, `Mutate` | A callback that produces more records, after those of `WithRequests`. |
| `WithReadConsume`, `WithMutateConsume` | `Read`, `Mutate` | A callback that receives each result as it arrives, instead of the call returning them. |
| `WithQueryVisible` | `Mutate` | Wait until normal queries can see the mutations. |
| `WithChunks` | `Native` | A callback that receives the response body in chunks, instead of `Body` holding it. |

### Mutation actions

| Action | Use |
| --- | --- |
| `MutationCreate` | Write a record that must not exist. |
| `MutationReplace` | Write a record that must exist. |
| `MutationPut` | Create or replace a record. |
| `MutationDelete` | Delete a record. |
| `MutationAtomicTransform` | Change a record with a Lua function or a backend expression. |

### Backend formats

| Backend | Resource | Document content type |
| --- | --- | --- |
| MongoDB | `database/collection/s:key` | `application/bson`. The first field is an `_id` that matches the key. |
| Elasticsearch, OpenSearch | `index/s:key` | `application/json` |

MongoDB keys can also use `i:` for a 64-bit integer or `oid:` for an ObjectId in
lowercase hex. `weir.EncodeSegment` encodes a path segment. The `weir` package does
not encode documents; your code owns the document bytes. The `mongodb` and `search`
packages build resources and encode documents for you.

### Mutation outcomes

| Outcome | Meaning |
| --- | --- |
| `MutationApplied` | The requested semantics were satisfied, including a no-op such as deleting an absent record. It can still carry a Failure from a later acknowledgement. |
| `MutationNotStarted` | No backend mutation began. |
| `MutationNotApplied` | The mutation is known not to have applied. |
| `MutationUnknown` | The mutation may or may not have applied. |

### Rules

1. Create one Client and reuse it across goroutines. Close it after callers
   finish.
2. Set a context deadline on every call.
3. Check the Go error and every result. A record can fail while the Go error is
   nil. `MutationErr`, `ReadResult.Err` and `NativeResult.Err` turn a result into
   an error.
4. Never automatically resend a mutation that is `MutationApplied` or
   `MutationUnknown`, or that has no result. It has applied or may have applied.
   Reconcile it first.
5. A write may not be visible to the next Search query. Pass
   `WithQueryVisible` when it must be.
6. The application listener uses plaintext gRPC. Restrict its network access.

## Requirements

| Item | Requirement |
| --- | --- |
| Go | 1.27.4 |
| Weir server | Reachable at a `host:port` address. In Kubernetes, use a headless Service. |
| Collections and indices | Must exist before typed reads and mutations. `Native` can create them. |
| Docker | For the quick start only |

## Quick start

This writes one record to MongoDB through Weir and reads it back.

**Step 1.** Start MongoDB and Weir with steps 1 to 4 of the
[Weir quick start](https://github.com/batchstream/weir#quick-start).

**Step 2.** In a second terminal, create a module and add the SDK.

```sh
mkdir hello
cd hello
go mod init example.com/hello
go get github.com/batchstream/weir-go@v0.25.0 go.mongodb.org/mongo-driver/v2/bson
```

**Step 3.** Save this as `main.go`.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	weir "github.com/batchstream/weir-go"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := weir.New("127.0.0.1:7447")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	const storeName = "mongo"
	const resource = "weir_example/records/s:hello"

	// Write one record. The first field is an _id that matches the resource key.
	record := bson.D{{Key: "_id", Value: "hello"}, {Key: "message", Value: "hi"}}
	data, err := bson.Marshal(record)
	if err != nil {
		log.Fatal(err)
	}
	document := &weir.Document{ContentType: "application/bson", Data: data}
	write := &weir.MutateRequest{Resource: resource, Action: weir.MutationPut, Document: document}
	written, err := client.Mutate(ctx, storeName, weir.WithRequests(write))
	if err != nil {
		log.Fatal(err)
	}
	if written[0].GetFailure() != nil {
		log.Fatal(written[0].GetFailure())
	}
	fmt.Println("outcome:", written[0].GetOutcome())

	// Read the record back.
	read := &weir.ReadRequest{Resource: resource}
	results, err := client.Read(ctx, storeName, weir.WithRequests(read))
	if err != nil {
		log.Fatal(err)
	}
	result := results[0]
	if result.Failure != nil {
		log.Fatal(result.Failure)
	}
	if result.Missing {
		log.Fatal("record not found")
	}

	var stored bson.M
	err = bson.Unmarshal(result.Document.Data, &stored)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("message:", stored["message"])
}
```

**Step 4.** Run it.

```sh
go run .
```

Expected output:

```text
outcome: APPLIED
message: hi
```

**Step 5.** Stop Weir and MongoDB with step 6 of the
[Weir quick start](https://github.com/batchstream/weir#quick-start).

## Documentation

| Guide | Contents |
| --- | --- |
| [Connections](docs/connections.md) | Addresses, gRPC options, Kubernetes DNS names, keepalive and closing |
| [Operations](docs/operations.md) | Reads, writes, visibility, results and outcomes |
| [Lua](docs/lua.md) | Lua transforms |
| [Native](docs/native.md) | Backend commands and HTTP helpers |
| [MongoDB](docs/mongodb.md) | The `mongodb` package: collections, queries and commands |
| [Search](docs/search.md) | The `search` package: indexes, searches and HTTP requests |
| [Streaming](docs/streaming.md) | Callbacks, streams, deadlines and resending records |
| [Development](docs/development.md) | Tests, real backend tests and releases |
| [API reference](https://pkg.go.dev/github.com/batchstream/weir-go) | Every exported type and function |

| Example | Shows |
| --- | --- |
| [read](examples/read/main.go) | A typed read |
| [basic](examples/basic/main.go) | A mutation, then many reads in one call |
| [native](examples/native/main.go) | A native command and its response |
| [mongodb](examples/mongodb/main.go) | A typed MongoDB collection |
| [search](examples/search/main.go) | A typed search index and a search |

## Development

```sh
go test -race ./...
go vet ./...
```

Default tests need no databases. See [Development](docs/development.md) for real
backend tests.

- Contributing: [CONTRIBUTING.md](CONTRIBUTING.md)
- Security reports: [SECURITY.md](SECURITY.md)

## Related repositories

| Repository | Purpose |
| --- | --- |
| [weir](https://github.com/batchstream/weir) | Server |
| **[weir-go](https://github.com/batchstream/weir-go)** | Go client SDK (this repository) |
| [weir-protocol](https://github.com/batchstream/weir-protocol) | Public gRPC schema and validation |
| [weir-charts](https://github.com/batchstream/weir-charts) | Helm chart |
| [weir-tests](https://github.com/batchstream/weir-tests) | System tests and benchmarks |

## License

MIT. See [LICENSE](LICENSE).
