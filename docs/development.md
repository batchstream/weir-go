# Development

How to test the SDK and publish a version.

## Packages

| Package | Contents |
| --- | --- |
| `weir` | The client and its protocol types. It knows no backend. |
| `mongodb` | MongoDB documents and commands, built on `weir`'s public API |
| `search` | Elasticsearch and OpenSearch documents and HTTP requests, built on `weir`'s public API |
| `internal/record` | Single-record calls the two packages share |
| `internal/storetest` | A fake Weir for the two packages' tests |

`weir` cannot depend on a backend package: both import it, and Go rejects import
cycles.

## Module dependencies

The SDK depends on the stable `github.com/batchstream/weir-protocol v0.17.0`
release. The independent protocol repository owns public schemas, generated
protobuf types and shared validation. Both Weir and this SDK consume
it; neither the protocol nor SDK module depends on the server. The SDK does not
generate schemas.

## Examples

[read](../examples/read/main.go) performs a typed read, [basic](../examples/basic/main.go)
mutates a document then reads multiple resources in one call, and
[native](../examples/native/main.go) consumes
native responses. These examples use SDK request and result types.
[mongodb](../examples/mongodb/main.go) and [search](../examples/search/main.go) use
the `mongodb` and `search` packages.

## Tests

```sh
go test -race ./...
go vet ./...
```

Default tests use owned loopback fixtures, without databases or production I/O.
CI checks typed operation dispatch, acknowledgements with transport errors,
malformed/empty native chunks, incremental consumption,
completion/cancellation, refusal-only resends and deadline admission. It also checks that
the module graph does not contain the Weir server, that `go.mod` replaces no
module and that the protocol is a released `vMAJOR.MINOR.PATCH` version.

## Release

After a reviewed change is merged, an authorized maintainer publishes a stable
version with `gh workflow run release.yml --ref main -f version=vX.Y.Z`. The
workflow runs CI on the `main` commit, then tags that commit and creates its
GitHub release; it refuses a version already tagged at another commit.

## Real backend tests

Real backend tests require an explicit tag and operator-provided fixtures:

```sh
WEIR_ADDRESS=127.0.0.1:7447 \
WEIR_MONGO_STORE=mongo \
WEIR_MONGO_RESOURCE=weir_example/records \
WEIR_SEARCH_STORE=search \
WEIR_SEARCH_RESOURCE=weir_example \
go test -race -tags integration -v ./...
```

Pre-create disposable collections/indexes. Each test creates and cleans up only
its generated records. Lifecycle tests cover CRUD, backend expressions and
Native; streaming tests verify same-resource ordering through an individual
failure, Lua persistence, and repeated and missing reads. The `mongodb` and
`search` packages test every method, cursors and command failures against the
same fixtures. System integration tests, backend fault tests and throughput
benchmarks belong to the independent
[weir-tests](https://github.com/batchstream/weir-tests) repository.
