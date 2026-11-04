# Contributing to the Weir Go SDK

Focused bug fixes, documentation improvements, tests and feature proposals are
welcome. Public schemas belong to [weir-protocol](https://github.com/batchstream/weir-protocol); open an
[issue](https://github.com/batchstream/weir-go/issues/new/choose) first for a change that also affects it
or the [server](https://github.com/batchstream/weir).

## Report a problem

Use the issue forms and include the SDK version, the Weir server revision, the
backend and its version, the expected result and a minimal reproduction. Remove
credentials and private data from logs and configuration.

For vulnerabilities, follow [SECURITY.md](SECURITY.md).

## Work locally

Install the Go version declared in [go.mod](go.mod).

```sh
git clone https://github.com/batchstream/weir-go.git
cd weir-go
go test -race ./...
go vet ./...
```

Default tests use owned loopback fixtures, without databases.
[Real backend tests](docs/development.md#real-backend-tests) need the `integration` tag
and operator-provided fixtures.

## Prepare a pull request

- Keep the change focused and explain the observed problem and resulting behavior.
- Add a regression test when fixing behavior.
- Update the README, [docs](docs) and [examples](examples) when the API changes.
- Add system integration, backend fault tests and throughput benchmarks to [weir-tests](https://github.com/batchstream/weir-tests).
