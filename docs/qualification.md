# SDK qualification record

This record distinguishes protocol tests, real backend integration, and sustained
production qualification. Passing one does not establish the others.

## Protocol and installation

The initial implementation was reviewed through
[PR #1](https://github.com/batchstream/weir-go/pull/1). Both Linux amd64 and arm64
CI run the complete default race suite and vet with module networking disabled
after dependency preparation. Protocol tests use in-memory gRPC; the workload's
failure-path regression uses a loopback gRPC server.

The suite rejects missing/duplicate Ends, data after End, non-OK status after End,
incorrect Bulk/Scan counts, duplicate/out-of-range Bulk indexes, wrong result
families, malformed mutation outcomes and invalid envelopes. It preserves partial
Bulk results, bounds retained result bytes, joins blocked Native uploads, validates
whole batches before dispatch, and verifies explicit transport defaults. Review
found a typed-nil protobuf action panic; commit `ac698a6` rejects all five typed-nil
action wrappers and nested expression wrappers without making any RPC.

Commit `ac698a6` was independently installed from its public GitHub module into a
new consumer module and a new module cache. The consumer executed `Resource` and
resolved `v0.0.0-20260929011905-ac698a654e01` with no workspace replacements.
The stable release workflow repeats external installation for the published tag.

## Initial EKS integration, 2026-09-29

- Dedicated namespace: `weir-acceptance-20260929`.
- Weir image: `ghcr.io/batchstream/weir@sha256:aee0c24fd5a0edbe81d335522e2741b34ca267f8246c893e15d6ef0097c18bb4`.
- Image source: `278264db2f9617ad583c6b56d19b8aaf5943e771`.
- SDK source: `88dc043` (before the later input-validation-only correction).
- Application port-forward: `127.0.0.1:17447`.
- MongoDB resource: `weir://mongo/weir_acceptance/records`.
- Elasticsearch resource: `weir://search/records`.

The explicit integration command passed with `-race -tags integration`:
`TestMongoLifecycle` in 3.93 seconds and `TestSearchLifecycle` in 4.10 seconds.
Each executed Create, duplicate-create precondition, Replace, Put,
AtomicTransform, Read, ordered mixed Bulk, Scan, Native, Delete, and a missing
Read after deletion. Both touched only one unique generated record per backend;
the records were deleted on successful completion. Native Mongo count and Search
HTTP status were interpreted separately from transport completion.

This was the first compatibility run against the existing image. The final
release was subsequently verified separately below. Multi-replica rolling updates,
backend fault injection, resource plateaus, and the 24-hour run belong to the
deployment qualification record and are not implied by these short tests.

## Stable release integration, 2026-09-29

- Server: [Weir v0.1.0](https://github.com/batchstream/weir/releases/tag/v0.1.0),
  source `be155e053f94cc7a6f3e8ce524f639b46f75be16`.
- Image: `ghcr.io/batchstream/weir@sha256:93810bbfb9eb720d0d42296a3e856f60e1d94bdb22279555ad94a8fc79c0f7df`.
- SDK implementation: `04e6b5b0c1e9`, pinned to `github.com/batchstream/weir v0.1.0`.
- The same dedicated namespace and resource paths listed above; all three Weir
  replicas were Ready on the final digest before the test.

The complete opt-in lifecycle tests passed again with `-race -tags integration`:
MongoDB in 4.13 seconds and Elasticsearch in 4.20 seconds. Every operation listed
in the initial run was repeated against the final image, with successful cleanup
of each test's unique owned record.

A new external consumer module with a separate empty module cache installed the
public SDK commit, executed `Resource`, and resolved the stable server dependency
without any `replace`. Both `examples/read` and `examples/soak` were independently
installed using `go install package@version`. The release workflow repeats these
checks against the published SDK tag, including command-specific dependencies
that Go does not download when only the root SDK package is imported.

The soak runner's optional observer guard was cross-reviewed and tested under
race detection. Missing readiness, malformed or failed status, premature observer
completion, missing heartbeat, and stale heartbeat fail closed. File timestamps
only detect heartbeat changes; expiration uses the process monotonic clock. A
real loopback gRPC test blocks an in-flight write, publishes observer failure,
and verifies cancellation, UNKNOWN accounting, no replay, and no cleanup of
uncertain records. The runner checks observation again before final success.

## Network isolation gate

The current EKS environment does not enforce the chart's NetworkPolicy. The
operator explicitly deferred changes to the shared CNI. Functional tests and a
successful stability run on that cluster therefore do not qualify production
network isolation. Weir's plaintext, unauthenticated listeners still require an
enforced isolation boundary before production use. This gate stays open independently
of SDK correctness, release availability, and workload results.
