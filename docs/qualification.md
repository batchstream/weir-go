# SDK qualification record

This record distinguishes protocol tests, real backend integration, and sustained
production qualification. Passing one does not establish the others.

## Protocol and installation

The initial implementation was reviewed through
[PR #1](https://github.com/batchstream/weir-go/pull/1). Both Linux amd64 and arm64
CI run the complete default race suite and vet with module networking disabled
after dependency preparation. Tests use in-memory gRPC.

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

This is a first compatibility run against the existing image. The final server
release digest and the stable SDK protocol dependency still require a paired
rerun. Multi-replica rolling updates, backend fault injection, resource plateaus,
and the 24-hour run belong to the deployment qualification record and are not
implied by these short tests.
