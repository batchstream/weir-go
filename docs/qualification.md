# SDK qualification record

This record distinguishes protocol tests, real backend integration, and sustained
production qualification. Passing one does not establish the others.

## Current protocol migration

The SDK now implements ResolveStore initialization followed by direct finite
Execute RPCs. Source schemas and shared protocol/DNS helpers are supplied by the
pinned independent `github.com/batchstream/weir-protocol` revision. CI runs default
race, vet, tagged integration compilation, Python release preflight tests and
formatting on Linux amd64/arm64. Its dependency check scans normal/tagged test
package closures and the complete module graph,
including test and unused requirements. It rejects every Weir server dependency,
protocol paths leading back to the SDK/server, module replacements and locally
generated protobuf source. The protocol module owns public schemas and shared
helpers; neither it nor the SDK depends on the server. No release is published
by this migration; releases require a stable, unreplaced `weir-protocol` tag.

The current tests cover malformed or incomplete Events, unknown request IDs,
request completion and final status, bounded input, canceled senders, no replay,
directory conflicts/expiry, DNS replica refresh, and fixed owner workload routing.
The integration test explicitly initializes through an application listener and
uses one owned temporary record per backend. Historical five-RPC and installed
release evidence below does not establish compatibility or qualification for the
current protocol. Record new real backend/production results with exact source
revisions and images before making deployment claims.

## Server-owned protocol migration validation, 2026-10-03

The independent SDK passed with `GOWORK=off` against the published public API
revision `v0.1.1-0.20261002222539-3571ee4f843c`. Loopback tests used the current
Weir binary and task-owned MongoDB 8.0.32 and Elasticsearch 8.19.22 fixtures.
Both lifecycle tests passed under race: ResolveStore/Open, Create, duplicate-create
precondition, Replace, Put, AtomicTransform, Read, Complete-gated mixed Execute,
ScanPage, Native Execute and Delete. Generated records were cleaned up. Example
regressions also verify fixed owner routing, actual Execute dispatch, nonowner
rejection before business traffic, and uncertain-write preservation.

This validation used a local binary; it does not qualify an OCI image, Kubernetes
rollout or sustained production workload. The historical evidence below remains
specific to the retired release.

## Historical release qualification

The following dated evidence describes the retired five-RPC protocol and its
released SDK. It is retained as deployment history, not validation of the current
SDK API, bounds, or wire contract.

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

## Fixed-Pod qualification coverage

A three-minute Service-based paired run on the final releases completed 5,394
cycles in 180.0568 seconds with a 50 ms p99 upper bound, zero errors, and zero
UNKNOWN outcomes. Both the persisted runner exit status and observer reported
success, with continuous Pod identity and CPU/memory samples. Independent review
found that the six persistent gRPC connections reached only two of three Weir
Pods. This run validates the paired reporting workflow but does not pass the
three-Pod load coverage gate.

SDK v0.1.1 adds fixed targets only to the explicit soak command. Worker assignment
is stable and recorded, with six workers across three Pod IPs giving each Pod one
MongoDB and one Search worker at the same total rate. The public client API and
retry behavior are unchanged. A three-server loopback regression verifies actual
connections and the recorded worker-target mapping; malformed, duplicate and
uneven coverage configurations are rejected before dispatch. The final short
paired rerun and 24-hour run must independently show positive business RPC
counter deltas for every frozen Pod. Service and rolling behavior remain covered
by the separate deployment checks; fixed-Pod coverage does not replace them.

## Network isolation gate

The current EKS environment does not enforce the chart's NetworkPolicy. The
operator explicitly deferred changes to the shared CNI. Functional tests and a
successful stability run on that cluster therefore do not qualify production
network isolation. Weir's plaintext, unauthenticated listeners still require an
enforced isolation boundary before production use. This gate stays open independently
of SDK correctness, release availability, and workload results.
