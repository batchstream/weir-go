# Explicit stability workload

This executable contacts an operator-provided isolated Weir application endpoint.
It is never invoked by default `go test ./...`. It uses plaintext only; production
qualification requires verified network isolation. It does not install infrastructure or retry
writes. Use only disposable, pre-created collections/indexes owned by this run.

The published command can be installed outside this checkout with
`go install github.com/batchstream/weir-go/examples/soak@v0.2.0`. Go resolves the
command's BSON dependency even when a consumer only imported the SDK root package.

Build once from the exact reviewed SDK revision for the worker architecture:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -o soak ./examples/soak
```

Run inside the acceptance namespace and the real client NetworkPolicy path.
Use `-address OWNER:PORT` for a known owner or `-targets IP:PORT,IP:PORT,IP:PORT`
for fixed three-Pod coverage. These flags are mutually exclusive. Fixed targets
must be distinct literal Pod IPs with valid ports, never DNS names that can resolve
to changing Pods. Before any write, ResolveStore must advertise the target IP (directly or through
DNS); a mismatched seed is rejected. ResolveStore runs on the same connection
used for business Execute, without Open or redirection. `-address` must identify
an owner of both configured Stores; it is not an arbitrary discovery Service.
A worker always uses `targets[worker_index % target_count]`;
there is no failover or replay. The target count must be odd and workers must be a
multiple of twice that count, so every target gets equal MongoDB and Search load.
For three targets and six workers, each Pod receives one worker per backend.
The start record and worker events preserve the exact target assignments. Freeze
the corresponding Pod UIDs and image identities in the observer baseline.

Historical Service invocation and rolling-update checks belong to the earlier
release qualification; they do not qualify the current protocol. The 24-hour fixed-target run proves per-Pod steady coverage; it
does not establish Service load balancing behavior. A short calibration uses the same arguments with `-duration 3m`
and a fresh `-run-id`. Freeze all values and artifact identities before the 24-hour run:

```sh
./soak -targets POD_1_IP:7447,POD_2_IP:7447,POD_3_IP:7447 \
  -mongo-resource weir://mongo/weir_acceptance/records \
  -search-resource weir://search/records \
  -run-id weir-soak-UNIQUE-LOWERCASE-ID \
  -duration 24h -workers 6 -cycles-per-second 5 -max-p99 500ms \
  -server-revision FULL_SERVER_COMMIT -image-digest sha256:FULL_IMAGE_DIGEST \
  -chart-version CHART_VERSION -sdk-revision FULL_SDK_COMMIT \
  -observer-status /results/observations.jsonl.status.json \
  -observer-heartbeat /results/observations.jsonl.ready
```

Collection flags use `weir://STORE/...` only as operator input and audit metadata.
The runner separates StoreName and sends canonical relative Command targets.
Each worker owns a distinct string key, with half the workers assigned to each
backend. It Creates that record once, then schedules Put → Read → Replace → Read cycles, each using typed SDK commands over a finite Execute
RPC. Every acknowledged value is checked before the next dependent operation.
Each cycle performs four RPCs and two mutations; the default steady load is approximately
120 RPC/s and 60 mutations/s. Once a minute and at normal completion, one leader
per backend additionally checks Scan end/count/final status and read-only Native completion/body semantics.
This respects the server's single Native/Scan session per Store. Search
Scan runs after the first minute, allowing its normal asynchronous refresh.

This measures stability at a specific workload. It is not a peak capacity claim or
an implementation of the server's broader historical capacity matrix.

JSON lines include the run start UTC, exact artifact identities, monotonic elapsed
time, owned keys, periodic counters, cumulative and per-minute cycle p99 bucket
upper bounds, and a final `passed` or `failed` record. Errors include worker, key,
sequence, mutation uncertainty and available mutation outcomes. Successful
cycle counts require all four logical operations to pass; they are not total
backend execution counters. Initial/final records and stream checks are separate.

Any operation, completeness, value or per-minute latency failure cancels the
workload, retains uncertain records for reconciliation and exits nonzero. UNKNOWN
writes are never retried. A worker only deletes its own key after clean completion;
Delete is followed by a missing Read. Final success additionally requires actual
monotonic elapsed ≥ duration, ≥98% of the fixed scheduled cycle count, and the
cumulative p99 bound. SIGTERM/SIGINT is a failed run, not successful completion.

Use a Job with `backoffLimit: 0`, `restartPolicy: Never`, an active deadline slightly
longer than the requested duration, and a task-owned persistent results volume.
Redirect both stdout/stderr and persist the process exit status; retain the Job
and its UID. A controller must treat eviction, disappearance, restart or absent
final evidence as failure, never silently launch a new 24-hour window.

For an externally observed qualification, supply both observer paths. The observer
must publish its first heartbeat before the workload starts; startup waits at most
30 seconds. It refreshes the heartbeat by atomic rename after each complete valid
sample, and atomically publishes terminal JSON with a required `passed` boolean.
A missing, unreadable or unchanged heartbeat for over 150 seconds, malformed or
failed status, or an observer that exits early fails the workload. Heartbeat file
mtime only detects changes; the freshness interval uses the process monotonic
clock. The runner checks every second and again before reporting success, cancels
workers on failure and retains their records. It does not wait for observer success:
the observer verifies the completed Job and persisted runner report afterward.
Both final reports plus a successful Job exit are required. Use fresh paths per
run, and start the observer before opening the runner report file. Omitting both
flags remains useful for standalone functional calibration.

Separately sample each Weir Pod's UID, image digest, readiness, restart count,
OOM/termination state and Prometheus memory/admission/execution/queue metrics at
least once per minute. Preserve those observations with this log and reject
unexpected restarts, unknown gaps, unresolved resource growth, rejected work or
failed network isolation. The workload alone cannot verify those infrastructure
properties. Complete fault, rolling-upgrade and rollback tests before this steady
run; their expected errors must not be hidden inside the zero-error steady window.

The current EKS acceptance environment has an explicitly deferred CNI enforcement
gate. Its approved functional/stability run must be reported separately from
production isolation qualification; passing this workload cannot close that gate.
