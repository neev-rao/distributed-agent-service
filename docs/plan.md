# Build plan

Status: durable execution milestone implemented on 2026-10-17. See
[run.md](run.md) for operation and verification, and [design.md](design.md) for
architecture and future proposals.

## Milestones

| Milestone | Deliverable | Exit evidence |
| --- | --- | --- |
| 0: Design | Workload, lifecycle, scope, first task contract | Review against brief and failure scenarios |
| 1: Local slice | Go API, bounded workers, volatile state, fake executor | Submit/poll demo, failure/concurrency/shutdown tests |
| 2: Distributed jobs | PostgreSQL, separate workers, bounded retry and lease recovery | Crash/restart tests, stale completion rejection, admission and submission-idempotency tests |
| 3: Agent runtime | One hosted model, validated read-only tools, bounded execution | Scripted provider tests plus real-model quality evaluations |
| 4: Kubernetes | Reproducible local deployment and lifecycle settings | Pod termination and restart/recovery demonstrations |
| 5: Measurement | Repeatable workloads, traces, metrics, documented bottleneck | Environment and procedure alongside latency/throughput results |

Use correlation logs from the first slice; add observability alongside new
boundaries instead of waiting for the last milestone. Model API access is needed
only for milestone 3.

## Completed local implementation contract (`-local`)

Goal: submit one synthetic incident investigation asynchronously and retrieve a
deterministic diagnosis with evidence. Work only on milestone 1.

Implemented structure:

```text
cmd/server/          HTTP server entry point and lifecycle
internal/jobs/       state, admission, queue, workers, executor interface
internal/httpapi/    validation and HTTP representation
internal/fixture/    deterministic incident executor and evidence
```

Use Go's standard library where sufficient. Avoid generic plugin frameworks and
abstract storage layers before a second implementation needs them. Keep the
executor boundary small enough to substitute blocking/error executors in tests.

Acceptance criteria:

- Valid `POST /jobs` returns `202`, an opaque ID and status location. `GET` shows
  state and terminal result/error; unknown IDs return `404`.
- Invalid or unknown scenario input returns `400`; oversized bodies return `413`.
- Successful fixture execution returns a diagnosis with evidence identifiers.
- State follows `queued -> running -> succeeded | failed`; results are immutable
  after terminal transition. Graceful shutdown may fail queued jobs directly.
- Concurrency never exceeds two active executors; at most eight jobs wait and
  1,000 job records are retained. Saturation returns `503` with a stable error code
  and `Retry-After`. Rejection leaves no runnable job or retained orphan.
- Execution timeout begins at running, produces `execution_timeout`, and does not
  let a late result overwrite failure. Do not release an active execution slot
  until the cooperative executor has returned.
- Executor errors are inspectable and do not prevent subsequent jobs from running.
- An accepted job outlives its originating HTTP request/disconnection.
- Shutdown closes admission and drains within a configured grace period; remaining
  jobs fail/cancel according to design. Restart data loss is clearly documented.
- Bound retained result bytes as well as record count; no background cleanup
  service, persistence, model calls, deployment, or retries in this increment.

Verification should include API lifecycle tests, controlled blocking executors
for concurrent admission and worker limits, error/timeout tests, cancellation
ownership, terminal-state stability, and shutdown. Prefer synchronization over
arbitrary sleeps. Review tests against the contract, not just the code shape.
Run the Go race detector where supported; it detects races only on executed paths.
See [official race-detector guidance](https://go.dev/doc/articles/race_detector).

`go test ./...`, `go vet ./...`, and `go test -race ./...` pass on the verified
Linux toolchain. A compiled process smoke check covers submission, results,
validation, clean SIGTERM, bind failure, invalid timeout flags, and restart loss.

## Delivery workflow

1. Define outcome, scope, failure behavior, and acceptance evidence.
2. Explain the consequential choice and implement a small change.
3. Run checks and inspect the actual outputs.
4. Review against the contract, using a fresh reviewer for material changes.
5. Fix substantiated findings and rerun affected checks.
6. Document the result, verification, and remaining limitations.

Review agreement is not proof. Keep proposals, implementation, and verification
separate, and record performance claims only alongside measurements.

## Completed durable implementation contract

The API acknowledges committed PostgreSQL admission. Separate Go workers claim
and renew attempts, reject stale completions, and recover expired leases with a
bounded attempt count and backoff. Shared queue/record limits remain transactional,
matching idempotency keys replay retained jobs, and cleanup removes only expired
terminal records. See [durability.md](durability.md) for the precise guarantees.

Validation tightened two points: tokens protect stored results rather than
external effects, and recovery reserves freed queue capacity before new arrivals
can starve expired work. Shared policy is persisted to reject inconsistent process
configuration. Ordinary execution failure is terminal only if its write is
confirmed; database uncertainty can lead to repeated execution.

PostgreSQL integration tests use isolated schemas and require TEST_DATABASE_URL.
The explicit integration script fails when that variable is absent rather than
silently reporting skipped database checks as verification.

## Next milestone

Define a concrete model/tool task and objective evaluation criteria before adding
the bounded agent runtime. Keep model quality separate from execution reliability.
Resource-aware scheduling, Kubernetes, and product differentiation require their
own scoped designs and evidence.
