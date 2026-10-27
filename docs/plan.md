# Build plan

Status: durable execution implemented on 2026-10-17; bounded model/tool execution
implemented and verified offline on 2026-10-18. A manually reported live baseline
passed both synthetic cases on 2026-10-19. See
[run.md](run.md) for operation and verification, and [design.md](design.md) for
architecture and future proposals.

The evaluation harness was expanded on 2026-10-25 with alternative/conflicting
evidence, bounded repetitions, scenario filtering, versioned report metadata,
timing, and explicit failure/partial-run accounting. This increment was verified
offline; live results for the expanded suite remain to be collected.
The full ordinary and race-checked suites each passed 244 test/subtest results,
including PostgreSQL integration tests, with no skipped tests. Formatting,
static analysis, and all four executable builds also passed.

The container increment adds a reproducible API/worker/PostgreSQL Compose
deployment, readiness/liveness endpoints, a Go probe, and opt-in real-container
lifecycle verification. This completes application packaging toward milestone 4;
Kubernetes deployment and basic persistence verification were implemented on
2026-10-26. Detailed worker-pod recovery/termination evidence remains outstanding.

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

## Bounded agent increment

The existing executor boundary now supports one Gemini model and three strict,
read-only fixture tools. Four model requests and six read-tool calls fit within
a per-attempt execution deadline; sizes, output structure, and retrieved evidence
IDs are validated. The worker still owns lifecycle and lease cancellation. Shared
database profiles reject inconsistent executor/model/prompt/limit configurations.
The two cases are the checkout connection incident and missing diagnostic data.

Scripted model tests cover invalid actions, unsupported citations, exhausted
budgets, cancellation, and session isolation. Local provider HTTP fixtures cover
wire format, preserved signatures, bounded replies, no retries/redirects, and
sanitized failures. Docker-backed pipeline tests exercise real admission,
execution, result retrieval, provider failure, and lease-loss cancellation.
The complete ordinary and race-checked suites passed with 194 test/subtest results.

## Evaluation increment

The standalone evaluator now runs four evidence cases, including payment
gateway timeouts with available database capacity and conflicting pool signals.
The new cases are evaluation-only: public admission stays limited to the two
original scenarios, preserving existing worker profiles and durable jobs. The
checkout task prompt stays fixed; arbitrary task submission is a separate design.

Acceptance evidence covers 1 to 10 sequential repetitions, single-case filtering,
no provider use for listing/invalid input, separate execution/heuristic failures,
partial reports on cancellation, and independent conversations with correct tool
evidence through the Gemini HTTP adapter. Reports retain full diagnoses, model,
worker/suite identity, per-investigation timing, and requested/completed counts.
There are no automatic retries or new runtime dependencies.

## Container deployment increment

The Dockerfile builds static Go API, worker, database-tool, and probe executables
using pinned base images. The minimal non-root runtime contains the HTTPS trust
bundle. The application Compose overlay uses one image for independently scaled
API/workers, container-internal PostgreSQL addressing, runtime-only worker model
credentials, loopback API publication, resource limits, and preserved DB policy.

Readiness checks initialized, non-draining lifecycle plus bounded database
reachability. Liveness stays independent of dependencies. Worker health listeners
are optional for host processes; Compose uses them without host port publication.
SIGTERM reaches Go directly, with a forty-second worker drain inside a forty-five
second container stop grace. Existing job, lease, timeout, and retry semantics
are unchanged.

The explicit `containers` build-tag test verifies the built deployment with two
workers, four active slots, keyed replay, queued-job persistence on API restart,
SIGKILL recovery, database outage/recovery, and graceful active-job drain. It
uses disposable owned resources and makes no model calls. Normal Go tests remain
independent of Docker unless a PostgreSQL test URL is explicitly configured.
Both full Go suites passed 257 test/subtest results, with no skipped tests or
detected races. Static analysis and all five executable builds passed. The actual
Compose lifecycle test passed, including idle Gemini startup with placeholder
credentials in a separate fresh database and missing-key rejection. Live provider
behavior remains outside that verification.

## Local Kubernetes deployment increment

The kind configuration pins Kubernetes v1.37.0. Kustomize base/fixture/Gemini
overlays deploy the existing image, one API, two workers, native probes, resource
requests/limits, and a single PostgreSQL StatefulSet with a 2 GiB persistent claim.
Migration init containers reuse existing startup/profile checks. Credentials are
created from ignored data files, provider keys reach only Gemini workers, and
fixture/Gemini namespaces have separate volumes. Setup scripts use dedicated
kubeconfigs and ownership checks without changing a default context.

The explicit `kubernetes` build-tag test passed on 2026-10-26 using kind v0.33.0,
kubectl/Kubernetes v1.37.0, and PostgreSQL 18.6. It verified two workers/four slots,
keyed replay, reapplication, API replacement, database Pod replacement retaining
the same PVC/result, idle Gemini startup in another namespace, profile-change
rejection, and unrelated kubeconfig integrity. It made no provider requests.
Both full Go suites passed 257 test/subtest results with no skips or detected
races; static analysis, five builds, and the existing Compose regression passed.

## Next evidence and implementation

The first manually reported live evaluation with `gemini-3.8-flash` passed both
fixtures and the heuristic rubric on 2026-10-19. Review of the supplied output
found appropriate supported findings and uncertainty. The English-language rubric
is not a correctness proof, and a single run does not establish consistent quality.

Run the expanded opt-in suite with repeated live investigations and manually
review diagnoses against all four cases. Also demonstrate a live Gemini job
through the full API/PostgreSQL/worker path. The commands and report semantics
are documented in [agent.md](agent.md). Keep live model results separate from
offline and deployment reliability evidence. The next implementation slice is
Kubernetes worker-pod failure and graceful termination: demonstrate interrupted
attempt recovery, stale-result protection, draining, and bounded shutdown under
actual pod lifecycle operations. Observability and measurement follow. Resource-aware
scheduling remains outside the current MVP.
