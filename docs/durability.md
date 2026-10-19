# Durable execution contract

The API and worker processes share PostgreSQL as the authoritative job store and
queue. Each process uses a connection pool. No transaction stays open while an
executor runs. Fixture mode is deterministic; optional Gemini workers execute a
bounded model loop with read-only fixture tools. See [agent.md](agent.md).

## Admission and submission retries

Admission checks global queued-job and retained-record limits, inserts the job,
and commits before the API returns `202`. A short transaction locks one settings
row so concurrent submissions, claims, recovery, and cleanup agree on capacity.
This deliberately serializes queue bookkeeping; it is not a high-throughput
broker implementation or a demonstrated scalability claim.

`Idempotency-Key` is optional, limited to one header with 1–128 visible ASCII
characters without spaces. Matching keys and canonical input (`scenario_id`)
return the same job ID while retained, even when admission is otherwise full.
Reusing a key for different input conflicts. Without a key, resubmission can
create another job. The current HTTP workload supports two synthetic scenarios;
canonical input is their scenario ID. Executor/model policy is pinned to the
whole database rather than selected by an individual submission.

A connection failure during commit can leave the caller uncertain whether the
job exists. A storage error returns `503` instead of pretending nothing happened;
retry with the same key to resolve submission ambiguity. There is no in-memory
fallback after a database failure.

## Claims, deadlines, and ownership

A worker claims an eligible queued job in a short transaction. Each claim
increments the attempt count, records attempt history, issues a random token, and
sets a renewable lease plus a separate execution deadline using database time.
The worker begins execution only after claim commit is acknowledged.

Renewals and completions lock the job row and then sample database time. They
require the current token, worker identity, running state, and an unexpired lease.
Expired tokens cannot be revived. Completion after the database execution
deadline becomes `execution_timeout`; terminal job outcomes cannot be overwritten.

Workers conservatively start their local execution budget before the claim call,
so database and network latency consume that budget instead of extending it.
Database time independently checks completion. The two deadlines avoid relying
on synchronized worker wall clocks. A very slow claim can consume the entire
budget and produce timeout without invoking the executor.

Each worker process has two execution slots. Its timeout monitor can record a
failure while an uncooperative executor continues, but the slot remains occupied
until that executor returns. Losing or failing to renew ownership cancels the
executor and prevents it from intentionally recording a completion.

## Recovery and retries

Workers poll and perform periodic maintenance; there is no separate scheduler.
An expired running lease is requeued with backoff if another attempt remains.
Recovery cannot exceed queue capacity. A claim reserves its newly freed queue
slot for expired work before a fresh submission can take it. Exhausted jobs fail
with `attempts_exhausted` without needing a waiting slot.

Attempts count claims, including a crash between claiming and actually executing.
The defaults are three attempts, ten-second leases, one-second recovery backoff,
and a five-second execution budget per attempt. There is no overall job deadline.

Ordinary executor errors, oversized results, and timeouts are terminal when their
completion writes are confirmed. If the database cannot confirm an outcome, lease
recovery may repeat execution. A lost worker's attempt may be recovered even
after its original execution deadline: recovery creates a new attempt and budget.

A disconnected worker may continue executing while a new owner starts. Tokens
guard database writes, not external actions or model charges. The contract allows
repeated execution and does not claim exactly-once behavior. Queue order and
completion order are not strict FIFO fairness guarantees.

## Shutdown and database failure

API shutdown drains HTTP requests but does not cancel durable jobs. Worker
shutdown stops new claims and renews already running attempts during its grace
period. At the cutoff it cancels execution and abandons leases so another process
can recover them. It does not fail unrelated jobs or claim new work during drain.

Database errors prevent acknowledged admission and successful claims. A failed
renewal conservatively cancels execution. A failed completion is logged as
unconfirmed and can lead to repeated execution after lease expiry. Local timeouts
cannot guarantee that a disconnected external system stopped performing work.

## Retention and configuration

Default terminal retention is 24 hours after completion; active records and their
keys do not expire. Cleanup deletes at most 100 eligible terminal jobs per call,
including their attempt history and keys. Admission and worker maintenance call
cleanup; `dbtool -action prune` is an explicit cleanup entry point. Cleanup is
eventual, so matching keys can remain valid longer than the minimum window.

Restarting never clears durable capacity. After eligible records are deleted,
their IDs return `404` and the same key can admit a new job. Pending and running
records are never removed to make space.

Versioned embedded SQL migrations run transactionally under a startup advisory
lock. The first initialization persists the shared execution policy; later API,
worker, and database-tool startup rejects mismatched policy flags. Changing that
policy requires a coordinated database migration, not a rolling flag change.
Migration 002 labels existing databases `fixture-v1` and preserves all records
and per-job time budgets. API, worker, and database tool also verify the shared
execution profile, including the requested model and agent behavior fingerprint.
Use a fresh development database for Gemini mode and stop older processes before
upgrading a fixture schema; see [agent.md](agent.md) for the transition contract.

A single local PostgreSQL instance and persistent volume do not provide database
high availability, backups, tenant isolation, or production authentication. The
development services bind to loopback.
