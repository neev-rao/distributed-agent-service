# Operating guide

## Start PostgreSQL, API, and workers

Requirements: Go 1.25 or newer, plus PostgreSQL 18. Docker Compose supplies a local
PostgreSQL service. Replace the public development password in the ignored `.env`
file for your environment; the example is not a production credential.

```sh
cp .env.example .env
set -a
. ./.env
set +a
docker compose up -d --wait postgres
go run ./cmd/server
```

In another terminal, load the same environment and start a worker:

```sh
set -a
. ./.env
set +a
go run ./cmd/worker
```

Start additional worker processes the same way. Each has two execution slots and
an automatically generated identity. The API listens on `127.0.0.1:8080`; the
Compose database binds to `127.0.0.1:55432`. The API and workers are unauthenticated
local-development processes. No public exposure is configured.

The default mode requires `DATABASE_URL` and never falls back to memory when the
database is unavailable. SQL migrations run transactionally on startup. All
processes must use the same shared policy flags for a given initialized database:

```sh
go run ./cmd/server -execution-timeout 5s -lease-duration 10s -retry-backoff 1s
go run ./cmd/worker -execution-timeout 5s -lease-duration 10s -retry-backoff 1s
```

Other shared flags are `-queue-capacity`, `-max-records`, `-max-attempts`,
`-max-result-bytes`, and `-retention`. Initialized database policy is checked at
startup, so changing it requires a coordinated database migration. `-listen` is
API-specific; `-worker-id` and `-fixture-delay` are worker-specific.

For the original volatile single-process demonstration, explicitly use:

```sh
go run ./cmd/server -local
```

Local mode loses all jobs on restart, has no lease recovery, and rejects
`Idempotency-Key` instead of silently ignoring it.

## API

Submit the supported synthetic scenario, optionally using an idempotency key:

```sh
curl -i http://127.0.0.1:8080/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: investigation-001' \
  -d '{"scenario_id":"checkout-db-timeout"}'
```

The response is `202 Accepted`, a `Location` header, and:

```json
{"id":"<job-id>","status_url":"/jobs/<job-id>"}
```

Poll the returned path, substituting the ID:

```sh
curl http://127.0.0.1:8080/jobs/<job-id>
```

Repeated matching submissions with a retained key return the same ID. Keys are
1–128 visible ASCII characters without spaces. Duplicate headers are rejected.
With no key, every accepted submission creates a separate job. The key scope is
the whole development database; this version has no tenants or authentication.

A job contains `id`, `scenario_id`, `state`, and timestamps. Durable jobs include
`max_attempts` and, once claimed, `attempt`. Running jobs add `started_at`;
terminal jobs add `finished_at` and either `result` or `error`. After recovery,
`started_at` describes the latest attempt. History remains in `job_attempts`.

Successful results contain `diagnosis` and `evidence_ids`. The fixture maps those
identifiers to synthetic metrics, logs, and a runbook in
`internal/fixture/executor.go`. It uses a fixed diagnosis, not model reasoning or
live telemetry. A failed job is retrieved with HTTP `200` and `state: "failed"`.

Errors use `{"error":{"code":"...","message":"..."}}`:

| HTTP status | Meaning |
| --- | --- |
| 400 | Invalid JSON/schema/scenario/key, or idempotency used in local mode |
| 404 | Unknown route or retained job ID |
| 405 | Unsupported method; see `Allow` |
| 409 | Idempotency key already belongs to different canonical input |
| 413 | Request body exceeds 16 KiB, including trailing content |
| 503 | Queue/record capacity exhausted, shutdown, or unavailable storage |

`Retry-After: 1` is a suggestion, not guaranteed recovery. At the record limit,
capacity returns only through eligible terminal-record cleanup; restarting the API
does not clear the database. A commit error may mean a job was accepted even
though its response failed. Retry with the same key to resolve that ambiguity.

Execution errors include `execution_failed`, `execution_timeout`,
`result_too_large`, and `attempts_exhausted`. Lost leases retry; confirmed ordinary
executor errors/timeouts are terminal. See [durability.md](durability.md) for the
precise crash and database-failure contract.

## Resource limits and shutdown

| Resource | Default scope and value |
| --- | --- |
| Active execution slots | 2 per worker process |
| Waiting jobs, including delayed retries | 8 globally |
| Retained records, including completed jobs | 1,000 globally |
| Request body | 16 KiB |
| Serialized retained result | 64 KiB |
| Execution budget | 5 seconds per claim/attempt |
| Renewable ownership lease | 10 seconds |
| Expiry-recovery backoff | 1 second |
| Maximum attempts | 3 claims per job |
| Minimum terminal retention | 24 hours after completion |
| Shutdown grace | 10 seconds per process |

Queue waiting is outside execution time. Claim/network latency conservatively
consumes the worker's budget. Timing out does not free an execution slot until
the executor returns. Cancellation is cooperative; arbitrary code is not sandboxed.
The global waiting bound is not a global cap on active executor calls.

Ctrl+C or SIGTERM drains API requests without deleting jobs. Workers stop claiming
and drain active attempts. After their grace period they cancel execution and
abandon ownership; another worker can recover it after lease expiry. A detached
old executor may still run, so external actions may repeat despite stale-result
protection. The fixture remains read-only.

Cleanup is automatic during admission and worker maintenance. To request one
bounded cleanup batch explicitly, using matching shared policy flags:

```sh
go run ./cmd/dbtool -action prune
```

Each call removes at most 100 eligible terminal jobs, their attempt history, and
idempotency keys. Active jobs are never pruned. Keys remain effective while
retained; after cleanup the same key can create a new job.

Logs are JSON. Worker events include worker ID, job ID, and attempt token. A
`job_completion_recorded` event confirms a guarded write, not a particular final
state: the database independently enforces deadlines. GET is authoritative.

## Verification

Unit tests can run without PostgreSQL. Database integration tests explicitly skip
when `TEST_DATABASE_URL` is absent:

```sh
go test ./...
go vet ./...
go test -race ./...
```

Create a separate disposable test database in the local Compose instance:

```sh
docker compose exec postgres createdb -U agentservice agentservice_test
# .env already supplies TEST_DATABASE_URL for that database.
bash scripts/test-integration.sh
```

The database user must be allowed to create schemas. Tests create randomly named
private schemas and remove only those schemas; they do not migrate or drop the
public schema. Set TEST_DATABASE_URL explicitly when using a different database.

Integration tests exercise persistence across pools, concurrent idempotency and
admission, competing claims, stale/expired tokens, recovery capacity and attempt
limits, result/deadline enforcement, retention, and explicit database errors.
Worker tests use controlled executors for lease loss, deadlines, slot ownership,
shutdown, and database uncertainty.

Build standalone processes for a recovery demonstration:

```sh
go build -o bin/server ./cmd/server
go build -o bin/worker ./cmd/worker
```

Start a worker with `-fixture-delay 4s`, submit a job, and observe `job_claimed`.
Terminate that worker abruptly before it finishes; start a replacement worker.
After lease expiry and backoff, GET should show the same ID completing with a
higher attempt count. Use a fresh key for each demonstration. This delay is a
fixture-testing aid, not part of actual inference.

Verified 2026-10-17 using Go 1.27.2, Ubuntu WSL, PostgreSQL 18.6, and GCC 13.3.0:
the complete race-checked suite (including 12 storage integration tests), static
checks, and builds for API, worker, and database tool passed. A compiled-process
demonstration killed an executing worker and recovered the same job ID on attempt
2, preserved queued/completed records across API restarts, replayed idempotent
submissions, and returned `503` during a real database stop before recovering on
restart. That demonstration used a one-second lease and 100 ms backoff in a
private test schema; default-policy storage tests also passed.

Docker Desktop was unavailable on the verification host, so actual tests used a
task-local native PostgreSQL 18.6 instance. The Compose manifest is provided but
was not runtime-verified here. No throughput, multi-host deployment, database
crash/power-loss, or high-availability benchmark has been performed.

## Operational boundaries

PostgreSQL persistence does not provide database high availability or backups.
Queue bookkeeping uses a shared short settings-row lock and has no measured
throughput claim. There are no real model calls, broker, production authentication,
Kubernetes manifests, resource-aware scheduling, or exactly-once external effects.
