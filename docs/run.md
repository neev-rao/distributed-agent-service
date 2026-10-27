# Operating guide

For the local kind/Kubernetes deployment, see [kubernetes.md](kubernetes.md).
The Compose and host workflows below remain supported independently.

## Run the complete application with Docker Compose

Docker Desktop/the Docker engine and Compose are sufficient for the application;
Go is required only for host development and the Go test commands below. The
existing `compose.yaml` still runs PostgreSQL alone. Add `compose.app.yaml` to
build and start separate API and worker containers from the same image.

For a new configuration, copy `.env.example` to an ignored `.env` and replace the
development password consistently. For an existing `.env`, preserve its values
and add `CONTAINER_DATABASE_URL` and `AGENT_EXECUTOR=fixture` from the example.
Use the matching credentials and database name; URL-encode special characters
in URL credentials. The programs never rewrite an existing database's policy.

```sh
docker compose -f compose.yaml -f compose.app.yaml up --build -d --wait
docker compose -f compose.yaml -f compose.app.yaml ps
curl http://127.0.0.1:8080/readyz
```

The container URL uses `postgres:5432`, the database's service name and internal
port. Host commands keep using `DATABASE_URL` with `127.0.0.1:55432`. Inside a
container, `localhost` refers to that container, not the PostgreSQL service.
The API binds to all interfaces inside its container, but its published port is
restricted to host loopback. Worker health ports are not published. Override
`APP_API_PORT` if a host API already occupies port 8080.

Compose waits for PostgreSQL's health check before starting the application.
Each process still applies/verifies migrations and the shared execution profile
before becoming ready. A readiness check never runs migrations or model calls.
API readiness requires storage, not the presence of a worker: accepted work can
remain queued until workers start.

```sh
# Two workers, each with two execution slots. Queue limits remain global.
docker compose -f compose.yaml -f compose.app.yaml up -d --scale worker=2
docker compose -f compose.yaml -f compose.app.yaml logs -f api worker

# Stop processes while keeping the database volume and accepted jobs.
docker compose -f compose.yaml -f compose.app.yaml down
# Recreate containers and reuse that same project/database volume.
docker compose -f compose.yaml -f compose.app.yaml up -d --wait
```

Use a consistent Compose project name and environment file for every operation.
`down` preserves the volume; adding `--volumes` deletes that project's data and
is appropriate only for an intentionally disposable database. Do not use it as
an ordinary shutdown command. The application overlay reuses the existing
database volume in the same project; it does not create a fresh policy implicitly.

## Container runtime and health contract

The Dockerfile pins both base-image digests. Its build stage verifies Go modules
and compiles static API, worker, database-tool, and probe executables. The final
non-root runtime includes a trusted CA bundle for HTTPS; it has no shell, Go
compiler, source tree, or credentials. `.dockerignore` allows only Go source and
module files, excluding tests, environment files, private notes, and Git history.
Refresh base-image digests deliberately and repeat the build and verification.

Compose uses a read-only application filesystem, drops Linux capabilities, and
prevents privilege escalation. The API defaults to 0.5 CPU/128 MiB; each worker
defaults to 1 CPU/256 MiB. `API_CPUS`, `API_MEMORY`, `WORKER_CPUS`, and
`WORKER_MEMORY` override those local development limits. These values are initial
operating limits, not measured capacity or performance claims.

| Endpoint | Contract |
| --- | --- |
| `GET /livez` | `200` while the HTTP process responds, independent of database/provider state |
| `GET /readyz` | `200` after initialization when storage is reachable and drain has not begun; otherwise `503` |

Health responses do not reveal database errors or credentials. The database
check is bounded to one second. Explicit `-local` mode checks only lifecycle.
Readiness does not verify model access, job quality, free worker capacity, or the
progress of every goroutine. Worker health is enabled by `-health-listen` in
Compose and disabled by default for host worker processes.

`/app/probe` is a small Go HTTP client. It exits zero only for `200`, refuses
redirects, and defaults to a two-second request timeout. Compose runs it every
two seconds against readiness. Docker marks failed checks unhealthy; Compose
does not automatically restart or stop claiming work merely because that status
changes. During database outages the job API returns storage errors and workers
retain their existing cancellation/recovery behavior. Provider failures stay job
execution failures and do not cause health-driven restart loops.

The server/worker executable is the container's main process, so SIGTERM reaches
Go directly. The API keeps its ten-second drain grace. Compose configures each
worker with a forty-second grace, including fixture mode, and allows forty-five
seconds before Docker can force termination. While a worker drains, readiness
is false, liveness stays true, and the runtime stops claims while renewing active
leases. If overriding commands or grace periods, keep the outer stop grace
longer than the application's drain grace. A killed/expired worker can still
cause repeated external execution; containers do not change the lease contract.

References: [Compose startup order](https://docs.docker.com/compose/how-tos/startup-order/),
[service lifecycle settings](https://docs.docker.com/reference/compose-file/services/),
and [Distroless static image contents](https://github.com/GoogleContainerTools/distroless/blob/main/base/README.md).

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

These commands select the deterministic fixture executor. To use Gemini with
read-only tools, a 30-second attempt budget, and a fresh isolated database, follow
[agent.md](agent.md). All processes sharing that database must select the same
executor and model profile. The API does not require a provider key; the Gemini
worker does. Configuration is never changed implicitly on an existing database.

## API

Submit a supported synthetic scenario, optionally using an idempotency key:

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
`internal/fixture/executor.go`. Fixture mode uses a fixed diagnosis. Gemini mode
uses the same evidence through validated tools. The additional scenario
`checkout-insufficient-evidence` withholds diagnostic signals in both modes. A
failed job is retrieved with HTTP `200` and `state: "failed"`.

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
Agent mode additionally reports sanitized provider, invalid-action/result, and
budget errors described in [agent.md](agent.md).

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

With Docker running, explicitly opt into the real-image lifecycle test:

```sh
go test -tags=containers -v -count=1 -timeout=10m ./internal/containertest
```

It builds/starts a uniquely named Compose project on random loopback ports with
two workers and a three-second fixture delay. It checks four active slots,
keyed replay, API restart persistence, SIGKILL recovery on attempt two, actual
database outage/readiness recovery, and SIGTERM draining without further claims.
It also checks image identity, non-root execution, runtime constraints, the CA
bundle, and lack of published worker ports. The test does not load `.env` or
`.env.agent`; ownership labels are verified before removing its containers,
network, volume, and image tag. It is excluded from the normal suite by a build
tag and does not require or call a live model provider.
The final phase starts an idle Gemini pool in a fresh database using placeholder
credentials and verifies missing-key rejection. It validates deployment wiring
and preflight only; no investigation is submitted to that pool.

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

Verified 2026-10-17 using Go 1.27.2, Ubuntu WSL, and PostgreSQL 18.6 in Docker
Compose: the complete race-checked suite passed with 93 test/subtest results,
including all 12 storage integration tests, no skipped tests, and no detected
data races. Ordinary tests, static analysis, formatting, and builds for API,
worker, and database tool also passed. A compiled-process demonstration against
the Docker database killed an executing worker and recovered the same job ID on attempt
2, preserved queued/completed records across API restarts, replayed idempotent
submissions, and returned `503` during a real database stop before recovering on
restart. That demonstration used a one-second lease and 100 ms backoff in a
private test schema; default-policy storage tests also passed. A separate explicit
`-local` process check verified submission/results, input validation, clean
shutdown, bind/flag failures, and the expected loss of jobs after restart.

The Compose health check and PostgreSQL-backed suite were verified in an isolated
test project bound to loopback on port 55433; the documented development port
remains 55432. Earlier verification also passed against native PostgreSQL 18.6.
No throughput, multi-host deployment, database crash/power-loss, or
high-availability benchmark has been performed. Lost commit acknowledgements
have not been directly fault-injected.

Verified 2026-10-25 after the container increment: 257 passing tests/subtests
in both ordinary and race-checked suites, no skipped tests, static analysis, and
builds for API, worker, database tool, evaluator, and probe. Docker PostgreSQL 18.6 tests
include v1 profile migration preservation, profile mismatch rejection, concurrent
fresh migration startup, and HTTP-to-worker agent execution with a local Gemini
protocol fixture. Provider requests are canceled after forced lease loss. These
checks make no requests to Google.

The evaluator additionally covers alternative and conflicting evidence, fresh
conversations across repeated investigations, filters and limits, failure
classification, partial reports on cancellation, and CLI preflight/output
behavior. A local HTTP model fixture verifies the complete evaluation/agent/
Gemini transport/tool path for all four cases. The public API continues accepting
only the original two scenarios. See [agent.md](agent.md) for live evaluation
commands and the distinction between heuristic passes and model correctness.

The opt-in container lifecycle test also passed using Docker Engine 29.2.1,
Compose 5.0.2, and the pinned Go 1.25.14 builder. Host tests used Go 1.27.2 on
Ubuntu WSL. The complete one-command Compose build/start, two-worker execution,
API restart, worker kill/recovery, database outage, and graceful drain were
observed. An idle Gemini pool initialized against a separate fresh database
using placeholder credentials; missing-key startup failed. These container checks
made no model requests and do not establish live Gemini access or quality.

A manually run live evaluation reported on 2026-10-19 used `gemini-3.8-flash` and
passed both synthetic cases and the heuristic rubric. Review of the reported
diagnoses found the supported connection-pool finding in the complete-evidence
case and appropriate uncertainty in the missing-evidence case. This standalone
live check did not exercise the API/database/worker path and is one baseline,
not evidence of repeated-run consistency or broad model quality.

## Operational boundaries

PostgreSQL persistence does not provide database high availability or backups.
Queue bookkeeping uses a shared short settings-row lock and has no measured
throughput claim. Gemini mode can make external model calls; normal tests and
default fixture mode do not. There is no broker, production authentication,
resource-aware scheduling or exactly-once external effect. Local Kubernetes
deployment is documented and verified separately in [kubernetes.md](kubernetes.md).
