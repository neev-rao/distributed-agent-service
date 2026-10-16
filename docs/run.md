# Operating guide

## Requirements and execution

Use Go 1.25 or newer. The module has no third-party runtime dependencies.

From the repository root:

```sh
go run ./cmd/server
```

The default listener is `127.0.0.1:8080`. The service is unauthenticated and
intended for local development. Configuration flags:

```sh
go run ./cmd/server -listen 127.0.0.1:8081 -execution-timeout 5s -shutdown-grace 10s
```

Stop with Ctrl+C or SIGTERM. The HTTP listener closes and accepted jobs drain
within the shared shutdown grace period. An expired grace period fails unfinished
jobs, cancels execution, and exits with an error. A completed drain exits cleanly.

## API

Submit the supported synthetic scenario:

```sh
curl -i http://127.0.0.1:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"scenario_id":"checkout-db-timeout"}'
```

The response is `202 Accepted`, with a `Location` header and this body shape:

```json
{"id":"<job-id>","status_url":"/jobs/<job-id>"}
```

Poll the returned URL, replacing the placeholder with the ID:

```sh
curl http://127.0.0.1:8080/jobs/<job-id>
```

A job has `id`, `scenario_id`, `state`, and `created_at`. Running jobs add
`started_at`; terminal jobs add `finished_at` and either `result` or `error`.
The executor may finish before the caller's first poll.

Successful results contain `diagnosis` and `evidence_ids`. The fixture links those
identifiers to synthetic metrics, logs, and a runbook in
`internal/fixture/executor.go`. Its diagnosis identifies database connection-pool
exhaustion but does not claim to establish why the connections remain occupied.

Errors use `{"error":{"code":"...","message":"..."}}`. An accepted job can
later fail; retrieving that job still returns HTTP `200` with `state: "failed"`.

| HTTP status | Meaning |
| --- | --- |
| 400 | Invalid JSON/schema or unsupported scenario |
| 404 | Unknown route or job ID |
| 405 | Unsupported method; see `Allow` header |
| 413 | Request body exceeds 16 KiB, including trailing content |
| 503 | Queue full, record limit reached, or shutdown in progress |

Admission errors include a stable code and `Retry-After: 1`. For `queue_full`,
capacity may recover as workers progress. For `record_limit`, waiting alone will
not help: this version requires a restart, losing all jobs and results. Shutdown
also requires a newly running process. The header is not a promise of recovery.

Execution failures use `execution_failed`, `execution_timeout`,
`result_too_large`, or `service_shutdown` in the retained job's error field.

## Resource and lifecycle limits

| Resource | Default |
| --- | --- |
| Active executors | 2 |
| Waiting jobs | 8 |
| Retained records, including completed jobs | 1,000 |
| Request body | 16 KiB |
| Serialized retained result | 64 KiB |
| Execution timeout, starting at running | 5 seconds |
| Shutdown grace | 10 seconds |

Only the listen address, execution timeout, and shutdown grace are CLI flags.
Other limits are defined by `jobs.DefaultConfig` and can be varied in tests.
Queue waiting does not consume the execution timeout. Timing out records a failed
job, but the worker cannot take another job until its executor actually returns.
Cancellation is cooperative, not a mechanism to terminate arbitrary code.

Logs are JSON with job IDs, state, and `event_at` transition times. Running and
terminal events include queue wait; terminal running-job events include execution
duration. Concurrent logging happens outside the state lock, so sink order need
not match transition order. Use `event_at` and the job status to interpret events.

## Verification

```sh
go test ./...
go vet ./...
go test -race ./...
go build -o bin/server ./cmd/server
```

Tests cover the HTTP lifecycle and validation, concurrent admission, worker and
queue bounds, retained records/output, error recovery, immutable snapshots,
request-independent execution, deadlines, late results, and shutdown.

Verified on 2026-10-16 with Go 1.27.2, Linux/amd64 (Ubuntu WSL), CGO enabled, and
GCC 13.3.0: the test suite, `go vet`, and race detector passed. A compiled server
smoke check verified `202` submission, evidence-bearing success, `400` invalid
scenario, `413` oversized body, `404` unknown job, clean SIGTERM exit, and `404`
for an old job ID after restart. Bind failure and an invalid timeout flag both
returned exit status 1. A subsequent cancellation-during-drain regression test
also passed under the race detector. No throughput benchmark has been performed.

## Guarantees and limitations

- `202` acknowledges process-local admission, not durable storage or completion.
- All jobs and results disappear on restart. Two server processes do not share
  state and cannot be used as interchangeable replicas of this implementation.
- No retries or submission idempotency: losing the submit response leaves an
  ambiguous outcome; resubmission may duplicate work.
- Retained-output limits do not bound allocations made by arbitrary executors.
  Only trusted fixture code runs in this process; this is not a code sandbox.
- There is no database, broker, model provider, public authentication, or
  Kubernetes deployment in this milestone.
