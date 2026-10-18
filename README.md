# Distributed Agent Service

A Go service prototype for asynchronous task execution, with bounded admission,
worker concurrency, execution deadlines, and job status retrieval.

The current implementation separates the API from worker processes and uses
PostgreSQL for durable jobs, idempotent submission, renewable ownership, and
bounded crash recovery. The executor remains deterministic; model-directed tools
and Kubernetes deployment are future milestones.

The demonstration workload investigates synthetic checkout incidents using fixed
metrics, logs, and runbook evidence. It does not call a model or live service.

With Go and a PostgreSQL database configured via `DATABASE_URL`:

```sh
go run ./cmd/server
# In a second terminal with the same environment:
go run ./cmd/worker
```

The API listens on `127.0.0.1:8080` by default. See the
[operating guide](docs/run.md) for requests, verification, and limitations.

- [Agent working guide](AGENTS.md): collaboration rules and current project state.
- [MVP design](docs/design.md): scope, request lifecycle, state, and tradeoffs.
- [Implementation plan](docs/plan.md): milestones and the first task contract.
- [Durability contract](docs/durability.md): admission, ownership, retries, and retention.

The project advances through small engineering cycles: define behavior,
implement, test, review, explain, and record evidence.
