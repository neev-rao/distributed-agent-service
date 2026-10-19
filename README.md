# Distributed Agent Service

A Go service prototype for asynchronous task execution, with bounded admission,
worker concurrency, execution deadlines, and job status retrieval.

The current implementation separates the API from worker processes and uses
PostgreSQL for durable jobs, idempotent submission, renewable ownership, and
bounded crash recovery. Workers can use a deterministic fixture or an optional
Gemini investigation loop with validated read-only tools and bounded execution.

The workload investigates synthetic checkout incidents using metrics, logs, and
runbook evidence. Fixture mode returns a fixed result. Gemini mode calls a hosted
model to choose tools and synthesize a diagnosis; it requires a provider key and
an explicit model. Live production connectors and Kubernetes remain future work.

With Go and a PostgreSQL database configured via `DATABASE_URL`:

```sh
go run ./cmd/server
# In a second terminal with the same environment:
go run ./cmd/worker
```

The API listens on `127.0.0.1:8080` by default. See the
[operating guide](docs/run.md) for requests, verification, and limitations.
For the model-backed setup and separate live evaluation, see the
[investigation executor guide](docs/agent.md).

- [Agent working guide](AGENTS.md): collaboration rules and current project state.
- [MVP design](docs/design.md): scope, request lifecycle, state, and tradeoffs.
- [Implementation plan](docs/plan.md): milestones and the first task contract.
- [Durability contract](docs/durability.md): admission, ownership, retries, and retention.

The project advances through small engineering cycles: define behavior,
implement, test, review, explain, and record evidence.
