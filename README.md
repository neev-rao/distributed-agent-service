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
an explicit model. Live production connectors remain future work.
The standalone evaluator tests four synthetic evidence cases with optional
repetitions, timing, and explicit failure reporting; the API accepts the original
two scenarios. The checkout task prompt is fixed across cases.

With Go and a PostgreSQL database configured via `DATABASE_URL`:

```sh
go run ./cmd/server
# In a second terminal with the same environment:
go run ./cmd/worker
```

Alternatively, Docker Compose can build and run PostgreSQL, the API, and workers
without a host Go installation. For a fresh checkout, copy `.env.example` to an
ignored `.env` and configure its development credentials, then run:

```sh
docker compose -f compose.yaml -f compose.app.yaml up --build -d --wait
# Start a second worker (four execution slots in total):
docker compose -f compose.yaml -f compose.app.yaml up -d --scale worker=2
```

For an existing `.env`, add the `CONTAINER_DATABASE_URL` shown in the example,
using the matching credentials and database name. Container connections use
`postgres:5432`; the existing `DATABASE_URL` remains for host commands.

The API listens on `127.0.0.1:8080` by default. See the
[operating guide](docs/run.md) for requests, verification, and limitations.
It also documents health checks, container lifecycle, and data-preserving cleanup.
For a local Kubernetes deployment using kind, separate fixture/Gemini namespaces,
and persistent PostgreSQL storage, see the [Kubernetes guide](docs/kubernetes.md).
For the model-backed setup and separate live evaluation, see the
[investigation executor guide](docs/agent.md).

- [Agent working guide](AGENTS.md): collaboration rules and current project state.
- [MVP design](docs/design.md): scope, request lifecycle, state, and tradeoffs.
- [Implementation plan](docs/plan.md): milestones and the first task contract.
- [Durability contract](docs/durability.md): admission, ownership, retries, and retention.

The project advances through small engineering cycles: define behavior,
implement, test, review, explain, and record evidence.
