# Distributed Agent Service

A Go service prototype for asynchronous task execution, with bounded admission,
worker concurrency, execution deadlines, and job status retrieval.

The current implementation runs in one process with volatile job storage and a
deterministic incident executor. Distributed persistence, model-directed tools,
and Kubernetes deployment are planned follow-up milestones.

The demonstration workload investigates synthetic checkout incidents using fixed
metrics, logs, and runbook evidence. It does not call a model or live service.

With Go installed:

```sh
go run ./cmd/server
```

The API listens on `127.0.0.1:8080` by default. See the
[operating guide](docs/run.md) for requests, verification, and limitations.

- [Agent working guide](AGENTS.md): collaboration rules and current project state.
- [MVP design](docs/design.md): scope, request lifecycle, state, and tradeoffs.
- [Implementation plan](docs/plan.md): milestones and the first task contract.

The project advances through small engineering cycles: define behavior,
implement, test, review, explain, and record evidence.
