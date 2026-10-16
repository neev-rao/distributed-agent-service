# Distributed Agent Service

A learning project for building and understanding a distributed agent execution
platform in Go, eventually deployed on Kubernetes.

Current status: design only. There is no runnable service or verified build/test
command yet. Technology choices below are recommendations for the first build,
not implemented capabilities.

The proposed demonstration workload investigates synthetic service incidents
using fixture metrics, logs, and runbooks. Start with a deterministic executor;
introduce model-selected tools after the execution platform is testable.

- [Agent working guide](AGENTS.md): collaboration rules and current project state.
- [MVP design](docs/design.md): scope, request lifecycle, state, and tradeoffs.
- [Implementation plan](docs/plan.md): milestones and the first task contract.

The project advances through small engineering cycles: define behavior,
implement, test, review, explain, and record evidence. Application guarantees and
resume claims must follow demonstrated behavior.
