# Bounded investigation executor

For the Kubernetes Gemini overlay and its separate namespace/database, see
[kubernetes.md](kubernetes.md#gemini-namespace). The provider/key and execution
contracts below apply to host, Compose, and Kubernetes workers.

The optional Gemini executor uses one hosted model to investigate synthetic
checkout incidents. Go validates and executes three read-only tools, checks
evidence provenance, and returns the existing `diagnosis` / `evidence_ids` result.
The default executor remains the deterministic fixture. No live production data,
arbitrary user prompts, shell access, or additional service is involved.

## Start an isolated Gemini worker pool

Requirements: the existing Go/PostgreSQL setup, a Gemini API key, and an explicitly
selected function-calling model available to that provider project. Configure the
key locally; API and database-tool processes do not need it. Gemini API billing
and quotas apply independently to each provider project.

```sh
cp .env.agent.example .env.agent
# Edit .env.agent: select GEMINI_MODEL, set GEMINI_API_KEY, and replace the
# development password consistently in POSTGRES_PASSWORD and the database URLs.
set -a
. ./.env.agent
set +a
docker compose -p distributed-agent-service-ai --env-file .env.agent up -d --wait postgres
go run ./cmd/server -executor gemini
```

In another terminal, load the same environment and start:

```sh
set -a
. ./.env.agent
set +a
go run ./cmd/worker -executor gemini
```

The separate Compose project creates a fresh database volume on loopback port
55434. The existing fixture database is not converted. Run one API on its default
8080 port, or select another API port with `-listen`. `.env.agent` is ignored;
`.env.agent.example` contains only public development values and empty key/model
placeholders. The program does not automatically load environment files.

`-model` overrides `GEMINI_MODEL`. Worker startup fails without `GEMINI_API_KEY`;
there is no fallback to a fixed answer after an agent/provider failure. A model
ID must be a simple name, not a URL or path. No model is selected implicitly.

Submit either supported scenario through the existing API:

```sh
curl http://127.0.0.1:8080/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: checkout-investigation-001' \
  -d '{"scenario_id":"checkout-db-timeout"}'
```

`checkout-insufficient-evidence` withholds request logs and database connection
metrics. A grounded answer should identify the uncertainty rather than reuse the
connection-pool diagnosis. Both scenarios are synthetic and available in fixture
mode too. Poll `GET /jobs/{id}` for the authoritative stored outcome.

## Run the Gemini pool in containers

For a new `.env.agent`, the example includes container settings. For an existing
file, preserve the saved key/model and add `AGENT_EXECUTOR=gemini` and a matching
`CONTAINER_DATABASE_URL` using `postgres:5432` and the isolated Gemini database
name. Keep the password consistent and URL-encode it in database URLs.

```sh
docker compose -p distributed-agent-service-ai --env-file .env.agent \
  -f compose.yaml -f compose.app.yaml up --build -d --wait
```

Use those same project/file arguments for subsequent `ps`, `logs`, scaling, and
`down` commands. The separate Gemini project and volume are required; do not
point Gemini processes at an initialized fixture database. The Compose overlay
passes `GEMINI_API_KEY` only to workers at runtime, never to the API or build.
Do not dump resolved Compose configuration or container environments containing
the key into shared logs. Readiness performs no provider requests: a healthy
Gemini pool confirms local initialization and storage access, not valid provider
credentials, available quota, or diagnosis quality. Submit a job explicitly to
exercise the live provider. Normal/container tests make no live model calls.

## Execution and limits

Each attempt creates a separate model conversation. The model receives a neutral
incident description and tool declarations, not the fixture answer or scenario
identifier. A tool is an allowlisted Go function:

The task description is fixed: investigate checkout errors with steady request
volume. The operating instructions stay the same across evidence cases. Gemini
chooses tools and writes the diagnosis from their observations. General task
submission would require an explicit API, storage, and idempotency contract and
is not implemented by this executor or the evaluation harness.

| Tool | Exact input | Output |
| --- | --- | --- |
| `read_metrics` | `{"service":"checkout"}` | Error-rate and connection observations |
| `read_logs` | `{"service":"checkout"}` | Request logs or an explicit missing-data observation |
| `read_runbook` | `{"name":"database-pool"}` | Investigation guidance, not proof of a cause |

The provider requests functions; it does not execute them. The runtime validates
an entire requested batch before executing any tool, runs tools sequentially, and
returns evidence with stable IDs. It rejects unknown tools, duplicate call IDs,
missing/extra/duplicate JSON arguments, and actions mixed with a final result.

The model finishes by calling `finish_investigation`. Every cited evidence ID
must have been returned by a tool in this attempt; citations must be unique.
Diagnosis text is capped at 8 KiB. Valid citations establish provenance, not
semantic correctness or proof that every statement follows from the evidence.

| Resource | Gemini mode |
| --- | --- |
| Execution budget | 30 seconds per attempt by default; queue wait excluded |
| Model requests | At most 4 per attempt, including the final answer request |
| Read-tool calls | At most 6 per attempt |
| Individual model request | At most 10 seconds and never beyond the parent deadline |
| Cumulative serialized tool results | 16 KiB per attempt |
| Provider request / response | 64 KiB each |
| Model output setting | 1,024 tokens per request, including model-specific generation constraints |
| Final retained result | At most the shared database result limit; agent ceiling 64 KiB |
| Ownership lease | 10 seconds by default, renewed about every 3.33 seconds |
| Worker shutdown grace | 40 seconds by default in Gemini mode |

The model cannot extend these budgets. `-execution-timeout` and shared storage
policy flags must agree across processes. Increasing the attempt timeout also
requires considering `-shutdown-grace`; an explicitly shorter grace permits
abandonment and recovery. Call-count and size limits are bounded work controls,
not a guaranteed dollar-spend ceiling. Rate-limited, truncated, or malformed
provider replies fail the attempt rather than silently retrying.

Deadline, shutdown, and lease-loss cancellation propagate to provider HTTP
requests. PostgreSQL independently rejects late success and stale ownership.
An uncooperative executor retains its execution slot until it returns. Local
cancellation cannot guarantee that a provider stopped processing or billing.

The Gemini adapter uses the official GenerateContent REST protocol through Go's
standard HTTP client. It sends keys in headers, refuses redirects, makes no
automatic provider retries, and preserves opaque thought signatures privately
for multi-turn compatibility. It does not expose model thought text. The fixed
Google endpoint can be replaced only by an injected transport in tests; no
runtime endpoint flag accepts arbitrary destinations.

Structured events include model turn/request counts, elapsed time, reported
input/output tokens, tool names, job ID, and attempt identity. They exclude API
keys, complete prompts, provider bodies, and private thoughts. Provider errors
are stored as stable codes such as `provider_failure`, `provider_timeout`, or
`provider_response_invalid`. Invalid actions/results and exhausted limits use
`agent_invalid_action`, `agent_invalid_result`, and `agent_budget_exhausted`.

## Database profile and upgrades

Schema migration 002 adds an execution profile to shared settings. Existing v1
databases are labeled `fixture-v1`; their records, results, keys, attempt history,
and five-second per-job budgets are preserved. Stop old API/worker/database-tool
processes before applying the schema upgrade; older binaries reject v2 on their
next startup and should not run alongside the new binaries.

The Gemini profile fingerprints the requested model name, prompt, tool schemas,
runtime limits, and adapter behavior version. API, worker, and database tool must
use the same profile. Changing a model/prompt or mixing fixture and Gemini workers
causes startup rejection. A provider alias can still change remotely: the profile
identifies our requested configuration, not the provider's underlying weights.

Timeout is also snapshotted onto each job when admitted. Changing flags cannot
retroactively extend existing jobs. This increment has no live policy-upgrade
command; use the separate development database above. For Gemini database-tool
operations, pass `-executor gemini` and the same model/environment too.

Only lease-loss recovery retries automatically, up to the shared attempt limit.
A recovered attempt starts a new conversation and budget, and can repeat model
charges. Confirmed provider errors and execution timeouts remain terminal. A
valid job result means the runtime accepted its structure and citations; model
quality is evaluated separately.

## Tests and live evaluation

The normal suite uses scripted model turns and local HTTP provider fixtures. It
makes no requests to Google. With a disposable `TEST_DATABASE_URL`, it additionally
runs the real HTTP handler, PostgreSQL store, worker, agent, and Gemini transport
together; tests cover success, missing evidence, terminal provider failure,
idempotent replay, and provider cancellation after lease loss.

```sh
go test ./...
go vet ./...
TEST_DATABASE_URL='your-disposable-test-database-url' go test -race -count=1 ./...
```

List evaluation cases and review expectations without credentials or provider
access:

```sh
go run ./cmd/evaluate -list
```

The suite varies evidence while keeping the initial model prompt identical:

| Scenario | Evidence and expected behavior | Available through `/jobs` |
| --- | --- | --- |
| `checkout-db-timeout` | Acquisition timeouts and occupied connections support pool exhaustion; its underlying cause is unknown | Yes |
| `checkout-insufficient-evidence` | Missing logs and connection metrics prevent identifying the cause | Yes |
| `checkout-payment-timeout` | Payment gateway timeouts with healthy pool capacity support a different immediate failure; the gateway's underlying problem is unknown | Evaluation only |
| `checkout-conflicting-evidence` | Same-window logs and pool metrics disagree; source/pool identity must be reconciled before confirming a cause | Evaluation only |

The two new cases stay outside API admission to preserve existing durable job
and worker compatibility. Existing evidence, prompts, timeouts, and database
profiles are unchanged. Expanding the API workload would need versioned worker
compatibility; allowing new IDs with old workers could fail accepted jobs.

The opt-in command defaults to one investigation per case, with at most four
model requests each. It runs without PostgreSQL, the API, or worker processes:

```sh
# Load GEMINI_API_KEY and GEMINI_MODEL locally first. May incur provider charges.
go run ./cmd/evaluate -live

# Repeat all four cases three times (12 investigations; at most 48 requests).
mkdir -p .local/evaluations
go run ./cmd/evaluate -live -runs 3 > .local/evaluations/checkout.json

# Select just one case for a smaller run.
go run ./cmd/evaluate -live -scenario checkout-payment-timeout -runs 1
```

`-runs` accepts 1 through 10; `-scenario` selects one known case. Default is all
four. Work is sequential in suite order for each repetition, without automatic
retries. Every investigation starts a fresh model conversation with a 30-second
deadline and the existing four-request/six-read limits. The maximum selection is
40 investigations and 160 requests. Quota errors are recorded as failures;
consider selecting a single case when testing a provider's free quota.

Progress and the maximum request count go to stderr. A single JSON report goes
to stdout at completion or cooperative cancellation (Ctrl+C/SIGTERM). Forced
termination or output failure can prevent saving a report. Each record contains
the scenario, repetition number, elapsed milliseconds, and a full diagnosis with
checks or a sanitized execution error. The report also includes:

- Requested model, worker execution profile, UTC start time, and deadline.
- Schema version, suite version, and a fingerprint of synthetic observations and
  published review expectations. Bump the suite version when changing rubric
  logic; it is separate from the worker profile and does not pin provider weights.
- Requested and completed investigations, heuristic passes/failures, execution
  failures, and interruption status. Completed includes failed/canceled attempts
  that returned; unstarted work remains visible in the requested count.

The three checks are expected finding, stated uncertainty, and every required
diagnostic citation. Missing-evidence cases must cite both missing signals.
Execution errors do not count as heuristic failures or successes. A failing
check, execution error, or interrupted/partial run exits nonzero and sets
`rubric_passed` to false; the report is still printed. Request/call timeouts and
whole-investigation timeouts retain separate failure classifications.

These are English substring checks. They can reject valid paraphrases and
accept negated or contradictory prose. Human review of complete diagnoses is
required even when every check passes. Counts describe this selected synthetic
suite, not production accuracy. Per-investigation latency excludes queueing and
database work; it is not a service throughput or latency benchmark.

Offline tests cover repeated runs, filtering, invalid limits, classified errors,
partial cancellation, and fresh provider conversations across all four evidence
cases. The real agent/Gemini adapter runs against a local HTTP model fixture;
those scripted diagnoses verify data flow, not live model reasoning.

On 2026-10-19, a manually run evaluation reported with `gemini-3.8-flash` passed
both scenarios and all rubric checks. The supplied output correctly identified
connection-pool exhaustion without asserting its underlying cause, and declined
to confirm exhaustion when logs and connection metrics were unavailable. This
establishes one successful reported live baseline. The expanded four-case suite
was implemented and tested offline on 2026-10-25; no live provider calls were
made for that increment. Repeated-run model consistency, results on the two new
cases, broader incident accuracy, and live worker-path behavior remain unmeasured.

Protocol references: [function calling](https://ai.google.dev/gemini-api/docs/generate-content/function-calling),
[FunctionDeclaration](https://ai.google.dev/api/generate-content#FunctionDeclaration),
and [thought signatures](https://ai.google.dev/gemini-api/docs/generate-content/thought-signatures).
