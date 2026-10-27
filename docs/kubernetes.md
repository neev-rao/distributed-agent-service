# Local Kubernetes deployment

The existing Go image runs on a single-node kind cluster with one API replica,
two worker replicas, and one PostgreSQL instance. This is a local development
deployment. PostgreSQL still owns jobs and lease decisions; Kubernetes maintains
Pods and restarts processes. No application scheduler or queue was added.

## Requirements and scope

Use Linux or Ubuntu WSL with Bash, Docker, kind, kubectl, and GNU core utilities
on PATH. The verified tools are kind v0.33.0 and kubectl/Kubernetes v1.37.0.
The node image is pinned by digest in `deploy/kubernetes/kind.yaml`. The setup
checks for at least 4 GiB assigned to Docker and the kind `standard` storage
class. First use downloads container images; the application needs no host Go
installation. Go is required for the opt-in verification commands.

Install tools using the official [kind installation guide](https://kind.sigs.k8s.io/docs/user/quick-start/)
and [kubectl installation guide](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/).
Use the kind release's published node digest when changing Kubernetes versions.
The scripts do not install tools or enable Docker Desktop's Kubernetes feature.

## Start the fixture deployment

From the repository root, create an ignored credentials file if absent:

```sh
test -f .env.kubernetes || cp .env.kubernetes.example .env.kubernetes
# Edit .env.kubernetes locally. Replace the development password consistently.
bash scripts/kind-up.sh
bash scripts/kind-deploy.sh fixture
bash scripts/kind-kubectl.sh get pods,pvc -n agent-fixture
bash scripts/kind-kubectl.sh -n agent-fixture port-forward --address=127.0.0.1 service/api 8080:8080
```

In another terminal, use the existing POST/GET job API on `127.0.0.1:8080`.
There is no NodePort, Ingress, or public load balancer. Port-forwarding selects an
API Pod; restart that command after the selected Pod is replaced.

Credentials files contain the exact unquoted `KEY=value` pairs in their examples;
they are read as data and never sourced as shell code. URL-encode special
characters in URL passwords. The database user and database are `agentservice`;
the in-namespace database address is `postgres:5432`.

`CLUSTER_NAME` defaults to `agent-service`. Every script uses its dedicated
`.local/kubernetes/<cluster>/kubeconfig` and explicit context. Existing unrelated
clusters cannot be adopted without a project ownership record. Docker node
ownership labels are checked before project operations and destruction. The
scoped kubectl wrapper rejects connection overrides. No global kubeconfig or
default context is changed.

`DATABASE_ENV_FILE` selects another ignored database file. `NAMESPACE` selects an
`agent-...` namespace; defaults are `agent-fixture` and `agent-gemini`. Existing
namespace ownership and executor/model are checked before configuration changes.
Use a fresh namespace/database for a different model or executor. Shared policy
mismatches remain startup errors rather than implicit database upgrades.

## Images, configuration, and startup

The deploy script builds a uniquely tagged image, loads it into kind, renders the
selected Kustomize overlay, validates it against the API server, and applies it.
`APP_IMAGE` selects an explicit `distributed-agent-service:<tag>`; `SKIP_BUILD=1`
reuses an already built image. Application Pods use `imagePullPolicy: Never`,
so the exact image must have been loaded into kind. PostgreSQL uses a pinned
official image digest. Image references/IDs and rendered non-secret resources
are saved under the cluster's ignored state directory.

Non-secret shared policy lives in a ConfigMap. Database credentials are created
as a Kubernetes Secret from the ignored file. The provider key is referenced only
by the Gemini worker container. Credentials are neither build arguments nor
tracked manifests. Avoid printing Secret objects or runtime environments into
shared logs. Updating a Secret does not itself reload existing process variables;
changing an existing PostgreSQL role password requires an explicit rotation.

Each API/worker Pod first runs `/app/dbtool` as an init container to apply/verify
schema and execution policy. Concurrent initializers use the existing SQL
migration lock. Kubernetes retries failed init containers; the deploy script has
bounded rollout waits and reports Pods/PVCs/events on failure. Application startup
also rechecks policy, covering failures between initialization and process start.

The API has an internal Service. Workers pull jobs directly from PostgreSQL and
have no inbound dispatch Service. API and worker containers retain non-root,
read-only filesystems, dropped capabilities, and disabled privilege escalation.
Service-account tokens are not mounted. CPU/memory requests guide scheduling;
limits bound individual containers. PostgreSQL has a separate writable data
volume and runs as its image's PostgreSQL user.

## Health, scaling, and storage

Startup/liveness probes call `/livez`. Readiness calls `/readyz`, which includes
bounded database reachability and drain state. Provider access is not probed.
PostgreSQL uses `pg_isready` for startup/readiness. See the
[Kubernetes probe contract](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/).

The API has a ten-second application drain inside a fifteen-second Pod grace;
workers have forty seconds inside forty-five seconds. Workers stop claims on
SIGTERM through existing Go lifecycle logic. Readiness is an operational signal,
not an instruction to stop claiming jobs. Detailed worker-pod failure and
termination guarantees require the following recovery slice's demonstrations.

```sh
bash scripts/kind-kubectl.sh scale deployment/worker -n agent-fixture --replicas=3
```

Two worker replicas provide four steady-state slots. Each process retains its
two-slot bound; scaling/rollouts may overlap draining and replacement Pods.
There is no global execution-slot cap. Waiting-job and retained-record limits
remain globally enforced by PostgreSQL.

The single PostgreSQL StatefulSet mounts a 2 GiB claim at `/var/lib/postgresql`
for PostgreSQL 18. Replacing its Pod reuses that claim. The claim is local to the
kind node: deleting the claim, namespace, or cluster can delete data. This is
Pod-replacement persistence, not database high availability or backup recovery.

Stop application Pods without deleting database storage:

```sh
bash scripts/kind-kubectl.sh scale deployment/api deployment/worker -n agent-fixture --replicas=0
# Restore declared replica counts:
bash scripts/kind-deploy.sh fixture
```

Explicitly destroy the project cluster and its database data only when intended:

```sh
bash scripts/kind-destroy.sh --delete-data
```

This targets only the ownership-checked named kind cluster. Compose databases,
volumes, and unrelated clusters are not part of that operation.

## Gemini namespace

```sh
test -f .env.kubernetes.gemini || cp .env.kubernetes.gemini.example .env.kubernetes.gemini
# Configure model/key locally, keeping them out of chat and Git.
bash scripts/kind-deploy.sh gemini
bash scripts/kind-kubectl.sh get pods,pvc -n agent-gemini
bash scripts/kind-kubectl.sh -n agent-gemini port-forward --address=127.0.0.1 service/api 8081:8080
```

The Gemini overlay sets the executor and thirty-second attempt budget for API,
workers, and migration initialization. Its database/volume is separate from
fixture mode. A healthy idle pool confirms configuration and storage access;
it does not validate provider authentication, quota, or model quality. Submit
a job explicitly to make potentially billable model calls. See [agent.md](agent.md)
for live evaluation and failure semantics. Namespaces separate local resources;
this deployment does not claim tenant isolation or production authentication.

## Verification and diagnostics

```sh
go test -tags=kubernetes -v -count=1 -timeout=20m ./internal/kubetest
```

This opt-in test creates a uniquely named cluster and private kubeconfig, uses
only disposable credentials, and tests jobs through a loopback Service
port-forward. It checks both workers/four slots, idempotency, reapplication,
API replacement, and PostgreSQL Pod replacement retaining the same PVC/result.
An idle Gemini namespace uses placeholders and receives no jobs. The test checks
provider-key scope, profile-change rejection, and unrelated kubeconfig integrity.
Its owned cluster/data/image are removed afterward. Normal tests do not create
clusters or require Kubernetes tools. No live model requests occur in this test.

Verified on 2026-10-26: the real-cluster test passed in 184 seconds with kind
v0.33.0, Kubernetes/kubectl v1.37.0, and PostgreSQL 18.6. Full ordinary and
race-checked Go suites each passed 257 test/subtest results with no skipped tests
or detected races. Formatting, static analysis, all five executable builds, and
the existing Compose lifecycle regression passed. Additional refusal checks
covered connection overrides, unowned namespaces, duplicate credentials,
executor switching against existing data, and teardown without explicit consent.
These checks establish basic deployment/persistence behavior. Worker-pod crash
recovery and detailed termination correctness remain the next slice.

```sh
bash scripts/kind-kubectl.sh get pods,pvc -n agent-fixture
bash scripts/kind-kubectl.sh get events -n agent-fixture --sort-by=.lastTimestamp
bash scripts/kind-kubectl.sh logs deployment/api -n agent-fixture -c migrate
bash scripts/kind-kubectl.sh logs deployment/worker -n agent-fixture -c worker
```

Init failures commonly indicate database availability, credentials, or a profile
mismatch. Pending claims indicate a storage/scheduling problem. Missing app
images require loading the selected tag into this cluster. The setup never
falls back to memory or resets a volume to fix these conditions.
