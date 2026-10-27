#!/usr/bin/env bash
source "$(dirname -- "${BASH_SOURCE[0]}")/kind-common.sh"
require_tools
require_owned
mode="${1:-fixture}"
[[ "$mode" == fixture || "$mode" == gemini ]] || { printf '%s\n' 'Use fixture or gemini.' >&2; exit 1; }
namespace_for "$mode"
database_env="${DATABASE_ENV_FILE:-$project_root/.env.kubernetes}"
check_env_file "$database_env" 'POSTGRES_PASSWORD DATABASE_URL'
model=''
if [[ "$mode" == gemini ]]; then
  gemini_env="${GEMINI_ENV_FILE:-$project_root/.env.kubernetes.gemini}"
  check_env_file "$gemini_env" 'GEMINI_MODEL GEMINI_API_KEY'
  while IFS='=' read -r key value || [[ -n "$key" ]]; do
    [[ "$key" != GEMINI_MODEL ]] || model="${value%$'\r'}"
  done < "$gemini_env"
  [[ "$model" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] || { printf '%s\n' 'Configure a valid GEMINI_MODEL.' >&2; exit 1; }
fi
if kube get namespace "$namespace" >/dev/null 2>&1; then
  owner="$(kube get namespace "$namespace" -o 'jsonpath={.metadata.labels.app\.kubernetes\.io/managed-by}')"
  [[ "$owner" == agent-kind ]] || { printf '%s\n' 'Namespace is not owned by this setup.' >&2; exit 1; }
  old_mode="$(kube get configmap app-config -n "$namespace" --ignore-not-found -o 'jsonpath={.data.EXECUTOR}')"
  old_model="$(kube get configmap app-config -n "$namespace" --ignore-not-found -o 'jsonpath={.data.GEMINI_MODEL}')"
  [[ -z "$old_mode" || ( "$old_mode" == "$mode" && "$old_model" == "$model" ) ]] || {
    printf '%s\n' 'Executor/model changes require a fresh namespace and database; existing profile was preserved.' >&2; exit 1;
  }
else
  printf '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"%s","labels":{"app.kubernetes.io/managed-by":"agent-kind","app.kubernetes.io/part-of":"distributed-agent-service"}}}\n' "$namespace" | kube create -f -
fi
kubectl --kubeconfig "$kubeconfig_path" --context "kind-$cluster_name" -n "$namespace" create secret generic database \
  --from-env-file="$database_env" --dry-run=client -o json | kube apply --server-side --field-manager=agent-kind -f -
if [[ "$mode" == gemini ]]; then
  kubectl --kubeconfig "$kubeconfig_path" --context "kind-$cluster_name" -n "$namespace" create secret generic gemini \
    --from-env-file="$gemini_env" --dry-run=client -o json | kube apply --server-side --field-manager=agent-kind -f -
fi
image="${APP_IMAGE:-distributed-agent-service:kind-$(date -u +%Y%m%d%H%M%S)-$RANDOM}"
[[ "$image" =~ ^distributed-agent-service:[a-zA-Z0-9][a-zA-Z0-9._-]{0,100}$ ]] || { printf '%s\n' 'APP_IMAGE must be a tagged distributed-agent-service image.' >&2; exit 1; }
if [[ "${SKIP_BUILD:-0}" != 1 ]]; then docker build -t "$image" "$project_root"; fi
docker image inspect "$image" >/dev/null
kind load docker-image --name "$cluster_name" "$image"
render_dir="$(mktemp -d "$state_dir/render.XXXXXX")"
cleanup_render() {
  local resolved
  resolved="$(realpath -- "$render_dir")"
  case "$resolved" in
    "$state_dir"/render.*) rm -rf -- "$resolved" ;;
    *) printf '%s\n' 'Render directory left outside the expected state path; cleanup refused.' >&2 ;;
  esac
}
trap cleanup_render EXIT
overlay_relative="$(realpath --relative-to="$render_dir" "$project_root/deploy/kubernetes/overlays/$mode")"
cat > "$render_dir/kustomization.yaml" <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - $overlay_relative
namespace: $namespace
images:
  - name: distributed-agent-service
    newTag: ${image#*:}
configMapGenerator:
  - name: app-config
    behavior: merge
    literals:
      - GEMINI_MODEL=$model
generatorOptions:
  disableNameSuffixHash: true
EOF
manifest="$state_dir/$namespace.yaml"
kubectl kustomize "$render_dir" > "$manifest"
kube apply --dry-run=server -f "$manifest" >/dev/null
kube apply -f "$manifest"
if ! kube rollout status statefulset/postgres -n "$namespace" --timeout=240s --request-timeout=260s || \
   ! kube rollout status deployment/api -n "$namespace" --timeout=240s --request-timeout=260s || \
   ! kube rollout status deployment/worker -n "$namespace" --timeout=240s --request-timeout=260s; then
  kube get pods,pvc -n "$namespace"
  kube get events -n "$namespace" --sort-by=.lastTimestamp | tail -n 20
  printf '%s\n' 'Deployment did not become ready; inspect init/application logs with the scoped kubectl wrapper.' >&2
  exit 1
fi
printf '%s\n' "$image" > "$state_dir/$namespace.image"
docker image inspect "$image" --format '{{.Id}}' > "$state_dir/$namespace.image-id"
printf 'Ready: namespace=%s image=%s\n' "$namespace" "$image"
