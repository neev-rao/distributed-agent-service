#!/usr/bin/env bash
set -euo pipefail
umask 077
project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cluster_name="${CLUSTER_NAME:-agent-service}"
if [[ ! "$cluster_name" =~ ^[a-z0-9][a-z0-9-]{0,38}[a-z0-9]$ ]]; then
  printf '%s\n' 'CLUSTER_NAME must be 2..40 lowercase letters/digits/hyphens.' >&2; exit 1
fi
state_dir="$project_root/.local/kubernetes/$cluster_name"
kubeconfig_path="$state_dir/kubeconfig"
export KIND_EXPERIMENTAL_PROVIDER=docker

require_tools() {
  local tool
  for tool in docker kind kubectl; do
    command -v "$tool" >/dev/null || { printf 'Required tool missing: %s\n' "$tool" >&2; exit 1; }
  done
}

require_owned() {
  [[ -f "$state_dir/owned" && "$(cat "$state_dir/owned")" == "$cluster_name" ]] || {
    printf '%s\n' 'No ownership record for this cluster; run kind-up.sh for a new project cluster.' >&2; exit 1;
  }
  local node owner nodes
  nodes="$(kind get nodes --name "$cluster_name")"
  while IFS= read -r node; do
    [[ -n "$node" ]] || continue
    owner="$(docker inspect --format '{{index .Config.Labels "io.x-k8s.kind.cluster"}}' "$node")"
    [[ "$owner" == "$cluster_name" ]] || { printf '%s\n' 'Cluster ownership mismatch.' >&2; exit 1; }
  done <<< "$nodes"
}

kube() {
  kubectl --kubeconfig "$kubeconfig_path" --context "kind-$cluster_name" --request-timeout=20s "$@"
}

namespace_for() {
  namespace="${NAMESPACE:-agent-$1}"
  [[ "$namespace" =~ ^agent-[a-z0-9][a-z0-9-]{0,48}[a-z0-9]$ ]] || {
    printf '%s\n' 'NAMESPACE must start with agent- and contain lowercase letters/digits/hyphens.' >&2; exit 1;
  }
}

check_env_file() {
  local file="$1" allowed="$2" key value line count=0
  local -A seen=()
  [[ -f "$file" ]] || { printf 'Configure the ignored environment file: %s\n' "$file" >&2; exit 1; }
  # Values are data. Never source a credentials file as shell code.
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "$line" || "$line" == \#* ]] && continue
    [[ "$line" == *=* ]] || { printf '%s\n' 'Expected unquoted KEY=value entries.' >&2; exit 1; }
    key="${line%%=*}"; value="${line#*=}"
    [[ "$key" =~ ^[A-Z_]+$ && " $allowed " == *" $key "* && -n "$value" && -z "${seen[$key]:-}" ]] || {
      printf '%s\n' 'Unexpected/duplicate key or empty value in credentials file.' >&2; exit 1;
    }
    seen[$key]=1; count=$((count+1))
  done < "$file"
  [[ "$count" == 2 ]] || { printf '%s\n' 'Credentials file must contain exactly the two documented keys.' >&2; exit 1; }
}
