#!/usr/bin/env bash
source "$(dirname -- "${BASH_SOURCE[0]}")/kind-common.sh"
require_tools
docker info >/dev/null
if (( $(docker info --format '{{.MemTotal}}') < 4294967296 )); then
  printf '%s\n' 'This local deployment requires at least 4 GiB assigned to Docker.' >&2; exit 1
fi
if kind get clusters 2>/dev/null | grep -Fxq "$cluster_name"; then
  require_owned
else
  [[ ! -f "$state_dir/owned" ]] || { printf '%s\n' 'Cluster disappeared; inspect its saved state before reusing this name.' >&2; exit 1; }
  mkdir -p "$state_dir"
  printf '%s\n' "$cluster_name" > "$state_dir/owned"
  kind create cluster --name "$cluster_name" --config "$project_root/deploy/kubernetes/kind.yaml" \
    --kubeconfig "$kubeconfig_path" --wait 180s
fi
require_owned
kube wait --for=condition=Ready nodes --all --timeout=180s --request-timeout=200s
kube get storageclass standard >/dev/null || { printf '%s\n' 'The kind standard storage class is required.' >&2; exit 1; }
printf 'Cluster ready: %s; dedicated kubeconfig: %s\n' "$cluster_name" "$kubeconfig_path"
