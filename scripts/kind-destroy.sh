#!/usr/bin/env bash
source "$(dirname -- "${BASH_SOURCE[0]}")/kind-common.sh"
[[ "${1:-}" == --delete-data ]] || { printf '%s\n' 'Explicit teardown deletes cluster database data. Use --delete-data.' >&2; exit 1; }
require_tools
require_owned
kind delete cluster --name "$cluster_name" --kubeconfig "$kubeconfig_path"
rm -f -- "$state_dir/owned" "$kubeconfig_path"
