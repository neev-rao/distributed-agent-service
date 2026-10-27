#!/usr/bin/env bash
source "$(dirname -- "${BASH_SOURCE[0]}")/kind-common.sh"
require_tools
require_owned
for arg in "$@"; do
  case "$arg" in
    --context|--context=*|--kubeconfig|--kubeconfig=*|-s*|--server|--server=*|--cluster|--cluster=*|--user|--user=*|--proxy-url|--proxy-url=*)
      printf '%s\n' 'Connection overrides are disabled in the project kubectl wrapper.' >&2; exit 1 ;;
  esac
done
# Preserve kubectl's normal streaming semantics for port-forward/logs/exec.
exec kubectl --kubeconfig "$kubeconfig_path" --context "kind-$cluster_name" "$@"
