#!/usr/bin/env bash
set -euo pipefail
if [[ -z "${TEST_DATABASE_URL:-}" ]]; then
  printf '%s\n' 'Set TEST_DATABASE_URL to a disposable PostgreSQL database; tests create isolated schemas.' >&2
  exit 1
fi
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
go test -race -count=1 ./...
