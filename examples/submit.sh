#!/usr/bin/env bash
# Submit a local checkout to a running scheduler.
set -euo pipefail
: "${PACT_ADDR:=http://127.0.0.1:8080}"
: "${PACT_WORKDIR:?PACT_WORKDIR is required (a local checkout)}"

args=(submit --addr "$PACT_ADDR" --workdir "$PACT_WORKDIR" --event push --wait)
if [ -n "${PACT_TOKEN:-}" ]; then
  args+=(--token "$PACT_TOKEN")
fi
exec act "${args[@]}"
