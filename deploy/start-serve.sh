#!/usr/bin/env bash
# Start the parallel act scheduler on the host (no Docker-in-Docker).
#
# Everything deployment specific comes from the environment; no credentials or
# host paths are hardcoded.
#
# Required:
#   PACT_DATA_DIR          directory for logs, per-run caches and artifacts
#   PACT_WORKDIR_ROOTS     comma separated directories requests may execute
# Optional:
#   PACT_ADDR              listen address (default 127.0.0.1:8080)
#   PACT_TOKEN_FILE        file with the bearer token (required for non-loopback)
#   PACT_SECRET_FILE       file with NAME=value secrets (mode 0600)
#   PACT_MAX_CONTAINERS    global job container limit (default 20)
#   PACT_JOB_CPUS          CPU budget per job container (default 4, 0 disables)
#   PACT_WORKERS           worker goroutines (default max-containers + 8)
#   PACT_NETWORK           docker network (default host)
#   PACT_RUNNER_IMAGE      image for ubuntu-latest / ubuntu-24.04
#   PACT_PLATFORMS         extra "-P tag=image" pairs, space separated
#   PACT_EXTRA_ARGS        additional act serve flags (word split)
#   PACT_LOG_STDOUT        mirror job logs to stdout (1/0)
set -euo pipefail

: "${PACT_DATA_DIR:?PACT_DATA_DIR is required}"
: "${PACT_WORKDIR_ROOTS:?PACT_WORKDIR_ROOTS is required}"

PACT_ADDR=${PACT_ADDR:-127.0.0.1:8080}
PACT_MAX_CONTAINERS=${PACT_MAX_CONTAINERS:-20}
PACT_JOB_CPUS=${PACT_JOB_CPUS:-4}
PACT_NETWORK=${PACT_NETWORK:-host}
PACT_RUNNER_IMAGE=${PACT_RUNNER_IMAGE:-catthehacker/ubuntu:act-latest}
ACT_BIN=${ACT_BIN:-$(cd "$(dirname "$0")/.." && pwd)/dist/local/act}

args=(
  serve
  --addr "$PACT_ADDR"
  --data-dir "$PACT_DATA_DIR"
  --allowed-workdir-roots "$PACT_WORKDIR_ROOTS"
  --max-containers "$PACT_MAX_CONTAINERS"
  --job-cpus "$PACT_JOB_CPUS"
  --network "$PACT_NETWORK"
  -P "ubuntu-latest=$PACT_RUNNER_IMAGE"
  -P "ubuntu-24.04=$PACT_RUNNER_IMAGE"
)

if [ -n "${PACT_TOKEN_FILE:-}" ]; then
  args+=(--api-token "$(cat "$PACT_TOKEN_FILE")")
fi
if [ -n "${PACT_SECRET_FILE:-}" ]; then
  args+=(--secret-file "$PACT_SECRET_FILE")
fi
if [ -n "${PACT_WORKERS:-}" ]; then
  args+=(--workers "$PACT_WORKERS")
fi
if [ "${PACT_LOG_STDOUT:-0}" = "1" ]; then
  args+=(--log-stdout)
fi
if [ -n "${PACT_PLATFORMS:-}" ]; then
  # shellcheck disable=SC2206
  for pair in ${PACT_PLATFORMS}; do
    args+=(-P "$pair")
  done
fi
if [ -n "${PACT_EXTRA_ARGS:-}" ]; then
  # shellcheck disable=SC2206
  args+=(${PACT_EXTRA_ARGS})
fi

mkdir -p "$PACT_DATA_DIR"
exec "$ACT_BIN" "${args[@]}"
