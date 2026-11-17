# PACT — Parallel ACT

PACT (Parallel ACT) is a **parallel scheduler built on top of
[nektos/act](https://github.com/nektos/act)**. It turns the local GitHub Actions
runner into a long-lived service: one HTTP request describes a checkout and an
event, and every **job instance** (a job, or one matrix leg of it) runs in its
own Docker container, under a **global container limit** shared by all requests.

The upstream act CLI runs one plan per process and serializes `needs` stages
inside it. PACT keeps that plan semantics but schedules at job-instance
granularity, so runs of different repositories and commits share one worker pool
while stages inside a run stay ordered.

```
             HTTP / act submit
                     │
                     ▼
        ┌──────── act serve ────────┐
        │ API → Scheduler           │
        │   Queue (priority+FIFO)   │
        │   Semaphore (containers)  │
        │   Store / LogStore        │
        │   Engine → ExecutionPlan  │
        │   cleanup janitor         │
        └─────────┬─────────────────┘
                  │ per-run orchestrator: stage 1 → stage 2 → …
                  ▼
        global worker pool → one container per job instance
```

## Quickstart

Prerequisites: Go (version in `go.mod`), a Docker daemon, and a runner image
(e.g. `catthehacker/ubuntu:act-latest`).

```bash
# 1. build
go build -o dist/local/act main.go

# 2. start the scheduler; every path, port and limit is configurable
PACT_DATA_DIR=/var/lib/act-serve \
PACT_WORKDIR_ROOTS=/srv/ci/repos \
PACT_RUNNER_IMAGE=catthehacker/ubuntu:act-latest \
  ./deploy/start-serve.sh

# 3. submit a local checkout and wait for the result
act submit --addr http://127.0.0.1:8080 \
  --workdir /srv/ci/repos/myrepo \
  --event push --wait
```

Or with curl:

```bash
curl -sS -X POST http://127.0.0.1:8080/v1/runs \
  -H 'Content-Type: application/json' \
  -d '{"workdir":"/srv/ci/repos/myrepo","event":"push","priority":0}'
curl -sS http://127.0.0.1:8080/v1/runs/<id>
curl -sS 'http://127.0.0.1:8080/v1/runs/<id>/logs?job=CI%2Ftest%2F0&follow=1'
```

`deploy/start-serve.sh` reads its configuration from environment variables
(`PACT_ADDR`, `PACT_DATA_DIR`, `PACT_WORKDIR_ROOTS`, `PACT_MAX_CONTAINERS`,
`PACT_JOB_CPUS`, `PACT_NETWORK`, `PACT_RUNNER_IMAGE`, `PACT_TOKEN_FILE`,
`PACT_SECRET_FILE`, `PACT_PLATFORMS`, `PACT_EXTRA_ARGS`, …) and never contains
credentials.

## What PACT adds to act

- **Job-instance scheduling** — `ExpandRun` expands a job into matrix instances
  without executing it, `NewJobExecutor` builds one execution chain per instance.
- **Global resource control** — a global priority queue, a global container
  semaphore (`--max-containers`), admission control (`--max-queued-runs`) and
  per-group `max-parallel` gates.
- **Isolation** — containers, volumes and networks are named after the run id;
  each run gets its own `GITHUB_RUN_ID` and cache server; each job instance gets
  its own toolcache volume and log file.
- **CPU budgets** — each job container gets a CFS quota, a rotating CPU affinity
  window and common thread-pool environment variables (`--job-cpus`, default 4).
- **Observability** — `GET /v1/runs`, `/v1/runs/{id}`, `/v1/runs/{id}/logs`
  (`offset`/`follow`), `/v1/stats` and `/healthz`.
- **Crash-safe cleanup** — a janitor removes leftover containers, volumes,
  networks and act-built images after cancelled or killed runs, including after
  a scheduler restart (persisted run-id log).
- **Concurrency fixes inside act** — job results, step IDs, action fetch and
  clone, config cloning, log masking.

## Documentation

| Document | Content |
| --- | --- |
| [README.scheduler.md](README.scheduler.md) | design and implementation notes, caller guide, measured runs |
| [docs/mechanism.md](docs/mechanism.md) | terminology, architecture, concurrency, caching, networking, isolation and cleanup |
| [docs/pact-paper.md](docs/pact-paper.md) | paper-style description: methodology, pipeline and performance analysis |
| [docs/scheduler.md](docs/scheduler.md) | compact API and flag reference |
| [docs/upstream-README.md](docs/upstream-README.md) | the original nektos/act README |

## Configuration and limits

- Every request is executed inside a checkout on the scheduler host; PACT never
  clones or fetches repositories.
- `--allowed-workdir-roots` bounds which checkouts may be executed; binding the
  host workdir (`--bind`) is rejected by default because concurrent jobs would
  share it.
- State is kept in memory: a restart forgets queued/running runs. Logs, caches
  and artifacts persist under `--data-dir`.
- `fail-fast` is not enforced by act; `strategy.max-parallel` is.
- Stages of one run are strictly serial; runs are independent and share the
  container pool.
- Secrets are loaded at startup (`--secret-file`, `-s NAME=value`, environment)
  and referenced by name only; values never appear in API responses and are
  masked in logs.
- Binding a non-loopback address requires `--api-token`.

## Building, testing and development

```bash
make build          # dist/local/act
make test-fast      # go test -short ./... (no Docker)
make test           # full tests (needs a Docker daemon)
make lint-go        # golangci-lint
```

Per-package details are in [AGENTS.md](AGENTS.md). Deployment files live in
[deploy/](deploy/): a start script, a systemd unit and secret templates.

## License and attribution

PACT is a fork of [nektos/act](https://github.com/nektos/act), Copyright (c)
2019, licensed under the MIT License. The scheduler, the `act serve` /
`act submit` commands and the runner extensions are distributed under the same
license. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
