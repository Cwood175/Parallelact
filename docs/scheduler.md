# Parallel runs with `act serve`

> See [README.scheduler.md](../README.scheduler.md) for the implementation
> principles, a quickstart and a detailed caller guide. This page is the compact
> API/flag reference.

`act serve` turns act into a small CI scheduler: it accepts run requests over HTTP
and executes their jobs in parallel, each job instance in its own container.

This is useful when many commits of different repositories have to be validated at
the same time. The classic CLI processes one plan per invocation; the scheduler
keeps a global pool of containers busy instead.

## Concepts

- **Run** – one request: a local checkout (`workdir`), an event and optional
  metadata (`repo`, `commit`). The scheduler never clones anything, the caller
  provides the checkout.
- **Job instance** – one job of a workflow expanded with one matrix combination.
  A job instance runs in exactly one container. Each leg has its own result; a
  failed leg does not mark its siblings as failed (the matrix *group* remains
  failed for `needs`/run status).
- **Container limit** – at most `--max-containers` job containers run at the same
  time across *all* runs. The limit is enforced where job containers are created,
  so matrix instances and child jobs of reusable workflows count as well.
- **Stages** – the `needs` relationships of a run are honoured: a stage only starts
  when the previous one finished. Jobs of different runs are not ordered.
- **Isolation** – containers, volumes and networks are named with the run id, every
  run gets its own `GITHUB_RUN_ID` (used for artifact isolation) and cache server,
  every job instance gets its own toolcache volume, and every job writes its own
  log file.

## Starting the scheduler

```bash
act serve \
  --addr 127.0.0.1:8080 \
  --data-dir /var/lib/act-serve \
  --max-containers 20 \
  --allowed-workdir-roots /srv/ci \
  -P ubuntu-latest=catthehacker/ubuntu:act-latest
```

Important flags:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--addr` | `127.0.0.1:8080` | HTTP listen address |
| `--data-dir` | `<cache>/serve` | logs, per-run caches, artifacts |
| `--max-containers` | `20` | global cap of job containers (`PACT_MAX_CONTAINERS` in `deploy/start-serve.sh`) |
| `--workers` | `max-containers + 8` | worker goroutines executing jobs |
| `--job-cpus` | `4` | CPU budget of every job container (quota, affinity window, thread pool env); `0` disables it |
| `--max-queued-runs` | `100` | admitted (queued + running) runs |
| `--max-units-per-run` | `200` | job instances per run |
| `--run-timeout` | `0` | default timeout of a run |
| `--allowed-workdir-roots` | current directory | comma separated roots a request may execute |
| `--api-token` | – | bearer token, required for non loopback addresses |
| `--log-stdout` | `false` | mirror job logs to stdout |
| `--shared-toolcache` | `false` | share one toolcache volume between runs |
| `--per-run-action-cache` | `false` | isolate the action cache per run |

The run related flags of the CLI (`-P`, `-s`, `--env`, `--var`, `--pull`, ...) are
available on `serve` as well; they configure the base that every request inherits.

Secrets are loaded once at startup (`--secret-file`, `-s NAME=value`, environment)
and are only referenced by name in requests. They are masked in logs.

## Submitting a run

```bash
act submit \
  --addr http://127.0.0.1:8080 \
  --workdir /srv/ci/myrepo \
  --repo org/myrepo \
  --commit 4a526842b67e1c1a465e5a1d9d575305dfdbb274 \
  --event pull_request \
  --event-file event.json \
  --workflows-path /srv/ci/myrepo/.github/workflows/ci.yml \
  --matrix python-version:3.12 \
  --wait
```

Or directly with curl:

```bash
curl -sS -X POST http://127.0.0.1:8080/v1/runs \
  -H 'Content-Type: application/json' \
  -d '{
        "repo": "org/repo",
        "commit": "4a52684",
        "workdir": "/srv/ci/repo",
        "workflowsPath": ".github/workflows/ci.yml",
        "event": "pull_request",
        "eventJSON": {"action": "synchronize"},
        "matrix": {"python-version": ["3.12"]},
        "secretRefs": ["GITHUB_TOKEN"],
        "timeoutSeconds": 1800,
        "priority": 0
      }'
```

The response is `202 Accepted` with a snapshot of the run. `jobs` is usually empty
until stages are expanded; poll `GET /v1/runs/{id}` for the terminal state. Each
job then carries both `status` (`success`, `failure`, `cancelled`, ...) and a
rule-only binary `result` (`success` or `failure`; `cancelled` maps to `failure`).
`act serve` does not classify environment vs. business failures; that is done by
the verification layer from the job logs. The current verification path is a
pure-LLM classifier with a structured prompt (no regex rule decides the final
environment class); it returns `error_class`, `category`, `confidence`, root
cause step and evidence. Logs below 18 KB are sent in full; larger runs use the
failed job-level log, and runs with multiple failed jobs are first compressed
per job by the LLM (including the decisive raw log excerpt) before the final
classification.

`workdir` must be an absolute path below one of the allowed roots. `workflowsPath`
may be a directory or a single workflow file and must stay inside `workdir`.

## API

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/v1/runs` | admit a run, returns `202` with the run state |
| `GET` | `/v1/runs` | list runs (`limit`, `status`, `repo`, `commit`) |
| `GET` | `/v1/runs/{id}` | run state including its job instances |
| `GET` | `/v1/runs/{id}/logs` | list the log files of a run |
| `GET` | `/v1/runs/{id}/logs?job=KEY` | fetch one job log (`offset=N`, `follow=1` to stream) |
| `POST` | `/v1/runs/{id}/cancel` | cancel a queued or running run |
| `GET` | `/v1/stats` | queue, job and container counters |
| `GET` | `/healthz` | liveness (503 while draining) |

Job states are `queued`, `running`, `success`, `failure` or `cancelled`; `result`
is the binary projection of the terminal state. A run finishes with `failure` when
at least one of its jobs failed.

Proxy/network variables passed to `serve` via `--env` (`*PROXY*`,
`NODE_USE_ENV_PROXY`, `GIT_*`, `JAVA_TOOL_OPTIONS`, `GRADLE_OPTS`, `MAVEN_OPTS`)
are copied into job, action-step and Docker-action containers unless the workflow
already defines the same variable.

## Limits and caveats

- State is in memory only: a restart forgets queued and running runs and can leave
  containers behind. Leftover containers are force removed when a shutdown times
  out, but not after a crash.
- The artifact server is shared by all runs and isolates them by `GITHUB_RUN_ID`.
- `fail-fast` is not enforced by act, only `max-parallel` is.
- Binding the workdir (`--bind`) is rejected by default because concurrent jobs
  would mutate the same checkout.
- `--job-cpus` (default `4`) bounds a job container's CPU quota, gives it a
  rotating 4-CPU affinity window and sets common thread pool variables. It only
  applies to job containers, not to service containers or host mode jobs. Set it
  to `0` when the Docker daemon is remote and its CPU ids may not match.
- Job containers use `--network` (default `host`). With `bridge`, take care of
  docker0 (`192.168.0.0/24` here), not 172.x. With bridge the artifact server
  must listen on a host address reachable from the container (the docker0
  gateway), not `127.0.0.1`. Jobs with `services` get a per-job bridge network.
- The current host deployment uses transparent egress for docker0 containers and
  does not inject `HTTP_PROXY/HTTPS_PROXY/ALL_PROXY`; set
  `PACT_EXPLICIT_PROXY=1` to restore the old explicit-proxy behavior.
- The scheduler is meant for a trusted network: requests can execute any checkout
  below the allowed roots. Always set `--api-token` when listening on a public
  address.
