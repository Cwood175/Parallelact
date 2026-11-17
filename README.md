<p align="center">
  <img alt="act" src="docs/img/act-logo.svg" width="180" />
</p>

# Overview [![checks](https://github.com/nektos/act/actions/workflows/checks.yml/badge.svg?branch=main)](https://github.com/nektos/act/actions/workflows/checks.yml) [![awesome-runners](https://img.shields.io/badge/listed%20on-awesome--runners-blue.svg)](https://github.com/jonico/awesome-runners)

> "Think globally, `act` locally"

Run your [GitHub Actions](https://developer.github.com/actions/) locally! Why would you want to do this? Two reasons:

- **Fast Feedback** - Rather than having to commit/push every time you want to test out the changes you are making to your `.github/workflows/` files (or for any changes to embedded GitHub actions), you can use `act` to run the actions locally. The [environment variables](https://help.github.com/en/actions/configuring-and-managing-workflows/using-environment-variables#default-environment-variables) and [filesystem](https://help.github.com/en/actions/reference/virtual-environments-for-github-hosted-runners#filesystems-on-github-hosted-runners) are all configured to match what GitHub provides.
- **Local Task Runner** - I love [make](<https://en.wikipedia.org/wiki/Make_(software)>). However, I also hate repeating myself. With `act`, you can use the GitHub Actions defined in your `.github/workflows/` to replace your `Makefile`!

> [!TIP]
> **Now Manage and Run Act Directly From VS Code!**<br/>
> Check out the [GitHub Local Actions](https://sanjulaganepola.github.io/github-local-actions-docs/) Visual Studio Code extension which allows you to leverage the power of `act` to run and test workflows locally without leaving your editor.

# How Does It Work?

When you run `act` it reads in your GitHub Actions from `.github/workflows/` and determines the set of actions that need to be run. It uses the Docker API to either pull or build the necessary images, as defined in your workflow files and finally determines the execution path based on the dependencies that were defined. Once it has the execution path, it then uses the Docker API to run containers for each action based on the images prepared earlier. The [environment variables](https://help.github.com/en/actions/configuring-and-managing-workflows/using-environment-variables#default-environment-variables) and [filesystem](https://docs.github.com/en/actions/using-github-hosted-runners/about-github-hosted-runners#file-systems) are all configured to match what GitHub provides.

Let's see it in action with a [sample repo](https://github.com/cplee/github-actions-demo)!

![Demo](docs/img/act-quickstart.gif)

# Act User Guide

Please look at the [act user guide](https://nektosact.com) for more documentation.

# Running many workflows in parallel (PACT)

This fork adds **PACT**, a long-lived scheduler on top of act: it accepts run
requests over HTTP and executes their job instances concurrently, one container
per job instance, under a global container limit. Runs of different
repositories and commits share one worker pool and one set of container slots,
while `needs` stages stay serial inside a run.

- Design and caller guide: [README.scheduler.md](README.scheduler.md)
- Detailed mechanism (terms, cache, network, isolation, cleanup): [docs/mechanism.md](docs/mechanism.md)
- Paper-style description (methodology, pipeline, performance): [docs/pact-paper.md](docs/pact-paper.md)
- Compact API/flag reference: [docs/scheduler.md](docs/scheduler.md)

```bash
# build
go build -o dist/local/act main.go

# start a scheduler (all paths/ports are configurable, see deploy/)
PACT_DATA_DIR=/var/lib/act-serve PACT_WORKDIR_ROOTS=/srv/ci/repos \
  ./deploy/start-serve.sh

# submit a local checkout and wait for the result
act submit --addr http://127.0.0.1:8080 --workdir /srv/ci/repos/repo --event push --wait
```

What the fork adds on top of upstream act:

- job-instance level scheduling (`ExpandRun` / `NewJobExecutor`) with a global
  priority queue and a global container semaphore shared by all runs;
- admission control, per-group `max-parallel` gates, FIFO/priority fairness;
- run-scoped naming, per-run cache server, per-job toolcache volumes, CPU
  budgets and thread-pool defaults for job containers;
- an HTTP API with run/job state, log streaming (`offset`/`follow`) and counters;
- a cleanup janitor that reclaims containers, volumes and networks left by
  cancelled or crashed runs;
- concurrency fixes inside act (job results, step IDs, action fetch, config
  cloning).

Deployment templates live in [deploy/](deploy/); `deploy/start-serve.sh` reads
every path, port, image and limit from environment variables and never contains
credentials. A typical deployment runs one binary on the host and talks to the
host Docker daemon; putting act in a container works too (mount the host socket),
but DinD changes CPU ids, path mapping and cleanup boundaries.

# Support

Need help? Ask in [discussions](https://github.com/nektos/act/discussions)!

# Contributing

Want to contribute to act? Awesome! Check out the [contributing guidelines](CONTRIBUTING.md) to get involved.

## Manually building from source

- Install Go (version in `go.mod`, currently 1.26.0) - (<https://golang.org/doc/install>)
- Clone this repo `git clone git@github.com:nektos/act.git`
- Run unit tests with `make test` (requires Docker)
- Run fast tests without Docker with `make test-fast`
- Build and install: `make install`
