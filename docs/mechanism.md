# act 并行调度器机制说明

本文介绍本仓库在 [nektos/act](https://github.com/nektos/act) 之上增加的并行调度服务
（`act serve` / `act submit`）是怎么工作的，以及如何调用它。

- 服务做什么：通过 HTTP 接收「一个本地仓库 checkout + 要跑的 workflow」请求，把其中
  每个 job 实例放进一个独立的 Docker 容器执行；多个请求并行处理，全局同时运行的容器
  数有上限。
- 只想调用服务：直接看 [第 7 章 如何调用](#7-如何调用) 和 [第 8 章 Quickstart](#8-quickstart)。
- 相关文档：[`README.scheduler.md`](../README.scheduler.md)（实现细节与多任务实测记录）、
  [`scheduler.md`](scheduler.md)（英文参数速查）。

## 目录

1. [术语表](#1-术语表)
2. [总体架构](#2-总体架构)
3. [并发与调度](#3-并发与调度)
4. [缓存机制](#4-缓存机制)
5. [网络配置](#5-网络配置)
6. [隔离与安全](#6-隔离与安全)
7. [如何调用](#7-如何调用)
8. [Quickstart](#8-quickstart)
9. [示例部署](#9-示例部署)
10. [已知限制与排障](#10-已知限制与排障)
11. [代码索引](#11-代码索引)

---

## 1. 术语表

后文只使用本章定义过的词。术语分三组：GitHub Actions 本身的概念、上游 act 的模块、
本仓库调度器新增的模块。

### 1.1 GitHub Actions 概念

| 术语 | 含义 |
| --- | --- |
| workflow | 仓库 `.github/workflows/*.yml` 里的一个 YAML 文件，描述一次 CI 要做的事。 |
| job | workflow 里 `jobs:` 下的一项。一个 job 在一台 runner 机器（这里是一个容器）上顺序执行若干 step。 |
| step | job 里的一步：要么是一条 shell 命令（`run:`），要么调用一个 action（`uses:`）。 |
| action | 可复用的步骤包，例如 `actions/checkout@v4`、`actions/setup-python@v5`，本质是一个 git 仓库。 |
| `runs-on` | job 声明自己要在哪种机器上跑，如 `ubuntu-latest`。act 用它查找对应的 Docker 镜像。 |
| `needs` | job 之间的依赖。`needs: build` 表示等 `build` 结束后才开始。 |
| matrix（矩阵） | `strategy.matrix` 声明的参数组合，例如 `python: [3.11, 3.12]` 会把一个 job 展开成 2 份。 |
| matrix leg | 矩阵展开后的单个组合，即上例中的「python=3.11」这一份。 |
| `max-parallel` | `strategy.max-parallel`，同一个矩阵 job 最多同时运行几个 leg。 |
| event / event payload | 触发 workflow 的事件名（`push`、`pull_request`…）和描述事件的 JSON，workflow 里通过 `github.event` 读取。 |
| service 容器 | job 的 `services:` 声明的辅助容器（如数据库），与 job 共享一个网络。 |
| reusable workflow | 被其他 workflow 通过 `jobs.<id>.uses:` 调用的 workflow。父 job 自身不跑命令，由它的子 job 执行。 |
| secrets / vars / inputs | 分别是机密值（`secrets.X`）、普通变量（`vars.X`）、`workflow_dispatch` 等事件的输入参数（`inputs.X`）。 |

### 1.2 上游 act 的模块

act 是一个在本地用 Docker 执行 GitHub Actions workflow 的命令行工具。调度器复用了它的
解析和执行能力，涉及下列模块：

| 术语 | 代码位置 | 含义 |
| --- | --- | --- |
| Planner | `pkg/model/planner.go` | 读取 workflow YAML，按事件或 job ID 生成执行计划 Plan。 |
| Plan | 同上 | 一个执行计划，由若干 Stage 组成。 |
| Stage | 同上 | Plan 中按 `needs` 分出的层。第 N 层的 job 只依赖前面各层，同一层的 job 互不依赖。 |
| `model.Run` | 同上 | act 里的 `Run` 指「某个 workflow 中的一个 job」（`Workflow` + `JobID`）。**它和下文调度器的 Run 不是一回事**，本文提到它时一律写 `model.Run`。 |
| Runner | `pkg/runner/runner.go` | 把 Plan 变成可执行的函数链。本仓库把它拆出了两个可单独调用的接口：`ExpandRun`（把一个 `model.Run` 按矩阵展开成多个实例，不执行）和 `NewJobExecutor`（为一个实例生成执行链）。 |
| `runner.Config` | 同上 | 一次执行用到的全部配置（镜像映射、环境变量、secrets、缓存路径……）。 |
| RunContext | `pkg/runner/run_context.go` | 一个 job 实例运行时的状态：环境变量、矩阵取值、容器句柄、表达式求值器。容器的创建与清理在这里完成。 |
| Executor | `pkg/common/executor.go` | 类型 `func(ctx) error`，act 用它把步骤串成链：`.Then()` 顺序、`.Finally()` 无论成败都执行。 |
| job 容器 | — | act 为每个 job 实例启动的 Docker 容器，入口命令是 `tail -f /dev/null`，step 通过 `docker exec` 在里面执行。 |
| platform 映射（`-P`） | `cmd/root.go` | `-P ubuntu-latest=<镜像>`，把 `runs-on` 标签映射到 Docker 镜像。 |
| toolcache | — | 容器内的 `/opt/hostedtoolcache` 目录，`setup-python`、`setup-node` 等 action 把工具装到这里。act 用 Docker 卷承载它。 |

### 1.3 调度器的模块

调度器代码在 `pkg/scheduler/`，命令行入口在 `cmd/serve.go`（服务端）和 `cmd/submit.go`（客户端）。

| 术语 | 代码位置 | 含义 |
| --- | --- | --- |
| Run | `types.go` `RunState` | 调度器收到的**一次请求**：一个 workdir、一个事件、一组 workflow。 |
| workdir | `types.go` `RunRequest.Workdir` | 请求方事先在宿主机上 checkout 好的仓库目录（绝对路径）。调度器不负责 clone。 |
| run ID | `isolation.go` `newRunID` | Run 的字符串 ID，形如 `r20261030T0809196790a027`（`r` + UTC 时间 + 8 位随机 hex）。容器、卷、网络、日志目录都用它命名。 |
| `GITHUB_RUN_ID` | `isolation.go` `nextGitHubRunID` | 每个 Run 另分配一个进程内唯一的递增数字，注入 job 环境；artifact 按它隔离。 |
| job 实例 | `engine.go` `JobUnit` | 调度的最小单位：一个 job 的一个 matrix leg（非矩阵 job 只有 1 个实例）。**一个 job 实例对应一个 job 容器。** 代码里叫 JobUnit。 |
| JobGroup | `engine.go` | 同一个 job 的全部 job 实例，带有该 job 的 `max-parallel`。 |
| job key | `engine.go` `formatJobKey` | job 实例的名字：`<workflow 名>/<job id>/<矩阵序号>`，序号从 0 开始，如 `Concurrent/matrix/2`。 |
| Engine | `engine.go` | 把请求转换成 ExecutionPlan 的接口。真实实现 `RunnerEngine` 调用上游 Planner 和 Runner；测试里可换成假的。 |
| ExecutionPlan | `engine.go` | 调度器使用的计划：Stage 列表，每个 Stage 在轮到它时才展开成 JobGroup。 |
| orchestrator | `scheduler.go` `runRun` / `executeStages` | 每个 Run 一个 goroutine，负责逐个 Stage 展开 job 实例、放入队列、等待本 Stage 结束。它自己不执行 job。 |
| Queue（全局队列） | `queue.go` | 所有 Run 共用的优先级队列，元素是待执行的 job 实例。 |
| worker | `scheduler.go` `workerLoop` | 从全局队列取出 job 实例并执行的 goroutine，数量固定。 |
| 容器槽 | `semaphore.go` `Semaphore` | 全局计数信号量，容量为 `--max-containers`。job 容器创建前必须拿到一个槽。 |
| per-group gate | `scheduler.go` `executeStages` | orchestrator 为每个 JobGroup 建的小信号量，容量为该 job 的 `max-parallel`，限制同一 job 同时入队的 leg 数。 |
| Store | `store.go` | 内存里的 Run/job 状态表，API 读取时返回深拷贝。 |
| LogStore | `logs.go` | 管理日志文件：每个 job 实例一个文件，每个 Run 一个 `_scheduler.log`。 |
| 准入（admission） | `scheduler.go` `Submit` | 提交时检查「排队中 + 运行中的 Run 数」是否已达上限，达到则拒绝。 |
| draining | `scheduler.go` `Shutdown` | 收到退出信号后的关闭状态：拒绝新请求，取消已有 Run，等待清理。 |
| data dir | `--data-dir` | 调度器的数据根目录，存放日志、每个 Run 的依赖缓存和 artifact。 |
| cache server | `pkg/artifactcache` | 实现 `actions/cache` 协议的 HTTP 服务，用于缓存依赖（pip/npm 等目录）。 |
| artifact server | `pkg/artifacts` | 实现 `actions/upload-artifact` / `download-artifact` 协议的 HTTP 服务。 |

---

## 2. 总体架构

### 2.1 组件关系

```
 curl / act submit / 你的程序
          │  HTTP JSON
          ▼
 ┌─────────────────────────── act serve 进程 ───────────────────────────┐
 │  API (api.go)  ──►  Scheduler (scheduler.go)                          │
 │                       ├─ Store      内存状态                          │
 │                       ├─ LogStore   日志文件                          │
 │                       ├─ Engine     请求 → ExecutionPlan              │
 │                       ├─ Queue      全局优先级队列                    │
 │                       ├─ 容器槽     Semaphore(--max-containers)       │
 │                       └─ artifact server（整个进程一个）              │
 │                                                                       │
 │  每个 Run：orchestrator goroutine + 一个专属 cache server             │
 │      Stage 1 ──► Stage 2 ──► …   （串行）                             │
 │        └─ 展开 job 实例 ─► 经 per-group gate ─► 放入 Queue            │
 │                                                                       │
 │  worker × (max-containers+8)：从 Queue 取 job 实例                    │
 │      └─ runner.NewJobExecutor 生成的执行链                            │
 │           └─ RunContext.startContainer：先取容器槽，再建容器          │
 └───────────────────────────────┬───────────────────────────────────────┘
                                 │ /var/run/docker.sock
                                 ▼
                    宿主机 dockerd：每个 job 实例一个 job 容器
```

`act serve` 只是 Docker API 的客户端。推荐形态是在宿主机上直接运行
二进制，通过宿主的 `/var/run/docker.sock` 创建容器，这些容器与调度器是「兄弟」关系。
如果要把 act 本身放进容器，应把宿主 socket 挂进去（Docker-out-of-Docker），而不是在
容器里再起一个 dockerd（Docker-in-Docker，DinD）。

### 2.2 一个请求的生命周期

1. **校验**（`Scheduler.Submit`）：workdir 必须是允许根目录下的绝对路径；workflowsPath
   必须在 workdir 内；`eventJSON` 必须是 JSON 对象；`env` 不能设置 `GITHUB_RUN_ID` /
   `GITHUB_RUN_NUMBER`；`secretRefs` 里的名字必须存在。任一失败返回 400。
2. **准入**：排队中 + 运行中的 Run 数达到 `--max-queued-runs` 时返回 503；正在 draining
   时也返回 503。通过后分配 run ID 与 `GITHUB_RUN_ID`，HTTP 立即返回 202，后续步骤异步进行。
3. **启动 cache server**：为这个 Run 单独起一个依赖缓存服务（见 4.2）。
4. **生成本 Run 的配置**（`buildRunConfig`）：把服务启动时的基线 `runner.Config` 复制一份
   （`Clone()`），填入 workdir、事件、secrets、run ID、容器槽、toolcache 卷前缀、
   `GITHUB_RUN_ID`、`ACTIONS_CACHE_URL`，以及请求带来的 env/vars/inputs/matrix 过滤。
5. **解析 workflow**（`RunnerEngine.Prepare`）：用上游 Planner 生成 Plan，再包装成
   ExecutionPlan。此时还不展开矩阵。
6. **逐 Stage 执行**（`executeStages`）：轮到某个 Stage 时才调用 `ExpandRun` 展开矩阵，
   得到 job 实例；每个实例经过 per-group gate 后放入全局队列；orchestrator 等本 Stage
   所有实例结束，再进入下一 Stage。
7. **worker 执行**（`runTask`）：取出实例，打开该实例的日志文件，运行执行链。执行链在创建
   job 容器前先拿容器槽，结束时（成功、失败、被取消）在最外层的 `Finally` 中归还。
8. **收尾**（`finishRun`）：汇总结果，写入 Run 状态，关闭 cache server，删除本 Run 残留的
   toolcache 卷，释放准入名额。

---

## 3. 并发与调度

### 3.1 三层并发限制

| 层 | 限制什么 | 参数 / 默认值 | 超出时 |
| --- | --- | --- | --- |
| 准入 | 同时存在的 Run 数（排队中 + 运行中） | `--max-queued-runs` = 100 | 提交返回 503 |
| per-group gate | 同一个 job 同时进入队列的 matrix leg 数 | workflow 的 `max-parallel`，未写时为 4（且不超过 leg 数） | orchestrator 等待，不占 worker |
| 容器槽 | 全局同时存在的 job 容器数 | `--max-containers` = 20 | worker 阻塞在 Acquire，有容器结束后立即补位 |

另有 `--max-units-per-run`（默认 200）：一个 Run 展开出的 job 实例总数超过它时，该 Run 失败。

**容器槽为什么放在容器创建处**：`RunContext.startContainer()`（`pkg/runner/run_context.go`）
在创建任何容器或网络之前调用 `acquireJobSlot`；job 执行链最外层的 `Finally` 调用
`releaseJobSlot`（`pkg/runner/job_executor.go`）。因为限流点就在「建容器」这一步，以下情况都
被正确计数：

- 每个 matrix leg 各占一个槽；
- reusable workflow 的子 job 各占一个槽，父 job 不建容器，不占槽；
- 任何上游参数（如 `--concurrent-jobs`）都绕不过这个上限。

一个 job 实例只占一个槽，它的 service 容器不另外占槽。workflow 在 step 里通过 Docker socket
自己启动的容器也不计入（见 5.5）。

### 3.2 全局队列与 worker

- 所有 Run 的 job 实例进入同一个全局队列。出队顺序：请求的 `priority` 大的先出；
  `priority` 相同时按入队序号先进先出（`queue.go` 的 `taskHeap.Less`）。
- worker 数默认 `max-containers + 8`，可用 `--workers` 改。worker 比容器槽多几个，是为了
  让「已出队、正在准备、正在等槽」的实例不阻碍其他实例；等槽的 worker 只是阻塞在 channel
  上，不消耗 CPU。worker 数不随宿主核数放大。
- worker 出队后先检查该 Run 是否已取消，已取消则直接把实例记为 `cancelled`，不启动容器。

### 3.3 Run 内与 Run 间的顺序

- **同一 Run 内**：Stage 严格串行，后一个 Stage 等前一个 Stage 的所有实例结束才展开。
  这与上游 act 的 Plan 语义一致，也意味着即使后一 Stage 的某个 job 并不依赖前一 Stage，
  它也不会提前开始。
- **同一 Stage 内**：所有 job 的所有实例同时扇出到队列（受 per-group gate 和容器槽限制）。
- **不同 Run 之间**：没有任何顺序关系，只按队列优先级竞争容器槽。

**不会死锁的原因**：获取顺序固定为「per-group gate → 容器槽」，等待容器槽的 worker 不持有
其他容器槽；orchestrator 只持有 gate，不持有槽。

### 3.4 每个 job 的 CPU 预算（`--job-cpus`）

背景：容器默认能看到宿主全部 CPU（本机 104 核）。很多测试框架按 `nproc` 决定进程数，并按
比例申请内存，在大核数机器上单个 job 就可能 OOM。

`--job-cpus N`（默认 4，设 0 关闭）在创建 job 容器时同时做三件事（`pkg/runner/job_cpus*.go`）：

1. **CPU 配额**：`NanoCPUs = N × 10^9`，限制 job 实际能用的 CPU 时间。
2. **CPU 亲和窗口**：从调度器进程允许使用的 CPU（Linux 上读 `/proc/self/status` 的
   `Cpus_allowed_list`）中轮流分配连续 N 个，写入 `CpusetCpus`，例如第 1 个容器 `0-3`、
   第 2 个 `4-7`……。这样容器里的 `nproc`、Go 的 `runtime.NumCPU()` 等都报告 N。窗口用完后
   回绕，CPU 会被共享，但限制仍然有效。
3. **线程池环境变量**：如果 workflow 没设置，就注入 `GOMAXPROCS`、`OMP_NUM_THREADS`、
   `OPENBLAS_NUM_THREADS`、`MKL_NUM_THREADS`、`NUMEXPR_NUM_THREADS`、`VECLIB_MAXIMUM_THREADS`、
   `PYTEST_XDIST_AUTO_NUM_WORKERS`、`PYTHON_CPU_COUNT`，值都是 N。

该限制只作用于 job 容器，不作用于 service 容器。调度器最多占用 `max-containers × job-cpus`
个核（默认 20×4=80）。Docker daemon 在远端机器、CPU 编号可能不一致时，应设
`--job-cpus 0`。

### 3.5 结果判定

- act 会在内部吞掉 step 的失败，所以 worker 在执行前后读取 runner 记录的 job 结果，
  再结合执行链返回的 error 判定实例状态：`success` / `failure` / `cancelled`。
- 矩阵有两层结果：`Job.Result` 是**整个矩阵组**的结果（供 `needs` 和 Run 结论使用），
  `RunContext.LegResult` 是**单个 leg** 的结果。API 中每个 job 实例按 leg 显示，一个 leg
  失败不会把成功的兄弟 leg 标红。
- 所有 Stage 结束后，任一 job 失败则 Run 为 `failure`。被取消的 Run 为 `cancelled`；
  超时的 Run 为 `failure`（错误信息是 `context deadline exceeded`）。
- `fail-fast` 不生效（上游 act 只解析不执行）：一个 leg 失败不会提前取消其他 leg。

### 3.6 取消与超时

- `POST /v1/runs/{id}/cancel` 同时取消 Run 的 context 和 job 的 context；排队中的实例出队
  时检查 context，直接记为 `cancelled`；运行中的实例由 act 停止容器。
- 超时优先级：请求的 `timeoutSeconds` > 服务端 `--run-timeout` > 不限时。

### 3.7 为并发做的安全修补

上游 act 一次只跑一个 Plan，有些内部状态只在串行时安全。为了让多 Run、多 job 同时运行，
做了以下修补（均在 Go race detector 下验证过）：

| 位置 | 原问题 | 修法 |
| --- | --- | --- |
| `model.Job` 的结果和 outputs | 多个 leg 并发写 | 加锁，改用 `SetResult` / `GetResult` 等方法 |
| step 的 ID | 多个 leg 并发补写共享的 `step.ID` | `ExpandRun` 时预先补齐 |
| `RunContext` 的日志掩码列表 | 并发追加和读取 | `MaskList`（加锁 + 快照） |
| action 拉取（new action cache） | 同一仓库并发 `git init/fetch` | 按 bare repo 路径加互斥锁（`actionFetchLocks`） |
| action 克隆（默认模式） | 原来全局一把锁，所有下载串行 | 按目标目录加互斥锁（`pkg/common/git/git.go` 的 `cloneLocks`） |
| reusable workflow 克隆 | 共享 executor 并发调用 | 按克隆目标加互斥锁（`executorLocks`） |
| `runner.Config` | 运行期被并发修改 | 每个 Run `Clone()` 一份，运行期不改共享配置 |
| artifact server 启动 | 监听失败时 `log.Fatal` 直接杀进程 | 新增 `ServeE` 返回 error |

这里的「互斥锁」都是按 key 分开的（keyed mutex）：不同的 action 仓库可以同时下载，同一个
仓库的下载排队进行。

---

## 4. 缓存机制

调度器里有四种「缓存」，存的东西、位置和共享范围都不同，容易混淆。下表先给出总览，
后面逐个说明。

| 缓存 | 存什么 | 位置 | 共享范围 | 生命周期 |
| --- | --- | --- | --- | --- |
| action 缓存 | `uses:` 引用的 action 源码 | 宿主机 `--action-cache-path`（默认 `~/.cache/act`） | 所有 Run 共享（可改为每 Run 独立） | 永久，不自动清理 |
| 依赖缓存（cache server） | `actions/cache` 保存的目录（pip/npm 缓存等） | `<data-dir>/runs/<run ID>/cache` | **仅同一个 Run 内** | 随 Run 目录保留；条目按 GC 规则过期 |
| toolcache | `setup-*` 安装的 Python/Node/uv 等 | Docker 卷，挂到容器 `/opt/hostedtoolcache` | 每个 job 实例一个卷（可改为全局共享） | job 结束即删除 |
| 镜像 | `runs-on` 对应的 runner 镜像、service 镜像 | 宿主 Docker 本地镜像库 | 全局 | 由 Docker 管理 |

### 4.1 action 缓存

job 里的 `uses: owner/repo@ref` 需要先把 action 的代码取到宿主机，再拷进容器。这一步发生在
**调度器进程里**（不在容器里），因此需要宿主机能访问 GitHub，或者使用离线方案。

有三种获取方式，由启动参数决定（构造逻辑在 `cmd/config.go` 的 `newRunnerConfig`）：

1. **默认模式**：对每个 `uses` 做一次 `git clone` 到 `<action-cache-path>/<uses 的安全文件名>`
   （`safeFilename` 把 `/`、`:` 等换成 `-`，例如 `actions/checkout@v4` → `actions-checkout@v4`），
   之后每次使用都会 `git fetch` 更新。同一目录的克隆按目录加锁。
2. **new action cache 模式**（`--use-new-action-cache`）：每个 `owner/repo` 一个 bare git 仓库
   （`<action-cache-path>/owner-repo.git`），按 ref 只拉取需要的提交（`depth=1`），再从中导出
   tar 包拷进容器（`pkg/runner/action_cache.go`）。
3. **本地映射**（`--local-repository owner/repo@ref=/本地目录`，可重复）：匹配到的 action 直接从
   本地目录读取，完全不联网（`pkg/runner/local_repository_cache.go`）。未匹配的 action 退回到
   new action cache 模式。设置了该参数就会自动启用 new action cache。

`--action-offline-mode` 可以与以上任一方式组合：本地已有内容时不再 fetch，网络失败时使用已缓存
的版本；同时会关闭镜像强制拉取（`--pull`）。

默认情况下所有 Run 共享同一个 action 缓存目录，靠上面的按 key 互斥锁保证并发安全。
`--per-run-action-cache` 会让每个 Run 使用 `<data-dir>/runs/<run ID>/actions`；但在 new action
cache 模式下，bare 仓库仍放在共享目录，只是有锁保护，并非物理隔离。

### 4.2 依赖缓存（cache server）

`actions/cache` 这类 action 通过 HTTP 协议和一个「缓存服务」交互：先查找 key，找不到就
预留一个条目，上传、提交，下次运行时再按 key 取回。act 自带这个协议的实现
（`pkg/artifactcache`），调度器用它来提供缓存。

调度器的做法是**每个 Run 起一个独立的 cache server**（`scheduler.go` 的 `startCacheServer`）：

| 项 | 说明 |
| --- | --- |
| 存储目录 | `<data-dir>/runs/<run ID>/cache`（`cache/bolt.db` 存索引，`cache/cache/` 存数据） |
| 监听地址 | 宿主机出口 IP 上的随机端口（0 表示自动选端口） |
| 鉴权 | 每个实例随机生成 16 字节 token（32 个 hex 字符），拼在 URL 路径里 |
| 注入方式 | 通过环境变量 `ACTIONS_CACHE_URL` 传给 job |
| 失败处理 | 启动失败只打 warning，该 Run 不带缓存继续跑 |

**为什么不全局共享一个**：act 的缓存存储没有按仓库区分维度，多个 Run 共用一个服务会串仓。
代价是缓存只在同一个 Run 内命中——同一个 Run 的不同 job 之间能共享，跨 Run、跨 commit
不共享。一次 Run 只有几十秒时，这个缓存基本不会带来收益，主要价值在同一 Run 的多 job 复用。

条目 GC 规则（`artifactcache/handler.go` 的 `gcCache`，启动时执行一次，之后每次处理请求时
最多每小时执行一次）：

- 未完成（上传中断）且 5 分钟没更新的条目删除；
- 7 天没被使用的条目删除；
- 创建超过 30 天的条目删除；
- 相同 key + version 的多个条目只保留最新的，但最近 5 分钟内被用过的不删。

`--no-cache-server` 可整体关闭；关闭后 job 内不会有 `ACTIONS_CACHE_URL`。注意 `act serve`
**不读取**根命令的 `--cache-server-path` / `--cache-server-addr` / `--cache-server-port`，
存储位置固定为每 Run 目录。

### 4.3 toolcache

`setup-python`、`setup-node`、`setup-uv` 等 action 把工具安装到容器内 `/opt/hostedtoolcache`，
act 用 Docker 卷承载这个路径，让同一 job 的多个 step 复用已装的工具。

调度器默认**每个 job 实例一个卷**（`run_context.go` 的 `toolCacheVolume`）：

- 卷名：`act-toolcache-<run ID>-<job 容器名 sha256 的前 8 字节，16 个 hex 字符>`，因此矩阵 leg
  之间互不干扰；
- 创建：第一个引用它的容器启动时由 Docker 自动创建；
- 删除：job 正常结束时的清理链里删除；Run 结束时调度器再按 `act-toolcache-<run ID>` 前缀扫一遍，
  兜住被取消或超时的 job。删除失败只记 debug 日志，不影响 job 结果。

不共享的原因：矩阵 leg 并发往同一个卷装同一个版本的 Python，两个安装过程会互相覆盖，得到
损坏的工具目录。如果要跨 Run 复用工具（例如大量 job 都装 Python 3.12），用 `--shared-toolcache`
启用以 `act-toolcache` 为名的共享卷，此时调度器不会删除它。

### 4.4 镜像

job 容器和 service 容器使用的镜像来自宿主 Docker 的本地镜像库：

- 本地已有镜像时直接使用；`-p/--pull` 强制拉取最新版本，`--action-offline-mode` 会自动关闭
  强制拉取；
- 镜像由三层决定，优先级从高到低：workflow 里 job 级的 `container.image`、`serve` 启动参数
  `-P <runs-on 标签>=<镜像>`、act 内置默认映射（如 `ubuntu-latest` → `node:16-buster-slim`，
  一般不建议依赖）。

### 4.5 artifact server（不是缓存，但容易混淆）

`actions/upload-artifact` / `download-artifact` 用同一套 HTTP 协议族，由 artifact server 提供服务。
它与依赖缓存的区别：

| | cache server | artifact server |
| --- | --- | --- |
| 实例数 | 每个 Run 一个 | 整个进程一个 |
| 存储 | `<data-dir>/runs/<run ID>/cache` | `<data-dir>/artifacts/<GITHUB_RUN_ID>` |
| 隔离方式 | 独立进程地址 + token | 共享服务，按 `GITHUB_RUN_ID` 分目录 + 签名 token |
| 跨 Run 可见性 | 不可见 | 目录不同，不可见 |
| 用途 | 加速依赖安装 | 在 job 之间传递构建产物 |

artifact 在 Run 结束后不会自动删除，需要的话自行清理 `<data-dir>/artifacts`。

### 4.6 数据目录布局

```
<data-dir>/
├── runs/<run ID>/
│   ├── logs/<workflow>-<job>-<序号>.log   # 每个 job 实例一个日志文件
│   ├── logs/_scheduler.log                # 该 Run 的调度日志
│   ├── cache/                             # 该 Run 的依赖缓存
│   └── actions/                           # 仅 --per-run-action-cache
└── artifacts/<GITHUB_RUN_ID>/             # 共享 artifact server 的存储
```

日志文件名由 job key 生成，其中 `/` 和空格等字符会被替换成 `-`（`isolation.go` 的 `sanitizeKey`），
所以 `Concurrent/matrix/0` 对应 `logs/Concurrent-matrix-0.log`。`GET /v1/runs/{id}/logs` 列出的是
去掉 `.log` 后的文件名（即 `Concurrent-matrix-0`）；取单个 job 日志时写原标题或文件名都可以，
两者会映射到同一个文件（见 8.5）。reusable workflow 子 job 的日志单独成文件，文件名以父 job
的 key 为前缀。

---

## 5. 网络配置

调度器涉及的网络分四块：服务自身的 HTTP 监听、job 容器的网络模式、调度器内置服务（cache /
artifact）对容器的可达性、以及两处需要出网的地方。逐条说明。

### 5.1 API 监听与鉴权

- `--addr`：HTTP 监听地址，默认 `127.0.0.1:8080`。
- **监听非 loopback 地址时必须设置 `--api-token`**，否则 `serve` 直接拒绝启动
  （`cmd/serve.go` 的 `requireAPIToken`）。
- 鉴权方式是请求头 `Authorization: Bearer <token>`，比较时用常量时间比较；`/healthz` 不需要
  token，便于做存活探针。
- 服务端 HTTP 超时：读请求头 5 秒，空闲连接 2 分钟；请求体上限 1 MiB，且拒绝未知字段。

### 5.2 job 容器的网络模式

由根命令的持久参数 `--network` 决定，默认 `host`：

- **host 模式**（默认）：job 容器与宿主机共享网络栈。容器里访问 `127.0.0.1:<端口>` 就是访问
  宿主机上的端口，因此能直接连上 artifact server 和 cache server。代价是容器里的端口与宿主
  端口冲突。
- **其他模式**（如 `bridge`）：job 容器通过 Docker 网络访问宿主，需要填宿主的可达 IP（见 5.3）。
- **job 声明了 `services:` 时**：act 为该 job 单独创建一个网络，名字是 job 容器名再加
  `-<job id>-network` 后缀（job 容器名本身以 `act-<run ID>-` 开头，并以名称的 sha256 结尾），
  job 容器与各 service 容器都加入这个网络，容器名同时作为 service 的网络别名，job 结束
  （或清理）时删除该网络（`run_context.go` 的 `networkName`）。

### 5.3 内置服务的可达地址

| 服务 | 默认监听 | 说明 |
| --- | --- | --- |
| artifact server | `127.0.0.1:34567`（`--artifact-server-addr` / `--artifact-server-port`） | 只有 host 网络的 job 能访问。默认 addr 是 `127.0.0.1`；如果使用 bridge 网络或 service 容器，需要显式设成容器可达的地址（如宿主出口 IP），否则 artifact 上传/下载会超时 |
| cache server | 出口 IP + 随机端口 | 每个 Run 一个。出口 IP 由 `common.GetOutboundIP()` 得到：先 UDP 连 `8.8.8.8:80` 取本地地址，不通时回退到网卡上的第一个全局单播地址 |

artifact 的地址通过 `ACTIONS_RUNTIME_URL` / `ACTIONS_RESULTS_URL` 传给 job，token 通过
`ACTIONS_RUNTIME_TOKEN` 传入，由调度器用启动时生成的密钥签名，Run 之间用 `GITHUB_RUN_ID` 区分。

### 5.4 两处出网

**（1）调度器进程本身**。action 的 `git clone` 发生在宿主机，所以 `act serve` 进程要能访问
GitHub；它还会在启动时尝试通过 `gh` 读取 `GITHUB_TOKEN`。配置方式二选一：

- 在启动 `act serve` 前给进程设置 `http_proxy` / `https_proxy` 环境变量；
- 使用 4.1 的离线方案（`--action-offline-mode` + `--local-repository`），让 action 不必出网
  （见第 9 章的示例部署）。

**（2）job 容器内部**。`pip install`、`npm install`、`apt-get`、`setup-*` 的下载都在容器里进行，
宿主的代理变量不会自动进去。需要通过 `--env` 传给 job：

```bash
act serve ... \
  --env http_proxy=http://<proxy> --env https_proxy=http://<proxy> \
  --env HTTP_PROXY=http://<proxy> --env HTTPS_PROXY=http://<proxy> \
  --env no_proxy=127.0.0.1,localhost,<宿主出口 IP> \
  --env NO_PROXY=127.0.0.1,localhost,<宿主出口 IP>
```

注意事项：

- 大小写两种形式都要给，不同语言/工具的读取习惯不同；
- Node 20+ 的原生 `fetch` 默认不读代理环境变量，还要加 `--env NODE_USE_ENV_PROXY=1`；
- `no_proxy` 必须包含 `127.0.0.1`、`localhost` 和宿主出口 IP，否则访问 cache server 的请求
  会被代理走，表现为 `actions/cache` 卡住或报错；
- workflow 里显式写了同名 `env` 时以 workflow 为准。

### 5.5 Docker socket

默认情况下 act 把 Docker daemon socket 挂进 job 容器（`/var/run/docker.sock`），让 workflow
里的 `docker` 命令可以在宿主上创建「兄弟容器」。由此带来的两点：

- 这些嵌套容器**不受 `--max-containers` 统计**，也不会随 Run 自动清理；
- 不需要的话用 `--container-daemon-socket -` 关闭挂载，但依赖 Docker 的 workflow 会失败。

### 5.6 其他网络相关注意点

- host 网络下有些域名只解析出 IPv6 而宿主机 IPv6 不通，表现为 `curl` 失败或 Node 报
  `fetch failed`；配置代理可规避。
- 关闭 `--container-daemon-socket` 时，`--container-options-policy` 会自动限制 workflow 自定义的
  宿主机路径挂载，避免容器逃逸；需要放开时显式设 `allow`。

---

## 6. 隔离与安全

多 Run 共用一个宿主机，靠以下规则互相隔离：

| 资源 | 隔离方式 |
| --- | --- |
| job 容器、workdir 卷、env 卷 | 名字以 `act-<run ID>-` 开头 |
| 每次 job 的工作目录 | 默认把 workdir **复制**进该 job 自己的容器卷（不是 bind 挂载），并发 job 不会互相写 |
| `GITHUB_RUN_ID` / `GITHUB_RUN_NUMBER` | 每个 Run 一个进程内唯一数字，请求不能覆盖 |
| toolcache | 每个 job 实例一个卷（4.3） |
| 依赖缓存 | 每个 Run 一个 cache server 实例（4.2） |
| artifact | 一个服务，按 `GITHUB_RUN_ID` 分目录 |
| 日志 | 每个 job 实例一个文件（4.6） |

**请求侧校验**（`isolation.go`）：

- `workdir` 必须是绝对路径，先做 `EvalSymlinks` 解析，再检查是否位于 `--allowed-workdir-roots`
  的某个根目录之下（防止 `..` 和符号链接逃逸）；
- `workflowsPath` 解析后必须仍在 workdir 内；
- 请求体上限 1 MiB，未知字段直接 400（防止拼错字段被静默忽略）。

**其他约束**：

- 默认禁止请求使用 `--bind`（`--allow-bind-workdir` 才放开），因为并发 job 共享宿主目录会互相污染；
- secrets 只在启动时加载，请求只能按名字引用，值不会出现在 API 响应里，日志中会被掩码；
- **信任边界**：workflow 里的命令最终以容器 root 在宿主机 Docker 上执行，能访问容器网络和挂载。
  `--allowed-workdir-roots` 限制的是「能执行哪些 checkout」，不是「workflow 能做什么」。
  只应对可信仓库开放，公网监听时必须设 `--api-token`。

---

## 7. 如何调用

### 7.1 启动服务：`act serve`

常用参数（完整列表见 [`scheduler.md`](scheduler.md)）：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--addr` | `127.0.0.1:8080` | HTTP 监听地址；非 loopback 时必须同时给 `--api-token` |
| `--data-dir` | `~/.cache/serve` | 日志、缓存、artifact 根目录 |
| `--max-containers` | `20` | 全局同时运行的 job 容器上限 |
| `--workers` | `max-containers + 8` | 执行 job 的 worker 数 |
| `--job-cpus` | `4` | 每个 job 容器的 CPU 预算，0 关闭 |
| `--max-queued-runs` | `100` | 排队 + 运行中的 Run 上限 |
| `--max-units-per-run` | `200` | 单个 Run 的 job 实例上限 |
| `--run-timeout` | `0` | Run 默认超时，0 表示不限 |
| `--allowed-workdir-roots` | 当前目录 | 允许执行的目录，逗号分隔 |
| `--api-token` | 空 | Bearer token |
| `--log-stdout` | `false` | 同时把 job 日志输出到终端 |
| `--shared-toolcache` | `false` | 所有 Run 共享 `act-toolcache` 卷 |
| `--per-run-action-cache` | `false` | 每个 Run 独立的 action 缓存目录 |
| `--allow-bind-workdir` | `false` | 允许请求使用 `--bind` |
| `--keep-runs` | `0` | 内存中保留的已结束 Run 数，0 表示全部保留 |
| `--artifact-server-dir` | `<data-dir>/artifacts` | artifact 存储目录 |

此外还接受根命令的常见参数：`-P/--platform`、`-s/--secret`、`--secret-file`、`--var`、
`--env`、`--input`、`-p/--pull`、`-r/--reuse`、`--privileged`、`--container-options`、
`--container-options-policy`、`--local-repository`、`--action-offline-mode`、
`--use-new-action-cache`、`--network`、`--artifact-server-addr`、`--artifact-server-port`、
`--no-cache-server`。它们构成所有请求共享的**基线配置**。

secrets 在启动时一次性加载，来源包括 `-s NAME=value`、`--secret-file`、同名环境变量，
以及可用的 `GITHUB_TOKEN`（会尝试通过 `gh` 读取）。

### 7.2 发送请求：`RunRequest`

`POST /v1/runs` 的 JSON 体（`pkg/scheduler/types.go`）：

| 字段 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- |
| `workdir` | 是 | — | 已 checkout 的绝对路径，须在允许根目录下 |
| `workflowsPath` | 否 | `.github/workflows/` | 相对 workdir 或 workdir 内的绝对路径；可以是目录或单个文件 |
| `repo` | 否 | 空 | 元数据；未设置 `GITHUB_REPOSITORY` 时作为其默认值 |
| `commit` | 否 | 空 | 元数据；未设置 `GITHUB_SHA` 时作为其默认值 |
| `event` | 否 | `push` | 事件名；设置了 `job` 时忽略 |
| `eventJSON` | 否 | 空 | 内联事件 payload（JSON 对象），用于 `github.event` |
| `job` | 否 | 空 | 只跑指定 job ID（等价 `act -j`） |
| `matrix` | 否 | 空 | `{"key": ["v1","v2"]}`，只保留匹配的矩阵组合 |
| `env` | 否 | 空 | 追加/覆盖 job 环境变量 |
| `vars` | 否 | 空 | 追加/覆盖 `vars` |
| `inputs` | 否 | 空 | 仅在没给 `eventJSON` 时用于构造事件里的 `inputs` |
| `secretRefs` | 否 | 空 | 需要的 secret 名字列表；为空表示使用服务端全部 secrets；未知名字 400 |
| `timeoutSeconds` | 否 | 0 | 0 表示使用 `--run-timeout` |
| `priority` | 否 | 0 | 越大越先出队；同级 FIFO |
| `noRecurse` | 否 | `false` | 同 act 的同名行为（是否递归遍历子目录的 workflow） |
| `strict` | 否 | `false` | 同 act 的同名行为（未知事件或矩阵值是否报错） |

### 7.3 HTTP API

除 `/healthz` 外，设置了 `--api-token` 时都要带 `Authorization: Bearer <token>`。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/v1/runs` | 提交，立即返回 202 与 Run 状态 |
| `GET` | `/v1/runs` | 列表；查询参数 `limit`（默认 100）、`status`、`repo`、`commit` |
| `GET` | `/v1/runs/{id}` | Run 详情，含每个 job 实例的状态 |
| `GET` | `/v1/runs/{id}/logs` | `{"run": id, "logs": [{"key","size"}]}` |
| `GET` | `/v1/runs/{id}/logs?job=KEY` | 该 job 实例的日志文本 |
| `GET` | `/v1/runs/{id}/logs?job=KEY&offset=N&follow=1` | 从字节偏移 N 开始持续输出，直到 Run 结束 |
| `POST` | `/v1/runs/{id}/cancel` | 取消排队中或运行中的 Run |
| `GET` | `/v1/stats` | 队列、job、容器计数 |
| `GET` | `/healthz` | 存活检查；draining 时返回 503 |

错误码：`400` 参数非法（含未知字段、workdir 越界、未知 secret）、`401` token 缺失或不匹配、
`404` Run 不存在、`409` 取消一个已结束的 Run、`503` 队列已满或正在关闭。

### 7.4 使用客户端：`act submit`

`act submit` 是同一个二进制里的客户端子命令，封装了提交和轮询：

| 参数 | 说明 |
| --- | --- |
| `--addr` | 调度器地址，默认 `http://127.0.0.1:8080` |
| `--token` | API token |
| `--workdir` | checkout 目录，默认 `.`（会转成绝对路径） |
| `--repo` / `--commit` | 元数据 |
| `--workflows-path` | workflow 文件或目录 |
| `-e/--event` / `--event-file` | 事件名 / 事件 JSON 文件 |
| `-j/--job` | 只跑一个 job |
| `--matrix key:value` | 可重复 |
| `--env key=value` | 可重复 |
| `--secret-ref NAME` | 可重复 |
| `--timeout` | 秒，0 表示用服务端默认 |
| `--priority` | 越大越先调度 |
| `--wait` | 轮询直到结束并打印每个 job 的结果；Run 失败时退出码非 0 |
| `--output` | `text` 或 `json` |

### 7.5 状态与 job key

- job 实例状态：`queued` → `running` → `success` / `failure` / `cancelled`。
- Run 状态：`queued` → `running` → `success` / `failure` / `cancelled`。
- job key 格式：`<sanitize(workflow 名)>/<sanitize(job id)>/<矩阵序号>`，非矩阵 job 序号固定为 0。
  `sanitize` 把非 `[a-zA-Z0-9._-]` 字符替换成 `-`，最长 80 字符。
- 矩阵 job 的组结果与 leg 结果的区别见 3.5。

---

## 8. Quickstart

以下命令假设：有一台能访问 Docker daemon 的机器、Go 环境、一个 runner 镜像。
下面用仓库自带的示例 workflow `pkg/scheduler/testdata/concurrent/workflow.yml`，它包含：

- job `first`：`runs-on: ubuntu-latest`，`sleep 1`；
- job `second`：`needs: first`，所以是一个独立 Stage；
- job `matrix`：3 个矩阵取值 `n: [1,2,3]`，展开成 3 个 job 实例。

一共 2 个 Stage、5 个 job 实例。

### 8.1 构建

```bash
cd act
go build -o dist/local/act main.go
```

### 8.2 准备 workdir

调度器不 clone 仓库，需要事先放好 checkout（包含 `.github/workflows/`）：

```bash
mkdir -p /tmp/act-qs/repo/.github/workflows
cp pkg/scheduler/testdata/concurrent/workflow.yml \
   /tmp/act-qs/repo/.github/workflows/workflow.yml
```

### 8.3 启动调度器

```bash
mkdir -p /tmp/act-qs/data
./dist/local/act serve \
  --addr 127.0.0.1:8080 \
  --data-dir /tmp/act-qs/data \
  --max-containers 4 \
  --allowed-workdir-roots /tmp/act-qs \
  -P ubuntu-latest=catthehacker/ubuntu:act-latest
```

启动成功会打印（实测输出）：

```
level=info msg="Using docker host 'unix:///var/run/docker.sock', and daemon socket 'unix:///var/run/docker.sock'"
level=info msg="act scheduler listening on http://127.0.0.1:8080 (max 4 containers, data dir /tmp/act-qs/data)"
level=info msg="Start server on http://127.0.0.1:34567"
```

（`-P` 的镜像按本机已有的 runner 镜像替换；也可以用 `catthehacker/ubuntu:act-latest`。）

> 第三行是 artifact server。它的端口是固定的 `34567`，所以同一台机器同时只能跑一个默认配置的
> `act serve`；要并行跑多个实例，给后来的实例加 `--artifact-server-port <另一个端口>`，否则启动
> 会报 `failed to start artifact server ... address already in use`。

> 这个 `serve` 命令带 `--env http_proxy=...` 等参数时，代理变量会进入每个 job 容器（见 5.4）。

### 8.4 提交一个 Run

用同目录的客户端（推荐，`--wait` 会等到结束并打印结果）：

```bash
./dist/local/act submit \
  --addr http://127.0.0.1:8080 \
  --workdir /tmp/act-qs/repo \
  --repo demo/concurrent \
  --commit 0000000 \
  --event push \
  --wait
```

`--commit` 只是元数据（会作为 `GITHUB_SHA`），调度器不校验它对应的 commit 是否真实存在；
workdir 不是 git 仓库时日志里会出现 `unable to get git revision`，不影响执行。

输出形如（实测）：

```
Submitted run r20261104T03161989d00726 (status queued)
  inspect: http://127.0.0.1:8080/v1/runs/r20261104T03161989d00726
  cancel:  curl -X POST http://127.0.0.1:8080/v1/runs/r20261104T03161989d00726/cancel

Run r20261104T03161989d00726 finished: success
  Concurrent/first/0                       success
  Concurrent/matrix/0                      success
  Concurrent/matrix/1                      success
  Concurrent/matrix/2                      success
  Concurrent/second/0                      success

Logs: http://127.0.0.1:8080/v1/runs/r20261104T03161989d00726/logs
```

（workflow 名取自 YAML 顶部的 `name: Concurrent`。）

或者直接用 curl：

```bash
curl -sS -X POST http://127.0.0.1:8080/v1/runs \
  -H 'Content-Type: application/json' \
  -d '{
        "repo": "demo/concurrent",
        "commit": "0000000",
        "workdir": "/tmp/act-qs/repo",
        "workflowsPath": ".github/workflows/workflow.yml",
        "event": "push",
        "priority": 0
      }'
```

返回 202，响应里带 `id`。

### 8.5 查看进度与日志

```bash
ID=r20261104T03161989d00726         # 用上面返回的 id

curl -sS http://127.0.0.1:8080/v1/runs/$ID          # Run 详情与每个 job 实例状态
curl -sS http://127.0.0.1:8080/v1/stats             # 队列 / 容器计数
curl -sS http://127.0.0.1:8080/v1/runs/$ID/logs     # 日志文件清单
curl -sS 'http://127.0.0.1:8080/v1/runs/'$ID'/logs?job=Concurrent/first/0'
curl -sS 'http://127.0.0.1:8080/v1/runs/'$ID'/logs?job=Concurrent/first/0&follow=1'  # 边跑边看
```

`/v1/runs/{id}/logs` 返回的是**文件名**列表，key 里的 `/` 已经换成 `-`：

```json
{"run":"r20261104T03161989d00726",
 "logs":[{"key":"Concurrent-first-0","size":2772},
         {"key":"Concurrent-matrix-0","size":2828},
         ...]}
```

取单个 job 日志时两种写法都可以：`?job=Concurrent/first/0`（原标题）或 `?job=Concurrent-first-0`。
`follow=1` 在 Run 处于排队/运行状态时会一直保持连接，Run 进入终态且读完文件后自动结束；对已结束
的 Run 立即返回。注意 job 启动初期（拉镜像、建容器阶段）日志可能还是空的，`follow` 会一直等待。

验证要点（下面的时间戳就是实测结果）：

- `Concurrent/second/0` 的 `startedAt`（11:16:41.0557）晚于 3 个 `matrix` 实例的 `finishedAt`
  （最晚 11:16:41.0555），即 Stage 串行；
- 3 个 `matrix` 实例的 `startedAt` 都在 11:16:19.316~317，即同 Stage 内并行；
- 两个 Run 同时提交、共 8 个 job 实例时，`/v1/stats` 显示 `"containersUsed":4,"containersMax":4`，
  其余实例排队等待，不会超过上限。

### 8.6 取消与停止

```bash
curl -sS -X POST http://127.0.0.1:8080/v1/runs/$ID/cancel
kill -TERM <act serve 的 pid>    # 进入 draining，取消活动 Run，清理容器后退出
```

### 8.7 Python 客户端示例

```python
import time
import requests

BASE = "http://127.0.0.1:8080"
HEADERS = {"Authorization": "Bearer <token>"}   # 未设置 --api-token 时留空

resp = requests.post(f"{BASE}/v1/runs", headers=HEADERS, json={
    "workdir": "/tmp/act-qs/repo",
    "workflowsPath": ".github/workflows/workflow.yml",
    "event": "push",
    "repo": "demo/concurrent",
    "priority": 0,
})
resp.raise_for_status()
run = resp.json()
run_id = run["id"]
print("submitted", run_id)

while True:
    run = requests.get(f"{BASE}/v1/runs/{run_id}", headers=HEADERS).json()
    if run["status"] in ("success", "failure", "cancelled"):
        break
    time.sleep(2)

print("run", run["status"])
for job in run["jobs"]:
    print(f"  {job['key']:<40} {job['status']}")
    log = requests.get(f"{BASE}/v1/runs/{run_id}/logs",
                       headers=HEADERS, params={"job": job["key"]}).text
    print(log[-500:])
```

---

## 9. 示例部署

本节给出一种参考部署方式；所有路径、端口、镜像和凭据都通过环境变量或命令行参数配置，
示例中的值均为占位符。

### 9.1 形态

- **宿主机直跑**：`act serve` 是宿主机上的二进制进程，不是容器；通过宿主
  `/var/run/docker.sock` 创建 job 容器（兄弟容器）。
- 容器上限 `--max-containers 20`，每 job CPU 预算 `--job-cpus 4`。
- 允许执行的根目录：`/srv/ci/repos`（每个子目录是一个已 checkout 的任务实例）。
- `--container-options` 可把该根目录只读挂进 job 容器，配合 `--container-options-policy allow`。

### 9.2 镜像映射

| `runs-on` | 镜像 |
| --- | --- |
| `ubuntu-latest` / `ubuntu-24.04` | `<your-runner-image>`（可基于 catthehacker act 镜像构建） |
| `ubuntu-22.04` | `catthehacker/ubuntu:act-22.04` |
| `ubuntu-20.04` | `catthehacker/ubuntu:act-20.04` |
| `ubuntu-18.04` | `catthehacker/ubuntu:act-18.04` |

### 9.3 网络

- 可选：`--action-offline-mode` + `--local-repository ...=/本地目录`，让 action 从本地读取，
  守护进程不必访问外网；
- workflow 步骤内的下载（pip/npm/apt、`setup-*` 查版本）如需要代理，通过 `--env` 注入
  （`http_proxy`/`https_proxy` 及大写形式、`NODE_USE_ENV_PROXY=1`，`no_proxy` 排除本机地址）；
- `GITHUB_TOKEN` 通过权限 0600 的 secret 文件注入，因此 `github.token` / `GH_TOKEN` 可用。

### 9.4 启动命令模板

完整示例见 [`deploy/start-serve.sh`](../deploy/start-serve.sh)；最小命令：

```bash
./dist/local/act serve \
  --addr 127.0.0.1:8080 \
  --data-dir /var/lib/act-serve \
  --max-containers 20 \
  --job-cpus 4 \
  --allowed-workdir-roots /srv/ci/repos \
  --secret-file /etc/act/secrets.env \
  -P ubuntu-latest=catthehacker/ubuntu:act-latest
```

提交一个任务实例：

```bash
./dist/local/act submit --addr http://127.0.0.1:8080 \
  --workdir /srv/ci/repos/acme-demo \
  --workflows-path .github/workflows/ci.yml \
  --event pull_request --wait
```

---

## 10. 已知限制与排障

### 10.1 限制

- **状态只在内存**：重启会丢失排队中和运行中的 Run 记录；崩溃（`kill -9`）可能留下孤儿容器。
  正常 SIGTERM 会先取消、等待，超时后按 run ID 前缀强制清理。
- **没有日志滚动/保留策略**：日志、每 Run 的缓存和 artifact 会持续占用磁盘，需要外部清理。
- **`fail-fast` 不生效**，只有 `max-parallel` 生效。
- **同一 Run 的 Stage 严格串行**，即使后面的 job 与前面无关也不会提前跑。
- **必须由调用方事先 checkout**，调度器不做 clone/fetch。
- **`--job-cpus` 只约束 job 容器**：service 容器和 host 模式（`-self-hosted`）不受限；
  Docker daemon 在远端时应设为 0。
- **嵌套容器不计入上限**：workflow 通过 Docker socket 创建的容器既不受 `--max-containers`
  限制，也不会随 Run 清理（见 5.5）。
- **部分运行时仍报告宿主核数**：如 Python 3.12 的 `os.cpu_count()` 不感知亲和性，所以额外注入了
  线程池环境变量（3.4）；第三方工具若用自定义变量，需要在 workflow 里自己约束。
- 每个 Run 一个 cache server（随机端口），并发 Run 多时端口和进程数随之增长。

### 10.2 常见现象

| 现象 | 原因 / 处理 |
| --- | --- |
| `400 invalid JSON body: json: unknown field "..."` | 请求里有拼错或多余的字段，API 拒绝未知字段 |
| `400 invalid run request: workdir "..." is outside the allowed roots` | `--allowed-workdir-roots` 没覆盖该目录 |
| `400 invalid run request: unknown secret reference "X"` | 服务端启动时没加载名为 X 的 secret |
| `503 run queue is full` | 排队 + 运行中的 Run 达到 `--max-queued-runs` |
| `503 scheduler is shutting down` | 正在 draining，等待旧进程退出或重启 |
| `401 missing or invalid API token` | 请求没带 `Authorization: Bearer` |
| `404 run not found` | Run ID 不存在，或服务重启过（状态只在内存） |
| `409 run is not active` | 取消一个已经结束的 Run |
| 启动时报 `artifact server ... address already in use` | 同一台机器已有实例占用 34567，用 `--artifact-server-port` 或先停掉旧实例 |
| job 长时间停在下载步骤 | job 容器没配代理，见 5.4 |
| `actions/cache` 卡住或报 `Failed to ... cache` | 代理把访问 cache server 的请求也代理了，`no_proxy` 补上 `127.0.0.1,localhost,<出口 IP>` |
| artifact 上传超时 | 非 host 网络时 `--artifact-server-addr` 仍是 `127.0.0.1`，改成容器可达地址 |
| `Unable to locate package ...` | 镜像内软件源缺失，属于镜像问题，不是调度问题 |
| job 全都 `queued` 不动 | 检查 `/v1/stats` 的 `containersUsed` 是否已满、worker 是否卡在 docker 操作 |
| 取消后 job 报 `failed to acquire a job slot: context canceled` | 正常现象：排队等槽的实例被取消（实测输出如此） |
| 日志里出现 `unable to get git revision: repository does not exist` | workdir 不是 git 仓库；act 只把它当作「无法填充 git 上下文」，不影响执行 |
| 崩溃后容器残留 | 手动 `docker ps -a --filter name=act-<run ID>-` 后清理；正常 SIGTERM 会清理 |

---

## 11. 代码索引

| 路径 | 内容 |
| --- | --- |
| `cmd/serve.go` | `act serve` 子命令：参数、鉴权检查、HTTP server 启动与关闭 |
| `cmd/submit.go` | `act submit` 客户端：构造请求、轮询结果 |
| `cmd/config.go` | `newRunnerConfig`：CLI 参数 → `runner.Config`，包括 action 缓存的三层包装 |
| `pkg/scheduler/scheduler.go` | 调度核心：`Submit`（校验与准入）、`runRun` / `executeStages`（orchestrator）、`workerLoop` / `runTask` / `executeUnit`（worker）、`buildRunConfig`、`startCacheServer`、`Cancel`、`Shutdown` |
| `pkg/scheduler/queue.go` | 全局优先级队列（priority 降序 + FIFO） |
| `pkg/scheduler/semaphore.go` | 容器槽，实现 `runner.JobSlotLimiter` |
| `pkg/scheduler/engine.go` | `Engine` / `ExecutionPlan` / `JobGroup` / `JobUnit`，以及真实实现 `RunnerEngine` |
| `pkg/scheduler/store.go` | 内存状态表（读时深拷贝） |
| `pkg/scheduler/logs.go`、`logstream.go` | 日志文件管理、按 offset/follow 输出 |
| `pkg/scheduler/api.go` | HTTP 路由、Bearer 鉴权、错误码映射 |
| `pkg/scheduler/isolation.go` | run ID、`GITHUB_RUN_ID`、workdir/workflows 路径校验、job key 清洗 |
| `pkg/scheduler/types.go` | `RunRequest`、`RunState`、`JobState`、`Status`、`Config` |
| `pkg/runner/runner.go` | `ExpandRun`（矩阵展开）、`NewJobExecutor`（单实例执行链）、`Config.GetConcurrentJobs` |
| `pkg/runner/run_context.go` | 容器创建与清理、容器槽的获取、容器/卷/网络命名、toolcache 卷、leg 结果 |
| `pkg/runner/job_cpus.go`、`job_cpus_linux.go` | CPU 预算：配额、亲和窗口、线程池环境变量 |
| `pkg/runner/job_executor.go` | job 执行链，最外层 `Finally` 归还容器槽 |
| `pkg/runner/action_cache.go`、`action_cache_offline_mode.go`、`local_repository_cache.go` | action 缓存的三种实现 |
| `pkg/common/git/git.go` | action/仓库克隆，按目标目录加锁 |
| `pkg/artifactcache/handler.go` | cache server 协议、per-Run 存储、GC |
| `pkg/artifacts/` | artifact server 与 `ListenAddress` 规则 |
| `README.scheduler.md` | 更细的实现说明与多任务实测记录 |
| `docs/scheduler.md` | 英文参数与 API 速查 |
