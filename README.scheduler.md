# act 并行调度器（`act serve` / `act submit`）

本仓库在 [nektos/act](https://github.com/nektos/act) 的基础上增加了一个常驻调度器：
它通过 HTTP 接收多个仓库、多个 commit 的多份 workflow 请求，把每个 job 实例放到
独立容器里并行执行，并保证全局容器数不超过给定上限（`--max-containers`，代码默认
20；当前部署脚本默认 10，本轮 campaign 通过 `PACT_MAX_CONTAINERS=24` 启动）。

原始 act 一次进程调用只能跑一份 plan，且 plan 内 stage 串行；本改造在保留原有
CLI 接口的前提下，把「单个 job 实例」抽成可独立调度和执行的单元。默认行为有三处
调整（每个 job 4 核 CPU 预算、默认并发数受其限制、worker 池不再随核数放大），
详见第 1 章末尾的差异说明。

- 面向调用者的完整用法：[第 4 章](#4-调用者使用说明)
- 设计/实现细节：[第 2 章](#2-实现原理)
- 多任务实测结果：[第 5 章](#5-多任务实测fixbench)

调用结果分为两层，职责不同：

| 结果层 | 产生方式 | 粒度 | 字段 | 准确度目标 |
| --- | --- | --- | --- | --- |
| 二值 job 结果 | 调度器/runner 终态，纯规则 | 每个 job 实例 | `status`、`result` | 很高；不引入 LLM |
| 环境/代码类别 | 验证层：纯 LLM（结构化提示词 + 日志） | 每个 job 或整个 run | `error_class`、`error_category`、`job_categories` | 较高；允许有歧义 |

`act serve` 本身只返回第一层（以及原始日志）；第二层由调用方的验证/评分层读取
job 日志后生成，见 4.3.4。这样调度器的核心结论不会被模型的不确定性污染。

---

## 目录

1. [背景与目标](#1-背景与目标)
2. [实现原理](#2-实现原理)
3. [Quickstart](#3-quickstart)
4. [调用者使用说明](#4-调用者使用说明)
5. [多任务实测（fixbench）](#5-多任务实测fixbench)
6. [代码索引](#6-代码索引)

---

## 1. 背景与目标

CI 场景里同时到达的通常是「多个仓库 × 多个 commit」的验证请求。直接调用 act 只能
串行处理，而且矩阵 job 会被并发数截断。本改造提供：

| 目标 | 实现 |
| --- | --- |
| 多任务自动调度 | HTTP API + 优先级队列 + 全局 worker pool |
| 一个 job 一个容器 | 调度单元 = job 实例（job × matrix 展开项） |
| 跨 run 并行、run 内保持 `needs` 语义 | 每个 run 一个 orchestrator，stage 串行、stage 内扇出 |
| 全局 20 个并发容器（可配） | 容器创建处获取全局信号量，覆盖 matrix / reusable workflow |
| 多任务隔离 | 容器/卷/网络按 runID 命名，独立 `GITHUB_RUN_ID`、cache、toolcache、日志 |
| 保留原 CLI | 逐 job 执行 API 由原 `NewPlanExecutor` 委托复用；默认并发数受 `--job-cpus` 限制 |

调度器**不负责 clone**：请求方必须提供宿主机上已 checkout 的目录（`workdir`）。

> 与上游 act 的默认行为差异：
> - 每个 job 容器默认有 4 核 CPU 预算（`--job-cpus`，设 0 可关闭）；
> - CLI 未显式设置 `--concurrent-jobs` 时，默认并发数取 `min(NumCPU, --job-cpus)`，
>   不再直接等于宿主核数；
> - `serve` 的 worker 池默认固定为 `max-containers+8`，不再随宿主核数放大。

---

## 2. 实现原理

### 2.1 总体架构

```
             ┌──────────────────────────────────────────────┐
 HTTP 请求 → │ cmd/serve.go                                │
 act submit →│   └─ pkg/scheduler.Scheduler                │
             │        ├─ Store        内存状态（读时深拷贝） │
             │        ├─ Queue        优先级堆（大优先，同级 FIFO）│
             │        ├─ Semaphore    全局容器槽（默认 20） │
             │        ├─ LogStore     每 job 一个日志文件   │
             │        ├─ API          /v1/runs ...          │
             │        └─ Engine       请求 → ExecutionPlan  │
             └───────────────┬──────────────────────────────┘
                             │ 每 run 一个 orchestrator goroutine
                             ▼
        stage 1 ──► stage 2 ──► ...     （stage 由 needs 决定，串行）
          │ 扇出所有 job 实例到全局队列
          ▼
   worker pool（默认 max-containers+8）
          │ 每个 JobUnit 一条执行链
          ▼
   runner.ExpandRun / runner.NewJobExecutor
          │
          ▼
   RunContext.startContainer() ──Acquire 全局槽──► Docker 容器（1 job 实例）
          │
          └─ job executor 最外层无条件 Finally ──Release 槽
```

**运行形态**：`act serve` 只是 Docker API 客户端，本身不要求 DinD。推荐（也是当前部署）
在宿主机直接运行，通过 `/var/run/docker.sock` 创建兄弟容器；如果要把 act 放进容器，
应挂载宿主 socket（Docker-out-of-Docker），而不是在内层再起一个 dockerd。DinD 也能用
（`DOCKER_HOST` 指向内层 daemon），但 CPU 编号、路径映射、清理边界都会不同，详见 4.9。

### 2.2 调度模型

**Run / Stage / Job 实例 / 容器**

- 一个 **Run** 对应一个请求：一个 `workdir`、一个 event、一个 workflows 路径。
- Run 被 `model.Plan` 拆成 **Stage**，stage 之间严格串行（尊重 `needs`）。
- 一个 stage 内，所有 job 被 `ExpandRun` 展开成 **Job 实例**：
  - 非矩阵 job 只有 1 个实例；
  - 矩阵 job 有 N 个实例（每个 matrix 组合一个）；
  - 每个实例拥有独立的 `RunContext`、独立的容器。
- 同一 job 的矩阵实例受 `strategy.max-parallel`（默认 4）限制；这一限制用
  per-group gate 在 orchestrator 侧实现，**不会占用 worker**，因此不会饿死其他 run。

**全局队列与 worker**

- 所有 run 的所有 Job 实例进入同一个优先级队列：`priority` 大的先出队，
  相同优先级按提交序号 FIFO。
- worker 数量默认 `max-containers+8`，worker 从队列取出任务后执行
  `runner.NewJobExecutor` 生成的执行链。
- 一个 run 的 orchestrator 只负责展开与推进 stage，自身不执行 job，因此不会被
  阻塞；它等待本 stage 所有实例完成后才展开下一 stage。

**全局容器上限（`--max-containers`，默认 20）**

- `runner.Config.JobSlotLimiter` 指向调度器的全局信号量 `Semaphore`。
- `RunContext.startContainer()` 在真正创建容器**之前** `Acquire`；无论成功、失败、
  取消还是 panic，都在 job executor 最外层的**无条件 `Finally`** 中 `Release`。
- 因为限流点在容器创建层，所以：
  - 矩阵实例每个都计一个容器；
  - reusable workflow 的子 job 也计一个容器（父 job 本身不建容器、不占槽）；
  - `--concurrent-jobs` 等参数不会绕过该上限。
- 获取顺序固定为「per-job gate → 全局槽」，且等待槽的 worker 不持有其他槽，
  因此不存在循环等待。

**每个 job 的 CPU 预算（`--job-cpus`，默认 4）**

- 背景：容器默认继承宿主的 CPU 视图（本机 `nproc=104`）。很多测试框架按
  `nproc`/`NumCPU` 决定进程/线程池大小，并按同样比例申请内存，在大核数机器上
  会在单个 job 内 OOM。
- 创建 job 容器时同时设置：
  - `HostConfig.Resources.NanoCPUs = 4e9`：CFS 配额，限制实际可用的 CPU 时间；
  - `HostConfig.Resources.CpusetCpus`：从本进程允许的 CPU 中轮转分配 4 个核，
    使容器内 `nproc`、`sched_getaffinity`、Go `runtime.NumCPU()`、Java 等
    看到的核数都是 4；
  - 容器环境默认值（workflow 显式设置时以 workflow 为准）：`GOMAXPROCS`、
    `OMP_NUM_THREADS`、`OPENBLAS_NUM_THREADS`、`MKL_NUM_THREADS`、
    `NUMEXPR_NUM_THREADS`、`VECLIB_MAXIMUM_THREADS`、
    `PYTEST_XDIST_AUTO_NUM_WORKERS`、`PYTHON_CPU_COUNT`。
- 只作用于 **job 容器**（含矩阵实例和 reusable workflow 子 job），service 容器
  不受限；`--job-cpus 0` 关闭全部限制。
- 同时修正了两个「按宿主核数取默认值」的位置：
  - `runner.Config.GetConcurrentJobs()` 在未显式设置 `--concurrent-jobs` 时，
    默认并发数不再直接取 `runtime.NumCPU()`，而是被 `--job-cpus` 截断；
  - 调度器 `--workers` 默认固定为 `max-containers+8`，不再放大到 `2×CPU`。

**结果判定**

- act 的 step 失败会被内部吞掉，因此调度器在 job 执行前后读取 runner 记录的
  `Job.Result`，并结合执行返回的 error 判定该 job 的最终状态。
- 每个 job 实例对外同时暴露两个终态字段：
  - `status`：`success` / `failure` / `cancelled` 等完整状态；
  - `result`：只做二值化，`success` 或 `failure`，由终态规则直接映射，不经过
    任何模型；`cancelled` 也映射为 `failure`。
- Run 在所有 stage 完成后调用 `Plan.Finalize()`；任一 job 失败 → run `failure`。
- Matrix 有两层结果：runtime 的 `Job.Result` 是**组结果**（供 `needs`/plan 使用），
  `RunContext.LegResult` 是**本 leg 结果**；日志的 `🏁` 和调度器的 job 状态都按
  leg 记录，成功的 leg 不会因为兄弟 leg 失败而被标红。
- 因前一 stage 失败而没有实际执行的 job、排队阶段被取消的 job，也会被补记成
  `status=failure`/`cancelled` + `result=failure`，保证每个已登记 job 都有二值结果。
- 超时/取消通过 `common.JobCancelContext` 传播，job 状态记为 `cancelled`。

**取消与超时**

- `POST /v1/runs/{id}/cancel` 取消 run ctx 与 job ctx；排队中的 unit 出队时会检查
  ctx 并直接标记取消。
- 超时优先级：请求的 `timeoutSeconds` > 守护进程的 `--run-timeout` > 不限时。

### 2.3 Runner 层改造

`pkg/runner/runner.go` 把原来 `NewPlanExecutor` 中的逻辑拆成可单独调用的 API：

```go
ExpandRun(ctx, run) (*JobExpansion, error)      // 只展开 matrix / 计算名称，不执行
NewJobExecutor(ctx, inst, maxJobNameLen)        // 一个实例的执行链
```

- `NewPlanExecutor` 改为调用这两个 API 的薄编排层，矩阵命名、stage 宽度与执行顺序
  保持原样；默认并发数（`GetConcurrentJobs`）受 `--job-cpus` 截断，见 2.2。
- `ExpandRun` 会在扇出之前补齐 step ID，消除执行期对共享 `stepModel.ID` 的并发写。
- 新增 `runner.Config` 字段：

| 字段 | 用途 |
| --- | --- |
| `RunID` | 拼入容器/卷/网络名，避免同一 commit 并发时撞名 |
| `ToolCacheVolume` | toolcache 卷前缀；调度器为每个 job 实例追加唯一后缀（空则共享 `act-toolcache`） |
| `EventJSON` | 内联 event payload，优先于 `EventPath` |
| `JobSlotLimiter` | 全局容器信号量；nil 表示不限流（原 CLI 默认） |
| `JobCPUs` | 每 job 容器的 CPU 预算（quota、亲和窗口、线程池环境变量）；0 = 不限 |
| `Clone()` | 每 run 拷贝一份配置，运行期不再改共享 Config |

### 2.4 隔离

| 资源 | 隔离方式 |
| --- | --- |
| job 容器 / workdir 卷 / env 卷 / service 网络 | 名字以 `act-<runID>-...` 开头 |
| `GITHUB_RUN_ID` / `GITHUB_RUN_NUMBER` | 每 run 分配一个递增数值（进程内唯一） |
| toolcache | 每个 job 实例独立卷 `act-toolcache-<runID>-<jobhash>`，job 结束删除；避免 matrix leg 并发安装同一工具；`--shared-toolcache` 可共享 |
| action cache | 默认多 run 共享，但同一仓库的 fetch 用 keyed mutex 串行化；`--per-run-action-cache` 隔离每 run 的缓存目录（new cache 模式的 bare repo 仍共享） |
| artifact server | 守护进程一个实例，靠 `GITHUB_RUN_ID` 与 token 隔离目录 |
| dependency cache server | 每 run 一个实例（act 的缓存存储没有 repo 维度，共享会串仓） |
| 日志 | 每 job 实例一个文件；reusable workflow 子 job 也单独成文件 |
| 工作目录 | 每个 job 把 workdir 复制进自己的容器（默认），宿主目录不会被并发写 |

数据目录布局：

```
<data-dir>/
├── runs/<runID>/logs/<job-key>.log     # 每个 job 实例
├── runs/<runID>/logs/_scheduler.log    # run 级调度日志
├── runs/<runID>/cache/bolt.db          # 该 run 的 dependency cache
├── runs/<runID>/actions/               # 仅 --per-run-action-cache
└── artifacts/<GITHUB_RUN_ID>/          # 共享 artifact server 按 run 分目录
```

### 2.5 并发安全修补

为了让「多 run × 多 job × 多容器」在 Go race detector 下干净运行，修复了 act 内部
若干原本只在串行执行下才安全的点：

| 位置 | 问题 | 修法 |
| --- | --- | --- |
| `pkg/model.Job` | matrix 实例并发写 `Result`/`Outputs` | 加锁 + `SetResult/GetResult/SetOutputs/GetOutputs` |
| step 模型 | 并发写共享 `step.ID` | 展开阶段预置 |
| `RunContext.Masks` | 并发 append/读取 | `MaskList`（锁 + 快照） |
| `RunContext.Cancelled` | 并发读写 | `atomic.Bool` |
| action fetch | 同一仓库并发 `PlainInit/Fetch` | 按 bare-repo 路径 keyed mutex |
| git clone | 全局锁串行化所有下载 | 按目标目录 keyed mutex |
| `LocalRepositoryCache` | map 无锁 | `RWMutex` |
| reusable workflow | 共享 executor 并发调用 | 按 key keyed mutex |
| artifact server | listen 失败 `log.Fatal` 杀进程 | 新增 `ServeE` 返回 error |
| 运行期 Config 修改 | 共享 Config 被并发写 | 每 run `Clone()`，不再运行期改写 |

### 2.6 生命周期与关闭

- 启动时：创建数据目录 → 启动 artifact server（同步 bind，失败即退出）→ 启动 worker。
- `Submit` 同步完成校验与准入（排队 + 运行中的 run 数 ≤ `--max-queued-runs`），
  然后异步执行；返回 `202` 与 run 状态。
- 关闭：收到 SIGINT/SIGTERM 后进入 draining（`/healthz` 返回 503，新提交返回 503）→
  取消所有 run → 等待 worker（受关闭 ctx 约束）→ 关闭队列与日志 → 若 worker 超时
  未退出，按 runID 前缀强制删除残留容器、卷和网络。
- `--keep-runs` 控制内存中保留的历史 run（0 = 全部保留）。

### 2.7 遗留资源回收

正常路径上，job executor 的 `Finally` 链会删除自己的容器、env 卷、toolcache 卷和
service 网络。但它可能被跳过：`docker create` 之后、`docker start` 之前 run 被取消
（例如客户端超时后主动 cancel），容器只停留在 `Created` 状态，清理链不会执行。

为此调度器增加了三层兜底：

1. **job executor 最外层 Finally**：清理挂到整个 job 执行链（而不是内部
   stage 链）上，`Set up job` 失败也会执行；`stopContainer`/`closeContainer` 幂等，
   正常路径不会重复删除。
2. **每 run 清理队列（janitor）**：`finishRun` 把 runID 加入队列，`--cleanup-grace`
   （默认 45s）后按前缀删除 `act-<runID>-*` 容器/卷/网络和
   `act-toolcache-<runID>*` 卷；docker 暂时不可用时按 `--cleanup-retry-delay`
   重试，直到成功。调度器启动时会把 `data-dir/runs/` 下的历史 run 全部排入队列，
   覆盖上一次进程被 SIGKILL 的情况。
3. **孤儿扫描（orphan sweep）**：`--cleanup-orphans`（默认开启）每
   `--cleanup-orphan-interval`（默认 10m）扫描一次，删除名字里带合法 runID 且
   创建时间超过 `--cleanup-orphan-age`（默认 1h）的 `act-<runID>-*` /
   `act-toolcache-<runID>*` 容器、卷、网络，以及 act 构建的 docker action 镜像
   （`act-github-actions-exec-in-build-image-*`）和悬空镜像。仍处于 `running`
   的容器只有在超过 3 小时（远超 job 时长上限）时才强制删除；不属于 act 命名规则
   的卷（`pact-*`、`sonarqube_*`、`trace-hv-dind-*` 等）永远不会被触碰。

`--reuse`（保留容器）开启时所有清理自动跳过；`--shared-toolcache` 开启时不会删除
共享的 `act-toolcache` 卷。driver 侧在启动时也会清理 `_campaign/` 下超过 1 小时、
且不属于仍在排队/运行 run 的 worktree。

---

## 3. Quickstart

### 3.1 前置条件

- Docker daemon 可用（当前用户有权限访问 socket）。
- 一个已 checkout 的仓库目录，包含 `.github/workflows/*.yml`。
- 对应的 runner 镜像（例如 `-P ubuntu-latest=catthehacker/ubuntu:act-latest`）。
- 构建需要 Go；本仓库 `go.mod` 要求较新的 Go 版本，若本机 Go 较旧且允许联网，
  Go 会自动下载对应 toolchain。需要代理时先：

  ```bash
  export http_proxy=http://<proxy>:<port>
  export https_proxy=http://<proxy>:<port>
  ```

> 重要：未启用离线模式时，action 的 `git clone` 发生在**守护进程（宿主机）**里；
> 容器内 action 的下载/安装走 job 环境变量。两处都需要网络（见
> [4.8](#48-日志数据目录与排障)）。当前部署使用 `--action-offline-mode` +
> `--local-repository`，action 从本地读取，但步骤内的下载仍需要代理。

### 3.2 构建

```bash
cd act
go build -o dist/local/act main.go
```

### 3.3 启动调度器

```bash
./dist/local/act serve \
  --addr 127.0.0.1:8080 \
  --data-dir /var/lib/act-serve \
  --max-containers 20 \
  --job-cpus 4 \
  --allowed-workdir-roots /srv/ci \
  -P ubuntu-latest=catthehacker/ubuntu:act-latest
```

### 3.4 提交一个 run

CLI：

```bash
./dist/local/act submit \
  --addr http://127.0.0.1:8080 \
  --workdir /srv/ci/myrepo \
  --repo org/myrepo \
  --commit 4a526842b67e1c1a465e5a1d9d575305dfdbb274 \
  --event pull_request \
  --event-file /srv/ci/myrepo/event.json \
  --workflows-path .github/workflows/ci.yml \
  --wait
```

HTTP：

```bash
curl -sS -X POST http://127.0.0.1:8080/v1/runs \
  -H 'Content-Type: application/json' \
  -d '{
        "repo": "org/repo",
        "commit": "4a52684",
        "workdir": "/srv/ci/myrepo",
        "workflowsPath": ".github/workflows/ci.yml",
        "event": "pull_request",
        "eventJSON": {"action": "synchronize"},
        "matrix": {"python-version": ["3.12"]},
        "priority": 0
      }'
```

### 3.5 查看结果

```bash
curl -sS http://127.0.0.1:8080/v1/runs/<id>
curl -sS http://127.0.0.1:8080/v1/runs/<id>/logs
curl -sS 'http://127.0.0.1:8080/v1/runs/<id>/logs?job=Lint/jq/0'
curl -sS 'http://127.0.0.1:8080/v1/runs/<id>/logs?job=Lint/jq/0&follow=1'
```

### 3.6 停止

```bash
kill -TERM <pid>    # 取消活动 run，清理容器，然后退出
```

---

## 4. 调用者使用说明

### 4.1 `act serve` 参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--addr` | `127.0.0.1:8080` | HTTP 监听地址 |
| `--data-dir` | `~/.cache/serve` | 日志、cache、artifact 根目录 |
| `--max-containers` | `20` | **全局**同时运行的 job 容器上限；当前部署脚本用 `PACT_MAX_CONTAINERS` 覆盖（默认 10，本轮 campaign 为 24） |
| `--workers` | `max-containers + 8` | 执行 job 的 worker 数（不再随宿主核心数放大） |
| `--job-cpus` | `4` | 每个 job 容器的 CPU 预算（quota + 亲和性窗口 + 线程池默认值），0 = 不限制；亲和窗口仅 Linux（远端 daemon 见 4.9） |
| `--max-queued-runs` | `100` | 排队 + 运行中的 run 数上限，超出返回 503 |
| `--max-units-per-run` | `200` | 单个 run 的 job 实例上限，超出则 run 失败；当前部署为 300 |
| `--run-timeout` | `0` | run 默认超时（0 = 不限） |
| `--allowed-workdir-roots` | 当前目录 | 允许执行的 checkout 根目录，逗号分隔 |
| `--api-token` | 空 | Bearer token；监听非 loopback 地址时必填 |
| `--log-stdout` | `false` | 同时把 job 日志镜像到 stdout |
| `--shared-toolcache` | `false` | 所有 run 共享 `act-toolcache` 卷 |
| `--per-run-action-cache` | `false` | 每 run 独立的 action 缓存目录（见下方说明） |
| `--allow-bind-workdir` | `false` | 允许 `--bind`（默认禁止，避免并发写宿主目录） |
| `--keep-runs` | `0` | 内存中保留的已结束 run 数，0 = 全部保留；当前部署为 1000 |
| `--artifact-server-dir` | `<data-dir>/artifacts` | artifact 存储目录 |
| `--cleanup-interval` | `15s` | janitor 检查到期清理的周期 |
| `--cleanup-grace` | `45s` | run 结束后延迟多久清理遗留 docker 资源 |
| `--cleanup-orphans` | `true` | 周期性清理不属于活跃 run 的 act 资源（容器/卷/网络/act 镜像） |
| `--cleanup-orphan-age` | `1h` | 孤儿资源的最小年龄，低于该年龄不删除 |
| `--cleanup-orphan-interval` | `10m` | 孤儿扫描周期 |

同时接受原 CLI 的常见运行参数（`-P/--platform`、`-s/--secret`、`--var`、`--env`、
`--input`、`--matrix`、`-p/--pull`、`-r/--reuse`、`--privileged`、`--userns` 等），
它们构成所有请求共享的基线配置；`.actrc` 里的这些参数不会导致 `serve` 报错。
`--artifact-server-addr/--artifact-server-port` 是根命令的持久参数，同样可用。

> `--per-run-action-cache` 会为每个 run 设置独立的 `ActionCacheDir`；默认（旧）
> action cache 模式下，action 的克隆/解压目录也随之隔离。若同时启用
> `--use-new-action-cache`，底层以 `org/repo` 为键的 bare git 仓库仍在共享目录中，
> 由按仓库的 keyed mutex 串行化，是并发安全的，但不是物理隔离。

Secrets 在启动时一次性加载：`-s NAME=value`、`--secret-file`、环境变量，以及
`GITHUB_TOKEN`（若可用）。请求中**只能引用名字**，值默认会被日志掩码机制处理
（`--insecure-secrets` 会关闭掩码），也不会出现在 API 响应里。

> `--concurrent-jobs` 未显式设置时：CLI 的默认并发数为 `min(NumCPU, --job-cpus)`；
> `serve` 不使用这个默认值，它的并发由全局容器槽 `--max-containers` 控制。
> `--job-cpus` 是根命令的持久参数，对普通 `act` CLI 和 `act serve` 都生效。

#### 参考部署

一份参数化的启动脚本见 [`deploy/start-serve.sh`](deploy/start-serve.sh)。所有路径、端口、
容器上限、镜像和凭据都通过环境变量或命令行参数配置：

| 参数 | 默认/来源 | 说明 |
| --- | --- | --- |
| `--addr` | `127.0.0.1:8080` | HTTP 监听地址（`PACT_ADDR`） |
| `--api-token` | `$PACT_TOKEN_FILE` | 从 0600 文件读取；非 loopback 监听时必填 |
| `--data-dir` | `$PACT_DATA_DIR` | 日志、缓存、artifact 根目录 |
| `--allowed-workdir-roots` | `$PACT_WORKDIR_ROOTS` | 允许执行的 checkout 根目录 |
| `--network` | `$PACT_NETWORK`（默认 `host`） | Docker 网络；使用 bridge 时注意 artifact/cache 可达地址 |
| `--max-containers` | `$PACT_MAX_CONTAINERS`（默认 20） | 全局 job 容器上限 |
| `--job-cpus` | `$PACT_JOB_CPUS`（默认 4） | 每 job CPU 预算 |
| `--max-units-per-run` | `$PACT_MAX_UNITS_PER_RUN`（默认 200） | 单 run job 实例上限 |
| `--keep-runs` | `$PACT_KEEP_RUNS`（默认 0） | 内存保留的历史 run 数 |
| `--container-daemon-socket -` | 可选 | 不把宿主 Docker socket 挂进 job 容器 |
| runner 镜像 | `-P` 参数 | 见下方镜像选择 |

Secrets 通过 0600 文件或环境变量注入，文档和示例中只出现名字，不出现值。

#### 镜像选择

`RunRequest` 没有镜像字段，镜像由以下三层决定（优先级从高到低）：

1. workflow 的 job 级 `container.image`（可写死；对于声明了 inputs 的
   `workflow_dispatch` workflow，可写 `${{ inputs.image }}`，值通过
   `eventJSON.inputs` 传入，或在不提供 `eventJSON` 时用请求的 `inputs` 传入）；
2. `serve` 启动时通过 `-P <runs-on 标签>=<镜像>` 建立的映射，所有请求共享；
3. act 内置默认映射（例如 `ubuntu-latest` → `node:16-buster-slim`），一般不建议依赖。

service 容器的镜像由 workflow 的 `services.<id>.image` 决定。示例映射：

| `runs-on` | 镜像 |
| --- | --- |
| `ubuntu-latest` / `ubuntu-24.04` | `catthehacker/ubuntu:act-latest` |
| `ubuntu-22.04` | `catthehacker/ubuntu:act-22.04` |
| `ubuntu-20.04` | `catthehacker/ubuntu:act-20.04` |
| `ubuntu-18.04` | `catthehacker/ubuntu:act-18.04` |

```bash
act serve ... \
  -P ubuntu-latest=catthehacker/ubuntu:act-latest \
  -P ubuntu-22.04=catthehacker/ubuntu:act-22.04
```

### 4.2 请求字段（`RunRequest`）

| 字段 (JSON) | 必填 | 默认 | 说明 |
| --- | --- | --- | --- |
| `workdir` | 是 | – | 绝对路径；必须位于 `--allowed-workdir-roots` 下 |
| `repo` | 否 | 空 | 元数据；同时作为默认 `GITHUB_REPOSITORY` |
| `commit` | 否 | 空 | 元数据；同时作为默认 `GITHUB_SHA` |
| `workflowsPath` | 否 | `.github/workflows/` | 相对 `workdir`，或 `workdir` 内的绝对路径；可以是目录或单个文件 |
| `event` | 否 | `push` | 事件名；当设置了 `job` 时忽略 |
| `eventJSON` | 否 | 空 | 内联事件 payload（JSON 对象），用于 `github.event` |
| `job` | 否 | 空 | 只跑指定 job ID（等价 `act -j`） |
| `matrix` | 否 | 空 | `{"key":["v1","v2"]}`，只保留匹配的矩阵组合（等价 `--matrix key:v`） |
| `env` | 否 | 空 | 追加/覆盖 job 环境变量 |
| `vars` | 否 | 空 | 追加/覆盖 `vars` |
| `inputs` | 否 | 空 | 仅在没有 `eventJSON` 时用于构造事件 payload 的 `inputs`；提供 `eventJSON` 时请写在 `eventJSON.inputs` 里 |
| `secretRefs` | 否 | 空 | 需要的 secret 名字；为空表示使用守护进程全部 secrets；未知名字返回 400 |
| `timeoutSeconds` | 否 | 0 | 0 表示采用守护进程 `--run-timeout` |
| `priority` | 否 | 0 | 越大越先出队；同优先级 FIFO |
| `noRecurse` | 否 | `false` | 对应 act 的同名行为 |
| `strict` | 否 | `false` | 对应 act 的同名行为 |

约束：

- `workdir` 必须是绝对路径，会做 `EvalSymlinks` 后校验是否在允许根目录内。
- `workflowsPath` 解析后必须仍在 `workdir` 内。
- `env` 不能设置 `GITHUB_RUN_ID` / `GITHUB_RUN_NUMBER`，它们由调度器管理。
- 请求体上限 1 MiB，未知字段会被拒绝（`DisallowUnknownFields`）。

### 4.3 HTTP API

所有接口返回 JSON（job 日志流除外）。若启动了 `--api-token`，除 `/healthz` 外都需要：

```
Authorization: Bearer <token>
```

#### 4.3.1 接口总览

| 方法 | 路径 | 说明 | 成功码 |
| --- | --- | --- | --- |
| `POST` | `/v1/runs` | 提交 run，异步执行；立即返回提交时的 `RunState` 快照 | `202` |
| `GET` | `/v1/runs` | 列表；支持 `limit`（默认 100）、`status`、`repo`、`commit` 精确过滤；按创建时间倒序 | `200` |
| `GET` | `/v1/runs/{id}` | run 详情（含每个已展开 job 实例的状态） | `200` |
| `GET` | `/v1/runs/{id}/logs` | 返回 `{"run": id, "logs":[{key,size}]}` | `200` |
| `GET` | `/v1/runs/{id}/logs?job=KEY` | 返回该 job 日志文本 | `200` |
| `GET` | `/v1/runs/{id}/logs?job=KEY&offset=N&follow=1` | 从字节偏移开始，持续跟随到 run 结束 | `200` |
| `POST` | `/v1/runs/{id}/cancel` | 取消排队或运行中的 run；返回 `{"status":"cancelling"}` | `200` |
| `GET` | `/v1/stats` | 队列 / job / 容器计数 | `200` |
| `GET` | `/healthz` | 存活检查；draining 时 `503` | `200` |

错误码：

| 码 | 场景 |
| --- | --- |
| `400` | JSON 非法、未知字段、workdir/workflows 非法、未知 secret 引用、非法 env |
| `401` | token 缺失或不匹配 |
| `404` | run 不存在 |
| `409` | 取消一个已结束的 run |
| `503` | 队列已满（`--max-queued-runs`）或正在关闭 |

#### 4.3.2 返回对象字段

`RunState`（`POST /v1/runs`、`GET /v1/runs/{id}`、`GET /v1/runs` 列表元素）：
| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `id` | string | run ID；也是容器/卷/日志目录的命名依据 |
| `repo` | string | 请求里的仓库元数据，可为空 |
| `commit` | string | 请求里的 commit 元数据，可为空 |
| `workdir` | string | 实际执行的 checkout 绝对路径（已做符号链接解析和根目录校验） |
| `event` | string | 事件名；默认 `push` |
| `status` | enum | `queued` / `running` / `success` / `failure` / `cancelled` |
| `createdAt` | RFC3339 | 请求被接受的时间 |
| `startedAt` | RFC3339 | 开始执行的时间；排队中省略 |
| `finishedAt` | RFC3339 | 进入终态的时间；未结束时省略 |
| `error` | string | run 级错误（超时、取消、展开失败等）；成功时省略 |
| `jobs` | array | 已展开的 job 实例；提交响应里通常为空，stage 展开后才逐步出现 |

`GET /v1/runs` 返回 `{"runs": [RunState, ...]}`，按创建时间倒序；`limit` 是最大
条数，不是分页游标。`GET /v1/runs/{id}` 直接返回一个 `RunState`。

`JobState`（`jobs[]` 元素）：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `key` | string | job 实例唯一键：`<sanitize(workflow 名)>/<sanitize(job id)>/<matrix 序号>`，序号从 0 开始；非矩阵 job 固定为 0 |
| `workflow` | string | workflow 名（不是文件名） |
| `jobId` | string | workflow 中的 job id（不是显示名） |
| `matrix` | object | 该实例对应的矩阵组合；非矩阵 job 省略 |
| `status` | enum | `queued` / `running` / `success` / `failure` / `cancelled`；`skipped` 为保留值 |
| `result` | string | **二值结果**：`success` 或 `failure`；job 进入终态后由规则直接写入，`cancelled` 映射为 `failure`；未进入终态时省略 |
| `startedAt` | RFC3339 | 开始执行的时间 |
| `finishedAt` | RFC3339 | 进入终态的时间 |
| `error` | string | job 级错误；成功时省略 |

要点：

- `status` 是完整状态，`result` 是给调用方的二值结论。二者都只依赖 runner 终态，
  **不经过 LLM**。
- 提交响应是“提交瞬间”的快照，`jobs` 通常为空；要看 job 结果必须轮询
  `GET /v1/runs/{id}`。
- 已登记但未执行完的 job（例如排队阶段被取消，或 group finalize 已把同组判为失败）
  会被补记成 `failure`/`cancelled`，并带 `result=failure`；尚未展开的后续 stage
  不会凭空生成 job 条目。
- `POST /v1/runs` 的 `202` 响应不包含 `timeoutSeconds`、`priority`、`env` 等请求
  字段；需要复核时以调用方自己的请求记录为准。

`Stats`（`GET /v1/stats`）：

| 字段 | 含义 |
| --- | --- |
| `runsTotal` | 内存中保留的 run 总数（受 `--keep-runs` 影响） |
| `runsQueued` / `runsRunning` | 排队 / 运行中的 run 数 |
| `runsSucceeded` / `runsFailed` / `runsCancelled` | 已结束 run 的分类计数 |
| `jobsQueued` / `jobsRunning` | job 实例的排队 / 运行数 |
| `containersUsed` | 当前已占用的全局容器槽 |
| `containersMax` | 全局容器槽上限（`--max-containers`） |
| `queueLength` | 优先级队列中的 job 任务数 |

日志列表（`GET /v1/runs/{id}/logs`）：

```json
{
  "run": "<runID>",
  "logs": [
    {"key": "CI/lint/0", "size": 12345},
    {"key": "CI/test/0", "size": 67890}
  ]
}
```

`size` 是日志文件当前字节数，可用于增量读取；`key` 可以直接作为
`?job=KEY` 的参数。`follow=1` 返回 `text/plain` 流，不是 JSON。

#### 4.3.3 典型请求与结果示例

下面用一个最小请求验证 HTTP 链路。示例中的 token、路径、commit 需要替换。

提交：

```bash
curl -i -sS -X POST http://127.0.0.1:8080/v1/runs \
  -H "Authorization: Bearer $ACT_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "repo": "acme/demo",
    "commit": "4a526842b67e1c1a465e5a1d9d575305dfdbb274",
    "workdir": "/srv/ci/repos/acme-demo",
    "workflowsPath": ".github/workflows/ci.yml",
    "event": "pull_request",
    "eventJSON": {"action": "synchronize", "number": 42},
    "env": {"CI": "true"},
    "secretRefs": ["GITHUB_TOKEN"],
    "priority": 10,
    "timeoutSeconds": 1800
  }'
```

提交成功返回 `202 Accepted`，body 是提交时的快照（`jobs` 通常为空）：

```http
HTTP/1.1 202 Accepted
Content-Type: application/json
Date: Wed, 05 Nov 2026 10:15:00 GMT

{
  "id": "r20261105T1015001234abcd",
  "repo": "acme/demo",
  "commit": "4a526842b67e1c1a465e5a1d9d575305dfdbb274",
  "workdir": "/srv/ci/repos/acme-demo",
  "event": "pull_request",
  "status": "queued",
  "createdAt": "2026-11-05T10:15:00+08:00",
  "jobs": []
}
```

轮询结果：

```bash
curl -i -sS \
  -H "Authorization: Bearer $ACT_TOKEN" \
  http://127.0.0.1:8080/v1/runs/r20261105T1015001234abcd
```

终态返回 `200 OK`；每个已登记 job 都带 `status` 和 `result`：

```http
HTTP/1.1 200 OK
Content-Type: application/json
Date: Wed, 05 Nov 2026 10:18:20 GMT

{
  "id": "r20261105T1015001234abcd",
  "repo": "acme/demo",
  "commit": "4a526842b67e1c1a465e5a1d9d575305dfdbb274",
  "workdir": "/srv/ci/repos/acme-demo",
  "event": "pull_request",
  "status": "failure",
  "createdAt": "2026-11-05T10:15:00+08:00",
  "startedAt": "2026-11-05T10:15:01+08:00",
  "finishedAt": "2026-11-05T10:18:19+08:00",
  "jobs": [
    {
      "key": "CI/lint/0",
      "workflow": "CI",
      "jobId": "lint",
      "status": "success",
      "result": "success",
      "startedAt": "2026-11-05T10:15:02+08:00",
      "finishedAt": "2026-11-05T10:15:40+08:00"
    },
    {
      "key": "CI/test/0",
      "workflow": "CI",
      "jobId": "test",
      "matrix": {"python-version": "3.12"},
      "status": "failure",
      "result": "failure",
      "error": "job test failed",
      "startedAt": "2026-11-05T10:15:02+08:00",
      "finishedAt": "2026-11-05T10:18:19+08:00"
    }
  ]
}
```

查看日志列表和单个 job 日志：

```bash
curl -sS -H "Authorization: Bearer $ACT_TOKEN" \
  http://127.0.0.1:8080/v1/runs/r20261105T1015001234abcd/logs
# {"run":"r20261105T1015001234abcd","logs":[{"key":"CI/lint/0","size":12345}, ...]}

curl -sS -H "Authorization: Bearer $ACT_TOKEN" \
  'http://127.0.0.1:8080/v1/runs/r20261105T1015001234abcd/logs?job=CI%2Ftest%2F0'
```

请求校验失败示例（未知字段）：

```bash
curl -i -sS -X POST http://127.0.0.1:8080/v1/runs \
  -H "Authorization: Bearer $ACT_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "workdir": "/srv/ci/repos/acme-demo",
    "image": "ubuntu:24.04"
  }'
```

```http
HTTP/1.1 400 Bad Request
Content-Type: application/json

{"error":"invalid JSON body: json: unknown field \"image\""}
```

`RunRequest` 没有 `image` 字段；镜像由 workflow 的 `container.image` 和启动时的
`-P` 映射决定（见 4.1）。其他常见错误：

```json
{"error":"workdir \"/srv/ci/repo\" is outside the allowed roots"}
{"error":"missing or invalid API token"}
{"error":"run queue is full"}
```

健康检查与实时计数：

```bash
curl -sS http://127.0.0.1:8080/healthz
# {"status":"ok"}

curl -sS -H "Authorization: Bearer $ACT_TOKEN" \
  http://127.0.0.1:8080/v1/stats
# {"runsTotal":12,"runsQueued":0,"runsRunning":24,
#  "runsSucceeded":10,"runsFailed":2,"runsCancelled":0,
#  "jobsQueued":23,"jobsRunning":24,"containersUsed":24,
#  "containersMax":24,"queueLength":23}
```

#### 4.3.4 二值结果与类别结果

`act serve` 只返回 4.3.2 的 `status` / `result` 和原始 job 日志；它不返回
“环境/代码”类别。类别由外部验证/评分流程读取
job 日志后生成，字段如下：

| 字段 | 取值 | 含义 |
| --- | --- | --- |
| `error_class` | `environment` / `business` / `unknown` | run 级最终大类 |
| `error_category` | `proxy_network`、`package_registry`、`package_missing`、`dependency_resolution`、`docker_infrastructure`、`toolchain_missing`、`action_fetch`、`permission_auth`、`test_failure`、`compile_build_error`、`lint_format_type`、`unknown` 等 | 细分类；环境类和业务类共用一个字段空间 |
| `error_class_source` | `llm` / `llm_error` / `unknown` | 结论由纯 LLM 产生、LLM 调用失败，或历史数据/无法判断（旧轮次可能出现 `rule`） |
| `error_evidence` | string | 命中的日志证据或模型给出的简短理由 |
| `job_results` | `{jobKey: "success"|"failure"}` | 逐 job 二值结果（验证层按 run 的 job 列表归一化） |
| `job_categories` | `{jobKey: {error_class, category, source, evidence}}` | 逐 job 类别结果；只对失败 job 生成，成功 job 不产生条目 |

判定方式：

1. 验证层把失败 job 的日志、act 给出的失败 step、语言等上下文交给
   DeepSeek-V4.1-Flash，使用结构化提示词要求它只输出
   `environment` 或 `business`（并给出 category、confidence、root cause step、
   evidence、reasoning）；
2. 输入按长度和失败 job 数量选择：
   - 日志 `< 18KB`：全文输入；
   - 日志 `>= 18KB` 且只有一个失败 job：只输入该失败 job 的 job 级日志；
   - 日志 `>= 18KB` 且多个失败 job：先对每个失败 job 单独调用 LLM 做摘要，
     要求返回关键失败 step 的日志原文，形成缩短版；再把多个缩短版合并，
     做最终 environment/business 判定；
3. 不再用正则规则决定环境侧；`unknown` 只表示模型调用/解析失败，
   调用方不要把 `unknown` 当成业务失败；
4. LLM 只产生类别结果，不改变 `status` / `result` 和二值 job 结论。

验证层还会记录 `input_mode`（`full` / `job` / `job_summaries` /
`full_clipped`）、`input_chars` 和 `input_jobs`，用于审计本次分类实际使用了
哪种输入路径。

因此，调用方应把 `jobs[].result` 当作二值验收结论，把
`error_class` / `job_categories` 当作诊断信息；二者不要混用。

### 4.4 `act submit`

```bash
act submit \
  --addr http://127.0.0.1:8080 \
  --token <token> \
  --workdir /srv/ci/repo \
  --repo org/repo \
  --commit <sha> \
  --workflows-path .github/workflows/ci.yml \
  --event pull_request \
  --event-file event.json \
  --job test \
  --matrix python-version:3.12 \
  --env FOO=bar \
  --secret-ref GITHUB_TOKEN \
  --timeout 1800 \
  --priority 10 \
  --wait \
  --output text
```

| 参数 | 说明 |
| --- | --- |
| `--addr` | 调度器地址，默认 `http://127.0.0.1:8080` |
| `--token` | API token |
| `--workdir` | checkout 目录（会转成绝对路径），默认 `.` |
| `--repo` / `--commit` | 元数据 |
| `--workflows-path` | workflow 文件或目录 |
| `-e, --event` / `--event-file` | 事件名 / 事件 JSON 文件 |
| `-j, --job` | 只跑一个 job |
| `--matrix` | 可重复，`key:value` |
| `--env` | 可重复，`key=value` |
| `--secret-ref` | 可重复，secret 名字 |
| `--timeout` | 秒；0 使用服务端默认 |
| `--priority` | 越大越先调度 |
| `--wait` | 轮询直到结束并打印每个 job 的 `status` 和 `result`；run 失败时命令返回非 0 |
| `--output` | `text` 或 `json`；`json` 输出完整 `RunState`，含 `jobs[].result` |

### 4.5 状态与结果语义

- Job 状态：`queued` → `running` → `success` / `failure` / `cancelled`。
- Run 状态：`queued` → `running` → `success` / `failure` / `cancelled`。
- Job 二值结果：`result` 只在终态写入；`status=success` ↔ `result=success`，
  `status=failure` 或 `status=cancelled` ↔ `result=failure`。`result` 由规则直接
  映射，不读取日志文本，也不调用 LLM。
- 只要有一个 job 失败，run 即 `failure`（与 GitHub 的整体结论一致）。
- **注意**：act 本身不执行 `fail-fast`（只解析），因此失败 job 不会提前取消同组
  其他矩阵实例；`strategy.max-parallel` 是生效的。
- `needs` 依赖通过 stage 串行保证：同 run 内后一 stage 一定等前一 stage 全部结束；
  不同 run 之间没有任何顺序关系。
- 环境/代码类别不在调度器状态里；它是验证层基于日志的第二类结果，见 4.3.4。

### 4.6 容量与并发规划

- **并发单位是 job 实例**：`--max-containers 20` 表示最多 20 个 job 容器同时运行，
  不是 20 个 run。一个 run 可能瞬间展开 10 个矩阵实例。
- 矩阵 job 受 `max-parallel`（默认 4，未显式配置时）限制：同组最多 4 个实例同时
  进入全局队列。
- 全局队列有界只体现在「已准入 run 数」（`--max-queued-runs`）；job 队列本身无界，
  但被 `--max-units-per-run` 限制。
- 当 job 实例多于容器上限时，多余实例的 worker 会阻塞在 `Acquire`，容器数保持
  在上限内；有实例结束后立即补位（实测 22 个实例 / 上限 20：峰值恰好 20）。
- 容量建议：容器上限 ≈ 机器能同时承受的 CPU/内存压力；worker 默认值足够，
  因为阻塞槽位的 worker 不消耗 CPU。
- **CPU 预算**：`max-containers × job-cpus`（代码默认 20×4=80；当前部署脚本默认
  10×4=40，本轮 campaign 为 24×4=96）是调度器最多占用的核数；每个 job 的 CPU
  亲和性是轮转的 4 核窗口（如 `0-3`、`4-7`……），既限制实际用量，也让容器内
  `nproc` 等接口报告 4。若刻意让 job 使用更多核，可调大该值；设为 0 则恢复为
  不限制（容器看到全部宿主核）。当 `max-containers × job-cpus` 超过可用核数时，
  窗口会回绕并共享 CPU，限制仍然生效，只是并行度下降。
- `priority` 高的 run 的 job 先出队；大量高优先级 run 会推迟低优先级任务。

### 4.7 隔离与安全

- **workdir 逃逸防护**：绝对路径 + 符号链接解析 + 允许根目录前缀校验；
  workflows 路径也必须位于 workdir 内。
- **默认禁止 `--bind`**：并发 job 若共享宿主 workdir 会互相污染；确需使用需显式
  `--allow-bind-workdir`。
- **非 loopback 必须设置 token**：否则 `serve` 拒绝启动；token 比较使用常量时间。
- **secrets 只按名引用**：请求无法携带 secret 值；日志中仍会被 act 的掩码机制处理。
- **API 不会回显 secrets**：`GET /v1/runs*` 只包含元数据与状态。
- **artifact server**：host 网络模式下监听 `127.0.0.1:34567`；当前 bridge 部署改为
  监听 docker0 网关 `192.168.0.1:34567`，让 bridge job 容器可达。所有 run 共享，
  靠唯一 `GITHUB_RUN_ID` + 签名 token 隔离；不要在请求 env 中覆盖
  `GITHUB_RUN_ID`（会被拒绝）。
- **Docker socket**：act 默认把 Docker daemon socket 挂进 job 容器
  （`--container-daemon-socket` 未设置为 `-` 时；宿主机直跑时即宿主
  `/var/run/docker.sock`）。当前部署使用 `--container-daemon-socket -`，**不挂载**
  socket，因此 workflow 里的 `docker`/依赖 Docker 的 action 会失败；这类失败属于
  环境侧，不应被当成代码业务失败。若需要支持 Docker-in-workflow，需要显式放开
  socket，并注意这些兄弟容器不受 `--max-containers` 统计，也不会随 run 清理。
- **信任边界**：workflow 内的命令会在宿主机 Docker 上以容器 root 执行，能访问
  容器网络与挂载；只应接受可信仓库的请求。`--allowed-workdir-roots` 限制的是
  “能执行哪些 checkout”，不是“workflow 能做什么”。

### 4.8 日志、数据目录与排障

- 每个 job 一个日志文件：`<data-dir>/runs/<runID>/logs/<job-key>.log`；
  run 级日志：`_scheduler.log`。`--log-stdout` 会同时输出到终端。
- 日志接口按 job key 读取；`offset` 单位是字节，`follow=1` 会持续 tail，直到 run
  进入终态且读完文件。对已结束的 run 使用 `follow=1` 会立即返回。
- **action 获取**：默认由守护进程执行 `☁ git clone ...`，因此 `act serve` 本身要能
  访问 GitHub（或代理）。若启用 `--action-offline-mode` + `--local-repository`
  （当前部署的做法），已映射的 action 全部从本地目录读取，守护进程无需联网。
- **容器内下载**：action（如 `setup-python`）与 `pip/npm/apt` 在容器内执行。当前
  部署使用宿主机透明代理，默认**不注入** `HTTP_PROXY/HTTPS_PROXY/ALL_PROXY`
  （显式代理可用 `PACT_EXPLICIT_PROXY=1` 恢复）；需要显式代理时仍可给 `serve`
  加 `--env http_proxy=... --env https_proxy=...`（以及大写形式），并建议同时设置
  `no_proxy/NO_PROXY` 排除本机 cache/artifact 地址。调度器会把
  `serve --env` 中名字包含 `PROXY` 的变量，以及 `NODE_USE_ENV_PROXY`、
  `GIT_*`、`JAVA_TOOL_OPTIONS`、`GRADLE_OPTS`、`MAVEN_OPTS`，补进：
  - job 容器的基础环境；
  - action step 容器；
  - Docker action 容器。

  注入只补缺失的变量名，**workflow/job/action 自己声明的同名 env 优先**。Node
  原生 `fetch` 还需要 `NODE_USE_ENV_PROXY=1`；Java/Gradle/Maven 的工具链配置可
  通过对应的 `JAVA_TOOL_OPTIONS`/`GRADLE_OPTS`/`MAVEN_OPTS` 传入。显式代理
  配置错误时，常见表现是 job 长时间停在下载步骤，或出现 `Proxy CONNECT aborted`、
  `statusCode=407`、`Could not resolve host` 等网络错误；这些应归为环境失败。
- **代理凭据**：不要把代理 URL（含用户名/密码）、token 或 API key 写进请求、
  workflow、日志或文档。透明代理部署不需要在 `act.env` 中写代理变量；只有显式
  代理模式才从 0600 的 `.proxy` 读取，README 和示例只使用占位符。
- **宿主机直跑的网络**：当前默认 `network=bridge`，普通 job 使用 Docker 默认
  bridge `docker0`（当前 `192.168.0.0/24`，网关 `192.168.0.1`），不使用
  172.x 网段；有 `services` 的 job 仍由 act 创建每 job 独立网络。bridge 下 job
  不能访问宿主机 loopback，artifact server 因此监听 docker0 网关；cache server
  使用宿主机 outbound IP。若改回 `network=host`，artifact server 应使用
  `127.0.0.1`。host 网络下所有 job 共享宿主端口空间，固定端口会冲突；bridge
  下每个 job 有独立网络命名空间。某些域名可能只解析到 IPv6 而宿主机 IPv6 不通，
  表现为 curl 失败或 Node `fetch failed`；给 job 配代理（上一节）可以规避。
- 常见现象：
  - `400 unknown field`：请求里有拼错的字段（API 拒绝未知字段）。
  - `400 workdir is outside the allowed roots`：启动参数没覆盖到该目录。
  - `503 run queue is full`：已准入 run 达到 `--max-queued-runs`。
  - job 失败但日志显示 `Unable to locate package ...`：镜像内软件源/工具缺失，
    属于镜像环境问题，与调度无关（见第 5 章的实测说明）。
  - 崩溃/强杀后可能残留容器：正常 SIGTERM 会等待并在超时后按 runID 前缀强制清理，
    但 `kill -9` 不会。

### 4.9 已知限制

- 状态只在内存中：重启会丢失排队/运行中的 run 记录，崩溃可能留下孤儿容器。
- 没有日志滚动/保留策略；日志与 per-run cache 会持续占用磁盘。
- `fail-fast` 不生效（act 原有行为）。
- 每个 run 一个 dependency cache server（随机端口，数量 ≈ 并发 run 数）。
- 同一 run 的 stage 之间严格串行；即使后一 stage 的 job 与前一 stage 无依赖，
  也不会提前执行（与 act 的 plan 语义一致）。
- 请求必须指向本地已 checkout 的目录，调度器不做 clone/fetch。
- 当前部署为「宿主机直跑 act + 宿主 Docker」，不是 DinD；job 容器使用
  `--container-daemon-socket -`，不挂载 Docker socket。若把 act 放进容器，推荐
  挂载宿主 socket（DooD）；DinD 需要自备 dockerd，且 CPU 编号、路径映射、清理边界
  都不同。放开 socket 后 workflow 创建的嵌套容器不计入上限（见 4.7）。
- `act serve` 不判断失败是环境侧还是业务侧；它只提供 `status`/`result` 和日志。
  类别结果由验证层生成，见 4.3.4。
- `--job-cpus` 只约束 job 容器，不约束 service 容器；host 模式（`-self-hosted`）
  的 job 直接在宿主机运行，也不受 CPU 配额/亲和性限制。
- CPU 亲和性窗口按本进程可见的 CPU 列表轮转，默认假设 Docker daemon 与调度器
  在同一台机器（本地 socket）。若连接远端 daemon 且其 CPU 编号更少，id 可能
  不匹配；这种部署应使用 `--job-cpus 0` 关闭该限制。
- 某些运行时仍会报告宿主核数（例如 Python 的 `os.cpu_count()` 不感知亲和性，3.13+
  才提供 `PYTHON_CPU_COUNT` / `os.process_cpu_count()`）。因此除亲和性外还注入了
  `PYTEST_XDIST_AUTO_NUM_WORKERS` 等线程池变量；第三方工具若使用其他自定义变量，
  需要在 workflow 中自行约束。

---

## 5. 多任务实测（fixbench）

测试素材来自 `<fixbench 任务目录>` 下的
真实任务实例：每个实例包含 `repo.tar.gz`（含 `.git`）、`task.json`
（`baseline_green` / `baseline_failing`）、`workflow.yml` 与 `event.json`。

### 5.1 环境

- Docker 29.7.1（overlayfs），104 核 / 493G 内存。
- runner 镜像：`catthehacker/ubuntu:act-latest`（5.2-5.4 的历史测试；当前部署的镜像
  映射见 4.1）。
- 5.2-5.4 的测试在引入 `--job-cpus` 之前运行，未设置 CPU 预算；当前默认值为 4，
  复现时任务耗时会不同，但上述并发/隔离结论不受影响。
- 守护进程使用 **`go build -race`** 编译的二进制，`GORACE=log_path=... halt_on_error=0`，
  用于捕捉真实并发下的 data race。
- `act serve --max-containers 20`，action cache 共享（默认）；当时 toolcache 是每 run
  一个卷，现已改为每 job 实例独立。

### 5.2 第一波：7 个 run / 18 个 job 实例

| run | 仓库 | commit | job 数 | 期望 | 实际 |
| --- | --- | --- | --- | --- | --- |
| hacs-a | hacs/default | `1ae1646` | 3 | jq/jsonschema 成功，sorted 失败 | 全部符合 |
| hacs-b | hacs/default | `b6cdc5b` | 3 | 同上 | 全部符合 |
| cac | nmhjklnm/cac | `f046319` | 3 | js-check/shellcheck 成功，build-check 失败 | js-check、build-check 符合；shellcheck 失败* |
| egregore | egregore-labs/egregore | `b3a598c` | 2 | oss-validation 成功，worktree-tests 失败 | 全部符合 |
| egregore-dup | 同上（**同一 workdir 同时提交**） | `b3a598c` | 2 | 同上 | 全部符合 |
| lean4 | cameronfreer/lean4-skills | `0640d26` | 2 | ruff 成功，shellcheck 失败 | 全部符合 |
| scijs | scijs/fourier-transform | `289a974` | 3（矩阵 18/20/22） | test 全部失败 | 全部符合 |

- **并发峰值：18 个容器**，即 18 个 job 实例在提交后约 5 秒内全部同时运行；
  没有超过 `--max-containers 20`。
- 两个 hacs 实例是**同一仓库的两个 commit**，同时运行；action cache 共享但没有
  出现 fetch/checkout 错误。
- egregore 与 egregore-dup 是**同一 workdir 的两次并发提交**，两个 run 各自使用
  不同 runID 的容器/卷/日志，互不影响。
- `*`：`cac` 的 shellcheck job 在 `sudo apt-get install -y shellcheck` 时报
  `E: Unable to locate package shellcheck`。这是该测试镜像内 apt 索引不完整导致，
  与调度器无关；任务在原 GitHub runner 上预期成功。其余 17/18 个 job 的结论与
  baseline 一致。
- 三个“全绿 job”的 run（只跑 jq / jsonschema / oss-validation）均为 `success`，
  验证成功路径。

### 5.3 第二波：超过容器上限

同一守护进程（缓存已热）再并发提交 8 个 run，共 **22 个 job 实例**：

- 峰值容器数 **恰好 20**，采样期间从未超过上限（`over_cap_samples=0`）；
- 其余 2 个实例等待槽位，有实例结束后立即补位；
- 全部 run 在 41 秒内结束，结论仍与 baseline 一致（同样的 `cac/shellcheck` 环境差异）。

### 5.4 完整性检查

| 检查项 | 结果 |
| --- | --- |
| race detector | 15 个 run（40 个 job 日志 + 15 个 run 级日志）期间无任何 `DATA RACE` |
| 日志隔离 | 40 个 job 日志文件均只含该 job 自己的前缀（无跨 job 混写） |
| workdir 数据 | 所有任务目录两波测试后 `git status --porcelain` 为空，HEAD 未变 |
| 容器/卷/网络残留 | 所有测试 runID 前缀的容器、卷、网络均已清空 |
| 优雅关闭 | SIGTERM 后 2 秒退出，输出 `Scheduler stopped` |
| 共享 action cache | 同仓库多 commit 并发下无 fetch 冲突/损坏迹象 |
| 结果一致性 | 除上述镜像环境差异外，所有 job 结论与 baseline 一致 |

### 5.5 复现方式（概要）

```bash
# 1) 解压实例
mkdir -p /tmp/act-multi/tasks/hacs-a
tar xzf .../hacs_default_lint_yml__1ae16466dd__d3045cba/repo.tar.gz -C /tmp/act-multi/tasks/hacs-a

# 2) 启动（示例）
export http_proxy=http://<proxy>:<port> https_proxy=http://<proxy>:<port>
./dist/local/act serve --addr 127.0.0.1:8080 --data-dir /tmp/act-multi/data \
  --max-containers 20 --job-cpus 4 --allowed-workdir-roots /tmp/act-multi/tasks \
  --env http_proxy=$http_proxy --env https_proxy=$https_proxy \
  -P ubuntu-latest=catthehacker/ubuntu:act-latest

# 3) 并发提交多个实例（每个实例一个请求，eventJSON 取 event.json）
# 4) 轮询 /v1/stats 与 docker ps，检查峰值容器数、结果、残留
```

> 复现时注意：`workflow.yml` 应与实例目录中的版本一致（tar 内的 workflow 即该
> commit 的版本）；`workdir` 必须是允许根目录下的绝对路径。

### 5.6 当前部署验证（宿主机直跑）

当前线上形态与 5.2-5.4 的临时测试不同：act 直接在宿主机运行、调用宿主 dockerd，
不使用 DinD；启动脚本的 `--max-containers` 默认 10、`--job-cpus 4`，本轮 campaign
通过 `PACT_MAX_CONTAINERS=24` 设为 24，镜像映射见 4.1。用同一个 fixbench spectree
实例（`test` / matrix `python-version=3.12`）验证：

- 运行结果 `success`；
- job 容器镜像 `catthehacker/ubuntu:act-latest`；
- `NanoCPUs=4e9`、`Cpuset=0-3`，容器内 `nproc=4`、`GOMAXPROCS=4`、
  `PYTEST_XDIST_AUTO_NUM_WORKERS=4`；
- 本地 action（checkout-stub、setup-uv）正常命中，代理环境变量在 step 中生效。

当前部署的其余特征：

- act 是宿主机上的二进制进程（不是容器，也没有内层 dockerd）；
- 工作目录根是 `/srv/ci/repos`；
- 通过 `--container-options` 把 `/srv/ci/repos` 只读挂进 job 容器，并设置
  `--container-options-policy auto`；
- 使用 `--container-daemon-socket -`，job 容器内没有宿主 Docker socket；依赖 Docker
  的 workflow 会失败，验证层将其归为环境侧；
- 使用 `--action-offline-mode` + `--local-repository`，action 本身不依赖外网拉取；
  workflow 步骤内部的下载（如 setup-uv 查询版本、pip/npm 安装）仍需要网络，当前
  由 docker0 的宿主机透明代理完成，默认不注入显式 HTTP 代理变量；
- `GITHUB_TOKEN` 通过 0600 的 secret 文件注入，`github.token`/`GH_TOKEN` 可用
  （zizmor 等需要 token 的 action 已能通过）。

该验证与 5.1 的 catthehacker 测试环境不同，结论以各自的镜像/代理配置为准。

---

## 6. 代码索引

| 路径 | 内容 |
| --- | --- |
| `pkg/scheduler/types.go` | `RunRequest` / `RunState` / `JobState`（含二值 `result`）/ `Stats` |
| `pkg/scheduler/scheduler.go` | 调度核心：准入、orchestrator、worker、取消、关闭 |
| `pkg/scheduler/queue.go` | 优先级队列（priority 降序 + FIFO） |
| `pkg/scheduler/semaphore.go` | 全局容器信号量（实现 `runner.JobSlotLimiter`） |
| `pkg/scheduler/engine.go` | `Engine` 抽象与真实 `RunnerEngine` |
| `pkg/scheduler/plan.go` | event/job 选择、失败 job 汇总 |
| `pkg/scheduler/store.go` | 内存状态（读时深拷贝） |
| `pkg/scheduler/isolation.go` | runID、路径校验、数据目录 |
| `pkg/scheduler/logs.go` / `logstream.go` | 每 job 日志、tail/follow |
| `pkg/scheduler/api.go` | HTTP 路由与鉴权 |
| `pkg/runner/runner.go` | `ExpandRun` / `NewJobExecutor` / `JobSlotLimiter` / `JobCPUs` / `JobInstance.Result` / `Config.Clone` |
| `pkg/runner/job_cpus.go` / `job_cpus_linux.go` | CPU 预算、轮转亲和窗口、线程池环境变量 |
| `pkg/runner/run_context.go` | runID 命名、per-job toolcache 卷、容器槽获取、CPU 资源、per-leg 结果 |
| `pkg/runner/job_executor.go` | 无条件 Finally 释放容器槽、per-leg 结果与失败判定 |
| `pkg/runner/logger.go` | `MaskList`、命名 job logger、日志格式化 |
| `cmd/serve.go` / `cmd/submit.go` / `cmd/config.go` | 守护进程、客户端、共享配置构造 |
| `docs/scheduler.md` | 精简版 API/参数参考 |
| `pkg/scheduler/*_test.go` | 单元测试与 Docker 集成测试 |
