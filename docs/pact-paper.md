# PACT：面向 GitHub Actions 工作负载的并行调度与隔离执行系统

**Methodology · Pipeline · Performance Analysis**

本文描述 PACT（Parallel ACT）的系统设计、执行流水线与实测性能。PACT 构建在 nektos/act 之上，
把原本"一次进程执行一份 Plan"的本地 Actions 运行器，扩展为可接收多仓库、多 commit、
多 workflow 请求的常驻调度服务。文中所有实现均对应本仓库代码，所有性能数据来自
fixbench campaign 的实测记录（数据文件与统计口径见 3.1）。

---

## 1 Methodology

### 1.1 系统模型与术语

系统以 **job 实例（job instance）** 为最小调度与执行单元：

| 概念 | 定义 |
| --- | --- |
| Run | 一次验证请求：一个 workdir、一个 event、一组 workflows。 |
| Stage | Plan 中按 `needs` 划分的层；层间严格串行。 |
| JobGroup | 同一个 job 的全部矩阵实例，携带 `max-parallel`。 |
| Job 实例 | 一个 job 与一个矩阵组合；容器模式下，一个可执行实例对应一个 Docker 容器（reusable workflow 的父 job 自身不建容器，由子 job 实例执行；`-self-hosted` host 模式不建容器）。 |
| job key | `<workflow>/<jobId>/<矩阵序号>`，用于日志、API 与资源命名。 |
| 容器槽（slot） | 全局计数信号量，容量为 `--max-containers`。 |
| orchestrator | 每个 Run 一个 goroutine，展开 Stage 并把实例送入全局队列。 |
| worker | 固定数量的 goroutine，从全局队列取实例并执行。 |

与上游 act 的关键区别是：act 的调度单位是"一份 Plan"，PACT 的调度单位是"一个 job 实例"。
矩阵展开、reusable workflow 的子 job 都成为独立可调度对象，因此不同 Run 的实例可以共享
同一个全局 worker pool 和同一组容器槽。

### 1.2 架构与组件

```
HTTP / act submit
      │
      ▼
┌─────────────────────────── act serve ───────────────────────────┐
│ API(api.go) → Scheduler(scheduler.go)                           │
│   ├ Store      内存 Run/Job 状态（读时深拷贝）                  │
│   ├ Engine     请求 → ExecutionPlan（RunnerEngine）             │
│   ├ Queue      全局优先级队列（priority↓, FIFO）                │
│   ├ Semaphore  全局容器槽（容量 = max-containers）              │
│   ├ LogStore   每 job 一个日志文件 + 每 run 一个调度日志        │
│   ├ artifact server（进程级单例，按 GITHUB_RUN_ID 隔离）        │
│   └ cleanup janitor（每 run 清理 + 孤儿扫描 + 启动扫描）        │
│                                                                 │
│ 每 Run：orchestrator + 私有 cache server                        │
│   Stage 1 → Stage 2 → …（串行）                                 │
│     └ ExpandRun → per-group gate → Queue                        │
│                                                                 │
│ worker × (max-containers+8)                                     │
│   └ runner.NewJobExecutor → RunContext.startContainer           │
│        └ Acquire slot → docker create/start → steps → release    │
└────────────────────────────────┬────────────────────────────────┘
                                 │ /var/run/docker.sock
                                 ▼
                   宿主 dockerd：每 job 实例一个容器
```

组件职责与代码位置：

| 组件 | 代码 | 职责 |
| --- | --- | --- |
| API | `pkg/scheduler/api.go` | 路由、Bearer 鉴权、错误码、日志流 |
| Scheduler | `pkg/scheduler/scheduler.go` | 准入、orchestrator、worker、取消、关闭 |
| Queue | `pkg/scheduler/queue.go` | 优先级堆 + FIFO |
| Semaphore | `pkg/scheduler/semaphore.go` | 容器槽，实现 `runner.JobSlotLimiter` |
| Engine | `pkg/scheduler/engine.go` | 把请求转换为 `ExecutionPlan`，调用 `ExpandRun` / `NewJobExecutor` |
| Store | `pkg/scheduler/store.go` | Run/Job 状态、准入计数、裁剪 |
| LogStore | `pkg/scheduler/logs.go`、`logstream.go` | 每 job 日志、offset/follow 流式读取 |
| Cleanup | `pkg/scheduler/cleanup.go` | 遗留容器/卷/网络/镜像回收 |
| Runner 扩展 | `pkg/runner/runner.go`、`job_executor.go`、`run_context.go` | 可单独调用的实例展开与执行链、容器槽、CPU 预算、清理兜底 |

**Engine 抽象**是 PACT 最重要的结构性设计之一：它把"把 workflow 解析成可执行计划"与
"调度和执行"解耦。`Engine.Prepare` 只做解析与 Stage 构造，不执行任何 job；真实实现
`RunnerEngine` 复用上游 Planner 与 Runner，测试可替换为假 Engine 而不需要 Docker。
这一层也是把 act 的单进程 Plan 执行改造成"可调度实例集合"的切入点。

### 1.3 调度算法

PACT 使用**两级并行 + 三层限流**：

- 两级并行：每个 Run 一个 orchestrator（Stage 串行、Stage 内扇出） + 全局固定 worker pool；
- 三层限流：准入（Run 数）、per-group gate（矩阵并发）、全局容器槽（容器数）。

伪代码：

```
Submit(req):
    validate(workdir ∈ allowedRoots, workflowsPath ∈ workdir, secrets, env)
    admit if queued+running runs < max-queued-runs
    run ← create(id, GITHUB_RUN_ID)
    append run.id to <data>/runids.log
    go Run(run)

Run(run):
    cache ← start per-run cache server        # 每 Run 一个，见 1.4
    cfg ← baseConfig.Clone(); cfg.JobSlotLimiter ← globalSemaphore
    plan ← Engine.Prepare(run)
    for stage in plan.Stages:                  # 串行
        groups ← stage.Expand()                # ExpandRun：矩阵展开
        for group in groups:
            gate ← chan(max-parallel)
            for unit in group.Units:           # 扇出
                gate ← token
                queue.Push(Task{unit, priority, seq})
        wait all units of stage
    finishRun(run)                             # 结果 + 调度清理
    scheduleCleanup(run.id, grace)

Worker:
    task ← queue.Pop()                         # priority desc, FIFO
    if task.ctx cancelled: mark cancelled; release gate; continue
    run chain: startContainer → steps → post → complete
        startContainer:
            Acquire(globalSlot)                # 容器创建前
            docker create/start/copy workdir
        finally:
            Release(globalSlot)                # 无条件归还
```

**槽位不变式**：任一瞬间，处于 created/running 状态的 job 容器数 ≤ `max-containers`。
槽在 `startContainer` 创建任何容器或网络**之前**获取，在 job 执行链最外层 `Finally` 归还；
因此矩阵实例、reusable workflow 子 job 都被正确计数。

**无死锁论证**：资源获取顺序固定为「per-group gate → 全局槽」；等待全局槽的 worker 不持有
其他槽；orchestrator 只持有 gate、不持有槽；gate 在实例完成时释放。三者构成有向无环的等待图。

**公平性**：队列按请求 `priority` 降序、同优先级按提交序号 FIFO；不同 Run 的实例在同一队列中
竞争，单个 Run 的矩阵实例受自己的 `max-parallel` gate 限制，不会一次性挤占全部槽位。

### 1.4 隔离与资源模型

PACT 以 Run 为隔离边界，资源命名均带 `runID`：

| 资源 | 隔离方式 |
| --- | --- |
| job 容器 / workdir 卷 / env 卷 / service 网络 | 名称前缀 `act-<runID>-` |
| `GITHUB_RUN_ID` / `GITHUB_RUN_NUMBER` | 进程内递增数字，请求不可覆盖 |
| toolcache | 每 job 实例一个卷 `act-toolcache-<runID>-<hash>`；可切换共享卷 |
| 依赖缓存（`actions/cache`） | 每 Run 一个 cache server 实例 |
| artifact | 进程级单例，按 `GITHUB_RUN_ID` 分目录 + 签名 token |
| 日志 | 每 job 一个文件；每 Run 一个 `_scheduler.log` |
| CPU | 每 job 容器 `NanoCPUs = N×1e9` + 轮转 cpuset 窗口 + 线程池环境变量 |

两个非显然的设计：

1. **每 Run 一个 cache server**：act 的缓存存储没有仓库维度，多个 Run 共享会串仓；PACT 为每个
   Run 启动独立 cache server（随机端口 + URL token），把"跨仓库缓存污染"降级为"缓存不跨 Run 命中"。
2. **每 job 一个 toolcache 卷**：矩阵 leg 并发向同一卷安装同一版本工具会互相覆盖；按 job 实例
   建卷保证正确性。代价是镜像中 `/opt/hostedtoolcache` 的首次 copy-up（v2 镜像约 5.6 GB/卷），
   因此部署提供 `--shared-toolcache` 作为吞吐/磁盘的折中。

### 1.5 失败语义与结果判定

- act 内部会吞掉 step 失败，因此 worker 在执行前后读取 runner 记录的 `Job.Result`，并结合
  执行链返回的 error 判定实例状态：`success` / `failure` / `cancelled`。
- 矩阵有两层结果：`Job.Result` 是组结果（供 `needs` 与 Run 结论使用），`RunContext.LegResult`
  是本 leg 结果；API 与日志按 leg 展示，成功的兄弟 leg 不会被失败 leg 标红。
- Run 在所有 Stage 结束后由 `Plan.Finalize()` 汇总；任一 job 失败 → Run 失败。
- `fail-fast` 不生效（上游 act 行为）；`max-parallel` 生效。
- 取消/超时通过 `common.JobCancelContext` 传播；超时优先级 `timeoutSeconds > --run-timeout > 无限制`。

### 1.6 生命周期、异常与垃圾回收

正常路径由 job 执行链的 `Finally` 清理容器、workdir/env/toolcache 卷、service 网络。PACT 额外
提供三层兜底：

1. **执行链最外层兜底**：`stopContainer`/`closeContainer` 挂在完整 job 执行链之外，`Set up job`
   失败或被取消也会执行；容器清理幂等。
2. **每 Run janitor**：`finishRun` 后 `--cleanup-grace`（默认 45 s）按 runID 前缀删除容器/卷/网络；
   删除失败按 30 s 重试；启动时扫描 `data-dir/runs/` 与持久化的 `runids.log`，覆盖上一次进程
   被 SIGKILL 的情况。
3. **孤儿扫描**：周期扫描名字带合法 runID 且超过 `--cleanup-orphan-age`（默认 1 h）的 act 资源；
   对本调度器已知且已结束的 Run 立即清理，对未知 Run 保留年龄/运行时长保护；同时清理 act 构建的
   docker action 镜像与悬空镜像。

这一设计针对的是一类真实故障：客户端超时导致 Run 在 `docker create` 之后、`docker start` 之前
被取消，若清理只挂在"正常完成"路径上，会遗留 Created 容器与 toolcache 卷。三层兜底使资源回收
不依赖单一执行路径。

### 1.7 安全模型

- workdir 必须是允许根目录下的绝对路径（`EvalSymlinks` 后校验），workflowsPath 必须位于 workdir 内；
- 非 loopback 监听必须配置 `--api-token`，比较使用常量时间；
- secrets 启动时加载，请求只能按名引用，API 不回显，日志掩码；
- 默认禁止 `--bind`（并发 job 共享宿主目录会互相污染），可按需放开；
- 请求体 1 MiB、拒绝未知字段；
- 信任边界明确：`--allowed-workdir-roots` 限制"能执行哪些 checkout"，不限制 workflow 在容器内
  能做什么；因此只应对可信仓库开放。

### 1.8 相对 act 的优势与独特设计

| # | 设计 | 相对 act 的收益 |
| --- | --- | --- |
| 1 | **以 job 实例为调度单元**（`ExpandRun`/`NewJobExecutor`） | act 一次进程处理一份 Plan；PACT 可跨 Run 填充全局容器池 |
| 2 | **全局容器槽 + 两级调度** | 资源使用有上界；不同 Run 的实例可并发，Stage 内矩阵可并行 |
| 3 | **准入 + 优先级队列 + per-group gate** | 可控背压、避免单 Run 饿死其他 Run、矩阵并发可调 |
| 4 | **Run 级命名与缓存隔离** | 多仓库/多 commit 并发不撞名、不串缓存 |
| 5 | **每 job toolcache / CPU 预算** | 矩阵 leg 不互相破坏工具链；大核宿主上单 job 不按 104 核申请资源 |
| 6 | **可观测 API** | Run/Job 状态、日志 offset/follow、容器/队列计数，act CLI 无对应能力 |
| 7 | **清理 janitor + 启动扫描** | 异常取消/崩溃后资源可回收；act 依赖单次进程的正常退出 |
| 8 | **并发正确性修补** | Job 结果加锁、step ID 预置、action fetch/clone keyed mutex、Config.Clone |
| 9 | **保留原 CLI** | `NewPlanExecutor` 复用同一套展开/执行接口，PACT 不破坏 act 的本地用法 |

---

## 2 Pipeline

### 2.1 端到端流水线

```
客户端
  │  POST /v1/runs  (或 act submit)
  ▼
[1] 校验 ─► [2] 准入 ─► [3] 生成 runID / GITHUB_RUN_ID ─► 202
                              │
                              ▼
                    [4] 启动 per-run cache server
                              │
                              ▼
                    [5] Clone 基线配置 + 注入请求参数
                              │
                              ▼
                    [6] Engine.Prepare → ExecutionPlan
                              │
                              ▼
        ┌──────────── [7] Stage 循环（串行） ────────────┐
        │   stage.Expand → JobGroup[]                      │
        │   group gate(max-parallel) → Queue.Push(unit)    │
        │   等待本 stage 全部 unit 完成                    │
        └──────────────────────┬──────────────────────────┘
                               ▼
                    [8] worker 取任务 → 执行链
                               │
                     [9] startContainer：取槽 → 建容器
                               │
                    [10] step 执行 / 日志写入
                               │
                    [11] Finally：停容器/删卷/还槽
                               │
                               ▼
                    [12] 结果判定 + run 汇总 + 清理调度
```

### 2.2 请求接入与准入

`POST /v1/runs` 的请求字段（`pkg/scheduler/types.go`）：

| 字段 | 说明 |
| --- | --- |
| `workdir` | 必填；已 checkout 的绝对路径 |
| `workflowsPath` | 相对 workdir 的目录或文件，默认 `.github/workflows/` |
| `repo` / `commit` | 元数据，同时作为 `GITHUB_REPOSITORY` / `GITHUB_SHA` 默认值 |
| `event` / `eventJSON` | 事件名与 payload（`github.event`） |
| `job` / `matrix` | 只跑指定 job / 只保留指定矩阵组合 |
| `env` / `vars` / `inputs` | 追加/覆盖环境变量、vars、inputs |
| `secretRefs` | 需要的 secret 名字；为空表示使用服务端全部 secrets |
| `timeoutSeconds` / `priority` | 超时与队列优先级 |
| `noRecurse` / `strict` | 与 act 同名语义 |

校验失败返回 400；准入满返回 503；成功后返回 202 与 Run 状态，执行异步进行。

### 2.3 配置构造与计划生成

`buildRunConfig` 从基线的 `runner.Config` 克隆一份，注入：

- workdir、事件、secrets、`RunID`、`JobSlotLimiter`；
- `ToolCacheVolume` 前缀（每 Run 唯一，job 实例再追加哈希）；
- `GITHUB_RUN_ID`、`ACTIONS_CACHE_URL`、`GITHUB_REPOSITORY`、`GITHUB_SHA`；
- 请求的 env/vars/inputs 与 matrix 过滤；
- artifact server 的地址、存储目录、签名密钥。

`Engine.Prepare` 调用上游 Planner 生成 `model.Plan`，包装为 `ExecutionPlan`：每个 Stage 持有
一个惰性的 `Expand` 闭包，只有在轮到该 Stage 时才调用 `ExpandRun` 展开矩阵。这样大 workflow 的
矩阵展开成本被摊到执行时，且 Stage 之间的 `needs` 语义完全保留。

### 2.4 Stage 编排与矩阵展开

对每个 Stage：

1. `ExpandRun` 展开所有 `model.Run`，生成 `JobInstance`（每矩阵组合一个）并预置 step ID；
2. 每个 job 的实例组成 `JobGroup`，携带 `max-parallel`（默认 4，且不超过 leg 数）；
3. orchestrator 为每个 group 建一个 gate（带缓冲 channel）；取到 gate 后把 Task 放入全局队列；
4. 等待本 Stage 所有 Task 完成，再进入下一 Stage；任一实例失败不会中断其他同 Stage 实例。

Task 携带 `RunID`、`JobUnit`、`Config`、`Ctx`、`Priority`、`Seq`，以及状态回调 `start`/`finish`。
`start` 把 job 标记为 running；`finish` 释放 gate、更新 Store、唤醒 orchestrator。

### 2.5 队列、门控与 worker 执行

- 队列为 `container/heap`：`priority` 大者先出，同优先级按 `Seq`（提交序号）FIFO；
- worker 数默认 `max-containers + 8`；等槽的 worker 阻塞在信号量上，不消耗 CPU；
- worker 出队后先检查 Run ctx，已取消直接标记 cancelled（不建容器）；
- 每个实例的执行链由 `runner.NewJobExecutor` 生成：
  `Set up job → pre steps → steps → post steps → Complete job`，外层无条件执行
  `setJobResult → stopContainer → closeContainer → releaseJobSlot`。

### 2.6 容器生命周期

1. `startContainer` 首先 `Acquire` 全局槽（若 ctx 已取消则失败，不创建容器）；
2. 创建 job 容器（`tail -f /dev/null`），应用 CPU 配额/亲和窗口/线程池环境变量；
3. 若有 `services`，创建专用网络与 service 容器，等待健康；
4. 把 workdir 复制进 job 的 workdir 卷（默认非 bind）；
5. step 通过 `docker exec` 在容器内执行，stdout/stderr 归入该 job 的日志文件；
6. `Finally` 中删除容器、env 卷、toolcache 卷、service 容器与网络，归还槽；
7. 若正常路径失败或被取消，scheduler janitor 在 grace 后按 runID 前缀做兜底清理。

### 2.7 结果、日志与可观测性

- 每个 job 实例一个日志文件：`<data>/runs/<runID>/logs/<key>.log`；`/` 被替换为 `-`；
- 每 Run 一个 `_scheduler.log`，记录 run 开始/结束、Stage 等待、清理事件；
- API 支持 `GET /v1/runs/{id}/logs?job=KEY&offset=N&follow=1` 流式读取；
- `GET /v1/stats` 返回队列长度、jobs running/queued、`containersUsed/containersMax` 等；
- `GET /healthz` 在 draining 时返回 503，便于负载均衡摘除。

### 2.8 关闭与异常回收

收到 SIGINT/SIGTERM：

1. 进入 draining：`/healthz` 返回 503，新提交返回 503；
2. 取消所有 Run ctx，worker 完成或中止当前实例；
3. 关闭队列与日志；等待 worker；
4. 若 worker 超时未退出，按 runID 前缀强制清理容器/卷/网络；
5. 停止 cleanup janitor。

异常（SIGKILL、磁盘写满、Docker 卡死）后：重启时扫描 `data/runs/` 与 `runids.log`，把历史
runID 排入清理队列；孤儿扫描按年龄清理未知资源。该机制不依赖进程正常退出。

### 2.9 调用示例

```bash
# 启动
act serve --addr 127.0.0.1:8080 --data-dir /var/lib/act-serve \
  --max-containers 24 --job-cpus 4 --allowed-workdir-roots /srv/ci \
  -P ubuntu-latest=catthehacker/ubuntu:act-latest

# 提交并等待
act submit --addr http://127.0.0.1:8080 \
  --workdir /srv/ci/repo --repo org/repo --commit <sha> \
  --event pull_request --event-file event.json --wait

# 直接 HTTP
curl -sS -X POST http://127.0.0.1:8080/v1/runs \
  -H 'Content-Type: application/json' \
  -d '{"workdir":"/srv/ci/repo","event":"push","priority":0}'
```

---

## 3 Performance Analysis

### 3.1 实验环境与统计口径

- 宿主：104 vCPU、493 GB 内存；Docker 29.7（overlayfs）；`/srv` 存储。
- 调度器：`--max-containers 24`、`--job-cpus 4`、bridge 网络、`--container-daemon-socket -`；
  runner 镜像 `catthehacker/ubuntu:act-latest`（预置 Go/Python/Node/Java 多版本 toolcache）。
- 数据：fixbench campaign 的 5597 个已完成任务（inventory 5706，109 条 error）；每任务时间取
  配套实验仓库导出的 `state.jsonl` 中 `act_sec`（run 从 `createdAt` 到 `finishedAt` 的墙钟，
  见 3.6 口径说明）。实验代码与原始数据不在本仓库发布。
- 吞吐统计只采用未被中断的轮次（round 1/3/6/7）；round 2 有多次 rerun、round 4 曾中断重跑、
  round 8 受磁盘写满与 serve 重启影响，均不计入吞吐对比，单独说明。

### 3.2 吞吐量与容器利用率

定义：

- 吞吐 `X = N_tasks / T_wall`（任务/小时）；
- 容器利用率 `U = Σ act_sec / (T_wall × C)`，`C = 24`，衡量槽位被占用的比例（`act_sec`
  含取槽等待，故 `U` 是容器占用上界）。

| 轮次 | 任务数 | 墙钟 (h) | 吞吐 (任务/h) | act 总时长 (h) | 利用率 U |
| --- | --- | --- | --- | --- | --- |
| 1（小批量） | 53 | 0.59 | 90.1 | 4.3 | 30.5% |
| 3 | 458 | 3.78 | 121.3 | 71.8 | 79.2% |
| 6 | 133 | 1.15 | 115.7 | 21.7 | 78.7% |
| **7** | **1000** | **6.76** | **147.9** | **155.1** | **95.6%** |
| 8（受中断，参考） | 1753 | — | — | 285.7 | — |

- 满载轮次（round 7）达到 **148 任务/小时**，相当于每个容器每小时完成约 **6.2 个任务**；
- 利用率 95.6% 说明队列能在任务完成时立即补位，槽位空闲极少；
- 小批量（round 1）因冷启动与镜像/action 缓存未热，利用率仅 30.5%，属于 ramp-up。

### 3.3 延迟分布

**任务级 `act_sec`（n=5599）**：

| 分位 | 值 |
| --- | --- |
| p50 | 525 s（8.8 min） |
| p90 | 1746 s（29.1 min） |
| p95 | 2186 s（36.4 min） |
| p99 | 3154 s（52.6 min） |
| max | 5440 s（90.7 min） |

**round 7（n=1000）**：任务 p50 466 s、p90 945 s、p95 1146 s、p99 1809 s、max 2885 s；
job 实例 3510 个，p50 200 s、p90 331 s、p95 396 s、p99 810 s、max 2732 s。

分桶（全部 5599 条）：5–10 分钟 40.6%，10–20 分钟 25.3%，20–30 分钟 7.6%，>30 分钟 8.7%
（>60 分钟 0.6%）。长尾是吞吐的主要不确定性来源。

### 3.4 并发上限验证

早前的多任务实测（README.scheduler.md 第 5 章）：

- 7 个 Run / 18 个 job 实例并发：峰值 **18 个容器**，未超过上限；
- 8 个 Run / 22 个实例、上限 20：峰值 **恰好 20**，其余实例等槽，有实例结束立即补位；
- 15 个 Run / 40 个 job 日志：race detector 无 `DATA RACE`；日志无跨 job 混写；
- 同 workdir 的两次并发提交（不同 runID）互不影响，workdir 未被污染。

本部署在 round 7/8 中持续保持 `containersUsed = containersMax = 24`，与 `docker ps` 的
job 容器数一致；清理 janitor 修复后，运行结束容器数回落到 0。

### 3.5 与原生 act 的对比

原生 act 一次进程处理一份 Plan：Stage 串行，单个 Run 内的并发受
`GetConcurrentJobs()` 限制（本部署 `--job-cpus 4` 时默认并发为 4）；多个仓库/commit 需要多个
act 进程，进程之间没有统一资源上限，也没有全局队列与优先级。

PACT 的差异：

1. **跨 Run 共享容器池**：1000 个独立任务在 24 个槽位上持续填充，round 7 实测 6.76 h；
   若以单 act 进程的 4 路并发执行同样的任务（平均 558 s/任务），串行需要约
   `1000 × 558 / 4 / 3600 ≈ 38.8 h`，即 PACT 的 **~5.7×** 加速；理论上限为 24/4 = 6×，
   实测利用率 95.6% 解释了与上限的差距。
2. **资源上界与公平性**：原生多进程方案没有全局容器上限；PACT 用信号量把所有 Run 的
   job 容器限制在 24 个以内，并用优先级队列控制竞争。
3. **矩阵与 reusable workflow 计数**：PACT 在容器创建点限流，矩阵 leg 与子 job 都占槽；
   原生 act 的并发参数不覆盖这些边界。
4. **异常回收**：原生 act 依赖单次进程正常退出清理；PACT 的 janitor 在取消/崩溃/重启后仍能
   回收资源。
5. **可观测性**：PACT 提供 Run/Job 状态、日志流、队列与容器计数；原生 act 只有进程输出。

### 3.6 开销与瓶颈

- **toolcache copy-up**：v2 镜像的 `/opt/hostedtoolcache` 约 5.6 GB，每个 job 的独立卷首次挂载
  需要复制一次；24 并发时产生百余 GB 的临时占用与 I/O，是本轮磁盘压力的主要来源。部署通过
  `--shared-toolcache` 将复制降为一次，代价是并发安装同一工具的隔离性下降。
- **每 Run 一个 cache server**：并发 Run 数增加时端口与进程数线性增长（随机端口），对吞吐影响
  较小，但增加了运维复杂度。
- **Docker 守护进程**：容器创建/删除、卷删除（尤其是大卷）是主要串行瓶颈；实测中大量
  `context deadline exceeded` 与清理延迟都来自 Docker 侧。
- **长尾任务**：>30 分钟的任务占 8.7%，会长期占用槽位；`--job-cpus` 与超时预算用于限制单任务
  对整机的影响。
- **调度器开销**：Store/Queue 为内存结构，准入与入队为 O(log n)；相对容器启动（秒级）可忽略。
  实测任务 `elapsed_sec` 与 `act_sec` 的差（驱动侧 staging/评测）约数十秒量级。

### 3.7 失败、环境噪声与鲁棒性

- 失败判定：Run 级失败率（campaign）round 7 为 98.3% 的 run 以 failure 结束（这是 baseline
  失败任务，符合预期）；round 7 有效复现率 69.0%，round 8 因任务构成与运维事故为 47.5%。
- 环境失败率：round 7 27.3%、round 8 50.6%（round 8 纳入了 282 个归档环境任务，且受磁盘写满、
  serve 重启、容器泄漏影响；其 `docker_infrastructure` 类从 8 增至 237，不能与 round 7 直接比较）。
- 长任务：round 7 有 11 个任务 >30 分钟（1.1%）；全部历史 495/5597（8.8%）。
- 鲁棒性：race detector 下 15 个 Run/40 个 job 无数据竞争；清理 janitor 上线后，运行结束
  容器数回落到 0；SIGTERM 优雅关闭，SIGKILL 后重启可扫描历史 runID 回收资源。
- 已知缺陷：早期清理实现曾在 `create→start` 的取消窗口漏删容器，导致容器数超过槽位上限；
  修复后 `docker ps` 与 `containersUsed` 一致（24/24）。

### 3.8 局限性

1. 状态只在内存：重启丢失排队/运行中的 Run；崩溃可能留下无状态记录的资源（janitor 兜底）。
2. `act_sec` 含容器槽等待，严格"不含排队"的历史精确时间无法还原（`queued_sec` 未实现）。
3. 没有日志滚动与保留策略；`data/runs` 与 artifact 持续增长，需要外部清理。
4. `fail-fast` 不生效；同一 Run 的 Stage 严格串行。
5. 原生 act 的 `--bind`、宿主 Docker socket 等能力会破坏隔离，PACT 默认禁用或限制。
6. toolcache copy-up 与 Docker 串行删除是大规模吞吐的主要物理瓶颈；`--shared-toolcache`
   是折中而非消除。
7. 失败分类（env/business）依赖外部标注器，其与人工判断的一致性有限（详见分类器评估文档），
   不应直接等同于真实原因。

### 3.9 可复现性

- 调度器与扩展代码：`pkg/scheduler/`、`pkg/runner/{runner.go,job_executor.go,run_context.go}`、
  `cmd/serve.go`、`cmd/submit.go`；
- 吞吐数据：由配套实验仓库的 `state.jsonl`、`rounds/round-N.json`、`batches/` 生成；
- 分类与标注：配套实验仓库的 `log_classifier.py`、`annotation_stats.py`；
- 实测实验记录：`README.scheduler.md` 第 5 章（并发 18/20 峰值、race detector、日志隔离）。
