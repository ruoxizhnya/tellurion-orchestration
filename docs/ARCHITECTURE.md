---
status: active
last-verified: 2026-10-22
verified-by: M8 接口对表（QT-44 前置 ②）
---

# 架构 — tellurion-orchestration

## 1. 定位

编排 / worker 服务：job / batch / walkforward 的**算力出口**，schema `run`。

它存在的理由是"批量扫"这件事与"单次回测"的部署节奏完全不同：前者要横向扩、要排队、要被抢占；后者要低延迟、要确定性。

## 2. 分层

```
提交面（HTTP，异步）
  POST jobs / batch / walkforward  ── 幂等键 → job_id
        ↓
   run.backtest_jobs / batch_runs / walk_forward_reports / run_artifacts / worker_registry
        ↓
   worker 池（领取 / 心跳 / drain）
        ↓
   core 引擎（进程内执行）
        ↓
   结果 + 三 hash ──► /provenance
```

## 3. 组件职责

| 组件 | 职责 | 不做 |
|---|---|---|
| 队列 | 状态机、幂等、优先级 | 不执行 |
| 批次 | 参数网格展开、汇总排序 | 不解释单次结果 |
| 前向验证 | 窗口切分、逐窗调度 | 不算指标 |
| worker 管理 | 注册 / 心跳 / drain | 不做负载均衡算法之外的事 |
| 三 hash 留痕 | 结果坐标 | 不复制内容 |

## 4. 三份契约（功能的边界就在这三份文件里）

| 维度 | 文件 | 回答什么问题 |
|---|---|---|
| 数据形状 | [`db-schema.md`](db-schema.md) | 本仓拥有哪些表？谁能写？ |
| 接口形状 | [`api-schema.md`](api-schema.md) | 对外暴露什么？错误码与幂等怎么做？ |
| 流向与顺序 | [`dataflow.md`](dataflow.md) | 数据怎么流？哪里 fail-closed？ |

## 5. 依赖方向

```
contracts（RunSpec / ResultSpec / JobRecord）+ kit（logging/http/db/id）
        ↑
tellurion-orchestration ──► core 引擎（派活）
        └──► run.*（唯一 owner）
```

## 6. 关键决策

- [`adr/0001-只派活不裁决.md`](adr/0001-只派活不裁决.md)

## 7. 非目标（明确不做）

- 不做执行、不做分析、不做风控
- 不做数据准备
- 不做调度 UI
- 不做对象存储（artifact 本体归 strategy 仓）

## 8. 分期与状态

| 阶段 | 内容 | 状态 |
|---|---|---|
| X5a | job 队列 + 状态机 + 幂等 | ⬜ |
| X5b | batch + walkforward | ⬜ |
| X5c | worker 注册 / drain | ⬜ |
| X0 | 三 hash 留痕（`run_artifacts`） | ⬜ |

## 9. 接口对表（M8 前置 · ✅ 2026-10-22）

> **本节是什么**：`tellurion:docs/TASKS.md`「五、X 线」公共前置 ② 要求的**接口对表本体**（模板 = `tellurion:docs/design/qt-decoupling/data-service-contract-and-ownership.md` §4.1）。逐个 port / DTO / endpoint 列 **签名 + 错误语义 + 版本策略**；缺一列就是没对完表。
> **实测基线**：core `dbe4c68`（2026-10-22；QT-63 已落）。行号一律取 `git show HEAD:<path>`。复核：`git -C ../tellurion show HEAD:<path> | sed -n '<line>p'`。
> **列口径**：「今天」= 源码实测；「契约位」= S1（QT-05）后 contracts 的落点，写 `core-only` 表示今天**不满足** contracts ADR-0001 ① 的「≥2 个仓使用」判据（不是永久留 core）；「版本策略」按 QT-19 的 MAJOR / MINOR / PATCH 判定。
> **M5 口径**：六仓 ≠ 六进程（`tellurion:docs/design/qt-decoupling/repo-inventory.md` §1.2）。本仓今天不升格容器；**X5a 只落「队列表 + port + worker 领取」也不需要容器**（可以先是 core 进程里的一个 worker 装配），C 组 HTTP 面是升格时的目标形状。
> **H5 口径**：`strategy_state` **不属本表**（归 strategy-runtime 模块，core 写 `quant.strategy_state`）。本节不出现 `RunStateStore`，它不是遗漏，是 2026-10-21 裁决作废（见 §9.7 R4）。

### 9.1 八条前置事实（每条都会改变施工判断）

| # | 事实 | 取证（core `dbe4c68`） | 对施工的后果 |
|---|---|---|---|
| F1 | **今天的 `JobStore` 不是队列，是 `map[string]any` 的持久化面**：没有 worker 领取、租约、优先级、重试；`CreateJob` 落一行 `pending` 后**立刻在进程内起 goroutine**，并发上限为零；取消函数在 `sync.Map` 里。 | 接口 7 方法全是 `map[string]any`（`pkg/backtest/job/job.go:56`，首个方法 `pkg/backtest/job/job.go:57`）；`CreateJob` 建行后直接 `StartJob`（`pkg/backtest/job/job.go:292`）；`cancelFuncs sync.Map`（`pkg/backtest/job/job.go:37`）；`CancelJob` 对 `running` 只调内存里的 cancel（`pkg/backtest/job/job.go:566`）；关停时 `Shutdown` 取消在途、`CleanupStaleRunning` 把 `running` 刷成 `failed`（`pkg/backtest/job/job.go:613`、`pkg/backtest/job/job.go:667`）；关停路径（`pkg/bootstrap/server.go:267`）与**启动路径**（`cmd/analysis/setup.go:244`）都会调用它——启动时把任何 `running` 行刷成 `failed`，这在单进程成立，多 worker 下就是互相误杀。 | 目标 `JobQueue` 必须是**数据库状态机 + 原子领取**（`FOR UPDATE SKIP LOCKED` 或等价），取消走 `cancel_requested` 列而不是进程内 map——否则多 worker 下「取消别人的 job」永远做不到，重启后只能把在跑的 run 判失败。**不是给旧接口加两个方法**，是换实现。 |
| F2 | **异步 job 的请求面表达不了完整 RunSpec**：`CreateJobRequest` 没有 `fill_timing` / `seed` / `signals` / `risk_free_rate`（`StartJob` 重建请求时也不设 `RiskFreeRate`，异步 job 只能用 worker 引擎配置的默认值——零值再回落 3%，`pkg/backtest/engine.go:516-518`、`pkg/backtest/engine.go:282-283`）；`commission_rate` / `slippage_rate` 落进 `params JSONB` 后**没有执行期读取点**，调用方 `params` 里的策略参数也只被原样保存（执行期只读 `initial_capital` / `kernel` / `engine_params` 三个保留键）；`StartJob` 重建 `BacktestRequest` 时也不设 `FillTiming`。 | `CreateJobRequest`（`pkg/backtest/job/job.go:181`），`CommissionRate` / `SlippageRate`（`pkg/backtest/job/job.go:188-189`）；写出 `params["initial_capital"]` / `["commission_rate"]` / `["slippage_rate"]`（`pkg/backtest/job/job.go:233-239`）；执行期只读 `initial_capital` / `kernel` / `engine_params`，重建请求（`pkg/backtest/job/job.go:425`）——**没有 FillTiming**；`BacktestRequest` 的 `FillTiming` 在 `pkg/backtest/contracts/contracts.go:195`，默认解析在 `pkg/backtest/contracts/filltiming.go:148`；`seed` 是**进程级**引擎配置（`pkg/backtest/engine.go:35`），provenance 记的是 `e.config.Seed`（`pkg/backtest/provenance.go:199`）；策略参数只有注册表默认值，没有 run 级覆写（`pkg/backtest/provenance.go:226`，QT-21 待落）。 | 目标 job 行必须存**canonical `request_json`**（整份 `BacktestRequest`），worker 只能执行这一份、不得从扁平列重新拼；**今天收下但静默丢弃的旋钮（commission/slippage/params）必须在入队前拒绝或真正生效**，二选一，不许保持现状。`seed` 在 RunSpec 冻结面里没有位置（R3）：interim 由**编排配置固定 worker 的 `Config.Seed`、同部署所有 worker 同值**，并写进 run 身份；per-run seed 要动冻结面，另走评审。 |
| F3 | **没有幂等键、没有取消端点、没有状态过滤；`DELETE` 语义是删行**。 | `CreateJob` 每次 `uuid.New()`（`pkg/backtest/job/job.go:226`）；`GET /api/backtest` 只接 `limit`（`cmd/analysis/handlers_backtest.go:44-55`）；`POST /api/backtest` 把「异步 job」与「同步整测」塞进同一个端点、按 body 形状分流（`cmd/analysis/handlers_backtest.go:57`）；`CancelJob` 对 `pending` 直接删行（`pkg/backtest/job/job.go:566`），存储侧 SQL 也只删 `pending`（`pkg/storage/backtest_jobs.go:161`）；HTTP 层 `grep 'DELETE'` **零命中**（§9.7 的 checker 钉住）。旧批量/前向入口是 `/api/batch`（`cmd/analysis/handlers_batch.go:27-33`）与 `/api/walkforward`（`cmd/analysis/handlers_walkforward.go:12-18`）。 | 目标 `POST /api/runs/jobs` **必须带幂等键**：同键同请求 → 原 `job_id`（200）；同键不同请求 → 409。`DELETE` 的语义改为**取消并保留行**（`cancelled`），不再物理删——删除会同时删掉审计与幂等键。旧路径与新路径并存期是**双真相**（R1），必须有切换计划，不能长期两边都活。 |
| F4 | **provenance 已落 `quant.run_provenance`，但 `run_artifacts` 与 `/api/runs/{id}/provenance` 仍是零实现；且 provenance 表按裁决不存 `status`**。 | 表与写入路径：`pkg/storage/run_provenance.go:33`（记录）/`:56`（DDL）/`:86`（写）/`:126`（读），Migration 056 收编（`pkg/storage/postgres.go:1189-1193`）；run 开始、日循环之前写一次，写不进去 = run `failed`（`pkg/backtest/engine.go:875-878`）；`risk_rules_hash` 恒为空串（`pkg/backtest/provenance.go:194`）。`run_artifacts` **没有建表 / 写入实现**（唯一命中是 QT-63 的迁移债注释 `pkg/storage/run_provenance.go:19`）；`/api/runs` 在 `cmd`/`pkg` **零命中**。K9 冻结文本见 `docs/SPEC.md:3179`。 | `/provenance` 的返回面必须 **`run_artifacts`（输入身份）⋈ `backtest_jobs`（status / result_status）**——provenance 表本身永远不存 status（QT-63 ④）。目标 endpoint 不改 `BacktestResponse` 的 27 项冻结字段集，用一个**独立的响应 DTO**；把三 hash 塞进 `BacktestResponse` 才是冻结面变更（QT-03 ③-⑥ 的评审）。O1 的 `run_artifacts` 是 `quant.run_provenance` 的**取代者**（双写/对拍/切读/停写，QT-63 ③-①），不是并列的第二份。 |
| F5 | **batch 报告只在内存；且把 K9 `invalid` 当成功**。 | `reports map[string]*BatchReport`（`pkg/backtest/batch/batch.go:110`），`GetReport` 查内存（`pkg/backtest/batch/batch.go:144`）；`BatchEngine` 持有 `*walkforward.WalkForwardEngine` 具体类型（`pkg/backtest/batch/batch.go:102`、`:114`）；`BatchResult.Status` 只有 `completed`/`failed`：只要 `err == nil` 就写 `completed`（`pkg/backtest/batch/batch.go:249`），完全不看 `resp.Status`；`toBacktestResult` 把 `BacktestResponse` 拍成 `domain.BacktestResult`，**status 字段在转换中丢失**（`pkg/backtest/batch/batch.go:412-446`）；Scorer 只对 `Status=="completed"` 排名。 | batch 的每个 task 必须带 **K9 `result_status`**；`invalid` 不得进排名（否则「无效结果拿第一」）。报告必须落 `run.batch_runs`（今天重启即丢），且本仓只存汇总与指针、不存每个结果的内容。`BatchEngine` 对 walkforward 的依赖要改成 port（或两者同属本仓后不再跨包）。 |
| F6 | **walkforward 直连 `*storage.PostgresStore`，请求缺参，且逐窗失败静默消失、`invalid` 当成功、落库失败只 Warn**。 | 构造函数收具体存储（`pkg/backtest/walkforward/walkforward.go:37`）；请求只有策略名/票池/区间/资金/无风险利率 + 窗口参数（`pkg/backtest/walkforward/walkforward.go:56`）；交易日直接查 store（`pkg/backtest/walkforward/walkforward.go:110`）；每个窗口重建的请求**不带 params / kernel / fill_timing / seed**（`pkg/backtest/walkforward/walkforward.go:223`、`:232`）；只判 `err != nil`，`invalid` 响应被当有效结果参与聚合（`pkg/backtest/walkforward/walkforward.go:247-253`）；单窗失败 `return nil` 后被过滤掉、不留失败记录（`pkg/backtest/walkforward/walkforward.go:180-187`）；报告落库失败只 `Warn`、仍返回成功（`pkg/backtest/walkforward/walkforward.go:152`）；`WalkForwardParams` 没有 json tag（`pkg/domain/backtest.go:92`）。 | walkforward 必须走 `TradingCalendar` 只读 port（不直连别人 schema）；请求必须携带完整 base RunSpec；每个窗口的 train/test 都是**独立 run**（各自三 hash），窗口级 `result_status` 逐一列出；**部分窗口失败 ≠ 整体静默成功**，报告要显式计数；落库失败对「要留痕」的 run 是 fail-closed，不是 Warn。 |
| F7 | **没有 worker 注册 / 心跳 / drain；`run.worker_registry` 零实现**。 | `pkg/backtest/workers/workers.go:31` 的 `RunWorkers` 只是**请求内并发池**（泛型、纯标准库），不是 worker 注册表；`sync_jobs.worker_id`（`pkg/storage/postgres.go:883`、`:900`）是**数据面同步任务**的字段，不是回测 worker（同名不同物，别复用）；`run.worker_registry` 在 `*.go` 零命中。 | X5c 要新建注册表（worker_id / 能力 / 心跳 / active → draining → offline），`drain` 是**状态迁移 + 停止领取**，不是删进程。心跳超时的 `running` 行不能靠「启动时扫全表」兜底（多 worker 会互相误杀），要靠**租约过期**回收。 |
| F8 | **「三 hash 必留」不是入队校验，而是终态不变量；今天的 job 表还会把 `invalid` 写成 `failed`、并把结果体丢掉。** | 三 hash 在 run **开始时**才由生效值算出（`pkg/backtest/provenance.go:162`），其中 `dataset_snapshot_hash` 的 `DataVersion` 为空（`pkg/backtest/provenance.go:155`）、`risk_rules_hash` 为空串（`pkg/backtest/provenance.go:194`）——提交时**不可能**知道真值；`invalid` 走 `UpdateJobFailed`（`pkg/backtest/job/job.go:493`），存储写 `status='failed'`（`pkg/storage/backtest_jobs.go:48`），结果体不落（只有 `error_message`）。agent 侧已按「job 行 status 是生命周期、结果 status 才是结论」编码（`tellurion-agent:pkg/ai/client/backtest_client.go:222`、`tellurion-agent:pkg/ai/client/backtest_client_test.go:377`）。 | 入队只能校验**三份引用可解析**（strategy_ref 语法、dataset hash 形状、risk 规则快照版本可解析）；真值在终态校验：`result_status in (completed, invalid)` ⇒ `run_artifacts` 必须有行，否则 job 是 `failed`（数据完整性破坏，不是 normal path）。目标 job 行把 `invalid` 表示为 **`status=completed` + `result_status=invalid`**，不再复用 `failed`。 |

### 9.2 A 组 · port 与执行器接口

**A-1 目标 `JobQueue`（contracts，先落 orchestration 内部的同名接口，S1 迁）**

```go
// RunRecord 的字段见 §9.3；这里只钉方法集与错误语义。
type JobQueue interface {
    Enqueue(ctx context.Context, rec *RunRecord) (job *RunRecord, created bool, err error)
    Claim(ctx context.Context, workerID string, lease time.Duration) (*RunRecord, error)
    RenewLease(ctx context.Context, runID, workerID string, lease time.Duration) error
    Complete(ctx context.Context, runID, workerID, resultStatus, resultURI string) error
    Fail(ctx context.Context, runID, workerID, errMsg, resultStatus string) error
    RequestCancel(ctx context.Context, runID string) error
    AckCancel(ctx context.Context, runID, workerID string) error
    Get(ctx context.Context, runID string) (*RunRecord, error)
    List(ctx context.Context, filter ListFilter) ([]*RunRecord, error)
    ReapExpired(ctx context.Context, now time.Time) (int, error)
}

type WorkerRegistry interface {
    Register(ctx context.Context, w WorkerRecord) error
    Heartbeat(ctx context.Context, workerID string, at time.Time) error
    Drain(ctx context.Context, workerID string) error
    List(ctx context.Context) ([]WorkerRecord, error)
}
```

| 方法 | 今天 | 契约位 | 错误语义（本表冻结） | 版本策略 |
|---|---|---|---|---|
| `Enqueue` | 无（`CreateJob` 直接插行并起 goroutine） | contracts | 幂等键必填；同键同 `request_hash` → 返回原行、`created=false`、`err=nil`；同键不同 hash → `ErrIdempotencyConflict`；键为空 → `ErrInvalidInput`；队列深度达上限 → `ErrQueueFull`（HTTP 429）。**不执行、不路由**（market→kernel 已在调用方解析完） | 新增 port = MINOR；方法签名改 = MAJOR |
| `Claim` | 无 | contracts | 原子领取一条 `pending` 且 `lease_until IS NULL OR < now()` 的行，写 `running` + `worker_id` + `lease_until`；无活 → `(nil, nil)`；draining worker → `ErrWorkerDraining`；`lease <= 0` → `ErrInvalidInput`。并发安全 = `SKIP LOCKED` 或等价 | 同上 |
| `RenewLease` | 无 | contracts | 只允许当前 owner 在租约有效期内续；行不存在 / 已终态 / 别人持有 / 租约已过期 → `ErrLeaseLost`（worker 必须**立即停止**，不得再写结果） | 同上 |
| `Complete` | `UpdateJobCompleted`（无 owner 校验） | contracts | 必须持有效租约；写 `status=completed` + `result_status` + `result_uri`；`result_status` ∈ `{completed, invalid}`（**`failed` 走 `Fail`**）；同一终态同载荷重复调用 → `nil`（幂等）；不同载荷 / 已 `failed` / 已 `cancelled` → `ErrInvalidTransition`；租约过期 → `ErrLeaseLost` | 终态语义变更 = MAJOR |
| `Fail` | `UpdateJobFailed`（把 invalid 也塞进来） | contracts | 必须持有效租约；写 `status=failed` + `error_message`；`resultStatus` ∈ {`""`, `failed`}——确实产出过 ResultSpec 传 `failed`，否则传空串（列写 NULL）；其他值 → `ErrInvalidInput`；重复调用幂等 | 同上 |
| `RequestCancel` | `CancelJob`：pending 删行、running 调内存 cancel | contracts | 未知 id → `ErrJobNotFound`；`pending` → 直接 `cancelled`；`running` → `cancelling`（不动跑着的 worker）；终态 → `nil`（幂等）；重复 → `nil` | 语义变更 = MAJOR（删除→保留） |
| `AckCancel` | 无 | contracts | 仅 owner 在 `cancelling`/`running` 上确认；写 `cancelled`；非 owner / 租约过期 → `ErrLeaseLost`；已终态 → `ErrInvalidTransition` | 同上 |
| `Get` / `List` | `GetBacktestJob` / `ListBacktestJobs`（map） | contracts | `Get` 未知 → `ErrJobNotFound`；`List` 的 `limit` 上限由实现钳制（建议 100），`status` / `result_status` 过滤是**精确匹配**，未知值 → `ErrInvalidInput`（不猜） | 新增过滤字段 = MINOR |
| `ReapExpired` | `CleanupStaleRunning`（只扫 `running`，不区分租约） | contracts | 租约过期 → `pending` 且 `attempts+1`；超过 `max_attempts` → `failed` + `attempts_exhausted`；必须可并发/重入；返回本次迁移行数 | 新增 = MINOR |
| `Register` | 无 | contracts | 同 `worker_id` 重复注册 = 覆盖能力/心跳（幂等），不允许两个进程共用同一 id（由装配方保证）；空 id → `ErrInvalidInput` | 新增 = MINOR |
| `Heartbeat` | 无 | contracts | 未知 worker → `ErrWorkerNotFound`（**不自动注册**，fail-loud）；draining → 只更新心跳、返回 `nil` | 同上 |
| `Drain` | 无（只有进程级 `Shutdown`） | contracts | 未知 worker → `ErrWorkerNotFound`；`active → draining`；重复 drain → `nil`（幂等）；`offline` → `ErrInvalidTransition` | 同上 |
| `List`（worker） | 无 | contracts | 空列表 = `[]`，不是 `nil`（JSON 稳定） | 同上 |
| `EngineRunner.RunBacktest` | `pkg/backtest/contracts/contracts.go:404` | contracts（frozen） | 返回值 `Status` 是唯一判据；`error` 只装真实故障；未注册内核 `ErrKernelNotRegistered`，**零静默回退**（`tellurion-contracts:docs/API.md` §5.2） | **不改**（QT-44 ④ 明文） |
| `EngineRegistry.Resolve` | `pkg/backtest/contracts/registry.go:97` | contracts | 未注册 → `ErrKernelNotRegistered`；`Register` 仅装配期、重名 panic | **不改** |
| `MarketRouter.ResolveKernel` | `pkg/backtest/contracts`（DBT-06） | contracts | 未登记市场 → `ErrMarketNotRegistered`；路由表未装配 → **创建即拒绝、job 不落库** | **不改** |
| `TradingCalendar.TradingDates` | 直连 `*storage.PostgresStore.GetTradingDates`（`pkg/backtest/walkforward/walkforward.go:110`） | data 服务（X1b） | `start > end` → `ErrInvalidInput`；无交易日 → 空切片 + `nil`（由调用方判「区间无效」）；数据缺口 → `ErrDataGap`（QT-10 落地后） | 新增只读 port = MINOR；**禁止**本仓写 `market.*` |
| `JobStore`（今天 7 方法） | `pkg/backtest/job/job.go:56` | core-only（迁移期） | 无错误语义（`map` 原样透传）；**O0 后删除**，不是长期 port | 删除 = MAJOR（core 内部） |
| `RunStateStore` | **零实现** | — | **作废**（H5：`strategy_state` 归 strategy-runtime 模块，core 写 `quant.strategy_state`） | 不新增；见 §9.7 R4 |

### 9.3 B 组 · DTO 与 wire 形状

| DTO / 字段 | 今天 | 契约位 | 形状与判据 | 版本策略 |
|---|---|---|---|---|
| `RunRecord` | `JobRecord`（`pkg/backtest/job/job.go:68`） | contracts | `{ID, IdempotencyKey, RequestJSON, RequestHash, Status, ResultStatus, Priority, Attempts, MaxAttempts, WorkerID, LeaseUntil, CancelRequested, ErrorMessage, ResultURI, CreatedAt, StartedAt, CompletedAt}`；`Status` = 队列生命周期六值（§9.5），`ResultStatus` = K9 三值或空 | 加字段 = MINOR；改 `Status` 语义 = MAJOR |
| `RunSubmitRequest`（wire） | `CreateJobRequest`（`pkg/backtest/job/job.go:181`） | 本仓 HTTP | `{idempotency_key, run_spec: BacktestRequest, priority}`；`run_spec` 是**完整** RunSpec，不再拆分扁平列；`idempotency_key` 必填、长度上限 128 | 新 endpoint = MINOR；字段改名 = MAJOR |
| `ListFilter` | 无 | contracts | `{Status, ResultStatus, Limit, Before}`；`Limit` 由实现钳到 ≤100；未知枚举值拒绝 | 同上 |
| `WorkerRecord` | 无 | contracts | `{WorkerID, Kernels []string, Status(active/draining/offline), LastHeartbeatAt, DrainRequestedAt, StartedAt, Meta}`；`Kernels` 用于领取过滤（未知内核的 worker 不接该活） | 同上 |
| `BatchRequest` | `[]BatchTask + BatchConfig`（`pkg/backtest/batch/batch.go:24`、`:84`） | 本仓 HTTP | `{idempotency_key, base_run_spec: BacktestRequest, grid: {param: [values]}, concurrency, run_walk_forward, walk_forward}`；网格展开是**笛卡尔积**，展开后逐 task 生成完整 RunSpec（含 base 的 fill_timing/kernel） | 新 endpoint = MINOR |
| `BatchTaskResult` | `BatchResult`（`pkg/backtest/batch/batch.go:37`） | contracts | `{run_id, result_status(K9), error, duration_ms, score?, rank}`；**不内嵌结果内容**（只放 `result_uri`）；`invalid` 保留在列表里但 `rank=0` | 加字段 = MINOR；把 `invalid` 当 completed = MAJOR |
| `BatchSummary` | `BatchSummary`（`pkg/backtest/batch/batch.go:69`） | contracts | 计数三值分开：`completed / invalid / failed`；Scorer 只对 `completed` 排名；`best_run_id` 只在 `completed` 里选 | 同上 |
| `BatchRun` | `BatchReport`（内存，`pkg/backtest/batch/batch.go:56`） | contracts | `{batch_id, status(队列生命周期), total, completed, invalid, failed, summary, tasks[], created_at, completed_at}`；`status` 不是 K9（它不是单次 run 结果） | 同上 |
| `WalkForwardRequest` | `WalkForwardRequest`（`pkg/backtest/walkforward/walkforward.go:56`） | 本仓 HTTP | `{idempotency_key, run_spec: BacktestRequest, windows: {train_days, test_days, step_days, min_train_days}}`；每个窗口的 train/test 都从 `run_spec` **原样派生**（只改日期），不许丢 kernel/timing/seed | 新 endpoint = MINOR |
| `WalkForwardReport` | `domain.WalkForwardReport`（`pkg/domain/backtest.go:120`） | contracts | `{wf_id, status(队列生命周期), windows[], total/completed/invalid/failed_windows, aggregates}`；每个 window 带 `train_run_id` / `test_run_id` / 两侧 `result_status`；**部分失败显式计数，不静默丢** | 同上；旧 `windows_json` 的字段形态不冻结（新 endpoint 用新 DTO） |
| `WalkForwardParams` | 无 json tag（`pkg/domain/backtest.go:92`） | contracts | wire 用 `snake_case`（`train_days` / `test_days` / `step_days` / `min_train_days`），必填正整数；`min_train_days` 空 = `train_days/2`（与今天 `RunWalkForward` 一致） | 旧 `/api/walkforward` 的 `TrainDays` 形态保持到切换；新 endpoint 不复用 |
| `ProvenanceResponse` | 无 | 本仓 HTTP | `{run_id, run_spec_hash, dataset_snapshot_hash, risk_rules_hash, strategy_ref, artifact_uri, artifact_hash, version_hash, params_hash, seed, kernel, engine_version, fill_timing, result_status, created_at}`；**独立 DTO，不扩 `BacktestResponse`** | 加字段 = MINOR；hash 算法改 = MAJOR |
| `BacktestRequest` | `pkg/backtest/contracts/contracts.go:171` | contracts（frozen） | 本单只读、只存、只透传；**不改字段**（per-run seed / strategy params 见 §9.7 R3） | 冻结面，改动走 SPEC 评审 |
| `BacktestResponse` | `pkg/backtest/contracts/contracts.go`（K9，`docs/SPEC.md:3179`） | contracts（frozen） | 27 项字段集 + 三值 status；本仓**不解析错误串**、不重算 status，只原样存/取 | 冻结面，改动走 SPEC 评审 |
| `Job`（今天的响应） | `pkg/backtest/job/job.go:209` | core-only | 只有 `pending` / `running` / `completed` / `failed` 四值，没有 `result_status`；新 endpoint 不复用 | O0 后删除 |

### 9.4 C 组 · endpoints（目标形状；今天全部未实现）

| 方法 / 路径 | 请求 → 响应 | 错误码（§api-schema §2） | 幂等 | 版本策略 |
|---|---|---|---|---|
| `POST /api/runs/jobs` | `RunSubmitRequest` → 202 `{job_id, status:"pending", result_status:null}`；幂等命中 → 200 `{job_id, status, result_status, idempotent_replay:true}` | `unauthenticated` / `forbidden` / `invalid_params` / `missing_idempotency_key` / `idempotency_conflict` / `queue_full` | `idempotency_key` 全局唯一；同键同 `request_hash` 返回原 job | 新端点 = MINOR |
| `GET /api/runs/jobs/{id}` | `{job_id, status, result_status, error, result_uri, run_spec_hash, created_at, started_at, completed_at}` | `unauthenticated` / `forbidden` / `job_not_found` | 只读 | 加字段 = MINOR |
| `DELETE /api/runs/jobs/{id}` | → 202 `{job_id, status:"cancelling"}`；`pending` 直接 200 `{status:"cancelled"}`；终态 → 200 原状态 | `unauthenticated` / `forbidden` / `job_not_found` | 幂等（重复取消返回同终态） | 语义变更 = MAJOR（旧 `CancelJob` 对 pending 删行） |
| `GET /api/runs/jobs?status=&result_status=&limit=` | → `{jobs:[…], total}` | `unauthenticated` / `forbidden` / `invalid_params` | 只读 | 加过滤 = MINOR |
| `POST /api/runs/batch` | `BatchRequest` → 202 `{batch_id, status:"pending"}` | 同 submit + `invalid_params`（网格空/超上限） | `idempotency_key` 同 submit | 新端点 = MINOR |
| `GET /api/runs/batch/{id}` | → `BatchRun`（含 tasks 三值计数与 rank） | `unauthenticated` / `forbidden` / `batch_not_found` | 只读 | 加字段 = MINOR |
| `POST /api/runs/walkforward` | `WalkForwardRequest` → 202 `{wf_id, status:"pending"}` | 同 submit + `invalid_params`（窗口参数非法/区间不足） | `idempotency_key` 同 submit | 新端点 = MINOR |
| `GET /api/runs/walkforward/{id}` | → `WalkForwardReport`（逐窗状态 + 计数） | `unauthenticated` / `forbidden` / `walkforward_not_found` | 只读 | 加字段 = MINOR |
| `GET /api/runs/workers` | → `{workers:[WorkerRecord]}` | `unauthenticated` / `forbidden` | 只读 | 新端点 = MINOR |
| `POST /api/runs/workers/{id}/drain` | → 200 `{worker_id, status:"draining"}` | `unauthenticated` / `forbidden` / `worker_not_found` / `invalid_transition` | 幂等 | 新端点 = MINOR |
| `GET /api/runs/{id}/provenance` | → `ProvenanceResponse` | `unauthenticated` / `forbidden` / `job_not_found` / `provenance_not_ready`(409) / `provenance_not_found`(404) / `provenance_missing`(500) | 只读 | 新端点 = MINOR；三 hash 字段 = 冻结面（不扩 `BacktestResponse`） |

**fail-closed 三条**：① 未注册内核 / 未登记 market → **入队前拒绝**，行都不落（`MarketRouter` 与 `EngineRegistry` 的现有语义）；② 拿不到 `request_hash`（canonical 编码失败）→ 拒绝；③ 终态为 `completed` / `invalid` 但 `run_artifacts` 无行 → 该 job 判 `failed`（数据完整性，不是正常路径），并打 `provenance_missing`。

### 9.5 D 组 · `run` schema 目标键与写入语义

> 下面列的是**目标形状**（O0 落地前不改库；落地按 `db-schema.md` §4 的机制建表）。`run` schema 的 DDL **只归本仓**；跨 schema 只读视图或 API，不 `join` 别人的表。

**`run.backtest_jobs`（从 core `backtest_jobs` 迁入）**

| 列 | 类型 / 约束 | 语义 |
|---|---|---|
| `id` | `TEXT PRIMARY KEY`（UUID 文本，≤64） | run 身份；`quant.run_provenance.run_id` 与之对齐（QT-63 ⑤） |
| `idempotency_key` | `TEXT`，部分唯一索引 `WHERE idempotency_key IS NOT NULL` | 幂等键；**不可为空**由 API 层保证，DB 允许 NULL 只为迁移存量行 |
| `request_hash` | `CHAR(64) NOT NULL` | `sha256(canonical request_json)`；幂等冲突判据 |
| `request_json` | `JSONB NOT NULL` | 完整 canonical RunSpec（含 market→kernel 解析结果）；worker **只执行这一份** |
| `status` | `TEXT NOT NULL` + `CHECK` | `pending` / `running` / `cancelling` / `completed` / `failed` / `cancelled`（**队列生命周期，不是 K9**） |
| `result_status` | `TEXT` + `CHECK (… IS NULL OR … IN ('completed','invalid','failed'))` | K9 三值；终态前 NULL |
| `priority` | `INT NOT NULL DEFAULT 0` | 越大越先领 |
| `attempts` / `max_attempts` | `INT NOT NULL DEFAULT 0` / `DEFAULT 3` | 租约回收重试；超过 → `failed` + `attempts_exhausted` |
| `worker_id` | `TEXT` | 当前 owner；终态后保留（审计） |
| `lease_until` | `TIMESTAMPTZ` | 租约；过期即可被 `ReapExpired` 回收 |
| `cancel_requested` | `BOOLEAN NOT NULL DEFAULT FALSE` | 跨进程取消信号 |
| `error_message` | `TEXT` | `failed` 必填（与 K9 的 `error` 同义） |
| `result_uri` | `TEXT` | **结果本体指针**；`run` 不存结果内容（AGENTS 边界） |
| `created_at` / `started_at` / `completed_at` | `TIMESTAMPTZ` | 时间线 |

索引：`(status, priority DESC, created_at ASC) WHERE status='pending'`（领取）、`(worker_id, status)`（drain 收尾）、`(created_at DESC)`（列表）、`(idempotency_key)` 唯一（幂等）、`(result_status)`（按结论筛选）。**迁移期**保留旧 `result JSONB` 只读（O1 切读后删除），不在新代码里读写它。

**两轴映射（冻结；实现单按这张表写终态）**

| 执行结果 | `status` | `result_status` | `error_message` |
|---|---|---|---|
| 跑完且有效 | `completed` | `completed` | 空 |
| 跑完但结果无效（K9 `invalid`） | `completed` | `invalid`（`invalid_reasons` 在结果里） | **空**（无效不是故障） |
| 引擎故障（已产出 ResultSpec） | `failed` | `failed` | 必填 |
| 引擎故障（未产出 ResultSpec，如路由拒绝） | `failed` | `NULL` | 必填 |
| 取消确认 | `cancelled` | `NULL` | 可空 / `cancelled` |
| 租约重试耗尽 | `failed` | `NULL` | `attempts_exhausted` |

**`run.run_artifacts`（X0 载体；🔒 冻结最小集）**

| 列 | 说明 |
|---|---|
| `run_id` | `TEXT PRIMARY KEY`，FK → `run.backtest_jobs.id`（同 schema） |
| `strategy_ref` | 三形态 `id@artifact_hash` / `id@version_hash` / `id`（QT-03 冻结） |
| `dataset_snapshot_hash` | 数据集身份；含 `calendar_hash` / `adjustment_basis`（QT-03） |
| `risk_rules_hash` | 规则快照身份；QT-42 X3b 之前写 `""`（**空串 = 未登记**，不是「无规则」的假值） |
| `artifact_uri` | 明细 artifact 指针；内置策略可为 NULL |
| `artifact_hash` / `version_hash` | QT-63 ⑦ 的对应列 |
| `run_spec_hash` / `params_hash` / `params_json` | run 输入身份与生效参数（QT-63 ⑦） |
| `seed` / `kernel` / `engine_version` / `fill_timing` | 生效值（N6-5） |
| `created_at` | 写入时间 |

**不存 `status`**（QT-63 ④ / H5 的单一同源）；`/provenance` 由本表 ⋈ `backtest_jobs` 取 `result_status`。三 hash + `artifact_uri` 是**冻结面**：增列 / 改口径 / 改 hash 算法都走评审（`db-schema.md` §1 的冻结标注）。同一 `run_id` 重复写入：同身份幂等；**不同 hash → fail-loud**（`ErrRunProvenanceConflict`，QT-63 ⑦；禁止 `ON CONFLICT DO NOTHING` 静默吞身份漂移）。`invalid_reasons` **不进 `run`**——它在结果 artifact 里，本表只记结论 `result_status`。

**`run.batch_runs`**：`batch_id TEXT PK`、`idempotency_key`（唯一）、`request_json JSONB`（网格 + base RunSpec）、`status`（生命周期）、`total/completed/invalid/failed`、`summary_json JSONB`（只放排序与计数，不复制每个结果）、`created_at/completed_at`。

**`run.walk_forward_reports`**：`wf_id TEXT PK`、`idempotency_key`（唯一）、`request_json JSONB`、`window_config JSONB`、`status`、`total/completed/invalid/failed_windows`、`report_uri TEXT`（明细指针）、`summary_json JSONB`、`created_at/completed_at`。每个窗口的 train/test 是 `backtest_jobs` 的独立行（各自 `run_artifacts`）；窗口→run 的关系见 §9.7 R6。

**`run.worker_registry`**：`worker_id TEXT PK`、`kernels TEXT[]`、`status TEXT CHECK (active|draining|offline)`、`last_heartbeat_at TIMESTAMPTZ`、`drain_requested_at TIMESTAMPTZ`、`started_at TIMESTAMPTZ`、`meta JSONB`；心跳超时 → `offline`（由 `ReapExpired` 同批次处理），不是删行。

**`quant.strategy_state`**：**不在本表**（H5；`strategy_state` 归 strategy-runtime 模块，core 写 `quant.strategy_state`）。

### 9.6 E 组 · 版本策略

| 面 | 稳定性 | 变更规则 |
|---|---|---|
| `JobQueue` / `WorkerRegistry` 方法集 | 新增（S1 后 frozen） | **加方法 = MAJOR**（接口实现方全红）；加可选能力走新接口（`JobQueueV2`）= MINOR；改错误语义 = MAJOR |
| `RunRecord` / `WorkerRecord` / `ListFilter` 字段 | stable | 加字段 = MINOR（旧读者忽略未知 JSON）；改名 / 删除 = MAJOR |
| 队列 `status` 取值 | 新增 | 加**非终态** = MINOR（旧客户端按「未完成」处理）；加**终态** = MAJOR（旧客户端可能永远等）；改终态语义 = MAJOR |
| K9 `result_status` | frozen | 只 `completed` / `invalid` / `failed`；第四值 = 契约变更（SPEC 评审） |
| `BacktestRequest` / `BacktestResponse` | frozen | 本单不改；要加 per-run seed / params / 三 hash 返回字段 → 走 SPEC 冻结面评审（QT-03 ③-⑥） |
| `run` schema 列 | stable | 加列 = MINOR（expand-contract）；删列 / 改类型 / 改主键 = MAJOR + ADR |
| `run_artifacts` 三 hash + pointer | 🔒 frozen | 增列 / 改口径 / 改 hash 算法 = 评审；历史 hash 不可回填 |
| 幂等键派生 | stable | 改派生规则 = MAJOR（同请求会被当新 run 重跑） |
| HTTP 端点 | stable | 只增不删；`DELETE` 从「删行」改「取消保留」= MAJOR，必须给旧消费方切换窗口（R1） |

### 9.7 R 组 · 待裁事项（不落地不阻塞对表，但阻塞实现）

| # | 事项 | 现状 / 冲突 | 建议裁决 |
|---|---|---|---|
| R1 | **旧路径与新路径的双真相**：core `/api/backtest`（异步+同步混在一个 POST）、`/api/batch`、`/api/walkforward` 与目标 `/api/runs/*` | 旧端点今天仍在服务；`CreateJob` 的 goroutine 语义与目标队列不同 | 按 O0 的「双写 → 对拍 → 切读 → 停写」迁移；切换前新端点只读/影子运行。旧 `CancelJob` 对 `pending` 的「删行」语义必须在新端点上线前改为「取消保留」，否则幂等键随行消失 |
| R2 | **结果本体指针指向哪** | 今天 `backtest_jobs.result JSONB` 存内容；本仓边界禁止存内容；artifact store 归属未定（strategy 仓？core state store？） | 由 core 的 state store / strategy artifact 服务提供内容读取面，本仓只存 `result_uri`；O1 切读时一并落 |
| R3 | **per-run seed / 策略参数** | `BacktestRequest` 没有 `Seed` 与 `StrategyParams`；seed 是进程级 `Config.Seed`（`pkg/backtest/engine.go:35`），params 只有默认值（`pkg/backtest/provenance.go:226`） | interim：编排配置固定 worker seed、同部署同值，`run_spec_hash` 记生效 seed；per-run 覆写要改冻结的 RunSpec（QT-21 / SPEC 评审），不在 QT-44 擅改 |
| R4 | **`JobQueue` port 的归属与版本** | contracts 今天 `JobQueue` 零命中；本仓是 S0 占位；core 需要 adapter | S1 迁 contracts（core adapter + 本仓实现 = 两个使用方，满足 ADR-0001 ①）；先在本仓落同名接口，contracts 发布后切 import。`RunStateStore` **不新增**（H5 作废） |
| R5 | **鉴权与 worker 身份** | C 组全部端点未实现；worker_id 可伪造；`/api/runs/*` 与 QT-50 ④ 的鉴权矩阵未合流 | 与 QT-50 ④ 同一套 `unauthenticated` / `forbidden`；worker 注册必须带凭证（不能只报一个 id）；`drain` 只能由运维角色调用 |
| R6 | **batch / walkforward 的子 run 关系** | 每个 task / 窗口都是独立 run，但 `run` schema 只有 5 张表；直接在 `backtest_jobs` 加 `parent_id` 会跨表引用两种父（batch / wf） | 二选一：(a) `run.run_children(parent_kind, parent_id, child_run_id)` 第六张表（结构变更走 ADR）；(b) 子 run 的 `request_json` 带 `parent` 元数据、父表存 `child_run_ids TEXT[]`。**倾向 (a)**：可查询、可加唯一约束 |
| R7 | **队列深度与 `queue_full` 阈值** | 今天无上限（每 job 一个 goroutine）；429 语义在新端点冻结，但阈值是部署配置 | 契约只冻结「达上限 → 429 + `queue_full`」；默认值由部署配置给出（建议 `max_pending = worker 数 × 4`，压测后定），不进契约 |
