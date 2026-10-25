---
status: active
last-verified: 2026-10-24
verified-by: 施工 ⑤（R10 心跳超时 → offline 已落两个实现；§8/§9.2/§9.7/§9.8 同步）
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
   run.backtest_jobs / parent_jobs / run_artifacts / worker_registry（batch_runs / walk_forward_reports 为迁移期旧表）
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
| X5a | job 队列 + 状态机 + 幂等 | ✅ 内存 + PG 两个实现 + jobs 4 端点（施工 ②③④）；R8/R9 已落，见 §9.8 |
| X5b | batch + walkforward | 🔶 规划与汇总纯函数已落（施工 ⑥）；**父表 `parent_jobs` + `backtest_jobs.parent_job_id` 已落（施工 ⑦，裁-13）**，提交写父表 + 汇总写回已接 `internal/parents`；**父作业 HTTP 2 端点已落（施工 ⑧）**；子 run 入队、worker 执行未落 |
| X5c | worker 注册 / 心跳 / drain / 心跳超时 | 🔶 注册 / 心跳 / drain / 心跳超时 → `offline` 已落（内存 + PG，施工 ⑤）；worker 进程本身未做（派活契约未定），见 §9.8 |
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

// KernelAwareJobQueue 是 JobQueue 的**可选能力**（§9.7 R9 裁决，2026-10-23；
// §9.6：加可选能力走新接口 = MINOR，上面那 10 个方法一个字没动）。
// 装配方用类型断言探测；不提供它的实现行为 = 冻结的 `Claim`（不过滤）。
type KernelAwareJobQueue interface {
    ClaimByKernel(ctx context.Context, workerID string, lease time.Duration) (*RunRecord, error)
}

type WorkerRegistry interface {
    Register(ctx context.Context, w WorkerRecord) error
    Heartbeat(ctx context.Context, workerID string, at time.Time) error
    Drain(ctx context.Context, workerID string) error
    List(ctx context.Context) ([]WorkerRecord, error)
}

// WorkerReaper 是 WorkerRegistry 的**可选能力**（§9.7 R10，2026-10-24 落地；
// §9.6：加可选能力走新接口 = MINOR，上面 4 个方法一个字没动）。
type WorkerReaper interface {
    ReapOffline(ctx context.Context, now time.Time, timeout time.Duration) (int, error)
}
```

| 方法 | 今天 | 契约位 | 错误语义（本表冻结） | 版本策略 |
|---|---|---|---|---|
| `Enqueue` | 无（`CreateJob` 直接插行并起 goroutine） | contracts | 幂等键必填；同键同 `request_hash` → 返回原行、`created=false`、`err=nil`；同键不同 hash → `ErrIdempotencyConflict`；键为空 → `ErrInvalidInput`；队列深度达上限 → `ErrQueueFull`（HTTP 429）。**不执行、不路由**（market→kernel 已在调用方解析完） | 新增 port = MINOR；方法签名改 = MAJOR |
| `Claim` | 无 | contracts | 原子领取一条 `pending` 且 `lease_until IS NULL OR < now()` 的行，写 `running` + `worker_id` + `lease_until`；无活 → `(nil, nil)`；**非 active** 的已注册 worker（draining / offline）→ `ErrWorkerDraining`；`lease <= 0` → `ErrInvalidInput`。并发安全 = `SKIP LOCKED` 或等价 | 同上 |
| `RenewLease` | 无 | contracts | 只允许当前 owner 在租约有效期内续；行不存在 / 已终态 / 别人持有 / 租约已过期 → `ErrLeaseLost`（worker 必须**立即停止**，不得再写结果） | 同上 |
| `Complete` | `UpdateJobCompleted`（无 owner 校验） | contracts | 必须持有效租约；写 `status=completed` + `result_status` + `result_uri`；`result_status` ∈ `{completed, invalid}`（**`failed` 走 `Fail`**，其他值 → `ErrInvalidInput`）；`result_uri` 必须非空（结果内容不在 `run` 里，空指针 = 结果不可达）；同一终态同载荷重复调用 → `nil`（幂等）；不同载荷 / 已 `failed` / 已 `cancelled` → `ErrInvalidTransition`；租约过期 → `ErrLeaseLost` | 终态语义变更 = MAJOR |
| `Fail` | `UpdateJobFailed`（把 invalid 也塞进来） | contracts | 必须持有效租约；写 `status=failed` + `error_message`；`resultStatus` ∈ {`""`, `failed`}——确实产出过 ResultSpec 传 `failed`，否则传空串（列写 NULL）；其他值 → `ErrInvalidInput`；重复调用幂等 | 同上 |
| `RequestCancel` | `CancelJob`：pending 删行、running 调内存 cancel | contracts | 未知 id → `ErrJobNotFound`；`pending` → 直接 `cancelled`；`running` → `cancelling`（不动跑着的 worker）；终态 → `nil`（幂等）；重复 → `nil` | 语义变更 = MAJOR（删除→保留） |
| `AckCancel` | 无 | contracts | 仅 owner 在 `cancelling`/`running` 上确认；写 `cancelled`；非 owner / 租约过期 → `ErrLeaseLost`；已终态 → `ErrInvalidTransition` | 同上 |
| `Get` / `List` | `GetBacktestJob` / `ListBacktestJobs`（map） | contracts | `Get` 未知 → `ErrJobNotFound`；`List` 的 `limit` 上限由实现钳制（建议 100），`status` / `result_status` 过滤是**精确匹配**，未知值 → `ErrInvalidInput`（不猜） | 新增过滤字段 = MINOR |
| `ReapExpired` | `CleanupStaleRunning`（只扫 `running`，不区分租约） | contracts | 两类迁移（R8 于 2026-10-23 裁决后并入）：① `running` 且租约过期 → `pending` 且 `attempts+1`，超过 `max_attempts` → `failed` + `attempts_exhausted`；② `cancelling` 且租约过期 → `cancelled`，**不**计 attempts（它不是重试，是给已作废的结果收尾）。两类的 `worker_id` / `started_at` 都保留；必须可并发/重入；返回本次迁移行数 | 新增 = MINOR；R8 那条是**已裁语义**，不是新方法 |
| `ClaimByKernel`（可选接口 `KernelAwareJobQueue`） | 无（`Kernels` 此前只落库不消费） | contracts | 与 `Claim` 同序（priority DESC → created_at ASC → id ASC）、同租约判据、同 drain 校验；**多一条内核过滤**：能力取 worker 在注册表里声明的 `Kernels`（不是调用方现传），行的生效内核见下方口径；未注册 / 注册表未装配 / 声明为空 ⇒ 不过滤（与「没登记不等于在 drain」同形）；无活 → `(nil, nil)` | 可选新接口 = MINOR（§9.6）；冻结的 10 方法集未动 |

**生效内核的抽取规则（两侧同口径，2026-10-23 落地时实测钉住）**：`request_json` 里的
`kernel` **只有是 JSON 字符串才算内核**；缺键 / `null` / 数字 / 对象 / 顶层非对象 / 空串 /
纯空白一律按默认内核 `go`（`dataflow.md` §1「默认值不回填」是读侧口径）。字符串值两端空白不算名字的一部分。
Go 侧是 `queue.KernelOf`，PG 侧是 `claimSQL` 里 `jsonb_typeof(request_json -> 'kernel') = 'string'`
那层 CASE + `btrim`。少写任何一层都会造成同一行一边能领、另一边领不到——钉子是
`qtest` 的 `ClaimByKernelReadsOnlyStringKernels`（两个实现跑同一条用例）。

| `Register` | 无 | contracts | 同 `worker_id` 重复注册 = 覆盖能力/心跳（幂等），不允许两个进程共用同一 id（由装配方保证）；空 id → `ErrInvalidInput`。status：已 drain 的**永远保留**；`offline` 且从未请求 drain 的回到 `active`（存活证明，且刷新心跳，见 R10） | 新增 = MINOR |
| `Heartbeat` | 无 | contracts | 未知 worker → `ErrWorkerNotFound`（**不自动注册**，fail-loud）；draining → 只更新心跳、返回 `nil`；`offline` 且从未请求 drain → 回到 `active`（同上） | 同上 |
| `Drain` | 无（只有进程级 `Shutdown`） | contracts | 未知 worker → `ErrWorkerNotFound`；`active → draining`；重复 drain → `nil`（幂等）；`offline` → `ErrInvalidTransition` | 同上 |
| `List`（worker） | 无 | contracts | 空列表 = `[]`，不是 `nil`（JSON 稳定） | 同上 |
| `ReapOffline`（可选接口 `WorkerReaper`） | 无 | contracts | `timeout <= 0` → `ErrInvalidInput`（阈值属部署配置，不给默认值）；`last_heartbeat_at <= now-timeout`（含等于）→ `offline`，**不删行**、已 offline 不重复计数、NULL 心跳不参与判定；`draining` 同样在回收范围内（滚动升级的收尾）；返回本轮迁移行数 | 可选新接口 = MINOR（§9.6）；改判定判据 = MAJOR |
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
| `WorkerRecord` | 无 | contracts | `{WorkerID, Kernels []string, Status(active/draining/offline), LastHeartbeatAt, DrainRequestedAt, StartedAt, Meta}`；`Kernels` 的消费点在施工 ④ 已落——`ClaimByKernel` 按它过滤（未知内核的 worker 不接该活），空声明 = 不过滤 | 同上 |
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
| `POST /api/runs/parents` | `{idempotency_key, kind, batch?/walkforward?}` → 202 `{parent_job_id, kind, status:"running", plan_hash, total, counts}`；幂等重放 → 200 + `idempotent_replay:true` | `unauthenticated` / `forbidden` / `invalid_params`（网格空/超上限/窗口参数/日期）/ `missing_idempotency_key` / `idempotency_conflict` | **身份 = `plan_hash`**：同计划（换键也算）返回同一父 id；同键换计划 = 409 | 新端点 = MINOR（取代未实现的 batch/wf 4 端点） |
| `GET /api/runs/parents/{id}` | → `{parent_job_id, kind, status, plan_hash, total, counts:{completed,invalid,failed,pending,settled}, children:[{run_id,status,result_status,error,created_at}], summary, created_at, completed_at}` | `unauthenticated` / `forbidden` / `parent_not_found` | 只读；`children` 来自可选能力 `ParentChildLister`（不支持时为空数组） | 加字段 = MINOR |
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
| `request_json` | `JSONB NOT NULL` | 完整 canonical RunSpec 的 JSONB 投影（可查询）；worker **只执行这一份**，不从扁平列重建 |
| `request_canonical` | `TEXT NOT NULL` | 同一份内容的**字节锚点**：`request_hash = sha256(request_canonical)` 恒成立。读路径回这一列——JSONB 会规范化数字文本（实测 `1e-05` 读回 `0.00001`），拿它复算 hash 会把同一个幂等键判成两次不同请求（施工 ③ 实测，钉子 `internal/pgstore/hashanchor_test.go`） |
| `status` | `TEXT NOT NULL` + `CHECK` | `pending` / `running` / `cancelling` / `completed` / `failed` / `cancelled`（**队列生命周期，不是 K9**） |
| `result_status` | `TEXT` + `CHECK (… IS NULL OR … IN ('completed','invalid','failed'))` | K9 三值；终态前 NULL |
| `priority` | `INT NOT NULL DEFAULT 0` | 越大越先领 |
| `attempts` / `max_attempts` | `INT NOT NULL DEFAULT 0` / `DEFAULT 3` | 租约回收重试；超过 → `failed` + `attempts_exhausted` |
| `worker_id` | `TEXT` | 当前 owner；终态后保留（审计） |
| `lease_until` | `TIMESTAMPTZ` | 租约；过期即可被 `ReapExpired` 回收 |
| `cancel_requested` | `BOOLEAN NOT NULL DEFAULT FALSE` | 跨进程取消信号 |
| `error_message` | `TEXT` | `failed` 必填（与 K9 的 `error` 同义） |
| `result_uri` | `TEXT` | **结果本体指针**；`run` 不存结果内容（AGENTS 边界） |
| `parent_job_id` | `TEXT`，可空，FK → `run.parent_jobs.parent_job_id`（`ON DELETE SET NULL`）；部分索引 `WHERE parent_job_id IS NOT NULL` | 子 run 关系（裁-13）：批量网格点 / 前向验证的 train·test 都是独立 run，靠这一列归属父计划。**不**在 `request_json` 里塞父元数据（那会改 `request_hash`） |
| `created_at` / `started_at` / `completed_at` | `TIMESTAMPTZ` | 时间线 |

索引：`(priority DESC, created_at ASC, id ASC) WHERE status='pending'`（领取；`status` 不作键列——partial 谓词已限定，第三键 `id` 与内存实现的 tie-break 同形；谓词里**不放** `lease_until`，因为冻结判据是「已释放**或**已过期」，见 §9.2）、`(worker_id, status)`（drain 收尾）、`(created_at DESC)`（列表）、`(idempotency_key)` 唯一（幂等）、`(result_status)`（按结论筛选）。**迁移期**保留旧 `result JSONB` 只读（O1 切读后删除），不在新代码里读写它。

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

**`run.parent_jobs`（裁-13：一张表承载两种父）**：`parent_job_id TEXT PK`、`idempotency_key`（唯一）、`kind TEXT CHECK (batch|walkforward)`、`plan_canonical TEXT`（**字节锚点**）、`plan_hash CHAR(64)`（= `sha256(plan_canonical)`，**唯一**——计划身份，同计划换键也只留一行）、`status`（与 job 同六态词表）、`total/completed/invalid/failed`、`summary_json JSONB`（排序 / 逐窗结论，不复制子结果）、`created_at/completed_at`。提交由 `internal/parents.SubmitBatch` / `SubmitWalkForward` 写一行（`internal/plan` 展开 → canonical/hash → `EnqueueParent`）；汇总由 `ApplyBatchSummary` / `ApplyWalkForwardSummary` 复用 `plan.Aggregate*` 后写回三值计数与父状态。

**`run.batch_runs` / `run.walk_forward_reports`（迁移期旧表）**：DDL 保留只前滚，**停写**；父记录统一落 `parent_jobs`，O1 对拍后删除。

**`run.worker_registry`**：`worker_id TEXT PK`、`kernels TEXT[]`、`status TEXT CHECK (active|draining|offline)`、`last_heartbeat_at TIMESTAMPTZ`、`drain_requested_at TIMESTAMPTZ`、`started_at TIMESTAMPTZ`、`meta JSONB`；心跳超时 → `offline`（由 `WorkerReaper.ReapOffline` 写入，`cmd` 与 `ReapExpired` 同一轮周期任务调用；R10），不是删行。

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
| R6 | **batch / walkforward 的子 run 关系** | ✅ **已裁 2026-10-25（裁-13）**：建**一张** `run.parent_jobs`（`kind` + `plan_canonical`/`plan_hash` + 三值汇总），子 run 用 `run.backtest_jobs.parent_job_id` 列 + 索引表达；**不**塞 `request_json`（`request_canonical` 锚点已证明 JSONB 不能当身份），**不**建两张实体表（两种父共用一套生命周期，`internal/plansuite` 已钉「不各写一套」）。`batch_runs` / `walk_forward_reports` 降为迁移期旧表、停写。落地见 §9.8 施工 ⑦ |
| R7 | **队列深度与 `queue_full` 阈值** | 今天无上限（每 job 一个 goroutine）；429 语义在新端点冻结，但阈值是部署配置 | 契约只冻结「达上限 → 429 + `queue_full`」；默认值由部署配置给出（建议 `max_pending = worker 数 × 4`，压测后定），不进契约 |
| R8 | **`cancelling` 行的租约回收出口**（2026-10-23 施工 ① 新登记；**已裁 2026-10-23 = 选 (a)**；**已落 施工 ④，两个实现同批**） | §9.5 的 `ReapExpired` 只定义了 `running` 的回收：`cancelling` 行的 owner 若崩溃，`AckCancel` 因租约过期拒绝、`ReapExpired` 又不处理它——该行会永久停在中间态 | ✅ 落地形状：`ReapExpired` 的第二类迁移「`cancelling` 且租约过期 → `cancelled`」，**不**计 attempts（它不是重试，是给已作废的结果收尾）。内存侧在 `Memory.ReapExpired` 的分支里；PG 侧是 `reapCancellingSQL` 这条**独立语句**（`reapSQL` 保持原样：两条判据不同形就不合并成一条带 CASE 的 UPDATE）。钉子：`qtest.ReapLandsOrphanedCancellingRow`（两侧同跑），破坏腿见 `docs/TASKS.md` 施工 ④ |
| R9 | **按内核能力（`Kernels`）领取过滤**（2026-10-23 施工 ① 自审新登记；**已裁 2026-10-23 = 加可选新接口（MINOR）——既不是 (a)，也不是当初倾向的 (b)**；**已落 施工 ④**） | §9.3 写 `Kernels` 用于"未知内核的 worker 不接该活"，但 §9.2 冻结的 `Claim(ctx, workerID, lease)` **没有内核参数**——过滤无法在领取点实现；`WorkerRecord.Kernels` 落了库但没有消费方 | ✅ 落地形状：新接口 `KernelAwareJobQueue.ClaimByKernel`——§9.6「加可选能力走新接口 = MINOR」那条的第一次实际使用，冻结的 10 方法集一字未动（(a) 会破 §9.6，(b) 会让能力声明继续只落库）。能力取 worker **在注册表里声明的** `Kernels`，不是调用方现传；未注册 / 无声明 / 注册表未装配 ⇒ 不过滤。行的生效内核抽取规则见 §9.2 那条口径（只有 JSON 字符串算内核）。钉子：`ClaimByKernelReadsOnlyStringKernels`（两侧同口径）+ `ClaimWithoutKernelFilterStaysUnfiltered`（证明冻结面没被顺手加上过滤）。**仍未落**：worker 进程本身（本仓没有引擎），消费方目前只有套件与两个编译期断言 |
| R10 | **worker 心跳超时 → `offline`**（2026-10-24 施工 ⑤ 落地；裁决同 R9：加可选新接口 = MINOR） | §9.3 的 `WorkerStatus` 有 `offline`，但冻结的 4 方法集没有把 worker 推过去的入口——心跳一停，注册表里仍是 `active`，`Claim` / `ClaimByKernel` 继续把活派给一个已经死掉的 worker。补方法 = MAJOR（实现方全红），不补则 `offline` 永远是死状态 | ✅ 落地形状：可选接口 `WorkerReaper.ReapOffline(ctx, now, timeout)`——判据只有心跳时间（`last_heartbeat_at <= now-timeout`，含等于），`draining` 一并回收（滚动升级的收尾），不删行。**offline 是推断，不是运维意图**：心跳 / 重注册是存活证明，可把未请求过 drain 的 worker 拉回 `active`（并刷新心跳，否则下一轮立刻再判离线）；请求过 drain 的不可（否则 drain 可被一次重注册绕过）。理由：不这样做，worker id 会被一次瞬时超时永久作废（崩溃重启是同一个 id，没有别的恢复入口）。代价：`Claim` 对 offline 仍报 `ErrWorkerDraining`——冻结面只有一个「不配领新活」的哨兵，本单不新增错误语义；装配方要么开回收、要么显式关掉（`cmd` 在开启时 fail-closed 检查能力，见 §9.8）。钉子：`qtest` 的 `ReapOfflineMarksStaleWorkersOffline` / `ReapOfflineRejectsNonPositiveTimeout` / `OfflineWorkerRevivesOnProofOfLifeUnlessDrained` / `OfflineWorkerIsNotDispatchedUntilProofOfLife`（两个实现同跑） |

### 9.8 实现现状（2026-10-23 起，施工 ③④⑤）

> 本节是给下一个 agent 的判据：只看本仓即可分辨「已落地」与「只是接口就位」。与 §9.5 冲突时以 §9.5 为准（那是冻结形状）。

**落地包（`internal/queue`）**

| 文件 | 内容 | 对应 |
|---|---|---|
| `model.go` | `RunStatus` 六态 / `ResultStatus` 三值 / `RunRecord` / `ListFilter` / `WorkerStatus` / `WorkerRecord` / `DefaultKernel`+`KernelOf`+`MatchesKernels`（生效内核抽取规则，§9.2 口径） | §9.3 B 组 |
| `ports.go` | `JobQueue`（10 方法，冻结）/ `WorkerRegistry`（4 方法）/ **可选** `KernelAwareJobQueue`（R9）/ **可选** `WorkerReaper`（R10，心跳超时 → offline）——方法集按 §9.6 冻结，两条可选能力都是「新接口 = MINOR」 | §9.2 A-1 |
| `errors.go` | 8 个领域哨兵（wire code 归 HTTP 层，本包不定义） | §9.4 |
| `hash.go` | `Canonicalize`（键排序 + 去空白 + 保留数字原文）/ `RequestHash` = sha256 | §9.5 `request_hash` |
| `memory.go` | `JobQueue` 内存实现（幂等 / 领取 / 租约 / 两轴终态 / 取消 / 回收）+ `ClaimByKernel`（与 `Claim` 共用一个 `claim` 体，只差一条过滤）；`ReapExpired` 两类迁移（running / orphaned cancelling） | X5a |
| `worker.go` | `WorkerRegistry` 内存实现（注册 / 心跳 / drain）+ `WorkerReaper.ReapOffline`（心跳超时 → offline；offline 的存活证明可复活，drain 意图不可） | X5c |
| `ddl.go` | `run` schema 6 表 + 11 索引（含裁-13 的 `parent_jobs`、`backtest_jobs.parent_job_id` 列 / 索引，以及 X5b 的 `parent_jobs(plan_hash)` 唯一索引）；`Migrations()` 返副本 | §9.5 D 组、`db-schema.md` §5 |
| `parent_memory.go` | `ParentStore` 内存实现（**plan_hash 身份**：同计划换键也返回原行；同键换计划冲突；汇总写回）+ `PrepareParent`（**两个实现共用**的 canonical / hash / 计数校验） | 裁-13、X5b 施工 ⑦⑧ |

**落地包（`internal/plan` 的父计划身份，施工 ⑦）**

| 文件 | 内容 | 对应 |
|---|---|---|
| `internal/plan/parent.go` | `BuildBatchPlan` / `BuildWalkForwardPlan`（复用 `Expand*`）+ `ParentPlan.Canonical/Hash`（`plan_canonical` 的稳定字节与 sha256）+ `PendingCounts` | §9.5 `parent_jobs` |
| `internal/parents/service.go` | `SubmitBatch` / `SubmitWalkForward`（提交即写父表）+ `ApplyBatchSummary` / `ApplyWalkForwardSummary`（复用 `Aggregate*` 写回三值计数与父状态） | 裁-13 |
| `internal/plansuite/suite.go` | 共享套件新增 `ParentPlanIdentityIsDeterministic`：两种父同跑——canonical 逐字节稳定、hash 一致、Units×UnitSize=子 run 数、子计划与 `Plan` 同源 | X5b 验收 |

**落地包（`internal/httpapi`，施工 ②）**

| 文件 | 内容 | 对应 |
|---|---|---|
| `api.go` | jobs 4 端点（`POST`/`GET list`/`GET {id}`/`DELETE {id}`）+ `WithParents` 装配项 + 哨兵 → wire code 的 `classify` + 严格解码（拒未知字段 / 尾随内容 / 超 1 MiB） | §9.4 C 组「任务」表、`api-schema.md` §2 |
| `parents.go` | X5b 父作业 2 端点（`POST /api/runs/parents` 的 batch/wf 折装 + 字段点名；`GET /api/runs/parents/{id}` 的三值汇总 + 逐子状态） | §9.4「批量 / 前向」表、`api-schema.md` §1 |
| `api_test.go` / `parents_test.go` | 传输层单测：202 提交 / 键序无关的 200 重放 / 409 冲突 / 429 队列满 / 取消三态 / K9 两轴 / 列表 fail-closed；父作业 plan_hash 身份（换键同 id）/ 无效计划 400 点名 / 三值不塌缩 / children 逐子状态 | §9.4 |

**传输层口径（施工 ②）**：① 缺幂等键 → 400 `missing_idempotency_key`，在领域校验**之前**判（fail-closed ②，服务端不代生成键）；② 列表的 `total` 是**钳制后本次返回的行数**（冻结方法集没有 Count 能力，全量命中数待 PG 实现裁决）；③ `unauthenticated` / `forbidden` 暂不产生（R5 鉴权未裁）；④ `run_spec` 以 `json.RawMessage` 原样入队（形状校验归引擎侧，本层只管 canonical 化 + hash）。

**落地包（施工 ③：DDL 接 PG + 启动迁移）**

| 文件 | 内容 | 对应 |
|---|---|---|
| `internal/pgstore/store.go` | `Open`（DSN 空 fail-closed，不退回内存）/ `Migrate`（按序幂等执行 `queue.Migrations()`）/ `Options` / `jobCols` 派生两条列清单 / `rowToJob`（NULL↔零值、UTC、CHAR 补齐裁剪）/ `lockJob`（`FOR UPDATE`）/ `checkLease` | §9.2 A-1、§9.5 |
| `internal/pgstore/queue.go` | `JobQueue` 的 PG 实现 10 方法 + 可选的 `ClaimByKernel`：`Claim`/`ClaimByKernel` 共用**一条** `claimSQL`（`$4::text[]` 传 NULL 即不过滤，两个入口不会各写一份 SQL 而漂掉），`FOR UPDATE SKIP LOCKED` + `priority DESC → created_at → id`；`Enqueue` 在 `pg_advisory_xact_lock` 里做「查键 → 数深度 → 插行」；终态方法先锁行再按 §9.5 两轴映射表判定；回收是 `reapSQL`（running）+ `reapCancellingSQL`（R8）两条语句、**同一个事务**（否则半迁移态配一个 count 返回值是错的） | X5a 的 durable 侧 |
| `internal/pgstore/registry.go` | `WorkerRegistry` 的 PG 实现（UPSERT 保 status/started_at/drain 时刻；offline 的复活与心跳刷新是同一条 UPDATE 的 CASE）+ `reapOfflineSQL`（R10） | X5c 的 durable 侧 |
| `internal/pgstore/parents.go` | `ParentStore` 的 PG 实现（`EnqueueParent` 在 `pg_advisory_xact_lock` 里查键 / 插行；`SaveParentSummary` 单条 UPDATE；`plan_canonical` 只存 TEXT、不复算 JSONB） | 裁-13、X5b 施工 ⑦ |
| `internal/qtest/suite.go` | **语义套件**：30 条队列用例 + 9 条注册表用例 + 6 条跨 port 用例 = 每个实现 45 条（施工 ⑤ 新增 3 条注册表 + 1 条跨 port），内存与 PG 各跑一遍 | §9.8「换 PG 实现时行为不变」的可执行判据 |
| `internal/plan/` | **batch / walkforward 规划与汇总**（施工 ⑥，X5b）：`ExpandBatch`（网格笛卡尔积 + `strategy_params` 浅合并）、`SplitWindows` / `ExpandWalkForward`（交易日切窗 + 只改日期派生 train/test）、`AggregateBatch` / `AggregateWalkForward`（三值计数、只对 completed 且给分者排名、父状态判定） | §9.8 施工 ⑥、`dataflow.md` §2 第 9/10 步 |
| `internal/plansuite/suite.go` | **规划 / 汇总共享套件**（施工 ⑥⑦）：8 条共享用例同时跑在 batch 与 walkforward 两个 Subject 上（确定性、完整 RunSpec 保留、三值计数、invalid≠failed、部分失败、全失败、pending、父计划身份）；不依赖任何存储实现 | §9.8「两种父记录不各写一套汇总口径」的可执行判据 |
| `internal/testdb/testdb.go` | PG 腿 harness：跨测试**进程**的会话级 advisory lock + 每用例前清库 | 见下方「施工 ③ 的三条实测」第 3 条 |
| `cmd/orchestrationservice/main.go` | 进程装配：`-dsn`/`ORCHESTRATION_DSN`（两者都空即拒绝启动）→ `Migrate` → 监听（`127.0.0.1:8088`，jobs 4 端点 + `/healthz`）+ 周期回收（`-reap-interval` 默认 30s：租约 `ReapExpired` + 心跳 `ReapOffline`，后者由 `-heartbeat-timeout` 默认 90s 控制，0 = 关闭；开启时装配期 fail-closed 检查注册表确实实现了 `WorkerReaper`） | §9.5 建表入口、`dataflow.md` §2 第 7/8 步 |
| `internal/pgstore/*_test.go` | PG 专属腿：迁移幂等 + 表/索引形状、列清单双向核对、`CHAR(64)` 补齐、三条库约束（唯一索引 / 两个 CHECK / FK）、租约谓词、`request_canonical` 锚点、并发领取不重发 | 破坏验证的落点 |

**PG 侧口径（施工 ③）**：① 时间一律由**应用侧** UTC 时钟提供（`ReapExpired` 的签名本来就收 `now`；内存实现同样如此），不用 DB 的 `now()`——混用会让「租约是否过期」在一次调用里有两个答案；代价是跨进程时钟漂移直接移动租约边界，部署必须让 worker 与 PG 同时钟源。② 读路径的 `RequestJSON` 取 `request_canonical`（TEXT 锚点），`request_json`（JSONB）只作查询投影。③ 端口 `8088` 是本仓自定的监听地址（`-addr` 可覆盖），不在任何跨仓契约里；⚠️ 同期兄弟仓的 `cmd` 默认值有撞号（strategy `:8085` = core 的 analysis-service，risk `:8086` = ai-service，analytics `:8087`），登记在此、属跨仓问题，不由本仓单方面改别人的默认值。

**施工 ③ 的三条实测**（都是先红后修出来的，不是设计时想到的）：
1. `UPDATE … FROM (SELECT … FOR UPDATE SKIP LOCKED) RETURNING id, …` 里的裸 `id` 是**歧义列**（外层 `j` 与子查询 `c` 都有），PG 直接报 42702 → 列清单改为从 `jobCols` 派生带别名的版本。
2. 同一个参数在一条语句里既进 `CAST($4 AS jsonb)` 又进 TEXT 列时，PG 按 jsonb 上下文定型该参数，于是 TEXT 列存进去的是 **jsonb 的文本表示**（`{"a": 1}` 多一个空格）——锚点列当场失效。处置：两处都显式 `$4::text`。同类问题在 `worker_registry` 的 UPSERT 上以「未类型化 NULL 被推成 text」的形式又红了一次（显式 `::timestamptz`）。
3. `go test ./...` 会**并行跑不同包**的测试二进制，两个包的 PG 腿共用同一个测试库时，A 包的「用例前清库」删掉了 B 包正在断言的行——症状是幂等重放莫名变成 202 新建，且**单跑任一包永远绿**。处置：`internal/testdb` 的会话级 advisory lock 把 PG 腿跨进程排队（实测：并发启动时后者的耗时从 0.23s 变成 3.0s，即真的在等锁）。

**施工 ④（R8 / R9 落地）的实测**：
1. **「非字符串的 kernel」两侧本来会不一样**：`request_json->>'kernel'` 对数字给的是它的 JSON 文本（`123`），而 Go 把它解不进 `string` 字段、按默认内核处理。第一版 SQL 只写了 `COALESCE(NULLIF(btrim(...),''),'go')`，`{"kernel":123}` 这行在 PG 里谁都领不走、在内存里 `go` worker 能领走。处置：SQL 加一层 `jsonb_typeof(... ) = 'string'` 的 CASE，规则收敛成一句「只有 JSON 字符串才算内核」。这条不是假设——套件里 `ClaimByKernelReadsOnlyStringKernels` 同跑两个实现才把它逼出来（数字 / null / 顶层数组 / 补白四种形状）。顺带实测：`'["x"]'::jsonb -> 'kernel'` 在 PG 里返回 NULL 而不是报错，所以顶层非对象不需要额外一层守护。
2. **`kernels TEXT[] DEFAULT '{}'` 的空数组会被读成「什么都不能干」**：`= ANY('{}')` 恒假。口径是「没声明 = 不过滤」（与「未注册不等于在 drain」同形），所以 Go 侧在传参前把空切片归一成 NULL，SQL 侧靠 `$4::text[] IS NULL` 那一支。
3. **`Claim` 与 `ClaimByKernel` 必须共用同一条 SQL**：分成两条迟早漂（顺序、租约判据、drain 校验都得一致）。实现方式是同一个 `claimSQL` 多收一个 `$4`，冻结入口传 NULL。破坏腿把过滤器改成空转后，**只有** `ClaimByKernelConsumesTheDeclaredCapabilities` 红、其余三条 pair 用例照绿——这条红得精确，说明过滤器与冻结面确实是分开的两层。

**能跑 / 只是接口就位**

| 能力 | 现状 |
|---|---|
| 入队幂等（同键同 hash 重放 / 不同 hash 冲突 / 队列满） | ✅ 内存 + PG，同一套 qtest 用例；PG 的深度阈值在提交锁内判定（并发下精确，不是尽力而为） |
| 原子领取（priority → created_at → id）+ 租约续期 | ✅ 内存 + PG（`FOR UPDATE SKIP LOCKED` 实测并发不重发）|
| 两轴终态（`completed+completed` / `completed+invalid` / `failed+failed` / `failed+NULL`） | ✅ 内存 + PG |
| 取消（`pending`→`cancelled` 保留行；`running`→`cancelling`；owner `AckCancel`） | ✅ 内存 + PG |
| 租约回收（`attempts+1` / `attempts_exhausted`） | ✅ 内存 + PG（`SKIP LOCKED` 可重入）；`cmd` 里有周期调用 |
| worker 注册 / 心跳 / drain（已 drain 的不因重注册洗回 `active`，也不重置 `started_at`） | ✅ 内存 + PG |
| 跨进程共享队列（A 提交 / B 领取写终态 / A 查到结果） | ✅ `cmd/orchestrationservice` 的端到端腿（真库，两个独立连接池） |
| `run` schema 建表 | ✅ DDL 落盘 + `pgstore.Migrate` 启动执行；已在真库跑过（含幂等重跑） |
| 按内核能力（`Kernels`）过滤领取 | ✅ 内存 + PG（施工 ④，R9 裁决落地）——可选接口 `KernelAwareJobQueue.ClaimByKernel`；抽取规则两侧同口径由 `ClaimByKernelReadsOnlyStringKernels` 钉住。**消费方仍是空的**：本仓没有 worker 进程，目前只有套件与两个编译期断言用到它 |
| `cancelling` 行的租约回收出口 | ✅ 内存 + PG（施工 ④，R8 裁决落地）——租约过期的 `cancelling` 行落 `cancelled`，不计 attempts；`cmd` 的周期回收同批带上 |
| 入队前「内核 / 市场可解析」校验（§9.4 fail-closed ①） | ⬜ 未实现（依赖 `EngineRegistry` / `MarketRouter`，本仓不引） |
| batch / walkforward 的**规划与汇总**（网格展开 / 窗口切分 / 三值计数 / 排名 / 父状态） | ✅ 纯函数已落 `internal/plan`（施工 ⑥，X5b），共享套件同跑两种父；**未落**：`batch_runs` / `walk_forward_reports` 写入、batch/walkforward 4 端点、worker 领取执行 |
| `run_artifacts` / worker / provenance 端点 | ⬜ 未实现（X0 / R5）；jobs 4 端点已落（见上表） |
| worker 心跳超时 → `offline` | ✅ 内存 + PG（施工 ⑤，R10）——可选接口 `WorkerReaper.ReapOffline`；`offline` 的 worker 不再被派活（`Claim` / `ClaimByKernel` 同一个 active 门），存活证明（心跳 / 重注册）可复活未 drain 的 worker。**消费方**：`cmd` 的周期回收；worker 进程本身仍未做 |

**故意取舍（施工 ①②③ 累计）**

1. **DDL 用 Go 数组、不用 `migrations/*.sql`**：取向 ①（与 core 一致，运维零新增）；`Migrations()` 只返副本，已落库语句只前滚。
2. **`Enqueue` 现算 `request_hash`**：调用方自带 hash 与 `request_json` 不符时直接 `ErrInvalidInput`——幂等判据不允许有两个来源。
3. **`Complete` 不接受 `cancelling` 行**：取消一经请求结果即作废，worker 只能 `AckCancel`（两轴映射表里 `cancelling` 的唯一出口是 `cancelled`）。
4. **`ReapExpired` 的 `cancelling` 分支在裁决前刻意不写**（施工 ①②③）：owner 消失后该行没有出口，属 §9.5 未定义口径——**不擅自补语义**，登记为 R8。2026-10-23 裁 (a)「同批落 `cancelled`」后，施工 ④ 两个实现同批落地，不计 attempts。留这条不是为了纪念，是为了说明**为什么当时两侧一起红着也不补**：裁决前 PG 单方面支持会让「换实现行为不变」这条判据失效。
5. **`worker_id` 在回收后保留**：`pending` 行没有 owner，但留上一次 owner 便于审计；Claim 时覆盖。口径说明：§9.5 那列表把 `worker_id` 写成「当前 owner」，而 §9.2 的领取判据只用 `status` + `lease_until` 两列——这里按「租约才是派活判据，worker_id 是审计痕迹」解释，内存与 PG 两侧同形（PG 的 reap SQL 不碰该列），不是各写各的。
6. **`Kernels` 曾「只落库不消费」，施工 ④ 起被消费**（R9）：冻结的 `Claim` 没有内核参数，加参数 = MAJOR（会打红所有实现方与 S1 后的 contracts），把过滤移到编排层又让能力声明继续只是元数据。裁决走第三条：**可选新接口** `KernelAwareJobQueue`（§9.6「新接口 = MINOR」）。代价如实登记：装配方要自己做类型断言，且不提供该接口的实现会**静默退回不过滤**——所以两个实现都用编译期断言把能力钉住，不靠文档承诺。
7. **PG 的提交路径上一把 `pg_advisory_xact_lock`**（施工 ③）：「查幂等键 → 数 pending 深度 → 插一行」在内存里由单锁保证原子，PG 上不加锁就是两个并发提交各自看到「还没满」，`queue_full` 的 429 语义随之失效（用例 `ConcurrentQueueFullIsExact` 钉住：深度 1、8 个并发 → 恰好 1 接受 7 拒）。代价如实说：提交被串行化，但提交面本就低频（批量扫描是父记录一次提交 + 展开子行），换阈值精确是划算的。advisory lock 是**实例级**命名空间，键 `0x52554E51` 全仓唯一。
8. **时间戳由应用侧给，不用 DB 的 `now()`**（施工 ③）：`ReapExpired(ctx, now)` 的冻结签名本身就收 `now`，内存实现也用自己的时钟；混用会让一次调用里「租约是否过期」有两个答案。代价：跨 worker 进程时钟漂移直接移动租约边界 ⇒ 部署需同时钟源（NTP），已在「PG 侧口径」写明。
9. **`request_canonical` 与 `request_json` 存同一份内容两种形状**（施工 ③）：前者是 identity 的字节锚点、后者是查询投影。看起来像冗余存储，但红线 6「不存结果内容」管的是结果本体，不是请求身份；不这么做的替代方案是把 `request_json` 改成 TEXT（丢掉可查询性）或在读时复算 hash（实测会漂）。加列 = MINOR（§9.6）。
10. **PG 腿跨测试进程排队**（施工 ③）：`internal/testdb` 用会话级 advisory lock 把两个包的 PG 腿串行化。代价是本地跑全套 PG 腿约 3-4 秒；换来的是不再有「只有 `go test ./...` 才红、单跑永远绿」的假红。
11. **`offline` 的进入与退出都要有可执行的入口**（施工 ③ 原口径「不在套件里凑」，施工 ⑤ 收口）：施工 ③ 时 port 里没有 draining → offline 的入口，套件拒绝用「直接改库」凑覆盖（凑出来的绿比空白更容易骗人）。施工 ⑤ 用可选接口 `WorkerReaper` 补上了入口，所以那条断言现在走的是真 port；同时定死退出规则——`offline` 是**推断**，未请求过 drain 的可以被心跳 / 重注册拉回 `active`（并刷新心跳），请求过 drain 的不能（运维意图优先）。代价：`Claim` 对 offline 仍报 `ErrWorkerDraining`（冻结面只有一个「不配领新活」的哨兵，不新增错误语义），调用方要看报文里的实际状态。

**施工 ⑥（X5b：batch / walkforward 的规划与汇总）**

| 面 | 已落口径 | 未落 |
|---|---|---|
| batch 网格 | 参数名按字典序、最后一个参数变化最快；展开是**笛卡尔积**，每个点生成完整 RunSpec（`strategy_params` 浅合并，网格值覆盖同名 base 值，其余字段一字不改）；空网格 / 空取值 / 非标量 / 尾随内容 / 超过 100 个任务 → `invalid_params` | `/api/runs/batch` 端点、子 run 入队 |
| walkforward 窗口 | 交易日必须严格递增；只生成**完整**窗口（train/test 各按交易日数），`step_days` 必须为正；train/test 首尾相接不重叠；`min_train_days` 空 = `train_days/2`（固定窗口下恒成立）；不足一个完整窗口 / 日历违约 → `invalid_params` | 直连 data 服务的 `TradingCalendar` 只读 port（本层只收日历切片）、`/api/runs/walkforward` 端点 |
| 子 run 派生 | walkforward 每窗两条（train / test），只替换 `start_date` / `end_date`；kernel / fill_timing / seed / strategy_params / strategy_ref 原样保留（共享套件逐字段断言） | 子 run 入队与幂等键生成（关系列已落：`backtest_jobs.parent_job_id`） |
| 汇总 | 三值分开计数（`completed` / `invalid` / `failed`）+ `pending`；batch 必须传计划总数且与逐条回报数一致（漏报即拒），walkforward 缺段 / 缺窗算 pending；只对 `completed` 且给出有限 score 的子 run 排名（同分按 `run_id` 升序），`invalid` / `failed` 保留但 `rank=0`；walkforward 单段失败 = 整窗失败；父状态：还有未回报 → `running`，任一 failed → `failed`（**不吞**子失败），否则 `completed` | 结果内容解析 / 指标均值（本仓不碰结果内容；score 由执行侧给） |

**裁-13 落地（施工 ⑦）**：父记录不再等 R6——建 **一张** `run.parent_jobs`，子 run 用 `backtest_jobs.parent_job_id`（列 + 索引）归属父计划；`internal/parents` 的 `SubmitBatch` / `SubmitWalkForward` 提交即写父表，`ApplyBatchSummary` / `ApplyWalkForwardSummary` 复用 `internal/plan` 的汇总写回三值计数。`batch_runs` / `walk_forward_reports` 停写、保留为迁移期旧表。**仍未落**：batch / walkforward 的 4 个 HTTP 端点、子 run 入队、worker 执行。

**自检结论**（施工 ⑦ 后）：队列的语义面与 durable 面已落地（内存 + PG 同一套 qtest，45 条/实现；R10 的 PG 腿本机无库未跑，如实登记）；`internal/plan` 把 X5b 的**规划与汇总**做成了纯函数，`internal/plansuite` 的 8 条共享用例同跑两种父；父记录的身份 / 提交 / 汇总已落 `internal/parents` + 两个 `ParentStore` 实现。**仍未完成**：batch/walkforward 的 4 端点、子 run 入队、X0（`run_artifacts` 写入 + `/provenance`）、worker 领取执行的装配（本仓没有引擎，故 `ClaimByKernel` 目前没有生产消费方）、鉴权（R5）、以及 `pkg/backtest` 侧的迁移（属 core，见 `docs/TASKS.md` 验收对照）。判据是「队列可运行、父计划可提交 / 可汇总落库，但还不能从 HTTP 提交、也不能自己跑子 run」。
