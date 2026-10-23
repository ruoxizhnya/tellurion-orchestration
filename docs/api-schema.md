---
status: active
last-verified: 2026-10-23
verified-by: X5a/X5c 落地 + 文档同步
---

# API Schema — tellurion-orchestration

> **实现状态（2026-10-23）**：`internal/queue` 已落 `JobQueue` / `WorkerRegistry` 两个 **Go port** + 内存实现 + `run` schema DDL（X5a/X5c），但**没有任何 HTTP handler**——下列端点的传输层**仍未实现**。port 的方法级错误语义见 [`ARCHITECTURE.md`](ARCHITECTURE.md) §9.2，实现现状见 §9.8。
> 本文件是**目标契约**，不是现状说明书；开工顺序见 `docs/TASKS.md` 的「施工序」。
> **请求 / 响应形状、状态机、幂等键与 `run` schema 的目标键**见 [`ARCHITECTURE.md`](ARCHITECTURE.md) §9（M8 接口对表）。本文件只钉**对外行为**（方法、路径、错误码、幂等），不重复字段表。

> **契约地位**：HTTP。**提交即返回 job_id**（异步语义），进度靠轮询或事件。
> 本文件是**本仓对外接口的唯一真相源**。代码与它不一致时，先判断谁错：契约错 → 改本文 + 立 ADR；实现错 → 改代码。
> 跨仓消费方（core / agent / ui / 其他服务）按本文件编码，**不读本仓实现**。

## 1. 接口面

### 任务

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/api/runs/jobs` | 提交单次回测（幂等键）→ `job_id` |
| GET | `/api/runs/jobs/{id}` | 查状态 / 摘要结果 |
| DELETE | `/api/runs/jobs/{id}` | 取消 |
| GET | `/api/runs/jobs?status=&result_status=&limit=` | 列表 |

### 批量 / 前向

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/api/runs/batch` | 批量扫描（参数网格）→ `batch_id` |
| GET | `/api/runs/batch/{id}` | 汇总 + Scorer 排序 |
| POST | `/api/runs/walkforward` | 前向验证（窗口配置）→ `wf_id` |
| GET | `/api/runs/walkforward/{id}` | 各窗口报告 |

### 算力

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/api/runs/workers` | worker 池状态 |
| POST | `/api/runs/workers/{id}/drain` | 摘除 worker（滚动升级） |

### 结果坐标（X0）

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/api/runs/{id}/provenance` | 三 hash + 引擎名 + seed |

### port（给内核用，不是 HTTP）

`JobQueue` / `WorkerRegistry` 的**方法集、签名、错误语义**见 [`ARCHITECTURE.md`](ARCHITECTURE.md) §9.2；本文件不重复，避免两份签名漂移。

> ⚠️ **`RunStateStore` 已作废**（2026-10-21 H5 裁决）：`strategy_state` 归 strategy-runtime 模块，core 写 `quant.strategy_state`，**不迁 `run`**、不经本仓 port。这里曾经写过 `Save/Load` 两方法，是旧方案的残留；不要再按它编码。

## 2. 错误码与 fail-closed 语义

> **状态两轴**（`ARCHITECTURE.md` §9.1 F8）：`status` 是**队列生命周期**（`pending|running|cancelling|completed|failed|cancelled`），`result_status` 是 **K9 三值**（`completed|invalid|failed`）。`invalid` **不是** HTTP 错误——它是跑完的结果结论（`result_status=invalid`，`error` 为空）。

| code | HTTP | 触发 | 调用方行为 |
|---|---|---|---|
| `unauthenticated` | 401 | 缺凭证 / 凭证失效 | 重新认证，不重试业务 |
| `forbidden` | 403 | 有身份无权限（含非运维调 `drain`） | 不重试；找授权 |
| `invalid_params` | 400 | 请求体 / 区间 / 窗口参数 / 未知枚举值 | 修请求，原样重试无意义 |
| `missing_idempotency_key` | 400 | 提交类端点缺 `idempotency_key` | 补键重试；**不**由服务端代生成 |
| `idempotency_conflict` | 409 | 同键 + 不同 `request_hash` | 换键或确认意图；**不得**静默复用旧 job |
| `job_not_found` | 404 | `GET`/`DELETE` 未知 run id | 不重试 |
| `batch_not_found` | 404 | 未知 `batch_id` | 不重试 |
| `walkforward_not_found` | 404 | 未知 `wf_id` | 不重试 |
| `worker_not_found` | 404 | `drain` 未知 worker | 不重试 |
| `invalid_transition` | 409 | 对终态 job / worker 做非法迁移（如 terminal job 再 `Complete`、`offline` worker 再 drain） | 读当前状态再决定 |
| `provenance_not_ready` | 409 | job 非终态，provenance 尚未写 | 退避轮询 |
| `provenance_not_found` | 404 | job 终态 `failed` 且从未写 provenance（如路由拒绝） | 读 job 的 `error`；不重试读取 |
| `provenance_missing` | 500 | 终态为 `completed` / `invalid` 却查不到 `run_artifacts`（数据完整性破坏） | 告警；**不得**当成功使用 |
| `queue_full` | 429 | 待领取深度达上限 | 退避重试，**不 silently 丢弃** |

**非错误语义**：同幂等键重复提交 → **200 + 原 `job_id` + `idempotent_replay:true`**（视为成功，不重复跑）；取消 `running` → **202 + `cancelling`**（轮询确认终态）；取消 `pending` → **200 + `cancelled`**；取消终态 → 200 原状态（幂等）。

> **「没有 worker」不是提交错误**：任务照常入队（202，durable），`GET /api/runs/workers` 返回空列表供运维扩容。旧契约里的 `no_worker` 503 会让调用方误以为没入队而重试/放弃——**已从错误码集移除**（2026-10-22 对表裁决）。真正需要 worker 的同步面在本仓不存在（红线：所有 run 异步）。

**fail-closed 四条**：
1. **未注册内核 / 未登记 market** → 提交期拒绝（`invalid_params`），job 行都不落（`EngineRegistry` / `MarketRouter` 的现有语义，零静默回退）。
2. **幂等键缺失** → 拒绝；服务端**不**从请求内容派生键（派生会把两个不同意图静默合并）。
3. **`request_hash` 算不出**（canonical 编码失败）→ 拒绝。
4. **三 hash 不是入队门槛**：它们是 run 开始后才算出的**产物**（QT-63）。入队只校验三份引用可解析；终态 `result_status ∈ {completed, invalid}` 必须能在 `/provenance` 取到三 hash，取不到 → `provenance_missing`（数据完整性，不是正常路径）。

## 3. 幂等性与重试

- **提交类端点**（`POST /jobs`、`POST /batch`、`POST /walkforward`）**必须**带客户端提供的 `idempotency_key`；服务端**不派生**（见 §2 fail-closed ②）。
- 同键 + 同 `request_hash` → 返回**原 id**（200，`idempotent_replay:true`）；同键 + 不同 `request_hash` → 409 `idempotency_conflict`。`request_hash = sha256(canonical request_json)`，canonical 规则随 RunSpec（QT-03）冻结，改规则 = MAJOR。
- **取消幂等**：已取消再取消 → 同终态；`pending` 直接 `cancelled`，`running` 先 `cancelling` 再由 worker 确认。
- **`drain` 幂等**：重复 drain → `nil`；`offline` 再 drain → `invalid_transition`。
- **租约与重试**：`running` 的租约过期 → 回 `pending` 且 `attempts+1`；超过 `max_attempts` → `failed`（`attempts_exhausted`）。租约丢失的 worker **禁止**再写结果（`ErrLeaseLost`），否则两个 worker 会各写一份终态。
- 三 hash 相同 + 参数相同 → 可复用已完成结果（可选优化，但必须由 hash 决定，不能猜）；复用不改变「每次提交仍是一个有身份的 run 行」这一审计事实。

## 4. 版本与兼容

- **状态机**：job 的 `status` 是队列生命周期，K9 `result_status` 是结果结论（两轴，不合并）。新增**非终态** = MINOR（旧客户端按"未完成"处理）；新增**终态** = MAJOR（旧客户端可能永远等待）；改终态语义 = MAJOR。`result_status` 只允许 K9 三值，第四值 = 契约变更（SPEC 评审）。
- **三 hash 的算法变更** = MAJOR（历史 run 的坐标会变；`run_artifacts` 是冻结面）。
- **端点只增不删**；取消从「`pending` 删行」改「取消保留」是语义变更 = MAJOR，必须给旧消费方切换窗口（`ARCHITECTURE.md` §9.7 R1）。
- 详细版本矩阵见 [`ARCHITECTURE.md`](ARCHITECTURE.md) §9.6；不在本文件重复。

## 5. 不提供什么（边界）

- 不做策略执行（引擎在 core）
- 不做结果分析（那是 analytics）
- 不做数据准备（那是 data）
- 不做风控裁决（那是 risk / core）
- 不做调度 UI（ui 自己画）
- **不存结果内容**：`run` 只存 `result_uri` 指针 + 三 hash；结果明细归 core 的 state store / artifact 服务（`ARCHITECTURE.md` §9.7 R2）。
- **不接管 core 的旧端点**：`/api/backtest`、`/api/batch`、`/api/walkforward` 在 O0 切换前仍归 core；本仓的 `/api/runs/*` 是目标面，不反向代理旧路径（双真相见 §9.7 R1）。
- **不解析 `backtest_jobs.result` 的错误串**、不重算 `status`：K9 的 `status` 由引擎产出，本仓原样透传。
