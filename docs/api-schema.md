---
status: active
last-verified: 2026-11-09
verified-by: SVC-ORCH-G1 契约收编（claim / provenance / 父作业请求三类 wire 形状改引 `contracts/run`，本仓与 core 不再各持一份；全量 R5 仍未启用）；SVC-ORCH-G4（`/healthz` / `/readyz` 分工）；SVC-ORCH-G2（artifact 写端点 `POST /api/runs/jobs/{id}/artifacts` + R5 最小面 service 角色 + 新码 `artifact_conflict` / `artifact_not_found` / `auth_unavailable`）；2026-10-25 施工 ⑪（X5c worker 打活面）
---

# API Schema — tellurion-orchestration

> **实现状态（2026-10-24，施工 ③/⑥）**：`internal/queue` 已落 `JobQueue` / `WorkerRegistry` 两个 **Go port** + 内存实现 + `run` schema DDL **并已接 PG**（X5a/X5c + 施工 ③）：PG 实现在 `internal/pgstore`，与内存实现跑**同一套**语义用例（`internal/qtest`，每个实现 45 条 = 队列 30 + 注册表 9 + 跨 port 6），启动迁移在 `cmd/orchestrationservice`。`internal/httpapi` 已落**任务 4 端点**（`POST`/`GET list`/`GET {id}`/`DELETE {id}`，§1「任务」表）——端点行为按 §2 的错误码与幂等语义实现，哨兵 → wire code 的映射在 `internal/httpapi/api.go` 的 `classify`。**仍未实现**：全量鉴权（R5 归 `SVC-ORCH-G1` ⛔ 待契约形状）。**R5 最小面已落（2026-11-07，G2 ④）**：只保护 artifact 写端点（service 角色），其余端点仍无鉴权；`auth_unavailable` / `unauthenticated` / `forbidden` 只在受保护端点上产生。
> **X5b 父表（2026-10-25 施工 ⑦，裁-13）**：`run.parent_jobs` 与 `run.backtest_jobs.parent_job_id` 已落（内存 + PG 两个 `ParentStore` 实现），`internal/parents` 提供提交（写父表）与汇总（复用 `internal/plan` 写回三值计数）的服务层入口。
> **X5b 父作业端点（2026-10-25 施工 ⑧）**：`POST /api/runs/parents` 与 `GET /api/runs/parents/{id}` 已实现（`internal/httpapi/parents.go`）——batch / walkforward 共用一套父记录与身份（`plan_hash`），原「每类父各 POST + GET」的 4 个目标端点**不再作为目标契约**（从未实现，无调用方）。**子 run 入队已落（施工 ⑩）**：提交即写 `parent_job_id`；worker 执行仍未落。
> **X5c worker 打活面（2026-10-25 施工 ⑪）**：注册 / 心跳 / 领取 / 续租 / 上报结论 / 取消确认 + 运维面（列表 / drain）已在 `internal/httpapi/workers.go` 落地。**worker 进程不在本仓**（零 import core 是硬约束）：它跑 core 的引擎，经这组 HTTP 端点领活。子 run 上报结论后，服务端**立即重算并写回父汇总**（`parents.SyncSummary`）——这是 X5b「父汇总真实写回」的入口，不再依赖测试里的 SQL 模拟。
> 本文件是**目标契约**，不是现状说明书；开工顺序见 `docs/TASKS.md` 的「施工序」。
> **请求 / 响应形状、状态机、幂等键与 `run` schema 的目标键**见 [`ARCHITECTURE.md`](ARCHITECTURE.md) §9（M8 接口对表）。本文件只钉**对外行为**（方法、路径、错误码、幂等），不重复字段表。

> **契约地位**：HTTP。**提交即返回 job_id**（异步语义），进度靠轮询或事件。
> 本文件是**本仓对外接口的唯一真相源**。代码与它不一致时，先判断谁错：契约错 → 改本文 + 立 ADR；实现错 → 改代码。
> 跨仓消费方（core / agent / ui / 其他服务）按本文件编码，**不读本仓实现**。

## 1. 接口面

### 探针（公开，无鉴权）

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/healthz` | 进程在监听（`text/plain`：`ok`）；**不查任何依赖** |
| GET | `/readyz` | 能接活：PG 池可连 + `run.*` 六表已迁移；未就绪 `503` + 原因 |

两者都**不经过业务路由表**，也不在鉴权范围内（部署面必须无凭证可用）。`/healthz` 200 不代表服务可用；运维口径、失败语义与人工处置见 [`runbook.md`](runbook.md)。

### 任务

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/api/runs/jobs` | 提交单次回测（幂等键）→ `job_id` |
| GET | `/api/runs/jobs/{id}` | 查状态 / 摘要结果（含 `cancel_requested`） |
| DELETE | `/api/runs/jobs/{id}` | 取消 |
| GET | `/api/runs/jobs?status=&result_status=&limit=` | 列表 |
| POST | `/api/runs/jobs/{id}/lease` | worker 续租 |
| POST | `/api/runs/jobs/{id}/result` | worker 上报结论（completed / invalid / failed） |
| POST | `/api/runs/jobs/{id}/cancel-ack` | worker（owner）确认取消 |

### 批量 / 前向（父作业，X5b）

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/api/runs/parents` | 提交父计划（`kind = batch / walkforward`）→ `parent_job_id`；身份 = `plan_hash` |
| GET | `/api/runs/parents/{id}` | 三值汇总 + 逐子状态 + 汇总明细 |

**`POST /api/runs/parents` 请求形状**（`kind` 二选一，两个载荷同时出现 = `invalid_params`）：

```json
{
  "idempotency_key": "…",
  "kind": "batch",
  "batch": {
    "base_run_spec": { "…完整 RunSpec…": "…" },
    "grid": { "lookback": [10, 20], "threshold": [0.1, 0.2] }
  }
}
```

```json
{
  "idempotency_key": "…",
  "kind": "walkforward",
  "walkforward": {
    "base_run_spec": { "…完整 RunSpec…": "…" },
    "windows": { "train_days": 60, "test_days": 20, "step_days": 20, "min_train_days": 30 },
    "trading_days": ["2024-01-02", "2024-01-03", "…"]
  }
}
```

- `trading_days` **由调用方传入**（`YYYY-MM-DD`，严格递增）：本仓不自造交易日历，日历属 data 服务的只读 port（`dataflow.md`）；缺省 / 非法日期 → `invalid_params` 并点名到 `walkforward.trading_days[i]`。
- **提交即入队（施工 ⑩）**：父记录落库后，逐条把子 run 写进 `run.backtest_jobs` 并填 `parent_job_id`（batch = 每个网格点一条；walkforward = 每窗 `train` / `test` 各一条）。子 run 幂等键 = `parent:<parent_job_id>:<child_index>`（**stable**；改规则 = MAJOR），同一份计划重放（哪怕换幂等键）派生出同一批键。父记录与子 run 不在一个事务里：子 run 入队中途失败（如 429 `queue_full`）时父记录保留，**重放同一请求**按幂等键补齐即可，不会产生重复子 run。
- **父汇总真实写回（施工 ⑪）**：worker 每次经 `POST /api/runs/jobs/{id}/result` 或 `cancel-ack` 把一条子 run 写到终态，服务端就按计划顺序重算子 run 结论并写回 `run.parent_jobs`（`parents.SyncSummary`）。`summary` 里的 `Tasks`（batch）/ `Windows`（walkforward）顺序 = 计划顺序；没有 score 时 `rank=0`（本仓不解析结果内容，不猜分数）。子 run 齐全才写：少一条即报错，漏报不会被当成完成。
- 网格空 / 取值非标量 / 展开超过 100 / 窗口参数非法 / 区间不足 → `invalid_params`，报文点名字段（`grid`、`train_days`…）。
- 响应：新建 **202**，幂等重放 **200**，体为
  `{parent_job_id, kind, status, plan_hash, total, counts:{completed,invalid,failed,pending,settled}, idempotent_replay?}`。

**`GET /api/runs/parents/{id}` 响应形状**：

```json
{
  "parent_job_id": "…",
  "kind": "batch",
  "status": "running",
  "plan_hash": "…64 hex…",
  "total": 4,
  "counts": { "completed": 1, "invalid": 1, "failed": 1, "pending": 1, "settled": 3 },
  "children": [
    { "run_id": "…", "status": "completed", "result_status": "invalid", "run_spec_hash": "…64 hex…", "dataset_snapshot_hash": "…", "artifact_hash": "…", "created_at": "…" },
    { "run_id": "…", "status": "failed", "result_status": "failed", "error": "engine boom", "run_spec_hash": "…64 hex…", "created_at": "…" }
  ],
  "summary": { "…Aggregate* 的明细…": "…" },
  "created_at": "…",
  "completed_at": null
}
```

- 父 `status` 是生命周期（与 job 同六态），`children[].result_status` 才是 K9 结论；**父状态不塌缩**：`pending > 0` ⇒ `running`，任一子 `failed` ⇒ `failed`，`invalid` 不拖成 `failed`（`dataflow.md` §2 第 10 步）。
- `children` 由 `JobQueue` 的可选能力 `ParentChildLister` 提供；实现不支持时返回空数组（不伪造状态）。顺序为 `created_at` 升序 → `run_id` 升序。**施工 ⑩ 起 `children` 就是提交时入队的那批子 run**（batch = 网格点数；walkforward = 窗口数 × 2），不再通常为空。
- `children[]` 的三 hash（X0）：`run_spec_hash` 取自 `backtest_jobs.request_hash`，任何子 run 都有；`dataset_snapshot_hash` / `artifact_hash` 取自 `run_artifacts`，**只有留痕已写入时才有**（未跑完 / 从未产出的子省略这两个键，不伪造空值）。完整性判据仍由 `/provenance` 的 `provenance_missing` 报，父列表不制造第二个判据。

### 算力

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/api/runs/workers` | 注册 worker（幂等） |
| POST | `/api/runs/workers/{id}/heartbeat` | 心跳（存活证明） |
| POST | `/api/runs/workers/{id}/claim` | 按声明内核领取一条 run → `{job}` / `{job:null}` |
| POST | `/api/runs/workers/{id}/drain` | 摘除 worker（滚动升级） |
| GET | `/api/runs/workers` | worker 池状态 |

**worker 协议（X5c，施工 ⑪）**：

```json
// POST /api/runs/workers
{"worker_id": "host-pid-1700000000", "kernels": ["go"]}
// → 200 {"worker_id":"…","status":"active","kernels":["go"]}

// POST /api/runs/workers/{id}/claim   （无 body）
// → 200 {"job": {"run_id":"…","run_spec":{…完整 RunSpec…},"attempts":0,
//               "max_attempts":3,"lease_until":"…","cancel_requested":false,
//               "parent_job_id":"…"}}
// → 200 {"job": null}   无活（不是 404）

// POST /api/runs/jobs/{id}/result
{"worker_id":"…","status":"completed","result_status":"completed|invalid","result_uri":"backtest:…"}
{"worker_id":"…","status":"failed","result_status":"failed","error":"engine boom"}
```

- **`kernels` 空数组归一成 NULL**：空数组在 PG 的 `= ANY('{}')` 里是「什么都不能干」，而「未声明能力」的语义是**不过滤**。空名字（`""`）→ 400 `invalid_params`。
- **领取只认已注册的 worker**（未知 → 404 `worker_not_found`）；`draining` / `offline` → 409 `worker_draining`。
- **声明了 `kernels` 就必须按内核过滤**：队列实现不支持 `KernelAwareJobQueue` 时 → 503 `kernel_filter_unavailable`，**不**静默退回不过滤（那会把 nautilus 的活派给 go worker）。
- **租约由服务端决定**（部署参数 `-worker-lease`，默认 60s）；worker 在到期前续租（`POST /jobs/{id}/lease`）：不是 owner / 已过期 / 已终态 → 409 `lease_lost`，worker 必须立即停止写入。
- **`result` 写完后服务端立即重算父汇总**：`status=completed` 要求 `result_status ∈ {completed, invalid}` 且 `result_uri` 非空；`failed` 要求 `error` 非空。父汇总写不进去 → 500 `parent_summary_failed`（子 run 状态已写成功，重放同一上报即可，终态 + 同载荷是幂等的）。
- **取消**：`cancel_requested` 可经 `GET /jobs/{id}` 或 claim 响应读到；worker 中止后调 `cancel-ack`（owner 才能确认），服务端随后把该子 run 计入父汇总（`cancelled` 按 failed 计——它是终态但不是 K9 结论，不映射会让父记录永远停在 running）。
- `GET /api/runs/workers` 的 `kernels` 恒为数组（未声明 = `[]`）；`GET /api/runs/jobs/{id}` 新增 `cancel_requested`（加字段 = MINOR）。

### 结果坐标（X0）

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/api/runs/jobs/{id}/artifacts` | **X0 生产者**：core worker 回传三 hash（唯一写路径，**不直写库**）；R5 最小面 = service 角色 |
| GET | `/api/runs/{id}/provenance` | 三 hash + 引擎名 + seed |

**`GET /api/runs/{id}/provenance` 响应形状**（`ProvenanceResponse`，独立 DTO，**不扩**冻结的 `BacktestResponse`）：

```json
{
  "run_id": "…",
  "run_spec_hash": "…64 hex…",
  "dataset_snapshot_hash": "…",
  "risk_rules_hash": "",
  "strategy_ref": "momentum@deadbeef",
  "artifact_uri": "…",
  "artifact_hash": "…",
  "version_hash": "…",
  "params_hash": "…",
  "seed": 7,
  "kernel": "go",
  "engine_version": "engine-1.0",
  "fill_timing": "next_open",
  "result_status": "invalid",
  "created_at": "…"
}
```

- `result_status` **只从 `backtest_jobs` 取**（`run_artifacts` 没有 status 列）；`completed` / `invalid` 是跑完的两个结论，响应里都如实呈现——`invalid` **不是** `failed`。
- `artifact_uri` / `artifact_hash` / `version_hash` / `seed` 无值时出 **`null`**（内置策略没有独立 artifact；不省略键，读侧不用猜「没有」与「空串」的区别）。
- `risk_rules_hash` 空串 = **未登记**（QT-42 X3b 之前），不是「无规则」。
- 错误：非终态 → **409 `provenance_not_ready`**；终态 `failed` / `cancelled` 且无留痕 → **404 `provenance_not_found`**；终态 `completed` / `invalid` 却查不到 `run_artifacts` → **500 `provenance_missing`**（数据完整性，不得当成功使用）；未知 run → **404 `job_not_found`**。
- 三 hash 是 run **开始后**的产物，**不是入队门槛**（§2 fail-closed ④）。写入路径 = `queue.ArtifactStore`（内存 + PG 两个实现）；本仓没有 worker 进程，生产者随 worker 落地接入。

**`POST /api/runs/jobs/{id}/artifacts` 请求 / 响应（2026-11-07，SVC-ORCH-G2 ③）**：

- 请求体 = **本地 DTO** `queue.ArtifactRecord`（`run_id` 可省略；出现且与路径不一致 = `400 invalid_params`）。字段形状见 [`ARCHITECTURE.md`](ARCHITECTURE.md) §9.5 与 `internal/queue/artifacts.go`；**契约化**归 `SVC-ORCH-G1` ⛔ / `tellurion-contracts:CT-04`，今天按本地形状对接，不承诺跨仓冻结。
- `created_at` 由服务端盖戳（请求体里的值被忽略）——它是留痕时间，不是调用方可填的身份。
- 响应：首次 `201` + 落库行；同身份重放 `200` + **原行**（`created_at` 不刷新）；身份漂移 `409 artifact_conflict`（原行不被覆盖）；run 不存在 `404 job_not_found`；未装配鉴权闸 `503 auth_unavailable`；无凭证 `401`；非 service 角色 `403`。
- 幂等与冲突判定**只在 `ArtifactStore` 一处**：HTTP 层先查 `GetArtifact` 只为选 201 / 200，最终以 `SaveArtifact` 的 `ON CONFLICT … WHERE 身份全等` 为准。

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
| `idempotency_conflict` | 409 | 同键 + 不同 `request_hash`（job）/ 不同 `plan_hash`（父作业） | 换键或确认意图；**不得**静默复用旧 job |
| `job_not_found` | 404 | `GET`/`DELETE` 未知 run id | 不重试 |
| `parent_not_found` | 404 | 未知 `parent_job_id`（batch / walkforward 共用） | 不重试 |
| `worker_not_found` | 404 | 心跳 / 领取 / `drain` 未知 worker | 不重试 |
| `worker_draining` | 409 | `draining` / `offline` worker 领取新活 | 停止领取；等 `offline` 回收或换 worker |
| `lease_lost` | 409 | 续租 / 上报时租约已过期、已被回收或行已终态 | **立即停止**；不得再写结果（写不进去） |
| `kernel_filter_unavailable` | 503 | worker 声明了 `kernels`，但队列实现不支持按内核过滤 | 不重试领取；换部署 / 修装配（**不**静默不过滤） |
| `parent_summary_failed` | 500 | 子 run 状态已写，但父汇总重算 / 写回失败 | 幂等重放同一上报（终态 + 同载荷是 no-op） |
| `invalid_transition` | 409 | 对终态 job / worker 做非法迁移（如 terminal job 再 `Complete`、`offline` worker 再 drain） | 读当前状态再决定 |
| `provenance_not_ready` | 409 | job 非终态，provenance 尚未写 | 退避轮询 |
| `provenance_not_found` | 404 | job 终态 `failed` / `cancelled` 且从未写 provenance（如路由拒绝 / 从未跑到写留痕那一步） | 读 job 的 `error`；不重试读取 |
| `provenance_missing` | 500 | 终态为 `completed` / `invalid` 却查不到 `run_artifacts`（数据完整性破坏） | 告警；**不得**当成功使用 |
| `queue_full` | 429 | 待领取深度达上限（含父作业逐条入队子 run 时） | 退避重试；父记录保留，重放同一请求按幂等键补齐；**不 silently 丢弃** |
| `artifact_conflict` | 409 | artifact 写：同 `run_id` 但身份字段（`strategy_ref` / 三 hash / kernel / engine_version / fill_timing）不同 | 不重试；确认上游是否换了身份，换身份应换 run |
| `artifact_not_found` | 404 | 读路径查不到 artifact 行（写路径首次写入**不**返回本码） | 先写 artifact；不重试读取 |
| `auth_unavailable` | 503 | 受保护路由未装配鉴权闸（装配错误，非调用方问题） | 联系运维；不重试业务 |

**非错误语义**：同幂等键重复提交 → **200 + 原 `job_id` + `idempotent_replay:true`**（视为成功，不重复跑）；取消 `running` → **202 + `cancelling`**（轮询确认终态）；取消 `pending` → **200 + `cancelled`**；取消终态 → 200 原状态（幂等）。

> **「没有 worker」不是提交错误**：任务照常入队（202，durable），`GET /api/runs/workers` 返回空列表供运维扩容。旧契约里的 `no_worker` 503 会让调用方误以为没入队而重试/放弃——**已从错误码集移除**（2026-10-22 对表裁决）。真正需要 worker 的同步面在本仓不存在（红线：所有 run 异步）。

**fail-closed 四条**：
1. **未注册内核 / 未登记 market** → 提交期拒绝（`invalid_params`），job 行都不落（`EngineRegistry` / `MarketRouter` 的现有语义，零静默回退）。
2. **幂等键缺失** → 拒绝；服务端**不**从请求内容派生**调用方提交的**幂等键（派生会把两个不同意图静默合并）。子 run 的键是另一回事：由父记录 id + 计划序号派生（施工 ⑩，见 §3）。
3. **`request_hash` 算不出**（canonical 编码失败）→ 拒绝。
4. **三 hash 不是入队门槛**：它们是 run 开始后才算出的**产物**（QT-63）。入队只校验三份引用可解析；终态 `result_status ∈ {completed, invalid}` 必须能在 `/provenance` 取到三 hash，取不到 → `provenance_missing`（数据完整性，不是正常路径）。

## 3. 幂等性与重试

- **提交类端点**（`POST /jobs`、`POST /parents`）**必须**带客户端提供的 `idempotency_key`；服务端**不派生**（见 §2 fail-closed ②）。
- job：同键 + 同 `request_hash` → 返回**原 id**（200，`idempotent_replay:true`）；同键 + 不同 `request_hash` → 409 `idempotency_conflict`。`request_hash = sha256(canonical request_json)`，canonical 规则随 RunSpec（QT-03）冻结，改规则 = MAJOR。
- 父作业：身份是 **`plan_hash = sha256(plan_canonical)`**，不是幂等键——**同一份计划（即使换了 `idempotency_key`）返回同一 `parent_job_id`**（200，`idempotent_replay:true`）；同键但计划不同 → 409 `idempotency_conflict`。数据库侧由 `parent_jobs(plan_hash)` 唯一索引兜底（`db-schema.md` §5）。
- 子 run（施工 ⑩）：幂等键 = `parent:<parent_job_id>:<child_index>`（**stable**；改规则 = MAJOR）。同一计划重放派生出同一批键，因此「父行已写、子 run 未入完」可以靠重放同一 POST 补齐：父行按 `plan_hash` 返回原行，子 run 按键逐条命中原行。入队中途遇 429 `queue_full` 时**父记录不回滚**（两步没有跨表事务），调用方退避后重放即可。
- **取消幂等**：已取消再取消 → 同终态；`pending` 直接 `cancelled`，`running` 先 `cancelling` 再由 worker 确认。
- **`drain` 幂等**：重复 drain → `nil`；`offline` 再 drain → `invalid_transition`。
- **租约与重试**：`running` 的租约过期 → 回 `pending` 且 `attempts+1`；超过 `max_attempts` → `failed`（`attempts_exhausted`）。租约丢失的 worker **禁止**再写结果（`ErrLeaseLost`），否则两个 worker 会各写一份终态。
- 三 hash 相同 + 参数相同 → 可复用已完成结果（可选优化，但必须由 hash 决定，不能猜）；复用不改变「每次提交仍是一个有身份的 run 行」这一审计事实。
- **artifact 写（2026-11-07，G2 ③）**：身份 = `run_artifacts` 的身份字段全集（`db-schema.md` §1 + `ARCHITECTURE.md` §9.5，13 项）。首次 → `201`；同身份重放 → `200` + 原行且 **`created_at` 不刷新**；同 `run_id` 不同身份 → `409 artifact_conflict` 且原行不被覆盖。判定唯一入口是 `ArtifactStore.SaveArtifact`（PG `ON CONFLICT (run_id) DO UPDATE … WHERE 身份全等`；0 行受影响 = 冲突）。HTTP 层的 `GetArtifact` 只用于选 201/200，不参与冲突判定。

## 4. 版本与兼容

- **状态机**：job 的 `status` 是队列生命周期，K9 `result_status` 是结果结论（两轴，不合并）。新增**非终态** = MINOR（旧客户端按"未完成"处理）；新增**终态** = MAJOR（旧客户端可能永远等待）；改终态语义 = MAJOR。`result_status` 只允许 K9 三值，第四值 = 契约变更（SPEC 评审）。
- **三 hash 的算法变更** = MAJOR（历史 run 的坐标会变；`run_artifacts` 是冻结面）。
- **端点只增不删**；取消从「`pending` 删行」改「取消保留」是语义变更 = MAJOR，必须给旧消费方切换窗口（`ARCHITECTURE.md` §9.7 R1）。
- **X5c worker 端点（施工 ⑪）**：新增端点 / 新增 wire code / `GET /jobs/{id}` 加 `cancel_requested` 都是 **MINOR**（旧客户端不读新键即可）；`kernels` 空数组归一成 NULL 是**修复**既有实现的语义，不改变 wire 形状。
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

## 6. 鉴权现状（R5 最小面，2026-11-07）

- **代码登记**：`internal/httpapi/policy.go` 的 `AuthStatus = "write-endpoint-only"` 与策略表（只有一条：`POST /api/runs/jobs/{id}/artifacts` → `service` 角色）。
- **只保护写端点**的原因：这是本仓唯一「机器代外部写入 `run.*`」的入口（裁决 ⑱），不能裸奔。其余端点的全量鉴权归 `SVC-ORCH-G1`（🔶 2026-11-09：契约形状已收编，全量 R5 尚未启用——调用方还没注入 service token，单侧启用会断链），今天仍无鉴权——**不是遗漏，是已登记的欠账**。
- **wire 形状归 contracts（2026-11-09）**：claim / provenance / 父作业提交三类形状已统一为 `contracts/run`（`ClaimView` / `ProvenanceView` / `ParentSubmitRequest` 族），本仓与 core 不再各写一份；§1 的字段表仍是对外行为描述，形状的唯一 Go 实现在契约包。
- **fail-closed**：受保护路由未装配 Authorizer → `503 auth_unavailable`；无凭证 → `401 unauthenticated`；非 service → `403 forbidden`。启动闸同 core：有 `JWT_SECRET` 启用；无 secret + 回环 = 开放；无 secret + 非回环 = 拒绝启动。
- **service token 的签发 / 分发不在本仓**：归 `tellurion:QT-77`；本仓只校验。
