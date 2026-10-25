---
status: active
last-verified: 2026-10-25
verified-by: 施工 ⑨（X0 `run.run_artifacts` 写入语义落地：`queue.ArtifactStore` 内存 / PG 两个实现，`ON CONFLICT (run_id) DO UPDATE … WHERE <13 项身份全等>`，0 行受影响 = 冲突；生产者未落）
---

# DB Schema — tellurion-orchestration

> **所有权声明**：本仓**拥有 `run` schema 的 DDL**：任务队列、批量批次、前向验证报告、三 hash 留痕、worker 注册。是这组表的唯一写者。
> **运行状态检查点 `strategy_state` 归 strategy-runtime 模块（core 写 `quant.strategy_state`），不属本仓**（2026-10-21 裁决，指 `tellurion:docs/design/system-spec-review-2026-10-21.md` H5）。
>
> **`run` 是任务域，不是交易域**：里面**没有**订单 / 成交 / 持仓 / 资金——那些归 core 的 `quant`（实盘运行域）。
> 判据：`run` 里的行描述"某个任务跑到哪了"，`quant` 里的行描述"钱和仓位发生了什么"。两边的写入时机、事务边界、审计要求都不同，混在一起就无法拆分。
> 上级依据：`tellurion:docs/design/qt-decoupling/data-service-contract-and-ownership.md` §1 —— **DDL 只有一个 owner**，同一个 PG 实例、每服务一个 schema，将来拆库是"按 schema 搬走"而不是重写。
> 本文件是本仓表结构的**唯一真相源**：代码里的建表语句与它不一致时，**改代码**；要改表结构，先改本文件（结构级变更还要立 ADR）。

## 1. 归属表

| 表 | 内容 | 现状 |
|---|---|---|
| `run.backtest_jobs` | 任务队列（生命周期六态 + K9 `result_status` 两轴，见 [`ARCHITECTURE.md`](ARCHITECTURE.md) §9.5）；`parent_job_id` 指向父计划（可空，子 run 关系） | DDL 已落并**已接 PG**（`internal/pgstore` 的 `JobQueue` 实现 + 启动迁移） |
| `run.parent_jobs` | **父计划（batch / walkforward 共用一张表）**：`kind` + `plan_canonical` / `plan_hash`（唯一，计划身份）+ 三值汇总 + 父状态（裁-13） | DDL 已落并已接 PG（`internal/pgstore.Parents`）；提交 / 汇总写入已落 `internal/parents`；`POST/GET /api/runs/parents` 已落（施工 ⑧）；**子 run 入队已落（施工 ⑩，`parent_job_id` 由 `internal/parents` 写入）** |
| `run.walk_forward_reports` | **迁移期旧表**：前向验证报告（被 `parent_jobs` 取代，停写） | DDL 保留只前滚；不再写入，O1 对拍后删除 |
| `run.batch_runs` | **迁移期旧表**：批量扫描批次（被 `parent_jobs` 取代，停写） | DDL 保留只前滚；不再写入，O1 对拍后删除 |
| `run.run_artifacts` | 每次 run 的三 hash（`strategy_ref` / `dataset_snapshot_hash` / `risk_rules_hash`）+ artifact 指针 | DDL 已落（FK 生效已实测）；**写入 / 读已落两个实现**（`queue.ArtifactStore`：内存 + `pgstore.Artifacts`，施工 ⑨）与 `GET /api/runs/{id}/provenance`；**生产者（worker 进程）未落**——本仓没有引擎；🔒 冻结面，变更须走评审 |
| `run.worker_registry` | worker 注册与心跳（active / draining / offline） | DDL 已落并**已接 PG**（`internal/pgstore` 的 `WorkerRegistry` 实现）；心跳超时 → `offline` 由可选接口 `WorkerReaper.ReapOffline` 写入（施工 ⑤） |
| `quant.strategy_state`（不属本仓） | 运行状态检查点（断点续跑） | 归 strategy-runtime 模块 · core 写（2026-10-21 裁决，指 `tellurion:docs/design/system-spec-review-2026-10-21.md` H5） |

> 🔒 **冻结标注**：`run_artifacts` 的三 hash 与 artifact 指针属 **ResultSpec 冻结 schema 面**（`tellurion:docs/SPEC.md` K9「ResultSpec 冻结」；内容寻址三兄弟见 `tellurion-contracts:docs/api-schema.md` §3）——增列 / 改口径 / 改 hash 算法都算改冻结面，**须走评审**（`tellurion:docs/TASKS.md`「维护本文件」的冻结 schema 变更评审硬规则），不得顺手改。

**归属修正**（ADR-D19 已登记）：`backtest_jobs` 目前在内核侧持久化，**迁到 `run`**，内核走 port。~~`strategy_state` 一并迁 `run`~~ **（⚠️ 2026-10-21 H5 裁决作废：`strategy_state` 归 strategy-runtime 模块，core 继续写 `quant.strategy_state`，不迁 `run`，见 `tellurion:docs/design/system-spec-review-2026-10-21.md` H5）**。

> ⚠️ **2026-10-21 订正（H5 裁决）**：原「这条迁移给 core 留了一笔欠账：core 需要新增 `RunStateStore` port（`Save(ctx, runID, state)` / `Load(ctx, runID)`），实现在本仓；不补这个 port，X5 一动 core 就编不过——U3 是本仓施工序里 `X5a` 的前置」**作废**——该欠账（repo-inventory **U3**）以「`strategy_state` 迁 `run`」为前提，随裁决一并作废：`strategy_state` 留 core（strategy-runtime 模块，core 写 `quant.strategy_state`），不需要 `RunStateStore` port，`X5a` 不再以 U3 为前置。

## 2. 只读输入（不属于本仓，禁止写入）

| 数据 | 真实 owner |
|---|---|
| 回测结果明细 | core（引擎产出） |
| 策略定义与 artifact | `tellurion-strategy` |
| 数据集与 hash | `tellurion-data` |
| 规则快照 | `tellurion-risk` |

本仓只登记"这次 run 用了哪三份东西"，**不复制它们的内容**。

## 3. 红线

1. **只派活不裁决**：编排层不解释结果、不改结果、不重算指标。
2. **提交异步**：所有 run 走 job 队列，没有"同步跑完再返回"的端点。
3. **三 hash 必留**：任何**终态 `completed` / `invalid`** 的 run 必须能回答"哪份策略 / 数据 / 规则"。⚠️ 这不是**入队**门槛——三 hash 是 run 开始后才算出的产物（QT-63）；入队只校验三份引用可解析（`ARCHITECTURE.md` §9.1 F8）。
4. **worker 摘除要优雅**：`drain` 后不再接新活，跑完手上的再下线。
5. **内核无持久化状态**是目标态：任务状态写 `run`，内核走 port。⚠️ 例外已裁（2026-10-21 H5）：`strategy_state` 归 strategy-runtime 模块（K0 冻结契约），core 写 `quant.strategy_state`，不迁 `run`。
6. **`run` 不存结果内容**：只存 `result_uri` 指针 + 三 hash；结果明细归 core 的 state store / artifact 服务（`ARCHITECTURE.md` §9.7 R2）。
7. **两轴状态不合并**：`status` 是队列生命周期（`pending|running|cancelling|completed|failed|cancelled`），`result_status` 是 K9 三值（`completed|invalid|failed`）。`invalid` **不得**写成 `failed`，`cancelled` 不得删行。

## 4. 建表与迁移

**现状（必须先知道）**：core 的迁移是「**启动时按序执行一组幂等 DDL**」——唯一真相是 `pkg/storage/postgres.go` 的 `migrationStatements()` 数组（`migrate()` 在服务启动时逐条跑）；**没有版本表、没有 down、没有执行记录**（有意取舍，理由见 core 的 `migrations/README.md`）。`migrations/`（14 个）与 `docs/migrations/`（25 个）里的 `.sql` 是**历史镜像，不执行**——改它们不会改库。

**本仓约定**（从建仓第一天执行）：

- 迁移文件只放 `migrations/`，命名 `<序号>_<动作>.sql`，**本仓内**递增（不与别的仓共享序列，也不复用号）。
- **执行机制随第一个建表单定，二选一（不能"都不选"）**：
  1. **core 式**：Go 里的 `migrationStatements()` 数组 + 启动执行，幂等、无版本表——与 core 一致、运维零新增；
  2. **版本化**：引入迁移库 + `schema_migrations` 表（可 down、有执行记录）——服务变多后更稳，但要一次性 baseline。
  **取向**：X1~X3 期间优先 ①（少一层依赖），服务数 ≥3 或出现"必须先降级"的需求时重估 ②。
  **落定（2026-10-23，施工 ①）：取取向 ①**。DDL 的唯一真相是 `internal/queue/ddl.go` 的 `migrations` 切片（`Migrations()` 只返副本）；本仓**不建** `migrations/*.sql` 可执行件——上面「迁移文件只放 `migrations/`」那条在本仓按此口径解释为「若将来改取向 ② 才启用」。切换取向 = 结构级变更，须走 ADR。
  **执行入口落地（2026-10-23，施工 ③）**：`pgstore.Store.Migrate(ctx)` 按序执行该切片，`cmd/orchestrationservice` 在**监听之前**调用它；每条语句幂等（`IF NOT EXISTS`），可反复执行（测试里真跑两遍钉住）。首次真库落地时改了两处 DDL 原文（`request_canonical` 列 + 领取索引形状），因为当时**任何库里都还没有这套表**——追加 ALTER 是给已落库的语句用的。
- **只前滚**：已合并的迁移不改；修错靠新增一条。
- 每条必须可重入（`IF NOT EXISTS` / `IF EXISTS`），且能在空库上一次跑完。
- 跨 schema 取数只走**只读视图或 API**；出现 `join` 别人的表就是边界腐化的第一信号。
- 结构变更的同步义务：本文件 + `docs/ARCHITECTURE.md` 的「数据归属」节必须同一次提交里更新。

## 5. 目标键与迁移（M8 接口对表，2026-10-22）

> **本节只钉「表要回答什么、键怎么定」，列清单与类型在 [`ARCHITECTURE.md`](ARCHITECTURE.md) §9.5**（单一处维护，避免两份漂移）。落地前**不改库**；O0 按 §4 的机制建表。
>
> **现状（2026-10-23，施工 ③）**：五张表的 DDL 已按 §4 取向 ① 落在 `internal/queue/ddl.go`（含 7 条索引），逐条可重入，并**已在真库执行**（`pgstore.Migrate`，测试库 `tellurion_orchestration_test`）。列与 §9.5 的对应由 `internal/pgstore` 两条腿钉住：`TestMigrateIsIdempotentAndCreatesTheWholeSchema`（表数 / 索引形状）与 `TestJobColumnsMatchTheTableShape`（port 读的列 ⇄ 库有的列，双向）。

| 表 | 目标主键 / 唯一键 | 关键语义 |
|---|---|---|
| `run.backtest_jobs` | `id`；`UNIQUE(idempotency_key) WHERE NOT NULL`；索引 `(priority DESC, created_at ASC, id ASC) WHERE status='pending'` | `request_json` 是完整 canonical RunSpec 的 **JSONB 投影**（可查询），`request_canonical` 是同一份内容的 **TEXT 字节锚点**——`request_hash` 恒等于 `sha256(request_canonical)`，读路径回后者（JSONB 会把 `1e-05` 规范化成 `0.00001`，拿它复算 hash 会把同一个幂等键判成两次不同请求；实测钉子 `internal/pgstore/hashanchor_test.go`）；`status` 六态；`result_status` K9 三值可空；`worker_id` / `lease_until` / `attempts` / `cancel_requested` 支撑多 worker；`result_uri` 是结果指针；**目标无 `result` 内容列**（迁移期旧列保留只读，切读后删除） |
| `run.run_artifacts` | `run_id` | 三 hash + `artifact_uri` 是冻结最小集；**无 `status`**（QT-63 ④）；`/provenance` 与本表 ⋈ `backtest_jobs` 取结论。写入语义（施工 ⑨）：同身份重写幂等、**不刷 `created_at`**；同 `run_id` 身份漂移 → `ErrArtifactConflict`（`ON CONFLICT DO UPDATE … WHERE <身份全等>` 下 0 行受影响），**禁止** `DO NOTHING` 静默吞；未知 `run_id` 由 FK 拒（23503 → `ErrJobNotFound`）；可空列比较用 `IS NOT DISTINCT FROM` |
| `run.parent_jobs` | `parent_job_id`；`UNIQUE(idempotency_key) WHERE NOT NULL`；**`UNIQUE(plan_hash)`** | **一张表承载两种父**（裁-13）：`kind ∈ {batch, walkforward}`；身份 = `plan_canonical`（TEXT 字节锚点）+ `plan_hash = sha256(plan_canonical)`；**同计划（即使换幂等键）只留一行**——由 `parent_jobs_plan_hash_uidx` 兜底，`EnqueueParent` 先按计划身份查；`total/completed/invalid/failed` 三值计数；`status` 与 job 同词表；`summary_json` 只放排序 / 逐窗结论，不复制子结果 |
| `run.backtest_jobs.parent_job_id` | 可空 FK → `parent_jobs.parent_job_id`；索引 `WHERE parent_job_id IS NOT NULL` | 子 run 关系**只有这一列**（裁-13 明确不塞 `request_json` 父元数据，那会改 `request_hash` 的语义）。**写入方 = `internal/parents` 的子 run 入队（施工 ⑩）**；PG 的 FK 顺带兜住「父行必须已存在」 |
| `run.batch_runs` / `run.walk_forward_reports` | 旧键不变 | **迁移期旧表**：由 `parent_jobs` 取代，停写；O1 对拍后删除 |
| `run.worker_registry` | `worker_id` | 能力（`kernels`，施工 ④ 起由可选接口 `KernelAwareJobQueue.ClaimByKernel` 消费；空数组 = 不过滤，不是「什么都不能干」）/ 心跳 / active → draining → offline（施工 ⑤：`last_heartbeat_at <= now-timeout` 由 `ReapOffline` 推 offline，含 draining；未请求 drain 的可用心跳 / 重注册恢复 active）；超时不删行 |

**施工 ⑦（X5b，裁-13 落地）**：新增 `run.parent_jobs` 与 `backtest_jobs.parent_job_id`（列 + 索引），`internal/pgstore.Parents` 与 `internal/queue.MemoryParents` 跑同一套语义；提交写父表、汇总写回三值计数都由 `internal/parents` 完成（复用 `internal/plan`）。**施工 ⑧**：追加 `parent_jobs_plan_hash_uidx`（`plan_hash` 唯一）——计划身份跨幂等键去重；`POST/GET /api/runs/parents` 已落，`GET` 的 `children` 由 `ParentChildLister`（内存 + PG 两个 `JobQueue` 实现）按 `parent_job_id` 读取。**施工 ⑩**：子 run 入队落 `internal/parents`——父行落库后逐条 `Enqueue` 并写 `parent_job_id`（唯一写入方），幂等键 `parent:<parent_job_id>:<child_index>`；DDL 无改动。**未落**：worker 执行。

**迁移路径**（与 QT-63 ③-① 一致）：`batch_runs` / `walk_forward_reports` → `parent_jobs` 双写 → 对拍 → 切读 → 停写（本轮已直接停在「只写新表」：两张旧表自始无写入路径，不存在存量行）；`quant.run_provenance` 由 `run.run_artifacts` 双写/对拍/切读/停写后取代。**子 run 关系已定**：`backtest_jobs.parent_job_id`（R6 裁-13），不再是待裁项。

## 6. 待定

- `cmd/dualrun`（K15 双跑）**已裁决留 core**（2026-10-20）：它的产物（成交明细与对齐报告）不落 `run` schema，本仓不承接它的状态与结果。
