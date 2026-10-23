---
status: active
last-verified: 2026-10-23
verified-by: 施工 ①（QT-44：`run` schema DDL 落 `internal/queue/ddl.go`）
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
| `run.backtest_jobs` | 任务队列（生命周期六态 + K9 `result_status` 两轴，见 [`ARCHITECTURE.md`](ARCHITECTURE.md) §9.5） | DDL 已落（`internal/queue/ddl.go`）；内存实现可用，**未接 PG** |
| `run.walk_forward_reports` | 前向验证报告（`wf_id` + 逐窗 run 指针） | DDL 已落；功能未实现（X5b） |
| `run.batch_runs` | 批量扫描批次 + 评分汇总 | DDL 已落；功能未实现（X5b） |
| `run.run_artifacts` | 每次 run 的三 hash（`strategy_ref` / `dataset_snapshot_hash` / `risk_rules_hash`）+ artifact 指针 | DDL 已落；写入路径未实现（X0）；🔒 冻结面，变更须走评审 |
| `run.worker_registry` | worker 注册与心跳（active / draining / offline） | DDL 已落（`internal/queue/ddl.go`）；内存注册表可用，**未接 PG** |
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
- **只前滚**：已合并的迁移不改；修错靠新增一条。
- 每条必须可重入（`IF NOT EXISTS` / `IF EXISTS`），且能在空库上一次跑完。
- 跨 schema 取数只走**只读视图或 API**；出现 `join` 别人的表就是边界腐化的第一信号。
- 结构变更的同步义务：本文件 + `docs/ARCHITECTURE.md` 的「数据归属」节必须同一次提交里更新。

## 5. 目标键与迁移（M8 接口对表，2026-10-22）

> **本节只钉「表要回答什么、键怎么定」，列清单与类型在 [`ARCHITECTURE.md`](ARCHITECTURE.md) §9.5**（单一处维护，避免两份漂移）。落地前**不改库**；O0 按 §4 的机制建表。
>
> **现状（2026-10-23，施工 ①）**：五张表的 DDL 已按 §4 取向 ① 落在 `internal/queue/ddl.go`（含 6 条索引），逐条可重入；**尚未接 PG**（本仓没有数据库装配）。列与 §9.5 的对应关系由 `internal/queue/ddl_test.go` 钉住表数，列级核对在 §9.8 的验收腿里。

| 表 | 目标主键 / 唯一键 | 关键语义 |
|---|---|---|
| `run.backtest_jobs` | `id`；`UNIQUE(idempotency_key) WHERE NOT NULL`；索引 `(status, priority DESC, created_at)` where `pending` | `request_json` 是完整 canonical RunSpec（worker 只执行这一份）；`status` 六态；`result_status` K9 三值可空；`worker_id` / `lease_until` / `attempts` / `cancel_requested` 支撑多 worker；`result_uri` 是结果指针；**目标无 `result` 内容列**（迁移期旧列保留只读，切读后删除） |
| `run.run_artifacts` | `run_id` | 三 hash + `artifact_uri` 是冻结最小集；**无 `status`**（QT-63 ④）；`/provenance` 与本表 ⋈ `backtest_jobs` 取结论 |
| `run.batch_runs` | `batch_id`；`UNIQUE(idempotency_key)` | 网格 + base RunSpec + 三值计数 + 排序汇总；不复制子结果 |
| `run.walk_forward_reports` | `wf_id`；`UNIQUE(idempotency_key)` | 窗口配置 + 逐窗 run 指针 + 三值计数；部分失败显式计数 |
| `run.worker_registry` | `worker_id` | 能力（`kernels`）/ 心跳 / active → draining → offline；超时不删行 |

**迁移路径**（与 QT-63 ③-① 一致）：`backtest_jobs` / `walk_forward_reports` 双写 → 对拍 → 切读 → 停写；`quant.run_provenance` 由 `run.run_artifacts` 双写/对拍/切读/停写后取代。**子 run 关系**（batch / wf 的 task / window → `backtest_jobs` 行）待裁，见 `ARCHITECTURE.md` §9.7 R6，未定前不建第六张表。

## 6. 待定

- `cmd/dualrun`（K15 双跑）**已裁决留 core**（2026-10-20）：它的产物（成交明细与对齐报告）不落 `run` schema，本仓不承接它的状态与结果。
