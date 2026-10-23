---
status: active
last-verified: 2026-10-23
verified-by: X5a/X5c 落地 + 文档同步（QT-44 施工 ①）
---

# TASKS — tellurion-orchestration 未完成项

> 本仓**唯一**任务台账。跨仓任务只在 §二 留指针，**不镜像正文**（双写必然漂移）。
> 状态：`⬜ 待做` / `🔶 进行中` / `⛔ 阻塞（有未解除前置）` / `✅ 完成（附日期）`
> 每张单六字段（① 目标 ② 上下文 ③ 要求 ④ 约束与非目标 ⑤ 验收 ⑥ 边界），判据见 `tellurion:AGENTS.md` §8.5。
> 完成即标 ✅（附日期）并移入 `docs/archive/TASKS-YYYY-MM-DD-archived.md`；本文件只留未完成项。

---

## 一、本仓任务

> **约定变更（2026-10-21）**：原「占位仓不写正式六字段任务单」作废——X 线任务单已展开为本仓正式任务（见下）。M5/M8 开工前置不变；实现仍未开工，开工前仍须过前置两条。

### QT-44 · X5 编排 / worker 服务（job/batch/walkforward 上移）[P2] 🔶

> **迁入说明（2026-10-21）**：本单自 core 仓 `tellurion:docs/TASKS.md`「五、X 线」迁入（任务台账批量重分布：X 线单展开到各服务仓为正式六字段单，「占位仓不写正式任务单」旧约定同时作废）。六字段正文逐字保留、不改写；core 侧同号条目已改为指针。
>
> **施工记录（2026-10-23，施工 ①）**：X5a（`JobQueue` port + 状态机 + 幂等键）与 X5c（`WorkerRegistry` 注册 / 心跳 / `drain`）已在 `internal/queue` 落地——port 与方法级错误语义见 `ARCHITECTURE.md` §9.2，内存实现 + `run` schema 5 表 DDL 见 §9.8。**仍是 🔶**：X5b（batch / walkforward）、X0（`run_artifacts` 三 hash + `/provenance`）未做；DDL 未接 PG、无 HTTP 层；`cancelling` 行的租约回收出口待裁（§9.7 R8）。
>
> **M8 前置完成（2026-10-22）**：接口对表见 [`ARCHITECTURE.md`](ARCHITECTURE.md) §9。开工前先读 §9.1 的八条前置事实——`JobStore` **不是队列**（F1：`map[string]any` + `CreateJob` 直接起 goroutine + 内存 cancel）；异步请求**表达不了完整 RunSpec**：`commission_rate`/`slippage_rate` 落库后无读取点、策略 `params` 不被应用、`fill_timing` 缺位（F2）；无幂等键、无取消端点、`DELETE` 语义是删行（F3）；provenance 已落 `quant.run_provenance`，但 `run_artifacts` 与 `/api/runs/*` 零实现（F4）；batch 报告只在内存且把 K9 `invalid` 当成功（F5）；walkforward 直连 `*storage.PostgresStore`、请求缺参、逐窗失败静默消失（F6）；worker 注册/心跳/drain 零实现（F7）；**三 hash 是终态不变量、不是入队门槛**（F8）。本单当时**仍 ⬜**（对表是开工前置，不是实现）；R1~R7 未裁前 X5a/X5b/X5c/X0 各自阻塞。
> 对表自检（2026-10-22，基线 `dbe4c68`）：71 条 `path:line`（core 69 + agent 2）按 HEAD 逐条命中；C 组 11 端点 / 14 错误码与 `api-schema.md` 双向一致；破坏验证三腿（改错行号 / 删一行端点 / 删一个错误码）各自红，还原后绿（文件 sha256 与改前相同）。

- **① 目标**：批量扫描的算力出口独立成服务；`backtest_jobs` 归属迁出内核；三 hash 落 `run_artifacts`。
- **② 上下文**：`orchestration-service-design.md`（D22）+ BE-03；`BatchEngine` 已按 `contracts.EngineRunner` 注入，摘出成本低。
- **③ 要求**：① `job`/`batch`/`walkforward` 从 `pkg/backtest` 上移；② worker 池调度 + 断点续跑协调；③ `run` schema（`backtest_jobs`/`walk_forward_reports`/`run_artifacts`/`batch_runs`/`worker_registry`），**不含 `strategy_state`**（H5 裁决：归 strategy-runtime 模块，core 继续写 `quant.strategy_state`）；④ `/api/runs/{id}/provenance` 端点；⑤ 内核通过 `JobQueue` port 读写 job 表；`run_artifacts` 的吸收口径见 QT-63 裁决（过渡期 core `quant.run_provenance`，O1 双写/对拍/切读/停写后取代）。
- **② 前置（2026-10-21 订正：原 U3 `RunStateStore` port 已作废）**：H5 裁决（`tellurion:docs/design/qt-decoupling/orchestration-service-design.md` §3 归属修正）明确 `strategy_state` **不迁 `run`**、归 strategy-runtime 模块（K0 冻结契约 `contracts/kernel_modules.schema.sql`），core 继续写 `quant.strategy_state`；因此 `RunStateStore` port **不需要**，本单前置减少为「`JobQueue` port 外置」一项。
- **④ 约束与非目标**：不在内核保留编排逻辑；不改 `EngineRunner` 契约；不做 K8s；**不搬 `cmd/dualrun`**——它 2026-10-20 已裁决留 core（要同进程链接两个内核，且是引擎的验收门，见 `repo-inventory.md` §3）。
- **⑤ 验收**：`pkg/backtest` 里 `job|batch|walkforward` 零命中；同一 batch 在编排服务与旧路径结果一致；provenance 端点返回三 hash。
- **⑥ 边界**：只做编排与结果归属。


---

> **⚠️ 开工前置（refactor-plan-review M5/M8，2026-10-20 登记）**：本表任何单开工前，必须先过 core `tellurion:docs/TASKS.md`「五、X 线」的**公共前置**两条：① 建仓/升格前先与 ADR-027 §2 对账（12 仓是上限/当前规划，不是承诺终态）；② 本仓设计文档先写「**接口对表**」小节（签名 + 错误语义 + 版本策略，模板 = `tellurion:docs/design/qt-decoupling/data-service-contract-and-ownership.md` §4.1），**未对表不开工**。正文以彼处为准，此处只留指针、不镜像。
> **2026-10-22 订正**：原文模板指向 `target-module-topology.md` 的 UnifiedDataPoint 节——该引用在 core 仓是**幻影**（QT-42/QT-43 已实测订正），本仓同步改为 `data-service-contract-and-ownership.md` §4.1。M8 已落 §9（QT-44 侧）；M5 已结案（六仓 ≠ 六进程，本仓今天不升格容器）。

## 施工序（候选单，开工前必须展开成六字段）

> 这张表是**施工顺序**，不是任务台账：每条在执行前要展开成六字段单（目标/上下文/要求/约束/验收/边界），并写明冻结契约的路径与节号。
> 顺序有依赖，**不要跳级**：跳序的代价是返工（例：X1a 的协议没冻结就写 aggregator，等于把协议写死在一个服务里）。
> 2026-10-21：候选单已展开为「一、本仓任务」的正式六字段条目（表中「上游单号」指向的 QT 单正文现在就在本仓）。

| 阶段 | 内容 | 上游单号 | 前置 |
|---|---|---|---|
| X5a | job 队列 + 状态机 + 幂等键 🔶（施工 ① 已落 `internal/queue`，DDL 未接 PG） | `QT-44` | 本仓立项 |
| X5b | batch + walkforward（窗口切分与汇总） | `QT-44` | X5a |
| X5c | worker 注册 / 心跳 / `drain` 🔶（施工 ① 已落 `internal/queue`） | `QT-44` | X5a |
| X0 | `run_artifacts` 三 hash 留痕 + `/provenance` | `QT-03` | X5a |

## 二、跨仓指针（不在本仓，勿双写）

| 单号 | 主题 | 在哪 |
|---|---|---|
| QT-44 | X5 编排 / worker 服务（job/batch/walkforward 上移） | 已迁入本仓「一、本仓任务」（2026-10-21；core `tellurion:docs/TASKS.md` 留同号指针） |
| QT-08 | 本仓 CI / AGENTS / 台账由该单一并补齐 | `tellurion:docs/TASKS.md` |

---

## 维护本文件

- **新增单**：写全六字段；六字段里必须显式引用**冻结契约的路径与节号**（契约仓的包路径、设计文档的节号），不许写"参考相关实现"。
- **派工前先证伪前提**：读码核对工单写的前提；不符就停下回报 + 给拆法，不硬做、不扩范围。
- **跨仓引用写 `repo:path`**（如 `tellurion:docs/TASKS.md`），不做可点击相对链接。
- **顺序纪律**：同一波次内撞同一批文件的单按单号串行，一单一 commit。
