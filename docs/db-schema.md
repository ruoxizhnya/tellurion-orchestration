---
status: active
last-verified: 2026-10-20
verified-by: 功能边界固化（三份契约：db / api / dataflow）
---

# DB Schema — tellurion-orchestration

> **所有权声明**：本仓**拥有 `run` schema 的 DDL**：任务队列、批量批次、前向验证报告、三 hash 留痕、worker 注册。是这组表的唯一写者。
> 上级依据：`tellurion:docs/design/qt-decoupling/data-service-contract-and-ownership.md` §1 —— **DDL 只有一个 owner**，同一个 PG 实例、每服务一个 schema，将来拆库是"按 schema 搬走"而不是重写。
> 本文件是本仓表结构的**唯一真相源**：代码里的建表语句与它不一致时，**改代码**；要改表结构，先改本文件（结构级变更还要立 ADR）。

## 1. 归属表

| 表 | 内容 | 现状 |
|---|---|---|
| `run.backtest_jobs` | 任务队列（queued→running→done/failed） | 从 core 的 `backtest_jobs`（无归属 schema）迁入 |
| `run.walk_forward_reports` | 前向验证报告 | 从 core 迁入 |
| `run.batch_runs` | 批量扫描批次 + 评分汇总 | 待建 |
| `run.run_artifacts` | 每次 run 的三 hash（`strategy_ref` / `dataset_snapshot_hash` / `risk_rules_hash`）+ artifact 指针 | **待建（X0 载体）** |
| `run.worker_registry` | worker 注册与心跳 | 待建（多 worker 时） |

**归属修正**（ADR-D19 已登记）：`backtest_jobs` / `strategy_state` 目前在内核侧持久化，**迁到 `run`**，内核走 port —— 否则"内核无持久化状态"不成立。

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
3. **三 hash 必留**：任何 run 的结果必须能回答"哪份策略 / 数据 / 规则"。
4. **worker 摘除要优雅**：`drain` 后不再接新活，跑完手上的再下线。
5. **内核无持久化状态**是目标态：状态写 `run`，内核走 port。

## 4. 建表与迁移

**现状（必须先知道）**：core 今天**没有迁移执行器**——建表靠 `pkg/storage/*.go` 里的内联 `CREATE TABLE IF NOT EXISTS`，另有 `migrations/`（14 个）与 `docs/migrations/`（25 个）两处**手工执行**的 SQL。这是拆分服务时必须先收口的债，不要在本文假设某个执行器已经存在。

**本仓约定**（从建仓第一天执行）：

- 只用 `migrations/` 一处，命名 `<序号>_<动作>.sql`，**本仓内**递增（不与别的仓共享序列，也不复用号）。
- **只前滚**：已合并的迁移文件不改；修错靠新增一条。
- 每条必须可重入（`IF NOT EXISTS` / `IF EXISTS`），且能在空库上一次跑完。
- 跨 schema 取数只走**只读视图或 API**；出现 `join` 别人的表就是边界腐化的第一信号。
- 迁移执行器与 applied 版本表随**第一个真正需要建表的单**一起落地；落地前本仓不产生"半迁移"。
- 结构变更的同步义务：本文件 + `docs/ARCHITECTURE.md` 的「数据归属」节必须同一次提交里更新。

## 5. 待定

- `cmd/dualrun`（K15 双跑）归本仓还是 core **未裁决**；在裁决前不搬。
