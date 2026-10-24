---
status: active
last-verified: 2026-10-24
verified-by: 施工 ⑤（R10 心跳超时 → `offline`）+ 施工 ⑥（X5b 规划 / 汇总纯函数；父表写入与端点未落，链路 9/10 同步）
---

# Dataflow — tellurion-orchestration

> 目的：把「数据从哪来、在谁手里变形、到哪去」写死。**任何跨仓数据交换都必须能在本图上找到一条边**；找不到 = 这段代码在偷偷建第二条路。

## 1. 主链路

```
ui / agent / CI
      │  POST /api/runs/jobs（幂等键）
      ↓
tellurion-orchestration ── 写 run.*（队列 / 批次 / 报告 / 三 hash / worker）
      │  派活（port 或 HTTP，取决于部署形态）
      ↓
   内核路由（RunSpec.Kernel：空 = go；未注册 fail-closed，不回退）
      ├─ go 内核：core 引擎（进程内 · 逐 bar）
      └─ NT 侧车：NautilusTrader（独立进程，HTTP /v1/backtest）
        ── 产出结果 ──►  run 结果坐标（三 hash）
      ↑
   worker 池（drain / 心跳）
```

> **双内核**：派活按 `RunSpec.Kernel`（`BacktestRequest.Kernel`，空 = 默认 `go`，默认值不回填）经注册表路由——`go` 内核即 core 引擎（进程内逐 bar），NT 侧车是独立进程的 NautilusTrader 内核，派活走 HTTP `/v1/backtest`；未注册内核 fail-closed 拒绝，**不静默回退 go**。
> 内核接口面见 `tellurion:docs/design/qt-decoupling/kernel-interface-map.md`，双内核模型见 `tellurion:docs/design/qt-decoupling/backtest-extraction.md`。

## 2. 关键时序

**启动装配（施工 ③ 已落）**

0. `cmd/orchestrationservice`：`-dsn` / `ORCHESTRATION_DSN`（两者都空即**拒绝启动**，不退回内存队列）→ `pgstore.Migrate()` 按序幂等执行 `queue.Migrations()` → **迁移成功后**才监听（`127.0.0.1:8088`）→ 周期任务按 `-reap-interval` 调 `ReapExpired`。半套表开始领活比启动失败难查得多。

**提交 / 执行**

1. `POST /api/runs/jobs` → 校验 `idempotency_key` + RunSpec（market→kernel 在**入队前**解析）→ 算 `request_hash` → 写 `run.backtest_jobs(status=pending)` → 返回 `job_id`。同键同 hash = 原 job（200），同键不同 hash = 409。
2. worker 领取：原子 `pending → running` + `worker_id` + `lease_until`（PG 实现用 `FOR UPDATE SKIP LOCKED`，领取顺序 `priority DESC → created_at ASC → id ASC`，与内存实现的 tie-break 同形；施工 ③ 已落）；**worker 只执行行里的 `request_json`**，不从扁平列重建请求。
   - **内核匹配已落（施工 ④，R9）**：`Claim` 仍不按内核过滤（§9.2 冻结面，加了就是 MAJOR）；过滤走**可选**接口 `KernelAwareJobQueue.ClaimByKernel`——它读 worker **在注册表里声明的** `kernels`，只领生效内核匹配的行；未注册、注册表未装配、或声明为空 ⇒ 不过滤（与「没登记不等于在 drain」同形）。行的生效内核：**只有 JSON 字符串才算内核**，缺键 / null / 数字 / 顶层非对象 / 空串一律按默认 `go`；两侧同口径的钉子是套件里 `ClaimByKernelReadsOnlyStringKernels`。⚠️ 消费方（真正按能力派活的 worker 进程）本仓还没有，所以今天这条过滤只被套件与编译期断言用到。
3. 执行期心跳续租（`RenewLease`）；引擎在**日循环之前**写输入身份（interim：core `quant.run_provenance`；O1：`run.run_artifacts`，双写/对拍/切读/停写）。
4. 终态写一次：`Complete(result_status, result_uri)` 或 `Fail(error, result_status)`。`result_status` 是 K9 三值，和队列 `status` 是**两轴**（`ARCHITECTURE.md` §9.1 F8）；终态的每一行必须符合 §9.5 的**两轴映射表**（`invalid` → `status=completed` + `result_status=invalid`，`error` 为空；failed 时按是否产出 ResultSpec 传 `failed` 或空串）。
5. `GET /api/runs/jobs/{id}` 查生命周期 + 结果结论；`GET /api/runs/{id}/provenance` 是 `run_artifacts ⋈ backtest_jobs`，**不读** provenance 表里的 status（它没有 status）。

**取消 / 回收 / 升级**

6. `DELETE /api/runs/jobs/{id}`：`pending` → 直接 `cancelled`（保留行）；`running` → `cancelling` + `cancel_requested=true`；worker 看到后中止、`AckCancel` → `cancelled`。跨进程取消靠 DB，不靠内存 map。
7. `ReapExpired`（由 `cmd` 的周期任务按 `-reap-interval` 调用）两类迁移：**① `running`** 租约过期 → `pending` 且 `attempts+1`；超过 `max_attempts` → `failed`（`attempts_exhausted`）。被回收的 worker 即使后来跑完，也因 `ErrLeaseLost` 写不进终态（fencing，PG 与内存两侧同形，实测腿 `ExpiredLeaseFencesTheOldOwner`）。**② `cancelling`**（施工 ④，R8 裁决落地）租约过期 → `cancelled`，**不**计 attempts——owner 消失后 `AckCancel` 永远等不到，这行的唯一出口就是这里；`worker_id` 保留作审计。两侧同跑的钉子 `ReapLandsOrphanedCancellingRow`。
8. 滚动升级：`POST /api/runs/workers/{id}/drain` → `active → draining` → 不再领新活 → 跑完手上的 → 心跳停 → **心跳超时 → `offline`**（施工 ⑤，`WorkerReaper.ReapOffline`，由 `cmd` 的周期任务与租约回收同轮调用；`draining` 也在回收范围内）。`offline` 是**推断**而非运维意图：未请求过 drain 的 worker 可被心跳 / 重注册拉回 `active`（重注册同时刷新心跳）；请求过 drain 的不复活。`offline` 之后 `Claim` / `ClaimByKernel` 都不再派活（同一个 active 门）。

**批量 / 前向**

9. `POST /api/runs/batch` / `POST /api/runs/walkforward` → 父记录 `pending` + 展开子 run（每个子 run 一行 `backtest_jobs`，各自 provenance）；父报告只汇总计数与排序，不复制每个结果的内容。
   - **规划已落（施工 ⑥，X5b）**：`internal/plan.ExpandBatch`（网格笛卡尔积，网格值浅合并进 `strategy_params`）、`internal/plan.ExpandWalkForward`（只生成完整窗口；train/test 只替换 `start_date` / `end_date`，其余 RunSpec 字段原样保留）。**未落**：父记录写入（`batch_runs` / `walk_forward_reports`）、batch/walkforward 4 端点、子 run 入队；R6 未裁前不建第六张表、不塞父元数据。
10. 子 run 的 `result_status` 逐条可见：`completed` 才进排名 / 聚合；`invalid` / `failed` 显式计数，不静默丢弃。所有窗口失败 → 父 `failed`，不是"成功的空报告"。
   - **汇总已落（施工 ⑥）**：`AggregateBatch`（必须传计划总数 `plannedTotal`，与逐条回报数不符即拒——漏报不会被当成完成；只对 `completed` 且给出有限 score 的子 run 排名，同分按 `run_id` 升序；`invalid` / `failed` 保留但 `rank=0`）与 `AggregateWalkForward`（单段失败 = 整窗失败；缺段 / 缺窗算 pending）。父状态三条：还有未回报 → `running`；任一 `failed` → `failed`；否则 `completed`（`invalid` 不拖成 `failed`）。评分由执行侧给，本仓不解析结果内容。

## 3. 失败模式（每一处 fail-closed 都要能指名）

| 失败点 | 症状 | 兜底 |
|---|---|---|
| 队列满 | 提交失败 | 429 + `queue_full` + 退避（不丢任务） |
| 幂等键冲突 | 同键不同请求 | 409 `idempotency_conflict`，**不复用**旧 job |
| worker 崩溃 / 心跳停 | 任务卡 `running`；worker 仍显示在线 | 租约过期 → `ReapExpired` 回 `pending`；超次 → `failed`。worker 本身由心跳超时 → `offline`（`ReapOffline`），不再被派新活 |
| 取消跨进程 | 内存 cancel 打不到别人 | `cancel_requested` + `cancelling`，worker 主动确认 |
| 租约丢失后 worker 复活 | 两个 worker 抢写终态 | `ErrLeaseLost` fencing，旧 owner 写不进 |
| 三 hash 缺失（入队时） | — | **不是入队门槛**：入队只校验引用可解析（三 hash 是 run 产物） |
| 三 hash 缺失（终态） | 结果不可追溯 | 终态 `completed` / `invalid` 必须有 `run_artifacts` 行，否则 `provenance_missing` |
| batch / wf 部分失败 | 汇总看起来"完成" | 三值计数逐条可见；父 `status` 不吞子失败 |
| 编排层重启 | 队列丢 | 状态在 DB，不在内存；租约 + 重试幂等 |
| 结果内容写进 `run` | 边界腐化 | 只存 `result_uri` 指针；结果内容归 core / artifact 服务 |

## 4. 不变量（可被测试钉住的）

1. 每个 job 有且仅有一个终态（`completed` / `failed` / `cancelled`）；进入终态后不可再迁移。
2. 幂等键相同 + `request_hash` 相同 → 同一个 `job_id`；同键不同 hash → 409，绝不复用。
3. 任何 `result_status` 为 `completed` / `invalid` 的 job（队列 `status=completed`）都能通过 `/provenance` 取到三 hash；`failed` / `cancelled` 允许无 provenance（`failed` 若已跑过则可有）。
4. `drain` 后该 worker 不再接新任务；在途任务跑完（或租约到期被回收）才 `offline`。`offline` 同样不接新任务；未请求过 drain 的 `offline` 可被存活证明（心跳 / 重注册）恢复 `active`，请求过 drain 的不行。
5. `result_status` 只允许 K9 三值；**不存在第四值**；`invalid` 的 `error` 为空、`invalid_reasons` 在结果里。
6. 同一 job 的 `running` 租约在任一时刻只有一个有效 owner；过期 owner 的写入被拒。
7. `run` schema 里没有结果内容（只有指针 + 三 hash）；batch / wf 汇总不复制子结果明细。
8. 父记录（batch / wf）的 `status` 不是 K9：它是生命周期；子 run 的结论只看各自的 `result_status`。

## 5. 与契约文档的关系

- 表结构 → `docs/db-schema.md`
- 接口形状 → `docs/api-schema.md`
- 本文件只讲**流向与顺序**，不重复字段定义。
