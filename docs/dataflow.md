---
status: active
last-verified: 2026-10-22
verified-by: M8 接口对表（QT-44 前置 ②）
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

**提交 / 执行**

1. `POST /api/runs/jobs` → 校验 `idempotency_key` + RunSpec（market→kernel 在**入队前**解析）→ 算 `request_hash` → 写 `run.backtest_jobs(status=pending)` → 返回 `job_id`。同键同 hash = 原 job（200），同键不同 hash = 409。
2. worker 领取：原子 `pending → running` + `worker_id` + `lease_until`（`SKIP LOCKED` 或等价）；只领 `kernels` 匹配的活。**worker 只执行行里的 `request_json`**，不从扁平列重建请求。
3. 执行期心跳续租（`RenewLease`）；引擎在**日循环之前**写输入身份（interim：core `quant.run_provenance`；O1：`run.run_artifacts`，双写/对拍/切读/停写）。
4. 终态写一次：`Complete(result_status, result_uri)` 或 `Fail(error)`。`result_status` 是 K9 三值，和队列 `status` 是**两轴**（`ARCHITECTURE.md` §9.1 F8）；终态的每一行必须符合 §9.5 的**两轴映射表**（`invalid` → `status=completed` + `result_status=invalid`，`error` 为空）。
5. `GET /api/runs/jobs/{id}` 查生命周期 + 结果结论；`GET /api/runs/{id}/provenance` 是 `run_artifacts ⋈ backtest_jobs`，**不读** provenance 表里的 status（它没有 status）。

**取消 / 回收 / 升级**

6. `DELETE /api/runs/jobs/{id}`：`pending` → 直接 `cancelled`（保留行）；`running` → `cancelling` + `cancel_requested=true`；worker 看到后中止、`AckCancel` → `cancelled`。跨进程取消靠 DB，不靠内存 map。
7. `ReapExpired`：租约过期 → `pending` 且 `attempts+1`；超过 `max_attempts` → `failed`（`attempts_exhausted`）。被回收的 worker 即使后来跑完，也因 `ErrLeaseLost` 写不进终态（fencing）。
8. 滚动升级：`POST /api/runs/workers/{id}/drain` → `active → draining` → 不再领新活 → 跑完手上的 → `offline`。

**批量 / 前向**

9. `POST /api/runs/batch` / `POST /api/runs/walkforward` → 父记录 `pending` + 展开子 run（每个子 run 一行 `backtest_jobs`，各自 provenance）；父报告只汇总计数与排序，不复制每个结果的内容。
10. 子 run 的 `result_status` 逐条可见：`completed` 才进排名 / 聚合；`invalid` / `failed` 显式计数，不静默丢弃。所有窗口失败 → 父 `failed`，不是"成功的空报告"。

## 3. 失败模式（每一处 fail-closed 都要能指名）

| 失败点 | 症状 | 兜底 |
|---|---|---|
| 队列满 | 提交失败 | 429 + `queue_full` + 退避（不丢任务） |
| 幂等键冲突 | 同键不同请求 | 409 `idempotency_conflict`，**不复用**旧 job |
| worker 崩溃 / 心跳停 | 任务卡 `running` | 租约过期 → `ReapExpired` 回 `pending`；超次 → `failed` |
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
4. `drain` 后该 worker 不再接新任务；在途任务跑完（或租约到期被回收）才 `offline`。
5. `result_status` 只允许 K9 三值；**不存在第四值**；`invalid` 的 `error` 为空、`invalid_reasons` 在结果里。
6. 同一 job 的 `running` 租约在任一时刻只有一个有效 owner；过期 owner 的写入被拒。
7. `run` schema 里没有结果内容（只有指针 + 三 hash）；batch / wf 汇总不复制子结果明细。
8. 父记录（batch / wf）的 `status` 不是 K9：它是生命周期；子 run 的结论只看各自的 `result_status`。

## 5. 与契约文档的关系

- 表结构 → `docs/db-schema.md`
- 接口形状 → `docs/api-schema.md`
- 本文件只讲**流向与顺序**，不重复字段定义。
