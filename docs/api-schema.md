---
status: active
last-verified: 2026-10-20
verified-by: 功能边界固化（三份契约：db / api / dataflow）
---

# API Schema — tellurion-orchestration

> **实现状态（2026-10-20）**：本仓目前**只有占位 `doc.go`，没有任何 handler**——下列端点**全部未实现**。
> 本文件是**目标契约**，不是现状说明书；开工顺序见 `docs/TASKS.md` 的「施工序」。

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
| GET | `/api/runs/jobs?status=&limit=` | 列表 |

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

```go
type RunStateStore interface {  // 断点状态
    Save(ctx, runID string, state []byte) error
    Load(ctx, runID string) ([]byte, error)
}
type JobQueue interface {       // 现 JobStore 外置
    Create(ctx, job JobRecord) error
    UpdateStarted / UpdateCompleted / UpdateFailed(...)
    Get / List / Delete
}
```

## 2. 错误码与 fail-closed 语义

| 情况 | 返回 | 调用方行为 |
|---|---|---|
| 同幂等键重复提交 | 200 + 原 `job_id` | 视为成功，不重复跑 |
| 队列满 | 429 + `queue_full` | 退避重试，**不 silently 丢弃** |
| 取消正在跑的 job | 202 + `cancelling` | 轮询确认终态 |
| worker 不可用 | 503 + `no_worker` | 任务留在队列，不标记失败 |

**fail-closed**：拿不到三 hash 的 run 不许入队（否则结果无法追溯）。

## 3. 幂等性与重试

- `POST /jobs` 必须带幂等键（客户端提供或由 `run_identity_hash` 派生）。
- `cancel` 幂等（已取消再取消返回同终态）。
- `drain` 幂等。
- 三 hash 相同 + 参数相同 → 可复用已完成结果（可选优化，但必须由 hash 决定，不能猜）。

## 4. 版本与兼容

- job 状态机只增状态、不改语义；新增状态要能被旧客户端识别为"未完成"。
- 三 hash 的算法变更算 breaking（历史 run 的坐标会变）。
- 端点只增不删。

## 5. 不提供什么（边界）

- 不做策略执行（引擎在 core）
- 不做结果分析（那是 analytics）
- 不做数据准备（那是 data）
- 不做风控裁决（那是 risk / core）
- 不做调度 UI（ui 自己画）
