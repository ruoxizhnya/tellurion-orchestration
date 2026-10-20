---
status: active
last-verified: 2026-10-20
verified-by: 功能边界固化（三份契约：db / api / dataflow）
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
   run.backtest_jobs / batch_runs / walk_forward_reports / run_artifacts / worker_registry
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
| X5a | job 队列 + 状态机 + 幂等 | ⬜ |
| X5b | batch + walkforward | ⬜ |
| X5c | worker 注册 / drain | ⬜ |
| X0 | 三 hash 留痕（`run_artifacts`） | ⬜ |
