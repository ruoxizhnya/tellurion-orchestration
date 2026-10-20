---
status: active
last-verified: 2026-10-20
verified-by: 功能边界固化（三份契约：db / api / dataflow）
---

# Dataflow — tellurion-orchestration

> 目的：把「数据从哪来、在谁手里变形、到哪去」写死。**任何跨仓数据交换都必须能在本图上找到一条边**；找不到 = 这段代码在偷偷建第二条路。

## 1. 主链路

```
ui / agent / CI
      │  POST /runs/jobs（幂等键）
      ↓
tellurion-orchestration ── 写 run.*（队列 / 批次 / 报告 / 三 hash / worker）
      │  派活（port 或 HTTP，取决于部署形态）
      ↓
   core 引擎（进程内）  ── 产出结果 ──►  run 结果坐标（三 hash）
      ↑
   worker 池（drain / 心跳）
```

## 2. 关键时序

1. `POST /api/runs/jobs` → 校验三 hash 可解析 → 入队 → 返回 `job_id`
2. worker 领取（乐观锁）→ `running`
3. 引擎执行 → 结果 + 三 hash → `done` / `failed`
4. `GET /api/runs/jobs/{id}` 查摘要；`GET /api/runs/{id}/provenance` 查坐标
5. 滚动升级：`POST /workers/{id}/drain` → 跑完手上的 → 下线

## 3. 失败模式（每一处 fail-closed 都要能指名）

| 失败点 | 症状 | 兜底 |
|---|---|---|
| 队列满 | 提交失败 | 429 + 退避（不丢任务） |
| worker 崩溃 | 任务卡 running | 心跳超时 → 重新入队（幂等） |
| 三 hash 缺失 | 结果不可追溯 | 入队前拒绝 |
| 编排层重启 | 队列丢 | 状态在 DB，不在内存 |

## 4. 不变量（可被测试钉住的）

1. 每个 job 有且仅有一个终态。
2. 幂等键相同 → 同一个 `job_id`。
3. 任何 `done` 的 job 都能通过 `/provenance` 取到三 hash。
4. `drain` 后该 worker 不再接新任务。

## 5. 与契约文档的关系

- 表结构 → `docs/db-schema.md`
- 接口形状 → `docs/api-schema.md`
- 本文件只讲**流向与顺序**，不重复字段定义。
