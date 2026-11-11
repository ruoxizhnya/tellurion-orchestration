---
status: active
last-verified: 2026-11-11
verified-by: 2026-11-11 全面审查 W7 台账立单（A5 授权）——新立 1 条六字段单（`ORCH-RATELIMIT`），证据坐标引 ODR-069（`tellurion-docs:docs/archive/odr/`）；本轮只立单不修复。此前 QT-44 ✅（2026-11-10：X0 生产者 `analysisartifact.identityBody` → `POST /api/runs/jobs/{id}/artifacts` 真 run 写入三 hash；R5 鉴权由 SVC-ORCH-G2 ④ 补齐；worker 生产装配以真引擎 + 真行情 batch/walkforward 跑通；本仓零 import core 实测零命中；⑤-2 计数分歧为已登记代裁，待主任追认）；SVC-ORCH-G3 ✅（2026-11-10 收口：真栈真引擎 + 真行情跑通 batch 2 子 run 与 walkforward 1 窗 2 段，父记录双 `completed`、子结果含真实 metrics/`portfolio_values`；同轮取证 ⑤-1/⑤-2；⑤-3 为已登记代裁，待主任追认）；SVC-ORCH-G3 ⑤-4 取证（2026-11-10：真 orchestration 进程 + 真 PG 的 batch 2 子 run 与 walkforward 3 窗 / 6 段同轮 PASS，`go test ./internal/orchestrationworker -count=1 -v` EXIT=0；破坏验证真做真还原；该腿 runner 为 echoRunner）；SVC-ORCH-G1 ✅（2026-11-09：`contracts v0.9.0` 远端可解析、claim/provenance/父作业请求改引 `contracts/run`、逐字节 golden 三条、core 客户端与 analytics 调用方同批收编、全量 R5 16 条路由 service 角色 + 路由普查守卫）；SVC-ORCH-G4 ✅；SVC-ORCH-G2 ✅
---

# TASKS — tellurion-orchestration 未完成项

> **归档说明（2026-11-11）**：本文件已瘦身为「只装未做的事」。2026-11-10 及以前已收口（✅）的任务原文、施工记录、验收输出与破坏验证，已逐字移入 [`docs/archive/tasks-archive-2026-11.md`](archive/tasks-archive-2026-11.md)；该文件顶部有「检索索引」表（ID / 标题 / 收口日期 / commit / 证据链接）。检索：`grep -n "<任务ID或commit>" docs/archive/tasks-archive-2026-11.md`。
>
> 状态：`⬜ 待做` / `🔶 进行中` / `⛔ 阻塞（有未解除前置）` / `⏸ 挂起（外部依赖或已裁决暂不实施；**不计入开放项**，重开须另立单并满足该条「重开条件」）` / `✅ 完成（附日期，随即移入归档）`

## 一、未做 / 挂起 / 外部依赖

### ORCH-RATELIMIT · 编排服务无身份限流，429 不带 Retry-After [P2] ⬜

- **① 目标**：让本仓与四个兄弟服务（data/risk/analytics/strategy）一样有**按调用方身份的令牌桶限流**，且所有 429 响应带 `Retry-After`，使调用方能区分「限流/队列满」与「服务坏」并知道何时重试。
- **② 上下文（2026-11-11 全面审查 W2-F6，证据 ODR-069 `tellurion-docs:docs/archive/odr/odr-069-audit-wave2-core-architecture.md`）**：本仓 `internal/httpapi/` **无 `limit.go`**（data/risk/analytics/strategy 四仓均有按身份令牌桶，默认 50 rps / 突发 200、探针豁免、鉴权先于限流）；本仓唯一 429 是 `queue_full`（`internal/httpapi/api.go:351-352` `case errors.Is(err, queue.ErrQueueFull): return http.StatusTooManyRequests, "queue_full"`），**不带 `Retry-After`**（grep 全 httpapi 零命中）。对照 risk `internal/httpapi/auth.go:96` 限流 429 带 `Retry-After: 1`。冻结参照 = 兄弟仓 api-schema 的限流口径（如 `tellurion-data:docs/api-schema.md` 限流节）与本仓 `docs/api-schema.md` wire 词表。
- **③ 要求**：① 增按身份令牌桶限流（身份取值口径与兄弟仓一致：`username`→`subject`→`open-access` 共用一桶；默认 50 rps / 突发 200；`/healthz` `/readyz` 探针豁免）；② **鉴权先于限流**（401/403 不被 429 顶替）；③ 新增 `rate_limited` 429 与既有 `queue_full` 429 **都带 `Retry-After`**；④ `docs/api-schema.md` 登记 `rate_limited` 码与两码的 `Retry-After` 口径；⑤ 限流参数可配（与兄弟仓同口径，不硬编码死）。
- **④ 约束与非目标**：不改队列语义 / `Claim` / `FOR UPDATE SKIP LOCKED` / 父作业汇总；不引入 MQ；不改鉴权策略表（16 条路由 service 角色不动）；不把 `queue_full` 改成别的 status；不新增第三方依赖前先核兄弟仓用的是哪个限流库（优先复用同款，避免第二套）。
- **⑤ 验收**：用例断言超限 ⇒ 429 `rate_limited` + `Retry-After` 非空、`queue_full` ⇒ 429 + `Retry-After` 非空、探针不限流、401 不被 429 顶替；破坏验证 = 去掉 `Retry-After` 设置 ⇒ 用例红，还原复绿；`go build/vet/test ./... -count=1` 全绿。
- **⑥ 边界**：本仓 `internal/httpapi/`（新增 limit + 改 429 写头）+ `docs/api-schema.md`；broker 的 `rate_limited` 缺 Retry-After 归 `tellurion-broker:docs/TASKS.md` → `BROKER-RETRY-AFTER`。
- **owner**：待定

**其余无未做 / 挂起 / 外部依赖项**（截至 2026-11-11 台账重整 + 全面审查 W7 立单）。已收口单的正文与施工记录全部在归档文件里；新增工作一律另立单。

## 二、跨仓指针（不在本仓，勿双写）

| 事项 | 去哪 |
|---|---|
| 执行侧契约与闸门 | `tellurion:docs/TASKS.md` |
| 编排 wire 契约 | `tellurion-contracts:docs/TASKS.md` |
| 分析服务调用面 | `tellurion-analytics:docs/TASKS.md` |

## 维护本文件

- **新增单**：写全六字段；六字段里必须显式引用**冻结契约的路径与节号**（契约仓的包路径、设计文档的节号），不许写"参考相关实现"。
- **派工前先证伪前提**：读码核对工单写的前提；不符就停下回报 + 给拆法，不硬做、不扩范围。
- **跨仓引用写 `repo:path`**（如 `tellurion:docs/TASKS.md`），不做可点击相对链接。
- **顺序纪律**：同一波次内撞同一批文件的单按单号串行，一单一 commit。
