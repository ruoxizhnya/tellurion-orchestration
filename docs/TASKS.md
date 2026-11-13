---
status: active
last-verified: 2026-11-13
verified-by: 2026-11-13 `ORCH-CI-GATES` 收口 ✅——CI 起 `postgres:16` service + `pg_isready`、job 级注入 `ORCHESTRATION_TEST_DSN`（临时库 `tellurion_orchestration_test`，非部署库），库侧 **15 条**用例由 SKIP 转 PASS（带 DSN 168 PASS/0 SKIP，无 DSN 153 PASS/15 SKIP；单内「4 条」为旧口径，已按实测改正），迁移走 `internal/pgstore` 内嵌 DDL（CI 不另设迁移步）；覆盖率门 **51%** = 隔离 PG 实测 **56.4%** 取整 56 - 5，漏配 DSN 时实测 43.0% < 51 直接红，单文件最大影响 3.4 点（`internal/httpapi/workers_test.go` ⇒ 53.0%）故 5 点余量按防累计漂移定位；此前：2026-11-13 五角审查档 4 立单（只立单不施工）——新立 `ORCH-CI-GATES`；此前：2026-11-12 `X-DEP-BUMP-CONSUMERS` 回补 `govulncheck` 门（首轮 `d093481` 只升 pin 未加门，`4a7112c` 补 CI 步并真做降版破坏验证）；ORCH-RATELIMIT ✅（按调用方身份令牌桶 + 所有 429 带 Retry-After；鉴权先于限流、探针豁免；真做破坏验证后还原复绿；正文与施工记录已移入归档）；2026-11-11 全面审查 W7 台账立单（A5 授权）——新立 1 条六字段单（`ORCH-RATELIMIT`），证据坐标引 ODR-069（`tellurion-docs:docs/archive/odr/`）；本轮只立单不修复。此前 QT-44 ✅（2026-11-10：X0 生产者 `analysisartifact.identityBody` → `POST /api/runs/jobs/{id}/artifacts` 真 run 写入三 hash；R5 鉴权由 SVC-ORCH-G2 ④ 补齐；worker 生产装配以真引擎 + 真行情 batch/walkforward 跑通；本仓零 import core 实测零命中；⑤-2 计数分歧为已登记代裁，待主任追认）；SVC-ORCH-G3 ✅（2026-11-10 收口：真栈真引擎 + 真行情跑通 batch 2 子 run 与 walkforward 1 窗 2 段，父记录双 `completed`、子结果含真实 metrics/`portfolio_values`；同轮取证 ⑤-1/⑤-2；⑤-3 为已登记代裁，待主任追认）；SVC-ORCH-G3 ⑤-4 取证（2026-11-10：真 orchestration 进程 + 真 PG 的 batch 2 子 run 与 walkforward 3 窗 / 6 段同轮 PASS，`go test ./internal/orchestrationworker -count=1 -v` EXIT=0；破坏验证真做真还原；该腿 runner 为 echoRunner）；SVC-ORCH-G1 ✅（2026-11-09：`contracts v0.9.0` 远端可解析、claim/provenance/父作业请求改引 `contracts/run`、逐字节 golden 三条、core 客户端与 analytics 调用方同批收编、全量 R5 16 条路由 service 角色 + 路由普查守卫）；SVC-ORCH-G4 ✅；SVC-ORCH-G2 ✅
---

# TASKS — tellurion-orchestration 未完成项

> **归档说明（2026-11-11）**：本文件已瘦身为「只装未做的事」。2026-11-10 及以前已收口（✅）的任务原文、施工记录、验收输出与破坏验证，已逐字移入 [`docs/archive/tasks-archive-2026-11.md`](archive/tasks-archive-2026-11.md)；该文件顶部有「检索索引」表（ID / 标题 / 收口日期 / commit / 证据链接）。检索：`grep -n "<任务ID或commit>" docs/archive/tasks-archive-2026-11.md`。
>
> 状态：`⬜ 待做` / `🔶 进行中` / `⛔ 阻塞（有未解除前置）` / `⏸ 挂起（外部依赖或已裁决暂不实施；**不计入开放项**，重开须另立单并满足该条「重开条件」）` / `✅ 完成（附日期，随即移入归档）`

## 一、未做 / 挂起 / 外部依赖

#### 2026-11-13 五角审查 · 档 4（已收口）

> `ORCH-CI-GATES` 已 ✅（2026-11-13），原文与施工记录在归档「2026-11-13 收口」节，索引见归档顶部检索表。

**其余无未做 / 挂起 / 外部依赖项**（截至 2026-11-13：ORCH-RATELIMIT 已收口并移入归档；`ORCH-CI-GATES` 同日收口并移入归档）。已收口单的正文与施工记录全部在归档文件里；新增工作一律另立单。

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
