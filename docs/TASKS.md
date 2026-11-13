---
status: active
last-verified: 2026-11-13
verified-by: 2026-11-13 五角审查档 4 立单（只立单不施工）——新立 `ORCH-CI-GATES`；此前：2026-11-12 `X-DEP-BUMP-CONSUMERS` 回补 `govulncheck` 门（首轮 `d093481` 只升 pin 未加门，`4a7112c` 补 CI 步并真做降版破坏验证）；ORCH-RATELIMIT ✅（按调用方身份令牌桶 + 所有 429 带 Retry-After；鉴权先于限流、探针豁免；真做破坏验证后还原复绿；正文与施工记录已移入归档）；2026-11-11 全面审查 W7 台账立单（A5 授权）——新立 1 条六字段单（`ORCH-RATELIMIT`），证据坐标引 ODR-069（`tellurion-docs:docs/archive/odr/`）；本轮只立单不修复。此前 QT-44 ✅（2026-11-10：X0 生产者 `analysisartifact.identityBody` → `POST /api/runs/jobs/{id}/artifacts` 真 run 写入三 hash；R5 鉴权由 SVC-ORCH-G2 ④ 补齐；worker 生产装配以真引擎 + 真行情 batch/walkforward 跑通；本仓零 import core 实测零命中；⑤-2 计数分歧为已登记代裁，待主任追认）；SVC-ORCH-G3 ✅（2026-11-10 收口：真栈真引擎 + 真行情跑通 batch 2 子 run 与 walkforward 1 窗 2 段，父记录双 `completed`、子结果含真实 metrics/`portfolio_values`；同轮取证 ⑤-1/⑤-2；⑤-3 为已登记代裁，待主任追认）；SVC-ORCH-G3 ⑤-4 取证（2026-11-10：真 orchestration 进程 + 真 PG 的 batch 2 子 run 与 walkforward 3 窗 / 6 段同轮 PASS，`go test ./internal/orchestrationworker -count=1 -v` EXIT=0；破坏验证真做真还原；该腿 runner 为 echoRunner）；SVC-ORCH-G1 ✅（2026-11-09：`contracts v0.9.0` 远端可解析、claim/provenance/父作业请求改引 `contracts/run`、逐字节 golden 三条、core 客户端与 analytics 调用方同批收编、全量 R5 16 条路由 service 角色 + 路由普查守卫）；SVC-ORCH-G4 ✅；SVC-ORCH-G2 ✅
---

# TASKS — tellurion-orchestration 未完成项

> **归档说明（2026-11-11）**：本文件已瘦身为「只装未做的事」。2026-11-10 及以前已收口（✅）的任务原文、施工记录、验收输出与破坏验证，已逐字移入 [`docs/archive/tasks-archive-2026-11.md`](archive/tasks-archive-2026-11.md)；该文件顶部有「检索索引」表（ID / 标题 / 收口日期 / commit / 证据链接）。检索：`grep -n "<任务ID或commit>" docs/archive/tasks-archive-2026-11.md`。
>
> 状态：`⬜ 待做` / `🔶 进行中` / `⛔ 阻塞（有未解除前置）` / `⏸ 挂起（外部依赖或已裁决暂不实施；**不计入开放项**，重开须另立单并满足该条「重开条件」）` / `✅ 完成（附日期，随即移入归档）`

## 一、未做 / 挂起 / 外部依赖

#### 2026-11-13 五角审查 · 档 4（测试）

### ORCH-CI-GATES · CI 起 PG 并设覆盖率门（4 条 DB 用例不再 skip）[P2] ⬜

> 档 4（测试）· 来源：03-testing P1-3 / P2-2 · 顺序 4-8 · 依赖：core 伞 `X-COVERAGE-FLOOR`

- **① 目标**：CI 真跑 DB 腿（4 条按设计 skip 的用例），并补覆盖率门。
- **② 上下文**：Test job 无 postgres service / 无 DSN，库侧用例长期 skip；DSN 变量名与迁移入口施工时以测试代码为准。
- **③ 要求**：① 加 `postgres:16` service + healthcheck；② 注入 DSN；③ Test 前应用本仓迁移；④ 本地无 DSN 仍 skip；⑤ 覆盖率门 = 实测 - 5。
- **④ 约束与非目标**：不改业务代码；不把 skip 改成假通过；不顺手改编排语义。
- **⑤ 验收**：CI 中 DB 用例 PASS（数量 > 0）；删测试 ⇒ 门红；本地无 DSN ⇒ skip 且原因可读。
- **⑥ 边界**：本仓 CI + 测试夹具。

**其余无未做 / 挂起 / 外部依赖项**（截至 2026-11-13：ORCH-RATELIMIT 已收口并移入归档；2026-11-13 五角审查档 4 新立 `ORCH-CI-GATES`）。已收口单的正文与施工记录全部在归档文件里；新增工作一律另立单。

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
