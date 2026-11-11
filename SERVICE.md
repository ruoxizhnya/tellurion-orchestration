---
service-card: 1
repo: tellurion-orchestration
kind: service
lifecycle: production
owner: ruoxizhnya
impact: degrading

requires:
  - tellurion-contracts
  - tellurion-kit
required-by: []
calls: []
called-by:
  - tellurion
  - tellurion-analytics

truth:
  readme: README.md
  agents: AGENTS.md
  architecture: docs/ARCHITECTURE.md
  api: docs/api-schema.md
  db: docs/db-schema.md
  dataflow: docs/dataflow.md
  runbook: docs/runbook.md
  tasks: docs/TASKS.md

status: evergreen
last-verified: 2026-11-11
verified-by: 2026-11-11 实读 README / AGENTS / ARCHITECTURE / api-schema / db-schema / dataflow / runbook / go.mod / deploy/compose.yaml；端口、端点数量、表数量未写入
---

# 服务卡：tellurion-orchestration

> **一句话**：任务域——batch / walkforward 的父计划、子 run 入队与租约回收，存指针不存结果内容。
>
> `service` · `production` · `degrading` · `ruoxizhnya`

## 1. 这是什么

它把「批量扫」从 core 进程里搬出来：接收父计划（batch 网格 / walkforward 窗口），展开成子 run 入队，接受 worker 注册 / 心跳 / 领取 / 续租 / 上报，并在每个子 run 终态时重算父汇总。worker 进程本身在 core，本仓零 import core。

**它拥有**：`run` schema 的 DDL、启动迁移与唯一写权（job 元数据、父计划、artifact 指针）；`run.backtest_jobs.status` 的唯一状态词表。
**它不拥有**：回测结果内容、策略运行时、`strategy_state`、行情与因子、风控裁决。

## 2. 它不做什么

- 不在编排层做业务裁决：只负责把活派下去，不解释结果、不重算指标。
- 不持有回测结果内容：本仓只存指针与三 hash，结果归 core 的 state store。
- 不放 worker 进程：worker 在 core 的 `cmd/worker`，本仓只提供领活协议。
- 不接管 `strategy_state`（2026-10-21 裁决留 core）与 `cmd/dualrun`（同进程链接两内核，留 core）。
- 不 import core 的实现包；不写 core / data / risk / strategy 的任何表。

## 3. 与谁相连

### 调用它（入边）

| 来源 | 协议 | 用来做什么 | 本仓挂了会怎样 |
|---|---|---|---|
| `tellurion` | HTTP | worker 注册 / 心跳 / 领取 / 续租 / 上报 / 取消确认；提交父计划 | 异步批量与 walkforward 停摆；同步回测与交易主链路不受影响 |
| `tellurion-analytics` | HTTP | 提交 / 查询敏感性逐点回填的父作业 | 敏感性回填停在未完成态；分析报告计算本身不受影响 |

### 它调用（出边）

| 目标 | 协议 | 用来做什么 | 目标挂了会怎样 |
|---|---|---|---|
| PostgreSQL | PG | `run.*` 的 DDL、启动迁移、队列领取（`FOR UPDATE SKIP LOCKED`）与汇总写回 | 启动期 DSN 缺失或迁移失败即 **fail-closed 拒起**；运行期队列 503 |

> 不写 URL、端口、超时、租约参数。地址见 `deploy/compose.yaml`；租约 / 回收参数口径见 runbook §3。

## 4. 运行时身份

- **常驻进程**：`cmd/orchestrationservice`（唯一 HTTP 监听）；进程内周期执行租约回收与离线 worker 回收。
- **存活/就绪语义**：
  - `GET /healthz`：进程存活。
  - `GET /readyz`：PG 池可连 **且** `run.*` 迁移完成；未就绪 503。
- **鉴权**：全量路由挂 service 角色闸；未装配即拒绝，不放行。
- **部署真相**：`deploy/compose.yaml`、`Dockerfile`、`README.md` 快速开始。
- **端口**：不在此卡；见上述部署真相。

## 5. 数据与契约归属

| 维度 | 归属 | 真相源 |
|---|---|---|
| DB schema | `run` 的唯一写者与 DDL | `docs/db-schema.md` |
| 跨仓 DTO / 契约 | 形状归 `tellurion-contracts/run`；本仓不复刻 `ClaimView` | `tellurion-contracts:docs/API.md` |
| 对外 HTTP | jobs / parents / provenance / worker 打活面 / artifact 写 | `docs/api-schema.md` |

> 只写「拥有哪块」，不写表名清单、端点数量、字段枚举。

## 6. 坏了怎么办

- **影响面**：`degrading`——异步批量 / walkforward / 敏感性回填停摆；同步回测、交易与前端主链路不受影响。DSN 缺失是拒起而不是「无库降级」。
- **先确认（按顺序）**：
  1. `GET /healthz` 是否 200；不是则看启动日志里的 DSN fail-closed 判决。
  2. `GET /readyz` 是否 200；503 时看 PG 连通与 `run.*` 迁移。
  3. 作业卡在 `running` 看租约与回收参数、worker 心跳；按 runbook §3「回收参数」与 §6「人工释放 SQL」走。
- **升级路径**：`owner` → 跨仓争议 → core `docs/TASKS.md`
- **runbook**：`docs/runbook.md`（起停 §1~§2、回收参数 §3、失败语义 §4、人工释放 §6）。

## 7. 5 分钟后去哪

| 我想…… | 读这个 |
|---|---|
| 知道 jobs / parents / worker 打活面的形状 | `docs/api-schema.md` |
| 知道 `run.*` 表与状态词表 | `docs/db-schema.md` |
| 知道子 run 怎么入队、父汇总怎么算 | `docs/dataflow.md` |
| 起停 / 租约回收 / 人工释放 SQL | `docs/runbook.md` |
| 当前在做什么 | `docs/TASKS.md` |
| 架构与决策 | `docs/ARCHITECTURE.md` / `docs/adr/` |
