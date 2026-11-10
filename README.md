# tellurion-orchestration

> module: `tellurion-orchestration` ｜ 状态：**已收口（2026-11-11 台账清零）**——X5a/X5c（`JobQueue` + `WorkerRegistry` + `run` schema DDL + PG 实现 + 启动迁移）、X5b（`internal/plan` + `run.parent_jobs` + 父 2 端点 + 子 run 入队）、X0（`run_artifacts` 读写 + `/provenance` + artifact 写端点）、worker 打活面（8 端点）全部已落；全量 R5 16 条路由 service 角色、`/readyz`、失败语义五类见 `docs/runbook.md`。`cmd/dualrun` 已裁决留 core，不在本仓

编排 / worker 服务：job / batch / walkforward 的算力出口，schema `run`。把"批量扫"从 core 进程里搬出来。

## 目录

| 路径 | 内容 |
|---|---|
| `internal/queue/` | `JobQueue` / `WorkerRegistry` port + 内存实现 + `run` schema DDL（X5a/X5c） |
| `internal/pgstore/` | 同一对 port 的 PostgreSQL 实现 + `Open`/`Migrate`（驱动只进这一个包） |
| `internal/qtest/` | 语义套件：内存与 PG **共用**的 45 条用例（每个实现各跑一遍；判据「换实现行为不变」） |
| `internal/testdb/` | PG 腿的 harness：跨测试进程排队 + 每用例前清库 |
| `internal/httpapi/` | jobs 4 端点的 HTTP 面（X5a 的传输层） |
| `internal/plan/` | batch 网格展开 / walkforward 窗口切分 / 三值汇总与父状态（X5b） |
| `internal/plansuite/` | 规划 / 汇总共享套件（同跑 batch 与 walkforward） |
| `cmd/orchestrationservice/` | 进程装配：DSN fail-closed → 启动迁移 → 监听 → 周期租约回收 |
| `docs/` | 本仓文档：台账 / 边界 / 决策 / 归档 —— 从这里进：`docs/README.md` |
| `AGENTS.md` | 本仓入口（构建、边界、禁止、依赖版本） |
| `doc.go` | 根包说明（实现按能力拆子包） |

## 快速开始

```bash
go build ./...
go vet ./...
go test ./...
```

本仓**单独 checkout 就能跑通**上面三条。需要别的仓源码才过 → 边界错了，不是加 `replace` 能解决的问题。

跑起来：

```bash
# 本机直跑（DSN 必填；空 DSN 按设计 fail-closed 拒绝启动）
ORCHESTRATION_DSN='postgres://postgres:postgres@127.0.0.1:5432/tellurion?sslmode=disable' \
  go run ./cmd/orchestrationservice -addr 127.0.0.1:8088

# 或容器（单仓 compose；PG 默认 host.docker.internal:5432）
docker compose -f deploy/compose.yaml up --build
```

## 边界

**可以**：job / batch / walkforward 的编排与重试；持有 `run` schema 的 job 元数据；按算力配额分片调度

**不可以**：在编排层做业务裁决（它只负责"把活派下去"）；import core 的实现包；持有回测结果真相（结果归 core 的 state store）

## 文档

- 文档导航：`docs/README.md`
- 本仓台账：`docs/TASKS.md`（唯一；跨仓任务只留指针）
- 边界与依赖方向：`docs/ARCHITECTURE.md`
- 决策记录：`docs/adr/`

## 许可

MIT（见 `LICENSE`）。
