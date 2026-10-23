# tellurion-orchestration

> module: `tellurion-orchestration` ｜ 状态：**已启动（施工 ①）**——X5a/X5c 已落 `internal/queue`；X5b（batch/walkforward）与 X0（`run_artifacts`/`provenance`）未做（`cmd/dualrun` 已裁决留 core，不在本仓）

编排 / worker 服务：job / batch / walkforward 的算力出口，schema `run`。把"批量扫"从 core 进程里搬出来。

## 目录

| 路径 | 内容 |
|---|---|
| `internal/queue/` | `JobQueue` / `WorkerRegistry` port + 内存实现 + `run` schema DDL（X5a/X5c） |
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
