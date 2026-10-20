# tellurion-orchestration

> module: `tellurion-orchestration` ｜ 状态：**预留（触发式）**——当前为占位仓，**未启动**，且 `cmd/dualrun` 归属未裁决。

编排与 worker：批量回测（`batch`）、walk-forward（`walkforward`）、任务队列与调度。

⚠️ `cmd/dualrun`（K15 双内核对拍）**归属未定**：它必须能同时链接 Go 与 NT 两个内核，既像编排能力，又更像必须留在 core 的东西。**评估 X5 之前必须先回答它归谁**，否则双跑会在拆分中失去运行位置。

## 边界

**可以**
- 编排多个回测 run、管理 worker 池与队列
- 持有 `run` schema 的编排侧表（run 元数据、任务状态）

**不可以**
- 自己实现回测（必须调 core 的 `EngineRunner`）
- 承载 per-bar 逻辑

## 依赖方向

```
tellurion-orchestration
```

- 契约与工具一律以 **tag** 依赖（`github.com/ruoxizhnya/tellurion-contracts` / `-kit`），
  **禁止 `replace` 指向仓外路径**（单仓不可构建）。
- `go.work` 一律不入库。

## 施工入口

本仓所有任务登记在 **`tellurion` 仓的 `docs/TASKS.md`**（单一台账，避免双写）。
在台账出现本仓的单号之前，**不要在此写实现**。

## 权威文档

| 主题 | 位置 |
|---|---|
| 仓清单与命名 | `tellurion:docs/design/qt-decoupling/repo-inventory.md` |
| 分仓拓扑与阶梯 | `tellurion:docs/design/qt-decoupling/repo-topology-and-naming.md` |
| P0 修复基线 | `tellurion:docs/design/qt-decoupling/p0-round2-review.md` |
