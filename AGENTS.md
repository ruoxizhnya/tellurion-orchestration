# AGENTS — tellurion-orchestration

## 这是什么仓

编排与 worker：批量回测（`batch`）、walk-forward（`walkforward`）、任务队列与调度。

状态：**预留（触发式）**——当前为占位仓，**未启动**，且 `cmd/dualrun` 归属未裁决。

## 构建 / 测试

```bash
go build ./...
go vet ./...
go test ./...
```

本仓必须**单独 checkout 就能跑通**这三条（R1/R2）。若某一步需要别的仓的源码目录，
那是**契约画错了**，不是正常状态——停下来先修边界，不要加 `replace ../xxx`。

## 边界（硬约束）

允许：
- 编排多个回测 run、管理 worker 池与队列
- 持有 `run` schema 的编排侧表（run 元数据、任务状态）

禁止：
- 自己实现回测（必须调 core 的 `EngineRunner`）
- 承载 per-bar 逻辑
- `replace` 指向仓外路径；`go.work*` 提交入库。
- 引入未在本文件声明的第三方依赖（先改本文件，再改代码）。

## 依赖的契约版本

| 依赖 | 版本 | 说明 |
|---|---|---|
| `github.com/ruoxizhnya/tellurion-contracts` | 待定（S2 后 `v0.1.0`） | 契约变更只走 tag，不走源码路径 |
| `github.com/ruoxizhnya/tellurion-kit` | 待定（S2 后 `v0.1.0`） | 同上 |

## 该做什么

**唯一台账是 `tellurion` 仓的 `docs/TASKS.md`。** 本仓没有独立 TASKS，
跨仓只留指针、不镜像正文（避免双写漂移）。

开工前先读：
1. `tellurion:docs/design/qt-decoupling/repo-inventory.md` —— 仓名与 module 规则
2. `tellurion:docs/TASKS.md` —— 找本仓的单号；没有单号就别开工
3. `tellurion:docs/design/qt-decoupling/p0-round2-review.md` §4 —— wave 顺序
