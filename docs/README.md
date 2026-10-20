# 文档导航 — tellurion-orchestration

> 本目录只讲**本仓**。跨仓的拓扑、阶梯与全局决策读 `tellurion:docs/`（见文末）。
> 跨仓引用一律写成 `repo:path` 纯文本，**不做可点击相对链接**——独立 checkout 时 `../../` 必断。

| 文档 | 用途 | 什么时候读 |
|---|---|---|
| [`../AGENTS.md`](../AGENTS.md) | 本仓入口：构建命令 / 边界 / 禁止项 / 依赖的契约版本 | **第一次进本仓，必读** |
| [`TASKS.md`](TASKS.md) | 本仓任务台账（**唯一**） | 开工前；判断"这活归不归我" |
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | 本仓边界、依赖方向、关键约束 | 改结构；判断某个类型/文件该不该放这 |
| [`adr/`](adr/README.md) | 本仓决策记录（只增不删） | 想知道"为什么这样设计" |
| [`archive/`](archive/README.md) | 已完成任务与历史文档 | 查历史、避免重复踩坑 |

## 三条维护规则

1. **台账唯一**：任务只写 `TASKS.md`，别的文档只留指针，不镜像正文（双写必然漂移）。
2. **ADR 只增不删**：方案变更时新增一条并注明替代哪条，保留追溯链。
3. **文档要能被单仓验证**：任何需要跳出本仓才能点开的链接都不写；引用别的仓用 `tellurion:path` 这种纯文本形式。

## 跨仓只读资料（不在本仓维护）

| 主题 | 位置 |
|---|---|
| 仓清单与命名规则 | `tellurion:docs/design/qt-decoupling/repo-inventory.md` |
| 分仓拓扑与 S0~S3 阶梯 | `tellurion:docs/design/qt-decoupling/repo-topology-and-naming.md` |
| 全局决策 / ADR-D 索引 | `tellurion:docs/design/qt-decoupling/project-engineering-file.md` |
| 跨仓任务总台账 | `tellurion:docs/TASKS.md` |
