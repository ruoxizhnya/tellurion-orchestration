---
status: active
last-verified: 2026-10-20
verified-by: 功能边界固化（三份契约：db / api / dataflow）
---

# 文档导航 — {name}

> 本目录只讲**本仓**。跨仓拓扑、阶梯与全局决策读 `tellurion:docs/`（见文末）。
> 跨仓引用一律写成 `repo:path` 纯文本，**不做可点击相对链接**——独立 checkout 时 `../../` 必断。

## 1. 先读哪份

| 你想干什么 | 读这份 |
|---|---|
| 第一次进本仓 | [`../AGENTS.md`](../AGENTS.md) |
| 开工前找活 | [`TASKS.md`](TASKS.md)（**唯一台账**） |
| 确认"这个功能到底归不归我" | [`ARCHITECTURE.md`](ARCHITECTURE.md) 的定位与「三份契约」 |
| 改表 / 加列 | [`db-schema.md`](db-schema.md) |
| 加 / 改接口 | [`api-schema.md`](api-schema.md) |
| 改数据流向或顺序 | [`dataflow.md`](dataflow.md) |
| 想知道"为什么这样设计" | [`adr/`](adr/README.md) |
| 要做删档 / 迁移 / 流程变更 | [`odr/`](odr/README.md) |
| 查历史 | [`archive/`](archive/README.md) |

## 2. 文档地图

| 文件 | 职责 | 谁维护 |
|---|---|---|
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | 高层设计：分层、组件、依赖方向、非目标 | 改结构的人 |
| [`db-schema.md`](db-schema.md) | **表结构唯一真相源** + 所有权声明 | 改表的人 |
| [`api-schema.md`](api-schema.md) | **对外接口唯一真相源** + 错误码 / 幂等 / 兼容 | 改接口的人 |
| [`dataflow.md`](dataflow.md) | 数据流向、时序、fail-closed 点、不变量 | 改链路的人 |
| [`TASKS.md`](TASKS.md) | 可执行任务（六字段） | 每个人 |
| [`adr/`](adr/README.md) | 架构决策（只增不删） | 提变更的人 |
| [`odr/`](odr/README.md) | 运行 / 治理决策（删档、审计、迁移、工具） | 做事的人 |
| [`archive/`](archive/README.md) | 已完成任务与历史文档 | 收尾的人 |

## 3. 变更触发（代码动了，文档必须跟着动）

| 代码变更 | 必须更新的文档 |
|---|---|
| 建表 / 加列 / 加索引 / 改主键 | `db-schema.md`（+ 结构级变更立 ADR） |
| 新增 / 修改 / 废弃接口 | `api-schema.md`（错误码与幂等两节要一起看） |
| 新增数据源、改聚合口径、改顺序 | `dataflow.md` |
| 改分层、依赖方向、模块职责 | `ARCHITECTURE.md` + 一条 ADR |
| 删档 / 迁移 / 换 CI / 换工具链 | 一条 ODR |
| 完成一张单 | `TASKS.md` 标 ✅，整条移入 `archive/` |

> 这条表的作用不是提醒，是**评审时的检查项**：PR 里改了上表左列，右列没动，就该被打回。

## 4. 跨仓只读资料（不在本仓维护）

| 主题 | 位置 |
|---|---|
| 仓清单与命名规则 | `tellurion:docs/design/qt-decoupling/repo-inventory.md` |
| 分仓拓扑与 S0~S3 阶梯 | `tellurion:docs/design/qt-decoupling/repo-topology-and-naming.md` |
| 全局决策 / ADR-D 索引 | `tellurion:docs/design/qt-decoupling/project-engineering-file.md` |
| 数据归属总表（DDD 分库） | `tellurion:docs/design/qt-decoupling/data-service-contract-and-ownership.md` |
| 跨仓任务总台账 | `tellurion:docs/TASKS.md` |
