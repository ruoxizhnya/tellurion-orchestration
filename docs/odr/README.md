# ODR — tellurion-orchestration 的运行与治理决策记录

> **ADR 与 ODR 的分工**（沿用主仓 `tellurion:AGENTS.md` §10）：
> - **ADR**（`docs/adr/`）＝ 架构决策。它改变**系统的形状**：边界、依赖方向、分层、数据归属。写了就要能改变代码结构。
> - **ODR**（本目录）＝ 运行 / 治理决策。它改变**做事的方式**：删档、审计、迁移、工具与流程。不改系统形状。
>
> 判断口径：**"这条决定会不会让某段代码搬到别处？"会 → ADR；不会 → ODR。**

## 什么时候必须写

| 操作 | 类别 | 时机 |
|---|---|---|
| 删除 / 归档任何文档 | Cleanup | 操作后立即 |
| 审计、排查、复盘 | Audit | 完成后 48h 内 |
| 文档或代码目录迁移 | Migration | 完成后 72h 内 |
| 变更开发工具 / CI / 流程 | Tooling/Process | 上线前 |
| 依赖大版本升级（跨 MAJOR） | Tooling/Process | 合并前 |

## 不写什么

- 任务本身 → 写 `docs/TASKS.md`
- 架构选择 → 写 `docs/adr/`
- 一次性调试结论、临时 workaround → 不进文档（留在 commit message 里）

## 模板

```markdown
# ODR-NNN: <一句话标题>

> **Status**: Proposed / Accepted / Completed / Deprecated
> **Date**: YYYY-MM-DD
> **Category**: Cleanup / Audit / Migration / Tooling-Process
> **Related ADRs**: adr-NNNN（没有就写「无」）
> **Supersedes**: odr-NNN（没有就删这行）

## Context
为什么需要这次操作？是什么触发的？

## Decision
决定了什么？为什么选它而不是别的？

## Consequences
正面与负面影响，以及谁需要知道。

## Artifacts
创建 / 修改 / 删除的文件清单（路径即可，不要贴全文）。

## Metrics
可量化的结果：文档数变化、行数变化、CI 时长、覆盖率等。

## Lessons Learned
下次会怎么做？这一节可以留空，但不要写"无"——写"暂无"也行。
```

## 编号与归档

- 本目录 `NNN-kebab-title.md` 三名递增（`001` 起），**只增不删**。
- 被取代的 ODR 标 `Deprecated` 并注明 `Supersedes` 关系，正文不动。
- 超过 12 个月且不再被引用的，移到 `docs/archive/odr/`。

## 索引

| 编号 | 标题 | 状态 | 日期 |
|---|---|---|---|
| — | （暂无） | — | — |
