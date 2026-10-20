# ADR — tellurion-orchestration 的决策记录

> 只记**本仓**的决策。跨仓/全局决策在 `tellurion:docs/design/qt-decoupling/project-engineering-file.md`，
> 不要在这里抄一遍（双写必然漂移）。
> 与 ODR 的分工见 [`../odr/README.md`](../odr/README.md)。

## 什么时候写一条

1. 做了一个**以后会被问「为什么」**的选择（依赖方向、分层、命名、存储形态）；
2. 推翻 / 替换了本仓既有决策（**新增一条**并注明替代哪条，不删旧的）；
3. 接受了一个已知代价（写清代价，否则半年后没人知道这是有意为之）。

不写：实现细节、临时 workaround、一次性调试结论。

## 编号与命名

`NNNN-kebab-title.md`，四位递增。`0000-template.md` 是模板，不编号。

## 索引

| 编号 | 标题 | 状态 |
|---|---|---|
| [0001](0001-只派活不裁决.md) | 只派活不裁决 | 已裁决 |
