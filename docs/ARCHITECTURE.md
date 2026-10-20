# 本仓边界与依赖方向 — tellurion-orchestration

## 定位

编排 / worker 服务：job / batch / walkforward 的算力出口，schema `run`。把"批量扫"从 core 进程里搬出来。

状态：**预留（S0 占位，触发式启动）——尚未启动，无实现；`cmd/dualrun` 归属未裁决**

## 依赖方向（编译器强制）

```
contracts + kit
        ↑
tellurion-orchestration  ──(派活/收结果)──▶  core 引擎
```

- 箭头 = import 方向。**不允许反向**，也不允许成环 —— 反向 import 应当**编译失败**，这是判据不是口号。
- 跨仓只认版本号：`tellurion-orchestration` 以需要的版本出现在下游 `go.mod`。

## 边界表

| | 内容 |
|---|---|
| 允许 | job / batch / walkforward 的编排与重试；持有 `run` schema 的 job 元数据；按算力配额分片调度 |
| 禁止 | 在编排层做业务裁决（它只负责"把活派下去"）；import core 的实现包；持有回测结果真相（结果归 core 的 state store） |

## 关键约束

1. **单仓可构建**：`go build ./...` / `go vet ./...` / `go test ./...` 不依赖任何兄弟目录。
2. **无 `replace`**：指向仓外的 `replace` 一律不接受（CI 会查）。
3. **无 `go.work` 入库**：本地联调可以建，提交即错。
4. **只派活不裁决**：编排层不解释结果、不改结果、不重算指标。

## 未决 / 待定

- `cmd/dualrun`（K15 双跑）归属本仓还是 core，**尚未裁决**——落地前不要先搬。
