# AGENTS — tellurion-orchestration

> 本文件是**本仓**的唯一入口。判据：一个 agent 只 checkout 本仓，就能知道「怎么构建、边界在哪、依赖哪版契约、活在哪、禁止什么」，**不需要读别的仓的源码**。

## 1. 这是什么仓

编排 / worker 服务：job / batch / walkforward 的算力出口，schema `run`。把"批量扫"从 core 进程里搬出来。

**状态**：已启动（2026-10-25 施工 ①②③④⑤⑥⑦⑧）——X5a/X5c 的语义（`JobQueue` + `WorkerRegistry` + `run` schema DDL）已落 `internal/queue`，jobs 4 端点的 HTTP 面已落 `internal/httpapi`，**DDL 已接 PG 并有启动迁移**（`internal/pgstore` + `cmd/orchestrationservice`）；R8（`cancelling` 行的回收出口）与 R9（`Kernels` 的领取过滤）两条已裁事项**代码已落两个实现**；X5b 的**规划与汇总**已落 `internal/plan`（网格展开 / 窗口切分 / 三值计数 / 父状态）并由 `internal/plansuite` 共享套件同跑两种父；**裁-13 的父表已落**——`run.parent_jobs`（`kind` + `plan_canonical`/`plan_hash`（唯一，计划身份）+ 三值汇总）+ `run.backtest_jobs.parent_job_id` 列 / 索引，提交写父表与汇总写回在 `internal/parents`（内存 + PG 两个 `ParentStore` 实现）；**施工 ⑧ 的父作业 2 端点已落**——`POST /api/runs/parents`（batch / walkforward 共用，同计划换键也返回同一父 id）与 `GET /api/runs/parents/{id}`（三值汇总 + 逐子状态，`ParentChildLister` 可选能力）。**未落**：子 run 入队、worker 执行（本仓没有引擎）、X0（`run_artifacts`/`provenance`）

**今天的实现**（本仓，2026-10-25）：`internal/queue` 的两个 port + 可选接口 `KernelAwareJobQueue` / `WorkerReaper` + `ParentStore` + 内存实现 + DDL 常量（6 表：含 `parent_jobs`）；`internal/pgstore` 的 PG 实现（`Claim`/`ClaimByKernel` 共用**一条** `claimSQL`，`FOR UPDATE SKIP LOCKED`；提交路径上一把 `pg_advisory_xact_lock` 让 `queue_full` 阈值在并发下精确；回收是 running + cancelling 两条语句；父表 `Parents` 独立 advisory lock）；`internal/qtest` 的**语义套件**（每个实现 45 条用例 = 队列 30 + 注册表 9 + 跨 port 6，内存与 PG 各跑一遍——「换实现行为不变」这句判据只有共享用例才可执行）；`internal/plan`（X5b 的网格展开 / 窗口切分 / 三值汇总纯函数 + 父计划 `Canonical`/`Hash`）与 `internal/plansuite`（8 条共享用例同跑 batch / walkforward）；`internal/parents`（提交写父表 + 汇总写回，复用 `internal/plan`）；`internal/testdb`（PG 腿跨进程排队）；`internal/httpapi` 的父作业 2 端点（`parents.go`：batch/wf 折装、字段点名、三值汇总 + 逐子状态）；`cmd/orchestrationservice`（DSN fail-closed → 迁移 → 监听 `:8088` → 周期 `ReapExpired` / `ReapOffline`）。**未落**：worker / provenance 端点、子 run 入队、worker 进程本身（所以 `ClaimByKernel` 目前只被套件与编译期断言消费）、鉴权（R5）。细节见 `docs/ARCHITECTURE.md` §9.8。

**迁出前的老实现**（core，待切读后停写）：`core:pkg/backtest/batch` + `core:pkg/backtest/walkforward` + `backtest_jobs` / `walk_forward_reports` 表。

**不搬的**：`cmd/dualrun`（K15 双跑）**已裁决留 core**——它要同进程链接两个内核，且是引擎的验收门，不是本仓的 job。见 core [repo-inventory §3](https://github.com/ruoxizhnya/tellurion/blob/main/docs/design/qt-decoupling/repo-inventory.md)。

## 2. 构建 / 测试（必须单仓可跑）

```bash
go build ./...
go vet ./...
go test ./...
```

- 这三条是 R1/R2 的判据：**单独 checkout 就能绿**。
- 可选集成测试（无库时自动 skip，不是假绿）：
  - PG：`ORCHESTRATION_TEST_DSN='postgres://postgres:postgres@127.0.0.1:5432/tellurion_orchestration_test?sslmode=disable' go test ./internal/pgstore -count=1`
    —— 它跑的是与内存实现**同一套**语义套件（`internal/qtest`），外加 SKIP LOCKED / 唯一索引 / JSONB 三条 PG 专属腿。
- 若某一步需要别的仓的源码目录才过，那是**边界画错了**——停下来修边界，**不要**加 `replace ../xxx` 绕过。
- 本机同时改多个仓时可用 `go.work` 联调，但**它不入库**（`.gitignore` 已挡）。

## 3. 边界（硬约束）

**允许**

- job / batch / walkforward 的编排与重试
- 持有 `run` schema 的 job 元数据与 artifact 指针（~~状态检查点（`strategy_state`）~~ ⚠️ 2026-10-21 H5 裁决：`strategy_state` 归 strategy-runtime 模块、core 写 `quant.strategy_state`，不属本仓）
- 按算力配额分片调度

**禁止**

- 在编排层做业务裁决（它只负责"把活派下去"）
- import core 的实现包
- 持有回测结果**内容**（结果归 core 的 state store；本仓只存指针 + 三 hash，见 `run_artifacts`）

下面三条对所有仓都成立：

- **禁止** `replace` 指向仓外路径（单仓就不可构建了）。
- **禁止**提交 `go.work` / `go.work.sum`（本地产物；已在 `.gitignore`）。
- 新增第三方依赖前，先在第 4 节登记，再改 `go.mod`；没登记就加依赖 = 绕过评审。

## 4. 依赖的契约版本

| 依赖 | 版本 | 说明 |
|---|---|---|
| `github.com/ruoxizhnya/tellurion-contracts` | 未引入（最新 `v0.3.0`，2026-10-23 发版） | `RunSpec` / `ResultSpec` 只认契约 |
| `github.com/ruoxizhnya/tellurion-kit` | 未引入（`v0.1.0` 已发版） | 日志 / HTTP / DB 连接 |
| `github.com/jackc/pgx/v5` | `v5.7.3` | 2026-10-23 施工 ③ 登记。`internal/pgstore`（`run` schema 的 PG 实现：`JobQueue` / `WorkerRegistry` + `Migrate`）与 `cmd/orchestrationservice` 使用；版本与 core / kit / strategy / agent 对齐。**`internal/queue` 保持零第三方依赖**（port + DTO + 内存实现 + DDL 只用标准库），驱动只进 `pgstore` 一个包 |

> 今日 `go.mod` 的**唯一直接依赖**是 `pgx v5.7.3`（其余是它的间接依赖）。上表前两项在真正引入前不动 `go.mod`。

> 跨仓依赖**只认版本号**（伪版本或 tag），不认源码路径。契约变更走 `tellurion-contracts` 的发布，不走"顺手改一下"。

## 5. 活在哪、文档在哪

- **本仓台账**：`docs/TASKS.md`（唯一）。**没有单号别开工。**
- 文档导航：`docs/README.md`；边界与依赖方向：`docs/ARCHITECTURE.md`；决策：`docs/adr/`。
- 跨仓背景（只读，不在本仓改）：`tellurion:docs/design/qt-decoupling/repo-topology-and-naming.md`、`tellurion:docs/TASKS.md`。

> 跨仓引用一律写 `repo:path` 纯文本，**不做可点击相对链接**——独立 checkout 时 `../../` 必断，R2 会永远红。

## 6. 文档真相源（改功能前先看这里）

本仓的「功能边界」由三份文件钉住，**它们是唯一真相源，不是参考读物**：

| 维度 | 文件 |
|---|---|
| 数据形状（我拥有哪些表、谁能写） | `docs/db-schema.md` |
| 接口形状（对外暴露什么、错误码、幂等） | `docs/api-schema.md` |
| 流向与顺序（数据怎么走、哪里 fail-closed） | `docs/dataflow.md` |

**变更触发是硬约束**（完整表见 `docs/README.md` §3）：改表 → 改 `db-schema.md`；改接口 → 改 `api-schema.md`；改链路 → 改 `dataflow.md`；改分层 → 改 `ARCHITECTURE.md` + 一条 ADR；删档/迁移/换工具 → 一条 ODR。PR 里代码动了而对应文档没动，应被打回。
