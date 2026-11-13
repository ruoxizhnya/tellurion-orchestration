# AGENTS — tellurion-orchestration

> 本文件是**本仓**的唯一入口。判据：一个 agent 只 checkout 本仓，就能知道「怎么构建、边界在哪、依赖哪版契约、活在哪、禁止什么」，**不需要读别的仓的源码**。

## 1. 这是什么仓

编排 / worker 服务：job / batch / walkforward 的算力出口，schema `run`。把"批量扫"从 core 进程里搬出来。

**状态**：已启动（2026-10-25 施工 ①②③④⑤⑥⑦⑧⑨⑩⑪）——X5a/X5c 的语义（`JobQueue` + `WorkerRegistry` + `run` schema DDL）已落 `internal/queue`，jobs 4 端点的 HTTP 面已落 `internal/httpapi`，**DDL 已接 PG 并有启动迁移**（`internal/pgstore` + `cmd/orchestrationservice`）；R8（`cancelling` 行的回收出口）与 R9（`Kernels` 的领取过滤）两条已裁事项**代码已落两个实现**；X5b 的**规划与汇总**已落 `internal/plan`（网格展开 / 窗口切分 / 三值计数 / 父状态）并由 `internal/plansuite` 共享套件同跑两种父；**裁-13 的父表已落**——`run.parent_jobs`（`kind` + `plan_canonical`/`plan_hash`（唯一，计划身份）+ 三值汇总）+ `run.backtest_jobs.parent_job_id` 列 / 索引，提交写父表与汇总写回在 `internal/parents`（内存 + PG 两个 `ParentStore` 实现）；**施工 ⑧ 的父作业 2 端点已落**——`POST /api/runs/parents`（batch / walkforward 共用，同计划换键也返回同一父 id）与 `GET /api/runs/parents/{id}`（三值汇总 + 逐子状态，`ParentChildLister` 可选能力）；**施工 ⑩ 的子 run 入队已落**——提交即逐条写 `run.backtest_jobs` 并填 `parent_job_id`（这一列的唯一写入方），子 run 幂等键 `parent:<parent_job_id>:<child_index>`，部分失败靠重放同一请求幂等补齐；**施工 ⑨ 的 X0 读面已落**——`run_artifacts` 的写入端口 `queue.ArtifactStore`（内存 + PG 两个实现，幂等 / 身份漂移响亮）与 `GET /api/runs/{id}/provenance`（独立 15 字段 DTO，三态错误），父 GET 的 `children[]` 逐子带三 hash；**施工 ⑪ 的 worker 打活面已落**——注册 / 心跳 / 领取 / 续租 / 上报 / 取消确认 / 列表 / drain，子 run 上报终态即由 `parents.SyncSummary` 重算并写回父汇总（`cancelled` 按 failed 计），声明了 `kernels` 但队列不支持过滤时 503（不静默不过滤）。**worker 进程在 core**（`tellurion:cmd/worker`，本仓零 import core）。**SVC-ORCH-G2 已落（2026-11-07）**：`POST /api/runs/jobs/{id}/artifacts` 写端点 + R5 最小面（`kit` 的 `auth` + `auth/httpadapter`，先只保护该端点、service 角色；2026-11-09 由 G1 扩到全量 16 条路由）。**SVC-ORCH-G4 已落（2026-11-07）**：`/readyz`（PG 池 + `run.*` 六表迁移，未就绪 503；`/healthz` 保持进程语义）、失败语义五类、回收参数调界、运维可见性、人工释放 SQL 与两条内存级演练记录——全部成文于 `docs/runbook.md`。**已收口（2026-11-11 台账清零）**：全量鉴权（R5 覆盖 16 条路由 service 角色，`SVC-ORCH-G1` ✅ 2026-11-09）、worker 侧三 hash 回传（2026-11-10 `QT-44` 真 run 取证）。

**今天的实现**（本仓，2026-10-25）：`internal/queue` 的两个 port + 可选接口 `KernelAwareJobQueue` / `WorkerReaper` / `ParentChildLister` + `ParentStore` + `ArtifactStore` + 内存实现 + DDL 常量（6 表：含 `parent_jobs` / `run_artifacts`）；`internal/pgstore` 的 PG 实现（`Claim`/`ClaimByKernel` 共用**一条** `claimSQL`，`FOR UPDATE SKIP LOCKED`；提交路径上一把 `pg_advisory_xact_lock` 让 `queue_full` 阈值在并发下精确；回收是 running + cancelling 两条语句；父表 `Parents` / `Artifacts` 各自独立语句，`saveArtifactSQL` 用 `ON CONFLICT (run_id) DO UPDATE … WHERE <13 项身份全等>` 让身份漂移 0 行受影响）；`internal/qtest` 的**语义套件**（每个实现 45 条用例 = 队列 30 + 注册表 9 + 跨 port 6，内存与 PG 各跑一遍——「换实现行为不变」这句判据只有共享用例才可执行）；`internal/plan`（X5b 的网格展开 / 窗口切分 / 三值汇总纯函数 + 父计划 `Canonical`/`Hash`）与 `internal/plansuite`（8 条共享用例同跑 batch / walkforward）；`internal/parents`（提交写父表 + **逐条入队子 run 并写 `parent_job_id`** + 汇总写回，复用 `internal/plan`；子 run 键 `parent:<parent_job_id>:<child_index>`）；`internal/testdb`（PG 腿跨进程排队）；`internal/httpapi` 的 jobs 4 端点 + 父作业 2 端点（`parents.go`：batch/wf 折装、字段点名、三值汇总 + 逐子状态与三 hash）与 `provenance.go`（`GET /api/runs/{id}/provenance`，catch-all 手工切后缀）与 **worker 打活面 `workers.go`**（注册 / 心跳 / 领取 / 续租 / 上报 / 取消确认 / 列表 / drain，装配期断言 `KernelAwareJobQueue` + `ParentChildLister`）；`cmd/orchestrationservice`（DSN fail-closed → 迁移 → 监听 `:8088` → 周期 `ReapExpired` / `ReapOffline`，`-worker-lease` 默认 60s）。**已落**：worker 侧三 hash 回传（2026-11-10 `QT-44` 真 run 取证）、全量鉴权（R5 覆盖 16 条路由 service 角色，`SVC-ORCH-G1` ✅ 2026-11-09）；`SaveArtifact` 的生产消费方是 `POST /api/runs/jobs/{id}/artifacts`（`SVC-ORCH-G2`）。细节见 `docs/ARCHITECTURE.md` §9.8。

**迁出前的老实现**（core）：`core:pkg/backtest/batch` + `core:pkg/backtest/walkforward` 两个**目录仍在 core** —— 它们是引擎侧实现，不是本仓的 job，去留由 O1 对拍后另裁。core 侧的**持久化已退役**：`quant.backtest_jobs` 随 Migration 065 `DROP TABLE`（`SVC-ORCH-G3` 收尾腿，2026-11-09），`walk_forward_reports` 的持久化腿同日退役（`SVC-CORE-RUN-RETIRE`）。任务面与结论面的 owner 已是本仓：`run.backtest_jobs`（唯一 writer）＋ `run.batch_runs` / `run.walk_forward_reports`（迁移期旧表，**停写**，O1 对拍后删除）。

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
- CI（`.github/workflows/ci.yml`，单 job `go`）：`gofmt` → `go build` → `go vet` → `govulncheck@latest ./...`（依赖漏洞门，X-DEP-BUMP-CONSUMERS）→ `go test ./... -count=1 -coverprofile=coverage.out` + **覆盖率门**（ORCH-CI-GATES）→ `go test -race ./... -count=1`（竞态门，X-CI-RACE-STATICCHECK）→ `staticcheck@latest ./...`（`go install` 后跑，未钉版本）。
- **CI 的 DB 腿（ORCH-CI-GATES，2026-11-13）**：Test job 起 `postgres:16` service（`pg_isready -U postgres -d tellurion_orchestration_test`）+ job 级注入 `ORCHESTRATION_TEST_DSN`（CI 内 `127.0.0.1:5432/tellurion_orchestration_test`，**绝不指向部署库**）。本仓无 migrations 目录，DDL 由 `internal/pgstore` 内嵌、真库用例打开 store 时执行，因此 CI 不另设迁移步骤（手写 SQL 会与内嵌 DDL 双写漂移）。效果：库侧 **15 条**用例由 SKIP 转 PASS（带 DSN 168 PASS / 0 SKIP，无 DSN 153 PASS / 15 SKIP）；**本地不设 DSN 仍按设计 skip**，不是假绿。
- **覆盖率门（ORCH-CI-GATES）**：阈值 **51%**（语句加权，`go tool cover -func` 的 `total`），来源 = 2026-11-13 在隔离 PG 16 上真跑全套件实测 **56.4%**，取整 56 减 5（跨仓口径见 `tellurion:docs/TASKS.md` → `X-COVERAGE-FLOOR`）。低于阈值 `exit 1`，解析不出 total 也 `exit 1`。**这 5 点余量是防累计漂移的缓冲**：本仓 3,330 条语句，删单个可编译测试文件最多掉 3.4 点（`internal/httpapi/workers_test.go` ⇒ 53.0%）；漏配 DSN 时实测 **43.0% < 51% 直接红**（这才是这条门要拦的事故）。

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
| `github.com/ruoxizhnya/tellurion-contracts` | `v0.10.0`（2026-11-12 随 `X-DEP-BUMP-CONSUMERS` 从 `v0.9.0` 升；2026-11-08 发版，2026-11-09 `SVC-ORCH-G1` 引入 `v0.9.0`） | `run` 包的 `ClaimView` / `ProvenanceView` / `ParentSubmitRequest` 族；claim / provenance / 父作业请求直接引用契约类型（`internal/httpapi`）。`RunSpec` / `ResultSpec` 仍走本地 wire 面；本仓零 import core |
| `github.com/ruoxizhnya/tellurion-kit` | `v0.5.0`（2026-11-12 随 `X-DEP-BUMP-CONSUMERS` 从 `v0.2.0` 升；2026-11-07 引入 `v0.1.4`，2026-11-10 随 kit `KT-13` 升到 `v0.2.0`） | `auth` + `auth/httpadapter`：全量 R5 的 token 解析 / 角色判定 / 三态启动裁决（`IsLoopbackHost` / `InsecureExposureLoopbackPublished`）；2026-11-09 起覆盖全部 `/api/*` 路由（16 条，service 角色），策略见 `internal/httpapi/policy.go`。`v0.5.0` 新增 stdlib `net/http` 版 `RequestIDHandler`（`KIT-REQUESTID-STDLIB`），本仓未接线 |
| `github.com/jackc/pgx/v5` | `v5.11.0`（2026-11-12 随 `X-DEP-BUMP-CONSUMERS` 由 MVS 从 `v5.7.3` 抬升；`tellurion-kit v0.5.0` 自身 pin `v5.11.0`） | 2026-10-23 施工 ③ 引入。`internal/pgstore`（`run` schema 的 PG 实现：`JobQueue` / `WorkerRegistry` + `Migrate`）与 `cmd/orchestrationservice` 使用；升档后与 kit / analytics 同档（core / data / strategy / agent / broker 仍 `v5.7.3`、risk `v5.7.5` 是 2026-10-26 的旧实测，跨仓抄 `pgxpool` / `pgtype` 用法前先核目标仓 `go.mod`）。**`internal/queue` 保持零第三方依赖**（port + DTO + 内存实现 + DDL 只用标准库），驱动只进 `pgstore` 一个包 |
| Go 工具链（`go.mod` 的 `go` 指令） | `1.26.11`（2026-11-12 随 `X-DEP-BUMP-CONSUMERS` 从 `1.25.0` 升；下限由 `tellurion-kit v0.5.0` 的 `go 1.26.11` 决定） | 本机 Go 1.25.14 + `GOTOOLCHAIN=auto` 会自动切到缓存的 go1.26.11 构建；CI 的 `setup-go` 必须显式钉 `1.26.11` |

> 今日 `go.mod` 的**直接依赖**是 `pgx v5.11.0`（MVS 抬升）、`tellurion-kit v0.5.0` 与 `tellurion-contracts v0.10.0`（其余是它们的间接依赖；`golang.org/x/net v0.60.0`、`golang.org/x/text v0.42.0`、`golang.org/x/crypto v0.57.0`、`golang.org/x/sync v0.23.0`、`golang.org/x/sys v0.48.0`）。
>
> **鉴权三态（2026-11-12 收紧，同日修正第 3 态，随后统一第 2 态）**：有 `JWT_SECRET` → 全闸；无 secret + 无显式声明 →
> **回环绑定**下只读降级（读端点公开、写端点 `503 auth_unavailable`），**非回环绑定拒绝启动**；无 secret + **显式声明**
> （`AUTH_INSECURE_EXPOSURE=loopback-published`，或显式 `AUTH_ALLOW_INSECURE=true` / `-allow-insecure=true`）→
> **任何绑定都允许放开**，启动 WARN 同时写出「写面无鉴权」与绑定地址，非回环绑定额外点名「已对外暴露」。
> `allowInsecure` 由 `AUTH_ALLOW_INSECURE` / `-allow-insecure` 配置，不再硬编码。
> 细节与锚点见 `docs/api-schema.md` §2/§8、`docs/runbook.md` 三态段。

> 跨仓依赖**只认版本号**（伪版本或 tag），不认源码路径。契约变更走 `tellurion-contracts` 的发布，不走"顺手改一下"。

## 5. 活在哪、文档在哪

- **本仓台账**：`docs/TASKS.md`（唯一）。**没有单号别开工。**
- 文档导航：`docs/README.md`；边界与依赖方向：`docs/ARCHITECTURE.md`；值班 / 故障处置：`docs/runbook.md`；决策：`docs/adr/`。
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
