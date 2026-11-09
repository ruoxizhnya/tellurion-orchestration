---
status: active
last-verified: 2026-11-09
verified-by: SVC-ORCH-G1 ✅（2026-11-09：`contracts v0.9.0` 远端可解析、claim/provenance/父作业请求改引 `contracts/run`、逐字节 golden 三条、core 客户端与 analytics 调用方同批收编、全量 R5 16 条路由 service 角色 + 路由普查守卫）；SVC-ORCH-G4 ✅；SVC-ORCH-G2 ✅
---

# TASKS — tellurion-orchestration 未完成项

> 本仓**唯一**任务台账。跨仓任务只在 §二 留指针，**不镜像正文**（双写必然漂移）。
> 状态：`⬜ 待做` / `🔶 进行中` / `⛔ 阻塞（有未解除前置）` / `✅ 完成（附日期）`
> 每张单六字段（① 目标 ② 上下文 ③ 要求 ④ 约束与非目标 ⑤ 验收 ⑥ 边界），判据见 `tellurion:AGENTS.md` §8.5。
> 完成即标 ✅（附日期）并移入 `docs/archive/TASKS-YYYY-MM-DD-archived.md`；本文件只留未完成项。

> **环境口径（2026-10-26 复测，统一出处）**：本文件多处写「本机 `127.0.0.1:5432` 未监听 ⇒ PG 腿 skip」「本机 `-race` 跑不了」——**两条今天仍成立**（`netstat -ano` 里 5432 零条监听；`CGO_ENABLED=0`）。但**「本机无 Go」不成立**：Go 1.25.14 已装，只是不在默认 Git Bash 的 PATH，`export PATH="/c/Users/ruoxi/AppData/Local/Programs/go1.25.14/bin:$PATH"` 后 build/vet/test 可本机复跑。以前者为准的 skip 登记不许顺手写成「无工具链」，全组统一表见 `tellurion:docs/design/qt-decoupling/cross-repo-decision-record-2026-10-26.md` §三。

> **第三轮裁决指针（2026-10-25）**：⑱ X0 生产者 = **本仓新增 artifact 写端点**（`POST /api/runs/jobs/{id}/artifacts`，service 角色），core worker 调它、**不直写库**；幂等沿用 `queue.ArtifactStore.SaveArtifact`；**前置 = 本仓先做 R5 鉴权**（kit + policy）。DR⑬ 的 `producer_pending` 响应提示位并入本条。正文见 `tellurion:docs/design/qt-decoupling/cross-repo-decision-record-2026-10-25.md` §五。

---

## 一、本仓任务

> **约定变更（2026-10-21）**：原「占位仓不写正式六字段任务单」作废——X 线任务单已展开为本仓正式任务（见下）。M5/M8 开工前置不变；实现仍未开工，开工前仍须过前置两条。

### SVC-ORCH-G0 · 服务就绪门 G0：自有部署与本地启动（ADR-036）[P0] ✅（2026-10-26）

- **① 目标**：本仓作为运行编排控制面，能独立构建、启动、被健康检查；DSN 为空时仍 fail-closed。
- **② 上下文**：ADR-036 定本仓为服务；今天有 `cmd/orchestrationservice` + `run.*` 6 表 + `/healthz`，但无 `Dockerfile` / `deploy/`，且 `go.mod` 只有 pgx（无 contracts/kit）。
- **③ 要求**：新增 `Dockerfile`（多阶段、非 root、`curl`、`HEALTHCHECK /healthz`）；新增 `deploy/compose.yaml`（`ORCHESTRATION_DSN` 必填；端口 `:8088`）；补 `config/` 或 env 说明；README 写单仓启动命令。
- **④ 约束与非目标**：不改队列语义；不接 core 任务切读（G3）；不引入 K8s；不在 G0 阶段做鉴权（G1）。
- **⑤ 验收**：`docker compose -f deploy/compose.yaml config` 通过；无 DSN 启动失败；有 DSN 启动后 `GET /healthz` 200；无 `replace` / `go.work`。
- **⑥ 边界**：只改本仓 deployment；core worker 客户端由 `SVC-ORCH-G3` 跟踪。

> **施工记录（2026-10-26）**：
> - **落点**：`Dockerfile`（多阶段、非 root、`curl`、`HEALTHCHECK curl -fsS http://127.0.0.1:8088/healthz`）、`deploy/compose.yaml`（服务名 `orchestration`、`ORCHESTRATION_DSN`、`:8088`）；`cmd/orchestrationservice/main.go` 默认 `-addr` 保持 `127.0.0.1:8088`、`"/healthz"` 路由改为常量引用（可被文档 grep 锚定）；README 补直跑与 compose 启动命令。
> - **验收（真实输出）**：`docker compose -f deploy/compose.yaml config -q` → exit 0；`git ls-files Dockerfile deploy/` 非空；`rg -n '"/healthz"' cmd/orchestrationservice/main.go` → 1 行（`:135`）；`go build ./...` / `go vet ./...` / `go test ./... -count=1` 全绿（5 个测试包 + 4 个无测试包）；`gofmt -l .` 空。
> - **破坏验证**：临时给 `orchestration` 加 `depends_on: missing-svc` → `config -q` 退出 1（`service "orchestration" depends on undefined service "missing-svc"`）→ 逐字节还原后复绿（exit 0，SHA256 一致）。
> - **如实登记**：未改队列/worker 语义；`go.mod` 仍只有 pgx，未引 contracts / kit；`run_artifacts` 生产者与子 run 入队等仍属 G3。

### SVC-ORCH-G1 · 契约化与鉴权（ADR-036 G1）[P0] ✅（2026-11-09）

- **① 目标**：本仓的 wire 形状进 contracts，服务间调用用 kit service token，不再靠 core/本仓各写一份 DTO。
- **② 上下文**：今天 `go.mod` 只有 pgx；core `internal/orchestrationworker` 与本仓各写一套 claim/job 结构。
- **③ 要求**：contracts 新增 orchestration 任务/租约/结果/artifact 形状；本仓与 core 改为 `require` 版本；引 kit `v0.1.4` 做 service token 鉴权（裁-15 的触发条件已满足：服务对外发布）；公开 `/healthz` 保持无鉴权。
- **④ 约束与非目标**：不改变任务状态机语义；不把结果解释搬进本仓；不做用户 JWT。
- **⑤ 验收**：core/本仓无本地重复 DTO；service token 可调用；错误码闭合；`go.mod` 同时含 contracts + kit。
- **⑥ 边界**：只改本仓与 contracts 的接口层；core 客户端改造归 **`SVC-CORE-RUN-RETIRE`** 跟踪——条目**已在 core 立单**（2026-10-26 同日补，`tellurion:docs/TASKS.md:121`「六之二、退役执行单」，串行第 5；判据逐字抄 B 组材料 §4.2）。基线数会随施工漂移，开工时当场重跑。

> **⛔ 开工前置复核（2026-10-26，未开工）**：`go.mod` 只有 `github.com/jackc/pgx/v5 v5.7.3`，**无 contracts / kit**；contracts 侧**无编排形状包、无对应立单**。不得在本仓自造 claim / `run_artifacts` 结构（会变成第三份 DTO）。contracts 形状 + kit `v0.1.4` 升 pin 属跨仓腿，报协调者。
>
> **前置更新（2026-11-07）**：contracts 侧 **CT-04 已结案**——`tellurion-contracts` 新建 `run/` 包（`ClaimView` / `ProvenanceView` / `ParentSubmitRequest` 族 + `ChildIdempotencyKey`），本地 tag `v0.8.0`。但 tag **未 push**（本机纪律只 commit 不 push），`go get …@v0.8.0` 在远端解析不到 ⇒ 本仓**今天仍不能**把 `go.mod` 切到 contracts（会破坏单仓可构建）。本单剩余前置 = **contracts `v0.8.0` 推远端 + kit `v0.1.4` 已在远端**；码面改造（claim / 父作业提交改用契约类型）等前置解除后一笔做，不在契约侧代做。
>
> **施工记录（2026-11-09）**：前置已解除——`go get github.com/ruoxizhnya/tellurion-contracts@v0.9.0` 在本机 **exit 0**（远端可解析），`go.mod` 现同时含 `contracts v0.9.0`（direct）与 `kit v0.1.4`。
> - **本仓落点**：`internal/httpapi/workers.go` 删本地 `claimView` → `run.ClaimView`；`provenance.go` 删本地 `provenanceView` → `run.ProvenanceView`；`parents.go` 删本地 `ParentSubmitRequest` / `ParentBatchRequest` / `ParentWalkForwardRequest` / `ParentWindows` 四个 DTO → `contracts/run`，两个折叠方法改自由函数。`jobView` / `parentView` / `parentChildView` / `parentCountsView` / `WorkerRecord` **按契约包「不迁」清单保留在本仓**，不是重复 DTO。
> - **core 腿（同批，属 `SVC-CORE-RUN-RETIRE` 客户端腿）**：`internal/orchestrationworker/client.go` 删 `ClaimedJob` → `run.ClaimView`，`loop.go` 的 `execute` 签名与 `worker_test.go` 同步。
> - **逐字节证据**：新增 `internal/httpapi/contract_shapes_test.go` 三条冻结用例——claim（普通 run 无 `parent_job_id` 键 / 子 run 键在末尾）、provenance（15 字段，五个可空位显式 `null`）、父作业请求往返；任何字段重排 / 加 `omitempty` / 可空变省略键都会红。
> - **门禁实测（2026-11-09）**：orchestration `go build ./...` = 0、`go vet ./...` = 0、`go test ./... -count=1` = 0（8 个包 ok）、`gofmt -l .` 空；core `go build ./...` = 0、`go vet ./internal/orchestrationworker/...` = 0、`go test ./internal/orchestrationworker/... -count=1` = 0。
> - **未完成（如实，不写成已完成）**：无——全量 R5 已于同日收口（见下条）。
>
> **施工记录（2026-11-09，全量 R5 收口，本单结案）**：
> - **本仓**：`policy.go` 的 `AuthStatus` 改 `all-endpoints-service`，策略表覆盖 `/api/*` 全部 16 条路由（worker 面 / 作业面 / 父作业面 / provenance / artifact 写面），全部 `service` 角色；`api.go` / `workers.go` 每条路由经 `s.guarded(...)` 过 `requireAuth`。公开面只有进程入口的 `/healthz` / `/readyz`（不进本包 mux）。
> - **调用方（跨仓，同批）**：core `internal/orchestrationworker.Client` 新增 `Token`，`do()` 统一注入 `Authorization`；`cmd/worker` 新增 `-orchestration-token` / `ORCHESTRATION_SERVICE_TOKEN`。analytics `upstream.NewSensitivityQueueWithToken` + `-orchestration-token` / `ANALYTICS_ORCHESTRATION_TOKEN`（缺省读 `ORCHESTRATION_SERVICE_TOKEN`）；`cmd/analyticsservice` 已切。
> - **部署**：core `docker-compose.yml` 的 worker 面注入 `ORCHESTRATION_SERVICE_TOKEN`；`docker-compose.services.yml` 的 analytics 注入 `ANALYTICS_ORCHESTRATION_TOKEN`（均空占位，签法见 core `docs/SPEC.md#服务间凭证`）。
> - **守卫测试**：`TestPolicyCoversAllRoutesAsService` 是**路由普查**——16 条路由逐条断言「策略表有 service 角色 + 无凭证 = 401」，新增路由不改策略表即红。
> - **门禁实测（2026-11-09）**：本仓 build / vet / test 全 `EXIT=0`、`gofmt -l .` 空；core `go build ./...` = 0、`go vet ./internal/orchestrationworker/... ./cmd/worker/...` = 0、对应 `go test -count=1` = 0；analytics build / vet / test 全 0。
> - **破坏验证（真做真还原）**：把 `GET /api/runs/jobs` 的 `s.guarded(...)` 摘掉 → `TestPolicyCoversAllRoutesAsService` 红（无凭证从 401 变 200）；逐行还原后复绿，`go test ./internal/httpapi -count=1` = 0。

### SVC-ORCH-G3 · core 任务面切换与旧 job 实现退役（ADR-036）[P0] 🔶（2026-11-09 复核：三条前置全部满足，可开工）

- **① 目标**：core 所有异步任务创建走本仓；core `pkg/backtest/job` / `batch` 的旧写路径退役；`X5b` 关闭。
- **② 上下文**：core 仍可直跑并写裸表 `backtest_jobs`；`cmd/worker` 从本仓领活后又写回 core `backtest_jobs`；两套任务真相并存。
- **③ 要求**：定死「任务面归 run、结果面归 core」；core analysis 的异步入口改调本仓；worker 只上报结果；迁移存量行；删 core `pkg/backtest/job` / `batch` 的本地队列写路径；UI/openapi 同步。
- **④ 约束与非目标**：不重算指标；不改 run 三值状态；不把订单/持仓搬进 `run`。
- **⑤ 验收**：core `pkg/backtest` 对本仓的引用符合契约；core 裸表 `backtest_jobs` 零新增；同批 run 新旧路径结果一致；`X5b` 关闭。
- **⑥ 边界**：本仓只保证任务域；core 代码删除归 **`SVC-CORE-RUN-RETIRE`** 跟踪（条目已在 core `docs/TASKS.md:121`「六之二」立单，串行第 5，判据 = B 组材料 §4.2）。

> **⛔ 开工前置复核（2026-10-26，未开工）**：等 B 组 `SVC-CORE-RUN-RETIRE` 立单；本仓台账仍未落的 `run_artifacts` 生产者（写端点 + R5 鉴权）、worker 进程/端点、子 run 入队均依赖前置 2（contracts 编排形状）。本轮未自造 wire 形状，未改 worker 语义。
>
> ⚠️ **本行三处已于同日实测订正（2026-10-26 账实核对）**：① `SVC-CORE-RUN-RETIRE` **已立单**——`tellurion:docs/TASKS.md:121`「六之二」，串行第 5，所以它不再是本单的阻塞前置；② **「子 run 入队」已落**——`internal/parents/service.go:143` 的 `enqueueChildren` 是 `run.backtest_jobs.parent_job_id` 的唯一写入方（`:148` 填 `ParentJobID`），本文件 G3 ① 自己也记了 ✅（2026-10-25），把它列进「未落」是自相矛盾；③ 真正未落的三条**复核后仍成立**：`internal/httpapi/` 下**没有** `auth.go` / `policy.go`（R5 鉴权未接）、**没有** `POST …/artifacts` 路由（只有 `GET …/provenance` 读面），worker 进程在 core（`tellurion:cmd/worker`）而非本仓。⇒ 本单的剩余前置是 **contracts 编排形状（`CT-04`）+ R5 + artifact 写端点**，不是「等退役单」。
>
> **前置解除（2026-11-09，实测）**：三条剩余前置已全部满足——① `CT-04` 结案且 `contracts v0.9.0` 远端可解析（见 `SVC-ORCH-G1`）；② 全量 R5 已落（同上，16 条路由 service 角色）；③ `POST /api/runs/jobs/{id}/artifacts` 已在 `SVC-ORCH-G2` 落地。⇒ 本单从 ⛔ 转 🔶，可以开工；施工顺序仍是「core analysis 异步入口改调本仓 → core 裸表零新增 → 存量迁移 → 删 core 本地队列写路径 → UI/openapi 同步」。
>
> **施工记录（2026-11-09，第一腿：core 任务面客户端）**：`tellurion:internal/orchestrationworker/client.go` 新增 `Submit` / `List` / `Cancel`（路径与状态码按本仓 `docs/api-schema.md` §9.4），真 orchestration 进程 + 真 PG 的 wire 用例 `TestWireControlPlaneWithRealOrchestrationProcess` **PASS**（提交 202 → 同键重放 200 → 列表 → 取消 → 复读；1.48s）。本仓 wire 契约未改，无需动码。**仍未做**：core `cmd/analysis` 异步入口改调、结果面 `run.backtest_results`、存量迁移、删 core 本地队列 —— 本单保持 🔶。
>
> **施工记录（2026-11-09，第二腿：core 结果面存储基础）**：按 `tellurion:docs/design/qt-decoupling/data-service-contract-and-ownership.md:41` 的分工——任务面归本仓 `run.backtest_jobs`，**结果面 `run.backtest_results` 归 core、单一写者**——在 core 落 `tellurion:pkg/storage/backtest_results.go`：`BacktestResultsSchemaDDL`（3 条：schema + 表 + `result_status` 索引，CHECK 只允许 `completed` / `invalid` / `failed`）、`SaveBacktestResult`（按 `run_id` upsert，`created_at` 首写保留）、`GetBacktestResult`，经 core `pkg/storage/postgres.go` 的 Migration 063 接进 `migrationStatements()`。真库验收：本机 PG 16.15 上 `TestBacktestResult*` 六组全 PASS。本仓无码改动（结果内容不进本仓，符合 §3 禁止项）。**仍未做**：worker 结果 sink 改写、core `cmd/analysis` 异步入口改调、切读、存量迁移、删 core 本地队列 —— 本单保持 🔶。

> **施工记录（2026-11-09，第三腿：core worker 结果面 sink）**：core `internal/orchestrationworker` 新增 `ResultStoreSink`（写 core 的 `run.backtest_results`，只接受 completed / invalid）与 `MultiSink`；`tellurion:cmd/worker` 装配先过渡期双写（结果面新腿 + legacy `backtest_jobs` 腿），随后在第五腿收为只写结果面。单测 3 条 + 真库 1 条 PASS。本仓仍无码改动（结果内容不进本仓）。**仍未做**：切读、存量迁移、删 core 本地队列 —— 本单保持 🔶。
>
> **施工记录（2026-11-09，第五腿：core 存量回填 + 停写 legacy）**：core Migration 064 把 `backtest_jobs` 终态行回填 `run.backtest_results`（pending/running 不搬、ON CONFLICT DO NOTHING、重放幂等）；`tellurion:cmd/worker` 只保留 `ResultStoreSink`，`JobStoreSink` 已删。真库用例 PASS。**仍未做**：core 同步回测仍写 `backtest_jobs`、`pkg/backtest/job`/`batch` 本地队列未删、旧表未 drop —— 本单保持 🔶。
>
> **施工记录（2026-11-09，第四腿：core 异步入口切任务面 + 读面三源）**：core `cmd/analysis/orchestration_jobs.go` 新增 `orchestrationJobStore`——读面按「存量 `backtest_jobs` → 结果面 `run.backtest_results` → 本仓 `GET /api/runs/jobs/{id}`」解析，列表按 id 去重、`created_at` 降序，本仓不可用时降级只返本地行（不把整页打成 500）。`POST /api/backtest` 的异步分支在配置 `ORCHESTRATION_URL` 时把 `CreateJobRequest` 翻成 `run_spec` 调本仓 `POST /api/runs/jobs`，返回 202 `{job_id,status}`；run_spec 只翻本地执行路径消费的字段（strategy / stock_pool / 区间 / initial_capital / kernel / engine_params），不翻手续费/滑点/策略参数，保证新旧路径结果一致。本仓 wire 契约未改。**仍未做**：删 core 本地队列写路径、旧表 drop、UI/openapi 列表页对纯编排作业补策略元数据 —— 本单保持 🔶。
>
> **施工记录（2026-11-09，第六腿：core 同步回测切结果面）**：core `pkg/backtest/job` 增 `WithResultFace`，`SaveSyncResult` 注入后只写 `run.backtest_results`、不再插 `backtest_jobs`；`pkg/storage` 补 `ListBacktestResults`，core 读面因此能列出「只有结果行、没有任务行」的作业（同步回测与迁移存量）。本仓无码改动（任务面仍只由本仓写）。**仍未做**：删 core `pkg/backtest/job`/`batch` 本地队列、drop 旧表 —— 本单保持 🔶。
>
> **施工记录（2026-11-09，第七腿：core 本地队列退役 + 本仓 `run_spec` 读面补字段）**：core 侧 `pkg/storage/backtest_jobs.go` 整删、`pkg/backtest/job` 收为只读 + 结果面写口、Migration 065 `DROP TABLE IF EXISTS backtest_jobs`，异步 POST 未配 `ORCHESTRATION_URL` 即 503——任务面从此只有一个真相（本仓 `run.backtest_jobs`）。本仓侧为「列表页对纯编排作业补策略元数据」补了 `jobView.run_spec`（**MINOR**：加字段）：`GET /api/runs/jobs/{id}` 恒返回 `run_spec`；`GET /api/runs/jobs` 默认省略、`?include_spec=1` 时逐行返回，避免 1 MiB × 100 的响应放大。core 的 `List` 客户端据此多一个 `includeSpec` 实参（默认 false）。本仓单测/构建/vet 全绿，`docs/api-schema.md` §9.4 已同步。**仍未做**：本单 ⑤ 的「core `pkg/backtest` 对本仓引用符合契约」要等 core 后续单删掉 storage 直连；本单保持 🔶。

### QT-44 · X5 编排 / worker 服务（job/batch/walkforward 上移）[P2] 🔶

> **迁入说明（2026-10-21）**：本单自 core 仓 `tellurion:docs/TASKS.md`「五、X 线」迁入（任务台账批量重分布：X 线单展开到各服务仓为正式六字段单，「占位仓不写正式任务单」旧约定同时作废）。六字段正文逐字保留、不改写；core 侧同号条目已改为指针。
>
> **施工记录（2026-10-23，施工 ①）**：X5a（`JobQueue` port + 状态机 + 幂等键）与 X5c（`WorkerRegistry` 注册 / 心跳 / `drain`）已在 `internal/queue` 落地——port 与方法级错误语义见 `ARCHITECTURE.md` §9.2，内存实现 + `run` schema 5 表 DDL 见 §9.8。**仍是 🔶**：X5b（batch / walkforward）、X0（`run_artifacts` 三 hash + `/provenance`）未做；DDL 未接 PG、无 HTTP 层；`cancelling` 行的租约回收出口待裁（§9.7 R8）；按内核能力（`Kernels`）领取过滤在冻结的 `Claim` 上无法实现，待裁（§9.7 R9）。
> 自审补正（2026-10-23，施工 ①）：① 重复注册不再重置 `started_at`（原实现把 worker 起始时间洗成重注册时刻，查不出存活时长）；② 两处实现加**编译期接口断言**（`Memory` 满足 `JobQueue` 全部 10 方法、`MemoryRegistry` 满足 `WorkerRegistry` 全部 4 方法）——方法集是冻结面，少一个方法就是实现方全红；③ §9.8 索引数订正为 7（原写 6）；④ 补 `List` 钳制的**实测**断言（原测试只有 5 行，区分不出「上限 100」与「默认 50」）。
>
> **施工记录（2026-10-23，施工 ②）**：C 组**任务 4 端点**的 HTTP 面已在 `internal/httpapi` 落地（本单要求 ③ 的「取消端点」腿）——`POST`/`GET list`/`GET {id}`/`DELETE {id}`，行为按 `api-schema.md` §1/§2 冻结形状实现：202 提交 / 键序无关的 200 幂等重放（canonical 化在 queue 层实测）/ 409 `idempotency_conflict` / 429 `queue_full` / 400 `missing_idempotency_key`（在领域校验之前判，服务端不代生成键）/ 取消三态（`pending`→200 `cancelled`、`running`→202 `cancelling`、终态幂等 200 原状态）/ K9 `invalid` 以 `completed+invalid`、**error 为空**呈现（无效不是故障）。传输层口径四条（`total` 语义、鉴权缺位、`run_spec` 透传）见 `ARCHITECTURE.md` §9.8 施工 ② 小节。
> 验收对照（本单 ⑤ 的三条判据，施工 ② 后的状态）：「`pkg/backtest` 零命中」与「同一 batch 新旧路径一致」都属 X5b 迁移，**未达成**；「provenance 端点返回三 hash」属 X0，**未达成**。本单整体**仍是 🔶**：X5b、X0、PG 装配、鉴权（R5）、R8/R9 待裁均未动。
>
> **施工记录（2026-10-23，施工 ③：工单 G7 第 1 步「DDL 接 PG + 启动迁移」）**：`run` schema 的 5 表 DDL 已**在真库执行**，两个 port 各有了第二个实现。
> 落点：`internal/pgstore`（`Open` DSN 空即 fail-closed / `Migrate` 按序幂等 / `Queue` 的 `Claim` 用 `FOR UPDATE SKIP LOCKED` / `Registry` 的 UPSERT 保 status·started_at·drain 时刻）、`internal/qtest`（**共享语义套件** 29 条队列 + 6 条注册表 + 2 条跨 port，内存与 PG 各跑一遍——§9.8 那句「换 PG 实现时行为不变」只有共享用例才是可执行的）、`internal/testdb`、`cmd/orchestrationservice`（DSN → 迁移 → 监听 `127.0.0.1:8088` → 周期 `ReapExpired`）。依赖登记：`pgx v5.7.3` 先进 AGENTS.md §4 再进 `go.mod`，`internal/queue` 保持零第三方依赖。
> **DDL 首次落库时改的两处原文**（当时任何库里都还没有这套表，追加 ALTER 是给已落库的语句用的）：① `backtest_jobs.request_canonical TEXT NOT NULL`——`request_hash` 的**字节锚点**；② 领取索引去掉键里冗余的 `status`、补第三键 `id`（与内存实现的 tie-break 同形），且 partial 谓词**不放** `lease_until`（冻结判据是「已释放**或**已过期」）。两处均已同步 `db-schema.md` §5 / `ARCHITECTURE.md` §9.5。
> **四条实测换来的红（先红后修，不是设计时想到的）**：① `UPDATE…FROM(SELECT…FOR UPDATE SKIP LOCKED) RETURNING id,…` 里裸 `id` 是歧义列（42702）→ 列清单改由 `jobCols` 派生；② 同一参数既进 `CAST($4 AS jsonb)` 又进 TEXT 列时 PG 按 jsonb 定型，TEXT 列存进的是 **jsonb 的文本表示**（`{"a": 1}` 多一空格）→ 锚点当场失效，两处都显式 `::text`（同类问题在 worker UPSERT 的未类型化 NULL 上又红一次）；③ JSONB 把 `1e-05` 规范化成 `0.00001` ⇒ 拿 `request_json` 复算 hash 必然与 `request_hash` 不符，同一个幂等键会被判成两次不同请求——这是 ① 那条新列存在的全部理由，钉子 `hashanchor_test.go`；④ `go test ./...` **并行跑不同包**，两个包的 PG 腿共用一个测试库时 A 的清库删掉 B 正在断言的行（症状：重放莫名 202；单跑任一包永远绿）→ `internal/testdb` 用会话级 advisory lock 跨进程排队。
> **破坏验证（施工 ③，三条腿各自红→逐字节还原→整包绿）**。每条都是「改坏应红」，还原后 sha256 与改前**逐字节相同**（`internal/pgstore/queue.go` = `c209005b…`、`internal/pgstore/store.go` = `64db894e…`，改前改后各比对一次）：
> 1. **删掉 `Enqueue` 的 `pg_advisory_xact_lock`** → `TestPGSatisfiesJobQueueSuite/PG/ConcurrentQueueFullIsExact` 红：`round 0：深度 1 接受 12 行、拒 12 行（want 1 / 23）`；同批 `ConcurrentSameKeyYieldsOneRow` 也红（同键并发插入撞唯一索引后被错报成 `duplicate run id`）。⚠️ 这条腿**第一次跑时是绿的**——那时用例用「逐个 `go`」起跑，事务彼此错开、竞态窗口打不开，于是「护栏在」这个结论是假的；改成 barrier 起跑 + 3 轮后才有牙。教训如实登记：**破坏验证要先证明测试能被破坏**。
> 2. **把读路径的 `request_canonical` 换成 JSONB 的 `request_json`**（拆掉 identity 锚点）→ 四条红：`TestCanonicalBytesSurviveTheJSONBRoundTrip`（`request bytes changed on round trip`）、`TestJobColumnsMatchTheTableShape`、套件的 `EnqueueReturnsCleanPendingRow`（`{"strategy": "momentum", ...}` 多出空格）与 `IdempotencyConflictSameKeyDifferentBody`。这条腿同时是 `request_canonical` 那列的**存在性证明**。
> 3. **去掉 `Claim` 的租约谓词**（只留 `status='pending'`）→ `TestClaimPredicateSkipsARowWithAnUnreleasedLease` 红：把 `row-live`（租约在未来）派给了 worker，而按冻结判据它该拿 `row-expired`。
>
> 诚实登记覆盖面限制：`FOR UPDATE SKIP LOCKED` 的「**不阻塞**」这一半没有行为级用例——两个 worker 抢同一行时，带不带 `SKIP LOCKED` 都只影响排队与否，领取结果相同（并发不重发由 `FOR UPDATE` + 谓词保证，已被第 3 条腿与套件的 `ConcurrentClaimsHandOutDistinctRows` 钉住）。要测「不阻塞」只能造长时间持锁的开放事务并卡时间，属易碎断言，故不做，改为在实现注释里写明它的用途。
> 本机 `-race` 跑不了（`CGO_ENABLED=0`、无 C 编译器，`go test -race` 直接 exit 2），并发结论来自 barrier 起跑的真库跑；CI 在 ubuntu 上跑同一条命令。


> **施工记录（2026-10-24，施工 ④：工单 G7 第 4 步「停下要裁决的」——R8 / R9 落地）**：两条 2026-10-23 已裁事项在**两个实现同批**落代码，不再是「已裁待落」。
> **R8（`cancelling` 行的租约回收出口，裁 (a)）**：`ReapExpired` 现在做两类迁移——`running` 那类口径一字未改，新增「`cancelling` 且租约过期 → `cancelled`，**不**计 attempts」。内存侧是 `Memory.ReapExpired` 的一个分支（字段写法与 `AckCancel` 逐条同形：`status` / `result_status=""` / `cancel_requested` / `completed_at` / 释放租约）；PG 侧是 `reapCancellingSQL` 这条**独立语句**——没往 `reapSQL` 里塞 CASE，因为两条的判据本来就不同形（一条计 attempts、一条不计），合并只会让已测过的 running 语句背上新风险。两条语句套在**同一个事务**里（`inTx`）：内存实现是在一把锁下同时处理两类行，autocommit 分两次就会造出「running 已收、cancelling 失败」的半迁移态而调用方只看到一个 error；回滚后返回 `0 + error` 与「一行没动」同真。代价如实登记：**回滚腿没有用例**（要稳定让第二条语句失败只能改代码或造约束冲突，属易碎断言），且 `SKIP LOCKED` 在事务里的持锁窗口比 autocommit 略长——reaper 只跳不等的性质没变（两条语句的谓词互斥，且 reaper 从不 `FOR UPDATE` 等待别人的行）。
> **R9（`Kernels` 的领取过滤，裁「加可选新接口 = MINOR」）**：`KernelAwareJobQueue.ClaimByKernel`（`internal/queue/ports.go`），冻结的 10 方法集一字未动（§9.6「新接口 = MINOR」第一次实际使用）。能力取 worker **在注册表里声明的** `Kernels`，不是调用方现传——否则声明仍然只落库。`Claim` 与 `ClaimByKernel` 在 PG 上**共用同一条 `claimSQL`**（多收一个 `$4::text[]`，传 NULL 即不过滤），在内存上共用 `claim(..., byKernel)`：两个入口的顺序、租约判据、drain 校验不可能各写各的。
> **抽取规则是这轮唯一的"新发明"，且是被实测逼出来的**：`request_json` 里的 `kernel` **只有是 JSON 字符串才算内核**（缺键 / null / 数字 / 顶层非对象 / 空串 / 纯空白 = 默认 `go`）。第一版 SQL 只写了 `btrim` + `COALESCE`，实测两侧立刻分叉：`{"kernel":123}` 在 PG 里被 `->>` 读成文本 `123`（谁都领不走），在内存里解不进 `string` 按默认处理（`go` worker 能领走）。处置是 SQL 加一层 `jsonb_typeof(...) = 'string'` 的 CASE，Go 侧 `KernelOf` 补 `TrimSpace` 返回值（原来只用来判空、却把带空白的原值返回，`" go "` 在两侧含义不同）。规则本体写在 `queue.KernelOf` 的注释，SQL 是它的形。
> **套件计数订正（我自己的旧账写错了）**：施工 ③ 记「29 + 6 + 2 = 37 条」——按 `go test -v` 实测 HEAD 是 **29 + 6 + 1 = 36**（跨 port 只有 1 条，不是 2）。施工 ④ 后是 **每个实现 41 条**（队列 30：`ReapIgnoresTerminalAndCancelling` 拆成 `ReapIgnoresTerminalRows` + `ReapLandsOrphanedCancellingRow`；跨 port 5：新增 `ClaimByKernelConsumesTheDeclaredCapabilities` / `ClaimByKernelReadsOnlyStringKernels` / `ClaimWithoutKernelFilterStaysUnfiltered` / `ClaimByKernelRejectsDrainingAndUnregisteredIsAllowed`）。教训：**数用例要数测试运行器的输出，不要手数文档里的数字**。
> **破坏验证（施工 ④，五条腿各自红 → 逐字节还原 → 整包绿）**。还原用 sha256 比对（`internal/pgstore/queue.go` = `b6d1f555…`、`internal/queue/memory.go` = `519f0dd9…`，每条腿跑完都验一次还原相同）：
> 1. **PG：撤掉 `reapCancellingSQL` 的 `cancelling` 那一支**（谓词改成 `'cancelling-LEG1-disabled'`，语句照样合法、事务照样跑）→ 只 `PG/ReapLandsOrphanedCancellingRow` 红：`suite.go:1272: reap after expiry = 0/<nil>, want 1/nil`。
> 2. **PG：内核过滤器变空转**（`= ANY ($4)` 后补 `OR ($4 IS NOT NULL)`，$4 仍被引用所以语句照样合法）→ 只 `PG/ClaimByKernelConsumesTheDeclaredCapabilities` 红：`suite.go:185: w-go 第一次领取: claim = c8227c06…, want 054fc581…`（把 nautilus 行派给了 go worker）。另三条 pair 用例照绿，说明过滤器与冻结面确实是分开的两层。
> 3. **PG：去掉 `jsonb_typeof='string'` 那层 CASE** → 只 `PG/ClaimByKernelReadsOnlyStringKernels` 红：`suite.go:235: claim 0 = …IdempotencyKey:k-null…`（该拿 `k-num`），证明那层 CASE 不是装饰。
> 4. **Memory：丢掉 `MatchesKernels` 的结果** → 只 `Memory/ClaimByKernelConsumesTheDeclaredCapabilities` 红，同一行同一种错位（`claim = 1ae99fe7…, want 95e69a31…`）。
> 5. **Memory：回收条件退回只看 `running`** → 只 `Memory/ReapLandsOrphanedCancellingRow` 红：`reap after expiry = 0/<nil>, want 1/nil`——与第 1 条**同一条用例同一句报错**，这就是「两侧同口径」的可执行形式。
>
> 诚实登记**未落**的部分：`ClaimByKernel` **没有生产消费方**——本仓没有 worker 进程（引擎在 core，红线禁止 import），所以今天用到它的只有共享套件与两个编译期断言；派活装配落地时必须做类型断言，不提供该接口的实现会**静默退回不过滤**（这条代价写在 §9.8 取舍 6）。`kernels` 空数组会被 `= ANY('{}')` 读成「什么都不能干」，实现里归一成 NULL 并在两条用例上钉住；`worker 心跳超时 → offline` 仍未落（方法集没有回收方法，同 R8 那类问题）。
> 门禁：`gofmt -l .` 空 + `go build ./...` + `go vet ./...` + `go test ./... -count=1`（无 DSN，PG 腿 skip）与带 `ORCHESTRATION_TEST_DSN` 各跑一遍全绿；干净 worktree 复核见下。本机 `-race` 仍跑不了（`CGO_ENABLED=0`、无 C 编译器）。
> 验收对照（本单 ⑤ 三条判据，施工 ④ 后）：「`pkg/backtest` 零命中」与「同一 batch 新旧一致」**仍未达成**（属 X5b/迁移腿，且第一条不可能由本仓产出，见下）；「provenance 端点返回三 hash」仍未达成（属 X0）。整体仍 **🔶**。


> **施工记录（2026-10-24，施工 ⑤：工单 G3「QT-44 的 X5c 心跳超时」）**：`WorkerStatus` 里的 `offline` 之前只是枚举值——冻结的 4 方法集没有入口能写它，心跳一停注册表仍显示 `active`。本轮按 R9 的同一先例（§9.6「可选能力走新接口 = MINOR」）补 **R10**：
> **接口与实现**：`queue.WorkerReaper.ReapOffline(ctx, now, timeout)`（`internal/queue/ports.go`），冻结的 `WorkerRegistry` 4 方法一字未动。判据只有心跳时间：`last_heartbeat_at <= now-timeout`（含等于，与 `ReapExpired` 的租约判据同口径）→ `offline`；`draining` 一并回收（滚动升级的收尾就是「跑完手上的 → 心跳停 → offline」）；不删行、已 offline 不重复计数、NULL 心跳不参与判定（没有心跳时间就无从判断存活，不猜）；`timeout <= 0` → `ErrInvalidInput`（阈值属部署，不给默认值）。内存侧在 `MemoryRegistry.ReapOffline`，PG 侧是 `reapOfflineSQL` 单条 UPDATE（单语句原子；与心跳并发时由行锁裁决，心跳先提交则 WHERE 重判、不会误杀）。
> **进入与退出都定死**：`offline` 是**推断**，不是运维意图——未请求过 drain 的 worker 可以被心跳或重注册拉回 `active`（两种都是存活证明）；请求过 drain 的不能（否则 drain 可被一次重注册绕过）。复活时**必须刷新心跳**（重注册走复活分支时不再继承过期心跳），否则下一轮回收立刻再判 offline，恢复形同虚设。不这么做的话，worker id 会被一次瞬时超时永久作废（崩溃重启仍是同一个 id，没有别的恢复入口）。**代价**：`Claim` / `ClaimByKernel` 对 offline 仍报 `ErrWorkerDraining`——冻结面只有一个「不配领新活」的哨兵，本单不新增错误语义（报文里带实际状态）。
> **接线**：`cmd/orchestrationservice` 的周期任务现在同一轮做两件回收（`-reap-interval` 默认 30s）：租约 `ReapExpired` + 心跳 `ReapOffline`（`-heartbeat-timeout` 默认 90s，0 = 关闭）。开启时装配期做能力断言并 fail-closed——不提供 `WorkerReaper` 的注册表不允许带着「以为会回收」启动。
> **套件**：每个实现 **45 条**（队列 30 + 注册表 9 + 跨 port 6；新增 `ReapOfflineMarksStaleWorkersOffline` / `ReapOfflineRejectsNonPositiveTimeout` / `OfflineWorkerRevivesOnProofOfLifeUnlessDrained` / `OfflineWorkerIsNotDispatchedUntilProofOfLife`），内存与 PG 跑同一批。新用例同时把施工 ③ 登记为「从 port 不可达」的 `offline` 再 drain → `ErrInvalidTransition` 分支补上了真入口。
> **破坏验证（一条腿，改坏应红 → 逐字节还原 → 绿）**：Memory 的 `cutoff := at.UTC().Add(-timeout)` 改成 `Add(timeout)`（阈值方向反了，语句照样合法）→ 三条红：`Memory/ReapOfflineMarksStaleWorkersOffline`（`reap before the cutoff = 2/<nil>, want 0/nil`）、`Memory/OfflineWorkerRevivesOnProofOfLifeUnlessDrained`（`reap right after revival = 1/<nil>`）、`Memory/OfflineWorkerIsNotDispatchedUntilProofOfLife`（`reap = 2/<nil>, want 1/nil`）。还原后 `internal/queue/worker.go` 的 sha256 与改前**逐字节相同**（`5456e309…`），整包复绿。PG 侧同一条腿**本轮未跑**：本机没有可用 PG（5432 未监听；Docker Engine 29.8.0 已可达，但本轮未起 PG 容器），PG 腿全部 `skip`——这是如实登记的缺口，不是通过。
> **门禁**：`gofmt -l .` 空；`go build ./...` / `go vet ./...` / `go test ./... -count=1` 全绿（无 DSN，PG 腿 skip，输出见提交记录）。本机 `-race` 仍跑不了（`CGO_ENABLED=0`、无 C 编译器）。
> **验收对照（G3）**：① 内存与 PG **同一套**新用例——代码同批落、套件同源；本机只跑过内存腿，PG 腿待有库环境/CI 补跑。② `offline` 后 `ClaimByKernel` 跳过该 worker——`OfflineWorkerIsNotDispatchedUntilProofOfLife` 在内存腿实测绿（两个领取入口都拒绝，行保持 `pending`，由在线 worker 领走）。③ `drain` 行为不回归——既有 `DrainingWorkerCannotClaimButUnregisteredCan` / `DrainStateMachine` / `ReRegisterKeepsStatusAndStartedAt` 全绿，且请求过 drain 的 offline worker 不被心跳/重注册洗回 active。


>
> **施工记录（2026-10-24，施工 ⑥：工单 P5-2「X5b batch / walkforward 窗口切分与汇总」）**：本轮只做**规划与汇总**，不做父表持久化、不做 HTTP 端点、不动 `run` schema。
> **落点**：`internal/plan`（纯函数，零第三方依赖）——`ExpandBatch`（网格笛卡尔积 + `strategy_params` 浅合并）、`SplitWindows` / `ExpandWalkForward`（交易日切窗 + 只改 `start_date` / `end_date` 派生 train/test）、`AggregateBatch` / `AggregateWalkForward`（三值计数、排名、父状态）；`internal/plansuite`（**共享套件**：7 条用例同时跑在 batch 与 walkforward 两个 Subject 上）。
> **口径（写进代码注释，不是口头约定）**：① batch 参数名按字典序、最后一个参数变化最快，展开上限 100（与 core REV-11 同值），空网格 / 空取值 / 非标量 / 尾随内容 / 超限 → `ErrInvalidRequest`（HTTP 层映射 `invalid_params`）；② walkforward 只生成**完整**窗口（train/test 各按交易日数），`step_days` 必须为正，日历必须严格递增（不偷偷排序），train/test 首尾相接不重叠，`min_train_days` 空 = `train_days/2`（固定窗口下恒成立）；③ 子 run 只改日期，kernel / fill_timing / seed / strategy_params / strategy_ref 原样保留；④ 汇总三值分开（`invalid` **不得**算成 `completed`），batch 必须传计划总数且与逐条回报数一致（漏报即拒），walkforward 缺段 / 缺窗算 pending；只对 `completed` 且给出有限 score 的子 run 排名（同分按 `run_id` 升序），`invalid` / `failed` 保留但 `rank=0`；⑤ 父状态：还有未回报 → `running`，任一 failed → `failed`（**不吞**子失败），否则 `completed`（`invalid` 不拖成 failed）。
> **R6 裁决（2026-10-25，裁-13）**：建**一张** `run.parent_jobs` 父表（`kind ∈ {batch, walkforward}`、`plan_canonical` / `plan_hash`、三值汇总列、父状态），`run.jobs` 加**可空 `parent_job_id` 列 + 索引**表达子 run 关系；**不塞 `request_json`**（`request_canonical` 锚点已证明 JSONB 不能当身份），**不建两张实体表**（两种父共用一套生命周期，共享套件 `internal/plansuite` 已在钉「不各写一套」）。上一行「不建第六张表、不写父行」是 R6 未裁时的处置，**由本裁决取代**；表/端点/子 run 入队/worker 执行仍未落，见批次四 W3。
> ~~**裁-15（2026-10-25）：本仓鉴权暂不做**~~ → **触发条件已满足**（ADR-036 把本仓定为对外服务；G0 compose 会绑 `0.0.0.0:8088`）。**鉴权归 `SVC-ORCH-G1`**：上 service token（不复用用户 JWT），`/healthz` 保持公开。裁-15 原文保留作历史。
> **套件计数**（`go test -v` 实测，不手数）：`internal/plan` 22 个顶层用例 / 53 条含子测试；共享套件在每个 Subject 上 7 条（`PlanIsDeterministic` / `EveryChildIsACompleteRunSpec` / `SummaryCountsThreeValuesWithoutCollapsing` / `InvalidIsNotAFailure` / `AnyFailureFailsTheParent` / `AllFailuresAreNotAnEmptySuccess` / `PendingKeepsTheParentRunning`）。**X5c 复核**：内存腿的 `ReapOfflineMarksStaleWorkersOffline` / `ReapOfflineRejectsNonPositiveTimeout` / `OfflineWorkerRevivesOnProofOfLifeUnlessDrained` / `OfflineWorkerIsNotDispatchedUntilProofOfLife` 全绿；PG 腿仍因本机无库 `skip`（施工 ⑤ 已如实登记，不是本轮新增缺口；**2026-11-07 已在真库补跑，见 `SVC-ORCH-G2` 施工记录**）。
> **破坏验证（四条腿，改坏 → 红 → 逐字节还原 → 绿）**。还原用 sha256 比对（`internal/plan/batch.go` = `ab51cd70…`、`internal/plan/walkforward.go` = `c3d33894…`、`internal/plan/aggregate.go` = `86cfd1c8…`，每条腿跑完都验一次还原相同）：
> 1. **batch：丢掉网格值合并**（`merged[k] = v` 那一行改成空操作）→ `TestExpandBatchMergesGridAndKeepsBaseParams` 红：`child 1 lookback = 20, want 10`。
> 2. **batch 汇总：`invalid` 计成 `completed`** → 共享套件 Batch 侧三条红（`SummaryCountsThreeValuesWithoutCollapsing` / `InvalidIsNotAFailure` / `AnyFailureFailsTheParent`）+ `TestAggregateBatchRanksOnlyCompletedWithScore` 红；报错是计数错位，不是编译错。
> 3. **walkforward：`testStart` 改成 `trainEnd`**（训练与验证重叠一个交易日）→ `TestSplitWindowsExactBoundaries` 红：`window 1 test = 2024-01-04..2024-01-05`。
> 4. **walkforward 汇总：窗口 `invalid` 计成 `completed`** → 共享套件 WalkForward 侧同样三条红，与第 2 条**同一批断言**——这就是「两种父不各写一套汇总口径」的可执行形式。
> **门禁**：`gofmt -l .` 空；`go build ./...` / `go vet ./...` / `go test ./... -count=1` 全绿（无 DSN，PG 腿 skip）；输出见提交记录。本机 `-race` 仍跑不了（`CGO_ENABLED=0`、无 C 编译器）。
> **验收对照（本轮工单）**：① 网格展开 / 窗口切分 / 三值汇总有**共享套件**——`internal/plansuite` 同跑 batch 与 walkforward，7 条/Subject，全绿。② **不改 `run` schema 已冻结的 5 表语义**——`internal/queue/ddl.go` 与 `docs/db-schema.md` 的表结构零改动；R6 未裁前不建表、不塞父元数据。③ 未落项如实登记：`batch_runs` / `walk_forward_reports` 写入、batch/walkforward 4 端点、`TradingCalendar` 只读 port 接线、子 run 入队、worker 执行、R6 子 run 关系。

> **施工记录（2026-10-25，施工 ⑦：工单 O4-1「X5b 父表（裁-13）」）**：R6 已裁（一张 `run.parent_jobs` + `backtest_jobs.parent_job_id`），父记录持久化与提交 / 汇总入口落地。
> **DDL**：追加 `run.parent_jobs`（`kind ∈ {batch, walkforward}`、`plan_canonical`/`plan_hash`、`status` 六态、`total/completed/invalid/failed`、`summary_json`）与 `backtest_jobs.parent_job_id`（可空 FK `ON DELETE SET NULL` + 部分索引）。**改原文而非追加**只适用于从未落库的表；`backtest_jobs` 已落库，故用 `ALTER TABLE … ADD COLUMN IF NOT EXISTS`（只前滚）。`batch_runs` / `walk_forward_reports` 降为迁移期旧表、停写（自始无写入路径，不存在存量）。
> **落点**：`internal/queue` 的 `ParentKind` / `ParentRecord` / `ParentSummary` / `ParentStore` + `MemoryParents` + `PrepareParent`（两个实现共用的 canonical/hash/计数校验）；`internal/pgstore.Parents`（`EnqueueParent` 在独立 advisory lock 里查键 / 插行，`SaveParentSummary` 单条 UPDATE）；`internal/plan/parent.go`（`BuildBatchPlan` / `BuildWalkForwardPlan` + `Canonical`/`Hash`）；`internal/parents`（`SubmitBatch` / `SubmitWalkForward` 提交即写父表；`ApplyBatchSummary` / `ApplyWalkForwardSummary` 复用 `Aggregate*` 写回三值计数）。**本仓零 import core**（新增代码只 import 本仓内部包）。
> **身份口径**：`plan_canonical` 是**展开后**的计划（kind + units + 子 run 完整 RunSpec + 窗口），TEXT 字节锚点；`plan_hash = sha256(plan_canonical)`。不塞 `request_json`（JSONB 规范化数字文本，会让同一次提交算出两个身份）。幂等同键同 hash → 原行；同键不同计划 → `ErrIdempotencyConflict`。
> **双父同跑**：`internal/plansuite` 新增第 8 条共享用例 `ParentPlanIdentityIsDeterministic`——batch / walkforward 各跑一遍：canonical 两次逐字节一致、hash 一致且 64 hex、`Units × UnitSize = 子 run 数`、父计划的子 run 与 `Plan` 同源。
> **破坏验证（三条腿，改坏 → 红 → 逐字节还原 → 绿）**：
> 1. `PrepareParent` 的 `plan_hash` 校验短路（`if false && …`）⇒ `TestMemoryParents_RejectsBadInput/hash_mismatch` 红（`err = <nil>, want ErrInvalidInput`）；还原后 `internal/queue/parent_memory.go` sha256 与改前相同（`eceaaf08…`）且复绿。
> 2. `summaryOf` 把 `Invalid` 写成 `counts.Failed` ⇒ `TestApplyBatchSummaryMatchesPlanAggregation`（`1/0/0 want 1/2/0`）与 `TestApplyWalkForwardSummaryMatchesPlanAggregation`（`1/1/1 want 1/0/1`）两红；还原后 `internal/parents/service.go` sha256 相同（`fd8f3195…`）且复绿。
> 3. DDL 把 `backtest_jobs_parent_idx` 改名成 `…_LEG3` ⇒ 第一版断言**假绿**（只比索引前缀，改名后的串仍包含它）；收紧成「索引名 + `ON run.backtest_jobs` 全串」后 `TestMigrationsCoverAllRunTables` 红（`backtest_jobs.parent_job_id index is missing`）；还原后 `internal/queue/ddl.go` sha256 相同（`a318919f…`）且复绿。**这条发现并修掉了一个假护栏**，不是预先设计好的。
> 诚实登记：PG 腿（`TestPGParentStoreRoundTrip`）本机无 `ORCHESTRATION_TEST_DSN` 时 **skip**，不是通过；`docs/db-schema.md` / `ARCHITECTURE.md` / `api-schema.md` / `dataflow.md` 同批更新。
> **验收对照（本单）**：① 父表 + 列 + 索引已落（DDL 断言）；② 提交即写父表（`internal/parents` + 两个 `ParentStore` 实现，单测覆盖重放 / 冲突 / 未知 id / 汇总自洽）；③ 汇总复用 `internal/plan`（服务测试逐字段对齐 `Aggregate*`）；④ 本仓零 import core（新增代码无跨仓 import）；⑤ 网格 / 窗口 / 三值计数与旧路径逐位一致（plansuite 双父同跑 + Parent 与 Plan 子 run 逐字节相同）。**未落**：batch / walkforward 的 4 个 HTTP 端点、子 run 入队（`parent_job_id` 的写入方）、worker 执行——需要 `TradingCalendar` 只读 port 与派活契约，不在本单。
> **施工记录（2026-10-25，施工 ⑧：X5b 父作业 HTTP 端点）**：`POST /api/runs/parents`（batch / walkforward 共用）与 `GET /api/runs/parents/{id}`（三值汇总 + 逐子状态）落地。
> **落点**：`internal/httpapi/parents.go`（严格解码、wire → `plan.BatchRequest` / `WindowConfig` 折装、字段点名、响应视图）；`api.go` 加 `WithParents` 装配项与 `parent_not_found` / `plan.ErrInvalidRequest` 的 wire 映射；`parents.Service.Get`；`queue.ParentChildLister` 可选接口（`Memory` / `pgstore.Queue` 两个实现，`WHERE parent_job_id=$1 ORDER BY created_at, id`）；`cmd/orchestrationservice` 装配 `store.Parents()`。
> **身份口径（本单收紧）**：父记录的身份是 **`plan_hash`**，不是幂等键——同计划（换键也算）返回同一父 id（200 `idempotent_replay`），同键换计划 → 409。DDL 追加 `parent_jobs_plan_hash_uidx`（唯一）；`MemoryParents` 加 `byHash`；`pgstore.Parents` 在 advisory lock 内「先按幂等键判冲突 → 再按 plan_hash 判重放 → 才插入」。**顺序是刻意的**：同键换计划必须先报冲突，不能被计划身份静默吞掉。
> **GET 口径**：`counts` = `completed/invalid/failed` + 派生的 `pending/settled`；`children[]` 的 `status` 是队列生命周期、`result_status` 是 K9（两轴不合并）；父 `status` 直接读落库值，不在读路径重算（重算会把「未汇总」伪装成另一种状态）。`children` 只列已入队的子 run——子 run 入队仍未落，所以今天通常是空数组（如实）。
> **测试**：`internal/httpapi/parents_test.go`（batch 新建 / 同键重放 / 换键同计划同 id / 同键换计划 409；无效计划 400 点名 kind/batch/grid/trading_days；walkforward 10 日切 3 窗；三值不塌缩 + 逐子状态 + 终态 `completed_at`；空 children 是 `[]`；未知父 404）；`internal/queue/parent_memory_test.go` 新增 plan_hash 身份与 `ListChildren` 两条；`pgstore` 的父表腿补「换键同计划」与 `ListChildren`（无 DSN 时 skip）。
> **破坏验证（两条腿，改坏 → 红 → 逐字节还原 → 绿）**：
> 1. 删掉 `MemoryParents.EnqueueParent` 的 `byHash` 分支 → `TestMemoryParents_PlanHashIsTheIdentityAcrossKeys` 红（`duplicate parent id "parent-1"`）、`TestSubmitParent_BatchPlanHashIsTheIdentity` 红（换键同计划 = 400 而非 200）；还原后 `internal/queue/parent_memory.go` SHA256 = `1B0D5930…4F430A2` 与改前一致，复绿。
> 2. GET 的 `ParentChildLister` 分支短路（`&& false`）→ `TestGetParent_CountsAndChildrenDoNotCollapse` 红（`children = 0, want 3`）；还原后 `internal/httpapi/parents.go` SHA256 = `B3F15516…E4A1B5E6` 与改前一致，复绿。
> **门禁（真实输出）**：`gofmt -l .` 空；`go build ./...` / `go vet ./...` / `go test ./... -count=1` 全绿（`internal/httpapi 0.080s`、`internal/queue 0.090s`、`internal/pgstore 0.115s`、`cmd/orchestrationservice 0.146s`）；本机无 `ORCHESTRATION_TEST_DSN`，PG 腿按既有约定 skip。本机 `-race` 仍跑不了。
> **未落（如实）**：子 run 入队（`parent_job_id` 的写入方）与 worker 执行；`GET` 的 `children` 因此通常为空。鉴权仍按裁-15 缓做。
>
> **施工记录（2026-10-25，施工 ⑨：X0 三 hash 读面 · B6-4）**：`run.run_artifacts` 的写入端口 + `GET /api/runs/{id}/provenance` + 父 GET 逐子三 hash 落地。**状态仍 🔶**：读面与写入端口已落，**生产者（写入方）仍是 core 引擎**——本仓没有 worker 进程，`SaveArtifact` 今天只被测试与编译期断言消费。
> **落点**：`internal/queue/artifacts.go`（`ArtifactRecord` 15 字段 + `ArtifactStore` 两方法 + `PrepareArtifact` canonical 校验 + `sameArtifactIdentity`）、`artifact_memory.go`（`MemoryArtifacts`，与 PG **同一套口径**：未知 run 拒绝 / 同身份幂等不刷 `created_at` / 身份漂移 `ErrArtifactConflict`）、`errors.go`（`ErrArtifactNotFound` / `ErrArtifactConflict`；注释写明不直接等于 HTTP 码，要结合 job 两轴）、`internal/pgstore/artifacts.go`（`saveArtifactSQL` = `INSERT … ON CONFLICT (run_id) DO UPDATE SET run_id = EXCLUDED.run_id WHERE <13 项身份全等>`，可空列 `IS NOT DISTINCT FROM`；**0 行受影响 ⇒ 冲突**，不能 `DO NOTHING`——那会把冲突读成成功；FK 23503 ⇒ `ErrJobNotFound`）、`internal/httpapi/provenance.go`（`provenanceView` 15 字段独立 DTO + `provenanceOf` + 三态错误）、`api.go`（`WithArtifacts` 装配项；可选能力 = MINOR，§9.6）、`parents.go`（`children[]` 加 `run_spec_hash` / `dataset_snapshot_hash` / `artifact_hash`）、`cmd/orchestrationservice`（装配 `store.Artifacts()` + 编译期断言）。`BacktestResponse` 的 27 项冻结字段集**一个字未动**。
> **同源口径**：`result_status` **只从 `backtest_jobs` 取**（`run_artifacts` 无 status 列，不存在第二个判据）；`created_at` 是留痕写入时间，不复用 job 创建时间。三态：非终态 → 409 `provenance_not_ready`；终态 `failed` / `cancelled` 无行 → 404 `provenance_not_found`；`completed` / `invalid` 无行 → 500 `provenance_missing`（**`invalid` 不是 `failed`**，缺留痕同样是完整性破坏）。可空列显式出 `null`，不省略键。
> **路由口径（实测逼出来的）**：`GET /api/runs/parents/{id}` 与 `GET /api/runs/{id}/provenance` 在 Go 1.22 ServeMux 里对 `/api/runs/parents/provenance` **互不更具体 ⇒ 注册期 panic**。处置是 catch-all `GET /api/runs/{rest...}` 后手工切 `/provenance` 后缀；jobs / parents 两条更具体的路由仍优先，钉子 `TestProvenance_CatchAllDoesNotStealOtherRoutes`。
> **测试（真实输出）**：`internal/httpapi/provenance_test.go` 6 个顶层用例（含 `ReturnsTheThreeHashesAndK9Conclusion` 的 `completed` / `invalid` 两个子测试）+ 父腿新增 `TestGetParent_ChildrenCarryThreeHashes` / `TestGetParent_ChildrenWithoutArtifactsOmitHashes`；`internal/queue/artifact_test.go` 7 个顶层 + 8 个必填字段子测试；`internal/pgstore/artifacts_test.go` 3 条 + `columns_internal_test.go` 的列形状核对（本机无 DSN ⇒ **skip，不是通过**）。
> **破坏验证（两条腿，改坏 → 红 → 逐字节还原 → 绿）**：
> 1. **两轴塌缩**：`provenanceOf` 的 `ResultStatus` 改成 `queue.ResultStatus(job.Status)`（把结论当生命周期）→ `TestProvenance_ReturnsTheThreeHashesAndK9Conclusion/invalid` 红（`result_status = 0xc00002d1f0, want "invalid"`——`invalid` 被读成 `completed`）；还原后 `internal/httpapi/provenance.go` SHA256 = `F70DA9E922AC9B9F7CC8D1D3D2AE19D7EC35EBC459D70D0C577D4AED5B01DFE0` 与改前一致、复绿。⚠️ 第一版破坏是「删掉 `ResultInvalid` 分支」，**没有变红**（`Status == RunCompleted` 兜底仍命中）——如实登记：这条腿第一版不具代表性，换成上面那条才有牙。
> 2. **父 GET 漏填 `artifact_hash`**：`child.ArtifactHash = art.ArtifactHash` 改成 `_ = art.ArtifactHash` → `TestGetParent_ChildrenCarryThreeHashes` 红（`DatasetSnapshotHash:dataset-hash-x0 ArtifactHash:`）；还原后 `internal/httpapi/parents.go` SHA256 = `CD98B5195C36BCFC45C32BDBC1844E9810FBAB71CE0471CD5D0692D68DBB9575` 与改前一致、复绿。
> **门禁（真实输出）**：`gofmt -l .` 空；`go build ./...` 退出 0；`go vet ./...` 退出 0；`go test ./... -count=1` 全绿（`cmd/orchestrationservice 0.275s`、`internal/httpapi 0.249s`、`internal/parents 0.105s`、`internal/pgstore 0.143s`、`internal/plan 0.095s`、`internal/queue 0.117s`）。本机 `127.0.0.1:5432` 未监听（`Test-NetConnection` = False）⇒ PG 腿全部 **skip**，不是通过；本机 `-race` 仍跑不了（`CGO_ENABLED=0`、无 C 编译器）。
> **未落（如实）**：① `run_artifacts` 的**生产者**（core 引擎在日循环前写留痕；本仓零 import core，不能代替）；② worker 进程与 worker 端点；③ 子 run 入队（`parent_job_id` 的写入方，父 GET 的 `children` 因此通常为空）；④ 鉴权（R5 / 裁-15 缓做）。**验收对照（本单 ⑤ ③）**：「provenance 端点返回三 hash」**读面已达成**（两条破坏验证钉住），端到端达成仍要等生产者接线。
>
> **裁-19 定稿（2026-10-25）：本仓 `/provenance` 在 worker 落地前不作为生产读面。**
> 三 hash 由 core 引擎计算，core 有权威表 `quant.run_provenance`；本仓的 `run_artifacts` 重新定义为「**worker 写的投影**」——生产者是未来的 worker（执行完从 core 响应写入），**core 不做双写**。因此：
> - 生产读面 = core 新增的 `GET /api/backtest/{id}/provenance`（core QT-72，读 `quant.run_provenance ⋈ backtest_jobs`）；
> - 本仓 `GET /api/runs/{id}/provenance` 与父 GET 的三 hash 字段保留（施工 ⑨ 成果不回滚），但文档/响应须让运维看出 **`producer_pending`**——「查不到」不等于「run 不存在」；
>   > **⚠️ 2026-10-25 实测订正 + 裁决归属**：`producer_pending` 在本仓**代码零命中**（`grep -rn producer_pending` 在 `*.go` = 0）——它**不是第四个状态码**（不与 `provenance_not_ready` / `provenance_not_found` / `provenance_missing` 三码竞争），而是**本仓端点的一个待实现提示位**。**裁决（2026-10-25，主任口径）⑬**：该提示位**归本仓实现**；**core 侧 QT-72 不引入它**（core 的生产者就是 core 引擎，永远有写者，没有「查不到 ≠ run 不存在」这个问题；core 的三态锚点直接沿用本仓的三个 wire code）。裁决全文见 `tellurion:docs/design/qt-decoupling/cross-repo-decision-record-2026-10-25.md` ⑬。**本仓的动作**：在本仓 `/provenance` 的响应里让运维看得出「生产者未接线」。
> - worker 落地时由 worker 写入 `run_artifacts`，并用 `saveArtifactSQL` 已有的「13 项身份全等否则 0 行受影响」语义做对拍；**不引入第二个写者**。
>
> **施工记录（2026-10-25，施工 ⑩：G3 ① 子 run 入队——`parent_job_id` 的写入方）**：`POST /api/runs/parents` 不再只写父记录：父行落库后逐条入队子 run，`run.backtest_jobs.parent_job_id` 有了唯一写入方。
> **落点**：`internal/parents` 的 `Service` 增加 `jobs queue.JobQueue`（`New(store, jobs, opts...)`，两者 nil 均装配期 panic；`EnqueueParent` 返回空 id 按内部错误拒绝——否则不同计划会撞成 `parent::1`）；`submit` 在 `EnqueueParent` 后调用 `enqueueChildren`；新增 `ChildIdempotencyKey(parentID, childIndex)` = `parent:<parent_job_id>:<child_index>`（stable，改规则 = MAJOR）与 `enqueueChildren`；`internal/httpapi.WithParents(store)` 直接复用构造 Server 时的 `s.q`（不另立装配项，避免父子两条路径指向不同队列实现）；`cmd/orchestrationservice` 无需改动。
> **口径**：① batch = 每个网格点一条；walkforward = 每窗 train/test 两条（父 `total` 仍是窗口数）。② 子 run 的 `CreatedAt` = 提交时刻 + 计划序号 × 1µs（PG 时间精度微秒）：`ListChildren` 的排序键是 `created_at → id`，固定时钟下若时间戳相同，顺序会退化成随机 id 的字典序，与计划顺序不一致。③ 父行与子 run **不在一个事务里**（两张表、两个 store）：入队中途失败（如 429 `queue_full`）时父行保留、返回错误；**重放同一 POST** 按 `plan_hash` 命中父行、按派生键逐条命中子 run，幂等补齐且不产生重复行。④ 不新增跨仓依赖：walkforward 的 `trading_days` 仍由调用方传入（`api-schema.md` §1 已冻结）；core QT-67 的只读端点只解除「若需自取日历」的外部前置，本单没有引入 core 的 HTTP 客户端。
> **测试（真实输出）**：`internal/parents` 9 个顶层用例（新增 `TestSubmitBatchEnqueuesChildrenWithParentJobID` / `TestSubmitWalkForwardEnqueuesTrainAndTestLegs` / `TestSubmitResumesAfterPartialChildEnqueue`）；`internal/httpapi` 24 个顶层用例——父作业用例改为断言「提交即入队」（batch 4 条 / walkforward 6 条、重放不重复、顺序与计划一致），provenance 的 2 条父 GET 用例改为从**计划子 run** 里领取，而不是手工 Enqueue。
> **破坏验证（一条腿，改坏应红 → 逐字节还原 → 绿）**：把 `enqueueChildren` 里的 `ParentJobID: parent.ID` 删掉（键仍由父 id 派生，语句合法、编译通过）→ **8 条红**：`internal/parents` 3 条（`children = 0, want 4 / 6 / 2`）+ `internal/httpapi` 5 条（`children after submit = 0 / <nil>, want 4 / 6`、`children = 0, want 4（提交即入队）`）。还原后 `internal/parents/service.go` 的 sha256 = `2ff5bc8a810aa85e740d9e3e2dd63b7566bbac5f3ddd674979af88dd79fde0bb`，与改前**逐字节相同**，整包复绿。
> **门禁（真实输出）**：`gofmt -l .` 空；`go build ./...` 退出 0；`go vet ./...` 退出 0；`go test ./... -count=1` 全绿（`cmd/orchestrationservice 0.013s`、`internal/httpapi 0.013s`、`internal/parents 0.012s`、`internal/pgstore 0.008s`、`internal/plan 0.012s`、`internal/queue 0.013s`）。本机无 `ORCHESTRATION_TEST_DSN`（5432 未监听）⇒ PG 腿全部 **skip，不是通过**；本机 `-race` 仍跑不了（`CGO_ENABLED=0`、无 C 编译器）。
> **验收对照（本单）**：① `parent_job_id` 有唯一写入方——逐条断言 `ParentJobID` / 派生键 / `request_hash`；② 同计划重放不重复——同键与换键两条腿都断言子 run id 集合不变；③ 部分失败可续——深度 2 的队列走两轮「领取 → 重放」补齐 4 条且无重复键；④ `GET /api/runs/parents/{id}` 的 `children[]` 不再是空数组（batch 4 / walkforward 6）。**未落（如实）**：worker 执行（本仓没有 worker 进程）；PG 腿本轮未在有库环境实跑。
> **台账**：G3 ① 子 run 入队 ✅（2026-10-25）；QT-44 整体仍 🔶——worker 执行、worker 进程 / 端点、鉴权（R5）未落。

> **施工记录（2026-10-25，施工 ⑪：H5 worker 打活面 + 父汇总真实写回）**：`internal/queue` 的 port 早已落全，本轮补的是「谁调这些接口」与「子 run 上报后父汇总怎么真的写回」。
> **落点**：`internal/httpapi/workers.go` —— `POST /api/runs/workers`（注册，幂等）/ `POST /api/runs/workers/{id}/heartbeat` / `POST /api/runs/workers/{id}/claim` / `POST /api/runs/workers/{id}/drain` / `GET /api/runs/workers` / `POST /api/runs/jobs/{id}/lease` / `POST /api/runs/jobs/{id}/result` / `POST /api/runs/jobs/{id}/cancel-ack`；`api.go` 的 `WithWorkers` 装配项、`jobView` 加 `cancel_requested`、`classify` 加 `lease_lost` / `worker_draining` / `kernel_filter_unavailable` / `parent_summary_failed`；`internal/parents.SyncSummary`（从已入队子 run 重算并写回父汇总）；`cmd/orchestrationservice` 加 `-worker-lease` 与监听前的能力断言。
> **装配期能力断言（fail-closed）**：`WithWorkers` 要求队列实现 `KernelAwareJobQueue`；同时装了父作业时还要求 `ParentChildLister`。`cmd` 在监听前再显式检查一遍并给出可读错误——不提供这两条能力的实现**不允许带着「以为会按内核派活 / 以为会写回父汇总」启动**。领取时：worker 声明了 `kernels` 才走 `ClaimByKernel`；队列不支持 → 503 `kernel_filter_unavailable`，**绝不静默退回不过滤**。
> **`kernels: []` 归一成 NULL**：HTTP 层把空数组归一成 nil 再交给注册表（PG 的 `= ANY('{}')` 会被读成「什么都不能干」，与「未声明能力 = 不过滤」相反）；空名字（`""`）→ 400。
> **父汇总真实写回**：`result` / `cancel-ack` 写完子 run 终态后调用 `SyncSummary`。它按 `ParentChildLister` 读回全部子 run，batch 用 `AggregateBatch`、walkforward 用 `AggregateWalkForward`；walkforward 的「子 run → 窗口 / 段」映射取自 `plan_canonical`（不靠下标猜）；已终态且计数 / 明细未变的父记录不重写（重复上报不刷新 `completed_at`）。`cancelled` 是终态但不是 K9 结论，按 `failed` 计入父汇总——不映射会让父记录永远停在 `running`。汇总写失败 → 500 `parent_summary_failed`，子 run 状态已写成功，重放同一上报即可（终态 + 同载荷幂等）。
> **测试（真实输出）**：`internal/httpapi/workers_test.go` 16 个顶层用例（注册归一 / 去重与空名拒绝 / 未知 worker 404 / 内核过滤 / 空声明不过滤 / draining 拒绝 / 装配 panic / 续租与 fencing / 上报 completed 驱动父汇总并保序 / invalid 不是 failed / failed 带错误并拖垮父 / 载荷校验 / cancel-ack 收尾父 / walkforward 六段三窗 / 路由不被 provenance catch-all 抢 / HTTP 层真的把 nil 交给注册表）；内存腿与 `ORCHESTRATION_TEST_DSN` 的 PG 腿同跑。
> **破坏验证（三条腿，改坏 → 红 → 逐字节还原 → 绿；还原用 sha256 比对 `internal/httpapi/workers.go` = `7569e96c…`）**：
> 1. `syncParent` 直接 `return nil`（`if true || …`）→ 四条红：`TestReport_CompletedWritesParentSummaryFromChildren`（`after 1/2 … Completed:0 Pending:2`）、`TestReport_InvalidIsNotFailedInParentAggregation`（`status = running, want completed`）、`TestCancelAck_SettlesParentChild`（父仍 `running`）、`TestWalkForwardSummary_MapsLegsFromPlanCanonical`（父仍 `running`）。
> 2. 声明内核时改调 `Claim`（绕过 `ClaimByKernel`，`_ = kq` 保语句合法）→ `TestClaim_FiltersByDeclaredKernels` 红：`w-go claimed …「kernel":"nautilus"」…`。
> 3. `normalizeKernels` 的空数组分支改成 `return in, nil` → 第一版**假绿**：内存实现的 `Clone()` 会把非 nil 空切片洗成 nil，只断言 `List` 的结果测不到 HTTP 层。补 `recordingRegistry`（登记 `Register` 收到的原始记录、不 Clone）后 `TestRegisterWorker_HandsNilKernelsToRegistry` 红：`registry received kernels = []string{}, want nil`。**这是本轮发现并修掉的一条假护栏**。
> **门禁（真实输出）**：`gofmt -l .` 空；`go build ./...` 退出 0；`go vet ./...` 退出 0；`go test ./... -count=1` 全绿（`cmd/orchestrationservice 0.370s`、`internal/httpapi 0.323s`（含 16 条 worker 用例）、`internal/parents 0.235s`、`internal/pgstore 0.290s`、`internal/plan 0.235s`、`internal/queue 0.250s`）；`ORCHESTRATION_TEST_DSN=… go test ./internal/pgstore -count=1` **PASS 6.673s**（语义套件 + 注册表 + 跨 port，全部跑过；skip ≠ pass）。本机 `-race` 仍跑不了（`CGO_ENABLED=0`、无 C 编译器）。
> **未落（如实）**：① `run_artifacts` 的**生产者**——worker 进程已在 core（`tellurion:cmd/worker`），但它今天只上报 K9 结论与 `result_uri`，尚未把 core 的三 hash 写入本仓的 `run_artifacts`，本仓也没有对应的写入端点（X0 仍 🔶）；② 鉴权 R5 按裁-15 缓做，worker 身份仍只是一个 id；③ core worker 的生产装配仍有已知缺口（DataAdapter / 市场路由 / risk rules 取数口未搬，锚点 RunSpec 只能恢复 `backtest_jobs` 里有的字段）——见 `tellurion:docs/TASKS.md` QT-44 指针的施工记录。
> **台账**：X5c ✅（2026-10-25）；X5b 的父汇总写回 ✅；QT-44 整体仍 🔶（X0 生产者 / 鉴权 R5）。

> **⚠️ 工单 G7 的两条验收前提被源码证伪（2026-10-23 实测，如实登记，不当已完成）**：
> 1. **「`pkg/backtest` 里 `job|batch|walkforward` 零命中」不是本仓能产出的判据**：core `pkg/backtest` 实测 **1032 处命中 / 39 个文件**（`job/job.go` 207、`batch/batch.go` 67、`walkforward/walkforward.go` 34、`aliases.go` 39…）。达成它要的是 core 侧 O3（`tellurion:docs/design/qt-decoupling/orchestration-service-design.md` §7「编排层整体移出 `pkg/backtest`」），而 R1 与本仓 `db-schema.md` §5 冻结的路径是「双写 → 对拍 → 切读 → 停写」——旧路径在 O0 切换前**必须继续服务**。本仓能做的是把 durable 侧与 port 做出来，不能单方面让别人的包变空。
> 2. **「同一 batch 在服务与旧路径结果一致」与同一张工单的第 2 步自相矛盾**：core 的 batch 只要 `err == nil` 就记 `completed`、从不读 `resp.Status`（`core:pkg/backtest/batch/batch.go:249`），`toBacktestResult` 又把 status 字段丢掉（`:412-446`），于是 K9 `invalid` 被算进 `completed` / `success_count` / 评分均值；工单第 2 步又要求「批量报告不得把 `invalid` 当成功」。**修掉它的代价就是数字不再与旧路径相同**。本仓按「必须相同的部分（网格展开、窗口集合、逐 run 请求派生）钉一致，故意不同的部分（三值计数、失败窗口显式计数）钉分歧」执行，并在 X5b 的验收里逐条写明哪条是哪种。

>
> **M8 前置完成（2026-10-22）**：接口对表见 [`ARCHITECTURE.md`](ARCHITECTURE.md) §9。开工前先读 §9.1 的八条前置事实——`JobStore` **不是队列**（F1：`map[string]any` + `CreateJob` 直接起 goroutine + 内存 cancel）；异步请求**表达不了完整 RunSpec**：`commission_rate`/`slippage_rate` 落库后无读取点、策略 `params` 不被应用、`fill_timing` 缺位（F2）；无幂等键、无取消端点、`DELETE` 语义是删行（F3）；provenance 已落 `quant.run_provenance`，但 `run_artifacts` 与 `/api/runs/*` 零实现（F4）；batch 报告只在内存且把 K9 `invalid` 当成功（F5）；walkforward 直连 `*storage.PostgresStore`、请求缺参、逐窗失败静默消失（F6）；worker 注册/心跳/drain 零实现（F7）；**三 hash 是终态不变量、不是入队门槛**（F8）。本单当时**仍 ⬜**（对表是开工前置，不是实现）；R1~R7 未裁前 X5a/X5b/X5c/X0 各自阻塞。
> 对表自检（2026-10-22，基线 `dbe4c68`）：71 条 `path:line`（core 69 + agent 2）按 HEAD 逐条命中；C 组 11 端点 / 14 错误码与 `api-schema.md` 双向一致；破坏验证三腿（改错行号 / 删一行端点 / 删一个错误码）各自红，还原后绿（文件 sha256 与改前相同）。

- **① 目标**：批量扫描的算力出口独立成服务；`backtest_jobs` 归属迁出内核；三 hash 落 `run_artifacts`。
- **② 上下文**：`orchestration-service-design.md`（D22）+ BE-03；`BatchEngine` 已按 `contracts.EngineRunner` 注入，摘出成本低。
- **③ 要求**：① `job`/`batch`/`walkforward` 从 `pkg/backtest` 上移；② worker 池调度 + 断点续跑协调；③ `run` schema（`backtest_jobs`/`run_artifacts`/`worker_registry` + **裁-13 订正后的父表**：`batch_runs`/`walk_forward_reports` 合并为一张 `parent_jobs`，子关系用 `backtest_jobs.parent_job_id`），**不含 `strategy_state`**（H5 裁决：归 strategy-runtime 模块，core 继续写 `quant.strategy_state`）；④ `/api/runs/{id}/provenance` 端点；⑤ 内核通过 `JobQueue` port 读写 job 表；`run_artifacts` 的吸收口径见 QT-63 裁决（过渡期 core `quant.run_provenance`，O1 双写/对拍/切读/停写后取代）。
- **② 前置（2026-10-21 订正：原 U3 `RunStateStore` port 已作废）**：H5 裁决（`tellurion:docs/design/qt-decoupling/orchestration-service-design.md` §3 归属修正）明确 `strategy_state` **不迁 `run`**、归 strategy-runtime 模块（K0 冻结契约 `contracts/kernel_modules.schema.sql`），core 继续写 `quant.strategy_state`；因此 `RunStateStore` port **不需要**，本单前置减少为「`JobQueue` port 外置」一项。
- **④ 约束与非目标**：不在内核保留编排逻辑；不改 `EngineRunner` 契约；不做 K8s；**不搬 `cmd/dualrun`**——它 2026-10-20 已裁决留 core（要同进程链接两个内核，且是引擎的验收门，见 `repo-inventory.md` §3）。
- **⑤ 验收（2026-10-25 裁-13 改写）**：① **本仓零 import core**（本仓可自证的边界）；② 同一 batch 的网格展开 / 窗口集合 / 逐 run 请求派生与旧路径逐位一致，三值计数与失败窗口显式计数按 X5b 登记的分歧钉住；③ provenance 端点返回三 hash（属 X0）。~~「`pkg/backtest` 里 `job|batch|walkforward` 零命中」~~ **移出本仓验收**：core 实测 1032 处 / 39 文件，删除它属 core 侧 O3（双写→对拍→切读→停写），另立单，不由本仓产出。
- **⑥ 边界**：只做编排与结果归属。


---

> **⚠️ 开工前置（refactor-plan-review M5/M8，2026-10-20 登记）**：本表任何单开工前，必须先过 core `tellurion:docs/TASKS.md`「五、X 线」的**公共前置**两条：① 建仓/升格前先与 ADR-027 §2 对账（12 仓是上限/当前规划，不是承诺终态）；② 本仓设计文档先写「**接口对表**」小节（签名 + 错误语义 + 版本策略，模板 = `tellurion:docs/design/qt-decoupling/data-service-contract-and-ownership.md` §4.1），**未对表不开工**。正文以彼处为准，此处只留指针、不镜像。
> **2026-10-22 订正**：原文模板指向 `target-module-topology.md` 的 UnifiedDataPoint 节——该引用在 core 仓是**幻影**（QT-42/QT-43 已实测订正），本仓同步改为 `data-service-contract-and-ownership.md` §4.1。M8 已落 §9（QT-44 侧）；M5 已结案（六仓 ≠ 六进程，本仓今天不升格容器）。

### SVC-ORCH-G2 · 服务就绪门 G2：`run.*` 唯一 writer · artifact 写端点 · 迁移重放（ADR-036）[P0] ✅（2026-11-07 真库腿补跑通过）

> **2026-11-07 真库腿补跑（✅）**：便携 PostgreSQL 16.2 @ `127.0.0.1:55432`；
> `ORCHESTRATION_TEST_DSN='postgres://postgres@127.0.0.1:55432/tellurion_orchestration_test?sslmode=disable' go test ./... -count=1 -v`
> → 顶层 `PASS=313 / SKIP=0`、退出码 0；其中 `TestPGMigrationsAreReplayable` PASS，`internal/qtest` 的 PG 侧 45 条**不再 skip**。
> ⑤①~⑤⑥ 均已有真实输出（② 跨 schema 护栏破坏验证沿用本节原记录）。

- **① 目标**：`run` schema 六张表满足 ADR-036 §3 的 G2（自有 schema、迁移可重放、**无跨 schema 写**），并把裁决 ⑱ 要求的 artifact **写端点**落出来——它是「唯一 writer」这句话的实现，不是新功能。
- **② 上下文**：`internal/queue` 有六表 DDL（含 `run.parent_jobs` / `run.run_artifacts`），`internal/pgstore` 是 PG 实现，`cmd/orchestrationservice` 启动即迁移并监听 `:8088`（DSN 为空 fail-closed）；语义套件 `internal/qtest` 每个实现 45 条用例（内存与 PG 同跑）。**缺口**：① `SaveArtifact` 今天**只被测试与编译期断言消费**（本仓台账施工 ⑨ 自证），即 `run_artifacts` 有读面无写者；② 裁决 ⑱ 正文（本文件 `:14`）：「X0 生产者 = **本仓新增 artifact 写端点**（`POST /api/runs/jobs/{id}/artifacts`，service 角色），core worker 调它、**不直写库**；幂等沿用 `queue.ArtifactStore.SaveArtifact`；**前置 = 本仓先做 R5 鉴权**」——这条裁决**没有对应条目**，此前只存在于指针行；③ 鉴权未落（`internal/httpapi` 无 `auth.go` / `policy.go`，`go.mod` 无 kit），故 `SVC-ORCH-G1` ⛔；④ core 仍以裸表 `backtest_jobs` 为状态/结果权威（双 owner，SYSTEM-DESIGN §5）。
- **③ 要求**：① 迁移重放自证：同一测试库连跑两次 DDL，行数 / `pg_indexes` / 列集指纹逐位不变（`ORCHESTRATION_TEST_DSN` 腿，无库 skip）；② 禁跨 schema 写护栏：静态扫本仓 SQL，只允许 `run.` 的写语句，出现 `quant.` / `market.` / `analysis.` / `risk.` 即红；③ **artifact 写端点**：`POST /api/runs/jobs/{id}/artifacts`，复用 `ArtifactStore.SaveArtifact` 的幂等与身份漂移语义（同 13 项身份 ⇒ 幂等；漂移 ⇒ 冲突码而非静默覆盖，`saveArtifactSQL` 今天已是 `DO UPDATE … WHERE <身份全等>`、0 行受影响即冲突），**不新建第二套写入路径**；④ 前置 R5 鉴权的最小面（`service` 角色 + `policy.go`），使写端点不是裸口；⑤ 把 `run_artifacts` 的「谁写 / 谁读 / 保留期」写进 `db-schema.md`（core 侧材料 §5.2 K4 问 `run.walk_forward_reports` / `run.batch_runs` 的写者归谁，本单一并给答案）。
- **④ 约束与非目标**：**不自造 wire 形状**（`SVC-ORCH-G1` ⛔ 的禁区原文：「不得在本仓自造 claim / `run_artifacts` 结构（会变成第三份 DTO）」）——若写端点的请求体需要跨仓形状，先报协调者走 `tellurion-contracts:CT-04`，本仓可用**已有的本地 DTO** 起步但必须在 `api-schema.md` 标注它是本地形状；不改队列/租约语义；不动 core 的 `backtest_jobs`；不 `DROP` 表；不引入 contracts（裁决 #9 本轮不做）。
- **⑤ 验收**：① 重放腿两次指纹逐位相同（贴输出）；② 破坏验证：加一行 `UPDATE quant.runs` ⇒ 护栏红、还原复绿；③ 写端点四条用例：首次写入 201/200、同身份重放幂等（不刷 `created_at`）、身份漂移返回冲突码且**原行未被覆盖**、run 不存在 → 明确 404；④ 未带 service token 打写端点 ⇒ 401/403（证明它不是裸口）；⑤ `internal/qtest` 的 45 条在内存与 PG 两侧仍全绿（换实现行为不变）；⑥ 三条命令单仓绿。
- **⑥ 边界**：本仓 schema 与写端点；worker 侧产出与上报归 `tellurion:QT-78`，core 旧 job 表停写归 `tellurion:SVC-CORE-RUN-RETIRE` + 本仓 `SVC-ORCH-G3`。
- **⑦ 施工记录（2026-11-07）**：
  - **落地**：① 迁移重放用例 `internal/pgstore/g2_pg_test.go`（同一 DSN 连跑两遍 `Migrate`，比对列 / `pg_indexes` / 六表行数 sha256 指纹；`ORCHESTRATION_TEST_DSN` 未设则 skip）。② 跨 schema 护栏 `internal/httpapi/g2_schema_test.go`：静态扫全仓 `.go` 写语句，只允许 `run.` 前缀；本仓六表写成裸名也红。③ artifact 写端点 `internal/httpapi/artifacts.go`：`POST /api/runs/jobs/{id}/artifacts`，请求体 = **本地 DTO** `queue.ArtifactRecord`（`api-schema.md` §1 已标注本地形状；契约化归 `SVC-ORCH-G1` / `CT-04`），`run_id` 与路径不一致 → 400，`created_at` 服务端盖戳，首次 201 / 同身份重放 200（不刷 `created_at`）/ 漂移 409 `artifact_conflict` / run 不存在 404 `job_not_found`；冲突判定只在 `ArtifactStore.SaveArtifact`，HTTP 层不重造。④ R5 最小面：`internal/httpapi/auth.go` + `policy.go`（只有一条策略，`service` 角色；`AuthStatus="write-endpoint-only"`），`cmd` 加 `-jwt-secret` / `AUTH_JWT_SECRET` / `-insecure-exposure` + `DecideStartup` + `NewAuthorizer`；`go.mod` 引入 `tellurion-kit v0.1.4`。⑤ `db-schema.md` §1.1 登记六表「写者 / 读者 / 保留」，`walk_forward_reports` / `batch_runs` 明确「停写、O1 对拍后删除」。
  - **用例**：`internal/httpapi/g2_test.go` 六条 —— 201→200 且 `created_at` 不刷新、身份漂移 409 且原行 `params_hash` 未变、未知 run 404、`run_id` 不一致 400、无凭证 401 / viewer 403 / service 201、未装配鉴权闸 503；`TestArtifactWritePolicyIsServiceOnly` 钉住策略只有写端点一条且角色只有 service。
  - **破坏验证（真做真还原）**：① `run_id` 一致性检查短路 ⇒ `TestSaveArtifact_RejectsRunIDMismatch` 红（201 ≠ 400）；② 策略角色改 `viewer` ⇒ `TestSaveArtifact_RequiresServiceRole` 红（403 ≠ 201）；③ 临时加 `UPDATE quant.strategies …` ⇒ `TestNoCrossSchemaWrites` 红（报 `internal\httpapi\artifacts.go: UPDATE quant.strategies`）。三条还原后复绿。
  - **未执行（当日早先状态，已被本节开头「真库腿补跑（✅）」取代）**：迁移重放**真库腿**未执行（`ORCHESTRATION_TEST_DSN` 未设、本机 Docker 引擎不可用）；`internal/qtest` 的 **PG 侧 45 条**同样未执行（无库），内存侧全绿。handler 级用例不能替代「真库 + 真迁移」验证。
  - **文档**：`docs/api-schema.md`（§1 写端点 + 请求/响应、§2 三个新码、§3 artifact 幂等、§6 鉴权现状）、`docs/db-schema.md`（§1.1 + `run_artifacts` 行订正）、本节、`AGENTS.md`。
  - **门禁（真实退出码）**：`gofmt -l .` 无输出、`go build ./...`=0、`go vet ./...`=0、`go test ./... -count=1`=0（`cmd/orchestrationservice`、`internal/httpapi`、`internal/parents`、`internal/pgstore`、`internal/plan`、`internal/queue` 全 ok；含 skip 的重放腿）。

### SVC-ORCH-G4 · 服务就绪门 G4：租约/回收的失败语义 · worker 可观测 · runbook（ADR-036）[P0] ✅（2026-11-07，真库腿补齐）

- **① 目标**：把「worker 挂了 / 租约过期 / 任务卡在 running / 队列满」这四类失败的说法定成文档并有判据——本仓是整个编排链里唯一持有状态机的地方。
- **② 上下文**：`cmd/orchestrationservice` 已有周期 `ReapExpired` / `ReapOffline`（`-worker-lease` 默认 60s）、`Claim`/`ClaimByKernel` 共用一条 `claimSQL`（`FOR UPDATE SKIP LOCKED`）、提交路径用 `pg_advisory_xact_lock` 让 `queue_full` 阈值在并发下精确、R8（`cancelling` 行回收出口）与 R9（`Kernels` 领取过滤）已落两个实现、子 run 终态由 `parents.SyncSummary` 重算写回父汇总（`cancelled` 按 failed 计）；声明 `kernels` 但队列不支持过滤时 503（不静默不过录）——这条 fail-closed 姿势是对的。**缺口**：`/healthz` 有（`cmd/orchestrationservice/main.go`）但**无 `/readyz`**（全 workspace 0 命中；PG 不可达时进程仍「健康」是编排层的危险默认）；无 runbook（全 workspace `runbook` 只在 ADR-036 正文）；回收的两条语义（多久算过期、被回收的 run 归谁）只在代码与施工记录里。
- **③ 要求**：① 失败语义表五类：租约过期被回收 / worker 离线被 `ReapOffline` / `queue_full` / `cancelling` 长期不落终态 / 上游 core 引擎返回 `invalid`（非 `failed`），各给「状态怎么写、HTTP 返回什么、调用方该做什么、是否重投」；② `/readyz`：PG 池可连 + 迁移已执行，未就绪 503；`/healthz` 保持进程语义并在文档写明两者区别；③ 回收参数（lease、心跳间隔、reaper 周期）出厂值与调界口径成文，写明「调大 lease = 故障 worker 占活更久」这个代价；④ 运维可见性：至少给「当前 running / queued / 各 worker 在领什么」一条查询面（`workers.go` 已有列表与 drain，确认它足以定位，并把用法写进 runbook）；⑤ `docs/runbook.md`：起停、看回收、人工释放卡住的 run（含 SQL 姿势与授权边界）、父/子汇总不一致时怎么核对、集成腿怎么跑；⑥ 一次演练：杀掉一个领了活的 worker ⇒ 断言该 run 在 lease 过期后被回收且不丢、父汇总把 `cancelled`/失败计入（这条是本仓最值钱的回归证据，目前没有一次性演练记录）。
- **④ 约束与非目标**：不改状态机词汇；不动 core 的 `cmd/worker`（跨仓，归 `tellurion:QT-78` / `QT-79`）；不引 APM/编排框架；不做多副本 HA；不在本单落鉴权（属 `SVC-ORCH-G1` 与 `SVC-ORCH-G2` ④）。
- **⑤ 验收**：① 失败语义表五类各有用例或注明原因；② `/readyz` 两条用例（无 PG → 503、恢复 → 200）；③ 演练记录：杀 worker 前后各一次真输出（含被回收 run 的终态与父汇总数字），退出码如实；④ runbook 每条命令当场执行过；⑤ 三条命令绿。
- **⑥ 边界**：本仓失败语义与运维面；契约化与全量鉴权归 `SVC-ORCH-G1`，切读与旧 job 退役归 `SVC-ORCH-G3`。

> **施工记录（2026-11-07，代码 + 内存级演练）**：
> - **代码落点**：`cmd/orchestrationservice/main.go` 新增 `readiness` 接口与 `newMux`（`/healthz` = `text/plain` `ok`，不查依赖；`/readyz` = 2s 超时内查 PG 池 + `run.*` 六表迁移，`nil` → 503 `store=unwired`、err → 503 `store=postgres` + `error`、ok → 200 `{"status":"ready","store":"postgres"}`）；`internal/pgstore/store.go` 新增 `Store.Ready(ctx)`（Ping + `to_regclass` 六表）；`cmd/orchestrationservice/main_test.go` 新增 `fakeReadiness` + `TestProbesSplitProcessFromStore` + `TestReadyzFailsClosed`（业务面用 418 占位，探针误落业务面会立刻暴露）。
> - **演练用例**：`internal/queue/g4_drill_test.go`（入队 → `w-dead` 领取 → 时钟越过租约 → `ReapExpired` n=1 → 行回 `pending` / `attempts=1` / 租约清空 → **另一个 worker 领到同一条** → 写 `completed` → 全表仍 1 行）；`internal/parents/g4_drill_test.go`（4 条子 run 的 batch → 三轮「过期 → 回收」耗尽默认预算 → `failed` + `attempts_exhausted`；另一条 `pending → cancelled`；`SyncSummary` 后父计数 `total 4 / completed 0 / invalid 0 / failed 2`、状态仍 `running`、子行仍 4）。
> - **文档**：新增 `docs/runbook.md`（起停 / 探针分工 / 五类失败语义表 / 回收参数出厂值与调界（含「调大 lease = 故障 worker 占活更久」）/ 运维可见性 / 人工释放 SQL 与授权边界 / 父-子汇总核对与已知缺口 / 集成腿 / 演练记录）；`docs/api-schema.md` §1 增「探针（公开，无鉴权）」；`docs/README.md` 导航与文档地图补 runbook；`docs/ARCHITECTURE.md` §9.8 装配行补 `/readyz` 与 runbook 指针；`AGENTS.md` §1 / §5。
> - **演练真实输出**：`go test ./internal/queue -run TestDrillDeadWorkerRunIsReclaimedAndCompleted -v` → `--- PASS`，`ok … 0.046s`；`go test ./internal/parents -run TestDrillReclaimedAndCancelledChildrenCountAsFailed -v` → `--- PASS`，`ok … 0.043s`（退出码均为 0）；`go test ./cmd/orchestrationservice -run 'TestProbesSplitProcessFromStore|TestReadyzFailsClosed' -v` → 两条 `--- PASS`。
> - **破坏验证（真做真还原）**：把 `internal/parents/service.go` 的 `taskOutcomeOf` 里 `cancelled → failed` 临时改成空分支 → 演练红：`g4_drill_test.go:95: parent counts = total 4 / completed 0 / invalid 0 / failed 1, want 4/0/0/2`；逐字节还原后 `go test ./internal/parents -count=1` 复绿。另一条：把 `/readyz` 的 `ready == nil` 分支改成 200 → `TestReadyzFailsClosed` 红（`unwired /readyz = 200, want 503`），已还原。
> - **门禁（真实退出码）**：`gofmt -l .` 无输出、`FMT=0 BUILD=0 VET=0 TEST=0`（`cmd/orchestrationservice`、`internal/httpapi`、`internal/parents`、`internal/pgstore`、`internal/plan`、`internal/queue` 全 ok）。
> - **真库腿（2026-11-07 晚补齐；便携 PG 16.2 `127.0.0.1:55432` 库 `tellurion_orchestration_test` + 真 `cmd/orchestrationservice`，服务参数 `-reap-interval 2s -heartbeat-timeout 3s -worker-lease 5s`，监听 `127.0.0.1:18088`）**：
>   - **② `/readyz` 503 → 200（真做真还原，同一服务进程）**：起库后 `GET /readyz` → `200 {"status":"ready","store":"postgres"}`；`pg_ctl -D <data> stop -m fast` → `STOP_EXIT=0`，随后 `/readyz` → `503 {"error":"pgstore: ping: failed to connect to … dial tcp 127.0.0.1:55432: connectex: No connection could be made because the target machine actively refused it.","status":"not_ready","store":"postgres"}`，同刻 `/healthz` 仍 `200` + `ok`（探针分工成立）；重启库后 `/readyz` → `200 {"status":"ready","store":"postgres"}`（**未重启服务**，池自愈）。
>   - **③ 租约回收演练（真库 + 真服务；HTTP 级模拟 worker 消失，未杀 core 进程）**：`POST /api/runs/parents` → `202 parent_job_id=3866fdc02b693c0a9a6b006669f7900c`（`total=2`、`counts.pending=2`）；注册 `w-dead` 并 claim → `run_id=a45df712ba08e14b5499d50b4ec2b4ed`、`attempts=0`、`lease_until=2026-11-07T07:44:39Z`；停心跳 12s 后该行回 `pending`（`started_at` 保留、`completed_at=null`）、`w-dead` 变 `offline`；注册 `w-alive` 后 claim 到**同一条** `a45df712…`（`attempts=1`，**不丢、不新建行**）；租约再过期 → `attempts=2`；预算耗尽 → 子 run `failed` + `error="attempts_exhausted"` + `completed_at=2026-11-07T07:45:42.7539Z`，第二条子 run `e60182f4…` 同样 `failed/attempts_exhausted`。父汇总数字（真库 SQL，即 runbook §7.2 原文）：`status=running / total=2 / completed=0 / invalid=0 / failed=0 / children=2 / kids_failed=2 / kids_open=0`。
>   - **④ runbook §6 的 SQL 当场执行**：本机 PG 发行版只带 `initdb` / `pg_ctl` / `postgres`（**无 `psql` 客户端**），故用 `postgres --single -D <data> tellurion_orchestration_test` 执行同形 SQL（该模式逐行执行，语句须单行）。§6.2 观察命中 `ccb5e769… status=running attempts=1 worker_id=w-sql overdue=00:00:07.86214`；§6.3 释放 SQL 生效 → 后镜像 `status=pending attempts=2`（同一 SQL 在行已非 `running` 时 0 行受影响，守卫成立）；§6.5 校验输出与之一致；`SQL_EXIT=0`。恢复服务后 `GET /api/runs/jobs/ccb5e769…` → `200 status=pending`。
>   - **仍未执行（如实登记，不写成「通过」）**：① 杀**真** core worker 进程的端到端演练（worker 在 `tellurion:cmd/worker`，本仓无引擎；本次用 HTTP 停心跳模拟 worker 消失）；② `docker compose -f deploy/compose.yaml up`（Docker 引擎不可用）。
> - **遗留缺口（runbook §7.3 已写）**：`parents.SyncSummary` 只在子 run 上报路径触发；reaper / 人工 SQL 让子 run 落终态时父汇总不会立即重算，父记录会停在 `running`，直到同父下另一条子 run 上报。本单不扩范围修，显式登记。**本次真库演练已复现该缺口**（父 `status=running / failed=0` 而两个子 run 均 `failed`、`kids_open=0`），数字见上；不在本单修。

## 施工序（候选单，开工前必须展开成六字段）

> 这张表是**施工顺序**，不是任务台账：每条在执行前要展开成六字段单（目标/上下文/要求/约束/验收/边界），并写明冻结契约的路径与节号。
> 顺序有依赖，**不要跳级**：跳序的代价是返工（例：X1a 的协议没冻结就写 aggregator，等于把协议写死在一个服务里）。
> 2026-10-21：候选单已展开为「一、本仓任务」的正式六字段条目（表中「上游单号」指向的 QT 单正文现在就在本仓）。

| 阶段 | 内容 | 上游单号 | 前置 |
|---|---|---|---|
| X5a | job 队列 + 状态机 + 幂等键 🔶（施工 ① `internal/queue` 语义；② jobs 4 端点 HTTP 面；③ **DDL 已接 PG + 启动迁移 + 双实现共享用例**；④ R8 回收出口 + R9 `ClaimByKernel` 落两个实现）| `QT-44` | 本仓立项 |
| X5b | batch + walkforward（窗口切分与汇总）🔶 规划与汇总纯函数 + 共享套件（2026-10-24 施工 ⑥）+ **父表 / 子 run 关系 / 提交与汇总落库（2026-10-25 施工 ⑦，裁-13）** + **父作业 HTTP 2 端点（2026-10-25 施工 ⑧，`POST/GET /api/runs/parents`）** + **子 run 入队（2026-10-25 施工 ⑩，`parent_job_id` 唯一写入方）** + **父汇总真实写回（2026-10-25 施工 ⑪：子 run 上报终态即 `SyncSummary`）**；worker **执行**在 core（`tellurion:cmd/worker`），本仓零 import core | `QT-44` | X5a |
| X5c | worker 注册 / 心跳 / `drain` / 心跳超时 ✅（2026-10-25：施工 ① 内存；③ PG 已落；⑤ 心跳超时→`offline` 两个实现同批 + `cmd` 接线；⑪ **worker 打活 HTTP 面**：注册 / 心跳 / 领取 / 续租 / 上报 / 取消确认 / 列表 / drain，装配期能力断言 fail-closed。**worker 进程在 core**，本仓零 import core） | `QT-44` | X5a |
| X0 | `run_artifacts` 三 hash 留痕 + `/provenance` 🔶（施工 ⑨：`ArtifactStore` 写入端口 + 内存 / PG 两个实现、`GET /api/runs/{id}/provenance` 三态、父 GET 逐子三 hash；**写端点已落（2026-11-07，G2 ③）** `POST /api/runs/jobs/{id}/artifacts`，core worker 调它、不直写库（裁决 ⑱）；worker 侧实际回传仍归 `tellurion:QT-78`）。**裁-19 定稿（2026-10-25）**：生产读面改为 **core** `GET /api/backtest/{id}/provenance`（core QT-72）；本仓端点保持 `producer_pending`，`run_artifacts` = worker 写的投影，core 不双写 | `QT-03` | X5a |

## 二、跨仓指针（不在本仓，勿双写）

| 单号 | 主题 | 在哪 |
|---|---|---|
| QT-44 | X5 编排 / worker 服务（job/batch/walkforward 上移） | 已迁入本仓「一、本仓任务」（2026-10-21；core `tellurion:docs/TASKS.md` 留同号指针） |
| QT-08 | 本仓 CI / AGENTS / 台账由该单一并补齐 | `tellurion:docs/TASKS.md` |
| CT-04 | 编排 wire 形状已落 contracts（`run/`：`ClaimView` / `ProvenanceView` / `ParentSubmitRequest` 族 + 子 run 幂等键） | `tellurion-contracts:docs/TASKS.md`（✅ 2026-11-07；`v0.8.0` → `v0.9.0` 已可经远端解析）；本仓与 core 的收编已落（`SVC-ORCH-G1` 施工记录 2026-11-09） |

---

## 维护本文件

- **新增单**：写全六字段；六字段里必须显式引用**冻结契约的路径与节号**（契约仓的包路径、设计文档的节号），不许写"参考相关实现"。
- **派工前先证伪前提**：读码核对工单写的前提；不符就停下回报 + 给拆法，不硬做、不扩范围。
- **跨仓引用写 `repo:path`**（如 `tellurion:docs/TASKS.md`），不做可点击相对链接。
- **顺序纪律**：同一波次内撞同一批文件的单按单号串行，一单一 commit。
