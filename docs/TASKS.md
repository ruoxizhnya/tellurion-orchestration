---
status: active
last-verified: 2026-10-25
verified-by: QT-44 施工 ①②③④⑤⑥⑦（④ = R8 / R9；⑤ = R10；⑥ = X5b 规划与汇总 + 共享套件；⑦ = 裁-13 父表 `parent_jobs` + `backtest_jobs.parent_job_id` + 提交 / 汇总落库）
---

# TASKS — tellurion-orchestration 未完成项

> 本仓**唯一**任务台账。跨仓任务只在 §二 留指针，**不镜像正文**（双写必然漂移）。
> 状态：`⬜ 待做` / `🔶 进行中` / `⛔ 阻塞（有未解除前置）` / `✅ 完成（附日期）`
> 每张单六字段（① 目标 ② 上下文 ③ 要求 ④ 约束与非目标 ⑤ 验收 ⑥ 边界），判据见 `tellurion:AGENTS.md` §8.5。
> 完成即标 ✅（附日期）并移入 `docs/archive/TASKS-YYYY-MM-DD-archived.md`；本文件只留未完成项。

---

## 一、本仓任务

> **约定变更（2026-10-21）**：原「占位仓不写正式六字段任务单」作废——X 线任务单已展开为本仓正式任务（见下）。M5/M8 开工前置不变；实现仍未开工，开工前仍须过前置两条。

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
> **破坏验证（一条腿，改坏应红 → 逐字节还原 → 绿）**：Memory 的 `cutoff := at.UTC().Add(-timeout)` 改成 `Add(timeout)`（阈值方向反了，语句照样合法）→ 三条红：`Memory/ReapOfflineMarksStaleWorkersOffline`（`reap before the cutoff = 2/<nil>, want 0/nil`）、`Memory/OfflineWorkerRevivesOnProofOfLifeUnlessDrained`（`reap right after revival = 1/<nil>`）、`Memory/OfflineWorkerIsNotDispatchedUntilProofOfLife`（`reap = 2/<nil>, want 1/nil`）。还原后 `internal/queue/worker.go` 的 sha256 与改前**逐字节相同**（`5456e309…`），整包复绿。PG 侧同一条腿**本轮未跑**：本机没有可用 PG（5432 未监听、Docker daemon 未启动），PG 腿全部 `skip`——这是如实登记的缺口，不是通过。
> **门禁**：`gofmt -l .` 空；`go build ./...` / `go vet ./...` / `go test ./... -count=1` 全绿（无 DSN，PG 腿 skip，输出见提交记录）。本机 `-race` 仍跑不了（`CGO_ENABLED=0`、无 C 编译器）。
> **验收对照（G3）**：① 内存与 PG **同一套**新用例——代码同批落、套件同源；本机只跑过内存腿，PG 腿待有库环境/CI 补跑。② `offline` 后 `ClaimByKernel` 跳过该 worker——`OfflineWorkerIsNotDispatchedUntilProofOfLife` 在内存腿实测绿（两个领取入口都拒绝，行保持 `pending`，由在线 worker 领走）。③ `drain` 行为不回归——既有 `DrainingWorkerCannotClaimButUnregisteredCan` / `DrainStateMachine` / `ReRegisterKeepsStatusAndStartedAt` 全绿，且请求过 drain 的 offline worker 不被心跳/重注册洗回 active。


>
> **施工记录（2026-10-24，施工 ⑥：工单 P5-2「X5b batch / walkforward 窗口切分与汇总」）**：本轮只做**规划与汇总**，不做父表持久化、不做 HTTP 端点、不动 `run` schema。
> **落点**：`internal/plan`（纯函数，零第三方依赖）——`ExpandBatch`（网格笛卡尔积 + `strategy_params` 浅合并）、`SplitWindows` / `ExpandWalkForward`（交易日切窗 + 只改 `start_date` / `end_date` 派生 train/test）、`AggregateBatch` / `AggregateWalkForward`（三值计数、排名、父状态）；`internal/plansuite`（**共享套件**：7 条用例同时跑在 batch 与 walkforward 两个 Subject 上）。
> **口径（写进代码注释，不是口头约定）**：① batch 参数名按字典序、最后一个参数变化最快，展开上限 100（与 core REV-11 同值），空网格 / 空取值 / 非标量 / 尾随内容 / 超限 → `ErrInvalidRequest`（HTTP 层映射 `invalid_params`）；② walkforward 只生成**完整**窗口（train/test 各按交易日数），`step_days` 必须为正，日历必须严格递增（不偷偷排序），train/test 首尾相接不重叠，`min_train_days` 空 = `train_days/2`（固定窗口下恒成立）；③ 子 run 只改日期，kernel / fill_timing / seed / strategy_params / strategy_ref 原样保留；④ 汇总三值分开（`invalid` **不得**算成 `completed`），batch 必须传计划总数且与逐条回报数一致（漏报即拒），walkforward 缺段 / 缺窗算 pending；只对 `completed` 且给出有限 score 的子 run 排名（同分按 `run_id` 升序），`invalid` / `failed` 保留但 `rank=0`；⑤ 父状态：还有未回报 → `running`，任一 failed → `failed`（**不吞**子失败），否则 `completed`（`invalid` 不拖成 failed）。
> **R6 裁决（2026-10-25，裁-13）**：建**一张** `run.parent_jobs` 父表（`kind ∈ {batch, walkforward}`、`plan_canonical` / `plan_hash`、三值汇总列、父状态），`run.jobs` 加**可空 `parent_job_id` 列 + 索引**表达子 run 关系；**不塞 `request_json`**（`request_canonical` 锚点已证明 JSONB 不能当身份），**不建两张实体表**（两种父共用一套生命周期，共享套件 `internal/plansuite` 已在钉「不各写一套」）。上一行「不建第六张表、不写父行」是 R6 未裁时的处置，**由本裁决取代**；表/端点/子 run 入队/worker 执行仍未落，见批次四 W3。
> **裁-15（2026-10-25）：本仓鉴权暂不做**。客户端是 worker（注册 / 心跳 / 领取），上用户 JWT 等于要给 worker 发凭证，是新的失败面；默认监听 `127.0.0.1:8088` 已回环。**触发条件（任一）**：worker 跨主机 / 服务对外发布 / 引入外部提交方——届时上**服务间凭证**（service token），不复用用户 JWT。
> **套件计数**（`go test -v` 实测，不手数）：`internal/plan` 22 个顶层用例 / 53 条含子测试；共享套件在每个 Subject 上 7 条（`PlanIsDeterministic` / `EveryChildIsACompleteRunSpec` / `SummaryCountsThreeValuesWithoutCollapsing` / `InvalidIsNotAFailure` / `AnyFailureFailsTheParent` / `AllFailuresAreNotAnEmptySuccess` / `PendingKeepsTheParentRunning`）。**X5c 复核**：内存腿的 `ReapOfflineMarksStaleWorkersOffline` / `ReapOfflineRejectsNonPositiveTimeout` / `OfflineWorkerRevivesOnProofOfLifeUnlessDrained` / `OfflineWorkerIsNotDispatchedUntilProofOfLife` 全绿；PG 腿仍因本机无库 `skip`（施工 ⑤ 已如实登记，不是本轮新增缺口）。
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

## 施工序（候选单，开工前必须展开成六字段）

> 这张表是**施工顺序**，不是任务台账：每条在执行前要展开成六字段单（目标/上下文/要求/约束/验收/边界），并写明冻结契约的路径与节号。
> 顺序有依赖，**不要跳级**：跳序的代价是返工（例：X1a 的协议没冻结就写 aggregator，等于把协议写死在一个服务里）。
> 2026-10-21：候选单已展开为「一、本仓任务」的正式六字段条目（表中「上游单号」指向的 QT 单正文现在就在本仓）。

| 阶段 | 内容 | 上游单号 | 前置 |
|---|---|---|---|
| X5a | job 队列 + 状态机 + 幂等键 🔶（施工 ① `internal/queue` 语义；② jobs 4 端点 HTTP 面；③ **DDL 已接 PG + 启动迁移 + 双实现共享用例**；④ R8 回收出口 + R9 `ClaimByKernel` 落两个实现）| `QT-44` | 本仓立项 |
| X5b | batch + walkforward（窗口切分与汇总）🔶 规划与汇总纯函数 + 共享套件（2026-10-24 施工 ⑥）+ **父表 / 子 run 关系 / 提交与汇总落库（2026-10-25 施工 ⑦，裁-13）**；**HTTP 4 端点 / 子 run 入队 / worker 执行未落** | `QT-44` | X5a |
| X5c | worker 注册 / 心跳 / `drain` / 心跳超时 🔶（施工 ① 内存；③ PG 已落；⑤ 心跳超时→`offline` 两个实现同批 + `cmd` 接线；worker 进程本身未做） | `QT-44` | X5a |
| X0 | `run_artifacts` 三 hash 留痕 + `/provenance` | `QT-03` | X5a |

## 二、跨仓指针（不在本仓，勿双写）

| 单号 | 主题 | 在哪 |
|---|---|---|
| QT-44 | X5 编排 / worker 服务（job/batch/walkforward 上移） | 已迁入本仓「一、本仓任务」（2026-10-21；core `tellurion:docs/TASKS.md` 留同号指针） |
| QT-08 | 本仓 CI / AGENTS / 台账由该单一并补齐 | `tellurion:docs/TASKS.md` |

---

## 维护本文件

- **新增单**：写全六字段；六字段里必须显式引用**冻结契约的路径与节号**（契约仓的包路径、设计文档的节号），不许写"参考相关实现"。
- **派工前先证伪前提**：读码核对工单写的前提；不符就停下回报 + 给拆法，不硬做、不扩范围。
- **跨仓引用写 `repo:path`**（如 `tellurion:docs/TASKS.md`），不做可点击相对链接。
- **顺序纪律**：同一波次内撞同一批文件的单按单号串行，一单一 commit。
