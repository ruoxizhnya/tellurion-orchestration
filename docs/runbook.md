---
status: active
last-verified: 2026-11-11
verified-by: 代码-文档对齐审计（2026-11-11：回收参数默认值 `-worker-lease 60s` / `-reap-interval 90s` / `-offline-after 30s` 与代码复核；失败语义五类未变）；SVC-ORCH-G4（失败语义五类 + `/readyz` + 回收参数口径 + 运维可见性 + 两条内存级演练 + 2026-11-07 真库腿：`/readyz` 503→200、租约回收、§6 SQL）
---

# Runbook — tellurion-orchestration

> 读者：值班 / 部署方 / 下一个改本仓的人。
> 本文件只讲**怎么起、怎么看、怎么救**；形状与语义的真相源是
> [`api-schema.md`](api-schema.md)、[`db-schema.md`](db-schema.md)、[`dataflow.md`](dataflow.md)，本文件不重复定义。
> 跨仓引用一律写 `repo:path` 纯文本。

## 0. 本机执行边界（先读，避免把「写了」当「跑过」）

| 腿 | 状态 | 证据 |
|---|---|---|
| `gofmt` / `go build` / `go vet` / `go test` | **已执行** | 退出码 `FMT=0 BUILD=0 VET=0 TEST=0`（2026-11-07） |
| 两条探针用例（`/healthz` ≠ `/readyz`、未就绪 503） | **已执行** | `go test ./cmd/orchestrationservice -run 'TestProbesSplitProcessFromStore|TestReadyzFailsClosed' -v` → 两条 PASS |
| 内存级演练：死 worker 回收后仍可完成 | **已执行** | `go test ./internal/queue -run TestDrillDeadWorkerRunIsReclaimedAndCompleted -v` → PASS |
| 内存级演练：回收/取消计入父汇总 | **已执行** | `go test ./internal/parents -run TestDrillReclaimedAndCancelledChildrenCountAsFailed -v` → PASS |
| 无 DSN 拒绝启动 | **已执行** | `ORCHESTRATION_DSN='' go run ./cmd/orchestrationservice` → `exit status 1` + `no PostgreSQL DSN: …` |
| 真库 `/readyz` 503 → 200（同一服务进程） | **已执行** | 便携 PG 16.2 `127.0.0.1:55432`：`pg_ctl … stop -m fast`（`STOP_EXIT=0`）后 `/readyz` → 503（`pgstore: ping: … actively refused`），同刻 `/healthz` 仍 200 `ok`；重启库后 → 200（2026-11-07） |
| 真库租约回收演练（HTTP 级模拟 worker 消失） | **已执行** | 见 §9.4：`a45df712…` attempts 0→1→2 → `failed/attempts_exhausted`，父汇总数字同节 |
| 真库 §6 观察 / 人工释放 SQL | **已执行** | 本机 PG 发行版**无 `psql`**，用 `postgres --single -D <data> tellurion_orchestration_test` 执行同形 SQL；`SQL_EXIT=0`（2026-11-07，详见 §6 末注） |
| 杀真 worker 进程的端到端演练 | **未执行** | worker 进程在 core（`tellurion:cmd/worker`），本仓无引擎；本次以 HTTP 停心跳模拟 worker 消失 |
| `docker compose -f deploy/compose.yaml up` | **未执行** | Docker 引擎不可用 |

> 下文命令凡标「未执行」的，都是**按代码与 DDL 写的姿势**，不冒充已验收。第一条真库执行者请把结果补进本节与 `docs/TASKS.md` SVC-ORCH-G4 的施工记录。

## 1. 起停

### 1.1 依赖与启动顺序

```
DSN（-dsn 或 ORCHESTRATION_DSN，两者都空即拒绝启动）
  → pgstore.Open（连库）
  → store.Migrate（run.* 六表，按序幂等；在监听**之前**）
  → 装配 Jobs / Parents / Workers / Artifacts
  → 启动周期回收（-reap-interval > 0）
  → 监听 -addr（默认 127.0.0.1:8088）
```

不给默认 DSN 是故意的：默认值会替部署方猜库，猜错了服务照样起来「正常工作」。
**本仓不在启动时退回内存队列**——那会把「重启即丢」伪装成 durable 部署。

### 1.2 直接起（单仓）

```bash
go build ./...
ORCHESTRATION_DSN='postgres://postgres:postgres@127.0.0.1:5432/tellurion_orchestration?sslmode=disable' \
  go run ./cmd/orchestrationservice -addr 127.0.0.1:8088
```

真实失败腿（已执行）：

```text
$ ORCHESTRATION_DSN='' go run ./cmd/orchestrationservice
orchestrationservice: no PostgreSQL DSN: pass -dsn or set ORCHESTRATION_DSN (本仓不在启动时退回内存队列——那会把「重启即丢」伪装成 durable 部署)
exit status 1
```

### 1.3 compose（未执行）

```bash
docker compose -f deploy/compose.yaml up -d
```

### 1.4 停

`SIGTERM` / `Ctrl-C` → 停止接收新请求 → 最多 10s 优雅退出（`http.Server.Shutdown`）。
进程退出**不**改任何行状态：running 行的租约会在过期后由下一轮 reaper 回收，不依赖进程生死判定（这正是 core 旧做法 `CleanupStaleRunning` 被换掉的原因）。

### 1.5 启动参数（出厂值）

| flag | 环境变量 | 出厂值 | 说明 |
|---|---|---|---|
| `-dsn` | `ORCHESTRATION_DSN` | 无 | 必填；空即拒绝启动 |
| `-addr` | — | `127.0.0.1:8088` | 监听地址 |
| `-max-queue-depth` | — | `10000` | `pending` 深度上限；达上限提交回 `429 queue_full` |
| `-default-max-attempts` | — | `3` | 行未指定 `max_attempts` 时的重试预算 |
| `-reap-interval` | — | `30s` | 回收周期；`0` = 关闭两类回收 |
| `-heartbeat-timeout` | — | `90s` | 心跳超时 → worker `offline`；`0` = 关闭心跳回收 |
| `-worker-lease` | — | `60s` | worker 领取时写入的 `lease_until` |
| `-rate-limit` | — | `50` | 按调用方身份的令牌桶：每秒令牌数（与兄弟仓同口径；`<=0` 关闭） |
| `-rate-limit-burst` | — | `200` | 令牌桶突发上限（`<=0` 关闭） |
| `-startup-timeout` | — | `30s` | 连库 + 迁移的超时 |
| `-jwt-secret` | `JWT_SECRET` / `AUTH_JWT_SECRET` | 空 | 有 secret ⇒ 全闸；空 ⇒ 第 2 态（读公开、写 503 `auth_unavailable`），进程照常起 |
| `-insecure-exposure` | `AUTH_INSECURE_EXPOSURE` | 空 | 第 3 态的显式开放声明（只接受 `loopback-published`）；有声明时**任何绑定**都放开，启动 WARN 点名写面无鉴权与绑定地址 |
| `-allow-insecure` | `AUTH_ALLOW_INSECURE` | `true` | 是否允许无 secret 的降级/开放态；`false` = `JWT_SECRET` 必填，缺则拒绝启动 |

鉴权三态（2026-11-12 收紧，六仓统一口径）：

1. 有 `JWT_SECRET` → 全闸（401 / 403 不变）；
2. 无 secret + 无显式声明 → **只读降级**：读端点公开，写端点（含 `POST /api/runs/jobs`、worker claim/lease/result、artifact 写）一律 `503 auth_unavailable`；
3. 无 secret + 显式声明（`AUTH_INSECURE_EXPOSURE=loopback-published`，或显式 `AUTH_ALLOW_INSECURE=true` / `-allow-insecure=true`）→ **任何绑定**都放开写面：启动 WARN 同时写出「写面无鉴权」与绑定地址，非回环绑定额外点名「已对外暴露」；未知声明值直接拒绝启动。

启动日志会打印实际生效值：

```text
orchestrationservice: listening on 127.0.0.1:8088 (max_pending=10000, reap=30s, heartbeat_timeout=1m30s, auth=false, auth_scope=all-endpoints-service, rate_limit=50/s burst=200)
```

## 2. 探针：`/healthz` 与 `/readyz`

两个探针**都在鉴权之外**（部署面必须无凭证可用），也都不经过业务路由表。

| 探针 | 回答的问题 | 查什么 | 200 | 非 200 |
|---|---|---|---|---|
| `GET /healthz` | 进程还在监听吗 | 不查任何依赖 | `text/plain`：`ok` | 进程没了 = 连接失败 |
| `GET /readyz` | 现在能接活吗 | PG 池可连 + `run.*` 六表已迁移（`to_regclass`） | `{"status":"ready","store":"postgres"}` | `503` + `{"status":"not_ready","store":"unwired"}`（未装配）或 `{"status":"not_ready","store":"postgres","error":"…"}` |

口径三条：

- **`/healthz` 200 不代表能接活**：PG 挂了但进程还活着，它仍然 200。编排层唯一持有状态机，把「进程活着」当「服务可用」是危险默认。
- `/readyz` 的库检查有 2s 超时；失败原因原样进 `error`（便于区分「连不上」与「缺表」）。
- 装配错误（`ready == nil`）也 fail-closed 回 503，不默认 ready。

已执行用例：`TestProbesSplitProcessFromStore`、`TestReadyzFailsClosed`（`cmd/orchestrationservice/main_test.go`）。
真库恢复腿（503 → 200）**已执行（2026-11-07）**：便携 PG 16.2 + 真 `cmd/orchestrationservice`（`127.0.0.1:18088`）。`pg_ctl -D <data> stop -m fast` → `STOP_EXIT=0`；随后 `/readyz` → `503 {"error":"pgstore: ping: failed to connect to … dial tcp 127.0.0.1:55432: connectex: No connection could be made because the target machine actively refused it.","status":"not_ready","store":"postgres"}`，同刻 `/healthz` → `200` + `ok`；重启库后 `/readyz` → `200 {"status":"ready","store":"postgres"}`（**未重启服务**，连接池自愈）。

## 3. 回收参数与调界口径

### 3.1 出厂值

| 参数 | 出厂值 | 作用对象 |
|---|---|---|
| `-worker-lease` | `60s` | 行：领取 / 续租写入的 `lease_until` |
| `-reap-interval` | `30s` | 进程：每轮做「租约回收 + 心跳回收」 |
| `-heartbeat-timeout` | `90s` | worker 记录：`last_heartbeat_at <= now - timeout` → `offline` |
| `-default-max-attempts` | `3` | 行：回收时 `attempts` 的预算 |

### 3.2 调界口径

- **故障 worker 的行最晚多久被回收 ≈ `lease + reap-interval`**（默认 60s + 30s = 90s）。租约过期只是「可回收」，真正迁移要等下一轮 reaper。
- **调大 `-worker-lease` = 故障 worker 占活更久**：行在租约到期前不会被回收，整条链路的重试排队都会后移。调小的代价是网络抖动 / GC 停顿 / 慢 IO 时被误回收（worker 恢复后上报只会拿到 `409 lease_lost`，白干一次并消耗重试预算）。
- 经验关系：`-worker-lease` 应 ≥ 3 × worker 的续租间隔；`-heartbeat-timeout` 应 ≥ 3 × worker 的心跳间隔，且 **大于** `-worker-lease`（否则 worker 已被判 offline，它手里的行还没到期，`GET /api/runs/workers` 会先出现「全部 offline 但仍有 running 行」的迷惑画面）。
- `-reap-interval=0` 关闭**两类**回收；`-heartbeat-timeout=0` 只关心跳回收。关掉回收的后果：死 worker 的 running 行**永久**卡住，只能人工释放（§6）。
- 开启心跳回收但注册表实现不了 `WorkerReaper` 时，装配期直接拒绝启动——**不让「以为在回收、其实没有」的进程上线**。

## 4. 五类失败语义（故障排查的主表）

| # | 场景 | 判定 | 状态怎么写 | HTTP 表现 | 调用方 / 运维该做什么 | 是否重投 |
|---|---|---|---|---|---|---|
| 1 | **租约过期**（worker 崩溃 / 卡死 / 分区） | `status='running' AND lease_until <= now()` | `attempts+1`；未达 `max_attempts` → `pending`（`lease_until=NULL`，`worker_id` 保留作审计，下一次领取覆写）；达到 → `failed` + `error_message='attempts_exhausted'` + `completed_at=now()` | 无直接响应（后台迁移）。原 worker 若复活后 `POST …/lease` 或 `…/result` → `409 lease_lost` | 看 `attempts` 与 `error_message`；同一 `job_id` 不会新建行 | **是**（预算内）。被回收的行可被任意 worker 领取 |
| 2 | **worker 心跳超时** | `last_heartbeat_at <= now - heartbeat-timeout` | worker 记录 `active/draining → offline`；**不删行**；不直接改它手里的 run（行仍按租约判定） | `offline` / `draining` worker 领取 → `409 worker_draining` | 排查进程 / 网络；重新注册或心跳可恢复 `active`（未请求 drain 的行） | 行不重投；只是该 worker 不再领新活 |
| 3 | **队列满** | `pending` 行数 ≥ `-max-queue-depth` | 新行**不写**；已接受的行不受影响。父作业逐条入队子 run 时同样判定（父记录保留） | 提交 → `429 queue_full` | 退避重试；父作业用**同一幂等键**重放补齐，不换键 | **是**（退避）。不得静默丢弃 |
| 4 | **`cancelling` 长期不落终态** | `status='cancelling' AND lease_until <= now()`（owner 消失了） | → `cancelled`：`result_status=NULL`、`cancel_requested=TRUE`、`completed_at=now()`、`lease_until=NULL`；**不计 `attempts`**（不是重试，是给已作废的结果收尾） | owner 活着：`POST …/cancel-ack` → `200`；非 owner / 租约已过期 → `409 lease_lost` | 先看 `GET /api/runs/jobs/{id}` 的 `cancel_requested`；不要重复取消（幂等） | **否**。`cancelling` 的唯一出口是 `cancelled` |
| 5 | **上游引擎返回 `invalid`** | worker 调 `POST …/result` 报 `result_status=invalid` | `status='completed'` + `result_status='invalid'`，`error_message` **空**（无效不是故障） | 上报 200；查询见 `completed/invalid` | 读结果的 `result_uri` 判无效原因；父汇总把 `invalid` **单列**，不并入 `failed` | **否**（终态）。不得改写成 `failed` |

补充口径：

- `failed` 与 `invalid` 是两根轴：`status` 是生命周期，`result_status` 是 K9 三值。把 `invalid` 写成 `failed` 会让「跑了但结论无效」与「没跑成」混为一谈。
- 父记录的状态判据：还有子 run 未回报 → `running`；任一子 run 失败 → `failed`；否则 `completed`。`cancelled` 与无 K9 结论的 `failed` **按 failed 计入父汇总**（否则父记录会永远等一个不会来的回报）。
- `lease_lost` 的调用方纪律：**立即停止**，不得再写结果——写不进去，重试只会继续 409。

## 5. 运维可见性（判断「卡在哪」的最小面）

| 想知道 | 怎么看 |
|---|---|
| 有哪些 running / pending | `GET /api/runs/jobs?status=running&limit=100`、`GET /api/runs/jobs?status=pending&limit=100` |
| 某条 run 卡在哪一步 | `GET /api/runs/jobs/{id}`（`status` / `result_status` / `attempts` / `error` / `cancel_requested`） |
| worker 池状态 | `GET /api/runs/workers`（`status` / `kernels` / `last_heartbeat_at` / `drain_requested_at`） |
| 滚动升级摘除一台 | `POST /api/runs/workers/{id}/drain`（只停止领新活；它手里的 running 行仍归它到租约结束 / 上报） |
| 批量计划的进度 | `GET /api/runs/parents/{id}`（三值汇总 + 逐子状态 + 明细） |
| 三 hash 留痕 | `GET /api/runs/{id}/provenance`（未写 → `409 provenance_not_ready`；终态缺失 → `404/500` 见 `api-schema.md` §2） |

两条注意：

- **列表上限 100**（`limit<=0` 默认 50，超出钳到 100）。用 HTTP 列表数不出「离 `queue_full` 还有多远」；要精确深度用 §6 的 `count(*)` SQL。
- `GET /api/runs/workers` 只反映**注册表**；进程被杀但心跳还没超时的那段时间，它会显示 `active`——这正是要看 `last_heartbeat_at` 的原因。

## 6. 人工释放卡住的 run（SQL 姿势与授权边界）

### 6.1 授权边界（先看，再动手）

- **服务进程与 worker 不得直写 `run.*`**：行的唯一写路径是队列 port / HTTP 面（artifact 另有专用写端点）。直写会绕开租约判定与父汇总重算。
- 只有 DBA / 值班在 incident 下可以执行本文 SQL，且必须同时记录：工单号、`run_id`、执行前 `SELECT` 输出（前镜像）、执行后校验输出。
- **只能在租约已过期、且确认原 worker 不会再上报时**动手；否则会和 worker 的 `Complete/Fail` 抢同一行，结果由行锁先后决定。
- **不得**把行直接写成终态（`completed` / `failed` / `cancelled`）：父汇总的重算入口是「子 run 上报」（`parents.SyncSummary`），人工写终态**不会**触发它，父记录会停在 `running`。
- **不得**改 `idempotency_key` / `request_hash` / `request_canonical` / `plan_hash`：它们是幂等与计划身份锚点，改了等于换了一条任务。

### 6.2 观察（只读）

```sql
SELECT id, status, attempts, max_attempts, worker_id, lease_until,
       now() - lease_until AS overdue,
       cancel_requested, error_message
  FROM run.backtest_jobs
 WHERE status IN ('running','cancelling')
 ORDER BY lease_until;
```

先确认 reaper 是否在跑：日志里应有周期性 `reaped N expired leases`（N=0 不打印）。若 `-reap-interval=0` 或进程没起，才会走到人工这条腿。

### 6.3 释放一条 `running`（与 `reapSQL` 同形，单行）

```sql
BEGIN;

SELECT id, status, attempts, max_attempts, lease_until
  FROM run.backtest_jobs
 WHERE id = :'run_id'
   FOR UPDATE;

UPDATE run.backtest_jobs
   SET attempts      = attempts + 1,
       lease_until   = NULL,
       status        = CASE WHEN attempts + 1 >= max_attempts THEN 'failed' ELSE 'pending' END,
       result_status = CASE WHEN attempts + 1 >= max_attempts THEN NULL ELSE result_status END,
       error_message = CASE WHEN attempts + 1 >= max_attempts THEN 'attempts_exhausted' ELSE error_message END,
       completed_at  = CASE WHEN attempts + 1 >= max_attempts THEN now() ELSE completed_at END
 WHERE id = :'run_id'
   AND status = 'running'
   AND lease_until IS NOT NULL
   AND lease_until <= now();

COMMIT;
```

`WHERE` 里的三个条件就是守卫：状态变了 / 没租约 / 没过期，一行都不会动（`UPDATE 0`）。

### 6.4 给一条 `cancelling` 收尾（与 `reapCancellingSQL` 同形）

```sql
BEGIN;

UPDATE run.backtest_jobs
   SET status = 'cancelled', result_status = NULL, cancel_requested = TRUE,
       completed_at = now(), lease_until = NULL
 WHERE id = :'run_id'
   AND status = 'cancelling'
   AND lease_until IS NOT NULL
   AND lease_until <= now();

COMMIT;
```

同样**不计 `attempts`**。⚠️ 两条 SQL 都可能让子 run 落终态（`failed` / `cancelled`）而不触发父汇总重算，见 §7.3。

### 6.5 校验

```sql
SELECT id, status, result_status, attempts, error_message, completed_at, lease_until
  FROM run.backtest_jobs
 WHERE id = :'run_id';
```

预期：`pending` 且 `lease_until IS NULL`（可被重新领取），或 `failed/attempts_exhausted`，或 `cancelled`。

> 本节 SQL **已于 2026-11-07 在真库执行**：本机 PG 16.2 发行版只带 `initdb` / `pg_ctl` / `postgres`（**无 `psql` 客户端**），因此用 `postgres --single -D <data> tellurion_orchestration_test` 执行同形 SQL（单用户模式逐行执行，**语句必须写成单行**；`:'run_id'` 换成字面量）。实测：§6.2 观察命中 `ccb5e769… status=running attempts=1 worker_id=w-sql overdue=00:00:07.86214`；§6.3 释放 → 后镜像 `status=pending attempts=2`；同一 SQL 在行已非 `running`（被 reaper 回收后）再跑为 `UPDATE 0`，守卫成立；`SQL_EXIT=0`。恢复服务后 `GET /api/runs/jobs/ccb5e769…` → `200 status=pending`。

## 7. 父 / 子汇总核对

### 7.1 计数口径（先分清 batch 与 walkforward）

- **batch**：父 `total` = 子 run 数。父计数 = 子 run 两轴折算：`completed`（`status=completed AND result_status=completed`）/ `invalid`（`result_status=invalid`）/ `failed`（`status=failed` 或 `status=cancelled`，含无 K9 结论的失败）。
- **walkforward**：父 `total` = **窗口数**，子 run = `2 × total`（train / test 两腿）。两腿合并成窗口结论后才计入父计数（任一腿 failed → 窗口 failed；否则任一腿 invalid → 窗口 invalid；两腿都 completed → completed；缺腿 → pending）。所以**不能**拿子 run 行数直接和父 `total` 比。

### 7.2 batch 核对 SQL

```sql
SELECT p.parent_job_id, p.kind, p.status, p.total,
       p.completed, p.invalid, p.failed,
       count(c.id) AS children,
       count(*) FILTER (WHERE c.result_status = 'completed')              AS kids_completed,
       count(*) FILTER (WHERE c.result_status = 'invalid')                AS kids_invalid,
       count(*) FILTER (WHERE c.status IN ('failed','cancelled'))         AS kids_failed,
       count(*) FILTER (WHERE c.status IN ('pending','running','cancelling')) AS kids_open
  FROM run.parent_jobs p
  LEFT JOIN run.backtest_jobs c ON c.parent_job_id = p.parent_job_id
 WHERE p.parent_job_id = :'parent_job_id'
 GROUP BY p.parent_job_id, p.kind, p.status, p.total, p.completed, p.invalid, p.failed;
```

判读：

- `kids_open > 0` 且父 `status='running'` → 正常在跑。
- `kids_open = 0` 但父仍 `running` → **父汇总没重算**（见 §7.3），不是子 run 真没跑完。
- `children != total` → 子 run 入队不齐（父作业提交部分失败）；用同一幂等键重放父请求补齐。
- `kids_failed` 与父 `failed` 对不上 → 确认是否把 `cancelled` 记漏（`taskOutcomeOf` 会把它折成 failed）。

### 7.3 已知缺口：回收 / 人工 SQL 落终态不会触发父汇总重算

`parents.SyncSummary` 目前**只在子 run 上报路径**被调用（`internal/httpapi/workers.go`）。因此：

- reaper 把子 run 回收成 `failed`（`attempts_exhausted`）或 `cancelled` 后，父记录会停在 `running`，直到**同父下另一条子 run 上报**时才被一起重算。
- 人工 SQL（§6）同理。

现状处置：等同一父下任一条子 run 上报；或对该父下一条子 run 做一次幂等重放上报（终态 + 同载荷是 no-op，但会触发汇总）。这是 X5b 的遗留缺口，已在 `docs/TASKS.md` 如实登记；不假装它不存在。

## 8. 集成腿怎么跑

```bash
# 队列语义套件 + 迁移重放（同一把跨进程 advisory lock，别和其他仓共用同库并行跑）
ORCHESTRATION_TEST_DSN='postgres://postgres:postgres@127.0.0.1:5432/tellurion_orchestration_test?sslmode=disable' \
  go test ./internal/pgstore ./cmd/orchestrationservice -count=1
```

- 未设置 `ORCHESTRATION_TEST_DSN` 时这些腿 **skip**。skip 不是绿：它只证明「本机没跑」，不证明「接了库也对」。
- `TestTwoProcessesShareTheQueueAgainstPG` 是端到端判据：进程 A 提交、进程 B（独立连接池）领取并写终态、A 能看见、幂等键在库里不重复。
- 本机这条腿 **已执行（2026-11-07）**：`ORCHESTRATION_TEST_DSN='postgres://postgres@127.0.0.1:55432/tellurion_orchestration_test?sslmode=disable' go test ./internal/pgstore ./cmd/orchestrationservice -count=1` → 全 PASS，`internal/qtest` 的 PG 侧 45 条不再 skip（台账 `SVC-ORCH-G2` 施工记录同批取证）。Docker 引擎仍不可用，容器腿未执行。

## 9. 演练记录（SVC-ORCH-G4 ⑥）

### 9.1 死 worker 的活被回收且不丢（已执行）

```text
$ go test ./internal/queue -run TestDrillDeadWorkerRunIsReclaimedAndCompleted -count=1 -v
=== RUN   TestDrillDeadWorkerRunIsReclaimedAndCompleted
--- PASS: TestDrillDeadWorkerRunIsReclaimedAndCompleted (0.00s)
PASS
ok  	tellurion-orchestration/internal/queue	0.046s
```

断言链：入队 → `w-dead` 领取 → 时钟越过租约 → `ReapExpired` 回收 1 行（`pending` / `attempts=1` / 租约清空）→ **另一个 worker 领到同一条** → 写 `completed` → 全表仍 1 行。

### 9.2 回收与取消都计入父汇总（已执行）

```text
$ go test ./internal/parents -run TestDrillReclaimedAndCancelledChildrenCountAsFailed -count=1 -v
=== RUN   TestDrillReclaimedAndCancelledChildrenCountAsFailed
--- PASS: TestDrillReclaimedAndCancelledChildrenCountAsFailed (0.00s)
PASS
ok  	tellurion-orchestration/internal/parents	0.043s
```

断言链：4 条子 run 的 batch → 一条被 `w-dead` 领取后连续三轮「过期 → 回收」耗尽预算 → `failed/attempts_exhausted`；另一条 `pending → cancelled`；`SyncSummary` 后父计数 `total 4 / completed 0 / invalid 0 / failed 2`、状态仍 `running`（还剩两条），且子 run 行数仍 4（回收不新建行）。

破坏验证（真做真还原）：把 `parents.taskOutcomeOf` 里 `cancelled → failed` 的映射临时改成空分支 → 同一条用例红：

```text
g4_drill_test.go:95: parent counts = total 4 / completed 0 / invalid 0 / failed 1, want 4/0/0/2
FAIL	tellurion-orchestration/internal/parents	0.043s
```

逐字节还原后复绿（`go test ./internal/parents -count=1` → `ok`）。

### 9.3 未执行的演练腿

- **杀真 worker 进程**：worker 在 core（`tellurion:cmd/worker`），本仓无引擎；真实链路需要 core 侧配合。本次真库演练用 HTTP 停心跳模拟 worker 消失（§9.4），**不是**杀进程。
- **`docker compose -f deploy/compose.yaml up`**：Docker 引擎不可用。

以上两条**未执行**，不写成「通过」。

### 9.4 真库上的租约回收（已执行，2026-11-07）

环境：便携 PG 16.2（`127.0.0.1:55432`，库 `tellurion_orchestration_test`）+ 真 `cmd/orchestrationservice`（`-reap-interval 2s -heartbeat-timeout 3s -worker-lease 5s`，`127.0.0.1:18088`）。

```text
# 提交父作业（batch，2 个子 run）
POST /api/runs/parents → 202 parent_job_id=3866fdc02b693c0a9a6b006669f7900c total=2 counts.pending=2

# w-dead 领取后停心跳
claim(w-dead) → run_id=a45df712ba08e14b5499d50b4ec2b4ed attempts=0 lease_until=2026-11-07T07:44:39Z
12s 无心跳 → 该行 pending（started_at 保留、completed_at=null）；GET /api/runs/workers → w-dead=offline

# 另一个 worker 领到同一条（不丢、不新建行）
claim(w-alive) → run_id=a45df712… attempts=1
租约再过期 → attempts=2
预算耗尽 → status=failed error=attempts_exhausted completed_at=2026-11-07T07:45:42.7539Z
第二条子 run e60182f4… → 同样 failed/attempts_exhausted

# 父汇总（真库 SQL，§7.2 原文）
status=running total=2 completed=0 invalid=0 failed=0 children=2 kids_failed=2 kids_open=0
```

判读：子 run 终态与父汇总数字**不一致**——`kids_open=0` 而父仍 `running / failed=0`，正是 §7.3 登记的已知缺口（reaper 落终态不触发 `SyncSummary`）；本次真库演练把它复现并留档，按台账决定不在 G4 修。

`/readyz` 与 §6 SQL 的真库记录分别见 §2 与 §6 末注。
