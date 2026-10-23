package queue

// `run` schema 的建表语句（本仓是这组表的唯一 owner，见 docs/db-schema.md）。
//
// 执行机制取向 ①：Go 数组 + 启动时逐条执行，幂等、无版本表、无 down（与 core
// 一致）。因此这里**不放** migrations/*.sql 可执行件：DDL 的唯一真相是本切片。
//
// 形状对表 docs/ARCHITECTURE.md §9.5；列口径对表 docs/db-schema.md §5。
// 改表 = 先改文档（结构级变更还要 ADR），再改这里。

// migrations 只允许追加；已落库的语句不得改写（只前滚）。
var migrations = []string{
	`CREATE SCHEMA IF NOT EXISTS run`,

	// run.backtest_jobs：任务队列（两轴状态：status 生命周期 / result_status K9）。
	`CREATE TABLE IF NOT EXISTS run.backtest_jobs (
		id                TEXT PRIMARY KEY,
		idempotency_key   TEXT,
		request_hash      CHAR(64) NOT NULL,
		request_json      JSONB NOT NULL,
		status            TEXT NOT NULL DEFAULT 'pending'
		                  CHECK (status IN ('pending','running','cancelling','completed','failed','cancelled')),
		result_status     TEXT
		                  CHECK (result_status IS NULL OR result_status IN ('completed','invalid','failed')),
		priority          INT NOT NULL DEFAULT 0,
		attempts          INT NOT NULL DEFAULT 0,
		max_attempts      INT NOT NULL DEFAULT 3,
		worker_id         TEXT,
		lease_until       TIMESTAMPTZ,
		cancel_requested  BOOLEAN NOT NULL DEFAULT FALSE,
		error_message     TEXT,
		result_uri        TEXT,
		created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
		started_at        TIMESTAMPTZ,
		completed_at      TIMESTAMPTZ
	)`,
	// 幂等键唯一；DB 允许 NULL 只为迁移存量行（API 层保证必填）。
	`CREATE UNIQUE INDEX IF NOT EXISTS backtest_jobs_idempotency_key_uidx
		ON run.backtest_jobs (idempotency_key) WHERE idempotency_key IS NOT NULL`,
	// 领取顺序：priority 降序 → created_at 升序（PG 实现按此索引 SKIP LOCKED）。
	`CREATE INDEX IF NOT EXISTS backtest_jobs_claim_idx
		ON run.backtest_jobs (status, priority DESC, created_at ASC) WHERE status = 'pending'`,
	`CREATE INDEX IF NOT EXISTS backtest_jobs_worker_idx ON run.backtest_jobs (worker_id, status)`,
	`CREATE INDEX IF NOT EXISTS backtest_jobs_created_idx ON run.backtest_jobs (created_at DESC)`,
	`CREATE INDEX IF NOT EXISTS backtest_jobs_result_status_idx ON run.backtest_jobs (result_status)`,

	// run.run_artifacts：三 hash + artifact 指针。🔒 冻结最小集（增列走评审）。
	// 不存 status：结论只从 backtest_jobs 取（单一同源）。
	`CREATE TABLE IF NOT EXISTS run.run_artifacts (
		run_id                TEXT PRIMARY KEY REFERENCES run.backtest_jobs (id),
		strategy_ref          TEXT NOT NULL,
		dataset_snapshot_hash TEXT NOT NULL,
		risk_rules_hash       TEXT NOT NULL,
		artifact_uri          TEXT,
		artifact_hash         TEXT,
		version_hash          TEXT,
		run_spec_hash         TEXT NOT NULL,
		params_hash           TEXT NOT NULL,
		params_json           JSONB NOT NULL DEFAULT '{}'::jsonb,
		seed                  BIGINT,
		kernel                TEXT NOT NULL,
		engine_version        TEXT NOT NULL,
		fill_timing           TEXT NOT NULL,
		created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
	)`,

	// run.batch_runs：批量批次 + 三值计数 + 排序汇总（不复制子结果内容）。
	`CREATE TABLE IF NOT EXISTS run.batch_runs (
		batch_id          TEXT PRIMARY KEY,
		idempotency_key   TEXT,
		request_json      JSONB NOT NULL,
		status            TEXT NOT NULL DEFAULT 'pending'
		                  CHECK (status IN ('pending','running','cancelling','completed','failed','cancelled')),
		total             INT NOT NULL DEFAULT 0,
		completed         INT NOT NULL DEFAULT 0,
		invalid           INT NOT NULL DEFAULT 0,
		failed            INT NOT NULL DEFAULT 0,
		summary_json      JSONB,
		created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
		completed_at      TIMESTAMPTZ
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS batch_runs_idempotency_key_uidx
		ON run.batch_runs (idempotency_key) WHERE idempotency_key IS NOT NULL`,

	// run.walk_forward_reports：窗口配置 + 逐窗 run 指针 + 三值计数。
	`CREATE TABLE IF NOT EXISTS run.walk_forward_reports (
		wf_id             TEXT PRIMARY KEY,
		idempotency_key   TEXT,
		request_json      JSONB NOT NULL,
		window_config     JSONB NOT NULL,
		status            TEXT NOT NULL DEFAULT 'pending'
		                  CHECK (status IN ('pending','running','cancelling','completed','failed','cancelled')),
		total             INT NOT NULL DEFAULT 0,
		completed         INT NOT NULL DEFAULT 0,
		invalid           INT NOT NULL DEFAULT 0,
		failed_windows    INT NOT NULL DEFAULT 0,
		report_uri        TEXT,
		summary_json      JSONB,
		created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
		completed_at      TIMESTAMPTZ
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS walk_forward_reports_idempotency_key_uidx
		ON run.walk_forward_reports (idempotency_key) WHERE idempotency_key IS NOT NULL`,

	// run.worker_registry：心跳超时 → offline（不删行）。
	`CREATE TABLE IF NOT EXISTS run.worker_registry (
		worker_id          TEXT PRIMARY KEY,
		kernels            TEXT[] NOT NULL DEFAULT '{}',
		status             TEXT NOT NULL DEFAULT 'active'
		                   CHECK (status IN ('active','draining','offline')),
		last_heartbeat_at  TIMESTAMPTZ,
		drain_requested_at TIMESTAMPTZ,
		started_at         TIMESTAMPTZ,
		meta               JSONB
	)`,
}

// Migrations 返回 DDL 的副本：调用方只能按序执行，不能改切片的元素。
func Migrations() []string {
	out := make([]string, len(migrations))
	copy(out, migrations)
	return out
}
