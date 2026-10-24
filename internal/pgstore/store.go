// Package pgstore 是 internal/queue 两个 port 的 PostgreSQL 实现，也是本仓的
// 数据库装配入口（Open / Migrate）。
//
// 本包**不定义新的领域语义**：docs/ARCHITECTURE.md §9.8 的判据是「换 PG 实现时
// 行为不变」，所以每个方法都按 §9.2 冻结的错误语义写，并与 internal/queue.Memory
// 跑**同一套**语义套件（internal/qtest）。状态的判定逻辑刻意与内存实现逐行同形
// ——差异只应出现在「多进程原子性」这一层（SKIP LOCKED / 唯一索引 / 事务）。
//
// 第三方驱动只进本包（AGENTS.md §4）：internal/queue 保持零第三方依赖。
package pgstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"tellurion-orchestration/internal/queue"
)

// enqueueLockKey 是提交路径的 advisory lock 键（pg_advisory_xact_lock）。
//
// 为什么需要它：`Enqueue` 要做「按幂等键查 → 数 pending 深度 → 插一行」三件事，
// 内存实现靠一把互斥锁天然原子；PG 上三条语句之间没有锁，两个并发提交会各自看到
// 「还没满」从而双双越过 queue_full 阈值。锁键是任意常数但全仓唯一：advisory lock
// 是**实例级**命名空间，换键要先确认没有别的对象用同一个 int64。
const enqueueLockKey = int64(0x5255_4E51) // "RUNQ"

// Store 持有一个连接池；Queue / Registry 是它的两个 port 视图。
type Store struct {
	pool *pgxpool.Pool
}

// Open 建池并 ping。DSN 为空 fail-closed——不静默退回内存实现：
// 装配方以为自己在写库、实际在写进程内存，比启动失败严重得多。
func Open(ctx context.Context, dsn string) (*Store, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("pgstore: postgres dsn is required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgstore: parse dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgstore: new pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgstore: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// FromPool 用调用方持有的池构造 Store（连接池归装配方，与 kit/db 的取向一致）。
func FromPool(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Pool 返回底层池，只给装配与测试用；业务代码走 port。
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close 释放池。
func (s *Store) Close() { s.pool.Close() }

// Migrate 按序执行 queue.Migrations()：幂等、无版本表、只前滚
// （docs/db-schema.md §4 取向 ①）。服务启动时在监听之前调用。
//
// 失败即返回并带上序号：启动装配不许带着半套表开始领活。
func (s *Store) Migrate(ctx context.Context) error {
	stmts := queue.Migrations()
	for i, stmt := range stmts {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("pgstore: migration %d/%d: %w", i+1, len(stmts), err)
		}
	}
	return nil
}

// Options 是两个 port 实现共享的装配项。零值即默认，默认值的解析与内存实现共用
// queue.ResolveQueueDepth / ResolveMaxAttempts。
type Options struct {
	// MaxQueueDepth 是 pending 深度上限（<=0 = DefaultMaxQueueDepth）。
	MaxQueueDepth int
	// DefaultMaxAttempts 是未指定 max_attempts 时的取值（<=0 = DefaultMaxAttempts）。
	DefaultMaxAttempts int
}

// Queue 是 queue.JobQueue 的 PostgreSQL 实现。
//
// workers 为 nil 时不校验注册表（单机装配允许不建注册表）；非 nil 时**已注册且
// 非 active** 的 worker 被拒——与内存实现同一条口径。
type Queue struct {
	pool     *pgxpool.Pool
	maxDepth int
	maxAtt   int
	workers  queue.WorkerRegistry
}

// Registry 是 queue.WorkerRegistry 的 PostgreSQL 实现。
type Registry struct {
	pool *pgxpool.Pool
}

// 编译期断言：与 internal/queue/memory.go 里那两条对称——方法集是冻结面
// （docs/ARCHITECTURE.md §9.6），少一个方法就是实现方全红。
// KernelAwareJobQueue（§9.7 R9）也断在这里：两个实现都必须消费 Kernels，
// 否则「按能力过滤」只剩文档里的一句话。
var (
	_ queue.JobQueue            = (*Queue)(nil)
	_ queue.KernelAwareJobQueue = (*Queue)(nil)
	_ queue.WorkerRegistry      = (*Registry)(nil)
	_ queue.WorkerReaper        = (*Registry)(nil)
)

// Queue 返回 JobQueue 的 PG 实现。
func (s *Store) Queue(reg *Registry, opts Options) *Queue {
	var workers queue.WorkerRegistry
	if reg != nil {
		workers = reg
	}
	return &Queue{
		pool:     s.pool,
		maxDepth: queue.ResolveQueueDepth(opts.MaxQueueDepth),
		maxAtt:   opts.DefaultMaxAttempts,
		workers:  workers,
	}
}

// Registry 返回 WorkerRegistry 的 PG 实现。
func (s *Store) Registry() *Registry { return &Registry{pool: s.pool} }

// jobCols 是读写 RunRecord 的列名表。**两条 SQL 形状都从它派生**
// （裸列给 SELECT，带 `j.` 前缀给 UPDATE ... FROM —— 那里的裸 `id` 是歧义的：
// 外层表 j 与子查询 c 都有 id 列）。一张表两处写必然漂移。
var jobCols = []string{
	"id", "idempotency_key", "parent_job_id", "request_hash", "request_canonical", "status", "result_status",
	"priority", "attempts", "max_attempts", "worker_id", "lease_until", "cancel_requested",
	"error_message", "result_uri", "created_at", "started_at", "completed_at",
}

// columnsOf 生成 SELECT 列表；alias 非空时逐列加前缀。
//
// 顺序就是 rowToJob 的 Scan 顺序。两处之间的漂移由两条腿钉住：
// TestJobColumnsMatchTableShape（每个名字都必须在 run.backtest_jobs 上真实存在）
// 与 qtest 套件的逐字段回读断言（错位会立刻显形）。
func columnsOf(alias string) string {
	if alias != "" && !strings.HasSuffix(alias, ".") {
		alias += "."
	}
	parts := make([]string, 0, len(jobCols))
	for _, c := range jobCols {
		parts = append(parts, alias+c)
	}
	return strings.Join(parts, ", ")
}

var (
	jobColumns        = columnsOf("")
	jobColumnsAliased = columnsOf("j")
)

// RequestJSON 取 request_canonical 而不是 request_json：JSONB 会规范化数字文本
// （`1e-05` 读回成 `0.00001`），拿它复算 hash 会把同一个幂等键判成两次不同的请求。
// 见 internal/queue/ddl.go 的列注释与 hashanchor_test.go 的实测钉子。

// rowToJob 把一行扫成 RunRecord。
//
// 两条 PG 侧才需要的口径：
//   - NULL 列 → 零值（内存实现用空串/零时间表达同一件事，见 model.go 顶部注释）；
//   - TIMESTAMPTZ 统一取 UTC。PG 只到**微秒**，内存是纳秒，所以语义套件的断言只比
//     同一实现自己返回的时间，不跨实现比纳秒。
func rowToJob(row pgx.Row) (*queue.RunRecord, error) {
	var (
		rec                                queue.RunRecord
		idemKey, resultStatus              sql.NullString
		parentJobID                        sql.NullString
		workerID, errorMessage, resultURI  sql.NullString
		requestHash, status                string
		leaseUntil, startedAt, completedAt sql.NullTime
	)
	err := row.Scan(&rec.ID, &idemKey, &parentJobID, &requestHash, &rec.RequestJSON, &status, &resultStatus,
		&rec.Priority, &rec.Attempts, &rec.MaxAttempts, &workerID, &leaseUntil, &rec.CancelRequested,
		&errorMessage, &resultURI, &rec.CreatedAt, &startedAt, &completedAt)
	if err != nil {
		return nil, fmt.Errorf("pgstore: scan run record: %w", err)
	}
	rec.Status = queue.RunStatus(status)
	rec.IdempotencyKey = idemKey.String
	rec.ParentJobID = parentJobID.String
	// CHAR(64) 用空格补齐；64 位十六进制本来不含空格，补齐只可能来自存量脏数据。
	rec.RequestHash = strings.TrimRight(requestHash, " ")
	rec.ResultStatus = queue.ResultStatus(resultStatus.String)
	rec.WorkerID = workerID.String
	if leaseUntil.Valid {
		rec.LeaseUntil = leaseUntil.Time.UTC()
	}
	rec.ErrorMessage = errorMessage.String
	rec.ResultURI = resultURI.String
	rec.CreatedAt = rec.CreatedAt.UTC()
	if startedAt.Valid {
		rec.StartedAt = startedAt.Time.UTC()
	}
	if completedAt.Valid {
		rec.CompletedAt = completedAt.Time.UTC()
	}
	return &rec, nil
}

// nullableTime 把零值时间转成 SQL NULL。
func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC()
}

// nullableText 把空串转成 SQL NULL：两轴映射表要求「未产出结论 = NULL」，
// 内存实现用空串表达同一件事。
func nullableText(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func isUniqueViolation(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "23505"
}

// inTx 在一个事务里跑 fn：fn 出错即回滚。
func inTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgstore: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgstore: commit: %w", err)
	}
	return nil
}

// lockJob 在事务里锁住一行并返回它。未知 id → queue.ErrJobNotFound。
//
// 这里用 FOR UPDATE（**不**带 SKIP LOCKED）：终态写入必须串行——两个 worker 各写
// 一份终态正是 §9.2 用租约 + ErrLeaseLost fencing 要挡住的事。
func lockJob(ctx context.Context, tx pgx.Tx, runID string) (*queue.RunRecord, error) {
	row := tx.QueryRow(ctx, `SELECT `+jobColumns+` FROM run.backtest_jobs WHERE id = $1 FOR UPDATE`, runID)
	rec, err := rowToJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", queue.ErrJobNotFound, runID)
	}
	if err != nil {
		return nil, err
	}
	return rec, nil
}

// checkLease 与 internal/queue/memory.go 的 checkLeaseLocked 同形：owner + 有效期。
// at 由调用方传入，贯穿一次调用（不在判定中途反复取时钟）。
func checkLease(row *queue.RunRecord, workerID string, at time.Time) error {
	if workerID == "" || row.WorkerID != workerID {
		return fmt.Errorf("%w: %s is owned by %q, not %q", queue.ErrLeaseLost, row.ID, row.WorkerID, workerID)
	}
	if row.LeaseUntil.IsZero() || !at.Before(row.LeaseUntil) {
		return fmt.Errorf("%w: lease of %s expired", queue.ErrLeaseLost, row.ID)
	}
	return nil
}
