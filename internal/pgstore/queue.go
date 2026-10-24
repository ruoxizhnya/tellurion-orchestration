package pgstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"tellurion-orchestration/internal/queue"
)

// timeNow 是本包唯一的时间来源：统一 UTC，与内存实现的 `m.now().UTC()` 同口径。
//
// 时钟由**应用侧**提供、不用 DB 的 now()：ReapExpired 的签名收 `now` 参数（§9.2
// 冻结），内存实现也用自己的时钟。混用两种时钟会让「租约是否过期」在同一次调用里
// 有两个答案。代价如实登记在 docs/ARCHITECTURE.md §9.8：跨 worker 进程的时钟漂移
// 会直接移动租约边界，部署必须让 worker 与 PG 同时钟源（NTP）。
func timeNow() time.Time { return time.Now().UTC() }

// Enqueue 见 queue.JobQueue 的契约注释。校验分支与内存实现逐条同形：
// 空/超长幂等键、自带 hash 不符、新行非 pending、队列满、重复 run id。
//
// 「查幂等键 → 数深度 → 插入」三件事在 pg_advisory_xact_lock 里做（见 enqueueLockKey）。
func (q *Queue) Enqueue(ctx context.Context, rec *queue.RunRecord) (*queue.RunRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if rec == nil {
		return nil, false, fmt.Errorf("%w: nil record", queue.ErrInvalidInput)
	}
	if rec.IdempotencyKey == "" || len(rec.IdempotencyKey) > queue.MaxIdempotencyKeyLen {
		return nil, false, fmt.Errorf("%w: idempotency_key must be 1..%d bytes", queue.ErrInvalidInput, queue.MaxIdempotencyKeyLen)
	}
	canonical, err := queue.Canonicalize(rec.RequestJSON)
	if err != nil {
		return nil, false, err
	}
	sum, err := queue.RequestHash(canonical)
	if err != nil {
		return nil, false, err
	}
	if rec.RequestHash != "" && rec.RequestHash != sum {
		return nil, false, fmt.Errorf("%w: request_hash does not match request_json", queue.ErrInvalidInput)
	}
	if rec.Status != "" && rec.Status != queue.RunPending {
		return nil, false, fmt.Errorf("%w: new job must start as pending, got %q", queue.ErrInvalidInput, rec.Status)
	}

	id := rec.ID
	if id == "" {
		id = queue.NewRunID()
	}
	createdAt := rec.CreatedAt
	if createdAt.IsZero() {
		createdAt = timeNow()
	} else {
		createdAt = createdAt.UTC()
	}
	maxAttempts := queue.ResolveMaxAttempts(q.maxAtt, rec.MaxAttempts)

	var replayID string
	var replay bool
	err = inTx(ctx, q.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, enqueueLockKey); err != nil {
			return fmt.Errorf("pgstore: enqueue lock: %w", err)
		}

		var storedHash string
		scanErr := tx.QueryRow(ctx,
			`SELECT id, request_hash FROM run.backtest_jobs WHERE idempotency_key = $1`,
			rec.IdempotencyKey).Scan(&replayID, &storedHash)
		switch {
		case scanErr == nil:
			if strings.TrimSpace(storedHash) != sum {
				return fmt.Errorf("%w: key %q reused with a different request", queue.ErrIdempotencyConflict, rec.IdempotencyKey)
			}
			replay = true
			return nil
		case errors.Is(scanErr, pgx.ErrNoRows):
			// 没有同键行：继续做深度检查与插入。
		default:
			return fmt.Errorf("pgstore: idempotency lookup: %w", scanErr)
		}

		var pending int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM run.backtest_jobs WHERE status = 'pending'`).Scan(&pending); err != nil {
			return fmt.Errorf("pgstore: pending depth: %w", err)
		}
		if pending >= q.maxDepth {
			return fmt.Errorf("%w: pending depth reached %d", queue.ErrQueueFull, q.maxDepth)
		}

		// $4 两处都显式 ::text：不写的话 PG 会按 jsonb 列的上下文把参数定型，
		// request_canonical 里存进去的就是 **jsonb 的文本表示**（`{"a": 1}` 多一个
		// 空格），锚点即刻失效。真库实测见 hashanchor_test.go。
		_, err := tx.Exec(ctx, `
			INSERT INTO run.backtest_jobs
				(id, idempotency_key, parent_job_id, request_hash, request_json, request_canonical,
				 status, result_status, priority, attempts, max_attempts,
				 worker_id, lease_until, cancel_requested, error_message, result_uri,
				 created_at, started_at, completed_at)
			VALUES
				($1, $2, $8, $3, CAST($4::text AS jsonb), $4::text,
				 'pending', NULL, $5, 0, $6,
				 NULL, NULL, FALSE, NULL, NULL,
				 $7, NULL, NULL)`,
			id, rec.IdempotencyKey, sum, string(canonical), rec.Priority, maxAttempts, createdAt,
			nullableText(rec.ParentJobID))
		if isUniqueViolation(err) {
			// 幂等键的重复已在锁内查过，这里唯一能撞的就是主键：与内存实现同一条错误。
			return fmt.Errorf("%w: duplicate run id %q", queue.ErrInvalidInput, id)
		}
		if err != nil {
			return fmt.Errorf("pgstore: insert run: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}

	if replay {
		job, err := q.Get(ctx, replayID)
		return job, false, err
	}
	job, err := q.Get(ctx, id)
	if err != nil {
		return nil, true, err
	}
	return job, true, nil
}

// claimSQL 原子领取一条 pending 且租约已释放/已过期的行。
//
// 顺序 priority DESC → created_at ASC → id ASC 与内存实现的 claimOrder 逐键同形；
// 第三键 id 是刻意的：同一 priority 同一 created_at 时，两个实现必须领到同一条。
// FOR UPDATE SKIP LOCKED = 多 worker 并发领取的判据：别人锁着的行直接跳过，
// 不排队也不互斥（排队会让两个 worker 各领一条变成串行等待）。
//
// $4 是 worker 声明的内核集合（R9）：传 NULL = 不过滤，所以冻结的 `Claim` 与可选的
// `ClaimByKernel` 共用这一条语句、共用同一个顺序判据——两份 SQL 迟早会漂。
//
// 行的生效内核：**只有 JSON 字符串才算内核**，缺失 / null / 非字符串 / 空串 / 纯空白
// 一律回默认（`queue.DefaultKernel`）。`jsonb_typeof(...) = 'string'` 那一层是必须的：
// `->>` 对数字给的是它的 JSON 文本（`123`），而 Go 侧解不进 string 会按默认处理——
// 少了这层 CASE，同一行在两个实现里的可领取性就不同。`btrim` 对 Go 的 TrimSpace。
// claimSQL 是 var 而不是 const：RETURNING 列表由 jobCols 派生（见 store.go）。
var claimSQL = `
	UPDATE run.backtest_jobs AS j
		SET status = 'running', worker_id = $1, lease_until = $2,
		    started_at = COALESCE(j.started_at, $3)
		FROM (
			SELECT id FROM run.backtest_jobs
			WHERE status = 'pending' AND (lease_until IS NULL OR lease_until < $3)
			  AND ($4::text[] IS NULL
			       OR COALESCE(NULLIF(btrim(CASE WHEN jsonb_typeof(request_json -> 'kernel') = 'string'
			                                     THEN request_json ->> 'kernel' END), ''),
			                   '` + queue.DefaultKernel + `') = ANY ($4))
			ORDER BY priority DESC, created_at ASC, id ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		) AS c
		WHERE j.id = c.id
		RETURNING ` + jobColumnsAliased

// Claim 见 JobQueue 的契约注释。无活 → (nil, nil)。
//
// drain 校验口径与内存实现一致：**已注册且非 active** 才拒；未注册的 worker 不拒
// （单机装配可以完全不建注册表）。
//
// 本方法**不按内核过滤**（§9.2 冻结面）；要过滤走 ClaimByKernel。
func (q *Queue) Claim(ctx context.Context, workerID string, lease time.Duration) (*queue.RunRecord, error) {
	return q.claim(ctx, workerID, lease, false)
}

// ClaimByKernel 见 KernelAwareJobQueue 的契约注释（§9.7 R9 裁决的落点）：
// 能力取自 worker **在注册表里声明的** kernels，不是调用方现传的列表。
func (q *Queue) ClaimByKernel(ctx context.Context, workerID string, lease time.Duration) (*queue.RunRecord, error) {
	return q.claim(ctx, workerID, lease, true)
}

func (q *Queue) claim(ctx context.Context, workerID string, lease time.Duration, byKernel bool) (*queue.RunRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if workerID == "" || lease <= 0 {
		return nil, fmt.Errorf("%w: worker_id and positive lease are required", queue.ErrInvalidInput)
	}
	var declared []string
	if q.workers != nil {
		// 一次查回 status + kernels：kernels 无论如何都读，避免「按不按下」分成
		// 两条查询路径（drain 判据只有一条，两个入口必须同形）。
		var status string
		err := q.pool.QueryRow(ctx,
			`SELECT status, kernels FROM run.worker_registry WHERE worker_id = $1`, workerID).
			Scan(&status, &declared)
		switch {
		case err == nil:
			if queue.WorkerStatus(status) != queue.WorkerActive {
				return nil, fmt.Errorf("%w: %s is %s", queue.ErrWorkerDraining, workerID, status)
			}
			if !byKernel {
				declared = nil
			}
		case errors.Is(err, pgx.ErrNoRows):
			// 未注册：不拒、不过滤（与 Memory.Claim 同口径）。
			declared = nil
		default:
			return nil, fmt.Errorf("pgstore: worker registry lookup: %w", err)
		}
	}
	if len(declared) == 0 {
		// 空声明 = 不过滤。DDL 的 `kernels TEXT[] DEFAULT '{}'` 给的是空数组而不是
		// NULL，`= ANY('{}')` 恒假会把「没声明」读成「什么都不能干」。
		declared = nil
	}

	at := timeNow()
	row := q.pool.QueryRow(ctx, claimSQL, workerID, at.Add(lease), at, declared)
	rec, err := rowToJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return rec, nil
}

// RenewLease 见 JobQueue 的契约注释：只允许当前 owner 在租约有效期内续租。
func (q *Queue) RenewLease(ctx context.Context, runID, workerID string, lease time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if lease <= 0 {
		return fmt.Errorf("%w: positive lease is required", queue.ErrInvalidInput)
	}
	return inTx(ctx, q.pool, func(tx pgx.Tx) error {
		row, err := lockJob(ctx, tx, runID)
		if err != nil {
			// §9.2 的 RenewLease 一格把「行不存在」也归进 ErrLeaseLost：
			// worker 拿到 ErrJobNotFound 会以为「这行被删了」继续跑，
			// 拿到 ErrLeaseLost 才会立即停手——两个实现都必须按后者报。
			if errors.Is(err, queue.ErrJobNotFound) {
				return fmt.Errorf("%w: %s is not renewable", queue.ErrLeaseLost, runID)
			}
			return err
		}
		if row.Terminal() {
			return fmt.Errorf("%w: %s is not renewable", queue.ErrLeaseLost, runID)
		}
		at := timeNow()
		if err := checkLease(row, workerID, at); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE run.backtest_jobs SET lease_until = $2 WHERE id = $1`, runID, at.Add(lease))
		return err
	})
}

// Complete 见 JobQueue 的契约注释。
//
// `cancelling` 行不接受 Complete（内存实现同取舍：取消一经请求结果即作废，
// worker 只能 AckCancel）。
func (q *Queue) Complete(ctx context.Context, runID, workerID, resultStatus, resultURI string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rs := queue.ResultStatus(resultStatus)
	if rs != queue.ResultCompleted && rs != queue.ResultInvalid {
		return fmt.Errorf("%w: result_status must be completed or invalid, got %q", queue.ErrInvalidInput, resultStatus)
	}
	if resultURI == "" {
		return fmt.Errorf("%w: result_uri is required (result content is not stored in `run`)", queue.ErrInvalidInput)
	}
	return inTx(ctx, q.pool, func(tx pgx.Tx) error {
		row, err := lockJob(ctx, tx, runID)
		if err != nil {
			return err
		}
		if row.Status == queue.RunCompleted {
			if row.ResultStatus == rs && row.ResultURI == resultURI {
				return nil
			}
			return fmt.Errorf("%w: %s already completed with a different payload", queue.ErrInvalidTransition, runID)
		}
		if row.Status != queue.RunRunning {
			return fmt.Errorf("%w: %s is %s", queue.ErrInvalidTransition, runID, row.Status)
		}
		at := timeNow()
		if err := checkLease(row, workerID, at); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			UPDATE run.backtest_jobs
			SET status = 'completed', result_status = $2, result_uri = $3,
			    completed_at = $4, lease_until = NULL
			WHERE id = $1`, runID, string(rs), resultURI, at)
		return err
	})
}

// Fail 见 JobQueue 的契约注释：resultStatus 只接受 "" / failed。
func (q *Queue) Fail(ctx context.Context, runID, workerID, errMsg, resultStatus string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rs := queue.ResultStatus(resultStatus)
	if rs != "" && rs != queue.ResultFailed {
		return fmt.Errorf("%w: result_status must be empty or failed, got %q", queue.ErrInvalidInput, resultStatus)
	}
	if errMsg == "" {
		return fmt.Errorf("%w: error_message is required for a failed job", queue.ErrInvalidInput)
	}
	return inTx(ctx, q.pool, func(tx pgx.Tx) error {
		row, err := lockJob(ctx, tx, runID)
		if err != nil {
			return err
		}
		if row.Status == queue.RunFailed {
			if row.ResultStatus == rs && row.ErrorMessage == errMsg {
				return nil
			}
			return fmt.Errorf("%w: %s already failed with a different payload", queue.ErrInvalidTransition, runID)
		}
		if row.Status != queue.RunRunning {
			return fmt.Errorf("%w: %s is %s", queue.ErrInvalidTransition, runID, row.Status)
		}
		at := timeNow()
		if err := checkLease(row, workerID, at); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			UPDATE run.backtest_jobs
			SET status = 'failed', result_status = $2, error_message = $3,
			    completed_at = $4, lease_until = NULL
			WHERE id = $1`, runID, nullableText(resultStatus), errMsg, at)
		return err
	})
}

// RequestCancel 见 JobQueue 的契约注释：pending 直接落 cancelled（保留行——删行会
// 同时删掉幂等键与审计），running 只写 cancel_requested 并转 cancelling。
func (q *Queue) RequestCancel(ctx context.Context, runID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return inTx(ctx, q.pool, func(tx pgx.Tx) error {
		row, err := lockJob(ctx, tx, runID)
		if err != nil {
			return err
		}
		at := timeNow()
		switch row.Status {
		case queue.RunPending:
			_, err = tx.Exec(ctx, `
				UPDATE run.backtest_jobs
				SET status = 'cancelled', cancel_requested = TRUE, completed_at = $2, lease_until = NULL
				WHERE id = $1`, runID, at)
			return err
		case queue.RunRunning:
			_, err = tx.Exec(ctx, `
				UPDATE run.backtest_jobs
				SET status = 'cancelling', cancel_requested = TRUE
				WHERE id = $1`, runID)
			return err
		case queue.RunCancelling:
			_, err = tx.Exec(ctx, `UPDATE run.backtest_jobs SET cancel_requested = TRUE WHERE id = $1`, runID)
			return err
		default:
			// completed / failed / cancelled：取消是幂等空操作。
			return nil
		}
	})
}

// AckCancel 见 JobQueue 的契约注释。`pending` 行走租约校验报 ErrLeaseLost
// （§9.2 冻结表只列了两种出口，与内存实现一样不擅自补第三种）。
func (q *Queue) AckCancel(ctx context.Context, runID, workerID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return inTx(ctx, q.pool, func(tx pgx.Tx) error {
		row, err := lockJob(ctx, tx, runID)
		if err != nil {
			return err
		}
		if row.Terminal() {
			return fmt.Errorf("%w: %s is already %s", queue.ErrInvalidTransition, runID, row.Status)
		}
		at := timeNow()
		if err := checkLease(row, workerID, at); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			UPDATE run.backtest_jobs
			SET status = 'cancelled', result_status = NULL, cancel_requested = TRUE,
			    completed_at = $2, lease_until = NULL
			WHERE id = $1`, runID, at)
		return err
	})
}

// Get 见 JobQueue 的契约注释。
func (q *Queue) Get(ctx context.Context, runID string) (*queue.RunRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	row := q.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM run.backtest_jobs WHERE id = $1`, runID)
	rec, err := rowToJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", queue.ErrJobNotFound, runID)
	}
	if err != nil {
		return nil, err
	}
	return rec, nil
}

// List 见 JobQueue 的契约注释。排序 created_at 降序 → id 升序；`Before` 严格小于；
// 空结果返回空切片（不是 nil，保证 JSON 稳定）。limit 钳制与内存实现共用常量。
func (q *Queue) List(ctx context.Context, filter queue.ListFilter) ([]*queue.RunRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if filter.Status != "" && !queue.ValidRunStatus(filter.Status) {
		return nil, fmt.Errorf("%w: unknown status %q", queue.ErrInvalidInput, filter.Status)
	}
	if filter.ResultStatus != "" && !queue.ValidResultStatus(filter.ResultStatus) {
		return nil, fmt.Errorf("%w: unknown result_status %q", queue.ErrInvalidInput, filter.ResultStatus)
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = queue.DefaultListLimit
	}
	if limit > queue.MaxListLimit {
		limit = queue.MaxListLimit
	}

	rows, err := q.pool.Query(ctx, `
		SELECT `+jobColumns+` FROM run.backtest_jobs
		WHERE ($1 = '' OR status = $1)
		  AND ($2 = '' OR result_status = $2)
		  AND ($3::timestamptz IS NULL OR created_at < $3)
		ORDER BY created_at DESC, id ASC
		LIMIT $4`,
		string(filter.Status), string(filter.ResultStatus), nullableTime(filter.Before), limit)
	if err != nil {
		return nil, fmt.Errorf("pgstore: list runs: %w", err)
	}
	defer rows.Close()

	out := make([]*queue.RunRecord, 0)
	for rows.Next() {
		rec, err := rowToJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgstore: list runs rows: %w", err)
	}
	return out, nil
}

// reapSQL 把租约已到期（含等于 now）的 running 行收回：attempts+1；
// 超过 max_attempts → failed + attempts_exhausted。
//
// 与内存实现的三条同形口径：
//   - 判定是 `lease_until <= $1`（内存用 `!at.Before(lease)`，含等于）；
//   - `cancelling` 不在这条语句里，走 reapCancellingSQL（R8 于 2026-10-23 裁决后
//     分开的第二条，见下）；
//   - worker_id / started_at 保留（§9.8 取舍 5：留上次 owner 便于审计）。
//
// SKIP LOCKED 让多个 reaper 并发安全：别人正在写的行这次不碰，下次再来。
const reapSQL = `
	WITH c AS (
		SELECT id, attempts, max_attempts FROM run.backtest_jobs
		WHERE status = 'running' AND lease_until IS NOT NULL AND lease_until <= $1
		FOR UPDATE SKIP LOCKED
	)
	UPDATE run.backtest_jobs AS j
		SET attempts = c.attempts + 1,
		    lease_until = NULL,
		    status = CASE WHEN c.attempts + 1 >= c.max_attempts THEN 'failed' ELSE 'pending' END,
		    result_status = CASE WHEN c.attempts + 1 >= c.max_attempts THEN NULL ELSE j.result_status END,
		    error_message = CASE WHEN c.attempts + 1 >= c.max_attempts THEN $2 ELSE j.error_message END,
		    completed_at = CASE WHEN c.attempts + 1 >= c.max_attempts THEN $1 ELSE j.completed_at END
		FROM c
		WHERE j.id = c.id`

// reapCancellingSQL 是 R8 的落点（§9.7，2026-10-23 裁决「同批落 cancelled」）：
// owner 在 running 上被请求取消后消失，`AckCancel` 永远等不到，这行会停在中间态。
// 两轴映射表里 `cancelling` 的唯一出口是 `cancelled`，所以回收替它收尾。
//
// 刻意**不**动 attempts：它不是重试，是给已作废的结果收尾——计入重试预算会把
// 一次取消记成一次失败尝试（内存实现同口径，见 queue.Memory.ReapExpired）。
// 拆成第二条语句而不是往 reapSQL 里塞 CASE：那条已测过的 running 语句保持原样，
// R8 这条自成一格，读得出「两类迁移的判据不同」。
const reapCancellingSQL = `
	WITH c AS (
		SELECT id FROM run.backtest_jobs
		WHERE status = 'cancelling' AND lease_until IS NOT NULL AND lease_until <= $1
		FOR UPDATE SKIP LOCKED
	)
	UPDATE run.backtest_jobs AS j
		SET status = 'cancelled', result_status = NULL, cancel_requested = TRUE,
		    completed_at = $1, lease_until = NULL
		FROM c
		WHERE j.id = c.id`

// ReapExpired 见 JobQueue 的契约注释，返回本次迁移的行数（running 回收 +
// cancelling 收尾两类之和）。
//
// 两条语句在**同一个事务**里：内存实现是在一把锁下改完两类行，要么都动要么都不动。
// 分成两次 autocommit 就会出现「running 已回收、cancelling 那条失败」的半迁移态，
// 而调用方只看到一个 error——重跑虽然能收尾（两条都是一方向、幂等的迁移），
// 但返回值已经是错的了。事务里回滚 ⇒ 返回 0 + error，与「一行没动」同真。
func (q *Queue) ReapExpired(ctx context.Context, now time.Time) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	at := now
	if at.IsZero() {
		at = timeNow()
	}
	at = at.UTC()

	reaped := 0
	err := inTx(ctx, q.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, reapSQL, at, queue.AttemptsExhaustedMessage)
		if err != nil {
			return fmt.Errorf("pgstore: reap expired: %w", err)
		}
		reaped += int(tag.RowsAffected())
		tag, err = tx.Exec(ctx, reapCancellingSQL, at)
		if err != nil {
			return fmt.Errorf("pgstore: reap orphaned cancelling: %w", err)
		}
		reaped += int(tag.RowsAffected())
		return nil
	})
	if err != nil {
		return 0, err
	}
	return reaped, nil
}
