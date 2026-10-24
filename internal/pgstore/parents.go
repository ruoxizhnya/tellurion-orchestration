package pgstore

// `run.parent_jobs` 的 PostgreSQL 实现（裁-13，X5b）。
//
// 与 queue.MemoryParents 跑同一套语义断言（internal/queue 的单测 + 本仓 PG 腿）：
// 幂等键 + plan_hash 判冲突、未知 id → ErrParentNotFound、汇总计数自洽。
// 身份锚点只认 plan_canonical（TEXT），不复算 JSONB。

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"tellurion-orchestration/internal/queue"
)

// parentEnqueueLockKey 是父记录提交路径的 advisory lock 键（"RUNP"）。
// 与 job 队列的 enqueueLockKey 分开：两张表的并发提交互不阻塞。
const parentEnqueueLockKey = int64(0x5255_4E50)

// parentColumns 是读写 ParentRecord 的列清单（顺序 = rowToParent 的 Scan 顺序）。
const parentColumns = `parent_job_id, idempotency_key, kind, plan_canonical, plan_hash, status,
	total, completed, invalid, failed, summary_json, created_at, completed_at`

// Parents 是 queue.ParentStore 的 PG 实现。
type Parents struct{ pool *pgxpool.Pool }

// 编译期断言：方法集少一个就是实现方全红。
var _ queue.ParentStore = (*Parents)(nil)

// Parents 返回 ParentStore 的 PG 实现。
func (s *Store) Parents() *Parents { return &Parents{pool: s.pool} }

func rowToParent(row pgx.Row) (*queue.ParentRecord, error) {
	var (
		rec                 queue.ParentRecord
		idemKey             sql.NullString
		kind, planHash      string
		status              string
		summary             []byte
		createdAt           sql.NullTime
		completedAt         sql.NullTime
		id, planCanonical   string
		total, comp, inv, f int
	)
	err := row.Scan(&id, &idemKey, &kind, &planCanonical, &planHash, &status,
		&total, &comp, &inv, &f, &summary, &createdAt, &completedAt)
	if err != nil {
		return nil, fmt.Errorf("pgstore: scan parent record: %w", err)
	}
	rec.ID = id
	rec.IdempotencyKey = idemKey.String
	rec.Kind = queue.ParentKind(kind)
	rec.PlanCanonical = planCanonical
	rec.PlanHash = strings.TrimRight(planHash, " ")
	rec.Status = queue.RunStatus(status)
	rec.Total = total
	rec.Completed = comp
	rec.Invalid = inv
	rec.Failed = f
	if summary != nil {
		rec.SummaryJSON = append([]byte(nil), summary...)
	}
	if createdAt.Valid {
		rec.CreatedAt = createdAt.Time.UTC()
	}
	if completedAt.Valid {
		rec.CompletedAt = completedAt.Time.UTC()
	}
	return &rec, nil
}

// EnqueueParent 幂等写入一条父记录（同键同 plan_hash → 原行、created=false）。
func (p *Parents) EnqueueParent(ctx context.Context, rec *queue.ParentRecord) (*queue.ParentRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	canonical, hash, err := queue.PrepareParent(rec)
	if err != nil {
		return nil, false, err
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

	var replayID string
	var replay bool
	err = inTx(ctx, p.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, parentEnqueueLockKey); err != nil {
			return fmt.Errorf("pgstore: parent enqueue lock: %w", err)
		}

		var storedHash string
		scanErr := tx.QueryRow(ctx,
			`SELECT parent_job_id, plan_hash FROM run.parent_jobs WHERE idempotency_key = $1`,
			rec.IdempotencyKey).Scan(&replayID, &storedHash)
		switch {
		case scanErr == nil:
			if strings.TrimRight(storedHash, " ") != hash {
				return fmt.Errorf("%w: key %q reused with a different plan", queue.ErrIdempotencyConflict, rec.IdempotencyKey)
			}
			replay = true
			return nil
		case errors.Is(scanErr, pgx.ErrNoRows):
			// 继续插入。
		default:
			return fmt.Errorf("pgstore: parent idempotency lookup: %w", scanErr)
		}

		_, err := tx.Exec(ctx, `
			INSERT INTO run.parent_jobs
				(parent_job_id, idempotency_key, kind, plan_canonical, plan_hash, status,
				 total, completed, invalid, failed, summary_json, created_at, completed_at)
			VALUES
				($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL, $11, NULL)`,
			id, rec.IdempotencyKey, string(rec.Kind), canonical, hash, string(rec.Status),
			rec.Total, rec.Completed, rec.Invalid, rec.Failed, createdAt)
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: duplicate parent id %q", queue.ErrInvalidInput, id)
		}
		if err != nil {
			return fmt.Errorf("pgstore: insert parent: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}

	if replay {
		out, err := p.GetParent(ctx, replayID)
		return out, false, err
	}
	out, err := p.GetParent(ctx, id)
	return out, true, err
}

// GetParent 未知 id → ErrParentNotFound。
func (p *Parents) GetParent(ctx context.Context, id string) (*queue.ParentRecord, error) {
	row := p.pool.QueryRow(ctx, `SELECT `+parentColumns+` FROM run.parent_jobs WHERE parent_job_id = $1`, id)
	rec, err := rowToParent(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", queue.ErrParentNotFound, id)
	}
	if err != nil {
		return nil, err
	}
	return rec, nil
}

// SaveParentSummary 写回三值计数 / 父状态 / 明细。
func (p *Parents) SaveParentSummary(ctx context.Context, id string, summary queue.ParentSummary) (*queue.ParentRecord, error) {
	if err := summary.Validate(); err != nil {
		return nil, err
	}
	completedAt := summary.CompletedAt
	if completedAt.IsZero() && (summary.Status == queue.RunCompleted || summary.Status == queue.RunFailed || summary.Status == queue.RunCancelled) {
		completedAt = timeNow()
	}
	tag, err := p.pool.Exec(ctx, `
		UPDATE run.parent_jobs
		SET status = $2, completed = $3, invalid = $4, failed = $5,
		    summary_json = CASE WHEN $6::text IS NULL THEN summary_json ELSE CAST($6::text AS jsonb) END,
		    completed_at = COALESCE($7, completed_at)
		WHERE parent_job_id = $1 AND total = $8`,
		id, string(summary.Status), summary.Completed, summary.Invalid, summary.Failed,
		nullableText(string(summary.SummaryJSON)), nullableTime(completedAt), summary.Total)
	if err != nil {
		return nil, fmt.Errorf("pgstore: update parent summary: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// 两种原因：id 不存在，或汇总的 total 与提交期冻结的计划规模不一致。
		if _, getErr := p.GetParent(ctx, id); getErr != nil {
			return nil, getErr
		}
		return nil, fmt.Errorf("%w: summary total %d does not match the planned total", queue.ErrInvalidInput, summary.Total)
	}
	return p.GetParent(ctx, id)
}
