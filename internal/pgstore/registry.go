package pgstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"tellurion-orchestration/internal/queue"
)

// registerSQL 是「重复注册 = 幂等覆盖」的一条语句。
//
// 三个 COALESCE 分别复刻内存实现 MemoryRegistry.Register 的三条口径：
//   - status / drain_requested_at：**永远保留**库里已有的——已 drain 的 worker
//     不能靠重注册洗回 active（否则 drain 可被绕过）；
//   - started_at：调用方给了就用调用方的，否则保留原值，否则取 now
//     （它记「这个 worker 进程起于何时」，重注册不是新进程）；
//   - last_heartbeat_at：同上，只是它每轮心跳都会变。
//
// kernels / meta 是覆盖语义（内存实现也一样：调用方传 nil meta 就清空）。
// 参数号：$1 worker_id · $2 kernels · $3 心跳（可空）· $4 started_at（可空）
// · $5 meta（可空）· $6 本次时钟。
//
// 显式 ::timestamptz / ::text 不是装饰，是两条实测红换来的：
//   - 零值时间作为**未类型化 NULL** 进 COALESCE 时，PG 把分支推成 text，
//     报「column is of type timestamptz but expression is of type text」；
//   - 未类型化参数被别处（jsonb 列）定型后，同一参数在 text 列上会拿到
//     **jsonb 的文本表示**（键后多空格）——那会让 request/identity 一类的
//     字节锚点漂移，见 queue.go 的 INSERT 同一处置。
const registerSQL = `
	INSERT INTO run.worker_registry
		(worker_id, kernels, status, last_heartbeat_at, drain_requested_at, started_at, meta)
	VALUES
		($1, $2, 'active', COALESCE($3::timestamptz, $6::timestamptz), NULL,
		 COALESCE($4::timestamptz, $6::timestamptz), CAST($5::text AS jsonb))
	ON CONFLICT (worker_id) DO UPDATE SET
		kernels = EXCLUDED.kernels,
		meta = EXCLUDED.meta,
		status = run.worker_registry.status,
		drain_requested_at = run.worker_registry.drain_requested_at,
		last_heartbeat_at = COALESCE($3::timestamptz, run.worker_registry.last_heartbeat_at, $6::timestamptz),
		started_at = COALESCE($4::timestamptz, run.worker_registry.started_at, $6::timestamptz)`

// Register 见 queue.WorkerRegistry 的契约注释。
func (r *Registry) Register(ctx context.Context, w queue.WorkerRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if w.WorkerID == "" {
		return fmt.Errorf("%w: worker_id is required", queue.ErrInvalidInput)
	}
	at := timeNow()
	kernels := w.Kernels
	if kernels == nil {
		kernels = []string{}
	}
	var meta any
	if w.Meta != nil {
		// 传 text 再显式 CAST 成 jsonb：直接把 []byte 交给 PG，未类型化参数会被
		// 别处（jsonb 列）定型，同一参数在别的列上就变成 jsonb 的文本表示。
		meta = string(w.Meta)
	}
	_, err := r.pool.Exec(ctx, registerSQL,
		w.WorkerID, kernels, nullableTime(w.LastHeartbeatAt), nullableTime(w.StartedAt), meta, at)
	if err != nil {
		return fmt.Errorf("pgstore: register worker: %w", err)
	}
	return nil
}

// Heartbeat 见契约注释：未知 worker → ErrWorkerNotFound（不自动注册，fail-loud）；
// draining 只更新心跳、返回 nil。
func (r *Registry) Heartbeat(ctx context.Context, workerID string, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if at.IsZero() {
		at = timeNow()
	}
	tag, err := r.pool.Exec(ctx,
		`UPDATE run.worker_registry SET last_heartbeat_at = $2 WHERE worker_id = $1`,
		workerID, at.UTC())
	if err != nil {
		return fmt.Errorf("pgstore: worker heartbeat: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", queue.ErrWorkerNotFound, workerID)
	}
	return nil
}

// Drain 见契约注释：active → draining；重复 drain → nil（幂等）；
// offline → ErrInvalidTransition。
func (r *Registry) Drain(ctx context.Context, workerID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return inTx(ctx, r.pool, func(tx pgx.Tx) error {
		var status string
		err := tx.QueryRow(ctx, `SELECT status FROM run.worker_registry WHERE worker_id = $1 FOR UPDATE`, workerID).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: %s", queue.ErrWorkerNotFound, workerID)
		}
		if err != nil {
			return fmt.Errorf("pgstore: drain lookup: %w", err)
		}
		switch queue.WorkerStatus(status) {
		case queue.WorkerActive:
			_, err = tx.Exec(ctx,
				`UPDATE run.worker_registry SET status = 'draining', drain_requested_at = $2 WHERE worker_id = $1`,
				workerID, timeNow())
			return err
		case queue.WorkerDraining:
			return nil
		default:
			return fmt.Errorf("%w: %s is %s", queue.ErrInvalidTransition, workerID, status)
		}
	})
}

// List 见契约注释：按 worker_id 升序，空表返回 `[]`（不是 nil）。
func (r *Registry) List(ctx context.Context) ([]queue.WorkerRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// meta 走 ::text：JSONB 的 NULL 扫进 json.RawMessage 需要先判空，取文本形状更直白。
	// meta 不是身份锚点（不参与任何 hash），所以 JSONB 对数字文本的规范化在这里无代价。
	rows, err := r.pool.Query(ctx, `
		SELECT worker_id, kernels, status, last_heartbeat_at, drain_requested_at, started_at,
		       COALESCE(meta::text, '') AS meta
		FROM run.worker_registry
		ORDER BY worker_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("pgstore: list workers: %w", err)
	}
	defer rows.Close()

	out := make([]queue.WorkerRecord, 0)
	for rows.Next() {
		var (
			w                             queue.WorkerRecord
			status                        string
			heartbeat, drainAt, startedAt sql.NullTime
			meta                          string
		)
		if err := rows.Scan(&w.WorkerID, &w.Kernels, &status, &heartbeat, &drainAt, &startedAt, &meta); err != nil {
			return nil, fmt.Errorf("pgstore: scan worker: %w", err)
		}
		w.Status = queue.WorkerStatus(status)
		if heartbeat.Valid {
			w.LastHeartbeatAt = heartbeat.Time.UTC()
		}
		if drainAt.Valid {
			w.DrainRequestedAt = drainAt.Time.UTC()
		}
		if startedAt.Valid {
			w.StartedAt = startedAt.Time.UTC()
		}
		if meta != "" {
			w.Meta = json.RawMessage(meta)
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgstore: list workers rows: %w", err)
	}
	return out, nil
}
