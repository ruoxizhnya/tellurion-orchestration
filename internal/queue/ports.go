package queue

import (
	"context"
	"time"
)

// JobQueue 是内核与 worker 读写 `run.backtest_jobs` 的唯一通道
// （docs/ARCHITECTURE.md §9.2 A-1）。方法集冻结：加方法 = MAJOR。
//
// 错误语义见 errors.go 的哨兵；实现必须并发安全。
type JobQueue interface {
	// Enqueue 幂等入队。同键同 request_hash → 返回原行、created=false；
	// 同键不同 hash → ErrIdempotencyConflict；键为空 → ErrInvalidInput；
	// 队列满 → ErrQueueFull。**不执行、不路由**。
	Enqueue(ctx context.Context, rec *RunRecord) (job *RunRecord, created bool, err error)
	// Claim 原子领取一条可运行的行（pending 且租约已释放），写 running +
	// worker_id + lease_until。无活 → (nil, nil)。draining worker → ErrWorkerDraining。
	Claim(ctx context.Context, workerID string, lease time.Duration) (*RunRecord, error)
	// RenewLease 只允许当前 owner 在租约有效期内续租；否则 ErrLeaseLost。
	RenewLease(ctx context.Context, runID, workerID string, lease time.Duration) error
	// Complete 写 status=completed + result_status + result_uri。
	// resultStatus 只接受 completed / invalid（failed 走 Fail）；
	// resultURI 必须非空（结果内容不在 `run` 里）。同终态同载荷 → nil。
	Complete(ctx context.Context, runID, workerID, resultStatus, resultURI string) error
	// Fail 写 status=failed + error_message。resultStatus 只接受 "" / failed。
	Fail(ctx context.Context, runID, workerID, errMsg, resultStatus string) error
	// RequestCancel：pending → cancelled；running → cancelling；终态 → nil。
	RequestCancel(ctx context.Context, runID string) error
	// AckCancel 由 owner 确认取消；非 owner / 租约过期 → ErrLeaseLost。
	AckCancel(ctx context.Context, runID, workerID string) error
	// Get 未知 id → ErrJobNotFound。
	Get(ctx context.Context, runID string) (*RunRecord, error)
	// List 按过滤条件返回（未知枚举值 → ErrInvalidInput，limit 钳制 ≤100）。
	List(ctx context.Context, filter ListFilter) ([]*RunRecord, error)
	// ReapExpired 把租约超时行收回：attempts+1；超过 max_attempts → failed +
	// attempts_exhausted。返回本次迁移行数。
	ReapExpired(ctx context.Context, now time.Time) (int, error)
}

// WorkerRegistry 是 worker 注册 / 心跳 / drain 的面（与 JobQueue 分开：
// 单 worker 部署不需要它）。
type WorkerRegistry interface {
	Register(ctx context.Context, w WorkerRecord) error
	Heartbeat(ctx context.Context, workerID string, at time.Time) error
	Drain(ctx context.Context, workerID string) error
	List(ctx context.Context) ([]WorkerRecord, error)
}
