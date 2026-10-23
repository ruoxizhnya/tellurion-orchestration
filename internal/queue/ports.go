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
	// ReapExpired 把租约超时的行收回：
	//   - `running`：attempts+1；未超预算 → 回 `pending`，超 `max_attempts` →
	//     `failed` + `attempts_exhausted`；
	//   - `cancelling`：owner 消失后落 `cancelled`（§9.5 两轴映射表里 cancelling
	//     的唯一出口），**不**计 attempts——它不是重试，是把已作废的结果收尾。
	// 返回本次迁移的行数。
	ReapExpired(ctx context.Context, now time.Time) (int, error)
}

// KernelAwareJobQueue 是 JobQueue 的**可选能力**（docs/ARCHITECTURE.md §9.6：
// 加可选能力走新接口 = MINOR，冻结的 10 方法集不动）。
//
// 它解决 R9：§9.3 说 `WorkerRecord.Kernels` 用于「未知内核的 worker 不接该活」，
// 但冻结的 `Claim(ctx, workerID, lease)` 没有内核参数，也没有「领了再退回」的口子，
// 过滤放不进方法集。裁决（2026-10-23）= 另开一个接口，让能力声明真正被消费。
//
// 口径三条（与 Claim 同形，不另立规则）：
//   - 内核取自 worker **在注册表里声明的** `Kernels`，不是调用方现传的列表——
//     否则能力声明还是只落库不消费；
//   - 行的生效内核由 `KernelOf` 决定（**只有 JSON 字符串才算内核**，其余一律默认 `go`），
//     PG 侧 `claimSQL` 用同一规则的 SQL 形——两侧不同形就是两个实现行为不同；
//   - worker **未注册**、或注册表未装配（单机）、或它声明的 kernels 为空 ⇒ 不过滤，
//     与 `Claim` 的「未注册不拒 drain」同口径。kernels 非空 ⇒ 严格过滤。
//
// 装配方用类型断言探测：`if kq, ok := q.(queue.KernelAwareJobQueue); ok { ... }`。
type KernelAwareJobQueue interface {
	ClaimByKernel(ctx context.Context, workerID string, lease time.Duration) (*RunRecord, error)
}

// WorkerRegistry 是 worker 注册 / 心跳 / drain 的面（与 JobQueue 分开：
// 单 worker 部署不需要它）。
type WorkerRegistry interface {
	Register(ctx context.Context, w WorkerRecord) error
	Heartbeat(ctx context.Context, workerID string, at time.Time) error
	Drain(ctx context.Context, workerID string) error
	List(ctx context.Context) ([]WorkerRecord, error)
}
