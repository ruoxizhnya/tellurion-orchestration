package queue

import "errors"

// 领域哨兵。HTTP 层按 docs/api-schema.md §2 映射成 wire code；本包不定义
// wire code（那是 HTTP 层的事）。
var (
	// ErrInvalidInput 是入参不合法（空幂等键、空 worker、lease<=0、
	// 未知枚举值等）。调用方修正后重试。
	ErrInvalidInput = errors.New("queue: invalid input")
	// ErrIdempotencyConflict 是同幂等键但 request_hash 不同。
	ErrIdempotencyConflict = errors.New("queue: idempotency conflict")
	// ErrQueueFull 是 pending 深度达到上限（生产面映射 429）。
	ErrQueueFull = errors.New("queue: queue full")
	// ErrJobNotFound 是 run id 不存在。
	ErrJobNotFound = errors.New("queue: job not found")
	// ErrLeaseLost 是租约已过期 / 别人持有 / 行已终态——worker 必须立即停止。
	ErrLeaseLost = errors.New("queue: lease lost")
	// ErrInvalidTransition 是当前状态下不允许该迁移。
	ErrInvalidTransition = errors.New("queue: invalid transition")
	// ErrWorkerDraining 是**非 active**（draining / offline）的 worker 试图领取
	// 新活。冻结的方法集只有这一个「不配领新活」的哨兵，offline 不另立一个
	// （§9.6：改错误语义 = MAJOR）；报文里带上了实际状态，不靠猜。
	ErrWorkerDraining = errors.New("queue: worker draining")
	// ErrWorkerNotFound 是未知 worker（心跳 / drain 不自动注册）。
	ErrWorkerNotFound = errors.New("queue: worker not found")
)
