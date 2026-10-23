package queue

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// RegistryOptions 是 MemoryRegistry 的可注入项。
type RegistryOptions struct {
	// Now 返回当前时间（默认 time.Now），统一取 UTC。
	Now func() time.Time
}

// MemoryRegistry 是 WorkerRegistry 的进程内实现（X5c 之前够用）。
//
// 重复注册同一 worker_id 是幂等覆盖：只刷新能力 / 心跳 / meta，**不改 status**
// ——已 drain 的 worker 重新注册不能自己变回 active（否则 drain 可被绕过）。
type MemoryRegistry struct {
	mu      sync.Mutex
	workers map[string]*WorkerRecord
	now     func() time.Time
}

// 编译期断言：MemoryRegistry 必须满足 WorkerRegistry 的全部方法（方法集冻结）。
var _ WorkerRegistry = (*MemoryRegistry)(nil)

// NewMemoryRegistry 构造内存注册表。
func NewMemoryRegistry(opts ...RegistryOptions) *MemoryRegistry {
	var o RegistryOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	r := &MemoryRegistry{
		workers: make(map[string]*WorkerRecord),
		now:     o.Now,
	}
	if r.now == nil {
		r.now = time.Now
	}
	return r
}

// Register 见 WorkerRegistry 的契约注释：空 id → ErrInvalidInput；重复注册
// 覆盖能力 / 心跳（保留 status 与 drain_requested_at）。
func (r *MemoryRegistry) Register(ctx context.Context, w WorkerRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if w.WorkerID == "" {
		return fmt.Errorf("%w: worker_id is required", ErrInvalidInput)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.now().UTC()
	next := w.Clone()
	if existing, ok := r.workers[w.WorkerID]; ok {
		// 重复注册 = 幂等覆盖（能力 / 心跳 / meta），**不动** status、
		// drain_requested_at 与 started_at：
		//   - status 保留 → 已 drain 的 worker 不能靠重注册洗回 active；
		//   - started_at 保留 → 它记的是「这个 worker 进程起于何时」，
		//     重注册只是又一次心跳式登记，不是新进程（洗掉就查不出存活时长）。
		next.Status = existing.Status
		next.DrainRequestedAt = existing.DrainRequestedAt
		if next.LastHeartbeatAt.IsZero() {
			next.LastHeartbeatAt = existing.LastHeartbeatAt
		}
		if next.StartedAt.IsZero() {
			next.StartedAt = existing.StartedAt
		}
	} else {
		next.Status = WorkerActive
	}
	if next.LastHeartbeatAt.IsZero() {
		next.LastHeartbeatAt = now
	} else {
		next.LastHeartbeatAt = next.LastHeartbeatAt.UTC()
	}
	if next.StartedAt.IsZero() {
		next.StartedAt = now
	} else {
		next.StartedAt = next.StartedAt.UTC()
	}
	if !next.DrainRequestedAt.IsZero() {
		next.DrainRequestedAt = next.DrainRequestedAt.UTC()
	}
	r.workers[next.WorkerID] = &next
	return nil
}

// Heartbeat 见 WorkerRegistry 的契约注释：未知 worker → ErrWorkerNotFound
// （不自动注册，fail-loud）；draining 只更新心跳。
func (r *MemoryRegistry) Heartbeat(ctx context.Context, workerID string, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	w, ok := r.workers[workerID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrWorkerNotFound, workerID)
	}
	if at.IsZero() {
		at = r.now()
	}
	w.LastHeartbeatAt = at.UTC()
	return nil
}

// Drain 见 WorkerRegistry 的契约注释：active → draining；重复 drain → nil；
// offline → ErrInvalidTransition。
func (r *MemoryRegistry) Drain(ctx context.Context, workerID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	w, ok := r.workers[workerID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrWorkerNotFound, workerID)
	}
	switch w.Status {
	case WorkerActive:
		w.Status = WorkerDraining
		w.DrainRequestedAt = r.now().UTC()
		return nil
	case WorkerDraining:
		return nil
	default:
		return fmt.Errorf("%w: %s is %s", ErrInvalidTransition, workerID, w.Status)
	}
}

// List 见 WorkerRegistry 的契约注释：按 worker_id 升序，空表返回 `[]`。
func (r *MemoryRegistry) List(ctx context.Context) ([]WorkerRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r.mu.Lock()
	out := make([]WorkerRecord, 0, len(r.workers))
	for _, w := range r.workers {
		out = append(out, w.Clone())
	}
	r.mu.Unlock()

	sort.Slice(out, func(i, j int) bool { return out[i].WorkerID < out[j].WorkerID })
	return out, nil
}
