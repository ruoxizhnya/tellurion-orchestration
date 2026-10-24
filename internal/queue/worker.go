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
// 重复注册同一 worker_id 是幂等覆盖：刷新能力 / 心跳 / meta，status 分两种：
//   - 已 drain（draining、或 offline 且请求过 drain）→ 保留。drain 是运维意图，
//     重注册不能把它洗回 active；
//   - offline 且从未请求 drain → active。offline 是「心跳超时」的推断，重新注册
//     是存活证明，也是 worker 崩溃重启后的唯一恢复路径。
type MemoryRegistry struct {
	mu      sync.Mutex
	workers map[string]*WorkerRecord
	now     func() time.Time
}

// 编译期断言：MemoryRegistry 必须满足 WorkerRegistry 的全部方法（方法集冻结），
// 以及可选能力 WorkerReaper——两个实现都必须提供，否则「心跳超时 → offline」
// 又只剩文档里的一句话。
var (
	_ WorkerRegistry = (*MemoryRegistry)(nil)
	_ WorkerReaper   = (*MemoryRegistry)(nil)
)

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
		// 重复注册 = 幂等覆盖（能力 / 心跳 / meta），status 按「drain 意图是否在」分：
		//   - 已 drain → 保留（洗回 active 就等于绕过 drain）；
		//   - offline 且从未请求 drain → active（见类型注释：存活证明）。
		// started_at 一律保留：它记的是「这个 worker 进程起于何时」，重注册不是新进程。
		revive := existing.Status == WorkerOffline && existing.DrainRequestedAt.IsZero()
		if revive {
			next.Status = WorkerActive
		} else {
			next.Status = existing.Status
		}
		next.DrainRequestedAt = existing.DrainRequestedAt
		if next.LastHeartbeatAt.IsZero() && !revive {
			// 复活时**不**继承那条过期心跳：继承会让下一轮回收立刻再判 offline。
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
//
// offline + 从未请求 drain 的 worker 回到 active：心跳本身就是存活证明，可以推翻
// 「超时」这条推断；请求过 drain 的不复活（运维意图优先，与重注册同口径）。
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
	if w.Status == WorkerOffline && w.DrainRequestedAt.IsZero() {
		w.Status = WorkerActive
	}
	return nil
}

// ReapOffline 见 WorkerReaper 的契约注释：心跳超时 → offline（不删行）。
//
// 判据与 PG 侧 `reapOfflineSQL` 逐字同形：`last_heartbeat_at <= now-timeout`
// （含等于），NULL / 零值心跳不参与判定——没有心跳时间就无从判断存活，不能猜。
func (r *MemoryRegistry) ReapOffline(ctx context.Context, now time.Time, timeout time.Duration) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if timeout <= 0 {
		return 0, fmt.Errorf("%w: positive heartbeat timeout is required", ErrInvalidInput)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	at := now
	if at.IsZero() {
		at = r.now()
	}
	cutoff := at.UTC().Add(-timeout)

	reaped := 0
	for _, w := range r.workers {
		if w.Status == WorkerOffline {
			continue
		}
		if w.LastHeartbeatAt.IsZero() || w.LastHeartbeatAt.After(cutoff) {
			continue
		}
		w.Status = WorkerOffline
		reaped++
	}
	return reaped, nil
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
