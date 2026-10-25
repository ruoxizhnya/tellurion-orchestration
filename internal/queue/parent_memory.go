package queue

// `run.parent_jobs` 的进程内实现 + 两个实现共用的入参校验（裁-13，X5b）。
//
// 父记录与 job 队列是两张表、两套生命周期：父记录在提交时写一次，之后只随
// 三值汇总滚动（completed / invalid / failed），不入队、不领取、不租约。

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryParents 是 ParentStore 的进程内实现（测试 / 单机装配）。
type MemoryParents struct {
	mu      sync.Mutex
	parents map[string]*ParentRecord
	byKey   map[string]string // idempotency_key → parent_job_id
	byHash  map[string]string // plan_hash → parent_job_id（计划身份，跨键去重）
	now     func() time.Time
	newID   func() string
}

// 编译期断言：方法集少一个就是实现方全红（与 JobQueue 的两条断言同型）。
var _ ParentStore = (*MemoryParents)(nil)

// NewMemoryParents 构造内存父记录仓。opts 复用 MemoryOptions 的 Now / NewID。
func NewMemoryParents(opts ...MemoryOptions) *MemoryParents {
	var o MemoryOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	m := &MemoryParents{
		parents: make(map[string]*ParentRecord),
		byKey:   make(map[string]string),
		byHash:  make(map[string]string),
		now:     o.Now,
		newID:   o.NewID,
	}
	if m.now == nil {
		m.now = time.Now
	}
	if m.newID == nil {
		m.newID = randomID
	}
	return m
}

// PrepareParent 是**两个实现共用**的入参校验与身份归一。
//
// 返回生效的 canonical 与 hash：`plan_hash` 必须恒等于
// `sha256(canonical plan_canonical)`，与 `backtest_jobs.request_hash` 同一条纪律
// （JSONB 会规范化数字文本，所以身份只认 TEXT 锚点）。
func PrepareParent(rec *ParentRecord) (canonical, hash string, err error) {
	if rec == nil {
		return "", "", fmt.Errorf("%w: nil parent record", ErrInvalidInput)
	}
	if rec.IdempotencyKey == "" || len(rec.IdempotencyKey) > MaxIdempotencyKeyLen {
		return "", "", fmt.Errorf("%w: idempotency_key must be 1..%d bytes", ErrInvalidInput, MaxIdempotencyKeyLen)
	}
	if !ValidParentKind(rec.Kind) {
		return "", "", fmt.Errorf("%w: unknown parent kind %q", ErrInvalidInput, rec.Kind)
	}
	if rec.PlanCanonical == "" {
		return "", "", fmt.Errorf("%w: plan_canonical is required", ErrInvalidInput)
	}
	normalized, err := Canonicalize([]byte(rec.PlanCanonical))
	if err != nil {
		return "", "", fmt.Errorf("%w: plan_canonical: %v", ErrInvalidInput, err)
	}
	sum := hashBytes(normalized)
	if rec.PlanHash != "" && rec.PlanHash != sum {
		return "", "", fmt.Errorf("%w: plan_hash does not match plan_canonical", ErrInvalidInput)
	}
	if rec.Status == "" {
		rec.Status = RunPending
	}
	if !ValidRunStatus(rec.Status) {
		return "", "", fmt.Errorf("%w: unknown parent status %q", ErrInvalidInput, rec.Status)
	}
	if rec.Total <= 0 {
		return "", "", fmt.Errorf("%w: total must be positive", ErrInvalidInput)
	}
	if rec.Completed < 0 || rec.Invalid < 0 || rec.Failed < 0 {
		return "", "", fmt.Errorf("%w: parent counts must be non-negative", ErrInvalidInput)
	}
	if settled := rec.Completed + rec.Invalid + rec.Failed; settled > rec.Total {
		return "", "", fmt.Errorf("%w: settled counts (%d) exceed total (%d)", ErrInvalidInput, settled, rec.Total)
	}
	return string(normalized), sum, nil
}

// EnqueueParent 幂等写入。两条判据的次序是刻意的：
//   - 先按幂等键判「同键不同计划」的冲突（调用方意图被改写，必须报错）；
//   - 再按 plan_hash 判「同计划重复提交」——即使换了幂等键，同一份计划也只
//     有一个父记录（X5b 的 plan_hash 幂等；身份是计划，不是调用方随手起的键）。
func (m *MemoryParents) EnqueueParent(_ context.Context, rec *ParentRecord) (*ParentRecord, bool, error) {
	canonical, hash, err := PrepareParent(rec)
	if err != nil {
		return nil, false, err
	}
	cp := rec.Clone()
	cp.PlanCanonical = canonical
	cp.PlanHash = hash

	m.mu.Lock()
	defer m.mu.Unlock()

	if id, ok := m.byKey[cp.IdempotencyKey]; ok {
		existing := m.parents[id]
		if existing.PlanHash != cp.PlanHash {
			return nil, false, fmt.Errorf("%w: key %q reused with a different plan", ErrIdempotencyConflict, cp.IdempotencyKey)
		}
		return ptrParent(existing.Clone()), false, nil
	}
	if id, ok := m.byHash[cp.PlanHash]; ok {
		return ptrParent(m.parents[id].Clone()), false, nil
	}
	if cp.ID == "" {
		cp.ID = m.newID()
	}
	if _, exists := m.parents[cp.ID]; exists {
		return nil, false, fmt.Errorf("%w: duplicate parent id %q", ErrInvalidInput, cp.ID)
	}
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = m.now().UTC()
	} else {
		cp.CreatedAt = cp.CreatedAt.UTC()
	}
	m.parents[cp.ID] = &cp
	m.byKey[cp.IdempotencyKey] = cp.ID
	m.byHash[cp.PlanHash] = cp.ID
	return ptrParent(cp.Clone()), true, nil
}

func ptrParent(rec ParentRecord) *ParentRecord { return &rec }

// GetParent 未知 id → ErrParentNotFound。
func (m *MemoryParents) GetParent(_ context.Context, id string) (*ParentRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.parents[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrParentNotFound, id)
	}
	out := rec.Clone()
	return &out, nil
}

// SaveParentSummary 写回三值计数 / 父状态 / 明细。
func (m *MemoryParents) SaveParentSummary(_ context.Context, id string, summary ParentSummary) (*ParentRecord, error) {
	if err := summary.Validate(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.parents[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrParentNotFound, id)
	}
	if summary.Total != rec.Total {
		// total 是提交期冻结的计划规模：汇总阶段改它等于把「漏报」伪装成「计划变小」。
		return nil, fmt.Errorf("%w: summary total %d != planned total %d", ErrInvalidInput, summary.Total, rec.Total)
	}
	rec.Status = summary.Status
	rec.Completed = summary.Completed
	rec.Invalid = summary.Invalid
	rec.Failed = summary.Failed
	rec.SummaryJSON = append([]byte(nil), summary.SummaryJSON...)
	if rec.Terminal() {
		rec.CompletedAt = summary.CompletedAt.UTC()
		if rec.CompletedAt.IsZero() {
			rec.CompletedAt = m.now().UTC()
		}
	}
	out := rec.Clone()
	return &out, nil
}
