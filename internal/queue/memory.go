package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	// DefaultMaxQueueDepth 是 pending 深度上限的默认值（部署可配，契约只冻结
	// 「达上限 → ErrQueueFull」这件事本身）。
	DefaultMaxQueueDepth = 10000
	// DefaultMaxAttempts 与 `run.backtest_jobs.max_attempts DEFAULT 3` 对齐。
	DefaultMaxAttempts = 3
	// MaxIdempotencyKeyLen 是幂等键长度上限（docs/ARCHITECTURE.md §9.3）。
	MaxIdempotencyKeyLen = 128
	// MaxListLimit 是 List 的钳制上限；DefaultListLimit 是 filter.Limit<=0 时的取值。
	MaxListLimit     = 100
	DefaultListLimit = 50

	// AttemptsExhaustedMessage 是租约重试用尽时写入 error_message 的固定文案
	// （docs/ARCHITECTURE.md §9.5 的两轴映射表）。两个实现共用：审计与告警
	// 按这条串识别「不是引擎故障，是重试预算用尽」。
	AttemptsExhaustedMessage = "attempts_exhausted"
)

// MemoryOptions 是 Memory 的可注入项。零值即可用；测试用 Now/NewID 固定时钟。
type MemoryOptions struct {
	// Now 返回当前时间（默认 time.Now）。所有时间戳统一取 UTC。
	Now func() time.Time
	// NewID 生成 run id（默认 32 位十六进制随机串）。
	NewID func() string
	// MaxQueueDepth 是 pending 行数上限（默认 DefaultMaxQueueDepth）。
	MaxQueueDepth int
	// DefaultMaxAttempts 是未指定 max_attempts 时的取值（默认 DefaultMaxAttempts）。
	DefaultMaxAttempts int
	// Workers 是 Claim 时用来判 drain 的注册表；nil 表示单机装配、不做 drain 检查。
	Workers WorkerRegistry
}

// Memory 是 JobQueue 的进程内实现：进程重启即丢。
//
// 它存在的价值是先把状态机、幂等与租约语义钉住（X5a），后续换 PG 实现时
// 行为不变——所以这里**不做**任何 PG 做不到的事（如跨行的隐式事务）。
//
// 并发安全：单锁；所有对外的读写都拷贝出/入（RunRecord.Clone），调用方拿到
// 的行与库内行不共享底层数组。
type Memory struct {
	mu       sync.Mutex
	jobs     map[string]*RunRecord
	byKey    map[string]string // idempotency_key → run id
	now      func() time.Time
	newID    func() string
	maxDepth int
	maxAtt   int
	workers  WorkerRegistry
}

// 编译期断言：Memory 必须满足 JobQueue 的**全部**方法。
// 方法集是冻结面（docs/ARCHITECTURE.md §9.6），少一个方法就是实现方全红——
// 与其等到调用方编译失败，不如在这里钉住。
// KernelAwareJobQueue 是可选能力（R9 裁决）：它也断言在这里，因为本仓的两个实现
// 都必须提供它，否则「按能力过滤」又变成只写在文档里的一句话。
var (
	_ JobQueue            = (*Memory)(nil)
	_ KernelAwareJobQueue = (*Memory)(nil)
	_ ParentChildLister   = (*Memory)(nil)
)

// NewMemory 构造内存队列。opts 最多取第一个非零项。
func NewMemory(opts ...MemoryOptions) *Memory {
	var o MemoryOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	m := &Memory{
		jobs:     make(map[string]*RunRecord),
		byKey:    make(map[string]string),
		now:      o.Now,
		newID:    o.NewID,
		maxDepth: ResolveQueueDepth(o.MaxQueueDepth),
		maxAtt:   ResolveMaxAttempts(o.DefaultMaxAttempts, 0),
		workers:  o.Workers,
	}
	if m.now == nil {
		m.now = time.Now
	}
	if m.newID == nil {
		m.newID = randomID
	}
	return m
}

// ResolveQueueDepth 把装配项里的深度上限解析成生效值（<=0 = 未指定 = 默认）。
// 两个实现共用：同一份部署配置不能让内存实现限 10000、PG 实现不限深度。
func ResolveQueueDepth(fromOptions int) int {
	if fromOptions <= 0 {
		return DefaultMaxQueueDepth
	}
	return fromOptions
}

// ResolveMaxAttempts 把「行上的 max_attempts」按两级默认解析成生效值：
// 装配默认 <=0 时取 DefaultMaxAttempts，行未指定（<=0）时取装配默认。
// 两个实现共用，否则同一请求在两种装配下重试预算不同。
func ResolveMaxAttempts(optsDefault, rowMaxAttempts int) int {
	if optsDefault <= 0 {
		optsDefault = DefaultMaxAttempts
	}
	if rowMaxAttempts <= 0 {
		return optsDefault
	}
	return rowMaxAttempts
}

// NewRunID 生成一个 run id（32 位十六进制随机串）。两个实现共用它：
// 行的身份形状不属于某一个实现。
func NewRunID() string { return randomID() }

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 失败时没有安全的降级路径：宁可 panic 也不能生成可预测的 id。
		panic("queue: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// Enqueue 见 JobQueue 的契约注释。额外两条实现口径：
//   - `rec.RequestHash` 为空则按 request_json 现算；非空且与现算不符 → ErrInvalidInput
//     （拒绝调用方自带的过期 hash，避免幂等判据被污染）。
//   - 新行一律 pending：调用方传入非 pending 的 status 视为入参错误。
func (m *Memory) Enqueue(ctx context.Context, rec *RunRecord) (*RunRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if rec == nil {
		return nil, false, fmt.Errorf("%w: nil record", ErrInvalidInput)
	}
	if rec.IdempotencyKey == "" || len(rec.IdempotencyKey) > MaxIdempotencyKeyLen {
		return nil, false, fmt.Errorf("%w: idempotency_key must be 1..%d bytes", ErrInvalidInput, MaxIdempotencyKeyLen)
	}
	canonical, err := Canonicalize(rec.RequestJSON)
	if err != nil {
		return nil, false, err
	}
	sum := hashBytes(canonical)
	if rec.RequestHash != "" && rec.RequestHash != sum {
		return nil, false, fmt.Errorf("%w: request_hash does not match request_json", ErrInvalidInput)
	}
	if rec.Status != "" && rec.Status != RunPending {
		return nil, false, fmt.Errorf("%w: new job must start as pending, got %q", ErrInvalidInput, rec.Status)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if id, ok := m.byKey[rec.IdempotencyKey]; ok {
		existing := m.jobs[id]
		if existing.RequestHash != sum {
			return nil, false, fmt.Errorf("%w: key %q reused with a different request", ErrIdempotencyConflict, rec.IdempotencyKey)
		}
		out := existing.Clone()
		return &out, false, nil
	}
	if m.pendingLocked() >= m.maxDepth {
		return nil, false, fmt.Errorf("%w: pending depth reached %d", ErrQueueFull, m.maxDepth)
	}

	row := rec.Clone()
	row.RequestHash = sum
	row.RequestJSON = canonical
	row.Status = RunPending
	row.ResultStatus = ""
	row.Attempts = 0
	row.WorkerID = ""
	row.LeaseUntil = time.Time{}
	row.CancelRequested = false
	row.ErrorMessage = ""
	row.ResultURI = ""
	row.StartedAt = time.Time{}
	row.CompletedAt = time.Time{}
	row.MaxAttempts = ResolveMaxAttempts(m.maxAtt, row.MaxAttempts)
	if row.ID == "" {
		row.ID = m.newID()
	}
	if _, dup := m.jobs[row.ID]; dup {
		return nil, false, fmt.Errorf("%w: duplicate run id %q", ErrInvalidInput, row.ID)
	}
	if row.CreatedAt.IsZero() {
		row.CreatedAt = m.now().UTC()
	} else {
		row.CreatedAt = row.CreatedAt.UTC()
	}

	m.jobs[row.ID] = &row
	m.byKey[row.IdempotencyKey] = row.ID
	out := row.Clone()
	return &out, true, nil
}

// Claim 见 JobQueue 的契约注释。领取顺序：priority 降序 → created_at 升序 →
// id 升序（最后一个键保证结果确定，便于测试与排障）。
//
// workers 为 nil 时不校验注册表；非 nil 时**已注册且非 active** 的 worker 被拒
// （未注册的 worker 不拒——单机装配允许不建注册表）。
func (m *Memory) Claim(ctx context.Context, workerID string, lease time.Duration) (*RunRecord, error) {
	return m.claim(ctx, workerID, lease, false)
}

// ClaimByKernel 见 KernelAwareJobQueue 的契约注释（R9 裁决的落点）：能力取自
// worker **在注册表里声明的** Kernels，不是调用方现传的列表。
func (m *Memory) ClaimByKernel(ctx context.Context, workerID string, lease time.Duration) (*RunRecord, error) {
	return m.claim(ctx, workerID, lease, true)
}

// claim 是两个领取入口的共用体。byKernel = true 时把注册表里声明的能力用作过滤。
func (m *Memory) claim(ctx context.Context, workerID string, lease time.Duration, byKernel bool) (*RunRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if workerID == "" || lease <= 0 {
		return nil, fmt.Errorf("%w: worker_id and positive lease are required", ErrInvalidInput)
	}
	var declared []string
	if m.workers != nil {
		workers, err := m.workers.List(ctx)
		if err != nil {
			return nil, err
		}
		for _, w := range workers {
			if w.WorkerID == workerID {
				if w.Status != WorkerActive {
					return nil, fmt.Errorf("%w: %s is %s", ErrWorkerDraining, workerID, w.Status)
				}
				if byKernel {
					declared = w.Kernels
				}
			}
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	var best *RunRecord
	for _, r := range m.jobs {
		if r.Status != RunPending {
			continue
		}
		if !MatchesKernels(declared, r.RequestJSON) {
			continue
		}
		if best == nil || claimOrder(r, best) {
			best = r
		}
	}
	if best == nil {
		return nil, nil
	}

	now := m.now().UTC()
	best.Status = RunRunning
	best.WorkerID = workerID
	best.LeaseUntil = now.Add(lease)
	if best.StartedAt.IsZero() {
		best.StartedAt = now
	}
	out := best.Clone()
	return &out, nil
}

// claimOrder 报告 a 是否应排在 b 之前。
func claimOrder(a, b *RunRecord) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.Before(b.CreatedAt)
	}
	return a.ID < b.ID
}

// RenewLease 见 JobQueue 的契约注释。
func (m *Memory) RenewLease(ctx context.Context, runID, workerID string, lease time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if lease <= 0 {
		return fmt.Errorf("%w: positive lease is required", ErrInvalidInput)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	row, ok := m.jobs[runID]
	if !ok || row.Terminal() {
		return fmt.Errorf("%w: %s is not renewable", ErrLeaseLost, runID)
	}
	if err := m.checkLeaseLocked(row, workerID); err != nil {
		return err
	}
	row.LeaseUntil = m.now().UTC().Add(lease)
	return nil
}

// Complete 见 JobQueue 的契约注释。
//
// `cancelling` 行不接受 Complete：取消一经请求，结果即作废，worker 应走
// AckCancel（两轴映射表里 `cancelling` 的唯一出口是 `cancelled`）。
func (m *Memory) Complete(ctx context.Context, runID, workerID, resultStatus, resultURI string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rs := ResultStatus(resultStatus)
	if rs != ResultCompleted && rs != ResultInvalid {
		return fmt.Errorf("%w: result_status must be completed or invalid, got %q", ErrInvalidInput, resultStatus)
	}
	if resultURI == "" {
		return fmt.Errorf("%w: result_uri is required (result content is not stored in `run`)", ErrInvalidInput)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	row, ok := m.jobs[runID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrJobNotFound, runID)
	}
	if row.Status == RunCompleted {
		if row.ResultStatus == rs && row.ResultURI == resultURI {
			return nil
		}
		return fmt.Errorf("%w: %s already completed with a different payload", ErrInvalidTransition, runID)
	}
	if row.Status != RunRunning {
		return fmt.Errorf("%w: %s is %s", ErrInvalidTransition, runID, row.Status)
	}
	if err := m.checkLeaseLocked(row, workerID); err != nil {
		return err
	}

	row.Status = RunCompleted
	row.ResultStatus = rs
	row.ResultURI = resultURI
	row.CompletedAt = m.now().UTC()
	row.LeaseUntil = time.Time{}
	return nil
}

// Fail 见 JobQueue 的契约注释。`resultStatus` 只接受 "" / "failed"。
func (m *Memory) Fail(ctx context.Context, runID, workerID, errMsg, resultStatus string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rs := ResultStatus(resultStatus)
	if rs != "" && rs != ResultFailed {
		return fmt.Errorf("%w: result_status must be empty or failed, got %q", ErrInvalidInput, resultStatus)
	}
	if errMsg == "" {
		return fmt.Errorf("%w: error_message is required for a failed job", ErrInvalidInput)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	row, ok := m.jobs[runID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrJobNotFound, runID)
	}
	if row.Status == RunFailed {
		if row.ResultStatus == rs && row.ErrorMessage == errMsg {
			return nil
		}
		return fmt.Errorf("%w: %s already failed with a different payload", ErrInvalidTransition, runID)
	}
	if row.Status != RunRunning {
		return fmt.Errorf("%w: %s is %s", ErrInvalidTransition, runID, row.Status)
	}
	if err := m.checkLeaseLocked(row, workerID); err != nil {
		return err
	}

	row.Status = RunFailed
	row.ResultStatus = rs
	row.ErrorMessage = errMsg
	row.CompletedAt = m.now().UTC()
	row.LeaseUntil = time.Time{}
	return nil
}

// RequestCancel 见 JobQueue 的契约注释：pending 直接落 cancelled（保留行，
// 不删——删行会同时删掉幂等键与审计），running 只写 cancel_requested 并转
// cancelling，不碰跑着的 worker。
func (m *Memory) RequestCancel(ctx context.Context, runID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	row, ok := m.jobs[runID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrJobNotFound, runID)
	}
	switch row.Status {
	case RunPending:
		row.Status = RunCancelled
		row.CancelRequested = true
		row.CompletedAt = m.now().UTC()
		row.LeaseUntil = time.Time{}
		return nil
	case RunRunning:
		row.Status = RunCancelling
		row.CancelRequested = true
		return nil
	case RunCancelling:
		row.CancelRequested = true
		return nil
	default:
		// completed / failed / cancelled：取消是幂等空操作。
		return nil
	}
}

// AckCancel 见 JobQueue 的契约注释：只允许当前 owner 在 running/cancelling 上
// 确认；已终态 → ErrInvalidTransition。
//
// `pending` 行走下面的租约校验：它没有 owner、没有租约，因此报 ErrLeaseLost
// （§9.2 冻结表只列了「非 owner → ErrLeaseLost」与「已终态 → ErrInvalidTransition」
// 两种出口，这里不擅自加第三种）。
func (m *Memory) AckCancel(ctx context.Context, runID, workerID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	row, ok := m.jobs[runID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrJobNotFound, runID)
	}
	if row.Terminal() {
		return fmt.Errorf("%w: %s is already %s", ErrInvalidTransition, runID, row.Status)
	}
	if err := m.checkLeaseLocked(row, workerID); err != nil {
		return err
	}

	row.Status = RunCancelled
	row.ResultStatus = ""
	row.CancelRequested = true
	row.CompletedAt = m.now().UTC()
	row.LeaseUntil = time.Time{}
	return nil
}

// Get 见 JobQueue 的契约注释。
func (m *Memory) Get(ctx context.Context, runID string) (*RunRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	row, ok := m.jobs[runID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrJobNotFound, runID)
	}
	out := row.Clone()
	return &out, nil
}

// List 见 JobQueue 的契约注释。排序：created_at 降序 → id 升序；`Before` 严格
// 小于；空结果返回 `[]`（不是 nil，保证 JSON 稳定）。
func (m *Memory) List(ctx context.Context, filter ListFilter) ([]*RunRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if filter.Status != "" && !ValidRunStatus(filter.Status) {
		return nil, fmt.Errorf("%w: unknown status %q", ErrInvalidInput, filter.Status)
	}
	if filter.ResultStatus != "" && !ValidResultStatus(filter.ResultStatus) {
		return nil, fmt.Errorf("%w: unknown result_status %q", ErrInvalidInput, filter.ResultStatus)
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = DefaultListLimit
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}

	m.mu.Lock()
	rows := make([]RunRecord, 0, len(m.jobs))
	for _, r := range m.jobs {
		if filter.Status != "" && r.Status != filter.Status {
			continue
		}
		if filter.ResultStatus != "" && r.ResultStatus != filter.ResultStatus {
			continue
		}
		if !filter.Before.IsZero() && !r.CreatedAt.Before(filter.Before) {
			continue
		}
		rows = append(rows, r.Clone())
	}
	m.mu.Unlock()

	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].CreatedAt.Equal(rows[j].CreatedAt) {
			return rows[i].CreatedAt.After(rows[j].CreatedAt)
		}
		return rows[i].ID < rows[j].ID
	})
	if len(rows) > limit {
		rows = rows[:limit]
	}
	out := make([]*RunRecord, 0, len(rows))
	for i := range rows {
		out = append(out, &rows[i])
	}
	return out, nil
}

// ListChildren 见 ParentChildLister 的契约注释。顺序按 created_at 升序、id 升序：
// 子 run 通常是按计划顺序入队的，时间相同（测试固定时钟）时 id 兜底保证确定性。
// 未知 parentID 返回空切片而不是 ErrParentNotFound——父记录是否存在由
// ParentStore 判，本方法只回答「这张表里有没有指向它的行」。
func (m *Memory) ListChildren(ctx context.Context, parentID string) ([]*RunRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	rows := make([]RunRecord, 0)
	for _, r := range m.jobs {
		if r.ParentJobID != parentID {
			continue
		}
		rows = append(rows, r.Clone())
	}
	m.mu.Unlock()

	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].CreatedAt.Equal(rows[j].CreatedAt) {
			return rows[i].CreatedAt.Before(rows[j].CreatedAt)
		}
		return rows[i].ID < rows[j].ID
	})
	out := make([]*RunRecord, 0, len(rows))
	for i := range rows {
		out = append(out, &rows[i])
	}
	return out, nil
}

// ReapExpired 见 JobQueue 的契约注释。处理两类行（R8 于 2026-10-23 裁决后扩展）：
//
//   - `running` 且租约已到期（含等于 now）：attempts+1，未超预算回 pending，
//     超了落 failed + attempts_exhausted；
//   - `cancelling` 且租约已到期：owner 已经消失，`AckCancel` 永远等不到，
//     按两轴映射表落 `cancelled`。这类行**不**计 attempts——它不是重试，是给
//     已作废的结果收尾（计入重试预算会把一次取消记成一次失败尝试）。
//
// 两类的 `worker_id` / `started_at` 都保留，便于审计上一次 owner。
func (m *Memory) ReapExpired(ctx context.Context, now time.Time) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	at := now
	if at.IsZero() {
		at = m.now()
	}
	at = at.UTC()

	reaped := 0
	for _, r := range m.jobs {
		if r.Status != RunRunning && r.Status != RunCancelling {
			continue
		}
		if r.LeaseUntil.IsZero() || at.Before(r.LeaseUntil) {
			continue
		}
		reaped++
		if r.Status == RunCancelling {
			r.Status = RunCancelled
			r.ResultStatus = ""
			r.CancelRequested = true
			r.CompletedAt = at
			r.LeaseUntil = time.Time{}
			continue
		}
		r.Attempts++
		r.LeaseUntil = time.Time{}
		if r.Attempts >= r.MaxAttempts {
			r.Status = RunFailed
			r.ResultStatus = ""
			r.ErrorMessage = AttemptsExhaustedMessage
			r.CompletedAt = at
			continue
		}
		r.Status = RunPending
	}
	return reaped, nil
}

// checkLeaseLocked 校验 owner + 租约有效期。调用方必须持锁。
func (m *Memory) checkLeaseLocked(row *RunRecord, workerID string) error {
	if workerID == "" || row.WorkerID != workerID {
		return fmt.Errorf("%w: %s is owned by %q, not %q", ErrLeaseLost, row.ID, row.WorkerID, workerID)
	}
	if row.LeaseUntil.IsZero() || !m.now().UTC().Before(row.LeaseUntil) {
		return fmt.Errorf("%w: lease of %s expired", ErrLeaseLost, row.ID)
	}
	return nil
}

// pendingLocked 统计 pending 行数。调用方必须持锁。
func (m *Memory) pendingLocked() int {
	n := 0
	for _, r := range m.jobs {
		if r.Status == RunPending {
			n++
		}
	}
	return n
}
