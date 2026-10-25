// Package parents 是父计划（batch / walkforward）的提交与汇总入口（裁-13，X5b）。
//
// 它只做三件事：
//   - 提交：把父请求交给 `internal/plan` 展开成完整子 run 计划，算出
//     `plan_canonical` / `plan_hash`，写一条 `run.parent_jobs`；
//   - 汇总：把子 run 的 K9 结论交给 `internal/plan` 的三值汇总，写回计数与父状态；
//   - 拒绝：kind 不符、计数不自洽、漏报子 run —— 全部 fail-loud。
//
// 不做的：不执行 run（引擎在 core）、不入队子 run（调用方在拿到计划后自行入队，
// 幂等键由父计划派生）、不存结果内容（只存汇总 JSON 与指针）。
package parents

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"tellurion-orchestration/internal/plan"
	"tellurion-orchestration/internal/queue"
)

// Options 是 service 的可注入项（零值即可用）。
type Options struct {
	// Now 返回当前时间（默认 time.Now）。所有时间戳统一取 UTC。
	Now func() time.Time
}

// Service 是父计划的提交 / 汇总入口。
type Service struct {
	store queue.ParentStore
	now   func() time.Time
}

// New 构造 service。store 为 nil 直接 panic 不划算——每次调用都会 nil deref，
// 但装配期就能发现的错误不该拖到第一次提交；这里返回一个可用但会 fail 的
// service 没有意义，所以直接要求非 nil。
func New(store queue.ParentStore, opts ...Options) *Service {
	if store == nil {
		panic("parents: ParentStore is required")
	}
	var o Options
	if len(opts) > 0 {
		o = opts[0]
	}
	now := o.Now
	if now == nil {
		now = time.Now
	}
	return &Service{store: store, now: now}
}

// SubmitBatch 展开网格 → 写父记录。返回 (parent, created, err)：
// 同幂等键同 plan_hash → 原行 + created=false；同键不同计划 → ErrIdempotencyConflict。
func (s *Service) SubmitBatch(ctx context.Context, idempotencyKey string, req plan.BatchRequest) (*queue.ParentRecord, bool, error) {
	pp, err := plan.BuildBatchPlan(req)
	if err != nil {
		return nil, false, err
	}
	return s.submit(ctx, idempotencyKey, pp)
}

// SubmitWalkForward 切窗口 → 写父记录。days 是交易日历（由 data 服务的只读
// port 提供；本仓不自己造日历）。
func (s *Service) SubmitWalkForward(ctx context.Context, idempotencyKey string, base json.RawMessage, cfg plan.WindowConfig, days []time.Time) (*queue.ParentRecord, bool, error) {
	pp, err := plan.BuildWalkForwardPlan(base, cfg, days)
	if err != nil {
		return nil, false, err
	}
	return s.submit(ctx, idempotencyKey, pp)
}

func (s *Service) submit(ctx context.Context, idempotencyKey string, pp plan.ParentPlan) (*queue.ParentRecord, bool, error) {
	canonical, err := pp.Canonical()
	if err != nil {
		return nil, false, err
	}
	hash, err := pp.Hash()
	if err != nil {
		return nil, false, err
	}
	counts := pp.PendingCounts()
	status, err := plan.ResolveStatus(counts)
	if err != nil {
		return nil, false, err
	}
	rec := &queue.ParentRecord{
		IdempotencyKey: idempotencyKey,
		Kind:           pp.Kind,
		PlanCanonical:  string(canonical),
		PlanHash:       hash,
		Status:         status,
		Total:          counts.Total,
	}
	return s.store.EnqueueParent(ctx, rec)
}

// Get 读一条父记录（GET /api/runs/parents/{id} 用）。未知 id →
// queue.ErrParentNotFound；父状态与三值计数是已落库的值，不在读路径重算
// （重算会让「还没汇总」看起来像「已经汇总成另一种状态」）。
func (s *Service) Get(ctx context.Context, parentID string) (*queue.ParentRecord, error) {
	return s.store.GetParent(ctx, parentID)
}

// ApplyBatchSummary 把批量子 run 的结论汇总并写回父记录。
//
// 调用方必须为**每个**子 run 交一条 outcome（未回报的用空 ResultStatus）；
// 少交一条即拒绝（plan.AggregateBatch 的判据）——漏报不得被当成全部完成。
func (s *Service) ApplyBatchSummary(ctx context.Context, parentID string, outcomes []plan.TaskOutcome) (*queue.ParentRecord, error) {
	parent, err := s.load(ctx, parentID, queue.ParentBatch)
	if err != nil {
		return nil, err
	}
	summary, err := plan.AggregateBatch(outcomes, parent.Total)
	if err != nil {
		return nil, err
	}
	detail, err := json.Marshal(summary)
	if err != nil {
		return nil, fmt.Errorf("parents: marshal batch summary: %w", err)
	}
	return s.store.SaveParentSummary(ctx, parentID, s.summaryOf(summary.Counts, summary.Status, detail))
}

// ApplyWalkForwardSummary 把逐段结论汇总成逐窗结论并写回父记录。
func (s *Service) ApplyWalkForwardSummary(ctx context.Context, parentID string, legs []plan.LegOutcome) (*queue.ParentRecord, error) {
	parent, err := s.load(ctx, parentID, queue.ParentWalkForward)
	if err != nil {
		return nil, err
	}
	summary, err := plan.AggregateWalkForward(legs, parent.Total)
	if err != nil {
		return nil, err
	}
	detail, err := json.Marshal(summary)
	if err != nil {
		return nil, fmt.Errorf("parents: marshal walkforward summary: %w", err)
	}
	return s.store.SaveParentSummary(ctx, parentID, s.summaryOf(summary.Counts, summary.Status, detail))
}

func (s *Service) load(ctx context.Context, parentID string, want queue.ParentKind) (*queue.ParentRecord, error) {
	parent, err := s.store.GetParent(ctx, parentID)
	if err != nil {
		return nil, err
	}
	if parent.Kind != want {
		return nil, fmt.Errorf("%w: parent %s is %q, want %q", queue.ErrInvalidInput, parentID, parent.Kind, want)
	}
	return parent, nil
}

// summaryOf 把 plan.Counts / ParentStatus 映射成落库形状（唯一映射点）。
func (s *Service) summaryOf(counts plan.Counts, status plan.ParentStatus, detail json.RawMessage) queue.ParentSummary {
	out := queue.ParentSummary{
		Status:      status,
		Total:       counts.Total,
		Completed:   counts.Completed,
		Invalid:     counts.Invalid,
		Failed:      counts.Failed,
		SummaryJSON: detail,
	}
	if status != plan.ParentRunning {
		out.CompletedAt = s.now().UTC()
	}
	return out
}
