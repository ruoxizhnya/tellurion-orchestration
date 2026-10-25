// Package parents 是父计划（batch / walkforward）的提交与汇总入口（裁-13，X5b）。
//
// 它只做四件事：
//   - 提交：把父请求交给 `internal/plan` 展开成完整子 run 计划，算出
//     `plan_canonical` / `plan_hash`，写一条 `run.parent_jobs`；
//   - 入队：把展开后的子 run 逐条写进 `run.backtest_jobs`，填 `parent_job_id`
//     （这一列的唯一写入方），幂等键由父记录 id + 计划序号派生；
//   - 汇总：把子 run 的 K9 结论交给 `internal/plan` 的三值汇总，写回计数与父状态；
//   - 拒绝：kind 不符、计数不自洽、漏报子 run —— 全部 fail-loud。
//
// 不做的：不执行 run（引擎在 core）、不存结果内容（只存汇总 JSON 与指针）。
package parents

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
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
	jobs  queue.JobQueue
	now   func() time.Time
}

// New 构造 service。store / jobs 任一为 nil 直接 panic：每次调用都会 nil deref，
// 但装配期就能发现的错误不该拖到第一次提交；返回一个可用但会 fail 的 service
// 没有意义，所以直接要求非 nil。
func New(store queue.ParentStore, jobs queue.JobQueue, opts ...Options) *Service {
	if store == nil {
		panic("parents: ParentStore is required")
	}
	if jobs == nil {
		panic("parents: JobQueue is required")
	}
	var o Options
	if len(opts) > 0 {
		o = opts[0]
	}
	now := o.Now
	if now == nil {
		now = time.Now
	}
	return &Service{store: store, jobs: jobs, now: now}
}

// SubmitBatch 展开网格 → 写父记录 → 逐条入队子 run。返回 (parent, created, err)：
// 同幂等键同 plan_hash → 原行 + created=false；同键不同计划 → ErrIdempotencyConflict。
// 子 run 入队失败时父行保留，重放同一请求可幂等补齐（见 enqueueChildren）。
func (s *Service) SubmitBatch(ctx context.Context, idempotencyKey string, req plan.BatchRequest) (*queue.ParentRecord, bool, error) {
	pp, err := plan.BuildBatchPlan(req)
	if err != nil {
		return nil, false, err
	}
	return s.submit(ctx, idempotencyKey, pp)
}

// SubmitWalkForward 切窗口 → 写父记录 → 逐条入队子 run（每窗 train / test 两条）。
//
// days 是交易日历，由调用方提供（core QT-67 的只读端点可作来源）；本仓不自己
// 造日历，也不直连 `market.*`。
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
	parent, created, err := s.store.EnqueueParent(ctx, rec)
	if err != nil {
		return nil, false, err
	}
	if parent.ID == "" {
		// 空 id 会让 `parent_job_id` 与派生幂等键同时失去身份（不同计划会撞成
		// `parent::1`）。这是 store 违约，不是调用方请求问题——按内部错误报。
		return nil, false, fmt.Errorf("parents: ParentStore returned a parent without an id")
	}
	// 父行先落、再入队子 run：`parent_job_id` 的 FK 要求父行存在。两步不在一个
	// 事务里（两张表、两个 store），失败时父行保留、子 run 可能只入了一部分——
	// 恢复口径是「重放同一请求」：父行按 plan_hash 返回原行，子 run 按派生键
	// 逐条幂等补齐（见 enqueueChildren）。
	if err := s.enqueueChildren(ctx, parent, pp); err != nil {
		return nil, false, err
	}
	return parent, created, nil
}

// ChildIdempotencyKey 是子 run 幂等键的派生规则（stable；改规则 = MAJOR，
// 见 docs/ARCHITECTURE.md §9.6）。
//
// 形状：`parent:<parent_job_id>:<child_index>`。父 id 是计划身份（plan_hash）
// 在库里的行 id——同一份计划（即使换幂等键提交）拿到同一个父 id，因此重放
// 派生出同一批键；不同计划必然不同父 id，不会串键。`parent:` 是保留前缀：
// 调用方自己的幂等键不应使用它，否则可能与某个子 run 撞键。
func ChildIdempotencyKey(parentID string, childIndex int) string {
	return "parent:" + parentID + ":" + strconv.Itoa(childIndex)
}

// enqueueChildren 把展开后的子 run 逐条入队（`parent_job_id` 的写入方）。
//
// 幂等：键由父记录 id + 计划序号派生（ChildIdempotencyKey）。同一份计划重放
// （哪怕换了幂等键）都落回同一批子 run；因此「父行已写、子 run 没入完」可以
// 靠重放同一请求补齐，不需要跨两张表的事务。
//
// 顺序：子 run 的 created_at 按计划序号递增 1µs（PG 的时间精度是微秒）。
// 这不是装饰：ListChildren 的排序键是 created_at → id，同一提交内的行若时间
// 戳相同，顺序会退化成 id 的字典序，与计划的子 run 顺序不一致。
func (s *Service) enqueueChildren(ctx context.Context, parent *queue.ParentRecord, pp plan.ParentPlan) error {
	base := s.now().UTC()
	for i, child := range pp.Children {
		rec := &queue.RunRecord{
			IdempotencyKey: ChildIdempotencyKey(parent.ID, child.Index),
			ParentJobID:    parent.ID,
			RequestJSON:    child.Request,
			CreatedAt:      base.Add(time.Duration(i) * time.Microsecond),
		}
		if _, _, err := s.jobs.Enqueue(ctx, rec); err != nil {
			return fmt.Errorf("parents: enqueue child %d/%d of parent %s: %w",
				child.Index, len(pp.Children), parent.ID, err)
		}
	}
	return nil
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
