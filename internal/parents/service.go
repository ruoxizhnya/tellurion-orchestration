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

// SyncSummary 从**已入队**的子 run 重算父汇总并写回（worker 上报结论后由
// HTTP 层调用，X5b 的「真实写回」）。
//
// 与 ApplyBatchSummary 的区别：后者要调用方逐条交结论；这里直接读子 run 的
// 两轴（生命周期 + K9），因为 worker 已经通过 JobQueue 把结论写进了
// `run.backtest_jobs`。**不解析结果内容**，Score 一律为空（排名不在这里猜）。
//
// 三条判据：
//   - 子 run 必须齐全：少一条即拒绝（漏报不得被当成已完成）；
//   - `cancelled` 是终态但不是 K9 结论：按 failed 计入父汇总（否则父记录会
//     永远停在 running——子 run 不会再上报第二次）。不新增第四态；
//   - 已终态且计数 / 明细未变的父记录不重写：重复上报同一条子 run 不得刷新
//     `completed_at`。
func (s *Service) SyncSummary(ctx context.Context, parentID string) (*queue.ParentRecord, error) {
	parent, err := s.store.GetParent(ctx, parentID)
	if err != nil {
		return nil, err
	}
	lister, ok := s.jobs.(queue.ParentChildLister)
	if !ok {
		return nil, fmt.Errorf("parents: JobQueue %T cannot list children (ParentChildLister)", s.jobs)
	}
	rows, err := lister.ListChildren(ctx, parentID)
	if err != nil {
		return nil, err
	}

	var (
		counts plan.Counts
		status plan.ParentStatus
		detail []byte
	)
	switch parent.Kind {
	case queue.ParentBatch:
		outcomes, err := batchOutcomes(parent, rows)
		if err != nil {
			return nil, err
		}
		summary, err := plan.AggregateBatch(outcomes, parent.Total)
		if err != nil {
			return nil, err
		}
		counts, status = summary.Counts, summary.Status
		detail, err = json.Marshal(summary)
		if err != nil {
			return nil, fmt.Errorf("parents: marshal batch summary: %w", err)
		}
	case queue.ParentWalkForward:
		legs, err := walkForwardLegs(parent, rows)
		if err != nil {
			return nil, err
		}
		summary, err := plan.AggregateWalkForward(legs, parent.Total)
		if err != nil {
			return nil, err
		}
		counts, status = summary.Counts, summary.Status
		detail, err = json.Marshal(summary)
		if err != nil {
			return nil, fmt.Errorf("parents: marshal walkforward summary: %w", err)
		}
	default:
		return nil, fmt.Errorf("%w: parent %s has unknown kind %q", queue.ErrInvalidInput, parentID, parent.Kind)
	}

	if parent.Terminal() && parent.Status == status &&
		parent.Completed == counts.Completed && parent.Invalid == counts.Invalid &&
		parent.Failed == counts.Failed && len(parent.SummaryJSON) > 0 {
		return parent, nil
	}
	return s.store.SaveParentSummary(ctx, parentID, s.summaryOf(counts, status, detail))
}

func batchOutcomes(parent *queue.ParentRecord, rows []*queue.RunRecord) ([]plan.TaskOutcome, error) {
	if len(rows) != parent.Total {
		return nil, fmt.Errorf("parents: parent %s has %d queued children for %d planned tasks",
			parent.ID, len(rows), parent.Total)
	}
	out := make([]plan.TaskOutcome, len(rows))
	for i, row := range rows {
		out[i] = taskOutcomeOf(row)
	}
	return out, nil
}

// parentPlanShape 只解析 plan_canonical 里「子 run 顺序 → 窗口 / 段」的映射。
// 它是父计划身份的一部分（提交时已冻结），不是第二份真相。
type parentPlanShape struct {
	Children []struct {
		Kind   string `json:"kind"`
		Index  int    `json:"index"`
		Window *struct {
			Index int `json:"index"`
		} `json:"window"`
	} `json:"children"`
}

func walkForwardLegs(parent *queue.ParentRecord, rows []*queue.RunRecord) ([]plan.LegOutcome, error) {
	var shape parentPlanShape
	if err := json.Unmarshal([]byte(parent.PlanCanonical), &shape); err != nil {
		return nil, fmt.Errorf("parents: parent %s plan_canonical is not decodable: %w", parent.ID, err)
	}
	if len(shape.Children) != parent.Total*2 {
		return nil, fmt.Errorf("parents: parent %s plan has %d children for %d windows",
			parent.ID, len(shape.Children), parent.Total)
	}
	if len(rows) != len(shape.Children) {
		return nil, fmt.Errorf("parents: parent %s has %d queued children for %d planned legs",
			parent.ID, len(rows), len(shape.Children))
	}
	legs := make([]plan.LegOutcome, len(rows))
	for i, row := range rows {
		child := shape.Children[i]
		if child.Index != i+1 {
			return nil, fmt.Errorf("parents: parent %s child %d has plan index %d (order mismatch)",
				parent.ID, i+1, child.Index)
		}
		if child.Window == nil || child.Window.Index < 1 {
			return nil, fmt.Errorf("parents: parent %s child %d has no window", parent.ID, i+1)
		}
		kind := plan.LegKind(child.Kind)
		if kind != plan.LegTrain && kind != plan.LegTest {
			return nil, fmt.Errorf("parents: parent %s child %d has unknown leg %q", parent.ID, i+1, child.Kind)
		}
		outcome := taskOutcomeOf(row)
		legs[i] = plan.LegOutcome{
			Window:       child.Window.Index,
			Kind:         kind,
			RunID:        outcome.RunID,
			ResultStatus: outcome.ResultStatus,
			Error:        outcome.Error,
			DurationMs:   outcome.DurationMs,
		}
	}
	return legs, nil
}

// taskOutcomeOf 把一行子 run 折成汇总输入。
//
// 两轴不合并：ResultStatus 只在有 K9 结论时才有值。`cancelled` / 无结论的
// `failed` 都是终态，按 failed 计入（否则父汇总永远等一个不会来的回报）。
func taskOutcomeOf(row *queue.RunRecord) plan.TaskOutcome {
	status := row.ResultStatus
	errMsg := row.ErrorMessage
	if status == "" {
		switch row.Status {
		case queue.RunCancelled:
			status = queue.ResultFailed
			if errMsg == "" {
				errMsg = "cancelled"
			}
		case queue.RunFailed:
			status = queue.ResultFailed
		}
	}
	var ms int64
	if !row.StartedAt.IsZero() && row.CompletedAt.After(row.StartedAt) {
		ms = row.CompletedAt.Sub(row.StartedAt).Milliseconds()
	}
	return plan.TaskOutcome{
		RunID:        row.ID,
		ResultStatus: status,
		Error:        errMsg,
		DurationMs:   ms,
	}
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
