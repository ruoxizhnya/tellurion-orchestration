package parents

// 父计划提交 / 汇总的语义腿（裁-13，X5b）。
//
// 这里同时证明两件事：
//   - 提交即写父表：kind / plan_hash / total / running 一次落库，重放不新写；
//   - 汇总复用 internal/plan：写回的三个计数与父状态逐字段等于 Aggregate* 的产出。

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"tellurion-orchestration/internal/plan"
	"tellurion-orchestration/internal/queue"
)

const batchBase = `{
	"strategy": "momentum",
	"stock_pool": ["600000.SH"],
	"start_date": "2024-01-02",
	"end_date": "2024-12-31",
	"initial_capital": 1000000,
	"kernel": "go",
	"fill_timing": "next_open",
	"seed": 42
}`

func batchRequest() plan.BatchRequest {
	return plan.BatchRequest{
		BaseRunSpec: json.RawMessage(batchBase),
		Grid: map[string][]json.RawMessage{
			"lookback":  {json.RawMessage(`10`), json.RawMessage(`20`)},
			"threshold": {json.RawMessage(`0.1`), json.RawMessage(`0.2`)},
		},
	}
}

func wfDays() []time.Time {
	start := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	out := make([]time.Time, 10)
	for i := range out {
		out[i] = start.AddDate(0, 0, i)
	}
	return out
}

func TestSubmitBatchWritesParentOnSubmit(t *testing.T) {
	ctx := context.Background()
	store := queue.NewMemoryParents()
	svc := New(store, queue.NewMemory())

	parent, created, err := svc.SubmitBatch(ctx, "batch-key-1", batchRequest())
	if err != nil {
		t.Fatalf("SubmitBatch: %v", err)
	}
	if !created {
		t.Fatal("first submit must create the parent row")
	}
	if parent.Kind != queue.ParentBatch || parent.Total != 4 || parent.Status != queue.RunRunning {
		t.Fatalf("parent = %+v, want batch/4/running", parent)
	}
	if len(parent.PlanCanonical) == 0 || len(parent.PlanHash) != 64 {
		t.Fatalf("plan identity missing: %+v", parent)
	}

	// 重放：同键同计划 → 原行；不同计划 → 冲突。
	replayed, created, err := svc.SubmitBatch(ctx, "batch-key-1", batchRequest())
	if err != nil || created {
		t.Fatalf("replay: created=%v err=%v", created, err)
	}
	if replayed.ID != parent.ID {
		t.Fatalf("replay id = %s, want %s", replayed.ID, parent.ID)
	}

	other := batchRequest()
	other.Grid = map[string][]json.RawMessage{"lookback": {json.RawMessage(`5`)}}
	if _, _, err := svc.SubmitBatch(ctx, "batch-key-1", other); !errors.Is(err, queue.ErrIdempotencyConflict) {
		t.Fatalf("different plan: err = %v, want ErrIdempotencyConflict", err)
	}
}

func TestSubmitWalkForwardCountsWindowsNotLegs(t *testing.T) {
	ctx := context.Background()
	store := queue.NewMemoryParents()
	svc := New(store, queue.NewMemory())

	parent, created, err := svc.SubmitWalkForward(ctx, "wf-key-1",
		json.RawMessage(batchBase), plan.WindowConfig{TrainDays: 3, TestDays: 2, StepDays: 2}, wfDays())
	if err != nil || !created {
		t.Fatalf("SubmitWalkForward: created=%v err=%v", created, err)
	}
	// 10 个交易日、train3+test2、步长 2 ⇒ 3 个完整窗口（6 条子 run）。
	if parent.Kind != queue.ParentWalkForward || parent.Total != 3 {
		t.Fatalf("parent = %+v, want walkforward/3 units", parent)
	}
}

func TestApplyBatchSummaryMatchesPlanAggregation(t *testing.T) {
	ctx := context.Background()
	store := queue.NewMemoryParents()
	svc := New(store, queue.NewMemory())
	parent, _, err := svc.SubmitBatch(ctx, "batch-key-1", batchRequest())
	if err != nil {
		t.Fatal(err)
	}

	outcomes := []plan.TaskOutcome{
		{RunID: "r1", ResultStatus: queue.ResultCompleted, Score: ptr(1.5)},
		{RunID: "r2", ResultStatus: queue.ResultInvalid},
		{RunID: "r3", ResultStatus: queue.ResultInvalid},
		{RunID: "r4"}, // pending
	}
	got, err := svc.ApplyBatchSummary(ctx, parent.ID, outcomes)
	if err != nil {
		t.Fatalf("ApplyBatchSummary: %v", err)
	}

	want, err := plan.AggregateBatch(outcomes, 4)
	if err != nil {
		t.Fatal(err)
	}
	if got.Completed != want.Counts.Completed || got.Invalid != want.Counts.Invalid ||
		got.Failed != want.Counts.Failed || got.Total != want.Counts.Total {
		t.Fatalf("stored counts = %d/%d/%d total %d, want %d/%d/%d total %d",
			got.Completed, got.Invalid, got.Failed, got.Total,
			want.Counts.Completed, want.Counts.Invalid, want.Counts.Failed, want.Counts.Total)
	}
	if got.Status != want.Status || got.Status != queue.RunRunning {
		t.Fatalf("status = %q / %q, want running（还有未回报的子 run）", got.Status, want.Status)
	}
	if len(got.SummaryJSON) == 0 {
		t.Fatal("summary_json must carry the aggregate detail")
	}

	// 漏报（少交一条）必须拒绝，且不覆盖已写回的汇总。
	if _, err := svc.ApplyBatchSummary(ctx, parent.ID, outcomes[:3]); err == nil {
		t.Fatal("missing outcome must be rejected")
	}
}

func TestApplyWalkForwardSummaryMatchesPlanAggregation(t *testing.T) {
	ctx := context.Background()
	store := queue.NewMemoryParents()
	svc := New(store, queue.NewMemory())
	parent, _, err := svc.SubmitWalkForward(ctx, "wf-key-1",
		json.RawMessage(batchBase), plan.WindowConfig{TrainDays: 3, TestDays: 2, StepDays: 2}, wfDays())
	if err != nil {
		t.Fatal(err)
	}

	legs := []plan.LegOutcome{
		{Window: 1, Kind: plan.LegTrain, RunID: "w1t", ResultStatus: queue.ResultCompleted},
		{Window: 1, Kind: plan.LegTest, RunID: "w1v", ResultStatus: queue.ResultCompleted},
		{Window: 2, Kind: plan.LegTrain, RunID: "w2t", ResultStatus: queue.ResultFailed, Error: "engine"},
		{Window: 2, Kind: plan.LegTest, RunID: "w2v", ResultStatus: queue.ResultCompleted},
		{Window: 3, Kind: plan.LegTrain, RunID: "w3t"},
		{Window: 3, Kind: plan.LegTest, RunID: "w3v"},
	}
	got, err := svc.ApplyWalkForwardSummary(ctx, parent.ID, legs)
	if err != nil {
		t.Fatalf("ApplyWalkForwardSummary: %v", err)
	}

	want, err := plan.AggregateWalkForward(legs, 3)
	if err != nil {
		t.Fatal(err)
	}
	if got.Completed != want.Counts.Completed || got.Invalid != want.Counts.Invalid ||
		got.Failed != want.Counts.Failed || got.Total != want.Counts.Total {
		t.Fatalf("stored counts = %d/%d/%d total %d, want %d/%d/%d total %d",
			got.Completed, got.Invalid, got.Failed, got.Total,
			want.Counts.Completed, want.Counts.Invalid, want.Counts.Failed, want.Counts.Total)
	}
	if got.Status != want.Status || got.Status != queue.RunRunning {
		t.Fatalf("status = %q / %q, want running", got.Status, want.Status)
	}
}

func TestApplySummaryRejectsWrongParentKind(t *testing.T) {
	ctx := context.Background()
	store := queue.NewMemoryParents()
	svc := New(store, queue.NewMemory())
	parent, _, err := svc.SubmitBatch(ctx, "batch-key-1", batchRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ApplyWalkForwardSummary(ctx, parent.ID, nil); !errors.Is(err, queue.ErrInvalidInput) {
		t.Fatalf("err = %v, want ErrInvalidInput", err)
	}
}

func TestSubmitRejectsEmptyPlan(t *testing.T) {
	ctx := context.Background()
	svc := New(queue.NewMemoryParents(), queue.NewMemory())
	_, _, err := svc.SubmitBatch(ctx, "batch-key-1", plan.BatchRequest{BaseRunSpec: json.RawMessage(batchBase)})
	if !errors.Is(err, plan.ErrInvalidRequest) {
		t.Fatalf("err = %v, want plan.ErrInvalidRequest（空网格必须拒绝）", err)
	}
}

// TestSubmitBatchEnqueuesChildrenWithParentJobID 是「`parent_job_id` 的写入方」
// 的可执行判据：子 run 逐条落回队列、带上父 id、请求与计划逐字节同源、
// 幂等键由父 id + 计划序号派生；重放（同键 / 换键同计划）不新增行、不换 id。
func TestSubmitBatchEnqueuesChildrenWithParentJobID(t *testing.T) {
	ctx := context.Background()
	store := queue.NewMemoryParents()
	jobs := queue.NewMemory()
	svc := New(store, jobs)

	parent, created, err := svc.SubmitBatch(ctx, "batch-key-1", batchRequest())
	if err != nil || !created {
		t.Fatalf("SubmitBatch: created=%v err=%v", created, err)
	}
	want, err := plan.ExpandBatch(batchRequest())
	if err != nil {
		t.Fatal(err)
	}

	kids := listChildren(t, jobs, parent.ID)
	if len(kids) != len(want) {
		t.Fatalf("children = %d, want %d", len(kids), len(want))
	}
	for i, kid := range kids {
		if kid.ParentJobID != parent.ID {
			t.Fatalf("child %d parent_job_id = %q, want %q", i, kid.ParentJobID, parent.ID)
		}
		if wantKey := ChildIdempotencyKey(parent.ID, want[i].Index); kid.IdempotencyKey != wantKey {
			t.Fatalf("child %d key = %q, want %q", i, kid.IdempotencyKey, wantKey)
		}
		wantHash, err := queue.RequestHash(want[i].Request)
		if err != nil {
			t.Fatal(err)
		}
		if kid.RequestHash != wantHash {
			t.Fatalf("child %d request_hash = %s, want %s（子 run 必须是计划里的完整 RunSpec）",
				i, kid.RequestHash, wantHash)
		}
		if kid.Status != queue.RunPending || kid.ResultStatus != "" {
			t.Fatalf("child %d status = %s/%q, want pending", i, kid.Status, kid.ResultStatus)
		}
	}

	ids := childIDs(kids)
	// 同键重放：父行与子 run 全部落回原行。
	if _, created, err := svc.SubmitBatch(ctx, "batch-key-1", batchRequest()); err != nil || created {
		t.Fatalf("same key replay: created=%v err=%v", created, err)
	}
	// 换键同计划：身份是 plan_hash，父 id 与子 run 都必须保持不变。
	if _, created, err := svc.SubmitBatch(ctx, "batch-key-2", batchRequest()); err != nil || created {
		t.Fatalf("same plan new key: created=%v err=%v", created, err)
	}
	if got := childIDs(listChildren(t, jobs, parent.ID)); !slices.Equal(got, ids) {
		t.Fatalf("replay changed children: %v -> %v", ids, got)
	}
}

// TestSubmitWalkForwardEnqueuesTrainAndTestLegs：每个窗口两条子 run（train 在前
// test 在后），键与请求都来自同一份计划；Units 计的是窗口数，不是行数。
func TestSubmitWalkForwardEnqueuesTrainAndTestLegs(t *testing.T) {
	ctx := context.Background()
	store := queue.NewMemoryParents()
	jobs := queue.NewMemory()
	svc := New(store, jobs)

	cfg := plan.WindowConfig{TrainDays: 3, TestDays: 2, StepDays: 2}
	parent, created, err := svc.SubmitWalkForward(ctx, "wf-key-1", json.RawMessage(batchBase), cfg, wfDays())
	if err != nil || !created {
		t.Fatalf("SubmitWalkForward: created=%v err=%v", created, err)
	}
	if parent.Total != 3 {
		t.Fatalf("parent total = %d, want 3 windows", parent.Total)
	}
	want, err := plan.ExpandWalkForward(json.RawMessage(batchBase), cfg, wfDays())
	if err != nil {
		t.Fatal(err)
	}
	kids := listChildren(t, jobs, parent.ID)
	if len(kids) != len(want) || len(kids) != 6 {
		t.Fatalf("children = %d, want 6 (3 windows × train/test)", len(kids))
	}
	for i, kid := range kids {
		if kid.ParentJobID != parent.ID {
			t.Fatalf("child %d parent_job_id = %q, want %q", i, kid.ParentJobID, parent.ID)
		}
		if wantKey := ChildIdempotencyKey(parent.ID, want[i].Index); kid.IdempotencyKey != wantKey {
			t.Fatalf("child %d key = %q, want %q", i, kid.IdempotencyKey, wantKey)
		}
		wantHash, err := queue.RequestHash(want[i].Request)
		if err != nil {
			t.Fatal(err)
		}
		if kid.RequestHash != wantHash {
			t.Fatalf("child %d request_hash = %s, want %s", i, kid.RequestHash, wantHash)
		}
	}
}

// TestSubmitResumesAfterPartialChildEnqueue：父行与子 run 不在一个事务里。
// 队列满时父行保留、子 run 只入了一部分；重放同一请求按派生键幂等补齐，
// 不产生重复行——这是「部分失败」的唯一恢复口径。
func TestSubmitResumesAfterPartialChildEnqueue(t *testing.T) {
	ctx := context.Background()
	store := queue.NewMemoryParents(queue.MemoryOptions{NewID: func() string { return "parent-fixed" }})
	jobs := queue.NewMemory(queue.MemoryOptions{MaxQueueDepth: 2})
	svc := New(store, jobs)

	_, _, err := svc.SubmitBatch(ctx, "batch-key-1", batchRequest())
	if !errors.Is(err, queue.ErrQueueFull) {
		t.Fatalf("err = %v, want ErrQueueFull（4 条子 run 超过深度 2）", err)
	}
	if _, err := store.GetParent(ctx, "parent-fixed"); err != nil {
		t.Fatalf("父行必须已落（子 run 入队失败不回滚父记录）：%v", err)
	}
	if got := len(listChildren(t, jobs, "parent-fixed")); got != 2 {
		t.Fatalf("children before resume = %d, want 2", got)
	}

	// 每轮腾出一格再重放：第 0 轮补到 3 条仍会撞满，第 1 轮补齐 4 条。
	for round := 0; round < 2; round++ {
		if _, err := jobs.Claim(ctx, "w-resume", time.Minute); err != nil {
			t.Fatalf("round %d claim: %v", round, err)
		}
		_, _, err := svc.SubmitBatch(ctx, "batch-key-1", batchRequest())
		if round == 0 {
			if !errors.Is(err, queue.ErrQueueFull) {
				t.Fatalf("round 0 err = %v, want ErrQueueFull（还没补完）", err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("round 1 err = %v, want nil", err)
		}
	}

	kids := listChildren(t, jobs, "parent-fixed")
	if len(kids) != 4 {
		t.Fatalf("children after resume = %d, want 4", len(kids))
	}
	seen := make(map[string]bool, len(kids))
	for _, kid := range kids {
		if seen[kid.IdempotencyKey] {
			t.Fatalf("duplicate idempotency key %q", kid.IdempotencyKey)
		}
		seen[kid.IdempotencyKey] = true
	}
}

func listChildren(t *testing.T, jobs *queue.Memory, parentID string) []*queue.RunRecord {
	t.Helper()
	kids, err := jobs.ListChildren(context.Background(), parentID)
	if err != nil {
		t.Fatalf("ListChildren(%s): %v", parentID, err)
	}
	return kids
}

func childIDs(kids []*queue.RunRecord) []string {
	out := make([]string, len(kids))
	for i, kid := range kids {
		out[i] = kid.ID
	}
	return out
}

func ptr(v float64) *float64 { return &v }
