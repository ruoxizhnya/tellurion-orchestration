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
	svc := New(store)

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
	svc := New(store)

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
	svc := New(store)
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
	svc := New(store)
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
	svc := New(store)
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
	svc := New(queue.NewMemoryParents())
	_, _, err := svc.SubmitBatch(ctx, "batch-key-1", plan.BatchRequest{BaseRunSpec: json.RawMessage(batchBase)})
	if !errors.Is(err, plan.ErrInvalidRequest) {
		t.Fatalf("err = %v, want plan.ErrInvalidRequest（空网格必须拒绝）", err)
	}
}

func ptr(v float64) *float64 { return &v }
