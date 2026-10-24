package plan_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"testing"

	"tellurion-orchestration/internal/plan"
	"tellurion-orchestration/internal/plansuite"
)

const batchBase = `{
	"strategy": "momentum",
	"stock_pool": ["600000.SH", "000001.SZ"],
	"start_date": "2024-01-02",
	"end_date": "2024-12-31",
	"initial_capital": 1000000,
	"kernel": "go",
	"fill_timing": "next_open",
	"seed": 42,
	"strategy_params": {"lookback": 20, "threshold": 0.5, "smoothing": 5}
}`

func batchSubject() plansuite.Subject {
	return plansuite.Subject{
		Name:     "Batch",
		Base:     json.RawMessage(batchBase),
		AxisKeys: []string{"strategy_params"},
		UnitSize: 1,
		Plan: func(t *testing.T) []plan.Child {
			t.Helper()
			children, err := plan.ExpandBatch(plan.BatchRequest{
				BaseRunSpec: json.RawMessage(batchBase),
				Grid: map[string][]json.RawMessage{
					"lookback":  {json.RawMessage(`10`), json.RawMessage(`20`)},
					"threshold": {json.RawMessage(`0.1`), json.RawMessage(`0.2`)},
				},
			})
			if err != nil {
				t.Fatalf("ExpandBatch: %v", err)
			}
			return children
		},
		Summarize: func(t *testing.T, outcomes []plansuite.Outcome) (plan.Counts, plan.ParentStatus) {
			t.Helper()
			tasks := make([]plan.TaskOutcome, len(outcomes))
			for i, o := range outcomes {
				tasks[i] = plan.TaskOutcome{
					RunID:        o.RunID,
					ResultStatus: o.ResultStatus,
					Error:        o.Error,
					DurationMs:   o.DurationMs,
					Score:        o.Score,
				}
			}
			summary, err := plan.AggregateBatch(tasks, len(tasks))
			if err != nil {
				t.Fatalf("AggregateBatch: %v", err)
			}
			return summary.Counts, summary.Status
		},
	}
}

func TestBatchSatisfiesSharedPlanSuite(t *testing.T) {
	plansuite.Run(t, batchSubject())
}

func TestExpandBatchMergesGridAndKeepsBaseParams(t *testing.T) {
	children, err := plan.ExpandBatch(plan.BatchRequest{
		BaseRunSpec: json.RawMessage(batchBase),
		Grid: map[string][]json.RawMessage{
			"lookback":  {json.RawMessage(`10`)},
			"threshold": {json.RawMessage(`0.2`), json.RawMessage(`0.3`)},
		},
	})
	if err != nil {
		t.Fatalf("ExpandBatch: %v", err)
	}
	if len(children) != 2 {
		t.Fatalf("children = %d, want 2", len(children))
	}

	// 参数名字典序：lookback 是外层，threshold 变化最快。
	want := []struct {
		lookback  float64
		threshold float64
	}{
		{10, 0.2},
		{10, 0.3},
	}
	for i, w := range want {
		params := paramsOf(t, children[i].Request)
		if params["lookback"] != w.lookback {
			t.Fatalf("child %d lookback = %v, want %v", i+1, params["lookback"], w.lookback)
		}
		if params["threshold"] != w.threshold {
			t.Fatalf("child %d threshold = %v, want %v", i+1, params["threshold"], w.threshold)
		}
		if params["smoothing"] != float64(5) {
			t.Fatalf("child %d dropped base param smoothing: %v", i+1, params["smoothing"])
		}
		if children[i].Kind != plan.ChildBatch || children[i].Index != i+1 {
			t.Fatalf("child %d identity = %s/%d", i+1, children[i].Kind, children[i].Index)
		}
		if children[i].Window != nil {
			t.Fatalf("child %d has a walk-forward window", i+1)
		}
	}
}

func TestExpandBatchRejectsInvalidRequests(t *testing.T) {
	cases := []struct {
		name string
		req  plan.BatchRequest
	}{
		{
			name: "empty grid",
			req:  plan.BatchRequest{BaseRunSpec: json.RawMessage(batchBase), Grid: nil},
		},
		{
			name: "empty values",
			req: plan.BatchRequest{
				BaseRunSpec: json.RawMessage(batchBase),
				Grid:        map[string][]json.RawMessage{"lookback": {}},
			},
		},
		{
			name: "non scalar value",
			req: plan.BatchRequest{
				BaseRunSpec: json.RawMessage(batchBase),
				Grid:        map[string][]json.RawMessage{"lookback": {json.RawMessage(`{"nested":1}`)}},
			},
		},
		{
			name: "array value",
			req: plan.BatchRequest{
				BaseRunSpec: json.RawMessage(batchBase),
				Grid:        map[string][]json.RawMessage{"lookback": {json.RawMessage(`[1,2]`)}},
			},
		},
		{
			name: "trailing content in value",
			req: plan.BatchRequest{
				BaseRunSpec: json.RawMessage(batchBase),
				Grid:        map[string][]json.RawMessage{"lookback": {json.RawMessage(`1 2`)}},
			},
		},
		{
			name: "blank parameter name",
			req: plan.BatchRequest{
				BaseRunSpec: json.RawMessage(batchBase),
				Grid:        map[string][]json.RawMessage{"  ": {json.RawMessage(`1`)}},
			},
		},
		{
			name: "base is an array",
			req: plan.BatchRequest{
				BaseRunSpec: json.RawMessage(`[]`),
				Grid:        map[string][]json.RawMessage{"lookback": {json.RawMessage(`10`)}},
			},
		},
		{
			name: "base strategy_params is an array",
			req: plan.BatchRequest{
				BaseRunSpec: json.RawMessage(`{"strategy_params":[1,2]}`),
				Grid:        map[string][]json.RawMessage{"lookback": {json.RawMessage(`10`)}},
			},
		},
		{
			name: "base missing",
			req:  plan.BatchRequest{Grid: map[string][]json.RawMessage{"lookback": {json.RawMessage(`10`)}}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := plan.ExpandBatch(c.req)
			if !errors.Is(err, plan.ErrInvalidRequest) {
				t.Fatalf("ExpandBatch = %v, want ErrInvalidRequest", err)
			}
		})
	}
}

func TestExpandBatchRejectsOverLimit(t *testing.T) {
	values := make([]json.RawMessage, plan.MaxBatchTasks+1)
	for i := range values {
		b, _ := json.Marshal(i)
		values[i] = b
	}
	_, err := plan.ExpandBatch(plan.BatchRequest{
		BaseRunSpec: json.RawMessage(batchBase),
		Grid:        map[string][]json.RawMessage{"lookback": values},
	})
	if !errors.Is(err, plan.ErrInvalidRequest) {
		t.Fatalf("ExpandBatch = %v, want ErrInvalidRequest（上限 %d）", err, plan.MaxBatchTasks)
	}
}

func TestExpandBatchDoesNotMutateBase(t *testing.T) {
	before := []byte(batchBase)
	_, err := plan.ExpandBatch(plan.BatchRequest{
		BaseRunSpec: json.RawMessage(batchBase),
		Grid:        map[string][]json.RawMessage{"lookback": {json.RawMessage(`10`)}},
	})
	if err != nil {
		t.Fatalf("ExpandBatch: %v", err)
	}
	if !bytes.Equal(before, []byte(batchBase)) {
		t.Fatal("ExpandBatch mutated the caller's base spec")
	}
}

func TestAggregateBatchRanksOnlyCompletedWithScore(t *testing.T) {
	score := func(v float64) *float64 { return &v }
	summary, err := plan.AggregateBatch([]plan.TaskOutcome{
		{RunID: "run-a", ResultStatus: "completed", Score: score(1.5), DurationMs: 10},
		{RunID: "run-b", ResultStatus: "completed", Score: score(3.0), DurationMs: 20},
		{RunID: "run-c", ResultStatus: "invalid", Score: score(99), DurationMs: 30},
		{RunID: "run-d", ResultStatus: "failed", Error: "boom", DurationMs: 40},
		{RunID: "run-e", ResultStatus: ""},
		{RunID: "run-f", ResultStatus: "completed"}, // 无 score，不参与排名
	}, 6)
	if err != nil {
		t.Fatalf("AggregateBatch: %v", err)
	}
	if summary.Counts != (plan.Counts{Total: 6, Completed: 3, Invalid: 1, Failed: 1, Pending: 1}) {
		t.Fatalf("counts = %+v", summary.Counts)
	}
	if summary.BestRunID != "run-b" || summary.WorstRunID != "run-a" {
		t.Fatalf("best/worst = %q/%q, want run-b/run-a", summary.BestRunID, summary.WorstRunID)
	}
	ranks := map[string]int{}
	for _, task := range summary.Tasks {
		ranks[task.RunID] = task.Rank
	}
	wantRanks := map[string]int{"run-a": 2, "run-b": 1, "run-c": 0, "run-d": 0, "run-e": 0, "run-f": 0}
	for id, want := range wantRanks {
		if ranks[id] != want {
			t.Fatalf("%s rank = %d, want %d（invalid/failed 保留但 rank=0）", id, ranks[id], want)
		}
	}
	if summary.Tasks[2].Rank != 0 || summary.Tasks[2].ResultStatus != "invalid" {
		t.Fatalf("invalid task was not preserved: %+v", summary.Tasks[2])
	}
	// 还有一个子 run 未回报：父记录不能提前落 failed / completed。
	if summary.Status != plan.ParentRunning {
		t.Fatalf("status = %q, want running", summary.Status)
	}
}

func TestAggregateBatchTieBreaksByRunID(t *testing.T) {
	score := 1.0
	summary, err := plan.AggregateBatch([]plan.TaskOutcome{
		{RunID: "run-z", ResultStatus: "completed", Score: &score},
		{RunID: "run-a", ResultStatus: "completed", Score: &score},
	}, 2)
	if err != nil {
		t.Fatalf("AggregateBatch: %v", err)
	}
	if summary.Tasks[0].Rank != 2 || summary.Tasks[1].Rank != 1 {
		t.Fatalf("ranks = %d/%d, want 2/1（同分按 run_id 升序）", summary.Tasks[0].Rank, summary.Tasks[1].Rank)
	}
	if summary.BestRunID != "run-a" || summary.WorstRunID != "run-z" {
		t.Fatalf("best/worst = %q/%q", summary.BestRunID, summary.WorstRunID)
	}
}

func TestAggregateBatchRejectsDuplicateRunID(t *testing.T) {
	_, err := plan.AggregateBatch([]plan.TaskOutcome{
		{RunID: "run-a", ResultStatus: "completed"},
		{RunID: "run-a", ResultStatus: "failed", Error: "boom"},
	}, 2)
	if !errors.Is(err, plan.ErrInvalidRequest) {
		t.Fatalf("AggregateBatch = %v, want ErrInvalidRequest", err)
	}
}

func TestAggregateBatchRejectsNonFiniteScore(t *testing.T) {
	nan := math.NaN()
	_, err := plan.AggregateBatch([]plan.TaskOutcome{{RunID: "run-a", ResultStatus: "completed", Score: &nan}}, 1)
	if !errors.Is(err, plan.ErrInvalidRequest) {
		t.Fatalf("AggregateBatch(NaN score) = %v, want ErrInvalidRequest", err)
	}
}

func TestAggregateBatchRejectsEmptyInput(t *testing.T) {
	if _, err := plan.AggregateBatch(nil, 0); !errors.Is(err, plan.ErrInvalidRequest) {
		t.Fatalf("AggregateBatch(nil, 0) = %v, want ErrInvalidRequest", err)
	}
	// 计划 2 条、只回报 1 条 → 拒绝，而不是把「漏报」算成完成。
	if _, err := plan.AggregateBatch([]plan.TaskOutcome{{RunID: "run-a", ResultStatus: "completed"}}, 2); !errors.Is(err, plan.ErrInvalidRequest) {
		t.Fatalf("AggregateBatch(1 outcome for 2 tasks) = %v, want ErrInvalidRequest", err)
	}
}

func paramsOf(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var spec struct {
		StrategyParams map[string]any `json:"strategy_params"`
	}
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("child spec is invalid: %v", err)
	}
	return spec.StrategyParams
}
