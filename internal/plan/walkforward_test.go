package plan_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"tellurion-orchestration/internal/plan"
	"tellurion-orchestration/internal/plansuite"
)

const walkForwardBase = `{
	"strategy": "momentum",
	"stock_pool": ["600000.SH", "000001.SZ"],
	"start_date": "2024-01-02",
	"end_date": "2024-12-31",
	"initial_capital": 1000000,
	"kernel": "go",
	"fill_timing": "next_open",
	"seed": 42,
	"strategy_ref": "momentum@abc123",
	"strategy_params": {"lookback": 20}
}`

var wfConfig = plan.WindowConfig{TrainDays: 3, TestDays: 2, StepDays: 2}

// tradingDays 造一段连续「交易日」（本包不判断节假日：日历由 data 服务给）。
func tradingDays(n int) []time.Time {
	start := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	out := make([]time.Time, n)
	for i := range out {
		out[i] = start.AddDate(0, 0, i)
	}
	return out
}

func walkForwardSubject() plansuite.Subject {
	return plansuite.Subject{
		Name:     "WalkForward",
		Base:     json.RawMessage(walkForwardBase),
		AxisKeys: []string{"start_date", "end_date"},
		UnitSize: 2,
		Plan: func(t *testing.T) []plan.Child {
			t.Helper()
			children, err := plan.ExpandWalkForward(json.RawMessage(walkForwardBase), wfConfig, tradingDays(10))
			if err != nil {
				t.Fatalf("ExpandWalkForward: %v", err)
			}
			return children
		},
		Summarize: func(t *testing.T, outcomes []plansuite.Outcome) (plan.Counts, plan.ParentStatus) {
			t.Helper()
			if len(outcomes)%2 != 0 {
				t.Fatalf("walk-forward outcomes must pair up, got %d", len(outcomes))
			}
			legs := make([]plan.LegOutcome, len(outcomes))
			for i, o := range outcomes {
				kind := plan.LegTrain
				if i%2 == 1 {
					kind = plan.LegTest
				}
				legs[i] = plan.LegOutcome{
					Window:       i/2 + 1,
					Kind:         kind,
					RunID:        o.RunID,
					ResultStatus: o.ResultStatus,
					Error:        o.Error,
					DurationMs:   o.DurationMs,
				}
			}
			summary, err := plan.AggregateWalkForward(legs, len(outcomes)/2)
			if err != nil {
				t.Fatalf("AggregateWalkForward: %v", err)
			}
			return summary.Counts, summary.Status
		},
	}
}

func TestWalkForwardSatisfiesSharedPlanSuite(t *testing.T) {
	plansuite.Run(t, walkForwardSubject())
}

func TestSplitWindowsExactBoundaries(t *testing.T) {
	days := tradingDays(10)
	windows, err := plan.SplitWindows(days, wfConfig)
	if err != nil {
		t.Fatalf("SplitWindows: %v", err)
	}
	// start=0,2,4；start=6 时 6+5 > 10，不再生成。
	if len(windows) != 3 {
		t.Fatalf("windows = %d, want 3", len(windows))
	}
	want := []struct {
		trainStart, trainEnd, testStart, testEnd int
	}{
		{0, 2, 3, 4},
		{2, 4, 5, 6},
		{4, 6, 7, 8},
	}
	for i, w := range want {
		win := windows[i]
		if win.Index != i+1 {
			t.Fatalf("window %d index = %d", i+1, win.Index)
		}
		if !win.TrainStart.Equal(days[w.trainStart]) || !win.TrainEnd.Equal(days[w.trainEnd]) {
			t.Fatalf("window %d train = %s..%s", i+1, win.TrainStart, win.TrainEnd)
		}
		if !win.TestStart.Equal(days[w.testStart]) || !win.TestEnd.Equal(days[w.testEnd]) {
			t.Fatalf("window %d test = %s..%s", i+1, win.TestStart, win.TestEnd)
		}
	}
}

func TestSplitWindowsMinTrainDefaultsToHalf(t *testing.T) {
	cfg, err := (plan.WindowConfig{TrainDays: 4, TestDays: 2, StepDays: 2}).Normalize()
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if cfg.MinTrainDays != 2 {
		t.Fatalf("MinTrainDays = %d, want 2（train_days/2）", cfg.MinTrainDays)
	}
	if _, err := (plan.WindowConfig{TrainDays: 3, TestDays: 2, StepDays: 2, MinTrainDays: 4}).Normalize(); !errors.Is(err, plan.ErrInvalidRequest) {
		t.Fatalf("min_train_days > train_days = %v, want ErrInvalidRequest", err)
	}
}

func TestSplitWindowsHugeStepDoesNotOverflow(t *testing.T) {
	windows, err := plan.SplitWindows(tradingDays(10), plan.WindowConfig{
		TrainDays: 3,
		TestDays:  2,
		StepDays:  math.MaxInt,
	})
	if err != nil {
		t.Fatalf("SplitWindows: %v", err)
	}
	if len(windows) != 1 {
		t.Fatalf("windows = %d, want 1（step 超过日历长度只产生一个窗口）", len(windows))
	}
}

func TestSplitWindowsRejectsBadInput(t *testing.T) {
	cases := []struct {
		name string
		days []time.Time
		cfg  plan.WindowConfig
	}{
		{"zero step", tradingDays(10), plan.WindowConfig{TrainDays: 3, TestDays: 2}},
		{"insufficient days", tradingDays(4), wfConfig},
		{"unsorted calendar", []time.Time{tradingDays(2)[1], tradingDays(2)[0]}, plan.WindowConfig{TrainDays: 1, TestDays: 1, StepDays: 1}},
		{"duplicate calendar", []time.Time{tradingDays(1)[0], tradingDays(1)[0]}, plan.WindowConfig{TrainDays: 1, TestDays: 1, StepDays: 1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := plan.SplitWindows(c.days, c.cfg); !errors.Is(err, plan.ErrInvalidRequest) {
				t.Fatalf("SplitWindows = %v, want ErrInvalidRequest", err)
			}
		})
	}
}

func TestExpandWalkForwardDerivesDatesOnly(t *testing.T) {
	children, err := plan.ExpandWalkForward(json.RawMessage(walkForwardBase), wfConfig, tradingDays(10))
	if err != nil {
		t.Fatalf("ExpandWalkForward: %v", err)
	}
	if len(children) != 6 {
		t.Fatalf("children = %d, want 6（3 窗 × 2 段）", len(children))
	}
	base := specMap(t, json.RawMessage(walkForwardBase))
	for i, child := range children {
		if child.Window == nil {
			t.Fatalf("child %d has no window", i+1)
		}
		obj := specMap(t, child.Request)
		// 身份字段原样保留（窗口只改日期区间）。
		for _, key := range []string{"strategy", "stock_pool", "initial_capital", "kernel", "fill_timing", "seed", "strategy_ref", "strategy_params"} {
			if !jsonEqual(obj[key], base[key]) {
				t.Fatalf("child %d changed %s: %s vs %s", i+1, key, obj[key], base[key])
			}
		}
		switch i % 2 {
		case 0:
			if child.Kind != plan.ChildTrain {
				t.Fatalf("child %d kind = %s, want train", i+1, child.Kind)
			}
			if !jsonEqual(obj["start_date"], json.RawMessage(`"`+child.Window.TrainStart.Format(plan.DateLayout)+`"`)) {
				t.Fatalf("child %d train start = %s", i+1, obj["start_date"])
			}
			if !jsonEqual(obj["end_date"], json.RawMessage(`"`+child.Window.TrainEnd.Format(plan.DateLayout)+`"`)) {
				t.Fatalf("child %d train end = %s", i+1, obj["end_date"])
			}
		case 1:
			if child.Kind != plan.ChildTest {
				t.Fatalf("child %d kind = %s, want test", i+1, child.Kind)
			}
			if !jsonEqual(obj["start_date"], json.RawMessage(`"`+child.Window.TestStart.Format(plan.DateLayout)+`"`)) {
				t.Fatalf("child %d test start = %s", i+1, obj["start_date"])
			}
			if !jsonEqual(obj["end_date"], json.RawMessage(`"`+child.Window.TestEnd.Format(plan.DateLayout)+`"`)) {
				t.Fatalf("child %d test end = %s", i+1, obj["end_date"])
			}
		}
	}
}

func TestAggregateWalkForwardTestLegFailureFailsWindow(t *testing.T) {
	summary, err := plan.AggregateWalkForward([]plan.LegOutcome{
		{Window: 1, Kind: plan.LegTrain, RunID: "train-1", ResultStatus: "completed"},
		{Window: 1, Kind: plan.LegTest, RunID: "test-1", ResultStatus: "failed", Error: "engine failed"},
	}, 1)
	if err != nil {
		t.Fatalf("AggregateWalkForward: %v", err)
	}
	if summary.Windows[0].Status != plan.WindowFailed {
		t.Fatalf("window status = %q, want failed（一段失败 = 整窗失败）", summary.Windows[0].Status)
	}
	if summary.Windows[0].Error != "test: engine failed" {
		t.Fatalf("window error = %q", summary.Windows[0].Error)
	}
	if summary.Counts != (plan.Counts{Total: 1, Failed: 1}) || summary.Status != plan.ParentFailed {
		t.Fatalf("summary = %+v/%q, want 1 failed", summary.Counts, summary.Status)
	}
}

func TestAggregateWalkForwardMissingLegIsPending(t *testing.T) {
	summary, err := plan.AggregateWalkForward([]plan.LegOutcome{
		{Window: 1, Kind: plan.LegTrain, RunID: "train-1", ResultStatus: "completed"},
	}, 2)
	if err != nil {
		t.Fatalf("AggregateWalkForward: %v", err)
	}
	// 窗口 1 缺 test 段、窗口 2 整窗未回报：两个窗口都还没落定。
	if summary.Counts != (plan.Counts{Total: 2, Pending: 2}) {
		t.Fatalf("counts = %+v, want 2 pending", summary.Counts)
	}
	if summary.Status != plan.ParentRunning {
		t.Fatalf("status = %q, want running", summary.Status)
	}
	if summary.Windows[0].Status != plan.WindowPending || summary.Windows[0].TestRunID != "" {
		t.Fatalf("window 1 = %+v, want pending with test missing", summary.Windows[0])
	}
}

func TestAggregateWalkForwardRejectsDuplicatesAndRanges(t *testing.T) {
	cases := []struct {
		name string
		legs []plan.LegOutcome
		n    int
	}{
		{
			name: "duplicate leg",
			legs: []plan.LegOutcome{
				{Window: 1, Kind: plan.LegTrain, RunID: "a", ResultStatus: "completed"},
				{Window: 1, Kind: plan.LegTrain, RunID: "b", ResultStatus: "completed"},
			},
			n: 1,
		},
		{
			name: "window out of range",
			legs: []plan.LegOutcome{{Window: 2, Kind: plan.LegTrain, RunID: "a"}},
			n:    1,
		},
		{
			name: "duplicate run id across legs",
			legs: []plan.LegOutcome{
				{Window: 1, Kind: plan.LegTrain, RunID: "same"},
				{Window: 1, Kind: plan.LegTest, RunID: "same"},
			},
			n: 1,
		},
		{name: "zero windows", legs: nil, n: 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := plan.AggregateWalkForward(c.legs, c.n); !errors.Is(err, plan.ErrInvalidRequest) {
				t.Fatalf("AggregateWalkForward = %v, want ErrInvalidRequest", err)
			}
		})
	}
}

func TestAggregateWalkForwardCombinesErrorsDeterministically(t *testing.T) {
	summary, err := plan.AggregateWalkForward([]plan.LegOutcome{
		{Window: 1, Kind: plan.LegTest, RunID: "t", ResultStatus: "failed", Error: "test boom"},
		{Window: 1, Kind: plan.LegTrain, RunID: "r", ResultStatus: "failed", Error: "train boom"},
	}, 1)
	if err != nil {
		t.Fatalf("AggregateWalkForward: %v", err)
	}
	if got := summary.Windows[0].Error; got != "train: train boom; test: test boom" {
		t.Fatalf("error = %q", got)
	}
}

func TestResolveStatusRejectsEmptyPlan(t *testing.T) {
	if _, err := plan.ResolveStatus(plan.Counts{}); !errors.Is(err, plan.ErrInvalidRequest) {
		t.Fatalf("ResolveStatus(empty) = %v, want ErrInvalidRequest", err)
	}
	if _, err := plan.ResolveStatus(plan.Counts{Total: 2, Completed: 1}); !errors.Is(err, plan.ErrInvalidRequest) {
		t.Fatalf("ResolveStatus(inconsistent) = %v, want ErrInvalidRequest", err)
	}
}

func TestWindowConfigDoesNotMutateCaller(t *testing.T) {
	cfg := plan.WindowConfig{TrainDays: 4, TestDays: 2, StepDays: 2}
	if _, err := plan.SplitWindows(tradingDays(10), cfg); err != nil {
		t.Fatalf("SplitWindows: %v", err)
	}
	if cfg.MinTrainDays != 0 {
		t.Fatalf("caller's config was mutated: MinTrainDays = %d", cfg.MinTrainDays)
	}
}

// specMap 把 RunSpec 解成顶层键（测试断言用）。
func specMap(t *testing.T, raw json.RawMessage) map[string]json.RawMessage {
	t.Helper()
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("run spec is invalid: %v", err)
	}
	return obj
}

// jsonEqual 比较两份 JSON 的语义（键序无关），供「原样保留」断言使用。
func jsonEqual(a, b json.RawMessage) bool {
	var av, bv any
	if json.Unmarshal(a, &av) != nil || json.Unmarshal(b, &bv) != nil {
		return false
	}
	ab, err := json.Marshal(av)
	if err != nil {
		return false
	}
	bb, err := json.Marshal(bv)
	if err != nil {
		return false
	}
	return bytes.Equal(ab, bb)
}
