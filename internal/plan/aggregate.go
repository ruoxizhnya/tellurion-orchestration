package plan

import (
	"math"
	"sort"
	"strings"

	"tellurion-orchestration/internal/queue"
)

// TaskOutcome 是一条批量子 run 的结论（worker 回报的形状）。
//
// ResultStatus 为空 = 尚未回报（父记录 running）。Score 由执行侧给出——
// 本仓**不解析结果内容**，所以评分不能在这里从 Sharpe / 收益里现算。
type TaskOutcome struct {
	RunID        string
	ResultStatus ResultStatus
	Error        string
	DurationMs   int64
	Score        *float64
}

// TaskResult 是父报告里的逐条子结果（只放指针与结论，不内嵌结果内容）。
type TaskResult struct {
	RunID        string       `json:"run_id"`
	ResultStatus ResultStatus `json:"result_status,omitempty"`
	Error        string       `json:"error,omitempty"`
	DurationMs   int64        `json:"duration_ms"`
	Score        *float64     `json:"score,omitempty"`
	Rank         int          `json:"rank"`
}

// BatchSummary 是批量父记录的汇总。
type BatchSummary struct {
	Counts     Counts
	Status     ParentStatus
	BestRunID  string
	WorstRunID string
	Tasks      []TaskResult
}

// AggregateBatch 汇总批量子 run。
//
// 三条判据（docs/ARCHITECTURE.md §9.3 BatchSummary / BatchTaskResult）：
//   - 三值分开计数：`invalid` 不得算成 `completed`，也不得算成 `failed`；
//   - 只有 `completed` 且**给出 score** 的子 run 参与排名；`invalid` / `failed`
//     保留在列表里但 rank=0（否则「无效结果拿第一」会重现 core 的 F5 缺陷）；
//   - best / worst 只在已排名的 completed 里选。
//
// 输入顺序 = 父计划的子 run 顺序（`Child.Index`），输出保持同一顺序；
// 排名由 score 决定，不由输入顺序决定。plannedTotal 是规划期的任务数：
// 调用方必须为**每个**子 run 交一条 outcome（未回报的用空 ResultStatus），
// 少交一条即拒绝——否则「漏报」会被当成「已经全部完成」。
func AggregateBatch(outcomes []TaskOutcome, plannedTotal int) (BatchSummary, error) {
	if plannedTotal <= 0 {
		return BatchSummary{}, invalidf("plannedTotal must be positive")
	}
	if len(outcomes) != plannedTotal {
		return BatchSummary{}, invalidf("got %d outcomes for %d planned tasks", len(outcomes), plannedTotal)
	}

	tasks := make([]TaskResult, len(outcomes))
	counts := Counts{Total: plannedTotal}
	seen := make(map[string]int, len(outcomes))
	for i, o := range outcomes {
		if o.RunID == "" {
			return BatchSummary{}, invalidf("task %d: run_id is required", i+1)
		}
		if prev, dup := seen[o.RunID]; dup {
			return BatchSummary{}, invalidf("task %d: duplicate run_id %q (first at task %d)", i+1, o.RunID, prev+1)
		}
		seen[o.RunID] = i

		if o.ResultStatus != "" && !queue.ValidResultStatus(o.ResultStatus) {
			return BatchSummary{}, invalidf("task %d: unknown result_status %q", i+1, o.ResultStatus)
		}
		var score *float64
		if o.Score != nil {
			if math.IsNaN(*o.Score) || math.IsInf(*o.Score, 0) {
				return BatchSummary{}, invalidf("task %d: score must be finite", i+1)
			}
			v := *o.Score
			score = &v
		}
		switch o.ResultStatus {
		case queue.ResultCompleted:
			counts.Completed++
		case queue.ResultInvalid:
			counts.Invalid++
		case queue.ResultFailed:
			counts.Failed++
		default:
			counts.Pending++
		}

		tasks[i] = TaskResult{
			RunID:        o.RunID,
			ResultStatus: o.ResultStatus,
			Error:        o.Error,
			DurationMs:   o.DurationMs,
			Score:        score,
		}
	}

	status, err := ResolveStatus(counts)
	if err != nil {
		return BatchSummary{}, err
	}

	// 排名：score 降序；同分按 run_id 升序（确定性 tie-break）。
	ranked := make([]int, 0, counts.Completed)
	for i, t := range tasks {
		if t.ResultStatus == queue.ResultCompleted && t.Score != nil {
			ranked = append(ranked, i)
		}
	}
	sort.Slice(ranked, func(a, b int) bool {
		ta, tb := tasks[ranked[a]], tasks[ranked[b]]
		if *ta.Score != *tb.Score {
			return *ta.Score > *tb.Score
		}
		return ta.RunID < tb.RunID
	})

	summary := BatchSummary{Counts: counts, Status: status, Tasks: tasks}
	for rank, idx := range ranked {
		summary.Tasks[idx].Rank = rank + 1
	}
	if len(ranked) > 0 {
		summary.BestRunID = summary.Tasks[ranked[0]].RunID
		summary.WorstRunID = summary.Tasks[ranked[len(ranked)-1]].RunID
	}
	return summary, nil
}

// LegKind 是前向验证窗口里的两段。
type LegKind string

const (
	LegTrain LegKind = "train"
	LegTest  LegKind = "test"
)

// LegOutcome 是一条窗口子 run 的结论。ResultStatus 为空 = 尚未回报。
type LegOutcome struct {
	Window       int // 1-based
	Kind         LegKind
	RunID        string
	ResultStatus ResultStatus
	Error        string
	DurationMs   int64
}

// WindowStatus 是窗口级结论（两段合并后）。
type WindowStatus string

const (
	WindowPending   WindowStatus = "pending"
	WindowCompleted WindowStatus = "completed"
	WindowInvalid   WindowStatus = "invalid"
	WindowFailed    WindowStatus = "failed"
)

// WindowResult 是父报告里的一个窗口（只放 run 指针与结论，不放结果内容）。
type WindowResult struct {
	Window      int          `json:"window"`
	TrainRunID  string       `json:"train_run_id,omitempty"`
	TestRunID   string       `json:"test_run_id,omitempty"`
	TrainStatus ResultStatus `json:"train_status,omitempty"`
	TestStatus  ResultStatus `json:"test_status,omitempty"`
	Status      WindowStatus `json:"status"`
	Error       string       `json:"error,omitempty"`
	TrainMs     int64        `json:"train_duration_ms"`
	TestMs      int64        `json:"test_duration_ms"`
}

// WalkForwardSummary 是前向验证父记录的汇总。
type WalkForwardSummary struct {
	Counts  Counts
	Status  ParentStatus
	Windows []WindowResult
}

// AggregateWalkForward 把逐段结论汇总成逐窗结论与父报告。
//
// 窗口级判据（docs/dataflow.md §2 第 10 步）：
//   - 任一段未回报 → 窗口 pending（父记录 running）；
//   - 任一段 failed → 窗口 failed（**不**因为另一段成功就把窗口算成功）；
//   - 否则任一段 invalid → 窗口 invalid（跑了，但结论无效）；
//   - 两段都 completed → 窗口 completed。
//
// 部分窗口失败**显式计数**，不静默丢；父状态按 ResolveStatus 的三条判据走。
// plannedWindows 是规划期的窗口数——缺一段 / 缺整窗都算 pending，不会因为
// 少回报一个窗口就把父记录判成 completed。
func AggregateWalkForward(legs []LegOutcome, plannedWindows int) (WalkForwardSummary, error) {
	if plannedWindows <= 0 {
		return WalkForwardSummary{}, invalidf("plannedWindows must be positive")
	}

	type legKey struct {
		window int
		kind   LegKind
	}
	byKey := make(map[legKey]LegOutcome, len(legs))
	seenRun := make(map[string]int, len(legs))
	for i, leg := range legs {
		if leg.Window < 1 || leg.Window > plannedWindows {
			return WalkForwardSummary{}, invalidf("leg %d: window %d outside 1..%d", i+1, leg.Window, plannedWindows)
		}
		if leg.Kind != LegTrain && leg.Kind != LegTest {
			return WalkForwardSummary{}, invalidf("leg %d: unknown kind %q", i+1, leg.Kind)
		}
		if leg.RunID == "" {
			return WalkForwardSummary{}, invalidf("leg %d: run_id is required", i+1)
		}
		if prev, dup := seenRun[leg.RunID]; dup {
			return WalkForwardSummary{}, invalidf("leg %d: duplicate run_id %q (first at leg %d)", i+1, leg.RunID, prev+1)
		}
		seenRun[leg.RunID] = i
		if leg.ResultStatus != "" && !queue.ValidResultStatus(leg.ResultStatus) {
			return WalkForwardSummary{}, invalidf("leg %d: unknown result_status %q", i+1, leg.ResultStatus)
		}
		key := legKey{window: leg.Window, kind: leg.Kind}
		if _, dup := byKey[key]; dup {
			return WalkForwardSummary{}, invalidf("leg %d: duplicate %s leg for window %d", i+1, leg.Kind, leg.Window)
		}
		byKey[key] = leg
	}

	counts := Counts{Total: plannedWindows}
	windows := make([]WindowResult, 0, plannedWindows)
	for w := 1; w <= plannedWindows; w++ {
		train, hasTrain := byKey[legKey{window: w, kind: LegTrain}]
		test, hasTest := byKey[legKey{window: w, kind: LegTest}]

		wr := WindowResult{Window: w}
		if hasTrain {
			wr.TrainRunID, wr.TrainStatus, wr.TrainMs = train.RunID, train.ResultStatus, train.DurationMs
		}
		if hasTest {
			wr.TestRunID, wr.TestStatus, wr.TestMs = test.RunID, test.ResultStatus, test.DurationMs
		}

		switch {
		case !hasTrain || !hasTest || train.ResultStatus == "" || test.ResultStatus == "":
			wr.Status = WindowPending
			counts.Pending++
		case train.ResultStatus == queue.ResultFailed || test.ResultStatus == queue.ResultFailed:
			wr.Status = WindowFailed
			counts.Failed++
		case train.ResultStatus == queue.ResultInvalid || test.ResultStatus == queue.ResultInvalid:
			wr.Status = WindowInvalid
			counts.Invalid++
		case train.ResultStatus == queue.ResultCompleted && test.ResultStatus == queue.ResultCompleted:
			wr.Status = WindowCompleted
			counts.Completed++
		}
		wr.Error = combineLegErrors(train.Error, test.Error)
		windows = append(windows, wr)
	}

	status, err := ResolveStatus(counts)
	if err != nil {
		return WalkForwardSummary{}, err
	}
	return WalkForwardSummary{Counts: counts, Status: status, Windows: windows}, nil
}

// combineLegErrors 把两段的错误串成一条确定性的报文（train 在前）。
func combineLegErrors(trainErr, testErr string) string {
	parts := make([]string, 0, 2)
	if trainErr != "" {
		parts = append(parts, "train: "+trainErr)
	}
	if testErr != "" {
		parts = append(parts, "test: "+testErr)
	}
	return strings.Join(parts, "; ")
}
