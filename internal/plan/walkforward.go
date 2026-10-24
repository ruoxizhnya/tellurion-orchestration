package plan

import (
	"encoding/json"
	"math"
	"time"
)

// WindowConfig 是前向验证的窗口参数（wire 用 snake_case，见
// docs/ARCHITECTURE.md §9.3 的 `WalkForwardParams`）。
//
// 三个长度都是**交易日数**，不是自然日：日历由 data 服务的只读 port 提供
// （本仓不直连 `market.*`，也不自己造日历）。
type WindowConfig struct {
	TrainDays    int
	TestDays     int
	StepDays     int
	MinTrainDays int
}

// Normalize 补默认值并校验。MinTrainDays 为空（0）= TrainDays/2，
// 与 core 的 RunWalkForward 一致；StepDays 必须为正——core 里「StepDays==0
// 表示 expanding window」的分支在参数校验之后永远不可达，本仓不复制这段死代码。
func (c WindowConfig) Normalize() (WindowConfig, error) {
	if c.TrainDays <= 0 {
		return c, invalidf("train_days must be positive")
	}
	if c.TestDays <= 0 {
		return c, invalidf("test_days must be positive")
	}
	if c.StepDays <= 0 {
		return c, invalidf("step_days must be positive")
	}
	if c.MinTrainDays < 0 {
		return c, invalidf("min_train_days must not be negative")
	}
	if c.MinTrainDays == 0 {
		c.MinTrainDays = c.TrainDays / 2
	}
	if c.MinTrainDays > c.TrainDays {
		return c, invalidf("min_train_days (%d) exceeds train_days (%d)", c.MinTrainDays, c.TrainDays)
	}
	if c.TrainDays > math.MaxInt-c.TestDays {
		return c, invalidf("train_days + test_days overflows")
	}
	return c, nil
}

// SplitWindows 把交易日切成滚动窗口。
//
// 口径（与 core 的实际行为一致，而不是它注释里的 expanding 设想）：
//   - 只生成**完整**窗口：训练段 TrainDays 个交易日、验证段 TestDays 个；
//   - 窗口起点每次前进 StepDays 个交易日，允许窗口之间留空档；
//   - 训练段与验证段首尾相接、不重叠；
//   - MinTrainDays 在固定长度窗口下总是成立（训练段恒为 TrainDays）；保留字段
//     是为了与 wire 契约一致，将来扩成 expanding 窗口时它才是有效判据；
//   - 末尾不足一个完整窗口的交易日不生成半窗（partial test 会被
//     「至少一半」的阈值放大成另一种不确定性，契约没有授权它）。
//
// 日历必须严格递增（无重复、无乱序）；不满足就是上游违约，fail-loud，
// 不在这里偷偷排序——排序会掩盖 data 服务的数据缺口。
func SplitWindows(days []time.Time, cfg WindowConfig) ([]Window, error) {
	cfg, err := cfg.Normalize()
	if err != nil {
		return nil, err
	}
	normalized, err := normalizeCalendar(days)
	if err != nil {
		return nil, err
	}

	need := cfg.TrainDays + cfg.TestDays
	if len(normalized) < need {
		return nil, invalidf("insufficient trading days: need at least %d, got %d", need, len(normalized))
	}

	var out []Window
	for start := 0; start+need <= len(normalized); {
		trainEnd := start + cfg.TrainDays - 1
		testStart := trainEnd + 1
		testEnd := testStart + cfg.TestDays - 1

		out = append(out, Window{
			Index:      len(out) + 1,
			TrainStart: normalized[start],
			TrainEnd:   normalized[trainEnd],
			TestStart:  normalized[testStart],
			TestEnd:    normalized[testEnd],
		})

		// 先判再进位：step_days 是客户端可控值，直接 `start += step` 在
		// 接近 math.MaxInt 时会溢出成负数，下一次索引就 panic。
		if cfg.StepDays > len(normalized)-start {
			break
		}
		start += cfg.StepDays
	}
	if len(out) == 0 {
		return nil, invalidf("no walk-forward windows could be generated")
	}
	return out, nil
}

// ExpandWalkForward 把窗口切成子 run：每个窗口两条（train / test），
// 都从 base_run_spec **原样派生**，只替换 `start_date` / `end_date`。
//
// 不丢 kernel / fill_timing / seed / strategy_params / strategy_ref——这些是
// 同一实验的输入身份，窗口只改时间区间（docs/ARCHITECTURE.md §9.3）。
// 子 run 顺序按窗口升序、窗内 train 在前 test 在后，与父报告的行序一致。
func ExpandWalkForward(base json.RawMessage, cfg WindowConfig, days []time.Time) ([]Child, error) {
	baseObj, err := RunSpecObject(base)
	if err != nil {
		return nil, err
	}
	windows, err := SplitWindows(days, cfg)
	if err != nil {
		return nil, err
	}

	out := make([]Child, 0, len(windows)*2)
	for i := range windows {
		win := windows[i]
		train, err := deriveWindowRun(baseObj, win.TrainStart, win.TrainEnd)
		if err != nil {
			return nil, err
		}
		test, err := deriveWindowRun(baseObj, win.TestStart, win.TestEnd)
		if err != nil {
			return nil, err
		}
		// 两条子 run 各持一份窗口副本：共享指针会让调用方改一个子 run 的
		// 窗口时意外改到另一条。
		trainWin, testWin := win, win
		next := len(out) + 1
		out = append(out,
			Child{Kind: ChildTrain, Index: next, Request: train, Window: &trainWin},
			Child{Kind: ChildTest, Index: next + 1, Request: test, Window: &testWin},
		)
	}
	return out, nil
}

// deriveWindowRun 复制 base 并只替换日期两键。
func deriveWindowRun(base map[string]json.RawMessage, start, end time.Time) (json.RawMessage, error) {
	obj := cloneObject(base)
	obj["start_date"] = jsonString(start.Format(DateLayout))
	obj["end_date"] = jsonString(end.Format(DateLayout))
	return marshalObject(obj)
}

// normalizeCalendar 取每个时间戳的**日历日**（不看时区换算），校验严格递增。
//
// 用时间戳自己的年月日，而不是 `.UTC()`：data 服务若返回带偏移的午夜
// （如 `2026-01-02T00:00:00+08:00`），UTC 换算会把它变成前一天。
func normalizeCalendar(days []time.Time) ([]time.Time, error) {
	out := make([]time.Time, len(days))
	for i, d := range days {
		out[i] = time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
		if i > 0 && !out[i].After(out[i-1]) {
			return nil, invalidf("trading calendar must be strictly increasing: %s <= %s",
				out[i].Format(DateLayout), out[i-1].Format(DateLayout))
		}
	}
	return out, nil
}
