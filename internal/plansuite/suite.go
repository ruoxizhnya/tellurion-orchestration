// Package plansuite 是 batch / walkforward 规划与汇总的**共享语义套件**（X5b）。
//
// 同一批用例同时跑在两个 Subject 上：batch 的网格展开、walkforward 的窗口切分，
// 以及两者共用的三值计数 / 父状态判定。为什么共享：docs/dataflow.md §2 第 10 步
// 的判据是「子 run 的 result_status 逐条可见；invalid / failed 显式计数，不静默
// 丢弃」——**只有一处断言**才能证明两种父记录没有各写一套汇总口径。分别写两份
// 测试只会证明两份测试各自成立。
//
// 它是普通 .go 文件而不是 _test.go，因为 plan 包的测试要引用它（与 qtest 同理）。
// 约束两条：
//   - 只依赖 testing 与 plan / queue 的导出面；
//   - 不假设子 run 的执行顺序：汇总输入按 Child.Index 排列，结论内容由用例给定。
package plansuite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"tellurion-orchestration/internal/plan"
	"tellurion-orchestration/internal/queue"
)

// Outcome 是套件用的通用子结论。两种父各自映射到自己的 DTO：
// batch → plan.TaskOutcome，walkforward → plan.LegOutcome。
type Outcome struct {
	RunID        string
	ResultStatus queue.ResultStatus // 空 = 尚未回报
	Error        string
	DurationMs   int64
	Score        *float64
}

// Subject 描述一种父计划（batch / walkforward）。
type Subject struct {
	// Name 出现在子测试名里，`go test -v` 能直接看出是哪一侧红了。
	Name string
	// Base 是父计划的 base_run_spec（两种父共同的输入身份）。
	Base json.RawMessage
	// Plan 产出固定的子 run 计划。同一调用两次必须逐字节一致。
	Plan func(t *testing.T) []plan.Child
	// AxisKeys 是父计划**允许改动**的顶层键：
	// batch = strategy_params；walkforward = start_date / end_date。
	AxisKeys []string
	// UnitSize 是一个汇总单元里的子 run 数：batch=1（一个网格点），
	// walkforward=2（train + test）。套件按它构造「单元级」结论。
	UnitSize int
	// Summarize 把按 Child.Index 顺序给出的子结论汇总成父报告。
	Summarize func(t *testing.T, outcomes []Outcome) (plan.Counts, plan.ParentStatus)
}

// Run 把共享用例跑在给定 Subject 上。
func Run(t *testing.T, s Subject) {
	t.Helper()
	if s.UnitSize <= 0 {
		t.Fatalf("plansuite: %s.UnitSize must be positive", s.Name)
	}

	cases := []struct {
		Name string
		Run  func(t *testing.T, s Subject)
	}{
		{"PlanIsDeterministic", planIsDeterministic},
		{"EveryChildIsACompleteRunSpec", everyChildIsACompleteRunSpec},
		{"SummaryCountsThreeValuesWithoutCollapsing", summaryCountsThreeValues},
		{"InvalidIsNotAFailure", invalidIsNotAFailure},
		{"AnyFailureFailsTheParent", anyFailureFailsTheParent},
		{"AllFailuresAreNotAnEmptySuccess", allFailuresAreNotAnEmptySuccess},
		{"PendingKeepsTheParentRunning", pendingKeepsTheParentRunning},
	}
	for _, c := range cases {
		t.Run(s.Name+"/"+c.Name, func(t *testing.T) {
			c.Run(t, s)
		})
	}
}

func planIsDeterministic(t *testing.T, s Subject) {
	t.Helper()
	first := s.Plan(t)
	second := s.Plan(t)
	if len(first) != len(second) {
		t.Fatalf("plan sizes differ: %d vs %d", len(first), len(second))
	}
	for i := range first {
		a, b := first[i], second[i]
		if a.Kind != b.Kind || a.Index != b.Index {
			t.Fatalf("child %d identity differs: %s/%d vs %s/%d", i+1, a.Kind, a.Index, b.Kind, b.Index)
		}
		if !bytes.Equal(a.Request, b.Request) {
			t.Fatalf("child %d request is not byte-stable:\n%s\n%s", i+1, a.Request, b.Request)
		}
		if len(a.Params) != len(b.Params) {
			t.Fatalf("child %d params differ in size", i+1)
		}
		for k, v := range a.Params {
			if !bytes.Equal(v, b.Params[k]) {
				t.Fatalf("child %d param %q differs: %s vs %s", i+1, k, v, b.Params[k])
			}
		}
		if (a.Window == nil) != (b.Window == nil) {
			t.Fatalf("child %d window presence differs", i+1)
		}
		if a.Window != nil && *a.Window != *b.Window {
			t.Fatalf("child %d window differs: %+v vs %+v", i+1, *a.Window, *b.Window)
		}
	}
}

func everyChildIsACompleteRunSpec(t *testing.T, s Subject) {
	t.Helper()
	base, err := plan.RunSpecObject(s.Base)
	if err != nil {
		t.Fatalf("fixture base is invalid: %v", err)
	}
	axis := make(map[string]bool, len(s.AxisKeys))
	for _, k := range s.AxisKeys {
		axis[k] = true
	}

	children := s.Plan(t)
	if len(children) == 0 {
		t.Fatal("plan produced no children")
	}
	for _, child := range children {
		obj, err := plan.RunSpecObject(child.Request)
		if err != nil {
			t.Fatalf("child %d is not an object: %v", child.Index, err)
		}
		for k, want := range base {
			if axis[k] {
				continue
			}
			got, ok := obj[k]
			if !ok {
				t.Fatalf("child %d dropped base field %q", child.Index, k)
			}
			if !jsonEqual(got, want) {
				t.Fatalf("child %d changed base field %q: %s vs %s", child.Index, k, got, want)
			}
		}
		for k := range obj {
			if _, wasInBase := base[k]; !wasInBase && !axis[k] {
				t.Fatalf("child %d invented top-level field %q", child.Index, k)
			}
		}
	}
}

func summaryCountsThreeValues(t *testing.T, s Subject) {
	t.Helper()
	outcomes := outcomesFor(s.UnitSize, []unitOutcome{unitCompleted, unitInvalid, unitFailed, unitPending})
	counts, status := s.Summarize(t, outcomes)
	assertCounts(t, counts, plan.Counts{Total: 4, Completed: 1, Invalid: 1, Failed: 1, Pending: 1})
	if status != plan.ParentRunning {
		t.Fatalf("status = %q, want running（还有单元未回报）", status)
	}
}

func invalidIsNotAFailure(t *testing.T, s Subject) {
	t.Helper()
	outcomes := outcomesFor(s.UnitSize, []unitOutcome{unitCompleted, unitInvalid, unitInvalid})
	counts, status := s.Summarize(t, outcomes)
	assertCounts(t, counts, plan.Counts{Total: 3, Completed: 1, Invalid: 2})
	if status != plan.ParentCompleted {
		t.Fatalf("status = %q, want completed（invalid 是结论，不是执行失败）", status)
	}
}

func anyFailureFailsTheParent(t *testing.T, s Subject) {
	t.Helper()
	outcomes := outcomesFor(s.UnitSize, []unitOutcome{unitCompleted, unitCompleted, unitCompleted, unitInvalid, unitFailed})
	counts, status := s.Summarize(t, outcomes)
	assertCounts(t, counts, plan.Counts{Total: 5, Completed: 3, Invalid: 1, Failed: 1})
	if status != plan.ParentFailed {
		t.Fatalf("status = %q, want failed（父状态不吞子失败）", status)
	}
}

func allFailuresAreNotAnEmptySuccess(t *testing.T, s Subject) {
	t.Helper()
	outcomes := outcomesFor(s.UnitSize, []unitOutcome{unitFailed, unitFailed, unitFailed})
	counts, status := s.Summarize(t, outcomes)
	assertCounts(t, counts, plan.Counts{Total: 3, Failed: 3})
	if status != plan.ParentFailed {
		t.Fatalf("status = %q, want failed（全部失败不是「成功的空报告」）", status)
	}
}

func pendingKeepsTheParentRunning(t *testing.T, s Subject) {
	t.Helper()
	outcomes := outcomesFor(s.UnitSize, []unitOutcome{unitCompleted, unitCompleted, unitPending})
	counts, status := s.Summarize(t, outcomes)
	assertCounts(t, counts, plan.Counts{Total: 3, Completed: 2, Pending: 1})
	if status != plan.ParentRunning {
		t.Fatalf("status = %q, want running（未回报不能提前宣布完成）", status)
	}
}

// unitOutcome 是「一个汇总单元」的期望结论。walkforward 的一个单元有两段，
// 套件会把 failed / invalid 放在单元的第一段，第二段置 completed——用来证明
// 「单段失败 = 整窗失败」，而不是把两段各自计数。
type unitOutcome int

const (
	unitCompleted unitOutcome = iota
	unitInvalid
	unitFailed
	unitPending
)

// outcomesFor 按 UnitSize 展开单元结论，RunID 全局唯一且稳定。
func outcomesFor(unitSize int, units []unitOutcome) []Outcome {
	out := make([]Outcome, 0, len(units)*unitSize)
	for u, want := range units {
		for leg := 0; leg < unitSize; leg++ {
			o := Outcome{
				RunID:      fmt.Sprintf("run-%03d-%d", u+1, leg+1),
				DurationMs: int64((u + 1) * (leg + 1)),
			}
			switch want {
			case unitPending:
				o.ResultStatus = ""
			case unitFailed:
				if leg == 0 {
					o.ResultStatus = queue.ResultFailed
					o.Error = "engine failed"
				} else {
					o.ResultStatus = queue.ResultCompleted
				}
			case unitInvalid:
				if leg == 0 {
					o.ResultStatus = queue.ResultInvalid
				} else {
					o.ResultStatus = queue.ResultCompleted
				}
			default:
				o.ResultStatus = queue.ResultCompleted
			}
			out = append(out, o)
		}
	}
	return out
}

func assertCounts(t *testing.T, got, want plan.Counts) {
	t.Helper()
	if got != want {
		t.Fatalf("counts = %+v, want %+v", got, want)
	}
}

// jsonEqual 比较两份 JSON 的语义（对象键序无关），避免 RawMessage 的空白 / 键序
// 差异造成假红。
func jsonEqual(a, b json.RawMessage) bool {
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &bv); err != nil {
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
