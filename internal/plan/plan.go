// Package plan 是 batch / walkforward 的**规划与汇总**层（X5b）。
//
// 它只做三件事：把父请求展开成完整的子 run spec（batch 的网格笛卡尔积、
// walkforward 的训练 / 验证窗口）、把子 run 的**结论**汇总成父报告、
// 给出父记录的生命周期状态。
//
// 三条边界（与 docs/ARCHITECTURE.md §9 同源）：
//   - **不执行**回测：子 run 只是完整的 RunSpec，入队与执行归 JobQueue / worker；
//   - **不解析结果内容**：汇总只吃 K9 `result_status` 与调用方给的可选 score，
//     `run` schema 只存指针，不存结果明细；
//   - **只产出身份，不写表**：`ParentPlan` 的 canonical / hash 由 `internal/parents`
//     写进 `run.parent_jobs`（裁-13），本层不碰 store。
package plan

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"tellurion-orchestration/internal/queue"
)

// DateLayout 是 RunSpec 日期字段的 wire 形态（contracts.BacktestRequest 的
// `start_date` / `end_date`）。
const DateLayout = "2006-01-02"

// ErrInvalidRequest 是所有规划期校验失败的哨兵。带 `%w` 的具体原因由调用方
// 透传；HTTP 层应把它映射成 `invalid_params`（docs/api-schema.md §2）。
var ErrInvalidRequest = errors.New("plan: invalid request")

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidRequest, fmt.Sprintf(format, args...))
}

// ChildKind 标识子 run 在父计划里的角色。
type ChildKind string

const (
	// ChildBatch 是批量扫描网格里的一个点。
	ChildBatch ChildKind = "batch"
	// ChildTrain 是前向验证窗口的训练段。
	ChildTrain ChildKind = "train"
	// ChildTest 是前向验证窗口的验证段。
	ChildTest ChildKind = "test"
)

// Child 是一条完整的子 run 计划。Index 从 1 开始（父报告按它排序）。
//
// Request 是**完整** RunSpec 的 JSON：base 的 kernel / fill_timing / seed /
// strategy_params 等字段一个都不丢，只改父计划应当改的那一轴。它可以直接交给
// queue.JobQueue.Enqueue（幂等 key 由父记录生成，不在本层）。
type Child struct {
	Kind    ChildKind
	Index   int
	Request json.RawMessage
	// Params 只对 batch 子 run 有值：本次命中的网格点（不含 base 里原有的参数）。
	Params map[string]json.RawMessage
	// Window 只对 walkforward 子 run 有值：所属窗口（含训练 / 验证区间）。
	Window *Window
}

// Window 是一个前向验证窗口（全部为交易日，边界含端点）。
type Window struct {
	Index      int // 从 1 开始
	TrainStart time.Time
	TrainEnd   time.Time
	TestStart  time.Time
	TestEnd    time.Time
}

// RunSpecObject 把一份 RunSpec 解成顶层对象。规划只改顶层键，所以不需要知道
// BacktestRequest 的 Go 形状（契约是冻结面，本仓只透传）。
func RunSpecObject(raw json.RawMessage) (map[string]json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, invalidf("run spec is required")
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, invalidf("run spec must be a JSON object: %v", err)
	}
	if obj == nil {
		return nil, invalidf("run spec must be a JSON object, got null")
	}
	return obj, nil
}

// cloneObject 复制一层顶层键（值仍是 RawMessage，不做深拷贝——规划只替换值，
// 不修改已有值的字节）。
func cloneObject(in map[string]json.RawMessage) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// marshalObject 输出稳定的 JSON 字节：encoding/json 对 map 的键按字典序编码，
// RawMessage 的值按原文（仅压缩空白）写出，因此不丢数字精度。
func marshalObject(obj map[string]json.RawMessage) (json.RawMessage, error) {
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, invalidf("run spec cannot be encoded: %v", err)
	}
	return b, nil
}

// jsonString 生成一个 JSON 字符串字面量（用于日期 / 网格值）。
func jsonString(s string) json.RawMessage {
	b, _ := json.Marshal(s)
	return b
}

// Counts 是父计划的子结论计数。四类互斥且必须加总到 Total：
// completed / invalid / failed 是已落定结论，pending 是尚未回报。
type Counts struct {
	Total     int
	Completed int
	Invalid   int
	Failed    int
	Pending   int
}

// Settled 返回已落定的子 run 数（completed + invalid + failed）。
func (c Counts) Settled() int { return c.Completed + c.Invalid + c.Failed }

// Validate 检查计数自洽：非负，且四类加总必须等于 Total。
func (c Counts) Validate() error {
	if c.Total < 0 || c.Completed < 0 || c.Invalid < 0 || c.Failed < 0 || c.Pending < 0 {
		return invalidf("counts must be non-negative: %+v", c)
	}
	if c.Settled()+c.Pending != c.Total {
		return invalidf("counts do not add up: %+v", c)
	}
	return nil
}

// ParentStatus 是父记录的生命周期（**不是** K9），直接复用队列的三态子集：
// `run.batch_runs.status` / `run.walk_forward_reports.status` 与
// `run.backtest_jobs.status` 是同一个词表，不另立第二套字符串。
type ParentStatus = queue.RunStatus

const (
	ParentRunning   = queue.RunRunning
	ParentCompleted = queue.RunCompleted
	ParentFailed    = queue.RunFailed
)

// ResultStatus 是 K9 三值（终态前为空）。
type ResultStatus = queue.ResultStatus

// ResolveStatus 由计数推出父状态。三条判据（docs/dataflow.md §2 第 10 步）：
//
//  1. 还有子 run 未回报 → running（不能提前宣布完成）；
//  2. 任一子 run failed → failed（父状态**不吞**子失败；没有
//     `completed_with_failures` 这种第七态——加终态 = MAJOR）；
//  3. 否则 completed。`invalid` 是「跑完但结论无效」，不是执行失败，
//     它照常计入计数、但不会把父记录拖成 failed。
func ResolveStatus(c Counts) (ParentStatus, error) {
	if err := c.Validate(); err != nil {
		return "", err
	}
	if c.Total == 0 {
		return "", invalidf("cannot summarise an empty plan")
	}
	if c.Pending > 0 {
		return ParentRunning, nil
	}
	if c.Failed > 0 {
		return ParentFailed, nil
	}
	return ParentCompleted, nil
}
