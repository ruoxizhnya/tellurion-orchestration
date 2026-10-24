package plan

// 父计划的身份与 pending 计数（裁-13，X5b）。
//
// 两种父（batch / walkforward）共用一套父记录：kind 区分形态，plan_canonical /
// plan_hash 是提交身份，三值计数与状态由 Aggregate* 汇总后写回。本文件只做
// 「规划结果 → 父记录载荷」的确定性编码，不碰表、不碰 HTTP。

import (
	"encoding/json"
	"fmt"
	"time"

	"tellurion-orchestration/internal/queue"
)

// ParentKind 直接复用队列的父种类词表（一张表、一种状态机，不另立字符串）。
type ParentKind = queue.ParentKind

const (
	// ParentBatch 是网格批量。
	ParentBatch = queue.ParentBatch
	// ParentWalkForward 是前向验证。
	ParentWalkForward = queue.ParentWalkForward
)

// ParentPlan 是一个可提交的父计划：kind + 汇总单元数 + 完整子 run 计划。
//
// Units 是**汇总单元**数（batch = 子 run 数；walkforward = 窗口数），不是
// Children 的长度：前向验证一个窗口有 train / test 两条子 run。
type ParentPlan struct {
	Kind     ParentKind
	Units    int
	Children []Child
}

// BuildBatchPlan 展开网格并包成父计划。子 run 的顺序与 ExpandBatch 逐位一致。
func BuildBatchPlan(req BatchRequest) (ParentPlan, error) {
	children, err := ExpandBatch(req)
	if err != nil {
		return ParentPlan{}, err
	}
	return ParentPlan{Kind: ParentBatch, Units: len(children), Children: children}, nil
}

// BuildWalkForwardPlan 切窗口并包成父计划。Units = 窗口数（= Children/2）。
func BuildWalkForwardPlan(base json.RawMessage, cfg WindowConfig, days []time.Time) (ParentPlan, error) {
	children, err := ExpandWalkForward(base, cfg, days)
	if err != nil {
		return ParentPlan{}, err
	}
	return ParentPlan{Kind: ParentWalkForward, Units: len(children) / 2, Children: children}, nil
}

// PendingCounts 是提交时的计数：全部单元尚未回报。
func (p ParentPlan) PendingCounts() Counts {
	return Counts{Total: p.Units, Pending: p.Units}
}

// parentPlanWire 是 plan_canonical 的稳定形状。
//
// 刻意不复用 Child 本身：Child 的 Params / Window 是指针与 map，直接 marshal
// 会随字段增删漂移；这里的 wire 只放「重建计划必需」的字段，且顺序固定。
type parentPlanWire struct {
	Kind     ParentKind  `json:"kind"`
	Units    int         `json:"units"`
	Children []childWire `json:"children"`
}

type childWire struct {
	Kind    string          `json:"kind"`
	Index   int             `json:"index"`
	Request json.RawMessage `json:"request"`
	Window  *windowWire     `json:"window,omitempty"`
}

type windowWire struct {
	Index      int    `json:"index"`
	TrainStart string `json:"train_start"`
	TrainEnd   string `json:"train_end"`
	TestStart  string `json:"test_start"`
	TestEnd    string `json:"test_end"`
}

// Canonical 返回 plan_canonical 的稳定字节：子 run 顺序即计划顺序，
// Request 是完整 RunSpec 的 canonical JSON（Expand* 已保证确定性）。
func (p ParentPlan) Canonical() ([]byte, error) {
	if p.Units <= 0 {
		return nil, invalidf("parent plan has no units")
	}
	wire := parentPlanWire{Kind: p.Kind, Units: p.Units, Children: make([]childWire, 0, len(p.Children))}
	for _, c := range p.Children {
		cw := childWire{Kind: string(c.Kind), Index: c.Index, Request: c.Request}
		if c.Window != nil {
			cw.Window = &windowWire{
				Index:      c.Window.Index,
				TrainStart: c.Window.TrainStart.Format(DateLayout),
				TrainEnd:   c.Window.TrainEnd.Format(DateLayout),
				TestStart:  c.Window.TestStart.Format(DateLayout),
				TestEnd:    c.Window.TestEnd.Format(DateLayout),
			}
		}
		wire.Children = append(wire.Children, cw)
	}
	raw, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("plan: encode parent plan: %w", err)
	}
	// 再归一化一次：即使将来 wire 里出现 map，身份也不依赖编码顺序。
	canonical, err := queue.Canonicalize(raw)
	if err != nil {
		return nil, fmt.Errorf("plan: canonicalize parent plan: %w", err)
	}
	return canonical, nil
}

// Hash 返回 plan_hash（sha256(plan_canonical) 的 64 位 hex）。
func (p ParentPlan) Hash() (string, error) {
	canonical, err := p.Canonical()
	if err != nil {
		return "", err
	}
	return queue.RequestHash(canonical)
}
