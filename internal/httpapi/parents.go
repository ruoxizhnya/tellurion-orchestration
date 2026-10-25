package httpapi

// X5b 父作业 HTTP 面：`POST /api/runs/parents`（batch / walkforward 共用）与
// `GET /api/runs/parents/{id}`（三值汇总 + 逐子状态）。
//
// 端点形状对表 docs/api-schema.md §1「批量 / 前向」。本文件只做传输：严格解码、
// 把 wire 请求折成 internal/plan 的输入、状态码选择。计划展开、身份计算
// （plan_canonical / plan_hash）与汇总口径全部在 internal/plan + internal/parents。

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"tellurion-orchestration/internal/plan"
	"tellurion-orchestration/internal/queue"
)

// parentSubmitRequest 是 POST 的请求体。kind 决定 batch / walkforward **二者取一**：
// 同时给出两个载荷是歧义请求，直接 400（不猜哪一个才是意图）。
type parentSubmitRequest struct {
	IdempotencyKey string                    `json:"idempotency_key"`
	Kind           string                    `json:"kind"`
	Batch          *parentBatchRequest       `json:"batch"`
	WalkForward    *parentWalkForwardRequest `json:"walkforward"`
}

type parentBatchRequest struct {
	BaseRunSpec json.RawMessage              `json:"base_run_spec"`
	Grid        map[string][]json.RawMessage `json:"grid"`
}

type parentWalkForwardRequest struct {
	BaseRunSpec json.RawMessage `json:"base_run_spec"`
	Windows     parentWindows   `json:"windows"`
	// TradingDays 是调用方传入的交易日历（`YYYY-MM-DD`，严格递增）。
	// 本仓不自造日历（docs/dataflow.md；日历属 data 服务的只读 port）。
	TradingDays []string `json:"trading_days"`
}

type parentWindows struct {
	TrainDays    int `json:"train_days"`
	TestDays     int `json:"test_days"`
	StepDays     int `json:"step_days"`
	MinTrainDays int `json:"min_train_days"`
}

// parentCountsView 是三值汇总 + 两个派生值。`settled` 与 `pending` 由 total
// 推出，不另存列——落库的是四个计数，读侧不制造第二份真相。
type parentCountsView struct {
	Completed int `json:"completed"`
	Invalid   int `json:"invalid"`
	Failed    int `json:"failed"`
	Pending   int `json:"pending"`
	Settled   int `json:"settled"`
}

func countsOf(parent *queue.ParentRecord) parentCountsView {
	settled := parent.Completed + parent.Invalid + parent.Failed
	return parentCountsView{
		Completed: parent.Completed,
		Invalid:   parent.Invalid,
		Failed:    parent.Failed,
		Pending:   parent.Total - settled,
		Settled:   settled,
	}
}

// parentSubmitBody 是 POST 的响应体（202 新建 / 200 幂等重放）。
type parentSubmitBody struct {
	ParentJobID      string           `json:"parent_job_id"`
	Kind             string           `json:"kind"`
	Status           string           `json:"status"`
	PlanHash         string           `json:"plan_hash"`
	Total            int              `json:"total"`
	Counts           parentCountsView `json:"counts"`
	IdempotentReplay bool             `json:"idempotent_replay,omitempty"`
}

// parentChildView 是「逐子状态」的一行。子 run 的结论看 result_status（K9），
// 不看父状态——两轴不合并（docs/dataflow.md §4 第 8 条）。
//
// 三 hash 来自 `run_artifacts`（X0）：`run_spec_hash` 对任何子 run 都取得到
// （`backtest_jobs.request_hash`），`dataset_snapshot_hash` / `artifact_hash`
// 只有留痕已写入时才有——未跑完 / 未产出结果时省略键，不伪造空值。
type parentChildView struct {
	RunID               string     `json:"run_id"`
	Status              string     `json:"status"`
	ResultStatus        *string    `json:"result_status"`
	Error               string     `json:"error,omitempty"`
	RunSpecHash         string     `json:"run_spec_hash"`
	DatasetSnapshotHash string     `json:"dataset_snapshot_hash,omitempty"`
	ArtifactHash        string     `json:"artifact_hash,omitempty"`
	CreatedAt           *time.Time `json:"created_at"`
}

// parentView 是 GET 的响应体：父记录 + 三值汇总 + 逐子状态 + 汇总明细。
type parentView struct {
	ParentJobID string            `json:"parent_job_id"`
	Kind        string            `json:"kind"`
	Status      string            `json:"status"`
	PlanHash    string            `json:"plan_hash"`
	Total       int               `json:"total"`
	Counts      parentCountsView  `json:"counts"`
	Children    []parentChildView `json:"children"`
	Summary     json.RawMessage   `json:"summary,omitempty"`
	CreatedAt   *time.Time        `json:"created_at"`
	CompletedAt *time.Time        `json:"completed_at"`
}

// submitParent 见 docs/api-schema.md §1「批量 / 前向」：新建 202、幂等重放 200。
// 缺幂等键先于领域校验（与 jobs 端点同一 fail-closed 口径，服务端不代生成键）。
func (s *Server) submitParent(w http.ResponseWriter, r *http.Request) {
	var req parentSubmitRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeInvalidParams(w, err)
		return
	}
	if req.IdempotencyKey == "" {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "idempotency_key is required", Code: "missing_idempotency_key"})
		return
	}

	switch queue.ParentKind(req.Kind) {
	case queue.ParentBatch:
		planReq, err := req.batchRequest()
		if err != nil {
			writeInvalidParams(w, err)
			return
		}
		parent, created, err := s.psvc.SubmitBatch(r.Context(), req.IdempotencyKey, planReq)
		if err != nil {
			writeError(w, err)
			return
		}
		s.writeParentSubmit(w, parent, created)

	case queue.ParentWalkForward:
		base, cfg, days, err := req.walkForwardRequest()
		if err != nil {
			writeInvalidParams(w, err)
			return
		}
		parent, created, err := s.psvc.SubmitWalkForward(r.Context(), req.IdempotencyKey, base, cfg, days)
		if err != nil {
			writeError(w, err)
			return
		}
		s.writeParentSubmit(w, parent, created)

	default:
		writeInvalidParams(w, fmt.Errorf("kind must be %q or %q, got %q",
			queue.ParentBatch, queue.ParentWalkForward, req.Kind))
	}
}

func (s *Server) writeParentSubmit(w http.ResponseWriter, parent *queue.ParentRecord, created bool) {
	writeJSON(w, submitCode(created), parentSubmitBody{
		ParentJobID:      parent.ID,
		Kind:             string(parent.Kind),
		Status:           string(parent.Status),
		PlanHash:         parent.PlanHash,
		Total:            parent.Total,
		Counts:           countsOf(parent),
		IdempotentReplay: !created,
	})
}

// batchRequest 把 wire 载荷折成 plan.BatchRequest，并拒绝歧义 / 缺字段。
// 展开期错误（空网格、非标量、超上限）由 plan.ExpandBatch 报出并点名参数。
func (r parentSubmitRequest) batchRequest() (plan.BatchRequest, error) {
	if r.WalkForward != nil {
		return plan.BatchRequest{}, fmt.Errorf("walkforward must be absent when kind=%q", queue.ParentBatch)
	}
	if r.Batch == nil {
		return plan.BatchRequest{}, fmt.Errorf("batch is required when kind=%q", queue.ParentBatch)
	}
	if len(r.Batch.BaseRunSpec) == 0 {
		return plan.BatchRequest{}, fmt.Errorf("batch.base_run_spec is required")
	}
	return plan.BatchRequest{BaseRunSpec: r.Batch.BaseRunSpec, Grid: r.Batch.Grid}, nil
}

// walkForwardRequest 把 wire 载荷折成 plan 的三元输入，并逐项点名非法字段。
// 日期只接受 `YYYY-MM-DD`（不 trim、不猜）：日历严格递增与窗口参数范围由
// plan.SplitWindows 继续校验。
func (r parentSubmitRequest) walkForwardRequest() (json.RawMessage, plan.WindowConfig, []time.Time, error) {
	if r.Batch != nil {
		return nil, plan.WindowConfig{}, nil, fmt.Errorf("batch must be absent when kind=%q", queue.ParentWalkForward)
	}
	if r.WalkForward == nil {
		return nil, plan.WindowConfig{}, nil, fmt.Errorf("walkforward is required when kind=%q", queue.ParentWalkForward)
	}
	wf := r.WalkForward
	if len(wf.BaseRunSpec) == 0 {
		return nil, plan.WindowConfig{}, nil, fmt.Errorf("walkforward.base_run_spec is required")
	}
	if len(wf.TradingDays) == 0 {
		return nil, plan.WindowConfig{}, nil, fmt.Errorf("walkforward.trading_days is required")
	}

	days := make([]time.Time, len(wf.TradingDays))
	for i, raw := range wf.TradingDays {
		day, err := time.Parse(plan.DateLayout, raw)
		if err != nil {
			return nil, plan.WindowConfig{}, nil,
				fmt.Errorf("walkforward.trading_days[%d] must be YYYY-MM-DD, got %q", i, raw)
		}
		days[i] = day
	}

	cfg := plan.WindowConfig{
		TrainDays:    wf.Windows.TrainDays,
		TestDays:     wf.Windows.TestDays,
		StepDays:     wf.Windows.StepDays,
		MinTrainDays: wf.Windows.MinTrainDays,
	}
	return wf.BaseRunSpec, cfg, days, nil
}

// getParent 见 docs/api-schema.md §1：父记录 + 三值汇总 + 逐子状态。
//
// 状态与计数读的是已落库的值（写回入口 = internal/parents.Apply*Summary），
// 不在读路径重算；`children` 由 JobQueue 的可选能力 ParentChildLister 提供，
// 实现不支持时返回空数组而不是伪造状态。
func (s *Server) getParent(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	parent, err := s.psvc.Get(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}

	children := make([]parentChildView, 0)
	if lister, ok := s.q.(queue.ParentChildLister); ok {
		rows, err := lister.ListChildren(r.Context(), id)
		if err != nil {
			writeError(w, err)
			return
		}
		for _, row := range rows {
			child := parentChildView{
				RunID:        row.ID,
				Status:       string(row.Status),
				ResultStatus: resultStatusPtr(row.ResultStatus),
				Error:        row.ErrorMessage,
				RunSpecHash:  row.RequestHash,
				CreatedAt:    timePtr(row.CreatedAt),
			}
			// 只有可能已有留痕的子（终态结论已写）才去读 artifact：pending /
			// running 的子没有结论，查了必然 NotFound，也答不出坐标。
			if s.artifacts != nil && row.ResultStatus != "" {
				art, err := s.artifacts.GetArtifact(r.Context(), row.ID)
				switch {
				case err == nil:
					child.DatasetSnapshotHash = art.DatasetSnapshotHash
					child.ArtifactHash = art.ArtifactHash
				case errors.Is(err, queue.ErrArtifactNotFound):
					// 终态却没有留痕：这里不制造第二个完整性判据——三 hash 的
					// 完整性由 `/provenance` 的 provenance_missing 报，父列表只
					// 如实省略这两列。
				default:
					// 真错误（连接 / 扫描）不吞：吞掉等于把「读坏了」伪装成
					// 「这个子没有留痕」。
					writeError(w, err)
					return
				}
			}
			children = append(children, child)
		}
	}

	writeJSON(w, http.StatusOK, parentView{
		ParentJobID: parent.ID,
		Kind:        string(parent.Kind),
		Status:      string(parent.Status),
		PlanHash:    parent.PlanHash,
		Total:       parent.Total,
		Counts:      countsOf(parent),
		Children:    children,
		Summary:     parent.SummaryJSON,
		CreatedAt:   timePtr(parent.CreatedAt),
		CompletedAt: timePtr(parent.CompletedAt),
	})
}
