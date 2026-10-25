package httpapi

// X0（B6-4）：`GET /api/runs/{id}/provenance`。
//
// 三 hash 是 run **开始之后**才产出的身份（docs/api-schema.md §2 fail-closed ④），
// 所以这条端点是一次 `run.run_artifacts ⋈ run.backtest_jobs` 的读：
// hash / 指针 / 生效值来自留痕表，**status / result_status 只从 backtest_jobs 取**
// （QT-63 ④ 单一同源——`run_artifacts` 没有 status 列）。
//
// 路由形状是 catch-all：Go 1.22 ServeMux 里 `GET /api/runs/parents/{id}` 与
// `GET /api/runs/{id}/provenance` 在 `/api/runs/parents/provenance` 上互相覆盖
// 且**谁也不更具体**，两条同时注册是启动期 panic（实测）。这里用
// `GET /api/runs/{rest...}` 手工切后缀——jobs / parents 的路由都比 catch-all
// 更具体，仍然优先。

import (
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"tellurion-orchestration/internal/queue"
)

// provenanceView 是 `ProvenanceResponse`（docs/ARCHITECTURE.md §9.3）——
// **独立 DTO**，不扩冻结的 `BacktestResponse`（27 项字段集一个字不动）。
//
// `artifact_uri` / `artifact_hash` / `version_hash` 可空：内置策略没有独立
// artifact；显式出 null 而不是省略键，读侧不用猜「没有」与「空串」的区别。
// `risk_rules_hash` 是空串时表示**未登记**（QT-42 X3b 之前），不是「无规则」。
type provenanceView struct {
	RunID               string     `json:"run_id"`
	RunSpecHash         string     `json:"run_spec_hash"`
	DatasetSnapshotHash string     `json:"dataset_snapshot_hash"`
	RiskRulesHash       string     `json:"risk_rules_hash"`
	StrategyRef         string     `json:"strategy_ref"`
	ArtifactURI         *string    `json:"artifact_uri"`
	ArtifactHash        *string    `json:"artifact_hash"`
	VersionHash         *string    `json:"version_hash"`
	ParamsHash          string     `json:"params_hash"`
	Seed                *int64     `json:"seed"`
	Kernel              string     `json:"kernel"`
	EngineVersion       string     `json:"engine_version"`
	FillTiming          string     `json:"fill_timing"`
	ResultStatus        *string    `json:"result_status"`
	CreatedAt           *time.Time `json:"created_at"`
}

// provenanceOf 把「留痕 ⋈ job」折成响应体。
//
// result_status 与 created_at 的来源刻意分开：前者是 run 的结论（job 那一轴），
// 后者是留痕的写入时间（`run_artifacts.created_at`）。两个时间不混用——job 的
// 创建时间在 `GET /api/runs/jobs/{id}` 里已经有了。
func provenanceOf(job *queue.RunRecord, art *queue.ArtifactRecord) provenanceView {
	return provenanceView{
		RunID:               art.RunID,
		RunSpecHash:         art.RunSpecHash,
		DatasetSnapshotHash: art.DatasetSnapshotHash,
		RiskRulesHash:       art.RiskRulesHash,
		StrategyRef:         art.StrategyRef,
		ArtifactURI:         optString(art.ArtifactURI),
		ArtifactHash:        optString(art.ArtifactHash),
		VersionHash:         optString(art.VersionHash),
		ParamsHash:          art.ParamsHash,
		Seed:                art.Seed,
		Kernel:              art.Kernel,
		EngineVersion:       art.EngineVersion,
		FillTiming:          art.FillTiming,
		ResultStatus:        resultStatusPtr(job.ResultStatus),
		CreatedAt:           timePtr(art.CreatedAt),
	}
}

// provenance 见 docs/api-schema.md §1「结果坐标（X0）」与 §2 的三个 provenance 码：
// 非终态 → 409 `provenance_not_ready`（留痕还没写，等）；
// 终态 failed / cancelled 且无留痕 → 404 `provenance_not_found`（从未产出）；
// 终态 completed / invalid 却无留痕 → 500 `provenance_missing`（数据完整性破坏）。
func (s *Server) provenance(w http.ResponseWriter, r *http.Request) {
	runID, ok := provenanceRunID(r.PathValue("rest"))
	if !ok {
		http.NotFound(w, r)
		return
	}

	job, err := s.q.Get(r.Context(), runID)
	if err != nil {
		writeError(w, err)
		return
	}
	art, err := s.artifacts.GetArtifact(r.Context(), runID)
	switch {
	case err == nil:
		// 留痕在：照读。job 非终态却有留痕在真实时序里不该出现，但读面不撒谎——
		// 「有」就返回「有」，不拿状态机去否认可观测的事实。
	case errors.Is(err, queue.ErrArtifactNotFound):
		writeProvenanceAbsent(w, job)
		return
	default:
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, provenanceOf(job, art))
}

// writeProvenanceAbsent 按 job 的两轴决定「没有留痕」是哪种语义。
//
// 判据落在 `result_status`（K9 结论）而不是只落在 `status`：`status=completed`
// 时 `result_status` 可能是 `invalid`——两者都要求有留痕。`invalid` 是正常结论，
// 不是失败，所以它缺留痕同样是完整性破坏（500），**不能**读成 failed 的 404。
func writeProvenanceAbsent(w http.ResponseWriter, job *queue.RunRecord) {
	if !job.Terminal() {
		writeJSON(w, http.StatusConflict, errorBody{
			Error: "run is not terminal; provenance is written when it finishes",
			Code:  "provenance_not_ready",
		})
		return
	}
	if job.ResultStatus == queue.ResultCompleted ||
		job.ResultStatus == queue.ResultInvalid ||
		job.Status == queue.RunCompleted {
		log.Printf("httpapi: provenance_missing for %s (status=%s, result_status=%q)",
			job.ID, job.Status, job.ResultStatus)
		writeJSON(w, http.StatusInternalServerError, errorBody{
			Error: "terminal run has no provenance row",
			Code:  "provenance_missing",
		})
		return
	}
	writeJSON(w, http.StatusNotFound, errorBody{
		Error: "run never produced provenance",
		Code:  "provenance_not_found",
	})
}

// provenanceRunID 从 catch-all 的剩余路径里切出 run id。
// 只接受恰好 `<id>/provenance` 的形状；带斜杠的 id / 空 id / 别的后缀一律 404
// （不是本端点，也不猜）。
func provenanceRunID(rest string) (string, bool) {
	runID, ok := strings.CutSuffix(rest, "/provenance")
	if !ok || runID == "" || strings.Contains(runID, "/") {
		return "", false
	}
	return runID, true
}

// optString 把「可选文本列」折成 JSON null / 字符串。
func optString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
