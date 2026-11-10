package httpapi

// X0 生产者（SVC-ORCH-G2 ③）：`POST /api/runs/jobs/{id}/artifacts`。
//
// 裁决 ⑱：core worker 回传三 hash 时**调本端点、不直写库**；本仓是 `run.*`
// 的唯一 writer。写入路径只有一条——复用 `queue.ArtifactStore.SaveArtifact`
// 的幂等与身份漂移语义，不在 HTTP 层重造「先查再写」。
//
// 请求体形状 = 本地 DTO `queue.ArtifactRecord`（docs/api-schema.md 标注为本地
// 形状；契约化已由 tellurion-contracts:CT-04 收编，contracts/run v0.9.0 起可用，
// 跨仓冻结仍以 contracts 发布为准）。路径里的 id
// 是 run_id 的唯一来源：body 里的 `run_id` 若出现且不一致 → 400，不静默覆盖。

import (
	"errors"
	"net/http"
	"time"

	"tellurion-orchestration/internal/queue"
)

// saveArtifact 见 docs/api-schema.md §1「artifact 写入（X0 生产者）」：
//
//   - 首次写入 → 201；
//   - 同身份重放 → 200（`created_at` 不刷新）；
//   - 身份漂移 → 409 `artifact_conflict`，原行不被覆盖；
//   - run 不存在 → 404 `job_not_found`（PG 侧外键 / 内存侧显式查，同一报错）。
func (s *Server) saveArtifact(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	var rec queue.ArtifactRecord
	if err := decodeBody(w, r, &rec); err != nil {
		writeInvalidParams(w, err)
		return
	}
	if rec.RunID != "" && rec.RunID != runID {
		writeInvalidParams(w, errors.New("run_id in body must match the path"))
		return
	}
	rec.RunID = runID
	// created_at 是留痕的写入时间，不是调用方可填的身份：清空让存储层自己盖戳。
	rec.CreatedAt = time.Time{}

	_, err := s.artifacts.GetArtifact(r.Context(), runID)
	existed := err == nil
	if err != nil && !errors.Is(err, queue.ErrArtifactNotFound) {
		writeError(w, err)
		return
	}

	saved, err := s.artifacts.SaveArtifact(r.Context(), &rec)
	if err != nil {
		writeError(w, err)
		return
	}
	code := http.StatusOK
	if !existed {
		code = http.StatusCreated
	}
	writeJSON(w, code, saved)
}
