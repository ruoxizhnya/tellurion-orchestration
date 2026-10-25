package httpapi

// X0 provenance 端点的 HTTP 腿（内存实现，无库）。
//
// 钉住五件事：
//   - 三 hash 与生效值逐项可读（独立 DTO，不扩 BacktestResponse）；
//   - result_status 只从 `backtest_jobs` 那一轴取：`invalid` 读成 invalid，
//     **不被读成 failed**；
//   - 没有留痕的三态：非终态 409、终态 failed 404、终态 completed/invalid 500；
//   - 路由 catch-all 不抢 jobs / parents 的路由；
//   - 父作业 GET 能逐子读出三 hash 与三值。

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"tellurion-orchestration/internal/queue"
)

func newProvenanceServer(t *testing.T) (*Server, *queue.Memory, *queue.MemoryArtifacts) {
	t.Helper()
	m := queue.NewMemory(queue.MemoryOptions{Now: func() time.Time { return baseTime }})
	arts := queue.NewMemoryArtifacts(m, queue.MemoryOptions{Now: func() time.Time { return baseTime }})
	return New(m, WithArtifacts(arts)), m, arts
}

// seedRun 提交一条并推进到终态（resultStatus 为空 → 只跑到 running，用来造 409）。
func seedRun(t *testing.T, s *Server, jobs *queue.Memory, key, resultStatus string) string {
	t.Helper()
	id := submitJob(t, s, key, runSpecA)
	ctx := t.Context()
	if _, err := jobs.Claim(ctx, "w-1", time.Minute); err != nil {
		t.Fatalf("claim %s: %v", key, err)
	}
	switch resultStatus {
	case "":
		// 停在 running：证明 409 是「非终态」而不是「没提交」。
	case "failed":
		if err := jobs.Fail(ctx, id, "w-1", "engine boom", "failed"); err != nil {
			t.Fatalf("fail %s: %v", key, err)
		}
	default:
		if err := jobs.Complete(ctx, id, "w-1", resultStatus, "mem://results/"+id); err != nil {
			t.Fatalf("complete %s: %v", key, err)
		}
	}
	return id
}

func artifactOf(t *testing.T, jobs *queue.Memory, runID, resultStatus string) *queue.ArtifactRecord {
	t.Helper()
	job, err := jobs.Get(t.Context(), runID)
	if err != nil {
		t.Fatal(err)
	}
	seed := int64(7)
	return &queue.ArtifactRecord{
		RunID:               runID,
		StrategyRef:         "momentum@deadbeef",
		DatasetSnapshotHash: "dataset-hash-1",
		RiskRulesHash:       "",
		ArtifactURI:         "mem://artifacts/" + runID,
		ArtifactHash:        "artifact-hash-1",
		VersionHash:         "version-hash-1",
		RunSpecHash:         job.RequestHash,
		ParamsHash:          "params-hash-1",
		ParamsJSON:          json.RawMessage(`{"lookback":20}`),
		Seed:                &seed,
		Kernel:              "go",
		EngineVersion:       "engine-1.0",
		FillTiming:          "next_open",
	}
}

func TestProvenance_ReturnsTheThreeHashesAndK9Conclusion(t *testing.T) {
	// invalid 是**结论**不是故障：同一份留痕下，两个终态只差 job 的 result_status。
	for _, resultStatus := range []string{"completed", "invalid"} {
		t.Run(resultStatus, func(t *testing.T) {
			s, jobs, arts := newProvenanceServer(t)
			id := seedRun(t, s, jobs, "run-"+resultStatus, resultStatus)
			if _, err := arts.SaveArtifact(t.Context(), artifactOf(t, jobs, id, resultStatus)); err != nil {
				t.Fatalf("save artifact: %v", err)
			}

			code, resp := do(t, s, "GET", "/api/runs/"+id+"/provenance", "")
			wantCode(t, code, resp, http.StatusOK, "")

			var raw map[string]json.RawMessage
			if err := json.Unmarshal([]byte(resp), &raw); err != nil {
				t.Fatalf("provenance body: %v\n%s", err, resp)
			}
			if _, has := raw["status"]; has {
				t.Fatalf("provenance must be an independent DTO without lifecycle status: %s", resp)
			}

			var pv provenanceView
			if err := json.Unmarshal([]byte(resp), &pv); err != nil {
				t.Fatalf("provenance view: %v\n%s", err, resp)
			}
			job, err := jobs.Get(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			if pv.RunID != id || pv.RunSpecHash != job.RequestHash {
				t.Fatalf("identity = %+v, want run_id=%s run_spec_hash=%s", pv, id, job.RequestHash)
			}
			if pv.DatasetSnapshotHash != "dataset-hash-1" ||
				pv.ArtifactHash == nil || *pv.ArtifactHash != "artifact-hash-1" {
				t.Fatalf("三 hash = %+v", pv)
			}
			if pv.ResultStatus == nil || *pv.ResultStatus != resultStatus {
				t.Fatalf("result_status = %v, want %q（K9 结论只从 backtest_jobs 取）", pv.ResultStatus, resultStatus)
			}
			if job.Status != queue.RunCompleted {
				t.Fatalf("job status = %s, want completed（invalid 是完成后的结论）", job.Status)
			}
			if pv.StrategyRef != "momentum@deadbeef" || pv.ParamsHash != "params-hash-1" ||
				pv.Kernel != "go" || pv.EngineVersion != "engine-1.0" || pv.FillTiming != "next_open" {
				t.Fatalf("生效值 = %+v", pv)
			}
			if pv.Seed == nil || *pv.Seed != 7 || pv.CreatedAt == nil {
				t.Fatalf("seed/created_at = %v / %v", pv.Seed, pv.CreatedAt)
			}
		})
	}
}

func TestProvenance_NullableColumnsAreExplicitNull(t *testing.T) {
	s, jobs, arts := newProvenanceServer(t)
	id := seedRun(t, s, jobs, "run-nulls", "completed")
	art := artifactOf(t, jobs, id, "completed")
	art.ArtifactURI = ""
	art.ArtifactHash = ""
	art.VersionHash = ""
	art.Seed = nil
	if _, err := arts.SaveArtifact(t.Context(), art); err != nil {
		t.Fatalf("save artifact: %v", err)
	}

	code, resp := do(t, s, "GET", "/api/runs/"+id+"/provenance", "")
	wantCode(t, code, resp, http.StatusOK, "")

	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(resp), &raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"artifact_uri", "artifact_hash", "version_hash", "seed"} {
		v, ok := raw[key]
		if !ok {
			t.Fatalf("%s 必须显式出键（读侧要能区分「没有」与「未提供」）: %s", key, resp)
		}
		if string(v) != "null" {
			t.Fatalf("%s = %s, want null", key, v)
		}
	}
	// 空串 = 未登记，不是 null（QT-42 X3b 之前）。
	if string(raw["risk_rules_hash"]) != `""` {
		t.Fatalf("risk_rules_hash = %s, want \"\"（空串 = 未登记）", raw["risk_rules_hash"])
	}
}

func TestProvenance_NotReadyIs409(t *testing.T) {
	s, jobs, _ := newProvenanceServer(t)
	id := seedRun(t, s, jobs, "run-running", "")

	doWant(t, s, "GET", "/api/runs/"+id+"/provenance", "", http.StatusConflict, "provenance_not_ready")
}

func TestProvenance_TerminalWithoutRowSplits404And500(t *testing.T) {
	cases := []struct {
		name         string
		resultStatus string
		status       int
		code         string
	}{
		{"failed never produced provenance", "failed", http.StatusNotFound, "provenance_not_found"},
		{"completed lost its provenance", "completed", http.StatusInternalServerError, "provenance_missing"},
		{"invalid lost its provenance", "invalid", http.StatusInternalServerError, "provenance_missing"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, jobs, _ := newProvenanceServer(t)
			id := seedRun(t, s, jobs, "run-"+tc.resultStatus, tc.resultStatus)
			doWant(t, s, "GET", "/api/runs/"+id+"/provenance", "", tc.status, tc.code)
		})
	}
}

func TestProvenance_UnknownRunIs404JobNotFound(t *testing.T) {
	s, _, _ := newProvenanceServer(t)
	doWant(t, s, "GET", "/api/runs/nope/provenance", "", http.StatusNotFound, "job_not_found")
}

// TestProvenance_CatchAllDoesNotStealOtherRoutes 是路由形状的破坏腿：
// catch-all 只能接住 `<id>/provenance`，别的后缀 404 且不是 wire error；
// jobs / parents 的路由仍然优先（注册期不 panic 只是第一层）。
func TestProvenance_CatchAllDoesNotStealOtherRoutes(t *testing.T) {
	s, jobs, _ := newProvenanceServer(t)
	id := submitJob(t, s, "k-route", runSpecA)

	// 别的后缀：http 的 404（不是 JSON 的 internal）。
	code, resp := do(t, s, "GET", "/api/runs/"+id+"/something-else", "")
	if code != http.StatusNotFound || contains(resp, `"code"`) {
		t.Fatalf("unexpected suffix: status=%d body=%s", code, resp)
	}
	// 带斜杠的 id 也不是本端点。
	code, resp = do(t, s, "GET", "/api/runs/a/b/provenance", "")
	if code != http.StatusNotFound || contains(resp, `"code"`) {
		t.Fatalf("nested id: status=%d body=%s", code, resp)
	}
	// GET job 仍是 jobView（带 status，不是 provenance）。
	code, resp = do(t, s, "GET", "/api/runs/jobs/"+id, "")
	wantCode(t, code, resp, http.StatusOK, "")
	if !contains(resp, `"job_id"`) {
		t.Fatalf("jobs route stolen by catch-all: %s", resp)
	}
	_ = jobs
}

// TestGetParent_ChildrenCarryThreeHashes 钉「父作业 GET 逐子读出三 hash 与三值」。
func TestGetParent_ChildrenCarryThreeHashes(t *testing.T) {
	m := queue.NewMemory(queue.MemoryOptions{Now: func() time.Time { return baseTime }})
	parentStore := queue.NewMemoryParents(queue.MemoryOptions{
		Now:   func() time.Time { return baseTime },
		NewID: func() string { return "parent-fixed" },
	})
	arts := queue.NewMemoryArtifacts(m, queue.MemoryOptions{Now: func() time.Time { return baseTime }})
	s := New(m, WithParents(parentStore), WithArtifacts(arts))

	code, resp := do(t, s, "POST", "/api/runs/parents", batchParentBody("batch-x0"))
	if code != http.StatusAccepted {
		t.Fatalf("submit parent: %d %s", code, resp)
	}
	parent := decodeParentSubmit(t, resp)

	ctx := t.Context()
	child, _, err := m.Enqueue(ctx, &queue.RunRecord{
		IdempotencyKey: "child-x0",
		ParentJobID:    parent.ParentJobID,
		RequestJSON:    []byte(parentBaseSpec),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Claim(ctx, "w-1", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := m.Complete(ctx, child.ID, "w-1", "invalid", "mem://results/"+child.ID); err != nil {
		t.Fatalf("complete child: %v", err)
	}
	job, err := m.Get(ctx, child.ID)
	if err != nil {
		t.Fatal(err)
	}
	seed := int64(3)
	if _, err := arts.SaveArtifact(ctx, &queue.ArtifactRecord{
		RunID:               child.ID,
		StrategyRef:         "momentum",
		DatasetSnapshotHash: "dataset-hash-x0",
		RunSpecHash:         job.RequestHash,
		ParamsHash:          "params-hash-x0",
		ArtifactHash:        "artifact-hash-x0",
		Seed:                &seed,
		Kernel:              "go",
		EngineVersion:       "engine-x0",
		FillTiming:          "next_open",
	}); err != nil {
		t.Fatalf("save artifact: %v", err)
	}

	code, resp = do(t, s, "GET", "/api/runs/parents/"+parent.ParentJobID, "")
	wantCode(t, code, resp, http.StatusOK, "")
	var view parentView
	if err := json.Unmarshal([]byte(resp), &view); err != nil {
		t.Fatal(err)
	}
	if len(view.Children) != 1 {
		t.Fatalf("children = %d, want 1: %s", len(view.Children), resp)
	}
	c := view.Children[0]
	if c.RunID != child.ID || c.Status != "completed" {
		t.Fatalf("child = %+v", c)
	}
	if c.ResultStatus == nil || *c.ResultStatus != "invalid" {
		t.Fatalf("child result_status = %v, want invalid（不被读成 failed）", c.ResultStatus)
	}
	if c.RunSpecHash != job.RequestHash {
		t.Fatalf("run_spec_hash = %q, want %q", c.RunSpecHash, job.RequestHash)
	}
	if c.DatasetSnapshotHash != "dataset-hash-x0" || c.ArtifactHash != "artifact-hash-x0" {
		t.Fatalf("三 hash = %+v", c)
	}
}

// TestGetParent_ChildrenWithoutArtifactsOmitHashes：没写留痕的子（如 failed 在
// 路由拒绝阶段）不伪造 hash，但 run_spec_hash 照样有（它来自 backtest_jobs）。
func TestGetParent_ChildrenWithoutArtifactsOmitHashes(t *testing.T) {
	m := queue.NewMemory(queue.MemoryOptions{Now: func() time.Time { return baseTime }})
	parentStore := queue.NewMemoryParents(queue.MemoryOptions{
		Now:   func() time.Time { return baseTime },
		NewID: func() string { return "parent-fixed" },
	})
	arts := queue.NewMemoryArtifacts(m, queue.MemoryOptions{Now: func() time.Time { return baseTime }})
	s := New(m, WithParents(parentStore), WithArtifacts(arts))

	code, resp := do(t, s, "POST", "/api/runs/parents", batchParentBody("batch-no-art"))
	if code != http.StatusAccepted {
		t.Fatalf("submit parent: %d %s", code, resp)
	}
	parent := decodeParentSubmit(t, resp)

	ctx := t.Context()
	child, _, err := m.Enqueue(ctx, &queue.RunRecord{
		IdempotencyKey: "child-no-art",
		ParentJobID:    parent.ParentJobID,
		RequestJSON:    []byte(parentBaseSpec),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Claim(ctx, "w-1", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := m.Fail(ctx, child.ID, "w-1", "route rejected", "failed"); err != nil {
		t.Fatal(err)
	}

	code, resp = do(t, s, "GET", "/api/runs/parents/"+parent.ParentJobID, "")
	wantCode(t, code, resp, http.StatusOK, "")
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(resp), &raw); err != nil {
		t.Fatal(err)
	}
	var children []map[string]json.RawMessage
	if err := json.Unmarshal(raw["children"], &children); err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 {
		t.Fatalf("children = %d", len(children))
	}
	if _, has := children[0]["dataset_snapshot_hash"]; has {
		t.Fatalf("未产出的子不得伪造 dataset_snapshot_hash: %s", raw["children"])
	}
	if _, has := children[0]["artifact_hash"]; has {
		t.Fatalf("未产出的子不得伪造 artifact_hash: %s", raw["children"])
	}
	if string(children[0]["run_spec_hash"]) == "" || string(children[0]["run_spec_hash"]) == "null" {
		t.Fatalf("run_spec_hash 必须对任何子都在: %s", raw["children"])
	}
}
