package httpapi

// CT-04 / ORCH-G1 收编腿：生产侧改为直接使用 contracts/run 后，这里钉住
// **改前 / 改后逐字节一致**——冻结的是已上 wire 的字节（字段名 / 顺序 / 可空性），
// 不是 Go 结构体的指针身份。任何字段重排、加 omitempty、可空字段变省略键都会红。

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ruoxizhnya/tellurion-contracts/run"

	"tellurion-orchestration/internal/queue"
)

func TestClaimWireShapeUnchanged(t *testing.T) {
	lease := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	got, err := json.Marshal(claimBody{Job: claimViewOf(&queue.RunRecord{
		ID:          "r1",
		RequestJSON: json.RawMessage(`{"a":1}`),
		Attempts:    2,
		MaxAttempts: 3,
		LeaseUntil:  lease,
	})})
	if err != nil {
		t.Fatal(err)
	}

	// 普通 run：parent_job_id 带 omitempty，键不出现。
	const want = `{"job":{"run_id":"r1","run_spec":{"a":1},"attempts":2,"max_attempts":3,"lease_until":"2026-01-02T03:04:05Z","cancel_requested":false}}`
	if string(got) != want {
		t.Fatalf("claim bytes drifted:\n got %s\nwant %s", got, want)
	}

	// 子 run：parent_job_id 出现在末尾。
	got, err = json.Marshal(claimBody{Job: claimViewOf(&queue.RunRecord{
		ID:          "r2",
		RequestJSON: json.RawMessage(`{"a":1}`),
		LeaseUntil:  lease,
		ParentJobID: "p1",
	})})
	if err != nil {
		t.Fatal(err)
	}
	const wantChild = `{"job":{"run_id":"r2","run_spec":{"a":1},"attempts":0,"max_attempts":0,"lease_until":"2026-01-02T03:04:05Z","cancel_requested":false,"parent_job_id":"p1"}}`
	if string(got) != wantChild {
		t.Fatalf("child claim bytes drifted:\n got %s\nwant %s", got, wantChild)
	}
}

func TestProvenanceWireShapeUnchanged(t *testing.T) {
	got, err := json.Marshal(provenanceOf(
		&queue.RunRecord{ID: "r1"},
		&queue.ArtifactRecord{
			RunID:               "r1",
			RunSpecHash:         "h1",
			DatasetSnapshotHash: "d1",
			StrategyRef:         "s1",
			ParamsHash:          "p1",
			Kernel:              "go",
			EngineVersion:       "v1",
			FillTiming:          "next_open",
		},
	))
	if err != nil {
		t.Fatal(err)
	}

	// 三个可空指针与 seed / result_status / created_at 必须显式出 null。
	const want = `{"run_id":"r1","run_spec_hash":"h1","dataset_snapshot_hash":"d1","risk_rules_hash":"","strategy_ref":"s1","artifact_uri":null,"artifact_hash":null,"version_hash":null,"params_hash":"p1","seed":null,"kernel":"go","engine_version":"v1","fill_timing":"next_open","result_status":null,"created_at":null}`
	if string(got) != want {
		t.Fatalf("provenance bytes drifted:\n got %s\nwant %s", got, want)
	}
}

func TestParentSubmitRequestContractRoundTrip(t *testing.T) {
	const body = `{"idempotency_key":"k1","kind":"batch","batch":{"base_run_spec":{"x":1},"grid":{"p":[1]}},"walkforward":null}`

	var req run.ParentSubmitRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatal(err)
	}
	if req.IdempotencyKey != "k1" || req.Kind != "batch" || req.Batch == nil || len(req.Batch.Grid["p"]) != 1 {
		t.Fatalf("contract request decode = %+v", req)
	}
	if req.WalkForward != nil {
		t.Fatalf("walkforward must decode to nil, got %+v", req.WalkForward)
	}

	got, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != body {
		t.Fatalf("parent submit bytes drifted:\n got %s\nwant %s", got, body)
	}
}
