package queue

// `run.run_artifacts` 内存实现的语义腿（PG 侧跑同一批的另一条腿见
// internal/pgstore/artifacts_test.go）。

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func newArtifactFixture(t *testing.T) (*Memory, *MemoryArtifacts, string) {
	t.Helper()
	now := time.Date(2026, 10, 25, 9, 0, 0, 0, time.UTC)
	m := NewMemory(MemoryOptions{Now: func() time.Time { return now }})
	arts := NewMemoryArtifacts(m, MemoryOptions{Now: func() time.Time { return now }})
	job, _, err := m.Enqueue(context.Background(), &RunRecord{
		IdempotencyKey: "k-art",
		RequestJSON:    json.RawMessage(`{"kernel":"go"}`),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return m, arts, job.ID
}

func validArtifact(runID string, runSpecHash string) *ArtifactRecord {
	seed := int64(11)
	return &ArtifactRecord{
		RunID:               runID,
		StrategyRef:         "momentum@deadbeef",
		DatasetSnapshotHash: "dataset-hash",
		RiskRulesHash:       "",
		ArtifactURI:         "mem://artifacts/x",
		ArtifactHash:        "artifact-hash",
		VersionHash:         "version-hash",
		RunSpecHash:         runSpecHash,
		ParamsHash:          "params-hash",
		ParamsJSON:          json.RawMessage(`{"b":2,"a":1}`),
		Seed:                &seed,
		Kernel:              "go",
		EngineVersion:       "engine-1",
		FillTiming:          "next_open",
	}
}

func TestPrepareArtifact_RequiresTheIdentityFields(t *testing.T) {
	cases := []struct {
		name string
		drop func(*ArtifactRecord)
	}{
		{"run_id", func(r *ArtifactRecord) { r.RunID = "" }},
		{"strategy_ref", func(r *ArtifactRecord) { r.StrategyRef = "" }},
		{"dataset_snapshot_hash", func(r *ArtifactRecord) { r.DatasetSnapshotHash = "" }},
		{"run_spec_hash", func(r *ArtifactRecord) { r.RunSpecHash = "" }},
		{"params_hash", func(r *ArtifactRecord) { r.ParamsHash = "" }},
		{"kernel", func(r *ArtifactRecord) { r.Kernel = "" }},
		{"engine_version", func(r *ArtifactRecord) { r.EngineVersion = "" }},
		{"fill_timing", func(r *ArtifactRecord) { r.FillTiming = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := validArtifact("run-1", "spec-hash")
			tc.drop(rec)
			err := PrepareArtifact(rec)
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("err = %v, want ErrInvalidInput", err)
			}
		})
	}
	// risk_rules_hash 允许空（未登记），artifact 指针允许空（内置策略）。
	rec := validArtifact("run-1", "spec-hash")
	rec.RiskRulesHash = ""
	rec.ArtifactURI = ""
	rec.ArtifactHash = ""
	rec.VersionHash = ""
	if err := PrepareArtifact(rec); err != nil {
		t.Fatalf("optional fields must be allowed: %v", err)
	}
}

func TestPrepareArtifact_CanonicalizesParamsAndDefaultsToEmptyObject(t *testing.T) {
	rec := validArtifact("run-1", "spec-hash")
	rec.ParamsJSON = json.RawMessage(`{"b": 2, "a": 1}`)
	if err := PrepareArtifact(rec); err != nil {
		t.Fatal(err)
	}
	if string(rec.ParamsJSON) != `{"a":1,"b":2}` {
		t.Fatalf("params_json = %s, want canonical（键序无关的身份）", rec.ParamsJSON)
	}

	rec.ParamsJSON = nil
	if err := PrepareArtifact(rec); err != nil {
		t.Fatal(err)
	}
	if string(rec.ParamsJSON) != `{}` {
		t.Fatalf("empty params_json = %s, want {}", rec.ParamsJSON)
	}
}

func TestMemoryArtifacts_SaveAndGetRoundTrip(t *testing.T) {
	_, arts, runID := newArtifactFixture(t)
	rec := validArtifact(runID, "spec-hash")
	rec.ParamsJSON = json.RawMessage(`{"lookback":20}`)

	saved, err := arts.SaveArtifact(context.Background(), rec)
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	if saved.CreatedAt.IsZero() {
		t.Fatal("created_at 必须由实现写入")
	}
	got, err := arts.GetArtifact(context.Background(), runID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got.ParamsJSON) != `{"lookback":20}` || got.Seed == nil || *got.Seed != 11 {
		t.Fatalf("round trip = %+v", got)
	}

	// 出参是副本：改返回值不能改仓里的行。
	got.ParamsJSON[0] = 'X'
	*got.Seed = 99
	again, err := arts.GetArtifact(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if again.ParamsJSON[0] == 'X' || *again.Seed != 11 {
		t.Fatalf("仓里的行被出参别名改写: %+v", again)
	}
}

func TestMemoryArtifacts_SaveIsIdempotentForTheSameIdentity(t *testing.T) {
	_, arts, runID := newArtifactFixture(t)
	first, err := arts.SaveArtifact(context.Background(), validArtifact(runID, "spec-hash"))
	if err != nil {
		t.Fatal(err)
	}
	// 键序不同但归一后相同 → 同身份（params_json 已 canonical）。
	again := validArtifact(runID, "spec-hash")
	again.ParamsJSON = json.RawMessage(`{"a": 1, "b": 2}`)
	second, err := arts.SaveArtifact(context.Background(), again)
	if err != nil {
		t.Fatalf("same identity must be a no-op, got %v", err)
	}
	if !second.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("重放不得刷新 created_at: %v → %v", first.CreatedAt, second.CreatedAt)
	}
}

func TestMemoryArtifacts_IdentityDriftIsLoud(t *testing.T) {
	_, arts, runID := newArtifactFixture(t)
	if _, err := arts.SaveArtifact(context.Background(), validArtifact(runID, "spec-hash")); err != nil {
		t.Fatal(err)
	}

	drift := validArtifact(runID, "spec-hash")
	drift.DatasetSnapshotHash = "other-dataset"
	_, err := arts.SaveArtifact(context.Background(), drift)
	if !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("err = %v, want ErrArtifactConflict（禁止静默覆盖三 hash）", err)
	}
	// 原行一字不动。
	got, err := arts.GetArtifact(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if got.DatasetSnapshotHash != "dataset-hash" {
		t.Fatalf("冲突写覆盖了原留痕: %+v", got)
	}
}

func TestMemoryArtifacts_UnknownRunIsRejectedLikeTheForeignKey(t *testing.T) {
	_, arts, _ := newArtifactFixture(t)
	if _, err := arts.SaveArtifact(context.Background(), validArtifact("no-such-run", "spec-hash")); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("save to unknown run: err = %v, want ErrJobNotFound（与 PG 的 FK 同口径）", err)
	}
	if _, err := arts.GetArtifact(context.Background(), "no-such-run"); !errors.Is(err, ErrArtifactNotFound) {
		t.Fatalf("get unknown run: err = %v, want ErrArtifactNotFound", err)
	}
}

func TestMemoryArtifacts_NilRecordIsInvalidInput(t *testing.T) {
	_, arts, _ := newArtifactFixture(t)
	if _, err := arts.SaveArtifact(context.Background(), nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("err = %v, want ErrInvalidInput", err)
	}
}
