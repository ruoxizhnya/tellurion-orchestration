package pgstore_test

// `run.run_artifacts` 的 PG 腿：与 internal/queue/artifact_test.go 同一批语义
// （未知 run 拒写 / 同身份幂等 / 身份漂移响亮 / 未知 run 读 404），外加 JSONB
// 往返这一条 PG 专属断言。
//
// 无库即 skip（CI 无库时的预期状态）；收工结论必须带真库输出。

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"tellurion-orchestration/internal/pgstore"
	"tellurion-orchestration/internal/queue"
)

func pgArtifact(runID, runSpecHash string) *queue.ArtifactRecord {
	seed := int64(5)
	return &queue.ArtifactRecord{
		RunID:               runID,
		StrategyRef:         "momentum@deadbeef",
		DatasetSnapshotHash: "dataset-hash",
		RiskRulesHash:       "",
		ArtifactURI:         "mem://artifacts/pg",
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

func TestPGArtifacts_SaveGetIdempotentAndLoud(t *testing.T) {
	store := newStore(t)
	ctx := t.Context()
	artifacts := store.Artifacts()
	run, _, err := store.Queue(nil, pgstore.Options{}).Enqueue(ctx, &queue.RunRecord{
		IdempotencyKey: "k-art",
		RequestJSON:    json.RawMessage(`{"kernel":"go"}`),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	saved, err := artifacts.SaveArtifact(ctx, pgArtifact(run.ID, run.RequestHash))
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	if saved.CreatedAt.IsZero() || saved.RunID != run.ID {
		t.Fatalf("saved = %+v", saved)
	}

	// JSONB 会规范化字节（`{"a":1}` 读回 `{"a": 1}`），所以断言语义相等：
	// 身份比较在 SQL 里也是 jsonb 语义比较，不是字节比较。
	got, err := artifacts.GetArtifact(ctx, run.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	wantCanon, err := queue.Canonicalize(json.RawMessage(`{"a":1,"b":2}`))
	if err != nil {
		t.Fatal(err)
	}
	gotCanon, err := queue.Canonicalize(got.ParamsJSON)
	if err != nil {
		t.Fatalf("params_json 读回不是合法 JSON: %v (%s)", err, got.ParamsJSON)
	}
	if !bytes.Equal(gotCanon, wantCanon) || got.Seed == nil || *got.Seed != 5 {
		t.Fatalf("round trip = %+v (params %s)", got, got.ParamsJSON)
	}
	if got.ArtifactURI != "mem://artifacts/pg" || got.VersionHash != "version-hash" {
		t.Fatalf("nullable columns = %+v", got)
	}

	// 同身份重写：幂等，created_at 不刷新。
	again, err := artifacts.SaveArtifact(ctx, pgArtifact(run.ID, run.RequestHash))
	if err != nil {
		t.Fatalf("same identity must be a no-op: %v", err)
	}
	if !again.CreatedAt.Equal(saved.CreatedAt) {
		t.Fatalf("重放刷新了 created_at: %v → %v", saved.CreatedAt, again.CreatedAt)
	}

	// 身份漂移：响亮，且原行一字不动。
	drift := pgArtifact(run.ID, run.RequestHash)
	drift.DatasetSnapshotHash = "other-dataset"
	if _, err := artifacts.SaveArtifact(ctx, drift); !errors.Is(err, queue.ErrArtifactConflict) {
		t.Fatalf("err = %v, want ErrArtifactConflict", err)
	}
	after, err := artifacts.GetArtifact(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.DatasetSnapshotHash != "dataset-hash" {
		t.Fatalf("冲突写覆盖了原留痕: %+v", after)
	}
}

func TestPGArtifacts_UnknownRunIsRejectedByTheForeignKey(t *testing.T) {
	store := newStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	artifacts := store.Artifacts()

	if _, err := artifacts.SaveArtifact(ctx, pgArtifact("no-such-run", "spec-hash")); !errors.Is(err, queue.ErrJobNotFound) {
		t.Fatalf("save to unknown run: err = %v, want ErrJobNotFound（FK 23503 映射）", err)
	}
	if _, err := artifacts.GetArtifact(ctx, "no-such-run"); !errors.Is(err, queue.ErrArtifactNotFound) {
		t.Fatalf("get unknown run: err = %v, want ErrArtifactNotFound", err)
	}
}

func TestPGArtifacts_NullableColumnsRoundTripAsNull(t *testing.T) {
	store := newStore(t)
	ctx := t.Context()
	run, _, err := store.Queue(nil, pgstore.Options{}).Enqueue(ctx, &queue.RunRecord{
		IdempotencyKey: "k-art-null",
		RequestJSON:    json.RawMessage(`{"kernel":"go"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := pgArtifact(run.ID, run.RequestHash)
	rec.ArtifactURI = ""
	rec.ArtifactHash = ""
	rec.VersionHash = ""
	rec.Seed = nil
	if _, err := store.Artifacts().SaveArtifact(ctx, rec); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := store.Artifacts().GetArtifact(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ArtifactURI != "" || got.ArtifactHash != "" || got.VersionHash != "" || got.Seed != nil {
		t.Fatalf("nullable columns 读回 = %+v", got)
	}
	// 空串 = 未登记的 risk_rules_hash 是 NOT NULL 列，读回必须还是空串。
	if got.RiskRulesHash != "" {
		t.Fatalf("risk_rules_hash = %q, want \"\"", got.RiskRulesHash)
	}
}
