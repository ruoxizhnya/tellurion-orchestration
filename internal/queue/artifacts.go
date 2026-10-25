package queue

// X0（B6-4）：`run.run_artifacts` 的 port、DTO 与入参校验（docs/db-schema.md §5 /
// ARCHITECTURE.md §9.5）。
//
// 三 hash 是 run **开始之后**才产出的身份（docs/api-schema.md §2 fail-closed ④），
// 所以它们不进 `JobQueue` 的冻结方法集，也不进入队校验：这里单开一个可选接口，
// 与 `KernelAwareJobQueue` / `ParentChildLister` 同一种做法（§9.6：加可选能力 =
// MINOR，冻结的 10 方法集一个字不动）。
//
// 表**没有 status**：结论只从 `run.backtest_jobs` 取（单一同源，QT-63 ④）。

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// ArtifactRecord 是 `run.run_artifacts` 的一行。
type ArtifactRecord struct {
	RunID               string          `json:"run_id"`
	StrategyRef         string          `json:"strategy_ref"`
	DatasetSnapshotHash string          `json:"dataset_snapshot_hash"`
	RiskRulesHash       string          `json:"risk_rules_hash"`
	ArtifactURI         string          `json:"artifact_uri,omitempty"`
	ArtifactHash        string          `json:"artifact_hash,omitempty"`
	VersionHash         string          `json:"version_hash,omitempty"`
	RunSpecHash         string          `json:"run_spec_hash"`
	ParamsHash          string          `json:"params_hash"`
	ParamsJSON          json.RawMessage `json:"params_json,omitempty"`
	Seed                *int64          `json:"seed,omitempty"`
	Kernel              string          `json:"kernel"`
	EngineVersion       string          `json:"engine_version"`
	FillTiming          string          `json:"fill_timing"`
	CreatedAt           time.Time       `json:"created_at"`
}

// Clone 深拷贝（ParamsJSON 是切片，Seed 是指针——出库/入参都不能共享底层数组）。
func (a ArtifactRecord) Clone() ArtifactRecord {
	out := a
	if a.ParamsJSON != nil {
		out.ParamsJSON = append(json.RawMessage(nil), a.ParamsJSON...)
	}
	if a.Seed != nil {
		v := *a.Seed
		out.Seed = &v
	}
	return out
}

// ArtifactStore 是 `run.run_artifacts` 的读写门（X0 载体）。
//
// 错误语义：
//   - 未知 run_id 读 → ErrArtifactNotFound（由 HTTP 层按 job 的两轴区分 404/409/500）；
//   - 写一个不存在的 run → ErrJobNotFound（PG 侧是 FK、内存侧显式查——同一个报错，
//     否则同一段代码换实现就换了行为）；
//   - 同 run_id 身份漂移 → ErrArtifactConflict（**不覆盖**：三 hash 是坐标，
//     覆盖等于把两次 run 的留痕并成一次）；
//   - 同 run_id 身份全同 → 返回原行、不报错（worker 重试会重复写一次）。
type ArtifactStore interface {
	SaveArtifact(ctx context.Context, rec *ArtifactRecord) (*ArtifactRecord, error)
	GetArtifact(ctx context.Context, runID string) (*ArtifactRecord, error)
}

// PrepareArtifact 是**两个实现共用**的入参校验与归一。
//
// 必填项就是「没有它这条留痕就没有意义」的那些：run_id（FK 指向）、策略引用、
// 数据集身份、run_spec_hash、params_hash、内核三件套（kernel / engine_version /
// fill_timing）。`risk_rules_hash` 允许空串——空串 = 未登记（QT-42 X3b 之前，
// 见 ARCHITECTURE §9.5），不是「无规则」的假值。artifact 指针可空：内置策略没有
// 独立 artifact。
//
// params_json 归一到 canonical 字节：身份比较必须只依赖内容、不依赖键序。
func PrepareArtifact(rec *ArtifactRecord) error {
	if rec == nil {
		return fmt.Errorf("%w: nil artifact record", ErrInvalidInput)
	}
	required := []struct {
		name  string
		value string
	}{
		{"run_id", rec.RunID},
		{"strategy_ref", rec.StrategyRef},
		{"dataset_snapshot_hash", rec.DatasetSnapshotHash},
		{"run_spec_hash", rec.RunSpecHash},
		{"params_hash", rec.ParamsHash},
		{"kernel", rec.Kernel},
		{"engine_version", rec.EngineVersion},
		{"fill_timing", rec.FillTiming},
	}
	for _, f := range required {
		if f.value == "" {
			return fmt.Errorf("%w: %s is required", ErrInvalidInput, f.name)
		}
	}
	if len(bytes.TrimSpace(rec.ParamsJSON)) == 0 {
		rec.ParamsJSON = json.RawMessage("{}")
		return nil
	}
	canonical, err := Canonicalize(rec.ParamsJSON)
	if err != nil {
		return fmt.Errorf("%w: params_json: %v", ErrInvalidInput, err)
	}
	rec.ParamsJSON = canonical
	return nil
}

// sameArtifactIdentity 报告两条记录是否指同一次 run 的同一份留痕。
//
// 身份 = 全部内容字段（含 params_json 与 seed），不含 created_at：时间戳不是身份，
// 重试写回同一条留痕不该因为毫秒差被判成漂移。
func sameArtifactIdentity(a, b *ArtifactRecord) bool {
	if a.RunID != b.RunID ||
		a.StrategyRef != b.StrategyRef ||
		a.DatasetSnapshotHash != b.DatasetSnapshotHash ||
		a.RiskRulesHash != b.RiskRulesHash ||
		a.ArtifactURI != b.ArtifactURI ||
		a.ArtifactHash != b.ArtifactHash ||
		a.VersionHash != b.VersionHash ||
		a.RunSpecHash != b.RunSpecHash ||
		a.ParamsHash != b.ParamsHash ||
		a.Kernel != b.Kernel ||
		a.EngineVersion != b.EngineVersion ||
		a.FillTiming != b.FillTiming {
		return false
	}
	if !bytes.Equal(a.ParamsJSON, b.ParamsJSON) {
		return false
	}
	switch {
	case a.Seed == nil && b.Seed == nil:
		return true
	case a.Seed == nil || b.Seed == nil:
		return false
	default:
		return *a.Seed == *b.Seed
	}
}
