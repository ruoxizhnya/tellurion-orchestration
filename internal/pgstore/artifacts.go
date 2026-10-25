package pgstore

// `run.run_artifacts` 的 PostgreSQL 实现（X0，B6-4）。
//
// 与 queue.MemoryArtifacts 同口径的四条：未知 run → ErrJobNotFound（这里由外键
// 兜底）、同身份重写幂等、身份漂移 → ErrArtifactConflict、未知 run 读 →
// ErrArtifactNotFound。
//
// 「同身份幂等 / 漂移响亮」用 `ON CONFLICT (run_id) DO UPDATE ... WHERE <身份全等>`
// 实现：身份不同时 WHERE 为假 ⇒ 0 行受影响 ⇒ 报冲突，**不覆盖**已有留痕。
// 这比「先查再写」少一次竞态（两个 worker 同时收尾时只有一个能写进去）。

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"tellurion-orchestration/internal/queue"
)

// artifactColumns 是读写 ArtifactRecord 的列清单（顺序 = rowToArtifact 的 Scan 顺序）。
const artifactColumns = `run_id, strategy_ref, dataset_snapshot_hash, risk_rules_hash,
	artifact_uri, artifact_hash, version_hash, run_spec_hash, params_hash, params_json,
	seed, kernel, engine_version, fill_timing, created_at`

// saveArtifactSQL 写入 / 幂等重放一次留痕。
//
// WHERE 里的每个字段都是身份的一部分；`IS NOT DISTINCT FROM` 用于可空列
// （artifact_uri / artifact_hash / version_hash / seed），否则 NULL = NULL 为假会
// 把「两次都没填」误判成漂移。
var saveArtifactSQL = `
	INSERT INTO run.run_artifacts
		(run_id, strategy_ref, dataset_snapshot_hash, risk_rules_hash,
		 artifact_uri, artifact_hash, version_hash, run_spec_hash, params_hash, params_json,
		 seed, kernel, engine_version, fill_timing, created_at)
	VALUES
		($1, $2, $3, $4, $5, $6, $7, $8, $9, CAST($10::text AS jsonb),
		 $11, $12, $13, $14, $15)
	ON CONFLICT (run_id) DO UPDATE SET run_id = EXCLUDED.run_id
	WHERE run.run_artifacts.strategy_ref = EXCLUDED.strategy_ref
	  AND run.run_artifacts.dataset_snapshot_hash = EXCLUDED.dataset_snapshot_hash
	  AND run.run_artifacts.risk_rules_hash = EXCLUDED.risk_rules_hash
	  AND run.run_artifacts.artifact_uri IS NOT DISTINCT FROM EXCLUDED.artifact_uri
	  AND run.run_artifacts.artifact_hash IS NOT DISTINCT FROM EXCLUDED.artifact_hash
	  AND run.run_artifacts.version_hash IS NOT DISTINCT FROM EXCLUDED.version_hash
	  AND run.run_artifacts.run_spec_hash = EXCLUDED.run_spec_hash
	  AND run.run_artifacts.params_hash = EXCLUDED.params_hash
	  AND run.run_artifacts.params_json = EXCLUDED.params_json
	  AND run.run_artifacts.seed IS NOT DISTINCT FROM EXCLUDED.seed
	  AND run.run_artifacts.kernel = EXCLUDED.kernel
	  AND run.run_artifacts.engine_version = EXCLUDED.engine_version
	  AND run.run_artifacts.fill_timing = EXCLUDED.fill_timing`

// Artifacts 是 queue.ArtifactStore 的 PG 实现。
type Artifacts struct{ pool *pgxpool.Pool }

// 编译期断言：方法集少一个就是实现方全红。
var _ queue.ArtifactStore = (*Artifacts)(nil)

// Artifacts 返回 ArtifactStore 的 PG 实现。
func (s *Store) Artifacts() *Artifacts { return &Artifacts{pool: s.pool} }

func rowToArtifact(row pgx.Row) (*queue.ArtifactRecord, error) {
	var (
		rec                                 queue.ArtifactRecord
		artifactURI, artifactHash, versionH sql.NullString
		params                              []byte
		seed                                sql.NullInt64
		createdAt                           sql.NullTime
	)
	err := row.Scan(&rec.RunID, &rec.StrategyRef, &rec.DatasetSnapshotHash, &rec.RiskRulesHash,
		&artifactURI, &artifactHash, &versionH, &rec.RunSpecHash, &rec.ParamsHash, &params,
		&seed, &rec.Kernel, &rec.EngineVersion, &rec.FillTiming, &createdAt)
	if err != nil {
		return nil, fmt.Errorf("pgstore: scan run artifacts: %w", err)
	}
	rec.ArtifactURI = artifactURI.String
	rec.ArtifactHash = artifactHash.String
	rec.VersionHash = versionH.String
	if params != nil {
		rec.ParamsJSON = append([]byte(nil), params...)
	}
	if seed.Valid {
		v := seed.Int64
		rec.Seed = &v
	}
	if createdAt.Valid {
		rec.CreatedAt = createdAt.Time.UTC()
	}
	return &rec, nil
}

// SaveArtifact 见 queue.ArtifactStore 的契约注释。
func (a *Artifacts) SaveArtifact(ctx context.Context, rec *queue.ArtifactRecord) (*queue.ArtifactRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, fmt.Errorf("%w: nil artifact record", queue.ErrInvalidInput)
	}
	in := rec.Clone()
	if err := queue.PrepareArtifact(&in); err != nil {
		return nil, err
	}
	createdAt := in.CreatedAt
	if createdAt.IsZero() {
		createdAt = timeNow()
	} else {
		createdAt = createdAt.UTC()
	}

	var seed any
	if in.Seed != nil {
		seed = *in.Seed
	}
	tag, err := a.pool.Exec(ctx, saveArtifactSQL,
		in.RunID, in.StrategyRef, in.DatasetSnapshotHash, in.RiskRulesHash,
		nullableText(in.ArtifactURI), nullableText(in.ArtifactHash), nullableText(in.VersionHash),
		in.RunSpecHash, in.ParamsHash, string(in.ParamsJSON),
		seed, in.Kernel, in.EngineVersion, in.FillTiming, createdAt)
	if err != nil {
		if isForeignKeyViolation(err) {
			// 与内存实现同一条报错：留痕不能指向不存在的 run。
			return nil, fmt.Errorf("%w: %s", queue.ErrJobNotFound, in.RunID)
		}
		return nil, fmt.Errorf("pgstore: save run artifacts: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil, fmt.Errorf("%w: %s already has different artifacts", queue.ErrArtifactConflict, in.RunID)
	}
	return a.GetArtifact(ctx, in.RunID)
}

// GetArtifact 见 queue.ArtifactStore 的契约注释。
func (a *Artifacts) GetArtifact(ctx context.Context, runID string) (*queue.ArtifactRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	row := a.pool.QueryRow(ctx, `SELECT `+artifactColumns+` FROM run.run_artifacts WHERE run_id = $1`, runID)
	rec, err := rowToArtifact(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", queue.ErrArtifactNotFound, runID)
	}
	if err != nil {
		return nil, err
	}
	return rec, nil
}
