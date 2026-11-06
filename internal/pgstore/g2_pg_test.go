package pgstore

// SVC-ORCH-G2 ②：迁移可重放。真实 PG 腿用 ORCHESTRATION_TEST_DSN 触发；
// 未设则 skip（台账必须写「未执行」，不许标 ✅）。

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"
)

const testDSNEnv = "ORCHESTRATION_TEST_DSN"

var runTablesForFingerprint = []string{
	"backtest_jobs", "run_artifacts", "batch_runs", "walk_forward_reports", "worker_registry", "parent_jobs",
}

// TestPGMigrationsAreReplayable 同一 DSN 上跑两遍 Migrate，比对 schema 指纹：
// 列 / 索引 / 六表行数任一变化 = 迁移不幂等或数据被改写。
func TestPGMigrationsAreReplayable(t *testing.T) {
	dsn := os.Getenv(testDSNEnv)
	if dsn == "" {
		t.Skipf("%s not set: real-PostgreSQL migration replay leg not executed", testDSNEnv)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	store, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	before, err := schemaFingerprint(ctx, store)
	if err != nil {
		t.Fatalf("fingerprint before: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	after, err := schemaFingerprint(ctx, store)
	if err != nil {
		t.Fatalf("fingerprint after: %v", err)
	}
	if before != after {
		t.Fatalf("migrations are not replayable: %s → %s", before, after)
	}
}

func schemaFingerprint(ctx context.Context, store *Store) (string, error) {
	h := sha256.New()

	rows, err := store.pool.Query(ctx,
		`SELECT table_name || '.' || column_name || ':' || data_type || ':' || is_nullable
		   FROM information_schema.columns
		  WHERE table_schema = 'run'
		  ORDER BY 1`)
	if err != nil {
		return "", fmt.Errorf("columns: %w", err)
	}
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			rows.Close()
			return "", err
		}
		fmt.Fprintln(h, line)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return "", err
	}

	idx, err := store.pool.Query(ctx,
		`SELECT indexname || ':' || indexdef FROM pg_indexes WHERE schemaname = 'run' ORDER BY 1`)
	if err != nil {
		return "", fmt.Errorf("indexes: %w", err)
	}
	for idx.Next() {
		var line string
		if err := idx.Scan(&line); err != nil {
			idx.Close()
			return "", err
		}
		fmt.Fprintln(h, line)
	}
	idx.Close()
	if err := idx.Err(); err != nil {
		return "", err
	}

	for _, table := range runTablesForFingerprint {
		var n int64
		if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM run.`+table).Scan(&n); err != nil {
			return "", fmt.Errorf("count %s: %w", table, err)
		}
		fmt.Fprintf(h, "%s=%d\n", table, n)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
