package queue

import (
	"strings"
	"testing"
)

func TestMigrationsCoverAllRunTables(t *testing.T) {
	stmts := Migrations()
	if len(stmts) == 0 {
		t.Fatal("no migration statements")
	}
	if !strings.HasPrefix(strings.TrimSpace(stmts[0]), "CREATE SCHEMA IF NOT EXISTS run") {
		t.Fatalf("first statement must create the run schema, got %q", stmts[0])
	}

	// 六张表（docs/db-schema.md §1；parent_jobs 是裁-13 新增）：少一张就是没落地，
	// 多一张要走 ADR。
	want := []string{
		"run.backtest_jobs",
		"run.run_artifacts",
		"run.batch_runs",
		"run.walk_forward_reports",
		"run.worker_registry",
		"run.parent_jobs",
	}
	joined := strings.Join(stmts, "\n")
	for _, table := range want {
		if !strings.Contains(joined, "CREATE TABLE IF NOT EXISTS "+table) {
			t.Fatalf("missing DDL for %s", table)
		}
	}
	if n := strings.Count(joined, "CREATE TABLE IF NOT EXISTS"); n != len(want) {
		t.Fatalf("DDL declares %d tables, want %d", n, len(want))
	}

	// 每条都必须可重入（空库一次跑完 + 重复启动不报错）。
	for _, s := range stmts {
		normalized := strings.Join(strings.Fields(s), " ")
		if !strings.Contains(normalized, "IF NOT EXISTS") {
			t.Fatalf("statement is not re-runnable: %s", s)
		}
	}
	// 创建索引的语句也必须带 IF NOT EXISTS（上面已覆盖，这里单列判据防误改）。
	// 断言到 `ON <table>` 为止：只比索引前缀时，`…_idx_LEG3` 这类改名会假绿。
	normalized := normalizedOf(joined)
	if !strings.Contains(normalized, "CREATE UNIQUE INDEX IF NOT EXISTS backtest_jobs_idempotency_key_uidx ON run.backtest_jobs") {
		t.Fatal("idempotency key unique index is missing")
	}
	// 裁-13：子 run 关系是一列 + 索引，不是父表里的 request_json。
	if !strings.Contains(normalized, "ALTER TABLE run.backtest_jobs ADD COLUMN IF NOT EXISTS parent_job_id") {
		t.Fatal("backtest_jobs.parent_job_id column is missing")
	}
	if !strings.Contains(normalized, "CREATE INDEX IF NOT EXISTS backtest_jobs_parent_idx ON run.backtest_jobs") {
		t.Fatal("backtest_jobs.parent_job_id index is missing")
	}
}

// normalizedOf 把一段 SQL 的空白折成单空格，便于子串断言（DDL 里有换行与缩进）。
func normalizedOf(s string) string { return strings.Join(strings.Fields(s), " ") }

func TestMigrationsReturnsCopy(t *testing.T) {
	first := Migrations()
	first[0] = "DROP SCHEMA run CASCADE"

	second := Migrations()
	if second[0] == "DROP SCHEMA run CASCADE" {
		t.Fatal("Migrations must return a copy; caller mutated the source slice")
	}
}
