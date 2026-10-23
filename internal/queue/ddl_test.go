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

	// 五张表（docs/db-schema.md §1）：少一张就是没落地，多一张要走 ADR。
	want := []string{
		"run.backtest_jobs",
		"run.run_artifacts",
		"run.batch_runs",
		"run.walk_forward_reports",
		"run.worker_registry",
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
	if !strings.Contains(joined, "CREATE UNIQUE INDEX IF NOT EXISTS backtest_jobs_idempotency_key_uidx") {
		t.Fatal("idempotency key unique index is missing")
	}
}

func TestMigrationsReturnsCopy(t *testing.T) {
	first := Migrations()
	first[0] = "DROP SCHEMA run CASCADE"

	second := Migrations()
	if second[0] == "DROP SCHEMA run CASCADE" {
		t.Fatal("Migrations must return a copy; caller mutated the source slice")
	}
}
