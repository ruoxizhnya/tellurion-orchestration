package pgstore

// 内部测试（package pgstore）：只有实现自己能看到 jobCols。
//
// 这条腿钉的是「实现读的列」与「库里有的列」不能各写各的——
// 加一列忘了改另一处，坏法是最难查的那种：不报错，只是错位读成别的字段。

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestJobColumnsMatchTheTableShape(t *testing.T) {
	dsn := os.Getenv("ORCHESTRATION_TEST_DSN")
	if dsn == "" {
		t.Skip("ORCHESTRATION_TEST_DSN is not set; skipping PostgreSQL legs")
	}
	ctx := context.Background()
	store, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	have := map[string]bool{}
	rows, err := store.pool.Query(ctx, `
		SELECT column_name FROM information_schema.columns
		WHERE table_schema = 'run' AND table_name = 'backtest_jobs'`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		have[n] = true
	}
	rows.Close()

	// 正向腿：port 读的每一列都必须在库里。
	for _, c := range jobCols {
		if !have[c] {
			t.Fatalf("列 %q 被 port 读取但库里没有（jobCols 与 run.backtest_jobs 漂移）", c)
		}
	}
	// 反向腿：库里的每一列都得在 port 视野内，唯一的例外是 request_json
	// （读路径刻意避开它，见 hashanchor_test.go）。少一列说明有人加了列
	// 却没接进 RunRecord——那会静默丢数据。
	for name := range have {
		if name == "request_json" {
			continue
		}
		if !strings.Contains(columnsOf(","), name) {
			t.Fatalf("库里有的列 %q 不在 port 的读取列表里（加列必须同批改 jobCols 与 rowToJob）", name)
		}
	}
	if len(have) != len(jobCols)+1 {
		t.Fatalf("backtest_jobs 列数 = %d, want %d（jobCols %d + request_json）",
			len(have), len(jobCols)+1, len(jobCols))
	}
}

func TestColumnsOfQualifiesEveryColumn(t *testing.T) {
	// UPDATE ... FROM 里裸 id 是歧义的（外层表 j 与子查询 c 都有 id）——
	// 这是 2026-10-23 真库跑出来的第一条红，形状由这条钉住。
	if columnsOf("j") == columnsOf("") {
		t.Fatal(`columnsOf("j") must qualify`)
	}
	if columnsOf("j.") != columnsOf("j") {
		t.Fatalf("alias must be accepted with and without the dot:\n %s\n %s", columnsOf("j."), columnsOf("j"))
	}
	qualified := strings.Split(columnsOf("j."), ", ")
	if len(qualified) != len(jobCols) {
		t.Fatalf("qualified list has %d entries, want %d", len(qualified), len(jobCols))
	}
	for i, c := range jobCols {
		if qualified[i] != "j."+c {
			t.Fatalf("entry %d = %q, want %q（顺序必须与 jobCols 一致，Scan 按位置对齐）", i, qualified[i], "j."+c)
		}
	}
}
