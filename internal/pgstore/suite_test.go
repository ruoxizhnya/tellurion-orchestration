package pgstore_test

// PG 侧的 harness：把 qtest 套件（与内存实现**同一批**用例）跑在真实库上。
//
// 没有库就 skip，而不是假绿——但反过来，skip 也不是「已经通过」：
// 本单的收工结论必须带真库跑过的输出（见 docs/TASKS.md 施工 ③ 记录）。
// 复现：
//
//	ORCHESTRATION_TEST_DSN='postgres://postgres:postgres@127.0.0.1:5432/tellurion_orchestration_test?sslmode=disable' \
//	  go test ./internal/pgstore -count=1

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"tellurion-orchestration/internal/pgstore"
	"tellurion-orchestration/internal/qtest"
	"tellurion-orchestration/internal/queue"
	"tellurion-orchestration/internal/testdb"
)

// harness 是整个测试进程共用的一套：抢锁 → 建池 → 建表。
//
// 锁是必需的，不是锦上添花：`go test ./...` 会并行跑不同**包**的测试二进制，
// 而 PG 腿共用同一个库——没有锁时本包的「用例前清库」会删掉
// cmd/orchestrationservice 正在断言的行（实测：幂等重放莫名变成 202 新建）。
var harness *testdb.Harness

func TestMain(m *testing.M) {
	dsn := os.Getenv("ORCHESTRATION_TEST_DSN")
	if dsn != "" {
		h, err := testdb.Setup(context.Background(), dsn, 3*time.Minute)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pgstore: %v\n", err)
			os.Exit(1)
		}
		harness = h
	}
	code := m.Run()
	harness.Close()
	os.Exit(code)
}

// newStore 返回已建表的 Store，并把 run schema 清空——用例之间必须彼此看不见。
func newStore(t *testing.T) *pgstore.Store {
	t.Helper()
	if harness == nil {
		t.Skip("ORCHESTRATION_TEST_DSN is not set; skipping PostgreSQL legs（CI 无库时的预期状态）")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := harness.Reset(ctx); err != nil {
		t.Fatalf("%v", err)
	}
	return harness.Store()
}

func jobQueue(t *testing.T, s qtest.Spec) queue.JobQueue {
	t.Helper()
	store := newStore(t)
	var reg *pgstore.Registry
	if s.Workers != nil {
		reg = store.Registry()
	}
	return store.Queue(reg, pgstore.Options{MaxQueueDepth: s.MaxQueueDepth, DefaultMaxAttempts: s.DefaultMaxAttempts})
}

func TestPGSatisfiesJobQueueSuite(t *testing.T) {
	qtest.RunJobQueueSuite(t, "PG", jobQueue)
}

func TestPGSatisfiesWorkerRegistrySuite(t *testing.T) {
	qtest.RunWorkerRegistrySuite(t, "PG", func(t *testing.T) queue.WorkerRegistry {
		return newStore(t).Registry()
	})
}

func TestPGSatisfiesPairSuite(t *testing.T) {
	qtest.RunPairSuite(t, "PG", func(t *testing.T, s qtest.Spec) qtest.Pair {
		store := newStore(t)
		reg := store.Registry()
		return qtest.Pair{
			Jobs:    store.Queue(reg, pgstore.Options{MaxQueueDepth: s.MaxQueueDepth, DefaultMaxAttempts: s.DefaultMaxAttempts}),
			Workers: reg,
		}
	})
}
