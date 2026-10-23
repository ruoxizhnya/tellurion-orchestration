package queue_test

// 内存实现跑 qtest 套件：它是**参照实现**，套件先在它身上绿，
// PG 侧的绿才有意义（否则只是把同一个错误测了两遍）。
// 文件是 external test package（queue_test 而非 queue）：qtest 反向 import
// queue，内部测试包引用它会成环。

import (
	"testing"

	"tellurion-orchestration/internal/qtest"
	"tellurion-orchestration/internal/queue"
)

func memoryFactory(t *testing.T, s qtest.Spec) queue.JobQueue {
	t.Helper()
	return queue.NewMemory(queue.MemoryOptions{
		MaxQueueDepth:      s.MaxQueueDepth,
		DefaultMaxAttempts: s.DefaultMaxAttempts,
		Workers:            s.Workers,
	})
}

func TestMemorySatisfiesJobQueueSuite(t *testing.T) {
	qtest.RunJobQueueSuite(t, "Memory", memoryFactory)
}

func TestMemorySatisfiesWorkerRegistrySuite(t *testing.T) {
	qtest.RunWorkerRegistrySuite(t, "Memory", func(t *testing.T) queue.WorkerRegistry {
		return queue.NewMemoryRegistry()
	})
}

func TestMemorySatisfiesPairSuite(t *testing.T) {
	qtest.RunPairSuite(t, "Memory", func(t *testing.T, s qtest.Spec) qtest.Pair {
		reg := queue.NewMemoryRegistry()
		return qtest.Pair{
			Jobs:    memoryFactory(t, qtest.Spec{MaxQueueDepth: s.MaxQueueDepth, DefaultMaxAttempts: s.DefaultMaxAttempts, Workers: reg}),
			Workers: reg,
		}
	})
}

// TestMemoryOptionsDefaultsAreShared 钉住「两个实现共用默认值解析」这条口径：
// 同一份部署配置不能让内存实现限 2 条、PG 实现不限深度。
func TestMemoryOptionsDefaultsAreShared(t *testing.T) {
	if got := queue.ResolveQueueDepth(0); got != queue.DefaultMaxQueueDepth {
		t.Fatalf("ResolveQueueDepth(0) = %d, want %d", got, queue.DefaultMaxQueueDepth)
	}
	if got := queue.ResolveQueueDepth(-1); got != queue.DefaultMaxQueueDepth {
		t.Fatalf("ResolveQueueDepth(-1) = %d, want %d", got, queue.DefaultMaxQueueDepth)
	}
	if got := queue.ResolveQueueDepth(7); got != 7 {
		t.Fatalf("ResolveQueueDepth(7) = %d, want 7", got)
	}
	if got := queue.ResolveMaxAttempts(0, 0); got != queue.DefaultMaxAttempts {
		t.Fatalf("ResolveMaxAttempts(0,0) = %d, want %d", got, queue.DefaultMaxAttempts)
	}
	if got := queue.ResolveMaxAttempts(5, 0); got != 5 {
		t.Fatalf("ResolveMaxAttempts(5,0) = %d, want 5", got)
	}
	if got := queue.ResolveMaxAttempts(5, 2); got != 2 {
		t.Fatalf("ResolveMaxAttempts(5,2) = %d, want 2（行上的显式值优先）", got)
	}
	if queue.NewRunID() == "" || len(queue.NewRunID()) != 32 {
		t.Fatal("NewRunID must yield a 32-hex id")
	}
	// 两条 id 必须不同（碰撞在这里不是理论问题：它是主键）。
	if queue.NewRunID() == queue.NewRunID() {
		t.Fatal("NewRunID returned the same id twice")
	}
}
