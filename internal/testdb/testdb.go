// Package testdb 是 PG 腿的测试 harness（不进生产路径，但也不是 _test.go：
// 两个包的测试都要用它，而 go test 是**按包并行**跑测试二进制的）。
//
// 它解决一个只在并行时才暴露的问题：两个包的 PG 腿共用同一个测试库，
// A 包的「用例开始前清空 run schema」会把 B 包正在断言的行一起清掉——
// 表现是 B 包的幂等重放莫名变成「新建」，且只在 go test ./... 下复现，
// 单跑一个包永远绿。2026-10-23 施工 ③ 真撞到过（见 docs/TASKS.md 记录）。
//
// 处置：整个测试进程启动时抢一把**会话级** advisory lock，抢不到就排队。
// 代价是 PG 腿串行跑（本机约 4 秒），换来的是不再有小概率假红。
package testdb

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"tellurion-orchestration/internal/pgstore"
)

// harnessLockKey 是 PG 腿的跨进程互斥键。与会话连接同寿命：Close 即释放。
const harnessLockKey = int64(0x5255_4E54) // "RUNT"

// Harness 持有一个专用连接（只为锁）与一个 Store。
type Harness struct {
	store *pgstore.Store
	lock  *pgx.Conn
}

// Setup 抢锁 → 建池 → 跑启动迁移。锁最长等 wait 时间；等不到就报错，
// 不静默降级成「没有隔离」。
func Setup(ctx context.Context, dsn string, wait time.Duration) (*Harness, error) {
	lockCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	conn, err := pgx.Connect(lockCtx, dsn)
	if err != nil {
		return nil, fmt.Errorf("testdb: connect for lock: %w", err)
	}
	// 先试非阻塞：拿不到就显式说明「另一个测试进程还占着库」，
	// 比让调用方盯着一个不动的终端强。
	_, err = conn.Exec(lockCtx, `SELECT pg_advisory_lock($1)`, harnessLockKey)
	if err != nil {
		_ = conn.Close(context.Background())
		return nil, fmt.Errorf("testdb: take harness lock (%s): %w", wait, err)
	}

	store, err := pgstore.Open(ctx, dsn)
	if err != nil {
		_ = conn.Close(context.Background())
		return nil, fmt.Errorf("testdb: open: %w", err)
	}
	migrateCtx, cancelMigrate := context.WithTimeout(ctx, 30*time.Second)
	defer cancelMigrate()
	if err := store.Migrate(migrateCtx); err != nil {
		store.Close()
		_ = conn.Close(context.Background())
		return nil, fmt.Errorf("testdb: migrate: %w", err)
	}
	return &Harness{store: store, lock: conn}, nil
}

// Store 返回已建好表的 Store。
func (h *Harness) Store() *pgstore.Store { return h.store }

// Reset 清空 run schema 的全部五张表。
//
// 一次列全五张表：run_artifacts 对 backtest_jobs 有 FK，只清父表会被约束挡住。
// 只能在持锁期间调用——否则会把别的测试进程的行一起清掉（本包存在的理由）。
func (h *Harness) Reset(ctx context.Context) error {
	_, err := h.store.Pool().Exec(ctx, `TRUNCATE
		run.backtest_jobs, run.run_artifacts, run.batch_runs,
		run.walk_forward_reports, run.worker_registry`)
	if err != nil {
		return fmt.Errorf("testdb: truncate run schema: %w", err)
	}
	return nil
}

// Close 释放锁并关池。
func (h *Harness) Close() {
	if h == nil {
		return
	}
	if h.lock != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// 显式 unlock 再关连接：只靠断连释放会让锁在连接池回收前一直挂着。
		_, _ = h.lock.Exec(ctx, `SELECT pg_advisory_unlock($1)`, harnessLockKey)
		_ = h.lock.Close(ctx)
	}
	h.store.Close()
}
