package pgstore_test

// PG 专属的腿：只测「内存实现做不到、因而共享套件测不到」的那一层——
// 数据库约束本身、多进程并发领取、以及 SQL 谓词的形状。
// 语义本身（幂等 / 两轴 / 租约）已由 qtest 套件在真库上跑过，这里不重复。

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"tellurion-orchestration/internal/pgstore"
	"tellurion-orchestration/internal/queue"
)

// TestOpenFailsClosedOnEmptyDSN 不需要库，CI 也跑：
// 「DSN 空 → 退回内存实现」是这套装配最危险的假绿——进程以为自己 durable，其实重启即丢。
func TestOpenFailsClosedOnEmptyDSN(t *testing.T) {
	ctx := context.Background()
	for _, dsn := range []string{"", "   "} {
		if _, err := pgstore.Open(ctx, dsn); err == nil {
			t.Fatalf("Open(%q) must refuse, not fall back to an in-memory store", dsn)
		}
	}
	if _, err := pgstore.Open(ctx, "not a dsn at all"); err == nil {
		t.Fatal("Open must reject an unparseable dsn")
	}
}

func TestMigrateIsIdempotentAndCreatesTheWholeSchema(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	// 幂等：取向 ①（无版本表）要求同一份 DDL 可以反复执行（每次进程启动都跑一遍）。
	for i := 0; i < 2; i++ {
		if err := store.Migrate(ctx); err != nil {
			t.Fatalf("Migrate pass %d: %v", i+1, err)
		}
	}

	// 表数与清单对表 docs/db-schema.md §1：run schema 六张（裁-13 起含 parent_jobs；
	// strategy_state 不在此，H5 裁决）。
	var tables []string
	rows, err := store.Pool().Query(ctx, `
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = 'run' ORDER BY table_name`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, n)
	}
	rows.Close()
	want := []string{"backtest_jobs", "batch_runs", "parent_jobs", "run_artifacts", "walk_forward_reports", "worker_registry"}
	if strings.Join(tables, ",") != strings.Join(want, ",") {
		t.Fatalf("run schema tables = %v, want %v", tables, want)
	}

	// 索引：领取 / 幂等 / owner / 列表 / 结论 五类各至少一条（§9.5）。
	for _, idx := range []string{
		"backtest_jobs_idempotency_key_uidx", "backtest_jobs_claim_idx",
		"backtest_jobs_worker_idx", "backtest_jobs_created_idx", "backtest_jobs_result_status_idx",
		"backtest_jobs_parent_idx",
		"batch_runs_idempotency_key_uidx", "walk_forward_reports_idempotency_key_uidx",
		"parent_jobs_idempotency_key_uidx", "parent_jobs_plan_hash_uidx", "parent_jobs_kind_status_idx",
	} {
		var n int
		if err := store.Pool().QueryRow(ctx,
			`SELECT count(*) FROM pg_indexes WHERE schemaname='run' AND indexname=$1`, idx).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("index %s present = %d, want 1", idx, n)
		}
	}

	// 领取索引：键序 priority DESC → created_at → id，且**不能**把 lease_until
	// 写进 partial 谓词。注意 indexdef 是 PG 规范化后的文本（ASC 是默认方向会被省掉、
	// 'pending' 会带上 ::text 类型标注），所以这里比的是**包含关系与相对次序**，
	// 不是逐字节等值——逐字节等值会随 PG 版本红。
	var def string
	if err := store.Pool().QueryRow(ctx,
		`SELECT indexdef FROM pg_indexes WHERE schemaname='run' AND indexname='backtest_jobs_claim_idx'`).Scan(&def); err != nil {
		t.Fatal(err)
	}
	iPriority := strings.Index(def, "priority DESC")
	iCreated := strings.Index(def, "created_at")
	iID := strings.Index(def, "id)")
	if !(iPriority >= 0 && iPriority < iCreated && iCreated < iID) {
		t.Fatalf("claim index key order must be priority DESC -> created_at -> id, got: %s", def)
	}
	if !strings.Contains(def, "status = 'pending'") {
		t.Fatalf("claim index must be partial on pending, got: %s", def)
	}
	if strings.Contains(def, "lease_until") {
		t.Fatalf("claim index must not filter on lease_until（冻结判据是「已释放**或**已过期」，把 IS NULL 写进 partial 会让过期那一支走不到索引）: %s", def)
	}
	if strings.Contains(def, "(status,") || strings.Contains(def, "status, priority") {
		t.Fatalf("status 作为键列是冗余的（partial 谓词已经限定它），原 DDL 就是这么写的: %s", def)
	}
}

func TestClaimPredicateSkipsARowWithAnUnreleasedLease(t *testing.T) {
	// 「pending + 未来租约」这个形状**只能由直接改库产生**（内存实现的回收路径
	// 会把租约清成 NULL），所以共享套件测不到它。这里用真 SQL 钉住 PG 侧的谓词：
	// 它必须不派这一行——否则两个 worker 会同时跑同一个 run_id。
	store := newStore(t)
	ctx := context.Background()
	q := store.Queue(nil, pgstore.Options{})

	seed := func(id string, lease time.Time) {
		if _, err := store.Pool().Exec(ctx, `
			INSERT INTO run.backtest_jobs
				(id, idempotency_key, request_hash, request_json, request_canonical,
				 status, created_at, lease_until)
			VALUES ($1, $1, 'h', CAST('{}' AS jsonb), '{}', 'pending', $2, $3)`,
			id, time.Now().UTC().Add(-time.Hour), nullableLease(lease)); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
	future := time.Now().UTC().Add(time.Hour)
	expired := time.Now().UTC().Add(-time.Minute)
	released := time.Time{}

	seed("row-live", future)
	seed("row-expired", expired)
	seed("row-released", released)

	got, err := q.Claim(ctx, "w1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.ID != "row-expired" {
		t.Fatalf("first claim = %+v, want row-expired（过期租约可再领；live 租约必须跳过）", got)
	}
	got, err = q.Claim(ctx, "w1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.ID != "row-released" {
		t.Fatalf("second claim = %+v, want row-released", got)
	}
	// 带未来租约的那一行仍然 pending，没有被派出去。
	var status string
	if err := store.Pool().QueryRow(ctx, `SELECT status FROM run.backtest_jobs WHERE id='row-live'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Fatalf("row with an unexpired lease was dispatched (status=%q)", status)
	}
}

func nullableLease(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func TestDatabaseConstraintsAreRealNotJustGoSideChecks(t *testing.T) {
	// 三条约束必须由**库**执行，而不是只在 Go 里判：绕过 port 直接写库的
	// 运维/迁移脚本同样受这些约束保护。每条都用真 SQL 撞一次。
	store := newStore(t)
	ctx := context.Background()

	const base = `INSERT INTO run.backtest_jobs
		(id, idempotency_key, request_hash, request_json, request_canonical, status)
		VALUES ($1, $2, 'h', CAST('{}' AS jsonb), '{}', $3)`

	cases := []struct {
		name        string
		sql         string
		args        []any
		wantSQLCode string
	}{
		{"幂等键唯一", `INSERT INTO run.backtest_jobs
			(id, idempotency_key, request_hash, request_json, request_canonical)
			VALUES ($1, $2, 'h', CAST('{}' AS jsonb), '{}')`, []any{"x1", "dup-key"}, "23505"},
		{"status CHECK 拒绝未知生命周期", base, []any{"x2", "k2", "queued"}, "23514"},
		{"result_status CHECK 拒绝第四值", `INSERT INTO run.backtest_jobs
			(id, idempotency_key, request_hash, request_json, request_canonical, result_status)
			VALUES ($1, $2, 'h', CAST('{}' AS jsonb), '{}', $3)`, []any{"x3", "k3", "partial"}, "23514"},
		{"run_artifacts 的 FK 指向 run.backtest_jobs", `INSERT INTO run.run_artifacts
			(run_id, strategy_ref, dataset_snapshot_hash, risk_rules_hash,
			 run_spec_hash, params_hash, kernel, engine_version, fill_timing)
			VALUES ($1, 's', 'd', '', 'r', 'p', 'go', 'v', 'next_open')`, []any{"no-such-run"}, "23503"},
	}

	// 先占掉幂等键，再撞唯一约束。
	if _, err := store.Pool().Exec(ctx, base, "seed", "dup-key", "pending"); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	for _, c := range cases {
		_, err := store.Pool().Exec(ctx, c.sql, c.args...)
		var code string
		switch e := err.(type) {
		case interface{ SQLState() string }:
			code = e.SQLState()
		default:
			t.Fatalf("%s: expected a constraint error, got %v", c.name, err)
		}
		if code != c.wantSQLCode {
			t.Fatalf("%s: SQLSTATE = %s, want %s (err %v)", c.name, code, c.wantSQLCode, err)
		}
	}
}

func TestChar64HashIsPaddedAndReadBackTrimmed(t *testing.T) {
	// request_hash 是 CHAR(64)：短值在物理上被补齐到 64 字节（SQL 的 length() 按
	// 语义不计尾部空格，octet_length 才看得见补齐——实测 len=3 / olen=64）。
	// port 读回来必须是不带尾空格的原值，否则幂等比较（字符串相等）会把同一请求
	// 判成两次不同请求。rowToJob 的 TrimRight 就是这条保证，用例把它钉住。
	store := newStore(t)
	ctx := context.Background()
	if _, err := store.Pool().Exec(ctx, `
		INSERT INTO run.backtest_jobs
			(id, idempotency_key, request_hash, request_json, request_canonical)
			VALUES ('legacy-1', 'legacy-key', 'abc', CAST('{}' AS jsonb), '{}')`); err != nil {
		t.Fatal(err)
	}
	var sqlLen, octets int
	if err := store.Pool().QueryRow(ctx,
		`SELECT length(request_hash), octet_length(request_hash) FROM run.backtest_jobs WHERE id='legacy-1'`).
		Scan(&sqlLen, &octets); err != nil {
		t.Fatal(err)
	}
	if octets != 64 {
		t.Fatalf("octet_length = %d, want 64（CHAR(64) 物理补齐；列型被改过就说明 DDL 与文档漂移了）", octets)
	}
	if sqlLen != 3 {
		t.Fatalf("length = %d, want 3（SQL 语义不计 bpchar 的尾部填充）", sqlLen)
	}
	q := store.Queue(nil, pgstore.Options{})
	row, err := q.Get(ctx, "legacy-1")
	if err != nil {
		t.Fatal(err)
	}
	if row.RequestHash != "abc" {
		t.Fatalf("RequestHash = %q, want %q（读回必须裁掉补齐的空格）", row.RequestHash, "abc")
	}
	// 存量脏行也参与领取：没有 result_status、status 由默认值给 pending。
	if row.Status != queue.RunPending {
		t.Fatalf("status = %q, want pending", row.Status)
	}
}

func TestConcurrentClaimsNeverHandOutTheSameRowTwice(t *testing.T) {
	// 共享套件里已有同名用例，但那条跑在**默认装配**上；这里刻意把连接池压到
	// 大于领取并发数，确认真的并发在跑（而不是被池子排队排成了串行、
	// 让 SKIP LOCKED 看起来绿）。判据：领取次数 > 行数 且去重后行数不变。
	store := newStore(t)
	ctx := context.Background()
	q := store.Queue(nil, pgstore.Options{})

	const rows, goroutines = 4, 11
	for i := 0; i < rows; i++ {
		rec := &queue.RunRecord{IdempotencyKey: fmt.Sprintf("cc-%d", i), RequestJSON: []byte(fmt.Sprintf(`{"i":%d}`, i))}
		if _, _, err := q.Enqueue(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}

	seen := make(chan string, goroutines)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < goroutines; i++ {
			go func(i int) {
				row, err := q.Claim(ctx, fmt.Sprintf("w%d", i), time.Hour)
				if err != nil {
					t.Errorf("claim: %v", err)
					seen <- ""
					return
				}
				if row == nil {
					seen <- ""
					return
				}
				seen <- row.ID
			}(i)
			time.Sleep(2 * time.Millisecond) // 让 goroutine 真正重叠，而不是依次拿锁
		}
	}()
	<-done
	close(seen)

	distinct := map[string]int{}
	for id := range seen {
		if id == "" {
			continue
		}
		distinct[id]++
	}
	if len(distinct) != rows {
		t.Fatalf("distinct claimed = %d, want %d", len(distinct), rows)
	}
	for id, n := range distinct {
		if n != 1 {
			t.Fatalf("row %s handed to %d workers", id, n)
		}
	}
}

func TestRegistryQueriesUseTheSameConnectionPoolAcrossPorts(t *testing.T) {
	// Queue 的 drain 校验走 worker_registry：注册 → drain → 领取必须当场生效，
	// 不能依赖缓存或另一个实例。这条是 port 之间**共享同一个库**的接缝测试。
	store := newStore(t)
	ctx := context.Background()
	reg := store.Registry()
	q := store.Queue(reg, pgstore.Options{})

	if err := reg.Register(ctx, queue.WorkerRecord{WorkerID: "w1", Kernels: []string{"go"}}); err != nil {
		t.Fatal(err)
	}
	rec := &queue.RunRecord{IdempotencyKey: "pair-1", RequestJSON: []byte(`{"a":1}`)}
	if _, _, err := q.Enqueue(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Claim(ctx, "w1", time.Minute); err != nil {
		t.Fatalf("active worker: %v", err)
	}

	// 换成 drain 之后必须立刻拒领（同一份库，另一张表）。
	if err := reg.Register(ctx, queue.WorkerRecord{WorkerID: "w2"}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Drain(ctx, "w2"); err != nil {
		t.Fatal(err)
	}
	_, err := q.Claim(ctx, "w2", time.Minute)
	if !errors.Is(err, queue.ErrWorkerDraining) {
		t.Fatalf("draining worker = %v, want ErrWorkerDraining", err)
	}
}

// TestPGParentStoreRoundTrip 是裁-13 的 PG 腿：父表写入 / 幂等重放 / 冲突 /
// 汇总写回 / 子 run 的 parent_job_id 外键，全部在真库上跑一遍。
func TestPGParentStoreRoundTrip(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	parents := store.Parents()

	canonical := `{"kind":"batch","units":2,"children":[]}`
	hash, err := queue.RequestHash([]byte(canonical))
	if err != nil {
		t.Fatal(err)
	}
	rec := &queue.ParentRecord{
		IdempotencyKey: "parent-key-1",
		Kind:           queue.ParentBatch,
		PlanCanonical:  canonical,
		PlanHash:       hash,
		Status:         queue.RunRunning,
		Total:          2,
	}

	created, ok, err := parents.EnqueueParent(ctx, rec)
	if err != nil || !ok {
		t.Fatalf("EnqueueParent: created=%v err=%v", ok, err)
	}
	if created.ID == "" || created.PlanHash != hash || created.Total != 2 {
		t.Fatalf("row = %+v", created)
	}

	replayed, ok, err := parents.EnqueueParent(ctx, rec)
	if err != nil || ok {
		t.Fatalf("replay: created=%v err=%v", ok, err)
	}
	if replayed.ID != created.ID {
		t.Fatalf("replay id = %s, want %s", replayed.ID, created.ID)
	}

	// 换幂等键但同一份计划 → 仍是原父记录（身份是 plan_hash）。
	replayedByPlan, ok, err := parents.EnqueueParent(ctx, &queue.ParentRecord{
		IdempotencyKey: "parent-key-2",
		Kind:           queue.ParentBatch,
		PlanCanonical:  canonical,
		Total:          2,
	})
	if err != nil || ok {
		t.Fatalf("same plan under a new key: created=%v err=%v", ok, err)
	}
	if replayedByPlan.ID != created.ID {
		t.Fatalf("same plan id = %s, want %s", replayedByPlan.ID, created.ID)
	}

	conflict := *rec
	conflict.PlanCanonical = `{"kind":"batch","units":3,"children":[]}`
	conflict.PlanHash = ""
	conflict.Total = 3
	if _, _, err := parents.EnqueueParent(ctx, &conflict); !errors.Is(err, queue.ErrIdempotencyConflict) {
		t.Fatalf("different plan: err = %v, want ErrIdempotencyConflict", err)
	}

	summary, err := parents.SaveParentSummary(ctx, created.ID, queue.ParentSummary{
		Status: queue.RunCompleted, Total: 2, Completed: 1, Invalid: 1,
		SummaryJSON: []byte(`{"tasks":[1,2]}`),
	})
	if err != nil {
		t.Fatalf("SaveParentSummary: %v", err)
	}
	if summary.Status != queue.RunCompleted || summary.Completed != 1 || summary.Invalid != 1 || summary.CompletedAt.IsZero() {
		t.Fatalf("summary = %+v", summary)
	}

	if _, err := parents.GetParent(ctx, "missing-parent"); !errors.Is(err, queue.ErrParentNotFound) {
		t.Fatalf("unknown parent: err = %v, want ErrParentNotFound", err)
	}

	// 子 run 关系：parent_job_id 必须能写能读（FK 由库执行）。
	q := store.Queue(nil, pgstore.Options{})
	child, _, err := q.Enqueue(ctx, &queue.RunRecord{
		IdempotencyKey: "parent-child-1",
		ParentJobID:    created.ID,
		RequestJSON:    []byte(`{"strategy":"momentum"}`),
	})
	if err != nil {
		t.Fatalf("enqueue child: %v", err)
	}
	if child.ParentJobID != created.ID {
		t.Fatalf("child parent_job_id = %q, want %q", child.ParentJobID, created.ID)
	}

	// 逐子状态：ListChildren 只返回指向该父的行，空父返回空切片。
	kids, err := q.ListChildren(ctx, created.ID)
	if err != nil {
		t.Fatalf("ListChildren: %v", err)
	}
	if len(kids) != 1 || kids[0].ID != child.ID {
		t.Fatalf("children = %+v, want [%s]", kids, child.ID)
	}
	if empty, err := q.ListChildren(ctx, "parent-with-no-children"); err != nil || len(empty) != 0 {
		t.Fatalf("unknown parent children = %v / %v", empty, err)
	}
}
