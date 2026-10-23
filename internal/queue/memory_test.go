package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

type testClock struct{ at time.Time }

func (c *testClock) Now() time.Time          { return c.at }
func (c *testClock) Advance(d time.Duration) { c.at = c.at.Add(d) }
func newTestClock() *testClock {
	return &testClock{at: time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)}
}

func seqIDs() func() string {
	var n int
	return func() string {
		n++
		return fmt.Sprintf("run-%03d", n)
	}
}

func newTestMemory(c *testClock) *Memory {
	return NewMemory(MemoryOptions{Now: c.Now, NewID: seqIDs()})
}

const sampleSpec = `{"strategy":"ma_cross","initial_capital":1000000}`

func mustEnqueue(t *testing.T, m *Memory, key string, priority int) *RunRecord {
	t.Helper()
	job, created, err := m.Enqueue(context.Background(), &RunRecord{
		IdempotencyKey: key,
		RequestJSON:    json.RawMessage(sampleSpec),
		Priority:       priority,
	})
	if err != nil {
		t.Fatalf("Enqueue(%q): %v", key, err)
	}
	if !created {
		t.Fatalf("Enqueue(%q): expected a new row", key)
	}
	return job
}

func TestEnqueueIsIdempotentOnCanonicalJSON(t *testing.T) {
	c := newTestClock()
	m := newTestMemory(c)

	first := mustEnqueue(t, m, "k1", 0)
	if first.Status != RunPending || first.Attempts != 0 || first.MaxAttempts != DefaultMaxAttempts {
		t.Fatalf("unexpected fresh row: %+v", first)
	}
	if first.RequestHash == "" || len(first.RequestHash) != 64 {
		t.Fatalf("request_hash must be 64 hex chars, got %q", first.RequestHash)
	}

	// 同一个请求、键序不同 → 命中幂等，返回原行且 created=false。
	replay, created, err := m.Enqueue(context.Background(), &RunRecord{
		IdempotencyKey: "k1",
		RequestJSON:    json.RawMessage(`{ "initial_capital": 1000000, "strategy": "ma_cross" }`),
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if created {
		t.Fatal("replay must not create a row")
	}
	if replay.ID != first.ID {
		t.Fatalf("replay id = %q, want %q", replay.ID, first.ID)
	}
	if string(replay.RequestJSON) != `{"initial_capital":1000000,"strategy":"ma_cross"}` {
		t.Fatalf("request_json not canonicalized: %s", replay.RequestJSON)
	}
}

func TestEnqueueRejectsConflictingPayload(t *testing.T) {
	m := newTestMemory(newTestClock())
	mustEnqueue(t, m, "k1", 0)

	_, _, err := m.Enqueue(context.Background(), &RunRecord{
		IdempotencyKey: "k1",
		RequestJSON:    json.RawMessage(`{"strategy":"rsi"}`),
	})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}
}

func TestEnqueueValidation(t *testing.T) {
	m := newTestMemory(newTestClock())
	long := make([]byte, MaxIdempotencyKeyLen+1)
	for i := range long {
		long[i] = 'k'
	}

	cases := []struct {
		name string
		rec  *RunRecord
	}{
		{"nil", nil},
		{"empty key", &RunRecord{RequestJSON: json.RawMessage(sampleSpec)}},
		{"long key", &RunRecord{IdempotencyKey: string(long), RequestJSON: json.RawMessage(sampleSpec)}},
		{"empty json", &RunRecord{IdempotencyKey: "k", RequestJSON: json.RawMessage("  ")}},
		{"broken json", &RunRecord{IdempotencyKey: "k", RequestJSON: json.RawMessage(`{"a":`)}},
		{"stale hash", &RunRecord{IdempotencyKey: "k", RequestJSON: json.RawMessage(sampleSpec), RequestHash: "deadbeef"}},
		{"non-pending status", &RunRecord{IdempotencyKey: "k", RequestJSON: json.RawMessage(sampleSpec), Status: RunRunning}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := m.Enqueue(context.Background(), tc.rec); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("err = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestEnqueueQueueFull(t *testing.T) {
	c := newTestClock()
	m := NewMemory(MemoryOptions{Now: c.Now, NewID: seqIDs(), MaxQueueDepth: 1})

	mustEnqueue(t, m, "k1", 0)
	if _, _, err := m.Enqueue(context.Background(), &RunRecord{
		IdempotencyKey: "k2",
		RequestJSON:    json.RawMessage(sampleSpec),
	}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err = %v, want ErrQueueFull", err)
	}

	// 领走一条后，深度归零，队列重新可入。
	if _, err := m.Claim(context.Background(), "w1", time.Minute); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	mustEnqueue(t, m, "k2", 0)
}

func TestClaimOrderAndDefaults(t *testing.T) {
	c := newTestClock()
	m := newTestMemory(c)

	low := mustEnqueue(t, m, "low", 0)
	c.Advance(time.Second)
	high := mustEnqueue(t, m, "high", 9)
	c.Advance(time.Second)
	same := mustEnqueue(t, m, "same", 0)

	got, err := m.Claim(context.Background(), "w1", time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got.ID != high.ID {
		t.Fatalf("claimed %q, want highest priority %q", got.ID, high.ID)
	}
	if got.Status != RunRunning || got.WorkerID != "w1" || got.LeaseUntil.IsZero() {
		t.Fatalf("claim must write running+owner+lease: %+v", got)
	}
	if !got.StartedAt.Equal(c.Now()) {
		t.Fatalf("started_at = %v, want %v", got.StartedAt, c.Now())
	}

	// 同优先级按 created_at 升序 → low 先于 same。
	got, err = m.Claim(context.Background(), "w1", time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got.ID != low.ID {
		t.Fatalf("claimed %q, want older %q", got.ID, low.ID)
	}

	got, err = m.Claim(context.Background(), "w1", time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got.ID != same.ID {
		t.Fatalf("claimed %q, want last one %q", got.ID, same.ID)
	}

	// 无活 → (nil, nil)。
	got, err = m.Claim(context.Background(), "w1", time.Minute)
	if err != nil {
		t.Fatalf("Claim on empty pending: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil job, got %+v", got)
	}
}

func TestClaimRequiresValidWorkerAndLease(t *testing.T) {
	m := newTestMemory(newTestClock())
	mustEnqueue(t, m, "k1", 0)

	if _, err := m.Claim(context.Background(), "", time.Minute); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty worker: err = %v", err)
	}
	if _, err := m.Claim(context.Background(), "w1", 0); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("zero lease: err = %v", err)
	}
}

func TestClaimRejectsDrainingWorker(t *testing.T) {
	c := newTestClock()
	reg := NewMemoryRegistry(RegistryOptions{Now: c.Now})
	m := NewMemory(MemoryOptions{Now: c.Now, NewID: seqIDs(), Workers: reg})

	if err := reg.Register(context.Background(), WorkerRecord{WorkerID: "w1", Kernels: []string{"go"}}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := reg.Drain(context.Background(), "w1"); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	mustEnqueue(t, m, "k1", 0)

	if _, err := m.Claim(context.Background(), "w1", time.Minute); !errors.Is(err, ErrWorkerDraining) {
		t.Fatalf("err = %v, want ErrWorkerDraining", err)
	}
	// 未注册的 worker 不拒（单机装配允许不建注册表）。
	if _, err := m.Claim(context.Background(), "w2", time.Minute); err != nil {
		t.Fatalf("unregistered worker must be allowed: %v", err)
	}
}

func TestRenewLease(t *testing.T) {
	c := newTestClock()
	m := newTestMemory(c)
	mustEnqueue(t, m, "k1", 0)
	job, err := m.Claim(context.Background(), "w1", time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	c.Advance(30 * time.Second)
	if err := m.RenewLease(context.Background(), job.ID, "w1", time.Minute); err != nil {
		t.Fatalf("RenewLease: %v", err)
	}
	stored, _ := m.Get(context.Background(), job.ID)
	if !stored.LeaseUntil.Equal(c.Now().Add(time.Minute)) {
		t.Fatalf("lease_until = %v, want %v", stored.LeaseUntil, c.Now().Add(time.Minute))
	}

	if err := m.RenewLease(context.Background(), job.ID, "w2", time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("wrong owner: err = %v", err)
	}
	c.Advance(2 * time.Minute)
	if err := m.RenewLease(context.Background(), job.ID, "w1", time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired lease: err = %v", err)
	}
	if err := m.RenewLease(context.Background(), job.ID, "w1", 0); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("zero lease: err = %v", err)
	}
}

func TestCompleteTransitions(t *testing.T) {
	c := newTestClock()
	m := newTestMemory(c)
	mustEnqueue(t, m, "k1", 0)
	job, err := m.Claim(context.Background(), "w1", time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	if err := m.Complete(context.Background(), job.ID, "w1", "invalid", ""); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty result_uri: err = %v", err)
	}
	if err := m.Complete(context.Background(), job.ID, "w1", "failed", "s3://r/1"); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("failed must go through Fail: err = %v", err)
	}
	if err := m.Complete(context.Background(), job.ID, "w2", "completed", "s3://r/1"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("wrong owner: err = %v", err)
	}
	if err := m.Complete(context.Background(), job.ID, "w1", "invalid", "s3://r/1"); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	stored, _ := m.Get(context.Background(), job.ID)
	if stored.Status != RunCompleted || stored.ResultStatus != ResultInvalid || stored.ResultURI != "s3://r/1" {
		t.Fatalf("unexpected completed row: %+v", stored)
	}
	if stored.CompletedAt.IsZero() || !stored.LeaseUntil.IsZero() {
		t.Fatalf("terminal row must stamp completed_at and release the lease: %+v", stored)
	}

	// 同终态同载荷 → nil（幂等）。
	if err := m.Complete(context.Background(), job.ID, "w1", "invalid", "s3://r/1"); err != nil {
		t.Fatalf("idempotent Complete: %v", err)
	}
	// 同终态不同载荷 → 拒绝。
	if err := m.Complete(context.Background(), job.ID, "w1", "completed", "s3://r/2"); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("different payload: err = %v", err)
	}
}

func TestCompleteRejectsCancelling(t *testing.T) {
	c := newTestClock()
	m := newTestMemory(c)
	job := mustEnqueue(t, m, "k1", 0)
	if _, err := m.Claim(context.Background(), "w1", time.Minute); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := m.RequestCancel(context.Background(), job.ID); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}
	if err := m.Complete(context.Background(), job.ID, "w1", "completed", "s3://r/1"); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("err = %v, want ErrInvalidTransition (cancelling must ack, not complete)", err)
	}
	if err := m.AckCancel(context.Background(), job.ID, "w1"); err != nil {
		t.Fatalf("AckCancel: %v", err)
	}
	stored, _ := m.Get(context.Background(), job.ID)
	if stored.Status != RunCancelled || stored.ResultStatus != "" {
		t.Fatalf("unexpected cancelled row: %+v", stored)
	}
	if err := m.AckCancel(context.Background(), job.ID, "w1"); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("terminal ack: err = %v", err)
	}
}

func TestFailTransitions(t *testing.T) {
	c := newTestClock()
	m := newTestMemory(c)
	mustEnqueue(t, m, "k1", 0)
	job, err := m.Claim(context.Background(), "w1", time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	if err := m.Fail(context.Background(), job.ID, "w1", "", ""); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty error_message: err = %v", err)
	}
	if err := m.Fail(context.Background(), job.ID, "w1", "boom", "completed"); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("bad result_status: err = %v", err)
	}
	if err := m.Fail(context.Background(), job.ID, "w1", "boom", ""); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if err := m.Fail(context.Background(), job.ID, "w1", "boom", ""); err != nil {
		t.Fatalf("idempotent Fail: %v", err)
	}
	if err := m.Fail(context.Background(), job.ID, "w1", "other", ""); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("different payload: err = %v", err)
	}

	stored, _ := m.Get(context.Background(), job.ID)
	if stored.Status != RunFailed || stored.ResultStatus != "" || stored.ErrorMessage != "boom" {
		t.Fatalf("unexpected failed row: %+v", stored)
	}
}

func TestRequestCancel(t *testing.T) {
	c := newTestClock()
	m := newTestMemory(c)

	pending := mustEnqueue(t, m, "p", 0)
	if err := m.RequestCancel(context.Background(), pending.ID); err != nil {
		t.Fatalf("RequestCancel(pending): %v", err)
	}
	stored, _ := m.Get(context.Background(), pending.ID)
	if stored.Status != RunCancelled || !stored.CancelRequested || stored.CompletedAt.IsZero() {
		t.Fatalf("pending must go straight to cancelled and keep the row: %+v", stored)
	}

	running := mustEnqueue(t, m, "r", 0)
	if _, err := m.Claim(context.Background(), "w1", time.Minute); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := m.RequestCancel(context.Background(), running.ID); err != nil {
		t.Fatalf("RequestCancel(running): %v", err)
	}
	stored, _ = m.Get(context.Background(), running.ID)
	if stored.Status != RunCancelling || !stored.CancelRequested || stored.ResultURI != "" {
		t.Fatalf("running must become cancelling: %+v", stored)
	}
	// 重复取消是幂等空操作；终态也是。
	if err := m.RequestCancel(context.Background(), running.ID); err != nil {
		t.Fatalf("repeat RequestCancel: %v", err)
	}
	if err := m.RequestCancel(context.Background(), pending.ID); err != nil {
		t.Fatalf("terminal RequestCancel: %v", err)
	}
	if err := m.RequestCancel(context.Background(), "nope"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("unknown id: err = %v", err)
	}
}

func TestAckCancelRequiresOwner(t *testing.T) {
	c := newTestClock()
	m := newTestMemory(c)
	mustEnqueue(t, m, "k1", 0)
	job, err := m.Claim(context.Background(), "w1", time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := m.AckCancel(context.Background(), job.ID, "w2"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("wrong owner: err = %v", err)
	}
	c.Advance(2 * time.Minute)
	if err := m.AckCancel(context.Background(), job.ID, "w1"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired lease: err = %v", err)
	}
}

func TestGetUnknown(t *testing.T) {
	m := newTestMemory(newTestClock())
	if _, err := m.Get(context.Background(), "nope"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("err = %v, want ErrJobNotFound", err)
	}
}

func TestListFilteringAndClamp(t *testing.T) {
	c := newTestClock()
	m := newTestMemory(c)

	var ids []string
	for i := 0; i < 5; i++ {
		ids = append(ids, mustEnqueue(t, m, fmt.Sprintf("k%d", i), i).ID)
		c.Advance(time.Second)
	}
	job, err := m.Claim(context.Background(), "w1", time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := m.Complete(context.Background(), job.ID, "w1", "completed", "s3://r/1"); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	// 再领一条，好让 status 过滤有 running 行可比。
	if _, err := m.Claim(context.Background(), "w1", time.Minute); err != nil {
		t.Fatalf("Claim: %v", err)
	}

	all, err := m.List(context.Background(), ListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 5 {
		t.Fatalf("len = %d, want 5", len(all))
	}
	// created_at 降序。
	if all[0].ID != ids[4] {
		t.Fatalf("first = %q, want newest %q", all[0].ID, ids[4])
	}

	running, err := m.List(context.Background(), ListFilter{Status: RunRunning})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(running) != 1 || running[0].ResultStatus != "" {
		t.Fatalf("unexpected running rows: %+v", running)
	}

	done, err := m.List(context.Background(), ListFilter{ResultStatus: ResultCompleted})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(done) != 1 || done[0].Status != RunCompleted {
		t.Fatalf("unexpected completed rows: %+v", done)
	}

	// limit 钳制：请求 1000 → 上限 100；0 → 默认 50。
	// （真正的钳制断言在 TestListClampsLimit 里——这里只有 5 行，区分不出 100 与 50。）
	limited, err := m.List(context.Background(), ListFilter{Limit: 1000})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(limited) != 5 {
		t.Fatalf("len = %d, want 5", len(limited))
	}
	if _, err := m.List(context.Background(), ListFilter{Limit: 0}); err != nil {
		t.Fatalf("List default limit: %v", err)
	}

	// Before 严格小于：以第二条的 created_at 为界 → 只剩第一条。
	cutoff := all[len(all)-2].CreatedAt
	before, err := m.List(context.Background(), ListFilter{Before: cutoff})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(before) != 1 || before[0].ID != ids[0] {
		t.Fatalf("unexpected before rows: %+v", before)
	}

	if _, err := m.List(context.Background(), ListFilter{Status: "bogus"}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("unknown status: err = %v", err)
	}
	if _, err := m.List(context.Background(), ListFilter{ResultStatus: "degraded"}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("fourth result_status: err = %v", err)
	}

	empty, err := m.List(context.Background(), ListFilter{Status: RunCancelled})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("empty list must be [], got %#v", empty)
	}
}

func TestListClampsLimit(t *testing.T) {
	c := newTestClock()
	m := newTestMemory(c)

	// 150 行：够区分「上限 100」与「默认 50」两种钳制。
	const total = 150
	for i := 0; i < total; i++ {
		mustEnqueue(t, m, fmt.Sprintf("k%d", i), 0)
		c.Advance(time.Millisecond)
	}

	over, err := m.List(context.Background(), ListFilter{Limit: 1000})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(over) != MaxListLimit {
		t.Fatalf("limit 1000 → %d rows, want %d (clamped)", len(over), MaxListLimit)
	}

	def, err := m.List(context.Background(), ListFilter{Limit: 0})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(def) != DefaultListLimit {
		t.Fatalf("limit 0 → %d rows, want %d (default)", len(def), DefaultListLimit)
	}

	exact, err := m.List(context.Background(), ListFilter{Limit: 7})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(exact) != 7 {
		t.Fatalf("limit 7 → %d rows, want 7", len(exact))
	}
}

func TestReapExpiredRequeuesThenFails(t *testing.T) {
	c := newTestClock()
	m := newTestMemory(c)
	mustEnqueue(t, m, "k1", 0)
	job, err := m.Claim(context.Background(), "w1", time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// 租约未到期 → 不动。
	c.Advance(time.Second)
	if n, err := m.ReapExpired(context.Background(), c.Now()); err != nil || n != 0 {
		t.Fatalf("reap before expiry: n=%d err=%v", n, err)
	}

	c.Advance(2 * time.Minute)
	n, err := m.ReapExpired(context.Background(), c.Now())
	if err != nil || n != 1 {
		t.Fatalf("reap: n=%d err=%v", n, err)
	}
	stored, _ := m.Get(context.Background(), job.ID)
	if stored.Status != RunPending || stored.Attempts != 1 || !stored.LeaseUntil.IsZero() {
		t.Fatalf("expired row must go back to pending with attempts+1: %+v", stored)
	}

	// 再次过期 → attempts=2 → pending；第三次 → attempts=3 → failed。
	for i := 0; i < 2; i++ {
		if _, err := m.Claim(context.Background(), "w1", time.Minute); err != nil {
			t.Fatalf("Claim: %v", err)
		}
		c.Advance(2 * time.Minute)
		if _, err := m.ReapExpired(context.Background(), c.Now()); err != nil {
			t.Fatalf("ReapExpired: %v", err)
		}
	}
	stored, _ = m.Get(context.Background(), job.ID)
	if stored.Status != RunFailed || stored.ErrorMessage != attemptsExhaustedMessage || stored.Attempts != 3 {
		t.Fatalf("exhausted row must be failed/attempts_exhausted: %+v", stored)
	}
	if stored.ResultStatus != "" {
		t.Fatalf("exhausted row must not carry a K9 result_status: %+v", stored)
	}
}

func TestRowsAreCopiedOnTheWayOut(t *testing.T) {
	m := newTestMemory(newTestClock())
	job := mustEnqueue(t, m, "k1", 0)
	job.Priority = 99
	job.RequestJSON[0] = 'X'

	stored, err := m.Get(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Priority != 0 {
		t.Fatalf("stored row mutated through the returned copy: %+v", stored)
	}
	if string(stored.RequestJSON) != `{"initial_capital":1000000,"strategy":"ma_cross"}` {
		t.Fatalf("stored request_json mutated: %s", stored.RequestJSON)
	}
}
