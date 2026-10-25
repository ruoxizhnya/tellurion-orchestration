package queue

// 父记录（run.parent_jobs）内存实现的语义腿。PG 实现在 internal/pgstore 跑
// 同一批断言（幂等 / 冲突 / 未知 id / 汇总自洽）。

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func testParent(key, canonical string, total int) *ParentRecord {
	return &ParentRecord{
		IdempotencyKey: key,
		Kind:           ParentBatch,
		PlanCanonical:  canonical,
		Status:         RunRunning,
		Total:          total,
	}
}

func TestMemoryParents_ReplaysSameKeyAndPlan(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryParents(MemoryOptions{
		Now:   func() time.Time { return time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC) },
		NewID: func() string { return "parent-1" },
	})

	first, created, err := m.EnqueueParent(ctx, testParent("key-1", `{"kind":"batch","units":2}`, 2))
	if err != nil || !created {
		t.Fatalf("first enqueue: created=%v err=%v", created, err)
	}
	if first.ID != "parent-1" || first.PlanHash == "" {
		t.Fatalf("record = %+v", first)
	}

	second, created, err := m.EnqueueParent(ctx, testParent("key-1", `{"kind":"batch","units":2}`, 2))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if created {
		t.Fatal("same key + same plan must not create a second row")
	}
	if second.ID != first.ID || second.PlanHash != first.PlanHash {
		t.Fatalf("replay returned a different row: %+v vs %+v", second, first)
	}
}

// TestMemoryParents_PlanHashIsTheIdentityAcrossKeys：X5b 的「同计划重复提交返回
// 同一父 id」——换了幂等键也一样，身份是 plan_hash 而不是调用方起的键。
func TestMemoryParents_PlanHashIsTheIdentityAcrossKeys(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryParents(MemoryOptions{NewID: func() string { return "parent-1" }})

	first, created, err := m.EnqueueParent(ctx, testParent("key-1", `{"kind":"batch","units":2}`, 2))
	if err != nil || !created {
		t.Fatalf("first enqueue: created=%v err=%v", created, err)
	}
	second, created, err := m.EnqueueParent(ctx, testParent("key-2", `{"kind":"batch","units":2}`, 2))
	if err != nil {
		t.Fatalf("same plan under a new key: %v", err)
	}
	if created {
		t.Fatal("same plan must not create a second parent")
	}
	if second.ID != first.ID || second.PlanHash != first.PlanHash {
		t.Fatalf("same plan returned a different row: %+v vs %+v", second, first)
	}
}

func TestMemoryParents_ConflictOnDifferentPlan(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryParents(MemoryOptions{NewID: func() string { return "parent-1" }})

	if _, _, err := m.EnqueueParent(ctx, testParent("key-1", `{"kind":"batch","units":2}`, 2)); err != nil {
		t.Fatal(err)
	}
	_, _, err := m.EnqueueParent(ctx, testParent("key-1", `{"kind":"batch","units":3}`, 3))
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}
}

func TestMemoryParents_RejectsBadInput(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryParents()

	cases := []struct {
		name string
		rec  *ParentRecord
	}{
		{"empty key", testParent("", `{}`, 1)},
		{"unknown kind", func() *ParentRecord {
			r := testParent("k", `{}`, 1)
			r.Kind = "mystery"
			return r
		}()},
		{"empty canonical", testParent("k", "", 1)},
		{"zero total", testParent("k", `{}`, 0)},
		{"hash mismatch", func() *ParentRecord {
			r := testParent("k", `{"a":1}`, 1)
			r.PlanHash = "0000000000000000000000000000000000000000000000000000000000000000"
			return r
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := m.EnqueueParent(ctx, tc.rec); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("err = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestMemoryParents_SummaryRoundTrip(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	m := NewMemoryParents(MemoryOptions{Now: func() time.Time { return now }})

	rec, _, err := m.EnqueueParent(ctx, testParent("key-1", `{"kind":"batch","units":3}`, 3))
	if err != nil {
		t.Fatal(err)
	}

	got, err := m.SaveParentSummary(ctx, rec.ID, ParentSummary{
		Status:      RunCompleted,
		Total:       3,
		Completed:   2,
		Invalid:     1,
		SummaryJSON: []byte(`{"tasks":[1,2,3]}`),
	})
	if err != nil {
		t.Fatalf("SaveParentSummary: %v", err)
	}
	if got.Status != RunCompleted || got.Completed != 2 || got.Invalid != 1 || got.Failed != 0 {
		t.Fatalf("summary not stored: %+v", got)
	}
	if !got.CompletedAt.Equal(now) {
		t.Fatalf("completed_at = %v, want %v", got.CompletedAt, now)
	}

	// total 是提交期冻结的计划规模：汇总阶段改它必须拒绝。
	if _, err := m.SaveParentSummary(ctx, rec.ID, ParentSummary{Status: RunRunning, Total: 4}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("changing total: err = %v, want ErrInvalidInput", err)
	}
	if _, err := m.SaveParentSummary(ctx, "missing", ParentSummary{Status: RunRunning, Total: 3}); !errors.Is(err, ErrParentNotFound) {
		t.Fatalf("unknown parent: err = %v, want ErrParentNotFound", err)
	}
}

// TestMemoryListChildren：逐子状态的来源是 backtest_jobs（不是父表），顺序
// created_at 升序 → id 升序；未知父返回空切片。
func TestMemoryListChildren(t *testing.T) {
	ctx := context.Background()
	next := 0
	q := NewMemory(MemoryOptions{
		Now: func() time.Time { return time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC) },
		NewID: func() string {
			next++
			return fmt.Sprintf("run-%02d", next)
		},
	})

	for _, tc := range []struct {
		key    string
		parent string
	}{
		{"k1", "p-1"},
		{"k2", "p-1"},
		{"k3", "p-2"},
		{"k4", ""},
	} {
		if _, _, err := q.Enqueue(ctx, &RunRecord{
			IdempotencyKey: tc.key,
			ParentJobID:    tc.parent,
			RequestJSON:    []byte(`{"strategy":"momentum"}`),
		}); err != nil {
			t.Fatalf("enqueue %s: %v", tc.key, err)
		}
	}

	kids, err := q.ListChildren(ctx, "p-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(kids) != 2 || kids[0].ID != "run-01" || kids[1].ID != "run-02" {
		t.Fatalf("children of p-1 = %+v", kids)
	}

	empty, err := q.ListChildren(ctx, "p-missing")
	if err != nil {
		t.Fatal(err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("unknown parent must return an empty slice, got %#v", empty)
	}
}

func TestMemoryParents_SummaryValidation(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryParents()
	rec, _, err := m.EnqueueParent(ctx, testParent("key-1", `{"kind":"batch","units":2}`, 2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.SaveParentSummary(ctx, rec.ID, ParentSummary{Status: RunRunning, Total: 2, Completed: 3}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("settled > total: err = %v, want ErrInvalidInput", err)
	}
	if _, err := m.SaveParentSummary(ctx, rec.ID, ParentSummary{Status: "bogus", Total: 2}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("unknown status: err = %v, want ErrInvalidInput", err)
	}
}
