package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRegistryRegisterAndHeartbeat(t *testing.T) {
	c := newTestClock()
	r := NewMemoryRegistry(RegistryOptions{Now: c.Now})

	if err := r.Register(context.Background(), WorkerRecord{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty id: err = %v", err)
	}

	if err := r.Register(context.Background(), WorkerRecord{WorkerID: "w1", Kernels: []string{"go"}}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	list, err := r.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("len = %d, want 1", len(list))
	}
	w := list[0]
	if w.Status != WorkerActive || !w.LastHeartbeatAt.Equal(c.Now()) || !w.StartedAt.Equal(c.Now()) {
		t.Fatalf("unexpected fresh worker: %+v", w)
	}

	c.Advance(time.Minute)
	if err := r.Heartbeat(context.Background(), "w1", time.Time{}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	list, _ = r.List(context.Background())
	if !list[0].LastHeartbeatAt.Equal(c.Now()) {
		t.Fatalf("heartbeat not recorded: %+v", list[0])
	}

	if err := r.Heartbeat(context.Background(), "nope", time.Time{}); !errors.Is(err, ErrWorkerNotFound) {
		t.Fatalf("unknown worker: err = %v", err)
	}
}

func TestRegistryReRegisterKeepsDraining(t *testing.T) {
	c := newTestClock()
	r := NewMemoryRegistry(RegistryOptions{Now: c.Now})

	if err := r.Register(context.Background(), WorkerRecord{WorkerID: "w1", Kernels: []string{"go"}}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := r.Drain(context.Background(), "w1"); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	drainedAt := c.Now()

	c.Advance(time.Minute)
	// 重新注册只刷新能力，不能把 draining 洗回 active（否则 drain 可被绕过）。
	if err := r.Register(context.Background(), WorkerRecord{WorkerID: "w1", Kernels: []string{"nt"}}); err != nil {
		t.Fatalf("Re-Register: %v", err)
	}
	list, _ := r.List(context.Background())
	w := list[0]
	if w.Status != WorkerDraining {
		t.Fatalf("status = %q, want draining", w.Status)
	}
	if !w.DrainRequestedAt.Equal(drainedAt) {
		t.Fatalf("drain_requested_at lost: %+v", w)
	}
	if len(w.Kernels) != 1 || w.Kernels[0] != "nt" {
		t.Fatalf("kernels not refreshed: %+v", w)
	}
}

func TestRegistryDrain(t *testing.T) {
	c := newTestClock()
	r := NewMemoryRegistry(RegistryOptions{Now: c.Now})

	if err := r.Drain(context.Background(), "nope"); !errors.Is(err, ErrWorkerNotFound) {
		t.Fatalf("unknown worker: err = %v", err)
	}
	if err := r.Register(context.Background(), WorkerRecord{WorkerID: "w1"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := r.Drain(context.Background(), "w1"); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if err := r.Drain(context.Background(), "w1"); err != nil {
		t.Fatalf("repeat Drain must be idempotent: %v", err)
	}

	list, _ := r.List(context.Background())
	if list[0].Status != WorkerDraining || list[0].DrainRequestedAt.IsZero() {
		t.Fatalf("unexpected drained worker: %+v", list[0])
	}

	// drain 之后心跳仍可用（只是不接新活）。
	if err := r.Heartbeat(context.Background(), "w1", c.Now()); err != nil {
		t.Fatalf("Heartbeat on draining: %v", err)
	}
}

func TestRegistryListIsSortedAndNeverNil(t *testing.T) {
	c := newTestClock()
	r := NewMemoryRegistry(RegistryOptions{Now: c.Now})

	empty, err := r.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("empty list must be [], got %#v", empty)
	}

	for _, id := range []string{"w9", "w1", "w5"} {
		if err := r.Register(context.Background(), WorkerRecord{WorkerID: id}); err != nil {
			t.Fatalf("Register(%s): %v", id, err)
		}
	}
	list, _ := r.List(context.Background())
	if len(list) != 3 || list[0].WorkerID != "w1" || list[2].WorkerID != "w9" {
		t.Fatalf("unexpected order: %+v", list)
	}
}
