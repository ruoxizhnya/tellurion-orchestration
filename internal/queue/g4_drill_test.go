package queue

// SVC-ORCH-G4 ⑥ 的演练（内存级，确定性时钟）：
// 领了活的 worker 死掉 ⇒ 租约过期后该 run 被回收、仍可被别的 worker 领取，
// 最终能正常写终态——活不会丢。真库 / 真进程腿见台账的「未执行」记录。

import (
	"context"
	"testing"
	"time"
)

func TestDrillDeadWorkerRunIsReclaimedAndCompleted(t *testing.T) {
	ctx := context.Background()
	c := newTestClock()
	m := newTestMemory(c)

	mustEnqueue(t, m, "drill-key", 0)
	claimed, err := m.Claim(ctx, "w-dead", 30*time.Second)
	if err != nil || claimed == nil {
		t.Fatalf("claim by w-dead: %+v / %v", claimed, err)
	}

	// worker 被杀：不再续租，时钟越过 lease。
	c.Advance(31 * time.Second)
	n, err := m.ReapExpired(ctx, c.Now())
	if err != nil || n != 1 {
		t.Fatalf("reap after worker death: n=%d err=%v", n, err)
	}

	stored, err := m.Get(ctx, claimed.ID)
	if err != nil {
		t.Fatalf("get reclaimed row: %v", err)
	}
	if stored.Status != RunPending || !stored.LeaseUntil.IsZero() || stored.Attempts != 1 {
		t.Fatalf("reclaimed row lost or still owned: %+v", stored)
	}

	// 另一个 worker 必须还能领到同一条活（不丢、不重复建行）。
	again, err := m.Claim(ctx, "w-alive", 30*time.Second)
	if err != nil || again == nil || again.ID != claimed.ID {
		t.Fatalf("reclaim must be claimable by another worker: %+v / %v", again, err)
	}
	if err := m.Complete(ctx, claimed.ID, "w-alive", string(ResultCompleted), "mem://results/"+claimed.ID); err != nil {
		t.Fatalf("complete after reclaim: %v", err)
	}
	final, err := m.Get(ctx, claimed.ID)
	if err != nil {
		t.Fatalf("get final: %v", err)
	}
	if final.Status != RunCompleted || final.ResultStatus != ResultCompleted {
		t.Fatalf("final = %+v, want completed/completed", final)
	}
	var rows int
	if list, err := m.List(ctx, ListFilter{}); err == nil {
		rows = len(list)
	}
	if rows != 1 {
		t.Fatalf("reclaim created a second row: %d rows", rows)
	}
}
