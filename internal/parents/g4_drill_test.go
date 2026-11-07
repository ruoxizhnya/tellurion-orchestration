package parents

// SVC-ORCH-G4 ⑥ 的演练（服务层，确定性时钟）：
// 领了活的 worker 死掉 ⇒ 租约过期被回收、别的 worker 能接着干、重试预算耗尽后
// 落 failed；被取消的子 run 也一并计入父汇总（cancelled 按 failed 计）。
//
// 时钟用「未来时刻」而不是 sleep：ReapExpired 的判据是 `lease_until <= now`，
// 传未来时刻必然过期，演练因此是确定性的。真库 / 真进程腿见
// docs/TASKS.md SVC-ORCH-G4 的「未执行」记录。

import (
	"context"
	"testing"
	"time"

	"tellurion-orchestration/internal/queue"
)

func TestDrillReclaimedAndCancelledChildrenCountAsFailed(t *testing.T) {
	ctx := context.Background()
	svc := New(queue.NewMemoryParents(), queue.NewMemory())

	parent, created, err := svc.SubmitBatch(ctx, "g4-drill-batch", batchRequest())
	if err != nil || !created {
		t.Fatalf("SubmitBatch: created=%v err=%v", created, err)
	}

	// worker A 领到一条子 run，然后被杀（不再续租、不再上报）。
	claimed, err := svc.jobs.Claim(ctx, "w-dead", time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("claim by w-dead: %+v / %v", claimed, err)
	}
	failedID := claimed.ID

	// 每轮「租约过期 → 回收」让 attempts+1；第三轮耗尽默认预算（3）落 failed。
	// 第二轮起换 worker B 领取：证明被回收的活能被**别的** worker 接着干，且不新建行。
	expired := time.Now().Add(2 * time.Minute)
	for round := 1; round <= queue.DefaultMaxAttempts; round++ {
		if round > 1 {
			again, err := svc.jobs.Claim(ctx, "w-alive", time.Minute)
			if err != nil || again == nil {
				t.Fatalf("round %d: reclaim claim: %+v / %v", round, again, err)
			}
			if again.ID != failedID {
				t.Fatalf("round %d: reclaimed work must stay the same row: got %s want %s", round, again.ID, failedID)
			}
		}
		if _, err := svc.jobs.ReapExpired(ctx, expired); err != nil {
			t.Fatalf("round %d: reap: %v", round, err)
		}
	}

	row, err := svc.jobs.Get(ctx, failedID)
	if err != nil {
		t.Fatalf("get drilled row: %v", err)
	}
	if row.Status != queue.RunFailed || row.Attempts != queue.DefaultMaxAttempts {
		t.Fatalf("drilled row = %s/attempts %d, want failed/%d", row.Status, row.Attempts, queue.DefaultMaxAttempts)
	}
	if row.ErrorMessage != "attempts_exhausted" {
		t.Fatalf("error_message = %q, want attempts_exhausted", row.ErrorMessage)
	}

	// 另一条子 run 走取消：pending → cancelled，父汇总必须把它算成失败而不是继续等。
	lister, ok := svc.jobs.(queue.ParentChildLister)
	if !ok {
		t.Fatal("memory queue must implement ParentChildLister")
	}
	children, err := lister.ListChildren(ctx, parent.ID)
	if err != nil {
		t.Fatalf("ListChildren: %v", err)
	}
	if len(children) != parent.Total {
		t.Fatalf("children = %d, want %d（回收不得新建行）", len(children), parent.Total)
	}
	var cancelID string
	for _, child := range children {
		if child.ID != failedID && child.Status == queue.RunPending {
			cancelID = child.ID
			break
		}
	}
	if cancelID == "" {
		t.Fatalf("no pending child left to cancel: %+v", children)
	}
	if err := svc.jobs.RequestCancel(ctx, cancelID); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}

	got, err := svc.SyncSummary(ctx, parent.ID)
	if err != nil {
		t.Fatalf("SyncSummary: %v", err)
	}
	if got.Total != 4 || got.Completed != 0 || got.Invalid != 0 || got.Failed != 2 {
		t.Fatalf("parent counts = total %d / completed %d / invalid %d / failed %d, want 4/0/0/2",
			got.Total, got.Completed, got.Invalid, got.Failed)
	}
	if got.Status != queue.RunRunning {
		t.Fatalf("parent status = %q, want running（还剩两条 pending）", got.Status)
	}
	if len(got.SummaryJSON) == 0 {
		t.Fatal("summary_json must carry the aggregate detail")
	}
}
