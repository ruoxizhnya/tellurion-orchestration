package httpapi

// X5b 父作业端点的 HTTP 腿（内存实现，无库）。
//
// 钉住四件事：
//   - plan_hash 是身份：同计划（哪怕换幂等键）返回同一父 id；同键换计划 = 409；
//   - 无效计划 400 且报文点名出错的字段；
//   - 父状态不塌缩：三值计数逐项可见，pending 挡住完成、failed 不被吞；
//   - GET 给出逐子状态（子 run 的 K9 结论），不是把父状态复制 N 遍。

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"testing"
	"time"

	"tellurion-orchestration/internal/parents"
	"tellurion-orchestration/internal/plan"
	"tellurion-orchestration/internal/queue"
)

const parentBaseSpec = `{"strategy":"momentum","start_date":"2024-01-02","end_date":"2024-12-31","kernel":"go"}`

func newParentServer(t *testing.T) (*Server, *queue.Memory, *queue.MemoryParents) {
	t.Helper()
	m := queue.NewMemory(queue.MemoryOptions{Now: func() time.Time { return baseTime }})
	store := queue.NewMemoryParents(queue.MemoryOptions{
		Now:   func() time.Time { return baseTime },
		NewID: func() string { return "parent-fixed" },
	})
	return New(m, WithParents(store)), m, store
}

func batchParentBody(key string) string {
	return fmt.Sprintf(`{"idempotency_key":%q,"kind":"batch","batch":{"base_run_spec":%s,"grid":{"lookback":[10,20],"threshold":[0.1,0.2]}}}`,
		key, parentBaseSpec)
}

func decodeParentSubmit(t *testing.T, body string) parentSubmitBody {
	t.Helper()
	var out parentSubmitBody
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("parent submit body: %v\n%s", err, body)
	}
	return out
}

func TestSubmitParent_BatchPlanHashIsTheIdentity(t *testing.T) {
	s, _, _ := newParentServer(t)

	code, resp := do(t, s, "POST", "/api/runs/parents", batchParentBody("key-1"))
	if code != http.StatusAccepted {
		t.Fatalf("first submit: status = %d, want 202 (body %s)", code, resp)
	}
	first := decodeParentSubmit(t, resp)
	if first.ParentJobID == "" || first.Kind != "batch" || first.Status != "running" || first.Total != 4 {
		t.Fatalf("first submit body = %s", resp)
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(first.PlanHash) {
		t.Fatalf("plan_hash = %q, want 64 hex", first.PlanHash)
	}
	if first.Counts != (parentCountsView{Pending: 4}) {
		t.Fatalf("counts = %+v, want 0/0/0 pending 4", first.Counts)
	}
	if first.IdempotentReplay {
		t.Fatal("first submit must not be marked as a replay")
	}

	// 同键同计划 → 200 重放原行。
	code, resp = do(t, s, "POST", "/api/runs/parents", batchParentBody("key-1"))
	if code != http.StatusOK {
		t.Fatalf("same key replay: status = %d, want 200 (body %s)", code, resp)
	}
	replay := decodeParentSubmit(t, resp)
	if !replay.IdempotentReplay || replay.ParentJobID != first.ParentJobID {
		t.Fatalf("same key replay = %s", resp)
	}

	// 换幂等键、同一份计划 → 仍返回同一父 id（身份是 plan_hash，不是键）。
	code, resp = do(t, s, "POST", "/api/runs/parents", batchParentBody("key-2"))
	if code != http.StatusOK {
		t.Fatalf("same plan different key: status = %d, want 200 (body %s)", code, resp)
	}
	byPlan := decodeParentSubmit(t, resp)
	if !byPlan.IdempotentReplay || byPlan.ParentJobID != first.ParentJobID {
		t.Fatalf("same plan with a new key must return the original parent: %s", resp)
	}

	// 同键换计划 → 409；调用方的意图被改写，不得静默复用。
	other := fmt.Sprintf(`{"idempotency_key":"key-1","kind":"batch","batch":{"base_run_spec":%s,"grid":{"lookback":[5]}}}`, parentBaseSpec)
	doWant(t, s, "POST", "/api/runs/parents", other, http.StatusConflict, "idempotency_conflict")
}

func TestSubmitParent_InvalidPlanIs400AndNamesTheField(t *testing.T) {
	s, _, _ := newParentServer(t)

	cases := []struct {
		name string
		body string
		code int
		want string
	}{
		{"missing key", fmt.Sprintf(`{"kind":"batch","batch":{"base_run_spec":%s,"grid":{"a":[1]}}}`, parentBaseSpec),
			http.StatusBadRequest, "missing_idempotency_key"},
		{"unknown kind", `{"idempotency_key":"k","kind":"mystery"}`,
			http.StatusBadRequest, "kind"},
		{"missing batch payload", `{"idempotency_key":"k","kind":"batch"}`,
			http.StatusBadRequest, "batch"},
		{"batch without base spec", `{"idempotency_key":"k","kind":"batch","batch":{"grid":{"a":[1]}}}`,
			http.StatusBadRequest, "batch.base_run_spec"},
		{"empty grid", fmt.Sprintf(`{"idempotency_key":"k","kind":"batch","batch":{"base_run_spec":%s,"grid":{}}}`, parentBaseSpec),
			http.StatusBadRequest, "grid"},
		{"both payloads", fmt.Sprintf(`{"idempotency_key":"k","kind":"batch","batch":{"base_run_spec":%s,"grid":{"a":[1]}},"walkforward":{}}`, parentBaseSpec),
			http.StatusBadRequest, "walkforward"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, resp := do(t, s, "POST", "/api/runs/parents", tc.body)
			if code != tc.code {
				t.Fatalf("status = %d, want %d (body %s)", code, tc.code, resp)
			}
			if !contains(resp, tc.want) {
				t.Fatalf("error must name %q, got %s", tc.want, resp)
			}
		})
	}
}

func TestSubmitParent_WalkForwardCountsWindowsAndNamesBadDays(t *testing.T) {
	s, _, _ := newParentServer(t)

	days := make([]string, 10)
	start := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	for i := range days {
		days[i] = start.AddDate(0, 0, i).Format(plan.DateLayout)
	}
	daysJSON, err := json.Marshal(days)
	if err != nil {
		t.Fatal(err)
	}
	body := fmt.Sprintf(`{"idempotency_key":"wf-1","kind":"walkforward","walkforward":{
		"base_run_spec":%s,
		"windows":{"train_days":3,"test_days":2,"step_days":2},
		"trading_days":%s}}`, parentBaseSpec, daysJSON)

	code, resp := do(t, s, "POST", "/api/runs/parents", body)
	if code != http.StatusAccepted {
		t.Fatalf("walkforward submit: status = %d, want 202 (body %s)", code, resp)
	}
	out := decodeParentSubmit(t, resp)
	if out.Kind != "walkforward" || out.Total != 3 {
		t.Fatalf("walkforward body = %s, want kind=walkforward total=3 windows", resp)
	}

	bad := fmt.Sprintf(`{"idempotency_key":"wf-2","kind":"walkforward","walkforward":{
		"base_run_spec":%s,
		"windows":{"train_days":3,"test_days":2,"step_days":2},
		"trading_days":["2024-01-02","02/01/2024"]}}`, parentBaseSpec)
	code, resp = do(t, s, "POST", "/api/runs/parents", bad)
	if code != http.StatusBadRequest || !contains(resp, "walkforward.trading_days[1]") {
		t.Fatalf("bad trading day: status = %d body = %s", code, resp)
	}
}

// TestGetParent_CountsAndChildrenDoNotCollapse 是「父状态不塌缩」的可执行形式：
// 三值计数逐项保留（不是一句 completed 布尔），pending 挡住完成，failed 不被吞；
// children 给出每个子 run 自己的 K9 结论。
func TestGetParent_CountsAndChildrenDoNotCollapse(t *testing.T) {
	s, jobs, parentStore := newParentServer(t)
	svc := parents.New(parentStore)
	ctx := t.Context()

	code, resp := do(t, s, "POST", "/api/runs/parents", batchParentBody("batch-1"))
	if code != http.StatusAccepted {
		t.Fatalf("submit: %d %s", code, resp)
	}
	parent := decodeParentSubmit(t, resp)

	// 三个子 run：completed / invalid / failed 各一条（第四个留在 pending）。
	children := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		child, _, err := jobs.Enqueue(ctx, &queue.RunRecord{
			IdempotencyKey: fmt.Sprintf("child-%d", i),
			ParentJobID:    parent.ParentJobID,
			RequestJSON:    []byte(parentBaseSpec),
		})
		if err != nil {
			t.Fatal(err)
		}
		children = append(children, child.ID)
	}
	// 领取顺序由 id 决定，不一定等于入队顺序——结论按「实际被领取的子 run」
	// 记账，避免把 failed 贴到另一个 child 上。
	resultByChild := make(map[string]string, len(children))
	for i, result := range []string{"completed", "invalid", "failed"} {
		claimed, err := jobs.Claim(ctx, "w-1", time.Minute)
		if err != nil || claimed == nil {
			t.Fatalf("claim %d: job=%v err=%v", i, claimed, err)
		}
		resultByChild[claimed.ID] = result
		if result == "failed" {
			err = jobs.Fail(ctx, claimed.ID, "w-1", "engine boom", "failed")
		} else {
			err = jobs.Complete(ctx, claimed.ID, "w-1", result, "mem://results/"+claimed.ID)
		}
		if err != nil {
			t.Fatalf("terminal %d (%s): %v", i, result, err)
		}
	}

	// 汇总：1 completed / 1 invalid / 1 failed / 1 pending → running（不塌缩）。
	outcomes := make([]plan.TaskOutcome, 0, len(children)+1)
	for _, childID := range children {
		o := plan.TaskOutcome{RunID: childID, ResultStatus: queue.ResultStatus(resultByChild[childID])}
		if o.ResultStatus == queue.ResultCompleted {
			o.Score = ptrFloat(1.2)
		}
		if o.ResultStatus == queue.ResultFailed {
			o.Error = "engine boom"
		}
		outcomes = append(outcomes, o)
	}
	outcomes = append(outcomes, plan.TaskOutcome{RunID: "still-pending"})
	if _, err := svc.ApplyBatchSummary(ctx, parent.ParentJobID, outcomes); err != nil {
		t.Fatalf("apply summary: %v", err)
	}

	code, resp = do(t, s, "GET", "/api/runs/parents/"+parent.ParentJobID, "")
	if code != http.StatusOK {
		t.Fatalf("get parent: %d %s", code, resp)
	}
	var view parentView
	if err := json.Unmarshal([]byte(resp), &view); err != nil {
		t.Fatalf("parent view: %v\n%s", err, resp)
	}
	if view.Status != "running" {
		t.Fatalf("status = %q, want running（还有未回报的子 run）", view.Status)
	}
	want := parentCountsView{Completed: 1, Invalid: 1, Failed: 1, Pending: 1, Settled: 3}
	if view.Counts != want {
		t.Fatalf("counts = %+v, want %+v（三值不得被压成一个布尔）", view.Counts, want)
	}
	if len(view.Children) != 3 {
		t.Fatalf("children = %d, want 3", len(view.Children))
	}
	byID := map[string]parentChildView{}
	for _, c := range view.Children {
		byID[c.RunID] = c
	}
	for _, childID := range children {
		c, ok := byID[childID]
		if !ok {
			t.Fatalf("child %s missing from GET response", childID)
		}
		wantResult := resultByChild[childID]
		wantStatus := "completed"
		if wantResult == "failed" {
			wantStatus = "failed"
		}
		if c.Status != wantStatus {
			t.Fatalf("child %s status = %q, want %q（两轴：invalid 是 completed，failed 才是 failed）", childID, c.Status, wantStatus)
		}
		if c.ResultStatus == nil || *c.ResultStatus != wantResult {
			t.Fatalf("child %s result_status = %v, want %s", childID, c.ResultStatus, wantResult)
		}
	}

	// 全部落定且含失败 → failed；三值计数仍逐项保留。
	final := make([]plan.TaskOutcome, 0, len(children)+1)
	for _, childID := range children {
		final = append(final, plan.TaskOutcome{RunID: childID, ResultStatus: queue.ResultStatus(resultByChild[childID])})
	}
	final = append(final, plan.TaskOutcome{RunID: "still-pending", ResultStatus: queue.ResultFailed})
	if _, err := svc.ApplyBatchSummary(ctx, parent.ParentJobID, final); err != nil {
		t.Fatalf("apply final summary: %v", err)
	}
	code, resp = do(t, s, "GET", "/api/runs/parents/"+parent.ParentJobID, "")
	if code != http.StatusOK {
		t.Fatalf("get parent: %d %s", code, resp)
	}
	view = parentView{}
	if err := json.Unmarshal([]byte(resp), &view); err != nil {
		t.Fatalf("parent view: %v\n%s", err, resp)
	}
	if view.Status != "failed" {
		t.Fatalf("status = %q, want failed（任一子失败都不得被吞）", view.Status)
	}
	if view.Counts != (parentCountsView{Completed: 1, Invalid: 1, Failed: 2, Pending: 0, Settled: 4}) {
		t.Fatalf("final counts = %+v", view.Counts)
	}
	if view.CompletedAt == nil {
		t.Fatal("terminal parent must carry completed_at")
	}
}

func TestGetParent_FreshParentHasEmptyChildrenArray(t *testing.T) {
	s, _, _ := newParentServer(t)
	_, resp := do(t, s, "POST", "/api/runs/parents", batchParentBody("batch-1"))
	parent := decodeParentSubmit(t, resp)

	code, resp := do(t, s, "GET", "/api/runs/parents/"+parent.ParentJobID, "")
	if code != http.StatusOK {
		t.Fatalf("get parent: %d %s", code, resp)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(resp), &raw); err != nil {
		t.Fatal(err)
	}
	if string(raw["children"]) != "[]" {
		t.Fatalf("children = %s, want []（空列表 JSON 稳定）", raw["children"])
	}
}

func TestGetParent_UnknownIs404ParentNotFound(t *testing.T) {
	s, _, _ := newParentServer(t)
	doWant(t, s, "GET", "/api/runs/parents/nope", "", http.StatusNotFound, "parent_not_found")
}

func contains(haystack, needle string) bool {
	return len(needle) == 0 || regexp.MustCompile(regexp.QuoteMeta(needle)).MatchString(haystack)
}

func ptrFloat(v float64) *float64 { return &v }
