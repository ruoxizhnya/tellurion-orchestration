package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"tellurion-orchestration/internal/queue"
)

var baseTime = time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)

func newServer() (*Server, *queue.Memory) {
	m := queue.NewMemory(queue.MemoryOptions{Now: func() time.Time { return baseTime }})
	return New(m), m
}

func do(t *testing.T, s *Server, method, path, body string) (int, string) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rd)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

func wantCode(t *testing.T, got int, body string, wantStatus int, wantCode string) {
	t.Helper()
	if got != wantStatus {
		t.Fatalf("status = %d, want %d (body %s)", got, wantStatus, body)
	}
	if wantCode == "" {
		return
	}
	var eb errorBody
	if err := json.Unmarshal([]byte(body), &eb); err != nil {
		t.Fatalf("error body is not JSON: %v\n%s", err, body)
	}
	if eb.Code != wantCode {
		t.Fatalf("code = %q, want %q (body %s)", eb.Code, wantCode, body)
	}
}

// doWant 把一次请求与「状态码 + wire code」断言并成一句（wantCode 不收多值调用）。
func doWant(t *testing.T, s *Server, method, path, body string, status int, code string) {
	t.Helper()
	got, resp := do(t, s, method, path, body)
	wantCode(t, got, resp, status, code)
}

const runSpecA = `{"kernel":"default","symbol":"600000.SH"}`

// submitJob 走 HTTP 提交一条并断言 202；返回 job_id。
func submitJob(t *testing.T, s *Server, key, runSpec string) string {
	t.Helper()
	body := fmt.Sprintf(`{"idempotency_key":%q,"run_spec":%s}`, key, runSpec)
	code, resp := do(t, s, "POST", "/api/runs/jobs", body)
	if code != 202 {
		t.Fatalf("submit %s: status = %d, want 202 (body %s)", key, code, resp)
	}
	var sb statusBody
	if err := json.Unmarshal([]byte(resp), &sb); err != nil {
		t.Fatalf("submit body: %v\n%s", err, resp)
	}
	if sb.JobID == "" || sb.Status != "pending" || sb.ResultStatus != nil || sb.IdempotentReplay {
		t.Fatalf("submit body = %s", resp)
	}
	return sb.JobID
}

func TestSubmit_202PendingWithNullResultStatus(t *testing.T) {
	s, _ := newServer()
	id := submitJob(t, s, "k1", runSpecA)

	code, resp := do(t, s, "GET", "/api/runs/jobs/"+id, "")
	wantCode(t, code, resp, 200, "")
	var jv jobView
	if err := json.Unmarshal([]byte(resp), &jv); err != nil {
		t.Fatalf("job view: %v\n%s", err, resp)
	}
	if jv.JobID != id || jv.Status != "pending" || jv.ResultStatus != nil {
		t.Fatalf("view = %+v", jv)
	}
	if jv.StartedAt != nil || jv.CompletedAt != nil {
		t.Fatalf("unstarted job must have null started/completed: %s", resp)
	}
	if jv.CreatedAt == nil || !jv.CreatedAt.Equal(baseTime) {
		t.Fatalf("created_at = %v, want %v", jv.CreatedAt, baseTime)
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(jv.RunSpecHash) {
		t.Fatalf("run_spec_hash = %q, want 64 hex", jv.RunSpecHash)
	}
}

func TestSubmit_IdempotentReplayIsKeyOrderIndependent(t *testing.T) {
	s, _ := newServer()
	id := submitJob(t, s, "k1", runSpecA)

	// 同一请求换个键序（与空格）再提交：同键同 request_hash → 200 重放原 job。
	replay := `{"run_spec":{"symbol":"600000.SH","kernel":"default"},"priority":0,"idempotency_key":"k1"}`
	code, resp := do(t, s, "POST", "/api/runs/jobs", replay)
	if code != 200 {
		t.Fatalf("replay status = %d, want 200 (body %s)", code, resp)
	}
	var sb statusBody
	if err := json.Unmarshal([]byte(resp), &sb); err != nil {
		t.Fatalf("replay body: %v\n%s", err, resp)
	}
	if !sb.IdempotentReplay || sb.JobID != id || sb.Status != "pending" {
		t.Fatalf("replay body = %s", resp)
	}
}

func TestSubmit_SameKeyDifferentRequestConflicts(t *testing.T) {
	s, _ := newServer()
	submitJob(t, s, "k1", runSpecA)
	body := `{"idempotency_key":"k1","run_spec":{"kernel":"default","symbol":"000001.SZ"}}`
	doWant(t, s, "POST", "/api/runs/jobs", body, 409, "idempotency_conflict")
}

func TestSubmit_FailsClosedOnBadRequests(t *testing.T) {
	s, _ := newServer()
	cases := []struct {
		name string
		body string
		code int
		want string
	}{
		{"missing key", `{"run_spec":` + runSpecA + `}`, 400, "missing_idempotency_key"},
		{"missing run_spec", `{"idempotency_key":"k1"}`, 400, "invalid_params"},
		{"unknown field", `{"idempotency_key":"k1","run_spec":` + runSpecA + `,"extra":1}`, 400, "invalid_params"},
		{"empty body", ``, 400, "invalid_params"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, resp := do(t, s, "POST", "/api/runs/jobs", tc.body)
			if code != tc.code || !strings.Contains(resp, tc.want) {
				t.Fatalf("status = %d, body = %s; want %d with %q", code, resp, tc.code, tc.want)
			}
		})
	}
}

func TestSubmit_QueueFull(t *testing.T) {
	m := queue.NewMemory(queue.MemoryOptions{
		Now:           func() time.Time { return baseTime },
		MaxQueueDepth: 1,
	})
	s := New(m)
	submitJob(t, s, "k1", runSpecA)
	body := `{"idempotency_key":"k2","run_spec":` + runSpecA + `}`
	doWant(t, s, "POST", "/api/runs/jobs", body, 429, "queue_full")
}

func TestCancel_PendingGoesCancelledRunningGoesCancelling(t *testing.T) {
	s, m := newServer()
	ctx := t.Context()

	pending := submitJob(t, s, "k-pending", runSpecA)
	code, resp := do(t, s, "DELETE", "/api/runs/jobs/"+pending, "")
	if code != 200 {
		t.Fatalf("cancel pending: status = %d, want 200 (body %s)", code, resp)
	}
	var sb statusBody
	if err := json.Unmarshal([]byte(resp), &sb); err != nil || sb.Status != "cancelled" || sb.JobID != pending {
		t.Fatalf("cancel pending body = %s (err %v)", resp, err)
	}
	// 重复取消终态：幂等 200 原状态。
	doWant(t, s, "DELETE", "/api/runs/jobs/"+pending, "", 200, "")

	running := submitJob(t, s, "k-running", runSpecA)
	if _, err := m.Claim(ctx, "w1", time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	code, resp = do(t, s, "DELETE", "/api/runs/jobs/"+running, "")
	if code != 202 {
		t.Fatalf("cancel running: status = %d, want 202 (body %s)", code, resp)
	}
	sb = statusBody{}
	if err := json.Unmarshal([]byte(resp), &sb); err != nil || sb.Status != "cancelling" {
		t.Fatalf("cancel running body = %s (err %v)", resp, err)
	}
}

func TestCancel_CompletedIsIdempotentOriginalState(t *testing.T) {
	s, m := newServer()
	id := submitJob(t, s, "k1", runSpecA)
	if _, err := m.Claim(t.Context(), "w1", time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := m.Complete(t.Context(), id, "w1", "completed", "file:///results/r1"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	code, resp := do(t, s, "DELETE", "/api/runs/jobs/"+id, "")
	if code != 200 {
		t.Fatalf("cancel completed: status = %d, want 200 (body %s)", code, resp)
	}
	var sb statusBody
	if err := json.Unmarshal([]byte(resp), &sb); err != nil || sb.Status != "completed" {
		t.Fatalf("cancel completed body = %s (err %v)", resp, err)
	}
}

func TestCancel_UnknownJob(t *testing.T) {
	s, _ := newServer()
	doWant(t, s, "DELETE", "/api/runs/jobs/nope", "", 404, "job_not_found")
	doWant(t, s, "GET", "/api/runs/jobs/nope", "", 404, "job_not_found")
}

func TestGet_TerminalViews(t *testing.T) {
	s, m := newServer()
	ctx := t.Context()

	done := submitJob(t, s, "k-done", runSpecA)
	if _, err := m.Claim(ctx, "w1", time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := m.Complete(ctx, done, "w1", "completed", "file:///results/r1"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	code, resp := do(t, s, "GET", "/api/runs/jobs/"+done, "")
	wantCode(t, code, resp, 200, "")
	var jv jobView
	if err := json.Unmarshal([]byte(resp), &jv); err != nil {
		t.Fatalf("view: %v\n%s", err, resp)
	}
	if jv.Status != "completed" || jv.ResultStatus == nil || *jv.ResultStatus != "completed" {
		t.Fatalf("completed view = %s", resp)
	}
	if jv.ResultURI != "file:///results/r1" || jv.CompletedAt == nil {
		t.Fatalf("completed view = %s", resp)
	}

	invalid := submitJob(t, s, "k-invalid", runSpecA)
	if _, err := m.Claim(ctx, "w1", time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// K9 invalid 不是错误：status=completed、error 为空。
	if err := m.Complete(ctx, invalid, "w1", "invalid", "file:///results/r2"); err != nil {
		t.Fatalf("complete invalid: %v", err)
	}
	_, resp = do(t, s, "GET", "/api/runs/jobs/"+invalid, "")
	jv = jobView{}
	if err := json.Unmarshal([]byte(resp), &jv); err != nil {
		t.Fatalf("view: %v\n%s", err, resp)
	}
	if jv.Status != "completed" || jv.ResultStatus == nil || *jv.ResultStatus != "invalid" || jv.Error != "" {
		t.Fatalf("invalid run must surface as completed/invalid with empty error: %s", resp)
	}

	failed := submitJob(t, s, "k-failed", runSpecA)
	if _, err := m.Claim(ctx, "w1", time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := m.Fail(ctx, failed, "w1", "engine boom", "failed"); err != nil {
		t.Fatalf("fail: %v", err)
	}
	_, resp = do(t, s, "GET", "/api/runs/jobs/"+failed, "")
	jv = jobView{}
	if err := json.Unmarshal([]byte(resp), &jv); err != nil {
		t.Fatalf("view: %v\n%s", err, resp)
	}
	if jv.Status != "failed" || jv.ResultStatus == nil || *jv.ResultStatus != "failed" || jv.Error != "engine boom" {
		t.Fatalf("failed view = %s", resp)
	}
}

func TestList_FiltersLimitAndFailClosed(t *testing.T) {
	s, _ := newServer()
	for i, key := range []string{"k1", "k2", "k3"} {
		spec := runSpecA
		if i == 1 {
			spec = `{"kernel":"other","symbol":"000001.SZ"}`
		}
		submitJob(t, s, key, spec)
	}

	code, resp := do(t, s, "GET", "/api/runs/jobs", "")
	wantCode(t, code, resp, 200, "")
	var lb listBody
	if err := json.Unmarshal([]byte(resp), &lb); err != nil {
		t.Fatalf("list body: %v\n%s", err, resp)
	}
	if lb.Total != 3 || len(lb.Jobs) != 3 {
		t.Fatalf("list = %+v", lb)
	}
	if lb.Jobs[0].JobID == "" || lb.Jobs[0].Status != "pending" {
		t.Fatalf("list rows = %+v", lb.Jobs[0])
	}
	// jobs 必须是 [] 而不是 null（空列表 JSON 稳定）。
	if lb.Jobs == nil {
		t.Fatal("jobs must serialize as [], not null")
	}

	code, resp = do(t, s, "GET", "/api/runs/jobs?limit=2", "")
	if err := json.Unmarshal([]byte(resp), &lb); err != nil {
		t.Fatalf("list body: %v", err)
	}
	if code != 200 || len(lb.Jobs) != 2 || lb.Total != 2 {
		t.Fatalf("limit=2: status %d body %s", code, resp)
	}

	code, resp = do(t, s, "GET", "/api/runs/jobs?status=pending", "")
	if code != 200 {
		t.Fatalf("status filter: %d %s", code, resp)
	}
	doWant(t, s, "GET", "/api/runs/jobs?status=bogus", "", 400, "invalid_params")
	doWant(t, s, "GET", "/api/runs/jobs?result_status=bogus", "", 400, "invalid_params")
	doWant(t, s, "GET", "/api/runs/jobs?limit=abc", "", 400, "invalid_params")
}
