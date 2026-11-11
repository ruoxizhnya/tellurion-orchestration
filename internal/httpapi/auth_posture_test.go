package httpapi

import (
	"net/http"
	"testing"
	"time"

	"github.com/ruoxizhnya/tellurion-kit/auth"

	"tellurion-orchestration/internal/queue"
)

// TestAuthPostureThreeStates 钉住三态口径（六仓统一）：
//
//  1. 有 secret                → 全闸：写端点无凭证 401；
//  2. 无 secret + 无显式声明    → 只读降级：写端点 503 auth_unavailable，读端点公开；
//  3. 无 secret + 显式开放      → 写面全开放（仅启动期已裁决 loopback-published 时装配）。
func TestAuthPostureThreeStates(t *testing.T) {
	newJobsServer := func(a Authorizer) *Server {
		m := queue.NewMemory(queue.MemoryOptions{Now: func() time.Time { return baseTime }})
		return New(m, WithAuthorizer(a))
	}
	submitBody := `{"idempotency_key":"posture-k","run_spec":` + runSpecA + `}`

	// 第 1 态：有 secret，写端点无凭证 401。
	enforced := newJobsServer(NewAuthorizer(auth.NewTokenService(auth.Config{JWTSecret: []byte("orch-posture")})))
	code, body := do(t, enforced, http.MethodPost, "/api/runs/jobs", submitBody)
	wantCode(t, code, body, http.StatusUnauthorized, "unauthenticated")

	// 第 2 态：无 secret、无显式声明 → 写 503 / 读公开。
	degraded := newJobsServer(NewAuthorizer(auth.NewTokenService(auth.Config{})))
	code, body = do(t, degraded, http.MethodPost, "/api/runs/jobs", submitBody)
	wantCode(t, code, body, http.StatusServiceUnavailable, "auth_unavailable")
	// 读端点在降级态不过闸：handler 正常执行（不存在 ⇒ 404，而不是 401）。
	code, body = do(t, degraded, http.MethodGet, "/api/runs/jobs/does-not-exist", "")
	wantCode(t, code, body, http.StatusNotFound, "job_not_found")

	// 第 3 态：显式开放 → 写读都放行。
	open := newJobsServer(NewOpenAuthorizer())
	code, body = do(t, open, http.MethodPost, "/api/runs/jobs", submitBody)
	if code != http.StatusAccepted {
		t.Fatalf("open POST /api/runs/jobs = %d, want 202: %s", code, body)
	}
}
