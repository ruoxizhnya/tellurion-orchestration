package httpapi

// ORCH-RATELIMIT 的验收用例：按调用方身份的令牌桶（超限 429 `rate_limited` +
// `Retry-After`）、`queue_full` 429 同样带 `Retry-After`、鉴权先于限流
// （401 不被 429 顶替）、身份分桶互不串用。探针豁免在进程入口的 mux 上，
// 由 cmd/orchestrationservice 的用例覆盖。

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ruoxizhnya/tellurion-kit/auth"

	"tellurion-orchestration/internal/queue"
)

// limitedServer 用极低额度造超限：rate 极低、突发 1 ⇒ 同一身份第 2 次即 429。
func limitedServer(t *testing.T, tokens *auth.TokenService) *Server {
	t.Helper()
	m := queue.NewMemory(queue.MemoryOptions{Now: func() time.Time { return baseTime }})
	authn := Authorizer(allowAllAuthorizer{})
	if tokens != nil {
		authn = NewAuthorizer(tokens)
	}
	return New(m, WithAuthorizer(authn), WithRateLimit(0.0001, 1))
}

// doRecord 是带响应头的请求（429 的 Retry-After 只能从 recorder 读到）。
func doRecord(s *Server, method, path, body, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

func decodeErrorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var eb errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &eb); err != nil {
		t.Fatalf("error body is not JSON: %v\n%s", err, rec.Body.String())
	}
	return eb.Code
}

// 超限 ⇒ 429 `rate_limited` + `Retry-After` 非空。
func TestRateLimitOverLimitHasRetryAfter(t *testing.T) {
	s := limitedServer(t, nil)

	// 第一次消耗唯一令牌，落到 handler（未知 run → 404）。
	code, body := do(t, s, http.MethodGet, "/api/runs/jobs/no-such-run", "")
	wantCode(t, code, body, http.StatusNotFound, "job_not_found")

	rec := doRecord(s, http.MethodGet, "/api/runs/jobs/no-such-run", "", "")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second request = %d, want 429 (body %s)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Retry-After"); got == "" {
		t.Fatal("429 rate_limited without Retry-After：调用方无法知道何时重试")
	}
	if got := decodeErrorCode(t, rec); got != "rate_limited" {
		t.Fatalf("code = %q, want rate_limited", got)
	}
}

// `queue_full` ⇒ 429 + `Retry-After` 非空（与限流 429 同口径）。
func TestQueueFull429HasRetryAfter(t *testing.T) {
	m := queue.NewMemory(queue.MemoryOptions{
		Now:           func() time.Time { return baseTime },
		MaxQueueDepth: 1,
	})
	// 关掉限流，只考 queue_full 这一条判据。
	s := New(m, WithAuthorizer(allowAllAuthorizer{}), WithRateLimit(0, 0))
	submitJob(t, s, "full-1", runSpecA)

	rec := doRecord(s, http.MethodPost, "/api/runs/jobs",
		`{"idempotency_key":"full-2","run_spec":`+runSpecA+`}`, "")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("queue full = %d, want 429 (body %s)", rec.Code, rec.Body.String())
	}
	if got := decodeErrorCode(t, rec); got != "queue_full" {
		t.Fatalf("code = %q, want queue_full", got)
	}
	if got := rec.Header().Get("Retry-After"); got == "" {
		t.Fatal("429 queue_full without Retry-After：调用方无法区分队列满与服务坏")
	}
}

// 鉴权先于限流：桶耗尽后，无凭证仍必须 401，不被 429 顶替。
func TestAuthPrecedesRateLimit(t *testing.T) {
	tokens := auth.NewTokenService(auth.Config{JWTSecret: []byte("orch-limit-secret")})
	s := limitedServer(t, tokens)
	service := mustServiceToken(t, tokens)

	// 先耗尽 service 身份的唯一令牌（过闸 → 404 job_not_found）。
	code, body := doAuth(t, s, http.MethodGet, "/api/runs/jobs/no-such-run", "", service)
	wantCode(t, code, body, http.StatusNotFound, "job_not_found")

	// 无凭证：即使桶已空也必须是 401。
	code, body = doAuth(t, s, http.MethodGet, "/api/runs/jobs/no-such-run", "", "")
	wantCode(t, code, body, http.StatusUnauthorized, "unauthenticated")

	// 同一身份继续打：429 + Retry-After（而不是 401）。
	rec := doRecord(s, http.MethodGet, "/api/runs/jobs/no-such-run", "", service)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("exhausted identity = %d, want 429 (body %s)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Retry-After"); got == "" {
		t.Fatal("429 rate_limited without Retry-After")
	}
	if got := decodeErrorCode(t, rec); got != "rate_limited" {
		t.Fatalf("code = %q, want rate_limited", got)
	}
}

// 桶按身份隔离：一个调用方耗尽不会波及另一个。
func TestRateLimitBucketsPerIdentity(t *testing.T) {
	tokens := auth.NewTokenService(auth.Config{JWTSecret: []byte("orch-limit-bucket-secret")})
	s := limitedServer(t, tokens)
	a := mustServiceTokenAs(t, tokens, "service:a")
	b := mustServiceTokenAs(t, tokens, "service:b")

	if code, body := doAuth(t, s, http.MethodGet, "/api/runs/jobs/no-such-run", "", a); code != http.StatusNotFound {
		t.Fatalf("identity a first = %d, want 404 (body %s)", code, body)
	}
	// a 的桶已空；b 的桶仍是满的 ⇒ 必须过闸（404，而不是 429）。
	if code, body := doAuth(t, s, http.MethodGet, "/api/runs/jobs/no-such-run", "", b); code != http.StatusNotFound {
		t.Fatalf("identity b first = %d, want 404（身份之间串桶了）(body %s)", code, body)
	}
	rec := doRecord(s, http.MethodGet, "/api/runs/jobs/no-such-run", "", a)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("identity a second = %d, want 429 (body %s)", rec.Code, rec.Body.String())
	}
}

func mustServiceTokenAs(t *testing.T, tokens *auth.TokenService, subject string) string {
	t.Helper()
	token, err := tokens.IssueServiceToken(subject, auth.ServiceRole, time.Hour)
	if err != nil {
		t.Fatalf("issue service token %q: %v", subject, err)
	}
	return token
}
