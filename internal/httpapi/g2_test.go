package httpapi

// SVC-ORCH-G2 ③④ 的验收用例：artifact 写端点（201/200/409/404）、身份漂移
// 不覆盖原行、created_at 不刷新、R5 最小面（service 角色）与 fail-closed。

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ruoxizhnya/tellurion-kit/auth"

	"tellurion-orchestration/internal/queue"
)

// allowAllAuthorizer 只给「不测鉴权」的用例用；生产装配永远过 kit。
type allowAllAuthorizer struct{}

func (allowAllAuthorizer) Authorize(*http.Request) (string, error) { return "", nil }

func newArtifactServer(t *testing.T, tokens *auth.TokenService) (*Server, *queue.Memory, *queue.MemoryArtifacts, *time.Time) {
	t.Helper()
	now := baseTime
	clock := func() time.Time { return now }
	m := queue.NewMemory(queue.MemoryOptions{Now: clock})
	arts := queue.NewMemoryArtifacts(m, queue.MemoryOptions{Now: clock})
	authn := Authorizer(allowAllAuthorizer{})
	if tokens != nil {
		authn = NewAuthorizer(tokens)
	}
	return New(m, WithArtifacts(arts), WithAuthorizer(authn)), m, arts, &now
}

func doAuth(t *testing.T, s *Server, method, path, body, token string) (int, string) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

func artifactBody(paramsHash string) string {
	return fmt.Sprintf(`{"strategy_ref":"strategy-a","dataset_snapshot_hash":"dataset-1",`+
		`"run_spec_hash":"spec-1","params_hash":%q,"kernel":"default",`+
		`"engine_version":"1.0.0","fill_timing":"next_open"}`, paramsHash)
}

func TestSaveArtifact_CreatedThenReplayKeepsCreatedAt(t *testing.T) {
	s, _, arts, now := newArtifactServer(t, nil)
	runID := submitJob(t, s, "artifact-created", runSpecA)

	path := "/api/runs/jobs/" + runID + "/artifacts"
	code, body := do(t, s, http.MethodPost, path, artifactBody("p1"))
	if code != http.StatusCreated {
		t.Fatalf("first write = %d, want 201 (body %s)", code, body)
	}
	var first queue.ArtifactRecord
	if err := json.Unmarshal([]byte(body), &first); err != nil {
		t.Fatalf("first body: %v\n%s", err, body)
	}
	if !first.CreatedAt.Equal(baseTime) {
		t.Fatalf("created_at = %v, want %v", first.CreatedAt, baseTime)
	}

	*now = baseTime.Add(time.Hour)
	code, body = do(t, s, http.MethodPost, path, artifactBody("p1"))
	if code != http.StatusOK {
		t.Fatalf("replay = %d, want 200 (body %s)", code, body)
	}
	var second queue.ArtifactRecord
	if err := json.Unmarshal([]byte(body), &second); err != nil {
		t.Fatalf("second body: %v\n%s", err, body)
	}
	if !second.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("created_at refreshed on replay: %v → %v", first.CreatedAt, second.CreatedAt)
	}
	stored, err := arts.GetArtifact(t.Context(), runID)
	if err != nil {
		t.Fatalf("GetArtifact: %v", err)
	}
	if !stored.CreatedAt.Equal(baseTime) {
		t.Fatalf("stored created_at = %v, want %v", stored.CreatedAt, baseTime)
	}
}

func TestSaveArtifact_IdentityDriftConflictsWithoutOverwrite(t *testing.T) {
	s, _, arts, _ := newArtifactServer(t, nil)
	runID := submitJob(t, s, "artifact-drift", runSpecA)
	path := "/api/runs/jobs/" + runID + "/artifacts"

	if code, body := do(t, s, http.MethodPost, path, artifactBody("p1")); code != http.StatusCreated {
		t.Fatalf("first write = %d, want 201 (body %s)", code, body)
	}
	code, body := do(t, s, http.MethodPost, path, artifactBody("p2"))
	wantCode(t, code, body, http.StatusConflict, "artifact_conflict")

	stored, err := arts.GetArtifact(t.Context(), runID)
	if err != nil {
		t.Fatalf("GetArtifact: %v", err)
	}
	if stored.ParamsHash != "p1" {
		t.Fatalf("drifted write overwrote the original row: params_hash = %q", stored.ParamsHash)
	}
}

func TestSaveArtifact_UnknownRunIs404(t *testing.T) {
	s, _, _, _ := newArtifactServer(t, nil)
	code, body := do(t, s, http.MethodPost, "/api/runs/jobs/no-such-run/artifacts", artifactBody("p1"))
	wantCode(t, code, body, http.StatusNotFound, "job_not_found")
}

func TestSaveArtifact_RejectsRunIDMismatch(t *testing.T) {
	s, _, _, _ := newArtifactServer(t, nil)
	runID := submitJob(t, s, "artifact-mismatch", runSpecA)
	body := `{"run_id":"someone-else","strategy_ref":"s","dataset_snapshot_hash":"d",` +
		`"run_spec_hash":"r","params_hash":"p","kernel":"default",` +
		`"engine_version":"1","fill_timing":"next_open"}`
	code, resp := do(t, s, http.MethodPost, "/api/runs/jobs/"+runID+"/artifacts", body)
	wantCode(t, code, resp, http.StatusBadRequest, "invalid_params")
}

func TestSaveArtifact_RequiresServiceRole(t *testing.T) {
	tokens := auth.NewTokenService(auth.Config{JWTSecret: []byte("g2-orch-secret")})
	s, _, _, _ := newArtifactServer(t, tokens)
	service := mustServiceToken(t, tokens)
	runID := submitJobAs(t, s, service, "artifact-auth", runSpecA)
	path := "/api/runs/jobs/" + runID + "/artifacts"

	code, body := doAuth(t, s, http.MethodPost, path, artifactBody("p1"), "")
	wantCode(t, code, body, http.StatusUnauthorized, "unauthenticated")

	viewer := mustToken(t, tokens, "viewer")
	code, body = doAuth(t, s, http.MethodPost, path, artifactBody("p1"), viewer)
	wantCode(t, code, body, http.StatusForbidden, "forbidden")

	code, body = doAuth(t, s, http.MethodPost, path, artifactBody("p1"), service)
	if code != http.StatusCreated {
		t.Fatalf("service write = %d, want 201 (body %s)", code, body)
	}
}

func TestArtifactWriteFailsClosedWithoutAuthorizer(t *testing.T) {
	now := baseTime
	clock := func() time.Time { return now }
	m := queue.NewMemory(queue.MemoryOptions{Now: clock})
	s := New(m, WithArtifacts(queue.NewMemoryArtifacts(m, queue.MemoryOptions{Now: clock})))
	// 全量 R5 后每条路由都过鉴权闸：未装配 Authorizer 时，鉴权在 handler 之前
	// 就回 503，故不需要真实 run（本用例测的是「闸在不在」，不是业务校验）。
	code, body := do(t, s, http.MethodPost, "/api/runs/jobs/artifact-unwired/artifacts", artifactBody("p1"))
	wantCode(t, code, body, http.StatusServiceUnavailable, "auth_unavailable")
}

func TestPolicyCoversAllRoutesAsService(t *testing.T) {
	// 全量 R5 的路由普查：每一条 /api/* 都必须先过鉴权闸（无凭证 = 401），
	// 且策略表逐条登记为 service。新增路由不改这里 + 策略表 ⇒ 这条会红。
	routes := []struct{ method, path, key string }{
		{http.MethodPost, "/api/runs/jobs", "POST /api/runs/jobs"},
		{http.MethodGet, "/api/runs/jobs", "GET /api/runs/jobs"},
		{http.MethodGet, "/api/runs/jobs/x", "GET /api/runs/jobs/{id}"},
		{http.MethodDelete, "/api/runs/jobs/x", "DELETE /api/runs/jobs/{id}"},
		{http.MethodPost, "/api/runs/parents", "POST /api/runs/parents"},
		{http.MethodGet, "/api/runs/parents/x", "GET /api/runs/parents/{id}"},
		{http.MethodGet, "/api/runs/x/provenance", "GET /api/runs/{rest...}"},
		{http.MethodPost, "/api/runs/jobs/x/artifacts", "POST /api/runs/jobs/{id}/artifacts"},
		{http.MethodGet, "/api/runs/workers", "GET /api/runs/workers"},
		{http.MethodPost, "/api/runs/workers", "POST /api/runs/workers"},
		{http.MethodPost, "/api/runs/workers/x/heartbeat", "POST /api/runs/workers/{id}/heartbeat"},
		{http.MethodPost, "/api/runs/workers/x/claim", "POST /api/runs/workers/{id}/claim"},
		{http.MethodPost, "/api/runs/workers/x/drain", "POST /api/runs/workers/{id}/drain"},
		{http.MethodPost, "/api/runs/jobs/x/lease", "POST /api/runs/jobs/{id}/lease"},
		{http.MethodPost, "/api/runs/jobs/x/result", "POST /api/runs/jobs/{id}/result"},
		{http.MethodPost, "/api/runs/jobs/x/cancel-ack", "POST /api/runs/jobs/{id}/cancel-ack"},
	}

	tokens := auth.NewTokenService(auth.Config{JWTSecret: []byte("g2-orch-secret")})
	clock := func() time.Time { return baseTime }
	m := queue.NewMemory(queue.MemoryOptions{Now: clock})
	reg := queue.NewMemoryRegistry(queue.RegistryOptions{Now: clock})
	s := New(m,
		WithParents(queue.NewMemoryParents(queue.MemoryOptions{Now: clock})),
		WithArtifacts(queue.NewMemoryArtifacts(m, queue.MemoryOptions{Now: clock})),
		WithWorkers(reg),
		WithAuthorizer(NewAuthorizer(tokens)),
	)

	entries := PolicyEntries()
	if len(entries) != len(routes) {
		t.Fatalf("policy entries = %d, want %d (route census)", len(entries), len(routes))
	}
	for _, rt := range routes {
		key := rt.key
		rule, ok := entries[key]
		if !ok {
			t.Fatalf("route %q has no policy entry", key)
		}
		if len(rule.Roles) != 1 || rule.Roles[0] != auth.ServiceRole {
			t.Fatalf("%s roles = %v, want [service]", key, rule.Roles)
		}

		req := httptest.NewRequest(rt.method, rt.path, strings.NewReader(`{}`))
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s without token = %d, want 401 (gate not applied)", key, rec.Code)
		}
	}
	if AuthStatus != "all-endpoints-service" {
		t.Fatalf("AuthStatus = %q, want all-endpoints-service", AuthStatus)
	}
}

func mustToken(t *testing.T, tokens *auth.TokenService, role string) string {
	t.Helper()
	access, _, err := tokens.IssueTokens(42, "alice", role)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	return access
}

func mustServiceToken(t *testing.T, tokens *auth.TokenService) string {
	t.Helper()
	token, err := tokens.IssueServiceToken("service:core", auth.ServiceRole, time.Hour)
	if err != nil {
		t.Fatalf("issue service token: %v", err)
	}
	return token
}
