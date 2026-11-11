package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"tellurion-orchestration/internal/httpapi"
	"tellurion-orchestration/internal/pgstore"
	"tellurion-orchestration/internal/queue"
	"tellurion-orchestration/internal/testdb"
)

func TestResolveDSN(t *testing.T) {
	t.Setenv("ORCHESTRATION_DSN", "")
	if _, err := resolveDSN(""); err == nil {
		t.Fatal("empty flag + empty env must fail closed (no in-memory fallback)")
	}
	if _, err := resolveDSN("   "); err == nil {
		t.Fatal("whitespace-only DSN must fail closed")
	}
	t.Setenv("ORCHESTRATION_DSN", "postgres://from/env")
	got, err := resolveDSN("")
	if err != nil || got != "postgres://from/env" {
		t.Fatalf("env DSN: %q / %v", got, err)
	}
	got, err = resolveDSN("postgres://from/flag")
	if err != nil || got != "postgres://from/flag" {
		t.Fatalf("flag must win over env: %q / %v", got, err)
	}
}

// TestReapOnceMarksStaleWorkersOffline 钉住周期任务的**接线**：光有
// WorkerReaper 接口而 cmd 不调它，运行中的服务仍不会把死掉的 worker 摘出去。
// 用内存实现跑，不需要库。
func TestReapOnceMarksStaleWorkersOffline(t *testing.T) {
	ctx := context.Background()
	reg := queue.NewMemoryRegistry()
	jobs := queue.NewMemory(queue.MemoryOptions{Workers: reg})

	if err := reg.Register(ctx, queue.WorkerRecord{WorkerID: "w-stale"}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(ctx, queue.WorkerRecord{WorkerID: "w-fresh"}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Heartbeat(ctx, "w-stale", time.Now().UTC().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}

	reapOnce(ctx, jobs, reg, time.Minute, time.Now().UTC())

	list, err := reg.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status := make(map[string]queue.WorkerStatus, len(list))
	for _, w := range list {
		status[w.WorkerID] = w.Status
	}
	if status["w-stale"] != queue.WorkerOffline {
		t.Fatalf("w-stale = %q, want offline（周期任务没接上心跳回收）", status["w-stale"])
	}
	if status["w-fresh"] != queue.WorkerActive {
		t.Fatalf("w-fresh = %q, want active", status["w-fresh"])
	}

	// 关闭阈值 = 不做心跳回收（部署显式关掉时不偷偷按 0 处理成全离线）。
	if err := reg.Heartbeat(ctx, "w-fresh", time.Now().UTC().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	reapOnce(ctx, jobs, reg, 0, time.Now().UTC())
	if got := mustRegistryWorker(t, reg, "w-fresh"); got.Status != queue.WorkerActive {
		t.Fatalf("heartbeat timeout 0 must disable reaping, got %q", got.Status)
	}
}

func mustRegistryWorker(t *testing.T, reg queue.WorkerRegistry, id string) queue.WorkerRecord {
	t.Helper()
	list, err := reg.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range list {
		if w.WorkerID == id {
			return w
		}
	}
	t.Fatalf("worker %s not found", id)
	return queue.WorkerRecord{}
}

func TestDepsFieldsAreInterfaces(t *testing.T) {
	// 装配点声明成接口才有「换实现不改装配代码」的效果；
	// 写成 *pgstore.Queue 会让这条腿静默失效（core 的 QT-16 实测教训）。
	ty := reflect.TypeOf(Deps{})
	want := map[string]reflect.Type{
		"Jobs":    reflect.TypeOf((*queue.JobQueue)(nil)).Elem(),
		"Parents": reflect.TypeOf((*queue.ParentStore)(nil)).Elem(),
		"Workers": reflect.TypeOf((*queue.WorkerRegistry)(nil)).Elem(),
	}
	for name, iface := range want {
		f, ok := ty.FieldByName(name)
		if !ok {
			t.Fatalf("Deps has no field %s", name)
		}
		if f.Type != iface {
			t.Fatalf("Deps.%s = %s, want the interface %s（具体类型 = 换实现必须改装配）", name, f.Type, iface)
		}
		if iface.Kind() != reflect.Interface {
			t.Fatalf("harness bug: %s is not an interface type", iface)
		}
	}
}

type fakeReadiness struct{ err error }

func (f fakeReadiness) Ready(context.Context) error { return f.err }

// 探针分工（SVC-ORCH-G4 ②）：/healthz 只说进程活着；/readyz 才看 PG + 迁移。
// 业务面用 418 占位，任何探针落到业务面都会立刻暴露。
func TestProbesSplitProcessFromStore(t *testing.T) {
	api := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})

	srv := httptest.NewServer(newMux(api, fakeReadiness{}))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || strings.TrimSpace(string(body)) != "ok" {
		t.Fatalf("/healthz = %d %q, want 200 ok", resp.StatusCode, body)
	}

	resp, err = http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var ready map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&ready); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || ready["status"] != "ready" || ready["store"] != "postgres" {
		t.Fatalf("/readyz = %d %+v, want 200 ready/postgres", resp.StatusCode, ready)
	}
}

func TestReadyzFailsClosed(t *testing.T) {
	api := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})

	srv := httptest.NewServer(newMux(api, fakeReadiness{err: errors.New("pgstore: ping: connection refused")}))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var ready map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&ready); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable || ready["status"] != "not_ready" {
		t.Fatalf("/readyz = %d %+v, want 503 not_ready", resp.StatusCode, ready)
	}
	if !strings.Contains(ready["error"], "connection refused") {
		t.Fatalf("readyz must name the store failure: %+v", ready)
	}

	// 未装配判定（装配错误）也必须 503，不能默认 ready。
	unwired := httptest.NewServer(newMux(api, nil))
	defer unwired.Close()
	resp, err = http.Get(unwired.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unwired /readyz = %d, want 503", resp.StatusCode)
	}
}

// allowAllAuthorizer 替身：本用例考的是「队列真相在库里、跨进程可见」，
// 鉴权策略本身由 internal/httpapi 的用例覆盖。但不能不装——R5 之后未装配
// Authorizer 的受保护路由一律 503（fail-closed），漏装会让提交永远拿不到 202。
type allowAllAuthorizer struct{}

func (allowAllAuthorizer) Authorize(*http.Request) (string, error) { return "", nil }

// TestProbesExemptFromRateLimit 钉住 ORCH-RATELIMIT 的探针豁免：`/healthz`
// `/readyz` 挂在进程入口的 mux 上，不进业务限流桶——同一进程里连打探针
// 多少次都不 429，而业务面在突发耗尽后必须 429 且带 Retry-After。
// 探针被限流 = 部署面把「限流」读成「进程/依赖坏了」，会误重启。
func TestProbesExemptFromRateLimit(t *testing.T) {
	api := httpapi.New(queue.NewMemory(),
		httpapi.WithAuthorizer(allowAllAuthorizer{}),
		httpapi.WithRateLimit(0.0001, 1),
	).Handler()
	srv := httptest.NewServer(newMux(api, fakeReadiness{}))
	defer srv.Close()

	get := func(path string) *http.Response {
		t.Helper()
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp
	}

	for i := 0; i < 5; i++ {
		if code := get("/healthz").StatusCode; code != http.StatusOK {
			t.Fatalf("/healthz attempt %d = %d, want 200（探针不该占限流桶）", i, code)
		}
		if code := get("/readyz").StatusCode; code != http.StatusOK {
			t.Fatalf("/readyz attempt %d = %d, want 200（探针不该占限流桶）", i, code)
		}
	}

	// 业务面：突发 1 ⇒ 第一次 200，第二次 429 + Retry-After。
	if code := get("/api/runs/jobs").StatusCode; code != http.StatusOK {
		t.Fatalf("first business request = %d, want 200", code)
	}
	resp := get("/api/runs/jobs")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second business request = %d, want 429", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("429 rate_limited without Retry-After")
	}
}

// TestTwoProcessesShareTheQueueAgainstPG 是「接 PG」这件事的端到端判据：
// 进程 A（HTTP 面）提交，进程 B（另一个 Store 实例，独立连接池）领取并写终态，
// A 再查就必须看到 B 写的结果——队列的真相在库里，不在任何进程的内存里。
// 用内存实现跑这条会得到相反结论（B 看不见 A），所以它只在有库时跑。
func TestTwoProcessesShareTheQueueAgainstPG(t *testing.T) {
	dsn := os.Getenv("ORCHESTRATION_TEST_DSN")
	if dsn == "" {
		t.Skip("ORCHESTRATION_TEST_DSN is not set; skipping the PostgreSQL end-to-end leg")
	}
	ctx := context.Background()

	// harness 抢的是与 internal/pgstore **同一把**跨进程锁：本包的腿与那包的腿
	// 共用一个测试库，不排队就会互相清行（实测撞到过）。
	harness, err := testdb.Setup(ctx, dsn, 3*time.Minute)
	if err != nil {
		t.Fatalf("harness: %v", err)
	}
	defer harness.Close()
	if err := harness.Reset(ctx); err != nil {
		t.Fatalf("reset: %v", err)
	}
	a := harness.Store()

	// 进程 B：独立的第二个连接池，模拟另一个 worker 进程。
	b, err := pgstore.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open B: %v", err)
	}
	defer b.Close()

	// 进程 A：注册 + 起 HTTP 面（与 main 的装配顺序一致）。
	workersA := a.Registry()
	if err := workersA.Register(ctx, queue.WorkerRecord{WorkerID: "w-a", Kernels: []string{"go"}}); err != nil {
		t.Fatalf("register A: %v", err)
	}
	jobsA := a.Queue(workersA, pgstore.Options{MaxQueueDepth: 3, DefaultMaxAttempts: 2})
	srv := httptest.NewServer(httpapi.New(jobsA, httpapi.WithAuthorizer(allowAllAuthorizer{})).Handler())
	defer srv.Close()

	submit, err := http.Post(srv.URL+"/api/runs/jobs", "application/json",
		strings.NewReader(`{"idempotency_key":"e2e-1","priority":1,"run_spec":{"strategy":"momentum","capital":1000000,"slippage_rate":1e-05}}`))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	defer submit.Body.Close()
	if submit.StatusCode != http.StatusAccepted {
		t.Fatalf("submit status = %d, want 202", submit.StatusCode)
	}
	var body struct {
		JobID  string `json:"job_id"`
		Status string `json:"status"`
	}
	if err := json.NewDecoder(submit.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "pending" || body.JobID == "" {
		t.Fatalf("submit body = %+v", body)
	}

	// 进程 B：领取同一份库里的行并写终态。
	jobsB := b.Queue(nil, pgstore.Options{})
	claimed, err := jobsB.Claim(ctx, "w-b", time.Minute)
	if err != nil {
		t.Fatalf("claim B: %v", err)
	}
	if claimed == nil || claimed.ID != body.JobID {
		t.Fatalf("B claimed %+v, want %s", claimed, body.JobID)
	}
	if err := jobsB.Complete(ctx, body.JobID, "w-b", "invalid", "mem://results/"+body.JobID); err != nil {
		t.Fatalf("complete B: %v", err)
	}

	// A 侧必须看见 B 写的终态（两轴：status=completed + result_status=invalid + error 空）。
	get, err := http.Get(srv.URL + "/api/runs/jobs/" + body.JobID)
	if err != nil {
		t.Fatal(err)
	}
	defer get.Body.Close()
	var view struct {
		JobID        string  `json:"job_id"`
		Status       string  `json:"status"`
		ResultStatus *string `json:"result_status"`
		Error        string  `json:"error"`
		ResultURI    string  `json:"result_uri"`
	}
	if err := json.NewDecoder(get.Body).Decode(&view); err != nil {
		t.Fatal(err)
	}
	if view.Status != "completed" || view.ResultStatus == nil || *view.ResultStatus != "invalid" {
		t.Fatalf("cross-process terminal not visible: %+v", view)
	}
	if view.Error != "" {
		t.Fatalf("invalid 不是故障，error 必须为空: %q", view.Error)
	}
	if view.ResultURI == "" {
		t.Fatalf("result_uri pointer lost: %+v", view)
	}

	// B 也不能把同一份提交当成新任务（幂等键在库里，不在进程内存里）。
	replay, err := http.Post(srv.URL+"/api/runs/jobs", "application/json",
		strings.NewReader(`{"idempotency_key":"e2e-1","priority":1,"run_spec":{"slippage_rate":1e-05,"capital":1000000,"strategy":"momentum"}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Body.Close()
	if replay.StatusCode != http.StatusOK {
		t.Fatalf("replay status = %d, want 200 (same key, same canonical content)", replay.StatusCode)
	}
	var replayBody struct {
		JobID            string `json:"job_id"`
		IdempotentReplay bool   `json:"idempotent_replay"`
	}
	if err := json.NewDecoder(replay.Body).Decode(&replayBody); err != nil {
		t.Fatal(err)
	}
	if replayBody.JobID != body.JobID || !replayBody.IdempotentReplay {
		t.Fatalf("replay must return the original job with the replay marker: %+v", replayBody)
	}
	// 库里仍然只有一行。
	var rows int
	if err := a.Pool().QueryRow(ctx, `SELECT count(*) FROM run.backtest_jobs`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("row count = %d, want 1", rows)
	}
}

// TestResolveAuthMode 钉住启动期三态裁决与拒绝条件（六仓统一口径）。
func TestResolveAuthMode(t *testing.T) {
	cases := []struct {
		name          string
		secret        string
		allowInsecure bool
		host          string
		exposure      string
		want          authMode
		wantErr       bool
	}{
		{"secret wins", "s3cret", true, "0.0.0.0", "", authModeEnforced, false},
		{"secret wins even with insecure disabled", "s3cret", false, "127.0.0.1", "", authModeEnforced, false},
		{"no secret + no declaration = degraded", "", true, "127.0.0.1", "", authModeDegraded, false},
		{"no secret + container bind + no declaration = degraded", "", true, "0.0.0.0", "", authModeDegraded, false},
		{"explicit declaration on loopback = open", "", true, "127.0.0.1", "loopback-published", authModeOpen, false},
		{"explicit declaration on non-loopback refuses", "", true, "0.0.0.0", "loopback-published", 0, true},
		{"unknown declaration refuses", "", true, "127.0.0.1", "loopback", 0, true},
		{"insecure disabled refuses without secret", "", false, "127.0.0.1", "", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveAuthMode(tc.secret, tc.allowInsecure, tc.host, tc.exposure)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("resolveAuthMode = %v, want error (refuse to start)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveAuthMode error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("resolveAuthMode = %v, want %v", got, tc.want)
			}
		})
	}
}
