package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"tellurion-orchestration/internal/queue"
)

// newWorkerServer 装配 jobs + 父作业 + worker 三面，全部走内存实现。
//
// 时钟用一个可变指针：租约 / 父汇总幂等这些用例要能推进时间。
func newWorkerServer(t *testing.T) (*Server, *queue.Memory, *queue.MemoryRegistry, *time.Time) {
	t.Helper()
	clock := baseTime
	now := func() time.Time { return clock }
	reg := queue.NewMemoryRegistry(queue.RegistryOptions{Now: now})
	jobs := queue.NewMemory(queue.MemoryOptions{Now: now, Workers: reg})
	parents := queue.NewMemoryParents(queue.MemoryOptions{Now: now})
	s := New(jobs, WithParents(parents), WithWorkers(reg))
	return s, jobs, reg, &clock
}

func registerWorker(t *testing.T, s *Server, id, kernels string) {
	t.Helper()
	body := fmt.Sprintf(`{"worker_id":%q,"kernels":%s}`, id, kernels)
	code, resp := do(t, s, "POST", "/api/runs/workers", body)
	if code != 200 {
		t.Fatalf("register %s: status = %d (body %s)", id, code, resp)
	}
}

type claimResponse struct {
	Job *claimView `json:"job"`
}

func claim(t *testing.T, s *Server, workerID string) *claimView {
	t.Helper()
	code, resp := do(t, s, "POST", "/api/runs/workers/"+workerID+"/claim", "")
	if code != 200 {
		t.Fatalf("claim %s: status = %d (body %s)", workerID, code, resp)
	}
	var body claimResponse
	if err := json.Unmarshal([]byte(resp), &body); err != nil {
		t.Fatalf("claim body: %v\n%s", err, resp)
	}
	return body.Job
}

func reportCompleted(t *testing.T, s *Server, runID, workerID, resultStatus string) {
	t.Helper()
	body := fmt.Sprintf(`{"worker_id":%q,"status":"completed","result_status":%q,"result_uri":"backtest://%s"}`,
		workerID, resultStatus, runID)
	code, resp := do(t, s, "POST", "/api/runs/jobs/"+runID+"/result", body)
	if code != 200 {
		t.Fatalf("report %s: status = %d (body %s)", runID, code, resp)
	}
}

func TestRegisterWorker_NormalizesEmptyKernelsToNil(t *testing.T) {
	s, _, reg, _ := newWorkerServer(t)

	registerWorker(t, s, "w-empty", `[]`)
	workers, err := reg.List(t.Context())
	if err != nil {
		t.Fatalf("list workers: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("workers = %d, want 1", len(workers))
	}
	// 空数组必须落成 NULL：PG 的 `= ANY('{}')` 是「什么都不能干」，
	// 而「未声明能力」的语义是不过滤。两者不能混。
	if workers[0].Kernels != nil {
		t.Fatalf("kernels = %#v, want nil", workers[0].Kernels)
	}

	code, resp := do(t, s, "GET", "/api/runs/workers", "")
	if code != 200 {
		t.Fatalf("list: %d %s", code, resp)
	}
	var list workerListBody
	if err := json.Unmarshal([]byte(resp), &list); err != nil {
		t.Fatalf("list body: %v\n%s", err, resp)
	}
	if list.Workers[0].Kernels == nil || len(list.Workers[0].Kernels) != 0 {
		t.Fatalf("wire kernels = %#v, want []", list.Workers[0].Kernels)
	}
}

// recordingRegistry 记录 Register 收到的**原始**记录：内存实现的 Clone 会把
// 非 nil 空切片顺手变成 nil，只看 List 的结果会让「HTTP 层没归一」这条缺陷
// 被实现细节盖住（实测过的假护栏）。
type recordingRegistry struct {
	queue.WorkerRegistry
	last queue.WorkerRecord
}

func (r *recordingRegistry) Register(ctx context.Context, w queue.WorkerRecord) error {
	// 不 Clone：Clone 会把空切片洗成 nil，正好掩盖要测的那一层。
	r.last = w
	return r.WorkerRegistry.Register(ctx, w)
}

func TestRegisterWorker_HandsNilKernelsToRegistry(t *testing.T) {
	reg := &recordingRegistry{WorkerRegistry: queue.NewMemoryRegistry()}
	jobs := queue.NewMemory(queue.MemoryOptions{Now: func() time.Time { return baseTime }, Workers: reg})
	s := New(jobs, WithWorkers(reg))

	registerWorker(t, s, "w-empty", `[]`)
	if reg.last.Kernels != nil {
		t.Fatalf("registry received kernels = %#v, want nil (empty array must normalize at the HTTP layer)", reg.last.Kernels)
	}
}

func TestRegisterWorker_DedupesAndRejectsEmptyKernelNames(t *testing.T) {
	s, _, reg, _ := newWorkerServer(t)

	registerWorker(t, s, "w1", `[" go ", "go", "nautilus"]`)
	workers, _ := reg.List(t.Context())
	if got := workers[0].Kernels; len(got) != 2 || got[0] != "go" || got[1] != "nautilus" {
		t.Fatalf("kernels = %#v, want [go nautilus]", got)
	}

	doWant(t, s, "POST", "/api/runs/workers", `{"worker_id":"w2","kernels":[""]}`, 400, "invalid_params")
}

func TestWorkerEndpoints_RequireRegisteredWorker(t *testing.T) {
	s, _, _, _ := newWorkerServer(t)

	doWant(t, s, "POST", "/api/runs/workers/ghost/heartbeat", "", 404, "worker_not_found")
	doWant(t, s, "POST", "/api/runs/workers/ghost/claim", "", 404, "worker_not_found")
}

func TestClaim_FiltersByDeclaredKernels(t *testing.T) {
	s, _, _, _ := newWorkerServer(t)
	registerWorker(t, s, "w-go", `["go"]`)
	registerWorker(t, s, "w-nt", `["nautilus"]`)

	goJob := submitJob(t, s, "k-go", `{"kernel":"go","strategy":"m"}`)
	ntJob := submitJob(t, s, "k-nt", `{"kernel":"nautilus","strategy":"m"}`)

	if job := claim(t, s, "w-go"); job == nil || job.RunID != goJob {
		t.Fatalf("w-go claimed %+v, want %s", job, goJob)
	}
	if job := claim(t, s, "w-go"); job != nil {
		t.Fatalf("w-go claimed a second job: %+v", job)
	}
	if job := claim(t, s, "w-nt"); job == nil || job.RunID != ntJob {
		t.Fatalf("w-nt claimed %+v, want %s", job, ntJob)
	}
	if job := claim(t, s, "w-nt"); job != nil {
		t.Fatalf("w-nt claimed a second job: %+v", job)
	}
}

func TestClaim_EmptyDeclarationDoesNotFilter(t *testing.T) {
	s, _, _, _ := newWorkerServer(t)
	registerWorker(t, s, "w-any", `[]`)
	id := submitJob(t, s, "k-any", `{"kernel":"nautilus"}`)

	job := claim(t, s, "w-any")
	if job == nil || job.RunID != id {
		t.Fatalf("claim = %+v, want %s", job, id)
	}
	// run_spec 必须原样带回来：worker 只执行这一份，不从扁平列重建。
	if !strings.Contains(string(job.RunSpec), `"kernel":"nautilus"`) {
		t.Fatalf("run_spec = %s", job.RunSpec)
	}
	if job.LeaseUntil == nil || !job.LeaseUntil.After(baseTime) {
		t.Fatalf("lease_until = %v", job.LeaseUntil)
	}
}

func TestClaim_DrainingWorkerRejected(t *testing.T) {
	s, _, _, _ := newWorkerServer(t)
	registerWorker(t, s, "w1", `[]`)
	doWant(t, s, "POST", "/api/runs/workers/w1/drain", "", 200, "")

	submitJob(t, s, "k1", runSpecA)
	doWant(t, s, "POST", "/api/runs/workers/w1/claim", "", 409, "worker_draining")
}

// plainQueue 只暴露冻结的 JobQueue 方法：它**不**实现 KernelAwareJobQueue。
type plainQueue struct{ queue.JobQueue }

func TestWithWorkers_PanicsWithoutKernelAwareQueue(t *testing.T) {
	reg := queue.NewMemoryRegistry()
	jobs := queue.NewMemory()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("New did not panic: worker assembly without ClaimByKernel must be refused")
		}
	}()
	New(plainQueue{jobs}, WithWorkers(reg))
}

func TestRenewLease_AndFencing(t *testing.T) {
	s, jobs, _, clock := newWorkerServer(t)
	registerWorker(t, s, "w1", `[]`)
	registerWorker(t, s, "w2", `[]`)
	id := submitJob(t, s, "k1", runSpecA)
	claim(t, s, "w1")

	// 别人续租 → lease_lost（冻结面只有这一个「不是你的」哨兵）。
	doWant(t, s, "POST", "/api/runs/jobs/"+id+"/lease", `{"worker_id":"w2"}`, 409, "lease_lost")
	doWant(t, s, "POST", "/api/runs/jobs/"+id+"/lease", `{"worker_id":"w1"}`, 200, "")

	// 推过租约后旧 owner 不能再续，也不能写结果。
	*clock = baseTime.Add(2 * DefaultWorkerLease)
	doWant(t, s, "POST", "/api/runs/jobs/"+id+"/lease", `{"worker_id":"w1"}`, 409, "lease_lost")
	body := fmt.Sprintf(`{"worker_id":"w1","status":"completed","result_status":"completed","result_uri":"x"}`)
	doWant(t, s, "POST", "/api/runs/jobs/"+id+"/result", body, 409, "lease_lost")

	// 行仍是 running（租约过期不自动回 pending；由 ReapExpired 负责）。
	got, err := jobs.Get(t.Context(), id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != queue.RunRunning {
		t.Fatalf("status = %s, want running", got.Status)
	}
}

func TestReport_CompletedWritesParentSummaryFromChildren(t *testing.T) {
	s, _, _, _ := newWorkerServer(t)
	parentID := submitBatchParent(t, s, "p-2", `{"lookback":[10,20]}`)

	page := getParentBody(t, s, parentID)
	if page.Status != "running" || page.Counts.Pending != 2 || len(page.Children) != 2 {
		t.Fatalf("before workers: %+v", page)
	}

	registerWorker(t, s, "w1", `[]`)
	first := claim(t, s, "w1")
	if first == nil || first.ParentJobID != parentID {
		t.Fatalf("first claim = %+v", first)
	}
	reportCompleted(t, s, first.RunID, "w1", "completed")

	page = getParentBody(t, s, parentID)
	if page.Status != "running" || page.Counts.Completed != 1 || page.Counts.Pending != 1 {
		t.Fatalf("after 1/2: %+v", page)
	}

	second := claim(t, s, "w1")
	if second == nil || second.ParentJobID != parentID {
		t.Fatalf("second claim = %+v", second)
	}
	reportCompleted(t, s, second.RunID, "w1", "completed")

	page = getParentBody(t, s, parentID)
	if page.Status != "completed" || page.Counts.Completed != 2 || page.Counts.Settled != 2 {
		t.Fatalf("after 2/2: %+v", page)
	}
	var summary struct {
		Tasks []struct {
			RunID        string `json:"run_id"`
			ResultStatus string `json:"result_status"`
		} `json:"Tasks"`
	}
	if err := json.Unmarshal(page.Summary, &summary); err != nil {
		t.Fatalf("summary: %v\n%s", err, page.Summary)
	}
	if len(summary.Tasks) != 2 {
		t.Fatalf("summary tasks = %+v", summary.Tasks)
	}
	// 计划顺序：先 10 后 20。子 run 由提交时按计划顺序入队，领取也按
	// priority / created_at，因此 first / second 就是计划前两条。
	if summary.Tasks[0].RunID != first.RunID || summary.Tasks[1].RunID != second.RunID {
		t.Fatalf("summary order = %+v, want [%s %s]", summary.Tasks, first.RunID, second.RunID)
	}
	if summary.Tasks[0].ResultStatus != "completed" {
		t.Fatalf("task 1 = %+v", summary.Tasks[0])
	}

	// 重放同一条上报不得刷新父记录的 completed_at（幂等）。
	completedAt := page.CompletedAt
	reportCompleted(t, s, second.RunID, "w1", "completed")
	if again := getParentBody(t, s, parentID); again.CompletedAt == nil ||
		completedAt == nil || !again.CompletedAt.Equal(*completedAt) {
		t.Fatalf("completed_at drifted: %v → %v", completedAt, again.CompletedAt)
	}
}

func TestReport_InvalidIsNotFailedInParentAggregation(t *testing.T) {
	s, _, _, _ := newWorkerServer(t)
	parentID := submitBatchParent(t, s, "p-invalid", `{"lookback":[10,20]}`)
	registerWorker(t, s, "w1", `[]`)

	first := claim(t, s, "w1")
	reportCompleted(t, s, first.RunID, "w1", "invalid")
	second := claim(t, s, "w1")
	reportCompleted(t, s, second.RunID, "w1", "completed")

	page := getParentBody(t, s, parentID)
	if page.Status != "completed" {
		t.Fatalf("status = %s, want completed (invalid 不是 failed)", page.Status)
	}
	if page.Counts.Invalid != 1 || page.Counts.Failed != 0 || page.Counts.Completed != 1 {
		t.Fatalf("counts = %+v", page.Counts)
	}
}

func TestReport_FailedCarriesErrorAndFailsParent(t *testing.T) {
	s, _, _, _ := newWorkerServer(t)
	parentID := submitBatchParent(t, s, "p-fail", `{"lookback":[10]}`)
	registerWorker(t, s, "w1", `[]`)
	job := claim(t, s, "w1")

	body := `{"worker_id":"w1","status":"failed","error":"engine boom"}`
	code, resp := do(t, s, "POST", "/api/runs/jobs/"+job.RunID+"/result", body)
	if code != 200 {
		t.Fatalf("report failed: %d %s", code, resp)
	}
	var view jobView
	code, jobBody := do(t, s, "GET", "/api/runs/jobs/"+job.RunID, "")
	if code != 200 {
		t.Fatalf("get job: %d %s", code, jobBody)
	}
	if err := json.Unmarshal([]byte(jobBody), &view); err != nil {
		t.Fatalf("job view: %v", err)
	}
	if view.Status != "failed" || view.Error != "engine boom" {
		t.Fatalf("view = %+v", view)
	}
	if page := getParentBody(t, s, parentID); page.Status != "failed" || page.Counts.Failed != 1 {
		t.Fatalf("parent = %+v", page)
	}
}

func TestReport_ValidatesPayload(t *testing.T) {
	s, _, _, _ := newWorkerServer(t)
	registerWorker(t, s, "w1", `[]`)
	id := submitJob(t, s, "k1", runSpecA)
	claim(t, s, "w1")

	doWant(t, s, "POST", "/api/runs/jobs/"+id+"/result",
		`{"worker_id":"w1","status":"completed","result_status":"completed"}`, 400, "invalid_params")
	doWant(t, s, "POST", "/api/runs/jobs/"+id+"/result",
		`{"worker_id":"w1","status":"failed"}`, 400, "invalid_params")
	doWant(t, s, "POST", "/api/runs/jobs/"+id+"/result",
		`{"worker_id":"w1","status":"cancelled"}`, 400, "invalid_params")
}

func TestCancelAck_SettlesParentChild(t *testing.T) {
	s, _, _, _ := newWorkerServer(t)
	parentID := submitBatchParent(t, s, "p-cancel", `{"lookback":[10]}`)
	registerWorker(t, s, "w1", `[]`)
	job := claim(t, s, "w1")

	// running → cancelling
	doWant(t, s, "DELETE", "/api/runs/jobs/"+job.RunID, "", 202, "")
	// owner 确认 → cancelled；父汇总把 cancelled 计成 failed，不停在 running。
	body := `{"worker_id":"w1"}`
	code, resp := do(t, s, "POST", "/api/runs/jobs/"+job.RunID+"/cancel-ack", body)
	if code != 200 {
		t.Fatalf("cancel-ack: %d %s", code, resp)
	}
	page := getParentBody(t, s, parentID)
	if page.Status != "failed" || page.Counts.Failed != 1 || page.Counts.Pending != 0 {
		t.Fatalf("parent = %+v", page)
	}
}

func TestWalkForwardSummary_MapsLegsFromPlanCanonical(t *testing.T) {
	s, _, _, _ := newWorkerServer(t)
	parentID := submitWalkForwardParent(t, s, "p-wf")
	registerWorker(t, s, "w1", `[]`)

	got := 0
	for {
		job := claim(t, s, "w1")
		if job == nil {
			break
		}
		reportCompleted(t, s, job.RunID, "w1", "completed")
		got++
	}
	if got != 6 {
		t.Fatalf("executed %d legs, want 6", got)
	}

	page := getParentBody(t, s, parentID)
	if page.Status != "completed" || page.Counts.Completed != 3 || page.Counts.Settled != 3 {
		t.Fatalf("parent = %+v", page)
	}
	var summary struct {
		Windows []struct {
			Window     int    `json:"window"`
			TrainRunID string `json:"train_run_id"`
			TestRunID  string `json:"test_run_id"`
			Status     string `json:"status"`
		} `json:"Windows"`
	}
	if err := json.Unmarshal(page.Summary, &summary); err != nil {
		t.Fatalf("summary: %v\n%s", err, page.Summary)
	}
	if len(summary.Windows) != 3 {
		t.Fatalf("windows = %+v", summary.Windows)
	}
	for i, w := range summary.Windows {
		if w.Window != i+1 || w.Status != "completed" || w.TrainRunID == "" || w.TestRunID == "" {
			t.Fatalf("window %d = %+v", i+1, w)
		}
	}
}

func TestWorkerRoutes_NotStolenByProvenanceCatchAll(t *testing.T) {
	clock := baseTime
	now := func() time.Time { return clock }
	reg := queue.NewMemoryRegistry(queue.RegistryOptions{Now: now})
	jobs := queue.NewMemory(queue.MemoryOptions{Now: now, Workers: reg})
	s := New(jobs,
		WithParents(queue.NewMemoryParents(queue.MemoryOptions{Now: now})),
		WithArtifacts(queue.NewMemoryArtifacts(jobs, queue.MemoryOptions{Now: now})),
		WithWorkers(reg),
	)
	doWant(t, s, "GET", "/api/runs/workers", "", 200, "")
}

func submitBatchParent(t *testing.T, s *Server, key, gridJSON string) string {
	t.Helper()
	body := fmt.Sprintf(`{"idempotency_key":%q,"kind":"batch","batch":{"base_run_spec":%s,"grid":%s}}`,
		key, parentBaseSpec, gridJSON)
	code, resp := do(t, s, "POST", "/api/runs/parents", body)
	if code != 202 {
		t.Fatalf("submit parent %s: %d %s", key, code, resp)
	}
	return decodeParentSubmit(t, resp).ParentJobID
}

func submitWalkForwardParent(t *testing.T, s *Server, key string) string {
	t.Helper()
	start := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	days := make([]string, 10)
	for i := range days {
		days[i] = start.AddDate(0, 0, i).Format("2006-01-02")
	}
	raw, err := json.Marshal(days)
	if err != nil {
		t.Fatalf("marshal days: %v", err)
	}
	body := fmt.Sprintf(`{"idempotency_key":%q,"kind":"walkforward","walkforward":{
		"base_run_spec":%s,
		"windows":{"train_days":3,"test_days":2,"step_days":2},
		"trading_days":%s}}`, key, parentBaseSpec, raw)
	code, resp := do(t, s, "POST", "/api/runs/parents", body)
	if code != 202 {
		t.Fatalf("submit walkforward %s: %d %s", key, code, resp)
	}
	return decodeParentSubmit(t, resp).ParentJobID
}

func getParentBody(t *testing.T, s *Server, id string) parentView {
	t.Helper()
	code, resp := do(t, s, "GET", "/api/runs/parents/"+id, "")
	if code != 200 {
		t.Fatalf("get parent %s: %d %s", id, code, resp)
	}
	var out parentView
	if err := json.Unmarshal([]byte(resp), &out); err != nil {
		t.Fatalf("parent body: %v\n%s", err, resp)
	}
	return out
}
