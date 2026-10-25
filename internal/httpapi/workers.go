package httpapi

// X5c worker 打活面：注册 / 心跳 / 领取 / 续租 / 上报结论 / 取消确认 + 运维面
// （列表 / drain）。
//
// 形状对表 docs/api-schema.md §1「算力」。本文件只做传输与装配期能力断言：
// 领取、租约、状态机全部在 internal/queue；父汇总重算在 internal/parents。
//
// worker 进程不在本仓（零 import core）：它跑 core 的引擎，经这组 HTTP 端点
// 领活与回报。端点只认 worker_id 这一个身份（鉴权按裁-15 缓做）。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"tellurion-orchestration/internal/queue"
)

// DefaultWorkerLease 是领取时写入的租约时长。worker 用心跳续租；租约过期后
// 该行可被其他 worker 重新领取（attempts+1）。
const DefaultWorkerLease = 60 * time.Second

var (
	// errKernelFilterUnavailable 是「worker 声明了 kernels，但队列实现不支持
	// 按内核过滤」。这里必须 fail-closed：静默退回不过滤会把 nautilus 的活派给
	// go worker，产出一份看起来正常、内核却不对的结果。
	errKernelFilterUnavailable = errors.New("httpapi: queue does not implement KernelAwareJobQueue")
	// errParentSummary 是父汇总写回失败。子 run 的状态已经写成功，调用方
	// （worker）按幂等重放即可；把它报成 500 而不是吞掉。
	errParentSummary = errors.New("httpapi: parent summary writeback failed")
)

// WorkerOptions 是 worker 打活面的可选参数。
type WorkerOptions struct {
	// Lease 是领取 / 续租使用的租约；零值取 DefaultWorkerLease。
	Lease time.Duration
}

// WithWorkers 装配 X5c 的 worker 端点。未提供时这些路由不注册（与
// WithParents / WithArtifacts 同一口径：没有注册表就答不出，注册了只会
// 给假状态）。
//
// 装配期能力断言在 New 里做（fail-closed）：队列必须实现
// `KernelAwareJobQueue`；同时装了父作业时还必须实现 `ParentChildLister`
// ——否则「按能力派活」和「父汇总真实写回」都会在运行到第一次时才暴露。
func WithWorkers(reg queue.WorkerRegistry, opts ...WorkerOptions) Option {
	return func(s *Server) {
		if reg == nil {
			panic("httpapi: WithWorkers requires a non-nil WorkerRegistry")
		}
		lease := DefaultWorkerLease
		if len(opts) > 0 && opts[0].Lease != 0 {
			lease = opts[0].Lease
		}
		s.workers = reg
		s.workerLease = lease
	}
}

func (s *Server) registerWorkerRoutes() {
	s.mux.HandleFunc("GET /api/runs/workers", s.listWorkers)
	s.mux.HandleFunc("POST /api/runs/workers", s.registerWorker)
	s.mux.HandleFunc("POST /api/runs/workers/{id}/heartbeat", s.heartbeatWorker)
	s.mux.HandleFunc("POST /api/runs/workers/{id}/claim", s.claimJob)
	s.mux.HandleFunc("POST /api/runs/workers/{id}/drain", s.drainWorker)
	s.mux.HandleFunc("POST /api/runs/jobs/{id}/lease", s.renewLease)
	s.mux.HandleFunc("POST /api/runs/jobs/{id}/result", s.reportResult)
	s.mux.HandleFunc("POST /api/runs/jobs/{id}/cancel-ack", s.ackCancel)
}

type workerRegisterRequest struct {
	WorkerID string          `json:"worker_id"`
	Kernels  []string        `json:"kernels"`
	Meta     json.RawMessage `json:"meta"`
}

type workerView struct {
	WorkerID         string     `json:"worker_id"`
	Kernels          []string   `json:"kernels"`
	Status           string     `json:"status"`
	LastHeartbeatAt  *time.Time `json:"last_heartbeat_at"`
	DrainRequestedAt *time.Time `json:"drain_requested_at"`
	StartedAt        *time.Time `json:"started_at"`
}

type workerStatusBody struct {
	WorkerID string   `json:"worker_id"`
	Status   string   `json:"status"`
	Kernels  []string `json:"kernels,omitempty"`
}

type workerListBody struct {
	Workers []workerView `json:"workers"`
}

// claimView 是领取的响应体：worker 需要 run_spec 才能执行，所以不复用
// GET /api/runs/jobs/{id}（那里不返回请求体）。
type claimView struct {
	RunID           string          `json:"run_id"`
	RunSpec         json.RawMessage `json:"run_spec"`
	Attempts        int             `json:"attempts"`
	MaxAttempts     int             `json:"max_attempts"`
	LeaseUntil      *time.Time      `json:"lease_until"`
	CancelRequested bool            `json:"cancel_requested"`
	ParentJobID     string          `json:"parent_job_id,omitempty"`
}

type claimBody struct {
	Job *claimView `json:"job"`
}

type workerOnlyRequest struct {
	WorkerID string `json:"worker_id"`
}

type leaseBody struct {
	RunID      string     `json:"run_id"`
	LeaseUntil *time.Time `json:"lease_until"`
}

type reportRequest struct {
	WorkerID     string `json:"worker_id"`
	Status       string `json:"status"`
	ResultStatus string `json:"result_status"`
	ResultURI    string `json:"result_uri"`
	Error        string `json:"error"`
}

// registerWorker 见 docs/api-schema.md §1「算力」：重复注册幂等。
// `kernels: []` 归一成 nil（落库 NULL）——空数组在 PG 的 `= ANY('{}')` 里
// 是「什么都不能干」，语义与「未声明能力」（不过滤）相反。
func (s *Server) registerWorker(w http.ResponseWriter, r *http.Request) {
	var req workerRegisterRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeInvalidParams(w, err)
		return
	}
	if strings.TrimSpace(req.WorkerID) == "" {
		writeInvalidParams(w, errors.New("worker_id is required"))
		return
	}
	kernels, err := normalizeKernels(req.Kernels)
	if err != nil {
		writeInvalidParams(w, err)
		return
	}
	rec := queue.WorkerRecord{WorkerID: req.WorkerID, Kernels: kernels, Meta: req.Meta}
	if err := s.workers.Register(r.Context(), rec); err != nil {
		writeError(w, err)
		return
	}
	got, err := s.findWorker(r.Context(), req.WorkerID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, workerStatusBody{
		WorkerID: got.WorkerID,
		Status:   string(got.Status),
		Kernels:  kernelsOrEmpty(got.Kernels),
	})
}

// normalizeKernels 去空白 / 去重，空集合归一成 nil。空名字当场拒绝：
// 「声明了一个谁也对不上的内核」会让这张 worker 永远领不到活，属于请求错误。
func normalizeKernels(in []string) ([]string, error) {
	if len(in) == 0 {
		return nil, nil
	}
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		name := strings.TrimSpace(raw)
		if name == "" {
			return nil, errors.New("kernels must not contain empty names")
		}
		if seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func kernelsOrEmpty(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}

func (s *Server) listWorkers(w http.ResponseWriter, r *http.Request) {
	records, err := s.workers.List(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	views := make([]workerView, 0, len(records))
	for _, rec := range records {
		views = append(views, workerViewOf(rec))
	}
	writeJSON(w, http.StatusOK, workerListBody{Workers: views})
}

func workerViewOf(rec queue.WorkerRecord) workerView {
	return workerView{
		WorkerID:         rec.WorkerID,
		Kernels:          kernelsOrEmpty(rec.Kernels),
		Status:           string(rec.Status),
		LastHeartbeatAt:  timePtr(rec.LastHeartbeatAt),
		DrainRequestedAt: timePtr(rec.DrainRequestedAt),
		StartedAt:        timePtr(rec.StartedAt),
	}
}

// heartbeatWorker 是存活证明。时间取服务端时钟（不信任 worker 的时钟）；
// 未知 worker → 404（**不自动注册**，fail-loud）。
func (s *Server) heartbeatWorker(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.workers.Heartbeat(r.Context(), id, time.Now().UTC()); err != nil {
		writeError(w, err)
		return
	}
	got, err := s.findWorker(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, workerStatusBody{
		WorkerID: got.WorkerID,
		Status:   string(got.Status),
		Kernels:  kernelsOrEmpty(got.Kernels),
	})
}

// claimJob 按 worker 在注册表里**声明**的内核能力领取一条 pending 行。
//
// 声明了 kernels ⇒ 必须走 ClaimByKernel；队列不支持就 503 而不是静默不过滤。
// 声明为空 ⇒ Claim（不过滤，与 KernelAwareJobQueue 的口径一致）。
// 无活 → 200 `{"job": null}`（不是 404：没活是正常状态）。
func (s *Server) claimJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	worker, err := s.findWorker(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}

	var job *queue.RunRecord
	if len(worker.Kernels) > 0 {
		kq, ok := s.q.(queue.KernelAwareJobQueue)
		if !ok {
			writeError(w, errKernelFilterUnavailable)
			return
		}
		job, err = kq.ClaimByKernel(r.Context(), worker.WorkerID, s.workerLease)
	} else {
		job, err = s.q.Claim(r.Context(), worker.WorkerID, s.workerLease)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	if job == nil {
		writeJSON(w, http.StatusOK, claimBody{})
		return
	}
	writeJSON(w, http.StatusOK, claimBody{Job: claimViewOf(job)})
}

func claimViewOf(job *queue.RunRecord) *claimView {
	return &claimView{
		RunID:           job.ID,
		RunSpec:         job.RequestJSON,
		Attempts:        job.Attempts,
		MaxAttempts:     job.MaxAttempts,
		LeaseUntil:      timePtr(job.LeaseUntil),
		CancelRequested: job.CancelRequested,
		ParentJobID:     job.ParentJobID,
	}
}

// renewLease 续租。只有当前 owner 能续；租约过期 / 别人持有 / 行已终态 →
// 409 lease_lost（worker 必须立即停止，不得再写结果）。
func (s *Server) renewLease(w http.ResponseWriter, r *http.Request) {
	var req workerOnlyRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeInvalidParams(w, err)
		return
	}
	runID := r.PathValue("id")
	if err := s.q.RenewLease(r.Context(), runID, req.WorkerID, s.workerLease); err != nil {
		writeError(w, err)
		return
	}
	job, err := s.q.Get(r.Context(), runID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, leaseBody{RunID: job.ID, LeaseUntil: timePtr(job.LeaseUntil)})
}

// reportResult 是 worker 上报结论的唯一入口。
//
//   - `status=completed` → Queue.Complete（result_status 只接受 completed /
//     invalid，result_uri 必须非空）；
//   - `status=failed` → Queue.Fail（error 必填）。
//
// 写完状态后**立刻重算父汇总**（子 run 的结论是父汇总的输入）。汇总失败时
// 返回 500 parent_summary_failed：worker 重放这次上报即可（Complete / Fail
// 对同终态同载荷是幂等的）。
func (s *Server) reportResult(w http.ResponseWriter, r *http.Request) {
	var req reportRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeInvalidParams(w, err)
		return
	}
	if strings.TrimSpace(req.WorkerID) == "" {
		writeInvalidParams(w, errors.New("worker_id is required"))
		return
	}
	runID := r.PathValue("id")

	switch req.Status {
	case string(queue.RunCompleted):
		if err := s.q.Complete(r.Context(), runID, req.WorkerID, req.ResultStatus, req.ResultURI); err != nil {
			writeError(w, err)
			return
		}
	case string(queue.RunFailed):
		if strings.TrimSpace(req.Error) == "" {
			writeInvalidParams(w, errors.New("error is required when status=failed"))
			return
		}
		if err := s.q.Fail(r.Context(), runID, req.WorkerID, req.Error, req.ResultStatus); err != nil {
			writeError(w, err)
			return
		}
	default:
		writeInvalidParams(w, fmt.Errorf("status must be %q or %q, got %q",
			queue.RunCompleted, queue.RunFailed, req.Status))
		return
	}

	job, err := s.q.Get(r.Context(), runID)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := s.syncParent(r.Context(), job); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, statusBody{
		JobID:        job.ID,
		Status:       string(job.Status),
		ResultStatus: resultStatusPtr(job.ResultStatus),
	})
}

// ackCancel 由 owner 确认取消。写完状态后同样重算父汇总（取消是子 run 的
// 终态，父记录不能等一个不会再来的回报）。
func (s *Server) ackCancel(w http.ResponseWriter, r *http.Request) {
	var req workerOnlyRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeInvalidParams(w, err)
		return
	}
	runID := r.PathValue("id")
	if err := s.q.AckCancel(r.Context(), runID, req.WorkerID); err != nil {
		writeError(w, err)
		return
	}
	job, err := s.q.Get(r.Context(), runID)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := s.syncParent(r.Context(), job); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, statusBody{
		JobID:        job.ID,
		Status:       string(job.Status),
		ResultStatus: resultStatusPtr(job.ResultStatus),
	})
}

func (s *Server) drainWorker(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.workers.Drain(r.Context(), id); err != nil {
		writeError(w, err)
		return
	}
	got, err := s.findWorker(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, workerStatusBody{
		WorkerID: got.WorkerID,
		Status:   string(got.Status),
		Kernels:  kernelsOrEmpty(got.Kernels),
	})
}

func (s *Server) findWorker(ctx context.Context, id string) (queue.WorkerRecord, error) {
	if strings.TrimSpace(id) == "" {
		return queue.WorkerRecord{}, fmt.Errorf("%w: worker_id is required", queue.ErrInvalidInput)
	}
	records, err := s.workers.List(ctx)
	if err != nil {
		return queue.WorkerRecord{}, err
	}
	for _, rec := range records {
		if rec.WorkerID == id {
			return rec, nil
		}
	}
	return queue.WorkerRecord{}, fmt.Errorf("%w: %s", queue.ErrWorkerNotFound, id)
}

// syncParent 在子 run 状态变化后重算父汇总。没有父 / 未装配父服务时是 no-op。
func (s *Server) syncParent(ctx context.Context, job *queue.RunRecord) error {
	if s.psvc == nil || job.ParentJobID == "" {
		return nil
	}
	if _, err := s.psvc.SyncSummary(ctx, job.ParentJobID); err != nil {
		return fmt.Errorf("%w: parent %s: %v", errParentSummary, job.ParentJobID, err)
	}
	return nil
}
