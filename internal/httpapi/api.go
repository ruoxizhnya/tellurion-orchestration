// Package httpapi 是 `run` 队列的 HTTP 面（C 组 jobs 4 端点 + X5b 父作业
// 2 端点 + X0 provenance 端点，docs/ARCHITECTURE.md §9.4）。worker 端点属 X5c，
// 不在本切片。
//
// 领域语义全部在 internal/queue + internal/plan（哨兵 → wire code 的映射表 =
// docs/api-schema.md §2）；本包只做传输：严格解码、状态码选择、零值字段转 null。
package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"tellurion-orchestration/internal/parents"
	"tellurion-orchestration/internal/plan"
	"tellurion-orchestration/internal/queue"
)

// MaxBodyBytes 是单个请求体上限。提交的 run_spec 是完整 RunSpec JSON，管理面小 JSON。
const MaxBodyBytes = 1 << 20

// Server 是 jobs 4 端点 + 父作业 2 端点 + provenance 端点的 HTTP 面。
//
// artifacts 为 nil 时不注册 provenance 路由：没有留痕表就没有三 hash 可答，
// 注册了只会给一个假的 404「run 从未产出」，把「未装配」伪装成「没产出」。
type Server struct {
	q         queue.JobQueue
	psvc      *parents.Service
	artifacts queue.ArtifactStore
	mux       *http.ServeMux
}

// Option 是装配可选项（父作业端点需要 ParentStore，单跑 jobs 时不必提供）。
type Option func(*Server)

// WithParents 装配 `POST/GET /api/runs/parents`。未提供时这两条路由不注册：
// 没有父表就没法回答，注册了只会给一个假的 404「父记录不存在」。
func WithParents(store queue.ParentStore) Option {
	return func(s *Server) {
		if store != nil {
			s.psvc = parents.New(store)
		}
	}
}

// WithArtifacts 装配 `GET /api/runs/{id}/provenance`（X0）。
// 未提供时该路由不注册（见 Server 的注释）。
func WithArtifacts(store queue.ArtifactStore) Option {
	return func(s *Server) {
		if store != nil {
			s.artifacts = store
		}
	}
}

// New 构造 Server 并注册路由。
func New(q queue.JobQueue, opts ...Option) *Server {
	s := &Server{q: q, mux: http.NewServeMux()}
	for _, opt := range opts {
		opt(s)
	}
	s.mux.HandleFunc("POST /api/runs/jobs", s.submit)
	s.mux.HandleFunc("GET /api/runs/jobs", s.list)
	s.mux.HandleFunc("GET /api/runs/jobs/{id}", s.get)
	s.mux.HandleFunc("DELETE /api/runs/jobs/{id}", s.cancel)
	if s.psvc != nil {
		s.mux.HandleFunc("POST /api/runs/parents", s.submitParent)
		s.mux.HandleFunc("GET /api/runs/parents/{id}", s.getParent)
	}
	if s.artifacts != nil {
		// catch-all 而不是 `GET /api/runs/{id}/provenance`：后者与
		// `GET /api/runs/parents/{id}` 在 `/api/runs/parents/provenance` 上冲突
		// （互不更具体 ⇒ 注册期 panic）。jobs / parents 都比它具体，仍优先。
		s.mux.HandleFunc("GET /api/runs/{rest...}", s.provenance)
	}
	return s
}

// Handler 返回 HTTP handler。
func (s *Server) Handler() http.Handler { return s.mux }

type submitRequest struct {
	IdempotencyKey string          `json:"idempotency_key"`
	Priority       int             `json:"priority"`
	RunSpec        json.RawMessage `json:"run_spec"`
}

// statusBody 是提交 / 取消的响应体（§9.4：202 `{job_id, status, result_status}`，
// 幂等重放加 `idempotent_replay:true`；取消 202 `cancelling` / 200 终态）。
type statusBody struct {
	JobID            string  `json:"job_id"`
	Status           string  `json:"status"`
	ResultStatus     *string `json:"result_status"`
	IdempotentReplay bool    `json:"idempotent_replay,omitempty"`
}

// jobView 是 GET 的响应体（§9.4 冻结字段集；时间为零值时出 null）。
type jobView struct {
	JobID        string     `json:"job_id"`
	Status       string     `json:"status"`
	ResultStatus *string    `json:"result_status"`
	Error        string     `json:"error,omitempty"`
	ResultURI    string     `json:"result_uri,omitempty"`
	RunSpecHash  string     `json:"run_spec_hash"`
	CreatedAt    *time.Time `json:"created_at"`
	StartedAt    *time.Time `json:"started_at"`
	CompletedAt  *time.Time `json:"completed_at"`
}

type listBody struct {
	Jobs  []jobView `json:"jobs"`
	Total int       `json:"total"`
}

// submit 见 §9.4：202 `{job_id, status:"pending", result_status:null}`；幂等命中 →
// 200 + `idempotent_replay:true`。缺键 → 400 `missing_idempotency_key`（先于
// 领域校验：这是本层的 fail-closed ②，服务端不代生成键）。
func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeInvalidParams(w, err)
		return
	}
	if req.IdempotencyKey == "" {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "idempotency_key is required", Code: "missing_idempotency_key"})
		return
	}
	if len(req.RunSpec) == 0 {
		writeInvalidParams(w, errors.New("run_spec is required"))
		return
	}

	rec := queue.RunRecord{
		IdempotencyKey: req.IdempotencyKey,
		Priority:       req.Priority,
		RequestJSON:    req.RunSpec,
	}
	job, created, err := s.q.Enqueue(r.Context(), &rec)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, submitCode(created), statusBody{
		JobID:            job.ID,
		Status:           string(job.Status),
		ResultStatus:     resultStatusPtr(job.ResultStatus),
		IdempotentReplay: !created,
	})
}

// submitCode：新行 202（异步语义：提交即返回，不等待执行）；重放 200。
func submitCode(created bool) int {
	if created {
		return http.StatusAccepted
	}
	return http.StatusOK
}

// get 见 §9.4。
func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	job, err := s.q.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, jobViewOf(job))
}

func jobViewOf(job *queue.RunRecord) jobView {
	return jobView{
		JobID:        job.ID,
		Status:       string(job.Status),
		ResultStatus: resultStatusPtr(job.ResultStatus),
		Error:        job.ErrorMessage,
		ResultURI:    job.ResultURI,
		RunSpecHash:  job.RequestHash,
		CreatedAt:    timePtr(job.CreatedAt),
		StartedAt:    timePtr(job.StartedAt),
		CompletedAt:  timePtr(job.CompletedAt),
	}
}

// cancel 见 §9.4：`running` → 202 `cancelling`（worker 走 AckCancel 确认）；
// `pending` → 200 `cancelled`；终态 → 200 原状态（幂等，重复取消不报错）。
// RequestCancel 返回 nil 时不区分落在哪个分支，重读一次拿真实状态定状态码。
func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.q.RequestCancel(r.Context(), id); err != nil {
		writeError(w, err)
		return
	}
	job, err := s.q.Get(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	code := http.StatusOK
	if job.Status == queue.RunCancelling {
		code = http.StatusAccepted
	}
	writeJSON(w, code, statusBody{JobID: job.ID, Status: string(job.Status), ResultStatus: resultStatusPtr(job.ResultStatus)})
}

// list 见 §9.4：`{jobs:[…], total}`。未知枚举值由 queue 拒成 invalid_params（fail-closed）。
//
// total 语义：**本次返回的行数**（List 按 limit 钳制 ≤100）。全量命中数需要
// Count 能力，冻结方法集没有它——PG 实现落地时一并裁决（TASKS 施工②）。
func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	filter, err := listFilterOf(r)
	if err != nil {
		writeInvalidParams(w, err)
		return
	}
	jobs, err := s.q.List(r.Context(), filter)
	if err != nil {
		writeError(w, err)
		return
	}
	views := make([]jobView, 0, len(jobs))
	for _, j := range jobs {
		views = append(views, jobViewOf(j))
	}
	writeJSON(w, http.StatusOK, listBody{Jobs: views, Total: len(views)})
}

func listFilterOf(r *http.Request) (queue.ListFilter, error) {
	var f queue.ListFilter
	f.Status = queue.RunStatus(r.URL.Query().Get("status"))
	f.ResultStatus = queue.ResultStatus(r.URL.Query().Get("result_status"))
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return f, errors.New("limit must be a positive integer")
		}
		f.Limit = n
	}
	return f, nil
}

func resultStatusPtr(rs queue.ResultStatus) *string {
	if rs == "" {
		return nil
	}
	s := string(rs)
	return &s
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

type errorBody struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// decodeBody 严格解码：拒未知字段、拒尾随内容、限制体积。
func decodeBody(w http.ResponseWriter, r *http.Request, dst any) error {
	if r.Body == nil {
		return errors.New("request body is required")
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		if errors.Is(err, io.EOF) {
			return errors.New("request body is required")
		}
		return err
	}
	if dec.More() {
		return errors.New("request body must contain a single JSON object")
	}
	return nil
}

// classify 把领域哨兵映射成 wire code（词表唯一真相 = docs/api-schema.md §2）。
func classify(err error) (int, string) {
	switch {
	case errors.Is(err, queue.ErrJobNotFound):
		return http.StatusNotFound, "job_not_found"
	case errors.Is(err, queue.ErrParentNotFound):
		return http.StatusNotFound, "parent_not_found"
	case errors.Is(err, plan.ErrInvalidRequest):
		return http.StatusBadRequest, "invalid_params"
	case errors.Is(err, queue.ErrIdempotencyConflict):
		return http.StatusConflict, "idempotency_conflict"
	case errors.Is(err, queue.ErrQueueFull):
		return http.StatusTooManyRequests, "queue_full"
	case errors.Is(err, queue.ErrInvalidTransition):
		return http.StatusConflict, "invalid_transition"
	case errors.Is(err, queue.ErrInvalidInput):
		return http.StatusBadRequest, "invalid_params"
	default:
		return http.StatusInternalServerError, "internal"
	}
}

func writeInvalidParams(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusBadRequest, errorBody{Error: err.Error(), Code: "invalid_params"})
}

func writeError(w http.ResponseWriter, err error) {
	status, code := classify(err)
	// 4xx 的原文透传给调用方（便于修正请求），5xx 才隐藏细节。
	msg := err.Error()
	if status == http.StatusInternalServerError {
		log.Printf("httpapi: internal error: %v", err)
		msg = "internal error"
	}
	writeJSON(w, status, errorBody{Error: msg, Code: code})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("httpapi: encode response: %v", err)
	}
}
