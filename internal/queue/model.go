// Package queue 是编排服务的 job 队列核心：RunRecord 状态机 + JobQueue port
// + 内存实现 + `run` schema DDL。
//
// 边界（docs/ARCHITECTURE.md §9）：队列只负责「把活派下去」——不解释结果、
// 不重算指标、不持有结果内容（只存 result_uri 指针）。两轴状态不合并：
// Status 是队列生命周期六态，ResultStatus 是 K9 三值。
package queue

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// RunStatus 是队列生命周期（**不是 K9 三值**）。
type RunStatus string

const (
	RunPending    RunStatus = "pending"
	RunRunning    RunStatus = "running"
	RunCancelling RunStatus = "cancelling"
	RunCompleted  RunStatus = "completed"
	RunFailed     RunStatus = "failed"
	RunCancelled  RunStatus = "cancelled"
)

// ResultStatus 是 K9 三值（run 的结论），终态前为空。
type ResultStatus string

const (
	ResultCompleted ResultStatus = "completed"
	ResultInvalid   ResultStatus = "invalid"
	ResultFailed    ResultStatus = "failed"
)

// DefaultKernel 是 RunSpec.Kernel 为空时的生效内核名。
//
// 口径不在本仓发明：docs/dataflow.md §1 已冻结「`BacktestRequest.Kernel` 空 =
// 默认 `go`，默认值不回填」——所以**读侧**要按这个默认解释，写侧不改请求。
const DefaultKernel = "go"

// KernelOf 取一份 canonical request_json 的生效内核名。
//
// 规则一句话：**只有 JSON 字符串才算内核**；缺失 / null / 非字符串（数字、布尔、
// 对象）/ 空串 / 纯空白一律按 DefaultKernel。这条不是随手定的：PG 侧的抽取表达式
// （`internal/pgstore.claimSQL`）用 `jsonb_typeof(...) = 'string'` 实现同一个规则，
// 两侧必须逐字节同口径，否则同一行在内存里能被 w-go 领、在 PG 里领不到。
//
// 不报解析错误：能进队列的行已在 Enqueue 里过了 Canonicalize（非法 JSON 当场拒），
// 解析失败只可能来自形状奇怪的行（`{"kernel":123}`、顶层数组）——按默认内核处理。
func KernelOf(raw json.RawMessage) string {
	if len(raw) == 0 {
		return DefaultKernel
	}
	var probe struct {
		Kernel string `json:"kernel"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return DefaultKernel
	}
	// 两侧同形：SQL 用 btrim()，这里用 TrimSpace（" go " 是同一个内核名）。
	if k := strings.TrimSpace(probe.Kernel); k != "" {
		return k
	}
	return DefaultKernel
}

// MatchesKernels 报告一行的生效内核是否在 worker 声明的能力集合里。
// declared 为空（未声明能力）时**不过滤**——见 KernelAwareJobQueue 的口径。
func MatchesKernels(declared []string, raw json.RawMessage) bool {
	if len(declared) == 0 {
		return true
	}
	want := KernelOf(raw)
	for _, k := range declared {
		if strings.TrimSpace(k) == want {
			return true
		}
	}
	return false
}

// ValidRunStatus 报告 s 是否是六态之一（含空串由调用方决定是否允许）。
func ValidRunStatus(s RunStatus) bool {
	switch s {
	case RunPending, RunRunning, RunCancelling, RunCompleted, RunFailed, RunCancelled:
		return true
	}
	return false
}

// ValidResultStatus 报告 s 是否是 K9 三值之一。
func ValidResultStatus(s ResultStatus) bool {
	switch s {
	case ResultCompleted, ResultInvalid, ResultFailed:
		return true
	}
	return false
}

// RunRecord 是 `run.backtest_jobs` 的一行。
//
// 时间字段用零值表示「未发生」（不用指针，避免拷贝时的别名问题）。
type RunRecord struct {
	ID              string          `json:"id"`
	IdempotencyKey  string          `json:"idempotency_key,omitempty"`
	ParentJobID     string          `json:"parent_job_id,omitempty"`
	RequestHash     string          `json:"request_hash"`
	RequestJSON     json.RawMessage `json:"request_json"`
	Status          RunStatus       `json:"status"`
	ResultStatus    ResultStatus    `json:"result_status,omitempty"`
	Priority        int             `json:"priority"`
	Attempts        int             `json:"attempts"`
	MaxAttempts     int             `json:"max_attempts"`
	WorkerID        string          `json:"worker_id,omitempty"`
	LeaseUntil      time.Time       `json:"lease_until,omitempty"`
	CancelRequested bool            `json:"cancel_requested"`
	ErrorMessage    string          `json:"error_message,omitempty"`
	ResultURI       string          `json:"result_uri,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	StartedAt       time.Time       `json:"started_at,omitempty"`
	CompletedAt     time.Time       `json:"completed_at,omitempty"`
}

// Terminal 报告该行是否已到终态（completed / failed / cancelled）。
func (r RunRecord) Terminal() bool {
	return r.Status == RunCompleted || r.Status == RunFailed || r.Status == RunCancelled
}

// Clone 深拷贝（RequestJSON 是切片，出库/入参都不能共享底层数组）。
func (r RunRecord) Clone() RunRecord {
	out := r
	if r.RequestJSON != nil {
		out.RequestJSON = append(json.RawMessage(nil), r.RequestJSON...)
	}
	return out
}

// ListFilter 是 List 的过滤条件。零值 = 不过滤；Limit 由实现钳制 ≤100。
type ListFilter struct {
	Status       RunStatus
	ResultStatus ResultStatus
	Limit        int
	Before       time.Time
}

// WorkerStatus 是 worker 生命周期（active → draining → offline）。
type WorkerStatus string

const (
	WorkerActive   WorkerStatus = "active"
	WorkerDraining WorkerStatus = "draining"
	WorkerOffline  WorkerStatus = "offline"
)

// WorkerRecord 是 `run.worker_registry` 的一行。
type WorkerRecord struct {
	WorkerID         string          `json:"worker_id"`
	Kernels          []string        `json:"kernels"`
	Status           WorkerStatus    `json:"status"`
	LastHeartbeatAt  time.Time       `json:"last_heartbeat_at,omitempty"`
	DrainRequestedAt time.Time       `json:"drain_requested_at,omitempty"`
	StartedAt        time.Time       `json:"started_at,omitempty"`
	Meta             json.RawMessage `json:"meta,omitempty"`
}

// Clone 深拷贝。
func (w WorkerRecord) Clone() WorkerRecord {
	out := w
	out.Kernels = append([]string(nil), w.Kernels...)
	if w.Meta != nil {
		out.Meta = append(json.RawMessage(nil), w.Meta...)
	}
	return out
}

// ParentKind 是父计划的两种形态（裁-13：一张 parent_jobs 表，不建两张实体表）。
type ParentKind string

const (
	ParentBatch       ParentKind = "batch"
	ParentWalkForward ParentKind = "walkforward"
)

// ValidParentKind 报告 k 是否是已知父种类。
func ValidParentKind(k ParentKind) bool {
	return k == ParentBatch || k == ParentWalkForward
}

// ParentRecord 是 `run.parent_jobs` 的一行。
//
// 身份是 plan_canonical（TEXT 字节锚点）+ plan_hash，不是 JSONB：与
// `backtest_jobs.request_canonical` 同一条实测教训（JSONB 会规范化数字文本，
// 拿它复算 hash 会把同一次提交判成两次）。三值计数只放汇总，不放子结果内容。
type ParentRecord struct {
	ID             string          `json:"parent_job_id"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Kind           ParentKind      `json:"kind"`
	PlanCanonical  string          `json:"plan_canonical"`
	PlanHash       string          `json:"plan_hash"`
	Status         RunStatus       `json:"status"`
	Total          int             `json:"total"`
	Completed      int             `json:"completed"`
	Invalid        int             `json:"invalid"`
	Failed         int             `json:"failed"`
	SummaryJSON    json.RawMessage `json:"summary_json,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	CompletedAt    time.Time       `json:"completed_at,omitempty"`
}

// Terminal 报告父记录是否已到终态。
func (p ParentRecord) Terminal() bool {
	return p.Status == RunCompleted || p.Status == RunFailed || p.Status == RunCancelled
}

// Clone 深拷贝（PlanCanonical 是字符串不可变，SummaryJSON 是切片）。
func (p ParentRecord) Clone() ParentRecord {
	out := p
	if p.SummaryJSON != nil {
		out.SummaryJSON = append(json.RawMessage(nil), p.SummaryJSON...)
	}
	return out
}

// ParentSummary 是汇总写回的载荷（三值计数 + 父状态 + 可选明细）。
//
// 它是 plan.Counts 的**落库形**：plan 包 import queue，反向不成立，所以这里
// 用基本类型承载，映射只发生在 service 层一处。
type ParentSummary struct {
	Status      RunStatus
	Total       int
	Completed   int
	Invalid     int
	Failed      int
	SummaryJSON json.RawMessage
	CompletedAt time.Time
}

// Validate 检查计数自洽（与 plan.Counts.Validate 同口径：非负且已落定 + 未回报 = Total）。
func (s ParentSummary) Validate() error {
	if s.Total < 0 || s.Completed < 0 || s.Invalid < 0 || s.Failed < 0 {
		return fmt.Errorf("%w: parent counts must be non-negative", ErrInvalidInput)
	}
	if !ValidRunStatus(s.Status) {
		return fmt.Errorf("%w: unknown parent status %q", ErrInvalidInput, s.Status)
	}
	if settled := s.Completed + s.Invalid + s.Failed; settled > s.Total {
		return fmt.Errorf("%w: settled counts (%d) exceed total (%d)", ErrInvalidInput, settled, s.Total)
	}
	return nil
}
