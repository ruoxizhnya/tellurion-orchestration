// Package queue 是编排服务的 job 队列核心：RunRecord 状态机 + JobQueue port
// + 内存实现 + `run` schema DDL。
//
// 边界（docs/ARCHITECTURE.md §9）：队列只负责「把活派下去」——不解释结果、
// 不重算指标、不持有结果内容（只存 result_uri 指针）。两轴状态不合并：
// Status 是队列生命周期六态，ResultStatus 是 K9 三值。
package queue

import (
	"encoding/json"
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
