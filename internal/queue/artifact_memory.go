package queue

// `run.run_artifacts` 的进程内实现（X0，B6-4）。
//
// 它必须与 PG 实现行为一致的那几条：未知 run → ErrJobNotFound、同身份重写幂等、
// 身份漂移 → ErrArtifactConflict、未知 run 读 → ErrArtifactNotFound。

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryArtifacts 是 ArtifactStore 的进程内实现（测试 / 单机装配）。
//
// runs 是同一个 Memory 队列：写留痕前先确认这行存在，与 PG 侧的外键同口径
// （否则「同一个调用换实现就换了行为」）。runs 为 nil 时不校验——只给不关心
// 存在性的单测用，装配路径必须传。
type MemoryArtifacts struct {
	mu   sync.Mutex
	rows map[string]*ArtifactRecord
	now  func() time.Time
	runs *Memory
}

// 编译期断言：方法集少一个就是实现方全红。
var _ ArtifactStore = (*MemoryArtifacts)(nil)

// NewMemoryArtifacts 构造内存留痕仓。opts 复用 MemoryOptions 的 Now。
func NewMemoryArtifacts(runs *Memory, opts ...MemoryOptions) *MemoryArtifacts {
	var o MemoryOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	m := &MemoryArtifacts{
		rows: make(map[string]*ArtifactRecord),
		now:  o.Now,
		runs: runs,
	}
	if m.now == nil {
		m.now = time.Now
	}
	return m
}

// SaveArtifact 见 ArtifactStore 的契约注释。
func (m *MemoryArtifacts) SaveArtifact(ctx context.Context, rec *ArtifactRecord) (*ArtifactRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, fmt.Errorf("%w: nil artifact record", ErrInvalidInput)
	}
	in := rec.Clone()
	if err := PrepareArtifact(&in); err != nil {
		return nil, err
	}
	if m.runs != nil {
		if _, err := m.runs.Get(ctx, in.RunID); err != nil {
			return nil, err
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.rows[in.RunID]; ok {
		if !sameArtifactIdentity(existing, &in) {
			return nil, fmt.Errorf("%w: %s already has different artifacts", ErrArtifactConflict, in.RunID)
		}
		out := existing.Clone()
		return &out, nil
	}
	if in.CreatedAt.IsZero() {
		in.CreatedAt = m.now().UTC()
	} else {
		in.CreatedAt = in.CreatedAt.UTC()
	}
	m.rows[in.RunID] = &in
	out := in.Clone()
	return &out, nil
}

// GetArtifact 见 ArtifactStore 的契约注释。
func (m *MemoryArtifacts) GetArtifact(ctx context.Context, runID string) (*ArtifactRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.rows[runID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrArtifactNotFound, runID)
	}
	out := row.Clone()
	return &out, nil
}
