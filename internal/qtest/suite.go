// Package qtest 是 JobQueue / WorkerRegistry 的**语义套件**：同一批断言既跑在
// internal/queue.Memory 上，也跑在 internal/pgstore.Queue 上。
//
// 为什么存在：docs/ARCHITECTURE.md §9.8 的判据是「内存实现先把状态机、幂等与租约
// 语义钉住，换 PG 实现时行为不变」。**只有一处断言**才能证明「不变」——分别写两份
// 测试只会证明两份测试各自成立。
//
// 它是普通 .go 文件而不是 _test.go，因为两个包的测试都要引用它。约束两条：
//   - 只依赖 testing 与 queue 的**导出面**：实现私有的口径（注入时钟、连接池）不进套件；
//   - 时间断言只比「同一实现自己返回的值」：内存是纳秒、PG timestamptz 是微秒，
//     跨实现比纳秒会假红。
package qtest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"tellurion-orchestration/internal/queue"
)

// Spec 是套件需要的最小装配项。刻意不引用任何一侧实现的 Options 类型：
// 套件描述的是 port 的行为，不是某个构造函数的参数。
type Spec struct {
	// MaxQueueDepth 是 pending 深度上限（<=0 = 实现默认）。
	MaxQueueDepth int
	// DefaultMaxAttempts 是行未指定 max_attempts 时的取值（<=0 = 实现默认）。
	DefaultMaxAttempts int
	// Workers 是 Claim 用来判 drain 的注册表；nil = 单机装配，不校验。
	Workers queue.WorkerRegistry
}

// JobQueueFactory 造一个**全新的、与其他用例互不干扰的**队列实例。
// 隔离由 harness 负责（PG 侧要在返回前清表）。
type JobQueueFactory func(t *testing.T, s Spec) queue.JobQueue

// RegistryFactory 造一个全新的 worker 注册表。
type RegistryFactory func(t *testing.T) queue.WorkerRegistry

// Case 是一条具名用例（队列侧）。
type Case struct {
	Name string
	Run  func(t *testing.T, q queue.JobQueue)
}

// RegistryCase 是一条具名用例（注册表侧）。两类分开设类型：把
// WorkerRegistry 塞进 Case 会让编译期看不出「套件跑错了实现」。
type RegistryCase struct {
	Name string
	Run  func(t *testing.T, r queue.WorkerRegistry)
}

// SpecCase 需要指定装配项的用例（队列深度、重试用尽）。
type SpecCase struct {
	Name string
	Spec Spec
	Run  func(t *testing.T, q queue.JobQueue)
}

// RunJobQueueSuite 把整套用例跑在给定实现上。名字里带实现标识，
// 这样 `go test -v` 的输出能直接看出「哪一侧红了」。
func RunJobQueueSuite(t *testing.T, label string, factory JobQueueFactory) {
	t.Helper()
	for _, c := range JobQueueCases() {
		t.Run(label+"/"+c.Name, func(t *testing.T) {
			c.Run(t, factory(t, Spec{}))
		})
	}
	for _, c := range JobQueueSpecCases() {
		t.Run(label+"/"+c.Name, func(t *testing.T) {
			c.Run(t, factory(t, c.Spec))
		})
	}
}

// RunWorkerRegistrySuite 跑注册表侧的套件。
func RunWorkerRegistrySuite(t *testing.T, label string, factory RegistryFactory) {
	t.Helper()
	for _, c := range RegistryCases() {
		t.Run(label+"/"+c.Name, func(t *testing.T) {
			c.Run(t, factory(t))
		})
	}
}

// Pair 是一次装配产出的两个 port：Jobs 已经把 Workers 接进 drain 校验。
//
// 为什么单独一类：「drain 的 worker 不能领活」这条语义横跨两个 port，
// 单跑 JobQueue 的工厂拿不到注册表句柄，写不出这条用例。
type Pair struct {
	Jobs    queue.JobQueue
	Workers queue.WorkerRegistry
}

// PairFactory 造一对联好线的新实例（隔离同 JobQueueFactory）。
type PairFactory func(t *testing.T, s Spec) Pair

// PairCase 是横跨两个 port 的用例。
type PairCase struct {
	Name string
	Spec Spec
	Run  func(t *testing.T, p Pair)
}

// RunPairSuite 跑横跨两个 port 的用例集。
func RunPairSuite(t *testing.T, label string, factory PairFactory) {
	t.Helper()
	for _, c := range PairCases() {
		t.Run(label+"/"+c.Name, func(t *testing.T) {
			c.Run(t, factory(t, c.Spec))
		})
	}
}

// PairCases 返回需要 queue + registry 同时装配的用例。
func PairCases() []PairCase {
	return []PairCase{
		{
			Name: "DrainingWorkerCannotClaimButUnregisteredCan",
			Run: func(t *testing.T, p Pair) {
				ctx := ctxT(t)
				// §9.2：draining 的 worker 领新活 → ErrWorkerDraining；
				// 跑完手上的活（心跳）不受影响（§9.8 不变量 4）。
				if err := p.Workers.Register(ctx, queue.WorkerRecord{WorkerID: "w1"}); err != nil {
					t.Fatal(err)
				}
				if err := p.Workers.Drain(ctx, "w1"); err != nil {
					t.Fatal(err)
				}
				_, err := p.Jobs.Claim(ctx, "w1", time.Minute)
				mustErr(t, err, queue.ErrWorkerDraining, "Claim by a draining worker")

				// 未注册的 worker 不被 drain 校验挡住：单机装配可以完全不建注册表，
				// 「没登记」不等于「在 drain」（内存实现同口径，两侧必须一致）。
				job := enqueue(t, p.Jobs, "k-pair", `{"a":1}`, baseTime)
				got, err := p.Jobs.Claim(ctx, "w-ghost", time.Minute)
				if err != nil {
					t.Fatalf("Claim by an unregistered worker: %v", err)
				}
				if got == nil || got.ID != job.ID {
					t.Fatalf("claim = %+v, want %s", got, job.ID)
				}
				// drain 不影响已领取的行收尾（优雅下线：跑完手上的再走）。
				if err := p.Jobs.Complete(ctx, job.ID, "w-ghost", "completed", "mem://x"); err != nil {
					t.Fatalf("Complete after drain: %v", err)
				}
			},
		},
		{
			Name: "ClaimByKernelConsumesTheDeclaredCapabilities",
			Run: func(t *testing.T, p Pair) {
				ctx := ctxT(t)
				// §9.7 R9（2026-10-23 裁决 = 可选新接口，MINOR）：
				// `WorkerRecord.Kernels` 从「只落库不消费」变成领取过滤器。
				kq := kernelAware(t, p.Jobs)
				if err := p.Workers.Register(ctx, queue.WorkerRecord{WorkerID: "w-go", Kernels: []string{"go"}}); err != nil {
					t.Fatal(err)
				}
				if err := p.Workers.Register(ctx, queue.WorkerRecord{WorkerID: "w-nautilus", Kernels: []string{"nautilus"}}); err != nil {
					t.Fatal(err)
				}
				// created_at 递增 = 领取顺序里 nautilus 行**最靠前**：w-go 领到别的
				// 行只可能是过滤的结果，不是排序的巧合（否则这条用例是假护栏）。
				naut := enqueue(t, p.Jobs, "k-naut", `{"kernel":"nautilus"}`, at(1))
				dflt := enqueue(t, p.Jobs, "k-dflt", `{"a":1}`, at(2))
				explicit := enqueue(t, p.Jobs, "k-go", `{"kernel":"go"}`, at(3))
				empty := enqueue(t, p.Jobs, "k-empty", `{"kernel":""}`, at(4))

				want := func(what string, got *queue.RunRecord, expect *queue.RunRecord) *queue.RunRecord {
					t.Helper()
					if got == nil {
						t.Fatalf("%s: claim = nil, want %s", what, expect.ID)
					}
					if got.ID != expect.ID {
						t.Fatalf("%s: claim = %s, want %s", what, got.ID, expect.ID)
					}
					return got
				}

				// 缺 kernel 键 = 默认内核 go（dataflow §1「默认值不回填」是读侧口径）。
				first := want("w-go 第一次领取", mustClaimKernel(t, kq, "w-go"), dflt)
				if first.WorkerID != "w-go" || first.Status != queue.RunRunning {
					t.Fatalf("claimed row = %+v", first)
				}
				// 被跳过的那一行必须原样留在队列里，不能悄悄贴上别的 owner。
				if left := mustGet(t, p.Jobs, naut.ID); left.Status != queue.RunPending || left.WorkerID == "w-go" {
					t.Fatalf("skipped row was touched: %+v", left)
				}
				want("w-nautilus 领取", mustClaimKernel(t, kq, "w-nautilus"), naut)
				want("w-go 第二次领取", mustClaimKernel(t, kq, "w-go"), explicit)
				// 只剩内核为 `""` 的行：nautilus worker 依旧领不到。
				if got, err := kq.ClaimByKernel(ctx, "w-nautilus", time.Minute); err != nil || got != nil {
					t.Fatalf("w-nautilus claim on the last (default-kernel) row = %+v/%v, want nil/nil", got, err)
				}
				want("空串内核按默认解释", mustClaimKernel(t, kq, "w-go"), empty)
				if got, err := kq.ClaimByKernel(ctx, "w-go", time.Minute); err != nil || got != nil {
					t.Fatalf("exhausted queue = %+v/%v, want nil/nil", got, err)
				}
			},
		},
		{
			Name: "ClaimByKernelReadsOnlyStringKernels",
			Run: func(t *testing.T, p Pair) {
				ctx := ctxT(t)
				// 生效内核的抽取规则两侧必须逐字节同口径（queue.KernelOf 的注释
				// 是规则本体，pgstore.claimSQL 的 CASE 是它的 SQL 形）。
				// 「非字符串 = 默认」不是洁癖：`->>` 对数字给的是它的 JSON 文本，
				// Go 侧解不进 string，少写一层判断就会出现同一行一边能领一边不能。
				kq := kernelAware(t, p.Jobs)
				if err := p.Workers.Register(ctx, queue.WorkerRecord{WorkerID: "w-go", Kernels: []string{"go"}}); err != nil {
					t.Fatal(err)
				}
				if err := p.Workers.Register(ctx, queue.WorkerRecord{WorkerID: "w-nautilus", Kernels: []string{"nautilus"}}); err != nil {
					t.Fatal(err)
				}
				// 四条按 created_at 递增：w-go 必须**按这个顺序**领完它们。
				weird := []*queue.RunRecord{
					enqueue(t, p.Jobs, "k-num", `{"kernel":123}`, at(1)),
					enqueue(t, p.Jobs, "k-null", `{"kernel":null}`, at(2)),
					enqueue(t, p.Jobs, "k-array", `["not-an-object"]`, at(3)),
					enqueue(t, p.Jobs, "k-pad", `{"kernel":" go "}`, at(4)),
				}
				naut := enqueue(t, p.Jobs, "k-naut", `{"kernel":"nautilus"}`, at(5))

				for i, want := range weird {
					got, err := kq.ClaimByKernel(ctx, "w-go", time.Minute)
					if err != nil {
						t.Fatalf("claim %d: %v", i, err)
					}
					if got == nil || got.ID != want.ID {
						t.Fatalf("claim %d = %+v, want %s（这一行的内核不是字符串，应按默认 %q 解释）",
							i, got, want.ID, queue.DefaultKernel)
					}
				}
				// 四条都领完了，nautilus 行还该在原处等它的 worker。
				if left := mustGet(t, p.Jobs, naut.ID); left.Status != queue.RunPending {
					t.Fatalf("nautilus row taken by a go worker: %+v", left)
				}
				if got, err := kq.ClaimByKernel(ctx, "w-nautilus", time.Minute); err != nil || got == nil || got.ID != naut.ID {
					t.Fatalf("w-nautilus claim = %+v/%v, want %s", got, err, naut.ID)
				}
			},
		},
		{
			Name: "ClaimWithoutKernelFilterStaysUnfiltered",
			Run: func(t *testing.T, p Pair) {
				ctx := ctxT(t)
				// 冻结的 `Claim` 一个字没动（§9.6：改签名 = MAJOR）。
				// 能力过滤只长在可选接口上，所以注册了 kernels 的 worker
				// 走 Claim 时**照旧**按 claim 顺序领取，哪怕那活它声明干不了。
				if err := p.Workers.Register(ctx, queue.WorkerRecord{WorkerID: "w-go", Kernels: []string{"go"}}); err != nil {
					t.Fatal(err)
				}
				naut := enqueue(t, p.Jobs, "k-naut", `{"kernel":"nautilus"}`, at(1))
				mine := enqueue(t, p.Jobs, "k-go", `{"kernel":"go"}`, at(2))

				got, err := p.Jobs.Claim(ctx, "w-go", time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				if got == nil || got.ID != naut.ID {
					t.Fatalf("frozen Claim = %+v, want the first row %s (no kernel filter)", got, naut.ID)
				}
				// 未声明能力（kernels 为空）的 worker 走可选接口同样不过滤：
				// 「没登记能力」不等于「什么都不能干」，与 drain 那条同口径。
				if err := p.Workers.Register(ctx, queue.WorkerRecord{WorkerID: "w-any"}); err != nil {
					t.Fatal(err)
				}
				kq := kernelAware(t, p.Jobs)
				got2, err := kq.ClaimByKernel(ctx, "w-any", time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				if got2 == nil || got2.ID != mine.ID {
					t.Fatalf("undeclared worker = %+v, want %s", got2, mine.ID)
				}
			},
		},
		{
			Name: "ClaimByKernelRejectsDrainingAndUnregisteredIsAllowed",
			Run: func(t *testing.T, p Pair) {
				ctx := ctxT(t)
				kq := kernelAware(t, p.Jobs)
				// drain 判定与 Claim 同形：不因为换了入口就放过 draining worker。
				if err := p.Workers.Register(ctx, queue.WorkerRecord{WorkerID: "w1", Kernels: []string{"go"}}); err != nil {
					t.Fatal(err)
				}
				enqueue(t, p.Jobs, "k-drain", `{"a":1}`, at(1))
				if err := p.Workers.Drain(ctx, "w1"); err != nil {
					t.Fatal(err)
				}
				_, err := kq.ClaimByKernel(ctx, "w1", time.Minute)
				mustErr(t, err, queue.ErrWorkerDraining, "ClaimByKernel by a draining worker")

				// 未注册的 worker 不拒（单机装配可以不建注册表，同 Claim 口径）。
				if got, err := kq.ClaimByKernel(ctx, "w-ghost", time.Minute); err != nil || got == nil {
					t.Fatalf("ClaimByKernel by an unregistered worker = %+v/%v, want a row/nil", got, err)
				}
			},
		},
		{
			Name: "OfflineWorkerIsNotDispatchedUntilProofOfLife",
			Run: func(t *testing.T, p Pair) {
				// R10 的验收：心跳超时 → offline 之后，**两个领取入口**都不再派活；
				// 同一个活由在线 worker 领走（离线只是摘掉这一台，不是停队列）；
				// 心跳复活后又能领。
				ctx := ctxT(t)
				rr := reaperOf(t, p.Workers)
				kq := kernelAware(t, p.Jobs)
				const timeout = time.Minute
				stale := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)

				for _, id := range []string{"w-go", "w-backup"} {
					if err := p.Workers.Register(ctx, queue.WorkerRecord{WorkerID: id, Kernels: []string{"go"}}); err != nil {
						t.Fatal(err)
					}
				}
				job := enqueue(t, p.Jobs, "k-offline", `{"kernel":"go"}`, at(1))
				// 只让 w-go 的心跳过期：backup 刚注册，心跳是新的。
				if err := p.Workers.Heartbeat(ctx, "w-go", stale); err != nil {
					t.Fatal(err)
				}
				if n, err := rr.ReapOffline(ctx, time.Now().UTC(), timeout); err != nil || n != 1 {
					t.Fatalf("reap = %d/%v, want 1/nil", n, err)
				}
				if got := mustWorker(t, p.Workers, "w-go"); got.Status != queue.WorkerOffline {
					t.Fatalf("w-go = %q, want offline", got.Status)
				}

				// 两个入口走同一个 active 门：offline 与 draining 共用 ErrWorkerDraining
				// （冻结方法集只有这一个「不配领新活」的哨兵），且都不碰行。
				if _, err := kq.ClaimByKernel(ctx, "w-go", time.Minute); !errors.Is(err, queue.ErrWorkerDraining) {
					t.Fatalf("ClaimByKernel by an offline worker = %v, want ErrWorkerDraining", err)
				}
				if _, err := p.Jobs.Claim(ctx, "w-go", time.Minute); !errors.Is(err, queue.ErrWorkerDraining) {
					t.Fatalf("Claim by an offline worker = %v, want ErrWorkerDraining", err)
				}
				if left := mustGet(t, p.Jobs, job.ID); left.Status != queue.RunPending || left.WorkerID != "" {
					t.Fatalf("the offline worker touched the row: %+v", left)
				}
				if got := mustClaimKernel(t, kq, "w-backup"); got == nil || got.ID != job.ID {
					t.Fatalf("backup claim = %+v, want %s", got, job.ID)
				}

				// 心跳复活：存活证明推翻「离线」推断，重新可派活。
				if err := p.Workers.Heartbeat(ctx, "w-go", time.Time{}); err != nil {
					t.Fatalf("heartbeat after timeout: %v", err)
				}
				again := enqueue(t, p.Jobs, "k-revived", `{"kernel":"go"}`, at(2))
				if got := mustClaimKernel(t, kq, "w-go"); got == nil || got.ID != again.ID {
					t.Fatalf("claim after revival = %+v, want %s", got, again.ID)
				}
			},
		},
	}
}

// ---------------------------------------------------------------- 构造辅助

var baseTime = time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)

// at 返回第 n 个时间戳（纳秒步长，跨实现只在「同一实现内部」比较）。
// 它用来钉住 created_at 这类**调用方给定**的时间，不用于租约判断。
func at(n int) time.Time { return baseTime.Add(time.Duration(n) * time.Second) }

// future / past 相对**墙上时钟**：租约是相对 now 写的（Claim 的 lease 参数），
// 拿 baseTime 那种固定日期去判过期会随运行时刻漂移——本仓的 baseTime 是
// 2026-10-23 12:00Z，而真实运行时间可能在它之后，at(1000) 就成了过去，
// 用例会把「没东西可收」读成「回收逻辑坏了」。
func future() time.Time { return time.Now().UTC().Add(time.Hour) }
func past() time.Time   { return time.Now().UTC().Add(-time.Hour) }

func ctxT(t *testing.T) context.Context {
	t.Helper()
	return context.Background()
}

// enqueue 入队一条并返回行；把 CreatedAt 显式钉住，让排序类用例不依赖真实时钟。
func enqueue(t *testing.T, q queue.JobQueue, key, body string, created time.Time) *queue.RunRecord {
	t.Helper()
	rec := &queue.RunRecord{IdempotencyKey: key, RequestJSON: []byte(body), CreatedAt: created}
	job, created2, err := q.Enqueue(ctxT(t), rec)
	if err != nil {
		t.Fatalf("Enqueue(%s): %v", key, err)
	}
	if !created2 {
		t.Fatalf("Enqueue(%s): expected created=true on first submit", key)
	}
	return job
}

// enqueueWithID 允许显式指定 run id（重复 id 用例用）。
func enqueueWithID(t *testing.T, q queue.JobQueue, id, key, body string) (*queue.RunRecord, bool, error) {
	t.Helper()
	rec := &queue.RunRecord{ID: id, IdempotencyKey: key, RequestJSON: []byte(body)}
	return q.Enqueue(ctxT(t), rec)
}

// claim 领取一条；出错即 fatal（除非用例本来就在测错误）。
func claim(t *testing.T, q queue.JobQueue, worker string, lease time.Duration) *queue.RunRecord {
	t.Helper()
	job, err := q.Claim(ctxT(t), worker, lease)
	if err != nil {
		t.Fatalf("Claim(%s): %v", worker, err)
	}
	return job
}

// kernelAware 探测可选能力接口（R9 的落点）。断言不过就是实现漏了能力声明，
// 这比让用例去调一个不存在的方法更适合当报错：套件里能直接看出是哪一侧红的。
func kernelAware(t *testing.T, q queue.JobQueue) queue.KernelAwareJobQueue {
	t.Helper()
	kq, ok := q.(queue.KernelAwareJobQueue)
	if !ok {
		t.Fatalf("%T does not implement queue.KernelAwareJobQueue (§9.7 R9: Kernels 要真的被消费)", q)
	}
	return kq
}

// reaperOf 探测可选能力接口（R10 的落点）：心跳超时 → offline 必须两个实现都能做，
// 缺了就是能力声明没落地——不静默退回「永不 offline」。
func reaperOf(t *testing.T, r queue.WorkerRegistry) queue.WorkerReaper {
	t.Helper()
	rr, ok := r.(queue.WorkerReaper)
	if !ok {
		t.Fatalf("%T does not implement queue.WorkerReaper (§9.7 R10: 心跳超时 → offline 要真的可执行)", r)
	}
	return rr
}

// mustClaimKernel 走可选接口领取一条；出错即 fatal。
func mustClaimKernel(t *testing.T, kq queue.KernelAwareJobQueue, worker string) *queue.RunRecord {
	t.Helper()
	job, err := kq.ClaimByKernel(ctxT(t), worker, time.Minute)
	if err != nil {
		t.Fatalf("ClaimByKernel(%s): %v", worker, err)
	}
	return job
}

func mustErr(t *testing.T, err error, want error, what string) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("%s: expected %v, got %v", what, want, err)
	}
}

func wantJobNotFound(t *testing.T, q queue.JobQueue, id string) {
	t.Helper()
	_, err := q.Get(ctxT(t), id)
	mustErr(t, err, queue.ErrJobNotFound, "Get("+id+")")
}

// expireLease 等一个租约真的过期。**不能靠反复 RenewLease 探**——续租本身会把
// 租约推后，探针会把要测的状态破坏掉。只读 Get，直到 LeaseUntil 落到过去。
func expireLease(t *testing.T, q queue.JobQueue, id string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		row, err := q.Get(ctxT(t), id)
		if err != nil {
			t.Fatalf("Get(%s) while waiting for lease expiry: %v", id, err)
		}
		if row.LeaseUntil.IsZero() || time.Now().UTC().Before(row.LeaseUntil) {
			if time.Now().After(deadline) {
				t.Fatalf("lease of %s never expired (still %s)", id, row.LeaseUntil)
			}
			time.Sleep(2 * time.Millisecond)
			continue
		}
		return
	}
}

// ---------------------------------------------------------------- 用例集

// JobQueueCases 返回与默认装配无关的用例集。
func JobQueueCases() []Case {
	return []Case{
		{"EnqueueReturnsCleanPendingRow", func(t *testing.T, q queue.JobQueue) {
			body := `{"strategy":"momentum","universe":["600000.SH"]}`
			job := enqueue(t, q, "k1", body, baseTime)
			if job.ID == "" {
				t.Fatal("Enqueue must generate a run id")
			}
			if job.Status != queue.RunPending {
				t.Fatalf("status = %q, want pending", job.Status)
			}
			if job.ResultStatus != "" {
				t.Fatalf("result_status = %q, want empty before terminal", job.ResultStatus)
			}
			if job.Attempts != 0 || job.WorkerID != "" || !job.LeaseUntil.IsZero() || job.CancelRequested {
				t.Fatalf("fresh row must be unclaimed: %+v", job)
			}
			if job.ErrorMessage != "" || job.ResultURI != "" || !job.StartedAt.IsZero() || !job.CompletedAt.IsZero() {
				t.Fatalf("fresh row must have no terminal payload: %+v", job)
			}
			if want, _ := queue.RequestHash([]byte(body)); job.RequestHash != want {
				t.Fatalf("request_hash = %q, want %q", job.RequestHash, want)
			}
			// 存的是 canonical 形式：键序无关、去空白。
			want, err := queue.Canonicalize([]byte(body))
			if err != nil || string(job.RequestJSON) != string(want) {
				t.Fatalf("request_json = %s, want canonical %s (err %v)", job.RequestJSON, want, err)
			}
			// 出库读回必须与入队返回**逐字段相同**：这是 PG 编码层最容易出错的地方。
			back, err := q.Get(ctxT(t), job.ID)
			if err != nil {
				t.Fatal(err)
			}
			assertSameJob(t, job, back)
			// 幂等键也必须在读回的行上（两轴之外的身份）。
			if back.IdempotencyKey != "k1" {
				t.Fatalf("idempotency_key round trip = %q", back.IdempotencyKey)
			}
			if back.MaxAttempts <= 0 {
				t.Fatalf("max_attempts = %d, want the resolved default (>0)", back.MaxAttempts)
			}
		}},

		{"EnqueueIgnoresCallerTerminalState", func(t *testing.T, q queue.JobQueue) {
			// 调用方不许把行"预置"成 running/completed 混进队列（§9.5 两轴映射表的前提）。
			rec := &queue.RunRecord{IdempotencyKey: "k-run", RequestJSON: []byte(`{"a":1}`), Status: queue.RunRunning}
			_, _, err := q.Enqueue(ctxT(t), rec)
			mustErr(t, err, queue.ErrInvalidInput, "Enqueue with status=running")

			rec2 := &queue.RunRecord{ID: "pre-created", IdempotencyKey: "k2", RequestJSON: []byte(`{"a":1}`),
				ResultStatus: queue.ResultCompleted, Attempts: 7, WorkerID: "w9", ResultURI: "mem://x"}
			got, _, err := q.Enqueue(ctxT(t), rec2)
			if err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
			if got.Status != queue.RunPending || got.ResultStatus != "" || got.Attempts != 0 ||
				got.WorkerID != "" || got.ResultURI != "" || !got.CompletedAt.IsZero() {
				t.Fatalf("Enqueue must normalize caller junk away: %+v", got)
			}
		}},

		{"IdempotentReplayIgnoresKeyOrder", func(t *testing.T, q queue.JobQueue) {
			first := enqueue(t, q, "same", `{"a":1,"b":{"x":1,"y":2}}`, baseTime)
			again, created, err := q.Enqueue(ctxT(t), &queue.RunRecord{
				IdempotencyKey: "same", RequestJSON: []byte(`{"b": {"y": 2, "x": 1}, "a": 1}`)})
			if err != nil {
				t.Fatalf("replay: %v", err)
			}
			if created {
				t.Fatal("same key + same canonical content must not create a second row")
			}
			if again.ID != first.ID {
				t.Fatalf("replay returned %s, want original %s", again.ID, first.ID)
			}
			jobs, err := q.List(ctxT(t), queue.ListFilter{Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(jobs) != 1 {
				t.Fatalf("row count = %d, want 1 (replay must not insert)", len(jobs))
			}
		}},

		{"IdempotencyConflictSameKeyDifferentBody", func(t *testing.T, q queue.JobQueue) {
			first := enqueue(t, q, "dup", `{"a":1}`, baseTime)
			_, _, err := q.Enqueue(ctxT(t), &queue.RunRecord{IdempotencyKey: "dup", RequestJSON: []byte(`{"a":2}`)})
			mustErr(t, err, queue.ErrIdempotencyConflict, "same key different request")
			// 冲突不许留下第二行，也不许改掉原行（§api-schema §2：不得静默复用旧 job）。
			rows, err := q.List(ctxT(t), queue.ListFilter{Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 {
				t.Fatalf("row count = %d, want 1", len(rows))
			}
			orig := mustGet(t, q, first.ID)
			if string(orig.RequestJSON) != `{"a":1}` || orig.Status != queue.RunPending {
				t.Fatalf("conflicting submit mutated the original row: %+v", orig)
			}
		}},

		{"EnqueueInputValidation", func(t *testing.T, q queue.JobQueue) {
			cases := []struct {
				name string
				rec  *queue.RunRecord
			}{
				{"nil record", nil},
				{"empty key", &queue.RunRecord{RequestJSON: []byte(`{}`)}},
				{"oversized key", &queue.RunRecord{IdempotencyKey: strings.Repeat("k", queue.MaxIdempotencyKeyLen+1), RequestJSON: []byte(`{}`)}},
				{"empty request_json", &queue.RunRecord{IdempotencyKey: "k", RequestJSON: nil}},
				{"malformed request_json", &queue.RunRecord{IdempotencyKey: "k", RequestJSON: []byte(`{"a":`)}},
				{"trailing request_json", &queue.RunRecord{IdempotencyKey: "k", RequestJSON: []byte(`{"a":1} {"b":2}`)}},
				{"caller hash mismatch", &queue.RunRecord{IdempotencyKey: "k", RequestJSON: []byte(`{"a":1}`), RequestHash: strings.Repeat("0", 64)}},
			}
			for _, c := range cases {
				_, _, err := q.Enqueue(ctxT(t), c.rec)
				if c.name == "nil record" {
					mustErr(t, err, queue.ErrInvalidInput, "Enqueue nil")
					continue
				}
				mustErr(t, err, queue.ErrInvalidInput, "Enqueue "+c.name)
			}
			// 校验必须在写库之前：上面任何一条都不该留下行。
			jobs, err := q.List(ctxT(t), queue.ListFilter{Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(jobs) != 0 {
				t.Fatalf("rejected submits left %d rows", len(jobs))
			}
		}},

		{"ClaimOrderPriorityThenCreatedThenID", func(t *testing.T, q queue.JobQueue) {
			// 顺序判据（§9.2 + §9.8）：priority 降序 → created_at 升序 → id 升序。
			// 最后一条 tie-break 用两个 id 只差最后一个字符的行来钉住。
			mk := func(id string, prio int, when time.Time) {
				rec := &queue.RunRecord{ID: id, IdempotencyKey: "k-" + id, Priority: prio,
					RequestJSON: []byte(`{"p":"` + id + `"}`), CreatedAt: when}
				if _, _, err := q.Enqueue(ctxT(t), rec); err != nil {
					t.Fatalf("Enqueue %s: %v", id, err)
				}
			}
			mk("row-a", 0, at(1))
			mk("row-b", 5, at(2))
			mk("row-c", 5, at(1))
			mk("row-d", 0, at(3))
			mk("tie-b", 1, at(1))
			mk("tie-a", 1, at(1))

			// p=5 的两条按 created_at 升序：row-c(at1) 先于 row-b(at2)；
			// p=1 的两条同 created_at，由 id 升序裁决；p=0 的按时间排。
			want := []string{"row-c", "row-b", "tie-a", "tie-b", "row-a", "row-d"}
			for i, id := range want {
				got := claim(t, q, "w1", time.Minute)
				if got == nil {
					t.Fatalf("claim %d returned nothing, want %s", i, id)
				}
				if got.ID != id {
					t.Fatalf("claim order %d = %s, want %s", i, got.ID, id)
				}
				if got.Status != queue.RunRunning || got.WorkerID != "w1" {
					t.Fatalf("claimed row must be running/owned: %+v", got)
				}
				if !got.LeaseUntil.After(time.Now().UTC()) {
					t.Fatalf("lease must be in the future: %s", got.LeaseUntil)
				}
				if got.StartedAt.IsZero() {
					t.Fatal("first claim must stamp started_at")
				}
			}
			if got := claim(t, q, "w1", time.Minute); got != nil {
				t.Fatalf("queue drained but got %s", got.ID)
			}
		}},

		{"ClaimInputValidation", func(t *testing.T, q queue.JobQueue) {
			enqueue(t, q, "k", `{"a":1}`, baseTime)
			_, err := q.Claim(ctxT(t), "", time.Minute)
			mustErr(t, err, queue.ErrInvalidInput, "Claim without worker_id")
			_, err = q.Claim(ctxT(t), "w1", 0)
			mustErr(t, err, queue.ErrInvalidInput, "Claim with zero lease")
			_, err = q.Claim(ctxT(t), "w1", -time.Minute)
			mustErr(t, err, queue.ErrInvalidInput, "Claim with negative lease")
		}},

		{"ClaimSkipsRowsWithAnUnreleasedLease", func(t *testing.T, q queue.JobQueue) {
			// 领取判据是「pending **且** 租约已释放/已过期」（§9.2）。
			// 「pending + 未来租约」这个形状内存实现自己造不出来（回收时把租约
			// 清成 NULL），所以只能由**直接改库**产生——PG 侧的谓词由
			// internal/pgstore 的 ClaimPredicateSkipsLiveLease 用例用真 SQL 钉住，
			// 这里只保住两侧共同可达的那一半：租约释放后同一行可以再领。
			job := enqueue(t, q, "k-requeue", `{"a":1}`, baseTime)
			claim(t, q, "w1", time.Minute)
			if n, err := q.ReapExpired(ctxT(t), future()); err != nil || n != 1 {
				t.Fatalf("reap = %d/%v, want 1/nil", n, err)
			}
			again := claim(t, q, "w2", time.Minute)
			if again == nil || again.ID != job.ID {
				t.Fatalf("released row must be claimable again, got %+v", again)
			}
		}},

		{"RenewLeaseOnlyByOwnerInForce", func(t *testing.T, q queue.JobQueue) {
			job := enqueue(t, q, "k", `{"a":1}`, baseTime)
			claim(t, q, "w1", time.Minute)

			if err := q.RenewLease(ctxT(t), job.ID, "w2", time.Minute); !errors.Is(err, queue.ErrLeaseLost) {
				t.Fatalf("renew by non-owner = %v, want ErrLeaseLost", err)
			}
			before := mustGet(t, q, job.ID)
			if err := q.RenewLease(ctxT(t), job.ID, "w1", time.Hour); err != nil {
				t.Fatalf("renew by owner: %v", err)
			}
			after := mustGet(t, q, job.ID)
			if !after.LeaseUntil.After(before.LeaseUntil) {
				t.Fatalf("lease did not move forward: %s -> %s", before.LeaseUntil, after.LeaseUntil)
			}
			if after.Status != queue.RunRunning || after.WorkerID != "w1" {
				t.Fatalf("RenewLease must not change lifecycle: %+v", after)
			}
			if err := q.RenewLease(ctxT(t), job.ID, "w1", 0); !errors.Is(err, queue.ErrInvalidInput) {
				t.Fatalf("renew with zero lease = %v, want ErrInvalidInput", err)
			}
			if err := q.RenewLease(ctxT(t), "nope", "w1", time.Minute); !errors.Is(err, queue.ErrLeaseLost) {
				t.Fatalf("renew unknown run = %v, want ErrLeaseLost", err)
			}
		}},

		{"ExpiredLeaseFencesTheOldOwner", func(t *testing.T, q queue.JobQueue) {
			// §9.8 不变量 6：过期 owner 的写入必须被拒——否则两个 worker 各写一份终态。
			job := enqueue(t, q, "k", `{"a":1}`, baseTime)
			claim(t, q, "w1", time.Millisecond)
			expireLease(t, q, job.ID)

			if err := q.RenewLease(ctxT(t), job.ID, "w1", time.Minute); !errors.Is(err, queue.ErrLeaseLost) {
				t.Fatalf("renew after expiry = %v, want ErrLeaseLost", err)
			}
			if err := q.Complete(ctxT(t), job.ID, "w1", "completed", "mem://late"); !errors.Is(err, queue.ErrLeaseLost) {
				t.Fatalf("Complete after expiry = %v, want ErrLeaseLost", err)
			}
			if err := q.Fail(ctxT(t), job.ID, "w1", "late", "failed"); !errors.Is(err, queue.ErrLeaseLost) {
				t.Fatalf("Fail after expiry = %v, want ErrLeaseLost", err)
			}
			if err := q.AckCancel(ctxT(t), job.ID, "w1"); !errors.Is(err, queue.ErrLeaseLost) {
				t.Fatalf("AckCancel after expiry = %v, want ErrLeaseLost", err)
			}
			// 被拒的写入一行都不许落地。
			row := mustGet(t, q, job.ID)
			if row.Status != queue.RunRunning || row.ResultStatus != "" || row.ResultURI != "" || row.ErrorMessage != "" {
				t.Fatalf("fenced writes leaked into the row: %+v", row)
			}
		}},

		{"CompleteWritesBothAxes", func(t *testing.T, q queue.JobQueue) {
			// 两轴映射表（§9.5）逐行钉住：invalid 是 completed + result_status=invalid + **error 空**。
			for _, rs := range []string{"completed", "invalid"} {
				job := enqueue(t, q, "k-"+rs, `{"a":1}`, at(len(rs)))
				claim(t, q, "w1", time.Minute)
				if err := q.Complete(ctxT(t), job.ID, "w1", rs, "mem://r/"+rs); err != nil {
					t.Fatalf("Complete(%s): %v", rs, err)
				}
				row := mustGet(t, q, job.ID)
				if row.Status != queue.RunCompleted || string(row.ResultStatus) != rs {
					t.Fatalf("result_status=%q must map to status=%q, got %q/%q", rs, queue.RunCompleted, row.Status, row.ResultStatus)
				}
				if row.ErrorMessage != "" {
					t.Fatalf("a finished run must carry no error (invalid 不是故障): %q", row.ErrorMessage)
				}
				if row.ResultURI == "" || row.CompletedAt.IsZero() || !row.LeaseUntil.IsZero() {
					t.Fatalf("terminal payload incomplete: %+v", row)
				}
				// 终态之后不可再迁移（不变量 1）。
				if err := q.Complete(ctxT(t), job.ID, "w1", rs, "mem://r/"+rs); err != nil {
					t.Fatalf("repeat identical Complete must be idempotent: %v", err)
				}
				if err := q.Complete(ctxT(t), job.ID, "w1", "completed", "mem://other"); !errors.Is(err, queue.ErrInvalidTransition) {
					t.Fatalf("different payload on terminal = %v, want ErrInvalidTransition", err)
				}
				if err := q.Fail(ctxT(t), job.ID, "w1", "x", "failed"); !errors.Is(err, queue.ErrInvalidTransition) {
					t.Fatalf("Fail on completed = %v, want ErrInvalidTransition", err)
				}
				if err := q.RenewLease(ctxT(t), job.ID, "w1", time.Minute); !errors.Is(err, queue.ErrLeaseLost) {
					t.Fatalf("RenewLease on terminal = %v, want ErrLeaseLost", err)
				}
			}
		}},

		{"CompleteInputValidation", func(t *testing.T, q queue.JobQueue) {
			job := enqueue(t, q, "k", `{"a":1}`, baseTime)
			claim(t, q, "w1", time.Minute)
			// failed 走 Fail，不是 Complete（§9.2）。
			if err := q.Complete(ctxT(t), job.ID, "w1", "failed", "mem://x"); !errors.Is(err, queue.ErrInvalidInput) {
				t.Fatalf("Complete(failed) = %v, want ErrInvalidInput", err)
			}
			if err := q.Complete(ctxT(t), job.ID, "w1", "", "mem://x"); !errors.Is(err, queue.ErrInvalidInput) {
				t.Fatalf("Complete(empty) = %v, want ErrInvalidInput", err)
			}
			if err := q.Complete(ctxT(t), job.ID, "w1", "bogus", "mem://x"); !errors.Is(err, queue.ErrInvalidInput) {
				t.Fatalf("Complete(bogus) = %v, want ErrInvalidInput", err)
			}
			// 空 result_uri = 结果不可达（run 不存结果内容，指针是唯一的出口）。
			if err := q.Complete(ctxT(t), job.ID, "w1", "completed", ""); !errors.Is(err, queue.ErrInvalidInput) {
				t.Fatalf("Complete without uri = %v, want ErrInvalidInput", err)
			}
			if err := q.Complete(ctxT(t), "nope", "w1", "completed", "mem://x"); !errors.Is(err, queue.ErrJobNotFound) {
				t.Fatalf("Complete unknown = %v, want ErrJobNotFound", err)
			}
			// 校验先于状态判定：非法载荷不该把 pending 行推去 running 的检查。
			if err := q.Complete(ctxT(t), job.ID, "w2", "completed", "mem://x"); !errors.Is(err, queue.ErrLeaseLost) {
				t.Fatalf("Complete by non-owner = %v, want ErrLeaseLost", err)
			}
			mustGet(t, q, job.ID) // 行还在、还是 running
		}},

		{"CompleteRejectsCancellingRow", func(t *testing.T, q queue.JobQueue) {
			// §9.8 取舍 3：取消一经请求，结果即作废——cancelling 的唯一出口是 cancelled。
			job := enqueue(t, q, "k", `{"a":1}`, baseTime)
			claim(t, q, "w1", time.Minute)
			if err := q.RequestCancel(ctxT(t), job.ID); err != nil {
				t.Fatal(err)
			}
			if err := q.Complete(ctxT(t), job.ID, "w1", "completed", "mem://x"); !errors.Is(err, queue.ErrInvalidTransition) {
				t.Fatalf("Complete on cancelling = %v, want ErrInvalidTransition", err)
			}
			if err := q.AckCancel(ctxT(t), job.ID, "w1"); err != nil {
				t.Fatalf("AckCancel: %v", err)
			}
			row := mustGet(t, q, job.ID)
			if row.Status != queue.RunCancelled || row.ResultStatus != "" || row.ResultURI != "" {
				t.Fatalf("cancelled row must keep no result: %+v", row)
			}
		}},

		{"FailWritesBothAxes", func(t *testing.T, q queue.JobQueue) {
			for _, tc := range []struct {
				rs      string
				wantRes queue.ResultStatus
			}{
				{"failed", queue.ResultFailed}, // 确实产出过 ResultSpec
				{"", ""},                       // 没产出（如路由拒绝）→ 列写 NULL
			} {
				job := enqueue(t, q, "k-fail-"+tc.rs, `{"a":1}`, baseTime)
				claim(t, q, "w1", time.Minute)
				if err := q.Fail(ctxT(t), job.ID, "w1", "engine blew up", tc.rs); err != nil {
					t.Fatalf("Fail(%q): %v", tc.rs, err)
				}
				row := mustGet(t, q, job.ID)
				if row.Status != queue.RunFailed || row.ResultStatus != tc.wantRes {
					t.Fatalf("Fail(%q) -> %q/%q, want failed/%q", tc.rs, row.Status, row.ResultStatus, tc.wantRes)
				}
				if row.ErrorMessage != "engine blew up" || row.CompletedAt.IsZero() || !row.LeaseUntil.IsZero() {
					t.Fatalf("failed row incomplete: %+v", row)
				}
				if row.ResultURI != "" {
					t.Fatalf("a failed run must not carry a result pointer: %+v", row)
				}
				if err := q.Fail(ctxT(t), job.ID, "w1", "engine blew up", tc.rs); err != nil {
					t.Fatalf("repeat identical Fail must be idempotent: %v", err)
				}
				if err := q.Fail(ctxT(t), job.ID, "w1", "different", tc.rs); !errors.Is(err, queue.ErrInvalidTransition) {
					t.Fatalf("Fail with another payload = %v, want ErrInvalidTransition", err)
				}
			}
			// 未领取的行不能 Fail（没有 owner、没有租约）。
			lone := enqueue(t, q, "k-lone", `{"a":1}`, at(9))
			if err := q.Fail(ctxT(t), lone.ID, "w1", "x", "failed"); !errors.Is(err, queue.ErrInvalidTransition) {
				t.Fatalf("Fail on pending = %v, want ErrInvalidTransition", err)
			}
		}},

		{"FailInputValidation", func(t *testing.T, q queue.JobQueue) {
			job := enqueue(t, q, "k", `{"a":1}`, baseTime)
			claim(t, q, "w1", time.Minute)
			if err := q.Fail(ctxT(t), job.ID, "w1", "", "failed"); !errors.Is(err, queue.ErrInvalidInput) {
				t.Fatalf("Fail without message = %v, want ErrInvalidInput", err)
			}
			for _, rs := range []string{"completed", "invalid", "bogus"} {
				if err := q.Fail(ctxT(t), job.ID, "w1", "x", rs); !errors.Is(err, queue.ErrInvalidInput) {
					t.Fatalf("Fail with result_status=%q = %v, want ErrInvalidInput", rs, err)
				}
			}
			if err := q.Fail(ctxT(t), "nope", "w1", "x", "failed"); !errors.Is(err, queue.ErrJobNotFound) {
				t.Fatalf("Fail unknown = %v, want ErrJobNotFound", err)
			}
			if err := q.Fail(ctxT(t), job.ID, "w2", "x", "failed"); !errors.Is(err, queue.ErrLeaseLost) {
				t.Fatalf("Fail by non-owner = %v, want ErrLeaseLost", err)
			}
		}},

		{"CancelPendingKeepsRow", func(t *testing.T, q queue.JobQueue) {
			// 取消 pending 是**保留行**的 cancelled（删行会同时删掉幂等键与审计）。
			job := enqueue(t, q, "k", `{"a":1}`, baseTime)
			if err := q.RequestCancel(ctxT(t), job.ID); err != nil {
				t.Fatal(err)
			}
			row := mustGet(t, q, job.ID)
			if row.Status != queue.RunCancelled || !row.CancelRequested || row.CompletedAt.IsZero() {
				t.Fatalf("cancel pending -> %+v", row)
			}
			// 幂等键仍在：同键重放必须回到这一行，而不是新建。
			again, created, err := q.Enqueue(ctxT(t), &queue.RunRecord{IdempotencyKey: "k", RequestJSON: []byte(`{"a":1}`)})
			if err != nil || created || again.ID != job.ID {
				t.Fatalf("idempotency key must survive cancel: created=%v id=%s err=%v", created, again.ID, err)
			}
			// 终态再取消 = 幂等空操作。
			if err := q.RequestCancel(ctxT(t), job.ID); err != nil {
				t.Fatalf("cancel a cancelled run = %v, want nil", err)
			}
			// 终态不可再迁移。
			if err := q.AckCancel(ctxT(t), job.ID, "w1"); !errors.Is(err, queue.ErrInvalidTransition) {
				t.Fatalf("AckCancel on terminal = %v, want ErrInvalidTransition", err)
			}
		}},

		{"CancelRunningWaitsForOwnerAck", func(t *testing.T, q queue.JobQueue) {
			job := enqueue(t, q, "k", `{"a":1}`, baseTime)
			claim(t, q, "w1", time.Minute)
			if err := q.RequestCancel(ctxT(t), job.ID); err != nil {
				t.Fatal(err)
			}
			row := mustGet(t, q, job.ID)
			if row.Status != queue.RunCancelling || !row.CancelRequested {
				t.Fatalf("cancel running -> %+v", row)
			}
			if row.WorkerID != "w1" || row.LeaseUntil.IsZero() {
				t.Fatalf("cancelling must keep the in-flight owner and lease: %+v", row)
			}
			// 重复取消仍是 cancelling，不动在跑的 worker（幂等）。
			if err := q.RequestCancel(ctxT(t), job.ID); err != nil {
				t.Fatalf("repeat cancel = %v", err)
			}
			if got := mustGet(t, q, job.ID); got.Status != queue.RunCancelling {
				t.Fatalf("repeat cancel moved status to %q", got.Status)
			}
			// 只有 owner 的确认能落终态。
			if err := q.AckCancel(ctxT(t), job.ID, "w2"); !errors.Is(err, queue.ErrLeaseLost) {
				t.Fatalf("AckCancel by non-owner = %v, want ErrLeaseLost", err)
			}
			if err := q.AckCancel(ctxT(t), job.ID, "w1"); err != nil {
				t.Fatalf("AckCancel by owner: %v", err)
			}
			final := mustGet(t, q, job.ID)
			if final.Status != queue.RunCancelled || final.ResultStatus != "" || final.CompletedAt.IsZero() {
				t.Fatalf("acked cancel -> %+v", final)
			}
		}},

		{"AckCancelOnPendingIsLeaseLost", func(t *testing.T, q queue.JobQueue) {
			// §9.2 冻结表只列了「非 owner → ErrLeaseLost」与「已终态 →
			// ErrInvalidTransition」两种出口；pending 行没有 owner 也没有租约，
			// 归到前一种，实现不擅自补第三种（内存实现同取舍）。
			job := enqueue(t, q, "k", `{"a":1}`, baseTime)
			if err := q.AckCancel(ctxT(t), job.ID, "w1"); !errors.Is(err, queue.ErrLeaseLost) {
				t.Fatalf("AckCancel on pending = %v, want ErrLeaseLost", err)
			}
			if err := q.AckCancel(ctxT(t), "nope", "w1"); !errors.Is(err, queue.ErrJobNotFound) {
				t.Fatalf("AckCancel unknown = %v, want ErrJobNotFound", err)
			}
		}},

		{"GetRejectsUnknownID", func(t *testing.T, q queue.JobQueue) {
			wantJobNotFound(t, q, "missing")
		}},

		{"ListFiltersAndOrdering", func(t *testing.T, q queue.JobQueue) {
			a := enqueue(t, q, "k-a", `{"a":1}`, at(1))
			b := enqueue(t, q, "k-b", `{"a":2}`, at(2))
			c := enqueue(t, q, "k-c", `{"a":3}`, at(3))
			claim(t, q, "w1", time.Minute) // 领走最早的一条（created_at 升序）= a
			if err := q.Complete(ctxT(t), a.ID, "w1", "invalid", "mem://a"); err != nil {
				t.Fatal(err)
			}

			// created_at 降序 → id 升序（§9.8）。
			all, err := q.List(ctxT(t), queue.ListFilter{Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(all) != 3 || all[0].ID != c.ID || all[1].ID != b.ID || all[2].ID != a.ID {
				t.Fatalf("order = %s", idsOf(all))
			}
			// 两轴各自可筛（精确匹配，不是前缀/模糊）。
			done, err := q.List(ctxT(t), queue.ListFilter{Status: queue.RunCompleted, Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(done) != 1 || done[0].ID != a.ID || done[0].ResultStatus != queue.ResultInvalid {
				t.Fatalf("status=completed -> %s", idsOf(done))
			}
			pending, err := q.List(ctxT(t), queue.ListFilter{Status: queue.RunPending, Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(pending) != 2 {
				t.Fatalf("status=pending -> %s", idsOf(pending))
			}
			invalid, err := q.List(ctxT(t), queue.ListFilter{ResultStatus: queue.ResultInvalid, Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(invalid) != 1 || invalid[0].ID != a.ID {
				t.Fatalf("result_status=invalid -> %s", idsOf(invalid))
			}
			// 两轴合取：running 且带结论 = 空（两轴不合并的证据）。
			mix, err := q.List(ctxT(t), queue.ListFilter{Status: queue.RunRunning, ResultStatus: queue.ResultInvalid, Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(mix) != 0 {
				t.Fatalf("running+invalid must be empty, got %s", idsOf(mix))
			}
			// Before 是严格小于。
			before, err := q.List(ctxT(t), queue.ListFilter{Before: at(2), Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(before) != 1 || before[0].ID != a.ID {
				t.Fatalf("created_at < at(2) -> %s", idsOf(before))
			}
			// 未知枚举值当场拒（不猜）。
			if _, err := q.List(ctxT(t), queue.ListFilter{Status: "queued"}); !errors.Is(err, queue.ErrInvalidInput) {
				t.Fatalf("unknown status = %v, want ErrInvalidInput", err)
			}
			if _, err := q.List(ctxT(t), queue.ListFilter{ResultStatus: "partial"}); !errors.Is(err, queue.ErrInvalidInput) {
				t.Fatalf("unknown result_status = %v, want ErrInvalidInput", err)
			}
		}},

		{"ListEmptyIsNotNil", func(t *testing.T, q queue.JobQueue) {
			rows, err := q.List(ctxT(t), queue.ListFilter{})
			if err != nil {
				t.Fatal(err)
			}
			if rows == nil {
				t.Fatal("empty List must return an empty slice, not nil (JSON 稳定：[] 而不是 null)")
			}
			if len(rows) != 0 {
				t.Fatalf("rows = %d", len(rows))
			}
		}},

		{"ClaimDoesNotStealAnotherWorkersLease", func(t *testing.T, q queue.JobQueue) {
			// 一个 worker 领完，另一个 worker 不能再领同一条（running 不是可领状态）。
			job := enqueue(t, q, "k", `{"a":1}`, baseTime)
			if got := claim(t, q, "w1", time.Hour); got.ID != job.ID {
				t.Fatalf("claim = %s", got.ID)
			}
			if got := claim(t, q, "w2", time.Hour); got != nil {
				t.Fatalf("second worker claimed the in-flight row %s", got.ID)
			}
		}},

		{"ConcurrentClaimsHandOutDistinctRows", func(t *testing.T, q queue.JobQueue) {
			// 这条是 PG 侧 SKIP LOCKED 的判据，内存侧靠互斥锁也必须给出同样结果：
			// N 个并发领取**不重不漏**，且不报错。
			const rows = 7
			ids := make(map[string]bool, rows)
			for i := 0; i < rows; i++ {
				j := enqueue(t, q, fmt.Sprintf("k-%d", i), fmt.Sprintf(`{"i":%d}`, i), at(i))
				ids[j.ID] = true
			}

			var mu sync.Mutex
			got := make(map[string]int, rows)
			var wg sync.WaitGroup
			for w := 0; w < 12; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for {
						row, err := q.Claim(ctxT(t), fmt.Sprintf("worker-%d", w), time.Hour)
						if err != nil {
							t.Errorf("concurrent Claim: %v", err)
							return
						}
						if row == nil {
							return
						}
						mu.Lock()
						got[row.ID]++
						mu.Unlock()
					}
				}(w)
			}
			wg.Wait()

			if len(got) != rows {
				t.Fatalf("claimed %d distinct rows, want %d", len(got), rows)
			}
			for id, n := range got {
				if n != 1 {
					t.Fatalf("row %s claimed %d times (double dispatch)", id, n)
				}
				if !ids[id] {
					t.Fatalf("claimed unknown row %s", id)
				}
			}
		}},

		{"ConcurrentSameKeyYieldsOneRow", func(t *testing.T, q queue.JobQueue) {
			// 同一个幂等键并发提交：只有一行，且所有调用方都拿到同一个 job_id
			// （202 与 200 的差别只在 created，不在身份）。
			const n = 12
			var wg sync.WaitGroup
			results := make(chan string, n)
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					job, _, err := q.Enqueue(ctxT(t), &queue.RunRecord{IdempotencyKey: "race", RequestJSON: []byte(`{"a":1}`)})
					if err != nil {
						t.Errorf("concurrent Enqueue: %v", err)
						return
					}
					results <- job.ID
				}()
			}
			wg.Wait()
			close(results)

			seen := map[string]bool{}
			for id := range results {
				seen[id] = true
			}
			if len(seen) != 1 {
				t.Fatalf("same key produced %d different run ids: %v", len(seen), keysOf(seen))
			}
			rows, err := q.List(ctxT(t), queue.ListFilter{Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 {
				t.Fatalf("row count = %d, want 1", len(rows))
			}
		}},
	}
}

// JobQueueSpecCases 需要指定装配项（深度上限 / 重试预算）的用例集。
func JobQueueSpecCases() []SpecCase {
	return []SpecCase{
		{
			Name: "QueueFullIsHardLimit",
			Spec: Spec{MaxQueueDepth: 2},
			Run: func(t *testing.T, q queue.JobQueue) {
				// 契约只冻结「达上限 → ErrQueueFull」这件事本身（R7：阈值属部署）。
				a := enqueue(t, q, "k-a", `{"a":1}`, at(1))
				enqueue(t, q, "k-b", `{"a":2}`, at(2))
				_, _, err := q.Enqueue(ctxT(t), &queue.RunRecord{IdempotencyKey: "k-c", RequestJSON: []byte(`{"a":3}`)})
				mustErr(t, err, queue.ErrQueueFull, "third pending row")
				if _, _, err := q.Enqueue(ctxT(t), &queue.RunRecord{IdempotencyKey: "k-a", RequestJSON: []byte(`{"a":1}`)}); err != nil {
					t.Fatalf("replay at a full queue must stay idempotent: %v", err)
				}
				// 领取腾出深度后可以继续提交。
				if got := claim(t, q, "w1", time.Hour); got.ID != a.ID {
					t.Fatalf("claim order = %s, want %s", got.ID, a.ID)
				}
				if _, created, err := q.Enqueue(ctxT(t), &queue.RunRecord{IdempotencyKey: "k-c", RequestJSON: []byte(`{"a":3}`)}); err != nil || !created {
					t.Fatalf("after freeing depth submit must succeed: created=%v err=%v", created, err)
				}
			},
		},
		{
			Name: "ConcurrentQueueFullIsExact",
			Spec: Spec{MaxQueueDepth: 1},
			Run: func(t *testing.T, q queue.JobQueue) {
				// 边界必须**精确**：内存实现靠单锁天然精确，PG 靠提交路径的
				// advisory lock。少了它，两个并发提交会各自看到「还没满」，
				// 深度上限就成了摆设（429 的语义也随之失效）。
				//
				// 起跑用 barrier：逐个 go 会把事务彼此错开，竞态窗口根本打不开——
				// 第一版就是这么「绿」在把 advisory lock 删掉的代码上的
				// （2026-10-23 破坏验证实测）。那种假绿比没有这条用例更危险。
				const rounds, n = 3, 24
				for round := 0; round < rounds; round++ {
					var wg sync.WaitGroup
					start := make(chan struct{})
					var mu sync.Mutex
					var accepted, full int
					for i := 0; i < n; i++ {
						wg.Add(1)
						go func(i int) {
							defer wg.Done()
							<-start
							key := fmt.Sprintf("race-%d-%d", round, i)
							_, _, err := q.Enqueue(ctxT(t), &queue.RunRecord{
								IdempotencyKey: key, RequestJSON: []byte(`{"i":1}`)})
							mu.Lock()
							defer mu.Unlock()
							switch {
							case err == nil:
								accepted++
							case errors.Is(err, queue.ErrQueueFull):
								full++
							default:
								t.Errorf("unexpected enqueue error: %v", err)
							}
						}(i)
					}
					close(start)
					wg.Wait()
					if accepted != 1 || full != n-1 {
						t.Fatalf("round %d：深度 1 接受 %d 行、拒 %d 行（want 1 / %d）", round, accepted, full, n-1)
					}
					// 腾出深度给下一轮：pending 行取消后仍在库里（保留行是契约）。
					rows, err := q.List(ctxT(t), queue.ListFilter{Status: queue.RunPending, Limit: 10})
					if err != nil {
						t.Fatal(err)
					}
					if len(rows) != 1 {
						t.Fatalf("round %d：pending 行数 = %d, want 1（并发越界就是这里看得出来）", round, len(rows))
					}
					if err := q.RequestCancel(ctxT(t), rows[0].ID); err != nil {
						t.Fatal(err)
					}
				}
			},
		},
		{
			Name: "ReapExpiredReturnsRowToPending",
			Spec: Spec{DefaultMaxAttempts: 5},
			Run: func(t *testing.T, q queue.JobQueue) {
				job := enqueue(t, q, "k", `{"a":1}`, baseTime)
				claim(t, q, "w1", time.Minute)

				// 未到期的租约不许动（$now 早于 lease_until）。
				if n, err := q.ReapExpired(ctxT(t), past()); err != nil || n != 0 {
					t.Fatalf("reap before expiry = %d/%v, want 0/nil", n, err)
				}
				n, err := q.ReapExpired(ctxT(t), future())
				if err != nil || n != 1 {
					t.Fatalf("reap after expiry = %d/%v, want 1/nil", n, err)
				}
				row := mustGet(t, q, job.ID)
				if row.Status != queue.RunPending {
					t.Fatalf("reaped row = %q, want pending", row.Status)
				}
				if row.Attempts != 1 || !row.LeaseUntil.IsZero() {
					t.Fatalf("reap must bump attempts and release the lease: %+v", row)
				}
				if row.WorkerID != "w1" || row.StartedAt.IsZero() {
					t.Fatalf("reap keeps the previous owner for audit (§9.8 取舍 5): %+v", row)
				}
				// 可重入：再扫一次没有行可迁。
				if n, err := q.ReapExpired(ctxT(t), future()); err != nil || n != 0 {
					t.Fatalf("second reap = %d/%v, want 0/nil", n, err)
				}
				// 回收后可以再领（换 owner）。
				again := claim(t, q, "w2", time.Hour)
				if again.ID != job.ID || again.WorkerID != "w2" {
					t.Fatalf("re-claim = %+v", again)
				}
				if again.StartedAt != row.StartedAt {
					t.Fatalf("re-claim must not restamp started_at: %s -> %s", row.StartedAt, again.StartedAt)
				}
			},
		},
		{
			Name: "ReapExhaustsAttemptsIntoFailed",
			Spec: Spec{DefaultMaxAttempts: 2},
			Run: func(t *testing.T, q queue.JobQueue) {
				// 两轴映射表最后一行：租约重试耗尽 → status=failed +
				// result_status NULL + error_message=attempts_exhausted。
				job := enqueue(t, q, "k", `{"a":1}`, baseTime)
				claim(t, q, "w1", time.Minute)
				if n, err := q.ReapExpired(ctxT(t), future()); err != nil || n != 1 {
					t.Fatalf("first reap = %d/%v", n, err)
				}
				claim(t, q, "w2", time.Minute)
				if n, err := q.ReapExpired(ctxT(t), future()); err != nil || n != 1 {
					t.Fatalf("second reap = %d/%v", n, err)
				}
				row := mustGet(t, q, job.ID)
				if row.Status != queue.RunFailed {
					t.Fatalf("attempts %d -> status %q, want failed", row.Attempts, row.Status)
				}
				if row.Attempts != 2 || row.ResultStatus != "" || row.ErrorMessage != queue.AttemptsExhaustedMessage {
					t.Fatalf("exhausted row = %+v", row)
				}
				if row.CompletedAt.IsZero() || !row.LeaseUntil.IsZero() {
					t.Fatalf("exhausted row must be terminal: %+v", row)
				}
				// 终态之后不可再迁移。
				if err := q.Complete(ctxT(t), row.ID, "w2", "completed", "mem://late"); !errors.Is(err, queue.ErrInvalidTransition) {
					t.Fatalf("Complete after exhaustion = %v, want ErrInvalidTransition", err)
				}
			},
		},
		{
			Name: "ReapIgnoresTerminalRows",
			Spec: Spec{DefaultMaxAttempts: 3},
			Run: func(t *testing.T, q queue.JobQueue) {
				// 三条终态出口（completed / failed / cancelled）都不许被回收碰。
				// 旧用例叫 ReapIgnoresTerminalAndCancelling：`cancelling` 那一半
				// 在 2026-10-23 的 R8 裁决后不再成立，拆成两条（另一条见下）。
				done := enqueue(t, q, "k-done", `{"a":1}`, at(1))
				claim(t, q, "w1", time.Minute)
				if err := q.Complete(ctxT(t), done.ID, "w1", "completed", "mem://done"); err != nil {
					t.Fatal(err)
				}
				broken := enqueue(t, q, "k-broken", `{"a":2}`, at(2))
				claim(t, q, "w2", time.Minute)
				if err := q.Fail(ctxT(t), broken.ID, "w2", "boom", "failed"); err != nil {
					t.Fatal(err)
				}
				// pending 行直接取消：终态 cancelled，且从未有过租约。
				quit := enqueue(t, q, "k-quit", `{"a":3}`, at(3))
				if err := q.RequestCancel(ctxT(t), quit.ID); err != nil {
					t.Fatal(err)
				}

				n, err := q.ReapExpired(ctxT(t), future())
				if err != nil {
					t.Fatal(err)
				}
				if n != 0 {
					t.Fatalf("reap touched %d terminal rows, want 0", n)
				}
				for _, id := range []string{done.ID, broken.ID, quit.ID} {
					row := mustGet(t, q, id)
					if !row.Terminal() {
						t.Fatalf("%s left terminal state: %+v", id, row)
					}
				}
				if got := mustGet(t, q, done.ID); got.Attempts != 0 || got.ResultStatus != queue.ResultCompleted {
					t.Fatalf("completed row was modified: %+v", got)
				}
				if got := mustGet(t, q, broken.ID); got.Attempts != 0 || got.ErrorMessage != "boom" {
					t.Fatalf("failed row was modified: %+v", got)
				}
			},
		},
		{
			Name: "ReapLandsOrphanedCancellingRow",
			Spec: Spec{DefaultMaxAttempts: 3},
			Run: func(t *testing.T, q queue.JobQueue) {
				// §9.7 R8（2026-10-23 裁决：同批落 cancelled）。owner 在
				// running 上被请求取消后崩溃，`AckCancel` 就永远等不到——
				// 两轴映射表里 `cancelling` 的唯一出口是 `cancelled`，
				// 回收是它唯一的落地路径。**不计 attempts**：它不是重试。
				job := enqueue(t, q, "k-orphan", `{"a":1}`, at(1))
				claim(t, q, "w1", time.Minute)
				if err := q.RequestCancel(ctxT(t), job.ID); err != nil {
					t.Fatal(err)
				}
				// 未到期不动（与 running 同一条谓词）。
				if n, err := q.ReapExpired(ctxT(t), past()); err != nil || n != 0 {
					t.Fatalf("reap before expiry = %d/%v, want 0/nil", n, err)
				}
				if got := mustGet(t, q, job.ID); got.Status != queue.RunCancelling {
					t.Fatalf("row before reap = %q, want cancelling", got.Status)
				}

				n, err := q.ReapExpired(ctxT(t), future())
				if err != nil || n != 1 {
					t.Fatalf("reap after expiry = %d/%v, want 1/nil", n, err)
				}
				row := mustGet(t, q, job.ID)
				if row.Status != queue.RunCancelled {
					t.Fatalf("orphaned cancelling row = %q, want cancelled", row.Status)
				}
				if row.Attempts != 0 {
					t.Fatalf("cancel reap must not charge the retry budget: attempts=%d", row.Attempts)
				}
				if !row.CancelRequested || row.ResultStatus != "" || row.ErrorMessage != "" {
					t.Fatalf("cancelled row = %+v", row)
				}
				if row.CompletedAt.IsZero() || !row.LeaseUntil.IsZero() {
					t.Fatalf("cancelled row must be terminal with the lease released: %+v", row)
				}
				// 保留上一次 owner 便于审计（§9.8 取舍 5，与 running 回收同口径）。
				if row.WorkerID != "w1" {
					t.Fatalf("reap must keep the last owner: %+v", row)
				}
				// 可重入：再扫没有行可迁。
				if n, err := q.ReapExpired(ctxT(t), future()); err != nil || n != 0 {
					t.Fatalf("second reap = %d/%v, want 0/nil", n, err)
				}
				// 终态之后不可再迁移。
				if err := q.AckCancel(ctxT(t), job.ID, "w1"); !errors.Is(err, queue.ErrInvalidTransition) {
					t.Fatalf("AckCancel after reap = %v, want ErrInvalidTransition", err)
				}
			},
		},
	}
}

// RegistryCases 是 WorkerRegistry 的套件。
func RegistryCases() []RegistryCase {
	return []RegistryCase{
		{"RegisterAndListRoundTrip", func(t *testing.T, r queue.WorkerRegistry) {
			ctx := ctxT(t)
			if err := r.Register(ctx, queue.WorkerRecord{WorkerID: "w1", Kernels: []string{"go", "nautilus"},
				Meta: []byte(`{"host":"lab-1"}`)}); err != nil {
				t.Fatal(err)
			}
			list, err := r.List(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(list) != 1 {
				t.Fatalf("list = %d", len(list))
			}
			w := list[0]
			if w.Status != queue.WorkerActive || w.WorkerID != "w1" {
				t.Fatalf("fresh worker = %+v", w)
			}
			if strings.Join(w.Kernels, ",") != "go,nautilus" {
				t.Fatalf("kernels round trip = %v", w.Kernels)
			}
			if !strings.Contains(string(w.Meta), "lab-1") {
				t.Fatalf("meta round trip = %s", w.Meta)
			}
			if w.StartedAt.IsZero() || w.LastHeartbeatAt.IsZero() {
				t.Fatalf("register must stamp started_at/heartbeat: %+v", w)
			}
			if w.DrainRequestedAt != (time.Time{}) {
				t.Fatalf("active worker must not carry drain time: %+v", w)
			}
		}},

		{"RegisterRejectsEmptyID", func(t *testing.T, r queue.WorkerRegistry) {
			if err := r.Register(ctxT(t), queue.WorkerRecord{}); !errors.Is(err, queue.ErrInvalidInput) {
				t.Fatalf("empty worker_id = %v, want ErrInvalidInput", err)
			}
		}},

		{"ReRegisterKeepsStatusAndStartedAt", func(t *testing.T, r queue.WorkerRegistry) {
			// 重复注册 = 覆盖能力/心跳，**不把 draining 改回 active**：已 drain 的
			// worker 不能靠重注册洗回 active（否则 drain 可被绕过）；started_at 也不能被洗成
			// 重注册时刻（2026-10-23 施工 ① 自审修掉的缺陷）。
			ctx := ctxT(t)
			if err := r.Register(ctx, queue.WorkerRecord{WorkerID: "w1", Kernels: []string{"go"}}); err != nil {
				t.Fatal(err)
			}
			first := mustWorker(t, r, "w1")
			if err := r.Drain(ctx, "w1"); err != nil {
				t.Fatal(err)
			}
			drained := mustWorker(t, r, "w1")
			if err := r.Register(ctx, queue.WorkerRecord{WorkerID: "w1", Kernels: []string{"nautilus"}}); err != nil {
				t.Fatal(err)
			}
			again := mustWorker(t, r, "w1")
			if again.Status != queue.WorkerDraining {
				t.Fatalf("re-register washed the drain: status = %q", again.Status)
			}
			if again.StartedAt != first.StartedAt {
				t.Fatalf("re-register reset started_at: %s -> %s", first.StartedAt, again.StartedAt)
			}
			if !again.DrainRequestedAt.Equal(drained.DrainRequestedAt) {
				t.Fatalf("re-register reset drain_requested_at: %s -> %s", drained.DrainRequestedAt, again.DrainRequestedAt)
			}
			if strings.Join(again.Kernels, ",") != "nautilus" {
				t.Fatalf("re-register must update kernels, got %v", again.Kernels)
			}
		}},

		{"HeartbeatRequiresRegistration", func(t *testing.T, r queue.WorkerRegistry) {
			// 未知 worker → ErrWorkerNotFound，**不自动注册**（fail-loud）。
			ctx := ctxT(t)
			if err := r.Heartbeat(ctx, "ghost", time.Now().UTC()); !errors.Is(err, queue.ErrWorkerNotFound) {
				t.Fatalf("heartbeat unknown = %v, want ErrWorkerNotFound", err)
			}
			if _, err := r.List(ctx); err != nil {
				t.Fatal(err)
			}
			if list, _ := r.List(ctx); len(list) != 0 {
				t.Fatalf("heartbeat must not register: %d workers", len(list))
			}
			if err := r.Register(ctx, queue.WorkerRecord{WorkerID: "w1"}); err != nil {
				t.Fatal(err)
			}
			before := mustWorker(t, r, "w1")
			// 显式给的时刻必须**原样**落库（PG 是 timestamptz，只到微秒，所以这里
			// 就把探针截到微秒——否则断言在内存侧绿、PG 侧假红）。
			stamp := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)
			if err := r.Heartbeat(ctx, "w1", stamp); err != nil {
				t.Fatal(err)
			}
			if got := mustWorker(t, r, "w1"); !got.LastHeartbeatAt.Equal(stamp) {
				t.Fatalf("explicit heartbeat time not stored verbatim: %s != %s", got.LastHeartbeatAt, stamp)
			}
			if before.LastHeartbeatAt.Equal(stamp) {
				t.Fatal("harness bug: stamp equals the registration heartbeat")
			}
			if err := r.Heartbeat(ctx, "w1", time.Time{}); err != nil {
				t.Fatalf("zero time = now, must be accepted: %v", err)
			}
			if got := mustWorker(t, r, "w1"); !got.LastHeartbeatAt.After(stamp) {
				t.Fatalf("zero-time heartbeat went backwards: %s", got.LastHeartbeatAt)
			}
		}},

		{"DrainStateMachine", func(t *testing.T, r queue.WorkerRegistry) {
			ctx := ctxT(t)
			if err := r.Drain(ctx, "ghost"); !errors.Is(err, queue.ErrWorkerNotFound) {
				t.Fatalf("drain unknown = %v, want ErrWorkerNotFound", err)
			}
			if err := r.Register(ctx, queue.WorkerRecord{WorkerID: "w1"}); err != nil {
				t.Fatal(err)
			}
			if err := r.Drain(ctx, "w1"); err != nil {
				t.Fatal(err)
			}
			first := mustWorker(t, r, "w1")
			if first.Status != queue.WorkerDraining || first.DrainRequestedAt.IsZero() {
				t.Fatalf("drain -> %+v", first)
			}
			// 重复 drain 幂等，不刷新 drain_requested_at。
			if err := r.Drain(ctx, "w1"); err != nil {
				t.Fatalf("repeat drain = %v, want nil", err)
			}
			if got := mustWorker(t, r, "w1"); got.DrainRequestedAt != first.DrainRequestedAt {
				t.Fatalf("repeat drain moved drain_requested_at")
			}
			// drain 不挡心跳（跑完手上的活还要续租）。
			if err := r.Heartbeat(ctx, "w1", time.Now().UTC()); err != nil {
				t.Fatalf("heartbeat on draining = %v, want nil", err)
			}
			// `offline` 再 drain → ErrInvalidTransition 这条分支施工 ③ 时从 port
			// 不可达（没有 draining → offline 的入口）；R10 的心跳回收补上了入口，
			// 断言见 ReapOfflineMarksStaleWorkersOffline 末尾。
		}},

		{"ListIsEmptyNotNilAndSorted", func(t *testing.T, r queue.WorkerRegistry) {
			ctx := ctxT(t)
			list, err := r.List(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if list == nil {
				t.Fatal("empty registry must list [] not nil (JSON 稳定)")
			}
			for _, id := range []string{"w3", "w1", "w2"} {
				if err := r.Register(ctx, queue.WorkerRecord{WorkerID: id}); err != nil {
					t.Fatal(err)
				}
			}
			list, err = r.List(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(list) != 3 || list[0].WorkerID != "w1" || list[1].WorkerID != "w2" || list[2].WorkerID != "w3" {
				t.Fatalf("list order = %v", list)
			}
		}},

		{"ReapOfflineMarksStaleWorkersOffline", func(t *testing.T, r queue.WorkerRegistry) {
			// R10：心跳超时 → offline。判据只用心跳时间（`<= now-timeout`，含等于），
			// 不删行、不重复计数；`draining` 同样在回收范围内（滚动升级的收尾）。
			ctx := ctxT(t)
			rr := reaperOf(t, r)
			const timeout = time.Minute
			// 截到微秒：PG 的 timestamptz 只到微秒，不截会在内存侧绿、PG 侧假红。
			stale := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)

			if err := r.Register(ctx, queue.WorkerRecord{WorkerID: "w-stale"}); err != nil {
				t.Fatal(err)
			}
			if err := r.Heartbeat(ctx, "w-stale", stale); err != nil {
				t.Fatal(err)
			}
			if err := r.Register(ctx, queue.WorkerRecord{WorkerID: "w-drain"}); err != nil {
				t.Fatal(err)
			}
			if err := r.Drain(ctx, "w-drain"); err != nil {
				t.Fatal(err)
			}
			if err := r.Heartbeat(ctx, "w-drain", stale); err != nil {
				t.Fatal(err)
			}
			if err := r.Register(ctx, queue.WorkerRecord{WorkerID: "w-fresh"}); err != nil {
				t.Fatal(err)
			}

			// 差一秒到阈值：一条都不动（边界是 `<=`，不是 `<`）。
			if n, err := rr.ReapOffline(ctx, stale.Add(timeout-time.Second), timeout); err != nil || n != 0 {
				t.Fatalf("reap before the cutoff = %d/%v, want 0/nil", n, err)
			}
			if got := mustWorker(t, r, "w-stale"); got.Status != queue.WorkerActive {
				t.Fatalf("w-stale = %q before the cutoff", got.Status)
			}

			// 恰好等于阈值：两条过期，fresh 不动。
			if n, err := rr.ReapOffline(ctx, stale.Add(timeout), timeout); err != nil || n != 2 {
				t.Fatalf("reap at the cutoff = %d/%v, want 2/nil", n, err)
			}
			if got := mustWorker(t, r, "w-stale"); got.Status != queue.WorkerOffline {
				t.Fatalf("stale active worker = %q, want offline", got.Status)
			}
			if got := mustWorker(t, r, "w-drain"); got.Status != queue.WorkerOffline {
				t.Fatalf("stale draining worker = %q, want offline（滚动升级的收尾）", got.Status)
			}
			if got := mustWorker(t, r, "w-fresh"); got.Status != queue.WorkerActive {
				t.Fatalf("fresh worker = %q, want active", got.Status)
			}

			// 幂等：已 offline 的不重复计数，也不动 drain_requested_at。
			before := mustWorker(t, r, "w-drain")
			if n, err := rr.ReapOffline(ctx, time.Now().UTC(), timeout); err != nil || n != 0 {
				t.Fatalf("second reap = %d/%v, want 0/nil", n, err)
			}
			if after := mustWorker(t, r, "w-drain"); !after.DrainRequestedAt.Equal(before.DrainRequestedAt) {
				t.Fatalf("reaping moved drain_requested_at: %s -> %s", before.DrainRequestedAt, after.DrainRequestedAt)
			}

			// offline 再 drain → ErrInvalidTransition：施工 ③ 时这条分支从 port
			// 不可达，心跳回收给了它入口（§9.8 取舍 11 的旧口径同时作废）。
			if err := r.Drain(ctx, "w-stale"); !errors.Is(err, queue.ErrInvalidTransition) {
				t.Fatalf("drain on an offline worker = %v, want ErrInvalidTransition", err)
			}
		}},

		{"ReapOfflineRejectsNonPositiveTimeout", func(t *testing.T, r queue.WorkerRegistry) {
			// 阈值属部署配置（R7 同口径）：本包不给默认值，0 不是「用默认」而是
			// 「没配」——拿 0 当兜底会让所有 worker 在一轮内全变 offline。
			rr := reaperOf(t, r)
			for _, timeout := range []time.Duration{0, -time.Second} {
				if n, err := rr.ReapOffline(ctxT(t), time.Now().UTC(), timeout); !errors.Is(err, queue.ErrInvalidInput) || n != 0 {
					t.Fatalf("ReapOffline(timeout=%s) = %d/%v, want 0/ErrInvalidInput", timeout, n, err)
				}
			}
		}},

		{"OfflineWorkerRevivesOnProofOfLifeUnlessDrained", func(t *testing.T, r queue.WorkerRegistry) {
			// offline 是「心跳超时」的**推断**，不是运维意图：心跳 / 重注册都是存活
			// 证明，可以推翻它；请求过 drain 的不能（运维意图优先，否则 drain 可被
			// 一次重注册绕过）。复活必须同时刷新心跳——继承过期心跳会让下一轮回收
			// 立刻再判 offline，恢复形同虚设。
			ctx := ctxT(t)
			rr := reaperOf(t, r)
			const timeout = time.Minute
			stale := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)
			now := time.Now().UTC()

			mkOffline := func(id string, drain bool) {
				t.Helper()
				if err := r.Register(ctx, queue.WorkerRecord{WorkerID: id}); err != nil {
					t.Fatal(err)
				}
				if drain {
					if err := r.Drain(ctx, id); err != nil {
						t.Fatal(err)
					}
				}
				if err := r.Heartbeat(ctx, id, stale); err != nil {
					t.Fatal(err)
				}
				if n, err := rr.ReapOffline(ctx, now, timeout); err != nil || n != 1 {
					t.Fatalf("reap %s = %d/%v, want 1/nil", id, n, err)
				}
				if got := mustWorker(t, r, id); got.Status != queue.WorkerOffline {
					t.Fatalf("%s = %q, want offline", id, got.Status)
				}
			}

			// 存活证明之一：心跳。
			mkOffline("w-alive", false)
			if err := r.Heartbeat(ctx, "w-alive", time.Time{}); err != nil {
				t.Fatalf("heartbeat after timeout: %v", err)
			}
			if got := mustWorker(t, r, "w-alive"); got.Status != queue.WorkerActive {
				t.Fatalf("heartbeat must revive an inferred offline, got %q", got.Status)
			}
			if n, err := rr.ReapOffline(ctx, time.Now().UTC(), timeout); err != nil || n != 0 {
				t.Fatalf("reap right after revival = %d/%v, want 0/nil", n, err)
			}

			// 存活证明之二：重注册（进程崩溃重启的唯一恢复路径）。
			mkOffline("w-restart", false)
			if err := r.Register(ctx, queue.WorkerRecord{WorkerID: "w-restart"}); err != nil {
				t.Fatalf("re-register: %v", err)
			}
			revived := mustWorker(t, r, "w-restart")
			if revived.Status != queue.WorkerActive {
				t.Fatalf("re-register must revive an inferred offline, got %q", revived.Status)
			}
			if time.Since(revived.LastHeartbeatAt) > time.Minute {
				t.Fatalf("revival kept the stale heartbeat: %s", revived.LastHeartbeatAt)
			}
			if n, err := rr.ReapOffline(ctx, time.Now().UTC(), timeout); err != nil || n != 0 {
				t.Fatalf("reap after re-register = %d/%v, want 0/nil", n, err)
			}

			// 已请求 drain：两种存活证明都不得洗回 active。
			mkOffline("w-drained", true)
			if err := r.Heartbeat(ctx, "w-drained", time.Time{}); err != nil {
				t.Fatalf("heartbeat on drained-offline: %v", err)
			}
			if got := mustWorker(t, r, "w-drained"); got.Status != queue.WorkerOffline {
				t.Fatalf("heartbeat washed the drain away: %q", got.Status)
			}
			if err := r.Register(ctx, queue.WorkerRecord{WorkerID: "w-drained"}); err != nil {
				t.Fatalf("re-register drained-offline: %v", err)
			}
			if got := mustWorker(t, r, "w-drained"); got.Status != queue.WorkerOffline {
				t.Fatalf("re-register washed the drain away: %q", got.Status)
			}
		}},
	}
}

// ---------------------------------------------------------------- 读辅助

func mustGet(t *testing.T, q queue.JobQueue, id string) *queue.RunRecord {
	t.Helper()
	row, err := q.Get(ctxT(t), id)
	if err != nil {
		t.Fatalf("Get(%s): %v", id, err)
	}
	return row
}

func mustWorker(t *testing.T, r queue.WorkerRegistry, id string) queue.WorkerRecord {
	t.Helper()
	list, err := r.List(ctxT(t))
	if err != nil {
		t.Fatalf("List workers: %v", err)
	}
	for _, w := range list {
		if w.WorkerID == id {
			return w
		}
	}
	t.Fatalf("worker %s not found in %d rows", id, len(list))
	return queue.WorkerRecord{}
}

func assertSameJob(t *testing.T, a, b *queue.RunRecord) {
	t.Helper()
	if a.ID != b.ID || a.IdempotencyKey != b.IdempotencyKey || a.RequestHash != b.RequestHash {
		t.Fatalf("identity changed on round trip: %s/%s/%s vs %s/%s/%s",
			a.ID, a.IdempotencyKey, a.RequestHash, b.ID, b.IdempotencyKey, b.RequestHash)
	}
	if string(a.RequestJSON) != string(b.RequestJSON) {
		t.Fatalf("request_json changed on round trip:\n %s\n %s", a.RequestJSON, b.RequestJSON)
	}
	if a.Status != b.Status || string(a.ResultStatus) != string(b.ResultStatus) ||
		a.Priority != b.Priority || a.Attempts != b.Attempts || a.MaxAttempts != b.MaxAttempts ||
		a.WorkerID != b.WorkerID || a.CancelRequested != b.CancelRequested ||
		a.ErrorMessage != b.ErrorMessage || a.ResultURI != b.ResultURI {
		t.Fatalf("field changed on round trip:\n %+v\n %+v", a, b)
	}
	if !sameTime(a.LeaseUntil, b.LeaseUntil) || !a.CreatedAt.Equal(b.CreatedAt) ||
		!a.StartedAt.Equal(b.StartedAt) || !a.CompletedAt.Equal(b.CompletedAt) {
		t.Fatalf("timestamp changed on round trip:\n %+v\n %+v", a, b)
	}
}

// sameTime 比两个时间戳，并把「零值 = 未设置」当成一个独立状态：
// PG 用它表达 NULL，内存实现用它表达 time.Time{}。
func sameTime(x, y time.Time) bool {
	if x.IsZero() || y.IsZero() {
		return x.IsZero() == y.IsZero()
	}
	return x.Equal(y)
}

func idsOf(rows []*queue.RunRecord) string {
	parts := make([]string, 0, len(rows))
	for _, r := range rows {
		parts = append(parts, string(r.Status)+"/"+string(r.ResultStatus)+":"+r.ID)
	}
	return strings.Join(parts, " ")
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
