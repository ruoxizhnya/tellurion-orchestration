// Command orchestrationservice 是编排服务的进程装配（X5 的第一个可运行件）。
//
// 今天它做的四件事：连库（DSN 空即拒绝启动）→ 启动迁移（queue.Migrations() 按序
// 幂等执行）→ 全量 HTTP 面（jobs 4 + parents 2 + provenance + artifacts + workers 8
// = 16 条 `/api/*`，加 `/healthz` / `/readyz`）→ 周期回收（租约过期 + 心跳超时）。
//
// 它**不做**的（都在 docs/ARCHITECTURE.md §9.8 如实登记）：
//   - 不执行 run：领取后的内核装配属 core / NT 两侧，本仓没有引擎（§5 边界）；
//   - 不做 worker 进程本身：worker 在 core（`tellurion:cmd/worker`），本仓只提供
//     派活 / 打活 HTTP 面；心跳超时回收走可选接口 queue.WorkerReaper（§9.7 R10）。
//
// 鉴权（全量 R5，SVC-ORCH-G1，2026-11-09）：`/api/*` 16 条路由全部 service 角色；
// `/healthz` / `/readyz` 公开。R5 最小面先由 SVC-ORCH-G2 ④ 覆盖 artifact 写端点。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ruoxizhnya/tellurion-kit/auth"
	kithttp "github.com/ruoxizhnya/tellurion-kit/auth/httpadapter"

	"tellurion-orchestration/internal/httpapi"
	"tellurion-orchestration/internal/pgstore"
	"tellurion-orchestration/internal/queue"
)

// Deps 是装配点的依赖，字段一律用**接口类型**：写成具体类型会让
// 「换实现 = 改装配代码」重新出现（core 的 QT-16 为此把 NewEngineDeps 的
// 字段改成了接口）。
type Deps struct {
	Jobs      queue.JobQueue
	Parents   queue.ParentStore
	Workers   queue.WorkerRegistry
	Artifacts queue.ArtifactStore
	Store     *pgstore.Store
}

// 编译期断言：装配点声明的 port 必须真能被 pgstore 的实现满足。
var (
	_ queue.JobQueue            = (*pgstore.Queue)(nil)
	_ queue.KernelAwareJobQueue = (*pgstore.Queue)(nil)
	_ queue.ParentChildLister   = (*pgstore.Queue)(nil)
	_ queue.ParentStore         = (*pgstore.Parents)(nil)
	_ queue.WorkerRegistry      = (*pgstore.Registry)(nil)
	_ queue.WorkerReaper        = (*pgstore.Registry)(nil)
	_ queue.ArtifactStore       = (*pgstore.Artifacts)(nil)
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "orchestrationservice:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		dsnFlag     = flag.String("dsn", "", "PostgreSQL DSN（空则读环境变量 ORCHESTRATION_DSN；两者都空即拒绝启动）")
		addr        = flag.String("addr", "127.0.0.1:8088", "listen address")
		depth       = flag.Int("max-queue-depth", queue.DefaultMaxQueueDepth, "pending 深度上限（达上限提交回 429；R7：阈值属部署，不进契约）")
		maxAttempts = flag.Int("default-max-attempts", queue.DefaultMaxAttempts, "未指定 max_attempts 时的重试预算")
		reapEvery   = flag.Duration("reap-interval", 30*time.Second, "租约回收周期（0 = 关闭）")
		heartbeatTO = flag.Duration("heartbeat-timeout", 90*time.Second, "超过该时长没有心跳的 worker → offline（0 = 关闭；按 worker 心跳间隔配置）")
		workerLease = flag.Duration("worker-lease", httpapi.DefaultWorkerLease, "worker 领取时写入的租约时长（worker 必须在此之前续租）")
		ratePerSec  = flag.Float64("rate-limit", httpapi.DefaultRateLimitPerSecond, "按调用方身份的令牌桶：每秒令牌数（与兄弟仓同口径，<=0 关闭）")
		rateBurst   = flag.Int("rate-limit-burst", httpapi.DefaultRateLimitBurst, "令牌桶突发上限（<=0 关闭）")
		startupTO   = flag.Duration("startup-timeout", 30*time.Second, "连库 + 迁移的超时")
		jwtSecret   = flag.String("jwt-secret", envFirst("JWT_SECRET", "AUTH_JWT_SECRET"),
			"JWT secret shared with core（空 = 回环开放访问；非回环无 secret 拒绝启动）")
		exposure = flag.String("insecure-exposure", os.Getenv("AUTH_INSECURE_EXPOSURE"),
			"显式开放声明（只接受 loopback-published）；有声明时任何绑定都放开写面")
		allowInsecure = flag.Bool("allow-insecure", envBool("AUTH_ALLOW_INSECURE", true),
			"允许无 secret 的降级/开放态（false = JWT_SECRET 必填，缺则拒绝启动）")
	)
	flag.Parse()

	// 「显式声明」= 部署者主动写下 AUTH_INSECURE_EXPOSURE=loopback-published，或显式把
	// AUTH_ALLOW_INSECURE / -allow-insecure 置为 true。默认值（未设置）**不算**声明。
	allowInsecureDeclared := insecureDeclared(*allowInsecure)

	dsn, err := resolveDSN(*dsnFlag)
	if err != nil {
		return err
	}

	// 三态启动裁决（2026-11-12 收紧，同日修正第 3 态）：
	//   1. 有 secret                → 全闸；
	//   2. 无 secret + 无显式声明    → 只读降级：读端点公开，写端点 503 auth_unavailable；
	//   3. 无 secret + 显式声明      → 任何绑定都放开（写面无鉴权），启动 WARN 同时写出
	//      「写面无鉴权」与绑定地址；非回环绑定额外点名「已对外暴露」。
	// allowInsecure 来自配置（AUTH_ALLOW_INSECURE / -allow-insecure），不再是代码里的常量。
	mode, err := resolveAuthMode(*jwtSecret, *allowInsecure, allowInsecureDeclared, bindHost(*addr), *exposure)
	if err != nil {
		return fmt.Errorf("auth: %w", err)
	}
	logAuthPosture(mode, *addr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startCtx, cancel := context.WithTimeout(ctx, *startupTO)
	defer cancel()

	store, err := pgstore.Open(startCtx, dsn)
	if err != nil {
		return err
	}
	defer store.Close()

	// 启动迁移在监听之前：半套表开始领活比启动失败难查得多（docs/db-schema.md §4）。
	if err := store.Migrate(startCtx); err != nil {
		return err
	}

	workers := store.Registry()
	jobs := store.Queue(workers, pgstore.Options{MaxQueueDepth: *depth, DefaultMaxAttempts: *maxAttempts})
	deps := Deps{
		Jobs:      jobs,
		Parents:   store.Parents(),
		Workers:   workers,
		Artifacts: store.Artifacts(),
		Store:     store,
	}

	// 心跳回收是可选能力：要开就得真能开，装配期 fail-closed，而不是运行到
	// 第一轮才发现注册表根本没实现它（§9.8 取舍 6 的同一类代价）。
	var reaper queue.WorkerReaper
	if *reapEvery > 0 && *heartbeatTO > 0 {
		var ok bool
		reaper, ok = deps.Workers.(queue.WorkerReaper)
		if !ok {
			return fmt.Errorf("worker registry %T does not implement queue.WorkerReaper: "+
				"关掉 -heartbeat-timeout 才能启动（否则 offline 回收永远不会发生）", deps.Workers)
		}
	}
	if *reapEvery > 0 {
		go reap(ctx, deps.Jobs, reaper, *reapEvery, *heartbeatTO)
	}

	// worker 打活面的两条能力断言（X5c）。放在监听之前：带着「以为会按内核
	// 派活 / 以为会写回父汇总」的进程启动，错误只会在第一次领取 / 第一次上报
	// 时才现形，而那时已经有运行中的行了。
	if _, ok := deps.Jobs.(queue.KernelAwareJobQueue); !ok {
		return fmt.Errorf("job queue %T does not implement queue.KernelAwareJobQueue: "+
			"worker 声明的 kernels 无法生效（拒绝启动）", deps.Jobs)
	}
	if _, ok := deps.Jobs.(queue.ParentChildLister); !ok {
		return fmt.Errorf("job queue %T does not implement queue.ParentChildLister: "+
			"子 run 结论无法重算父汇总（拒绝启动）", deps.Jobs)
	}
	if *workerLease <= 0 {
		return fmt.Errorf("-worker-lease must be positive, got %s", *workerLease)
	}

	var tokens *auth.TokenService
	var authorizer httpapi.Authorizer
	if mode == authModeOpen {
		authorizer = httpapi.NewOpenAuthorizer()
	} else {
		secret := ""
		if mode == authModeEnforced {
			secret = *jwtSecret
		}
		tokens = auth.NewTokenService(auth.Config{JWTSecret: []byte(secret)})
		authorizer = httpapi.NewAuthorizer(tokens)
	}

	api := httpapi.New(deps.Jobs,
		httpapi.WithParents(deps.Parents),
		httpapi.WithArtifacts(deps.Artifacts),
		httpapi.WithAuthorizer(authorizer),
		httpapi.WithRateLimit(*ratePerSec, *rateBurst),
		httpapi.WithWorkers(deps.Workers, httpapi.WorkerOptions{Lease: *workerLease})).Handler()

	srv := &http.Server{
		Addr:              *addr,
		Handler:           newMux(api, deps.Store),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("orchestrationservice: shutdown: %v", err)
		}
	}()

	log.Printf("orchestrationservice: listening on %s (max_pending=%d, reap=%s, heartbeat_timeout=%s, auth=%t, auth_scope=%s, rate_limit=%g/s burst=%d)",
		*addr, *depth, *reapEvery, *heartbeatTO, mode == authModeEnforced, httpapi.AuthStatus, *ratePerSec, *rateBurst)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("listen: %w", err)
	}
	return nil
}

// resolveDSN 取 flag，其次取环境变量；两者都空即 fail-closed。
//
// 不给默认 DSN：默认值会替部署方猜库，猜错了服务照样起来「正常工作」。
func resolveDSN(fromFlag string) (string, error) {
	if dsn := strings.TrimSpace(fromFlag); dsn != "" {
		return dsn, nil
	}
	if dsn := strings.TrimSpace(os.Getenv("ORCHESTRATION_DSN")); dsn != "" {
		return dsn, nil
	}
	return "", errors.New("no PostgreSQL DSN: pass -dsn or set ORCHESTRATION_DSN " +
		"(本仓不在启动时退回内存队列——那会把「重启即丢」伪装成 durable 部署)")
}

// readiness 是 /readyz 的最小依赖面：PG 池可连 + run.* 六表已迁移。
// 声明成接口是为了让探针本身可测（不需要真库）。
type readiness interface {
	Ready(ctx context.Context) error
}

// newMux 把公开探针与业务 HTTP 面拼起来（SVC-ORCH-G4 ②）。
//
// 探针分工固定：
//   - `/healthz` = 进程在监听（text/plain "ok"），不查任何依赖；
//   - `/readyz` = 能接活（PG 可连 + 迁移已执行），未就绪 503 且带原因。
//
// 两者都在鉴权之外：部署面必须无凭证可用。抽成函数是为了能测「探针确实
// 注册了、且 readyz 的判定真的接上了」——这不是靠读代码能断言的事实。
func newMux(api http.Handler, ready readiness) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", api)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if ready == nil {
			writeProbe(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "store": "unwired"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := ready.Ready(ctx); err != nil {
			writeProbe(w, http.StatusServiceUnavailable, map[string]string{
				"status": "not_ready", "store": "postgres", "error": err.Error(),
			})
			return
		}
		writeProbe(w, http.StatusOK, map[string]string{"status": "ready", "store": "postgres"})
	})
	return mux
}

func writeProbe(w http.ResponseWriter, code int, body map[string]string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("orchestrationservice: write probe: %v", err)
	}
}

// bindHost 从 -addr 取绑定主机；取不到端口时原样当主机比（空 = 全网卡）。
func bindHost(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

// insecureDeclared 判断部署者是否**主动**声明了放开（第 3 态）。
//
// AUTH_ALLOW_INSECURE 出厂值就是 true（envBool 默认），所以「没设置」不能算声明；
// 只有环境变量实际存在且为 true，或命令行显式给出 -allow-insecure=true 才算。
func insecureDeclared(allowInsecure bool) bool {
	if strings.TrimSpace(os.Getenv("AUTH_ALLOW_INSECURE")) != "" {
		return allowInsecure
	}
	declared := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "allow-insecure" {
			declared = allowInsecure
		}
	})
	return declared
}

// authMode 是本进程的鉴权三态（六仓统一口径）。
type authMode int

const (
	// authModeEnforced：有 JWT_SECRET，所有端点过 kit 闸（401 / 403）。
	authModeEnforced authMode = iota
	// authModeDegraded：无 secret、无显式开放声明。读端点公开，写端点 503。
	authModeDegraded
	// authModeOpen：无 secret + 显式声明（loopback-published 或显式 allow-insecure），
	// 任何绑定都放开写面（启动 WARN 点名「写面无鉴权」）。
	authModeOpen
)

// resolveAuthMode 按三态口径裁决启动模式；返回 error = 拒绝启动。
//
// 第 2 态不看 bindHost：容器里必然绑 0.0.0.0，回环约束在发布层（compose 的
// `127.0.0.1:` 端口映射）。进程无法自证发布层，所以降级态以「写面恒 503」
// 作为安全兜底——即使绑定被误暴露，也只泄露只读面。
//
// 第 3 态（2026-11-12 规则修正）不再因绑定地址拒绝启动：显式声明即允许任何绑定，
// 启动 WARN 负责把「写面无鉴权」与「绑定地址」写清楚；非回环绑定额外点名「已对外暴露」。
func resolveAuthMode(secret string, allowInsecure, allowInsecureDeclared bool, host, exposure string) (authMode, error) {
	if strings.TrimSpace(secret) != "" {
		return authModeEnforced, nil
	}
	switch strings.TrimSpace(exposure) {
	case "":
		if allowInsecureDeclared {
			return authModeOpen, nil
		}
		if !allowInsecure {
			return 0, errors.New("JWT secret missing and insecure mode disabled (AUTH_ALLOW_INSECURE=false) — refusing to start")
		}
		return authModeDegraded, nil
	case kithttp.InsecureExposureLoopbackPublished:
		return authModeOpen, nil
	default:
		return 0, fmt.Errorf("unknown AUTH_INSECURE_EXPOSURE %q (only %q is accepted) — refusing to start",
			exposure, kithttp.InsecureExposureLoopbackPublished)
	}
}

// logAuthPosture 打印三态启动 WARN。第 3 态必须同时写清「写面无鉴权」与绑定地址；
// 非回环绑定额外点名「已对外暴露」。
func logAuthPosture(mode authMode, addr string) {
	host := bindHost(addr)
	switch mode {
	case authModeOpen:
		if kithttp.IsLoopbackHost(host) {
			log.Printf("orchestrationservice: WARNING 写面无鉴权（authentication disabled）：绑定地址 %s（回环）；写端点不再校验 token", addr)
			return
		}
		log.Printf("orchestrationservice: WARNING 写面无鉴权（authentication disabled）：绑定地址 %s，已对外暴露；发布层必须回环或前置鉴权网关", addr)
	case authModeDegraded:
		log.Printf("orchestrationservice: WARNING authentication is not configured (no JWT_SECRET)：读端点公开、写端点 503 auth_unavailable；绑定地址 %s", addr)
	}
}

// envBool 解析布尔环境变量；缺省或无法解析时返回 def。
func envBool(name string, def bool) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return def
	}
	return v
}

// envFirst 按顺序取第一个非空环境变量（core 历史用法 JWT_SECRET、配置口径 AUTH_JWT_SECRET）。
func envFirst(names ...string) string {
	for _, name := range names {
		if v := strings.TrimSpace(os.Getenv(name)); v != "" {
			return v
		}
	}
	return ""
}

// reap 周期回收：租约过期的行（docs/dataflow.md §2 第 7 步）+ 心跳过期的 worker
// （第 8 步的收尾）。两件事同一轮做：它们判的是同一种「owner 已经不在了」，但
// 对象不同——前者是行，后者是派活对象。
//
// 为什么要有它：worker 崩溃后那一行会一直卡在 running，没有 reaper 就没人再派活；
// core 侧的旧做法是**启动时**把 running 刷成 failed（CleanupStaleRunning），
// 多 worker 下等于互相误杀（§9.1 F1），这里按租约过期判定、不按进程生死。
func reap(ctx context.Context, jobs queue.JobQueue, workers queue.WorkerReaper, every, heartbeatTimeout time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			reapOnce(ctx, jobs, workers, heartbeatTimeout, time.Now().UTC())
		}
	}
}

// reapOnce 是一轮回收。抽出来是为了能测：ticker 循环本身没有可断言的行为，
// 「两个回收都真的被调用」才是。
//
// 失败只记日志、不返回错误：下一轮还会再来（这条腿必须可重入）。把它升级成退出
// 会让一次数据库抖动停掉整个服务。
func reapOnce(ctx context.Context, jobs queue.JobQueue, workers queue.WorkerReaper, heartbeatTimeout time.Duration, now time.Time) {
	if n, err := jobs.ReapExpired(ctx, now); err != nil {
		log.Printf("orchestrationservice: reap expired leases: %v", err)
	} else if n > 0 {
		log.Printf("orchestrationservice: reaped %d expired leases", n)
	}
	if workers == nil || heartbeatTimeout <= 0 {
		return
	}
	if n, err := workers.ReapOffline(ctx, now, heartbeatTimeout); err != nil {
		log.Printf("orchestrationservice: reap offline workers: %v", err)
	} else if n > 0 {
		log.Printf("orchestrationservice: marked %d stale workers offline", n)
	}
}
