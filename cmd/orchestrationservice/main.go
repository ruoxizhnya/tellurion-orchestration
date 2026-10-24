// Command orchestrationservice 是编排服务的进程装配（X5 的第一个可运行件）。
//
// 今天它做的三件事：连库（DSN 空即拒绝启动）→ 启动迁移（queue.Migrations() 按序
// 幂等执行）→ 提供 jobs 4 端点的 HTTP 面 + 周期回收（租约过期 + 心跳超时）。
//
// 它**不做**的（都在 docs/ARCHITECTURE.md §9.8 如实登记）：
//   - 不执行 run：领取后的内核装配属 core / NT 两侧，本仓没有引擎（§5 边界）；
//   - 不鉴权：R5 未裁，unauthenticated / forbidden 今天不产生；
//   - 不做 worker 进程本身：派活契约未定，心跳超时回收走可选接口
//     queue.WorkerReaper（§9.7 R10），不依赖 worker 进程存在。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"tellurion-orchestration/internal/httpapi"
	"tellurion-orchestration/internal/pgstore"
	"tellurion-orchestration/internal/queue"
)

// Deps 是装配点的依赖，字段一律用**接口类型**：写成具体类型会让
// 「换实现 = 改装配代码」重新出现（core 的 QT-16 为此把 NewEngineDeps 的
// 字段改成了接口）。
type Deps struct {
	Jobs    queue.JobQueue
	Workers queue.WorkerRegistry
	Store   *pgstore.Store
}

// 编译期断言：装配点声明的 port 必须真能被 pgstore 的实现满足。
var (
	_ queue.JobQueue       = (*pgstore.Queue)(nil)
	_ queue.WorkerRegistry = (*pgstore.Registry)(nil)
	_ queue.WorkerReaper   = (*pgstore.Registry)(nil)
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
		startupTO   = flag.Duration("startup-timeout", 30*time.Second, "连库 + 迁移的超时")
	)
	flag.Parse()

	dsn, err := resolveDSN(*dsnFlag)
	if err != nil {
		return err
	}

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
	deps := Deps{Jobs: jobs, Workers: workers, Store: store}

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

	mux := http.NewServeMux()
	mux.Handle("/", httpapi.New(deps.Jobs).Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintln(w, "ok")
	})

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
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

	log.Printf("orchestrationservice: listening on %s (max_pending=%d, reap=%s, heartbeat_timeout=%s)",
		*addr, *depth, *reapEvery, *heartbeatTO)
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
