// Command orchestrationservice 是编排服务的进程装配（X5 的第一个可运行件）。
//
// 今天它做的三件事：连库（DSN 空即拒绝启动）→ 启动迁移（queue.Migrations() 按序
// 幂等执行）→ 提供 jobs 4 端点的 HTTP 面 + 租约回收的周期任务。
//
// 它**不做**的（都在 docs/ARCHITECTURE.md §9.8 如实登记）：
//   - 不执行 run：领取后的内核装配属 core / NT 两侧，本仓没有引擎（§5 边界）；
//   - 不鉴权：R5 未裁，unauthenticated / forbidden 今天不产生；
//   - 不把心跳超时的 worker 推到 offline：冻结方法集里没有这条迁移。
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

	if *reapEvery > 0 {
		go reap(ctx, deps.Jobs, *reapEvery)
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

	log.Printf("orchestrationservice: listening on %s (max_pending=%d, reap=%s)", *addr, *depth, *reapEvery)
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

// reap 周期回收租约已过的 running 行（docs/dataflow.md §2 第 7 步）。
//
// 为什么要有它：worker 崩溃后那一行会一直卡在 running，没有 reaper 就没人再派活；
// core 侧的旧做法是**启动时**把 running 刷成 failed（CleanupStaleRunning），
// 多 worker 下等于互相误杀（§9.1 F1），这里按租约过期判定、不按进程生死。
func reap(ctx context.Context, jobs queue.JobQueue, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := jobs.ReapExpired(ctx, time.Now().UTC())
			if err != nil {
				// 回收失败只记日志：下一轮还会再来（这条腿必须可重入）。
				// 把它升级成退出会让一次数据库抖动停掉整个服务。
				log.Printf("orchestrationservice: reap expired: %v", err)
				continue
			}
			if n > 0 {
				log.Printf("orchestrationservice: reaped %d expired leases", n)
			}
		}
	}
}
