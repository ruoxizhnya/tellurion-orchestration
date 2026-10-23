// Package orchestration 是 tellurion-orchestration 的根包。
//
// 本仓按能力拆子包，目前只有：
//   - internal/queue：JobQueue / WorkerRegistry 两个 port、内存实现、run schema DDL。
//   - internal/pgstore：同一对 port 的 PG 实现 + 连接池与启动迁移（第三方驱动只进这个包）。
//   - internal/qtest：两个实现共用的语义套件（每个实现 41 条用例）。
//   - internal/testdb：PG 腿的测试 harness（跨进程排队）。
//   - internal/httpapi：`/api/runs/*` 的 HTTP 面。
//   - cmd/orchestrationservice：进程装配（迁移在监听之前）。
//
// 根包本身不放实现，只用来让 `go build ./...` 与 `go list ./...` 识别本 module。
// 新增能力时按同样规则开子包，不要把所有代码堆在根包。
package orchestration
