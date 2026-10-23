// Package orchestration 是 tellurion-orchestration 的根包。
//
// 本仓按能力拆子包，目前只有：
//   - internal/queue：JobQueue / WorkerRegistry 两个 port、内存实现、run schema DDL。
//
// 根包本身不放实现，只用来让 `go build ./...` 与 `go list ./...` 识别本 module。
// 新增能力时按同样规则开子包，不要把所有代码堆在根包。
package orchestration
