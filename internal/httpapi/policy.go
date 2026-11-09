package httpapi

import kithttp "github.com/ruoxizhnya/tellurion-kit/auth/httpadapter"

// AuthStatus 是本服务鉴权现状的唯一代码登记（docs/api-schema.md §7）。
//
// "all-endpoints-service" = SVC-ORCH-G1 全量 R5：本服务是**服务间**面，
// `/api/*` 下每一条路由都要 service 凭证；公开面只有进程入口的
// `/healthz` / `/readyz`（`cmd/orchestrationservice` 的 mux，不进本包）。
// 改这里必须同时改文档与 TASKS。
const AuthStatus = "all-endpoints-service"

// orchestrationPolicy 是「端点 × 最低角色」策略表（全量 R5）。
//
// service 是**服务间**角色：core worker（领取 / 续租 / 上报 / artifact）与
// analytics（父作业提交 / 查询）都用它；不是人角色。真人今天走 core 的入口，
// 不直连本仓。策略表 ≡ 路由表由守卫测试钉住，运行时缺条目恒 fail-closed。
var orchestrationPolicy = kithttp.MustNewPolicy(map[string]kithttp.Rule{
	"POST /api/runs/jobs":                   {Roles: []string{"service"}},
	"GET /api/runs/jobs":                    {Roles: []string{"service"}},
	"GET /api/runs/jobs/{id}":               {Roles: []string{"service"}},
	"DELETE /api/runs/jobs/{id}":            {Roles: []string{"service"}},
	"POST /api/runs/parents":                {Roles: []string{"service"}},
	"GET /api/runs/parents/{id}":            {Roles: []string{"service"}},
	"GET /api/runs/{rest...}":               {Roles: []string{"service"}},
	"POST /api/runs/jobs/{id}/artifacts":    {Roles: []string{"service"}},
	"GET /api/runs/workers":                 {Roles: []string{"service"}},
	"POST /api/runs/workers":                {Roles: []string{"service"}},
	"POST /api/runs/workers/{id}/heartbeat": {Roles: []string{"service"}},
	"POST /api/runs/workers/{id}/claim":     {Roles: []string{"service"}},
	"POST /api/runs/workers/{id}/drain":     {Roles: []string{"service"}},
	"POST /api/runs/jobs/{id}/lease":        {Roles: []string{"service"}},
	"POST /api/runs/jobs/{id}/result":       {Roles: []string{"service"}},
	"POST /api/runs/jobs/{id}/cancel-ack":   {Roles: []string{"service"}},
})

// PolicyEntries 返回策略表副本，供守卫测试与路由表对拍。
func PolicyEntries() map[string]kithttp.Rule { return orchestrationPolicy.Entries() }
