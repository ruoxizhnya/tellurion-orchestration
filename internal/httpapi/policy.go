package httpapi

import kithttp "github.com/ruoxizhnya/tellurion-kit/auth/httpadapter"

// AuthStatus 是本服务鉴权现状的唯一代码登记（docs/api-schema.md §7）。
//
// "write-endpoint-only" = SVC-ORCH-G2 ④ 的最小面：只保护 artifact 写端点
// `POST /api/runs/jobs/{id}/artifacts`（service 角色）。其余端点仍无鉴权——
// 全量 R5 归 SVC-ORCH-G1（⛔ 待契约形状）。改这里必须同时改文档与 TASKS。
const AuthStatus = "write-endpoint-only"

// orchestrationPolicy 是「端点 × 最低角色」策略表（R5 最小面）。
//
// service 是**服务间**角色：core worker 回传三 hash 时用，不是人角色。
// 角色 any-of ⇒ 只写 service；真人运维今天走 core 的入口，不直连本仓。
var orchestrationPolicy = kithttp.MustNewPolicy(map[string]kithttp.Rule{
	"POST /api/runs/jobs/{id}/artifacts": {Roles: []string{"service"}},
})

// PolicyEntries 返回策略表副本，供守卫测试与路由表对拍。
func PolicyEntries() map[string]kithttp.Rule { return orchestrationPolicy.Entries() }
