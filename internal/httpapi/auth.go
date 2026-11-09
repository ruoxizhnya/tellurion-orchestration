package httpapi

import (
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/ruoxizhnya/tellurion-kit/auth"
	kithttp "github.com/ruoxizhnya/tellurion-kit/auth/httpadapter"
)

// 鉴权闸的失败分类（docs/api-schema.md §2）。
var (
	ErrUnauthenticated = errors.New("no valid credentials")
	ErrForbidden       = errors.New("insufficient role")
)

// Authorizer 是受保护路由的鉴权闸。nil = 闸未装配，受保护路由必须拒绝服务。
type Authorizer interface {
	Authorize(r *http.Request) error
}

// WithAuthorizer 装配鉴权闸。
func WithAuthorizer(a Authorizer) Option {
	return func(s *Server) { s.authorizer = a }
}

// NewAuthorizer 用 kit 的 net/http 适配构造鉴权闸：token 解析 / kind=access /
// 角色 any-of 都在 kit，本仓只提供 policy 表（policy.go）。
// TokenService 未启用（无 secret + 回环的显式开放）时放行。
func NewAuthorizer(svc *auth.TokenService) Authorizer {
	return kitAuthorizer{svc: svc}
}

type kitAuthorizer struct {
	svc *auth.TokenService
}

func (a kitAuthorizer) Authorize(r *http.Request) error {
	if a.svc == nil || !a.svc.Enabled() {
		return nil
	}
	key := kithttp.RouteKey(r)
	rule, declared := orchestrationPolicy.Rule(key)
	if !declared {
		// 受保护路由 ≡ 策略表由守卫测试钉住；运行时仍 fail-closed。
		return fmt.Errorf("%w: no authorization policy for %q", ErrForbidden, key)
	}
	claims, err := kithttp.Authenticate(a.svc, r)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnauthenticated, err)
	}
	if err := kithttp.Authorize(claims, rule); err != nil {
		if errors.Is(err, kithttp.ErrForbidden) {
			return fmt.Errorf("%w: %v", ErrForbidden, err)
		}
		return fmt.Errorf("%w: %v", ErrUnauthenticated, err)
	}
	return nil
}

// guarded 是全量 R5（SVC-ORCH-G1）的装配点：每一条 /api/* 路由都过鉴权闸。
// 公开面只有进程入口的 /healthz / /readyz，它们不经过本包的 mux。
func (s *Server) guarded(h http.HandlerFunc) http.HandlerFunc {
	return s.requireAuth(h).ServeHTTP
}

// requireAuth 是 fail-closed 闸：未装配 Authorizer → 503；无凭证 → 401；角色不足 → 403。
func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.authorizer == nil {
			writeJSON(w, http.StatusServiceUnavailable, errorBody{
				Error: "endpoints require an authorizer; none is configured",
				Code:  "auth_unavailable",
			})
			return
		}
		switch err := s.authorizer.Authorize(r); {
		case err == nil:
			next.ServeHTTP(w, r)
		case errors.Is(err, ErrUnauthenticated):
			writeJSON(w, http.StatusUnauthorized, errorBody{Error: "authentication required", Code: "unauthenticated"})
		case errors.Is(err, ErrForbidden):
			writeJSON(w, http.StatusForbidden, errorBody{Error: "insufficient role", Code: "forbidden"})
		default:
			log.Printf("httpapi: authorizer error: %v", err)
			writeJSON(w, http.StatusInternalServerError, errorBody{Error: "internal error", Code: "internal"})
		}
	})
}
