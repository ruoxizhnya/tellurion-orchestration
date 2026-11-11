package httpapi

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/ruoxizhnya/tellurion-kit/auth"
	kithttp "github.com/ruoxizhnya/tellurion-kit/auth/httpadapter"
)

// 鉴权闸的失败分类（docs/api-schema.md §2）。
var (
	ErrUnauthenticated = errors.New("no valid credentials")
	ErrForbidden       = errors.New("insufficient role")
	// ErrAuthUnavailable = 进程起来了但没配 secret（第 2 态）：读端点公开，
	// 写端点必须 503。绝不能把「没配 secret」当成放行。
	ErrAuthUnavailable = errors.New("authentication is not configured")
)

// Authorizer 是受保护路由的鉴权闸。nil = 闸未装配，受保护路由必须拒绝服务。
//
// 放行时返回调用方身份（限流分桶键）；无身份概念（开放访问）返回空串，
// 由 `rateKey` 归一到 `open-access`。身份只用于限流分桶，不参与鉴权判定
// ——鉴权结论仍由 error 表达。
type Authorizer interface {
	Authorize(r *http.Request) (principal string, err error)
}

// WithAuthorizer 装配鉴权闸。
func WithAuthorizer(a Authorizer) Option {
	return func(s *Server) { s.authorizer = a }
}

// NewAuthorizer 用 kit 的 net/http 适配构造鉴权闸：token 解析 / kind=access /
// 角色 any-of 都在 kit，本仓只提供 policy 表（policy.go）。
//
// TokenService 未启用（无 secret）时**不再放行**：读端点保持公开，写端点
// 一律 503 auth_unavailable（三态中的第 2 态）。要全放行必须显式装配
// NewOpenAuthorizer，且只有启动期已裁决 AUTH_INSECURE_EXPOSURE=loopback-published
// 的进程才允许这样做。
func NewAuthorizer(svc *auth.TokenService) Authorizer {
	return kitAuthorizer{svc: svc}
}

// NewOpenAuthorizer 是显式全放行闸（三态中的第 3 态）。
//
// 只在启动期已经接受 AUTH_INSECURE_EXPOSURE=loopback-published + 回环绑定
// 时装配；装配点必须同时打「写面无鉴权」WARN。不要用它来让「忘了配 secret」
// 的进程跑起来——那是第 2 态，走 NewAuthorizer。
func NewOpenAuthorizer() Authorizer {
	return kitAuthorizer{open: true}
}

type kitAuthorizer struct {
	svc  *auth.TokenService
	open bool
}

func (a kitAuthorizer) Authorize(r *http.Request) (string, error) {
	if a.open {
		return "", nil
	}
	if a.svc == nil || !a.svc.Enabled() {
		if kithttp.IsMutating(r.Method) {
			return "", fmt.Errorf("%w: JWT secret is not configured", ErrAuthUnavailable)
		}
		// 读端点：无 secret 的只读降级仍可用（第 2 态）。
		return "", nil
	}
	key := kithttp.RouteKey(r)
	rule, declared := orchestrationPolicy.Rule(key)
	if !declared {
		// 受保护路由 ≡ 策略表由守卫测试钉住；运行时仍 fail-closed。
		return "", fmt.Errorf("%w: no authorization policy for %q", ErrForbidden, key)
	}
	claims, err := kithttp.Authenticate(a.svc, r)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrUnauthenticated, err)
	}
	if err := kithttp.Authorize(claims, rule); err != nil {
		if errors.Is(err, kithttp.ErrForbidden) {
			return "", fmt.Errorf("%w: %v", ErrForbidden, err)
		}
		return "", fmt.Errorf("%w: %v", ErrUnauthenticated, err)
	}
	return principalOf(claims), nil
}

// principalOf 取限流键：username 优先、缺失退 subject（服务 token 只有 sub），
// 与 data / risk / analytics / strategy 同口径。
func principalOf(claims *auth.Claims) string {
	if claims == nil {
		return ""
	}
	if claims.Username != "" {
		return claims.Username
	}
	return claims.Subject
}

// guarded 是全量 R5（SVC-ORCH-G1）的装配点：每一条 /api/* 路由都过鉴权闸。
// 公开面只有进程入口的 /healthz / /readyz，它们不经过本包的 mux。
func (s *Server) guarded(h http.HandlerFunc) http.HandlerFunc {
	return s.requireAuth(h).ServeHTTP
}

// requireAuth 是 fail-closed 闸：未装配 Authorizer → 503；无凭证 → 401；角色不足 → 403；
// 通过鉴权后按调用方身份限流（超限 → 429 `rate_limited` + `Retry-After`）。
//
// 顺序是**鉴权先于限流**：401 / 403 永远不会被 429 顶替。反过来的话，
// 未登录调用方会被读成「被限流」；探针 `/healthz` `/readyz` 在进程入口的
// mux 上（不进本包），天然不占限流桶。
func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.authorizer == nil {
			writeJSON(w, http.StatusServiceUnavailable, errorBody{
				Error: "endpoints require an authorizer; none is configured",
				Code:  "auth_unavailable",
			})
			return
		}
		switch principal, err := s.authorizer.Authorize(r); {
		case err == nil:
			if s.limiter != nil && !s.limiter.allow(rateKey(principal), time.Now()) {
				w.Header().Set("Retry-After", retryAfterSeconds)
				writeJSON(w, http.StatusTooManyRequests, errorBody{
					Error: "request rate exceeded; retry after " + retryAfterSeconds + "s",
					Code:  "rate_limited",
				})
				return
			}
			next.ServeHTTP(w, r)
		case errors.Is(err, ErrAuthUnavailable):
			writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "authentication is not configured", Code: "auth_unavailable"})
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
