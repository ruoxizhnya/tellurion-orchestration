package httpapi

import (
	"sync"
	"time"
)

// 默认限流：50 rps / 突发 200，与 data / risk / analytics / strategy 四仓同口径
// （按调用方身份的令牌桶，进程内、不跨副本）。这是「不把自己打挂」的量级，
// 不是多租户配额；单人自托管下足够，且不引新依赖。
const (
	DefaultRateLimitPerSecond = 50
	DefaultRateLimitBurst     = 200
)

// retryAfterSeconds 是所有 429（`rate_limited` / `queue_full`）的 Retry-After
// 取值。桶按秒回填，1 秒即「下一枚令牌最早出现的时刻」；调用方据此区分
// 「限流/队列满」与「服务坏」。
const retryAfterSeconds = "1"

// rateLimiter 按调用方身份分桶的令牌桶（进程内，不跨副本）。
// 桶懒创建：只服务真实出现过的身份，避免为「身份空间」预分配。
type rateLimiter struct {
	mu      sync.Mutex
	rate    float64
	burst   float64
	buckets map[string]*tokenBucket
}

type tokenBucket struct {
	tokens float64
	last   time.Time
}

func newRateLimiter(perSecond, burst float64) *rateLimiter {
	return &rateLimiter{
		rate:    perSecond,
		burst:   burst,
		buckets: make(map[string]*tokenBucket),
	}
}

// allow 消耗一枚令牌。超限返回 false（调用方映射 429 + Retry-After）。
func (l *rateLimiter) allow(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[key]
	if !ok {
		b = &tokenBucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	} else {
		elapsed := now.Sub(b.last).Seconds()
		if elapsed > 0 {
			b.tokens += elapsed * l.rate
			if b.tokens > l.burst {
				b.tokens = l.burst
			}
			b.last = now
		}
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// rateKey 归一分桶键：没有身份概念（开放访问）的调用方共用一个桶。
func rateKey(principal string) string {
	if principal == "" {
		return "open-access"
	}
	return principal
}
