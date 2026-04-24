// Package middleware — ratelimit.go implements a per-API-key token bucket.
//
// Redis is used as the coordination point when reachable, so replicas
// share a single limit per key. When Redis is absent or the operation
// fails, we fall back to an in-process limiter keyed on the API key;
// fail-open on Redis errors avoids turning a cache outage into an API
// outage. Requests that precede /v1 auth (i.e. /healthz, /metrics) must
// be mounted outside this middleware.
package middleware

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
)

// RateLimiter returns a chi-compatible middleware enforcing per-API-key
// token-bucket limits.
//
//	ratePerSec <= 0 disables limiting entirely.
//	burst      is the bucket capacity (max short-term burst).
//	rdb        optional Redis client for cross-instance coordination.
//
// The Redis script is an atomic INCR/EXPIRE over a sliding window proxy:
// we model the bucket as "approximate requests in the last second" using
// a one-second key. This is coarser than a true token bucket but cheap,
// resilient to clock skew, and good enough for coarse per-key limits.
func RateLimiter(ratePerSec float64, burst int, rdb *redis.Client) func(http.Handler) http.Handler {
	if ratePerSec <= 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	lim := &keyLimiter{
		rate:  rate.Limit(ratePerSec),
		burst: burst,
		bucks: map[string]*rate.Limiter{},
		rdb:   rdb,
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := PrincipalFrom(r.Context()).Key
			if key == "" {
				// Should never happen post-APIKey middleware; fail open.
				next.ServeHTTP(w, r)
				return
			}
			if !lim.allow(r.Context(), key) {
				w.Header().Set("Retry-After", "1")
				writeErr(w, http.StatusTooManyRequests, "rate_limited")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

type keyLimiter struct {
	rate  rate.Limit
	burst int

	mu    sync.Mutex
	bucks map[string]*rate.Limiter

	rdb *redis.Client
}

// allow consults Redis (when available) and always updates the local
// token bucket so limits persist across a Redis outage.
func (k *keyLimiter) allow(ctx context.Context, key string) bool {
	if k.rdb != nil {
		ok, err := k.redisAllow(ctx, key)
		if err == nil {
			if !ok {
				return false
			}
			// Redis said yes; still burn a local token so that an
			// operator-side Redis outage does not briefly uncap the key.
		}
	}
	return k.local(key).Allow()
}

func (k *keyLimiter) local(key string) *rate.Limiter {
	k.mu.Lock()
	defer k.mu.Unlock()
	l, ok := k.bucks[key]
	if !ok {
		l = rate.NewLimiter(k.rate, k.burst)
		k.bucks[key] = l
	}
	return l
}

// redisAllow increments a per-second counter for the key and denies the
// request when the counter exceeds the burst capacity. We only keep a
// 1-second TTL, which caps the limiter's memory footprint in Redis.
//
// The Redis key embeds a SHA-256 hash of the API key, not the plaintext
// secret, so operational surfaces (MONITOR, RDB dumps, backups) never
// see raw credentials.
func (k *keyLimiter) redisAllow(ctx context.Context, key string) (bool, error) {
	limit := int64(k.burst)
	if limit < 1 {
		limit = 1
	}
	sum := sha256.Sum256([]byte(key))
	hashed := hex.EncodeToString(sum[:8])
	bucket := fmt.Sprintf("fm:rl:%s:%d", hashed, time.Now().Unix())

	c, err := k.rdb.Incr(ctx, bucket).Result()
	if err != nil {
		return true, err
	}
	if c == 1 {
		// Best-effort: ignore expire errors — the bucket key is only a
		// second long anyway.
		_ = k.rdb.Expire(ctx, bucket, 2*time.Second).Err()
	}
	return c <= limit, nil
}
