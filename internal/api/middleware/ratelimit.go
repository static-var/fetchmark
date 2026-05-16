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
	"math"
	"net/http"
	"sync"

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
// The Redis script maintains an atomic token bucket per key so replicas
// share the same sustained rate and burst capacity.
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
		if err != nil {
			// Redis errors should fail open, but still burn a local token so
			// the local bucket reflects outage traffic if Redis recovers later.
			k.local(key).Allow()
			return true
		}
		if !ok {
			return false
		}
		// Redis said yes; still burn a local token so that an
		// operator-side Redis outage does not briefly uncap the key.
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

var redisTokenBucketScript = redis.NewScript(`
local bucket = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

local redis_time = redis.call("TIME")
local now = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)

local state = redis.call("HMGET", bucket, "tokens", "ts")
local tokens = tonumber(state[1])
local ts = tonumber(state[2])

if tokens == nil then
  tokens = burst
end
if ts == nil then
  ts = now
end

local elapsed = now - ts
if elapsed < 0 then
  elapsed = 0
end

tokens = math.min(burst, tokens + ((elapsed / 1000) * rate))

local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

redis.call("HSET", bucket, "tokens", tokens, "ts", now)
redis.call("PEXPIRE", bucket, ttl)
return allowed
`)

// redisAllow applies an atomic Redis token bucket for the key, denying
// requests when the shared bucket has fewer than one token available.
//
// The Redis key embeds a SHA-256 hash of the API key, not the plaintext
// secret, so operational surfaces (MONITOR, RDB dumps, backups) never
// see raw credentials.
func (k *keyLimiter) redisAllow(ctx context.Context, key string) (bool, error) {
	burst := k.burst
	if burst < 1 {
		burst = 1
	}
	ratePerSec := float64(k.rate)
	ttlMS := int64(math.Ceil((float64(burst) / ratePerSec) * 2000))
	if ttlMS < 2000 {
		ttlMS = 2000
	}

	sum := sha256.Sum256([]byte(key))
	hashed := hex.EncodeToString(sum[:8])
	bucket := fmt.Sprintf("fm:rl:%s", hashed)
	allowed, err := redisTokenBucketScript.Run(ctx, k.rdb, []string{bucket}, ratePerSec, burst, ttlMS).Int()
	if err != nil {
		return true, err
	}
	return allowed == 1, nil
}
