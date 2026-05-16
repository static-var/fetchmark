package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TestRateLimiter_BurstEnforcedInProcess exercises the fallback (no
// Redis) local-bucket path: after draining the burst, the limiter must
// reject subsequent calls with 429.
func TestRateLimiter_BurstEnforcedInProcess(t *testing.T) {
	// 0 req/s sustained but a burst of 2 — after two calls the bucket
	// is empty and refills at 0/s, so every call after that is denied.
	h := RateLimiter(0.0001, 2, nil)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	call := func() int {
		ctx := context.WithValue(context.Background(), authKey, Principal{Key: "k1"})
		req := httptest.NewRequest("POST", "/v1/search", strings.NewReader("{}")).WithContext(ctx)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	if got := call(); got != http.StatusOK {
		t.Fatalf("call 1 status = %d", got)
	}
	if got := call(); got != http.StatusOK {
		t.Fatalf("call 2 status = %d", got)
	}
	if got := call(); got != http.StatusTooManyRequests {
		t.Fatalf("call 3 status = %d, want 429", got)
	}
}

// TestRateLimiter_Disabled confirms a rate of 0 means "no limiting".
func TestRateLimiter_Disabled(t *testing.T) {
	h := RateLimiter(0, 0, nil)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	ctx := context.WithValue(context.Background(), authKey, Principal{Key: "k1"})
	for i := 0; i < 20; i++ {
		req := httptest.NewRequest("POST", "/v1/search", strings.NewReader("{}")).WithContext(ctx)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("call %d denied at code %d", i, rec.Code)
		}
	}
}

// TestRateLimiter_RedisAllowDeny runs the middleware against an
// in-memory Redis and asserts the allow/deny semantics the cross-
// instance coordinator is supposed to enforce: same key exhausts the
// burst at once, a second key still passes.
func TestRateLimiter_RedisAllowDeny(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	// Low refill rate keeps the burst deterministic: key "kA" should go
	// 2 OK, 1 denied; key "kB" still OK.
	h := RateLimiter(0.5, 2, rdb)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	call := func(key string) int {
		ctx := context.WithValue(context.Background(), authKey, Principal{Key: key})
		req := httptest.NewRequest("POST", "/v1/search", strings.NewReader("{}")).WithContext(ctx)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	if got := call("kA"); got != http.StatusOK {
		t.Fatalf("kA call 1: %d", got)
	}
	if got := call("kA"); got != http.StatusOK {
		t.Fatalf("kA call 2: %d", got)
	}
	if got := call("kA"); got != http.StatusTooManyRequests {
		t.Fatalf("kA call 3: %d, want 429", got)
	}
	if got := call("kB"); got != http.StatusOK {
		t.Fatalf("different key must be unaffected; kB: %d", got)
	}
}

func TestRateLimiter_RedisErrorFailsOpenAfterLocalBucketExhausted(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:        mr.Addr(),
		DialTimeout: 10 * time.Millisecond,
		MaxRetries:  -1,
	})
	t.Cleanup(func() { _ = rdb.Close() })
	mr.Close()

	h := RateLimiter(0.0001, 2, rdb)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	call := func() int {
		ctx := context.WithValue(context.Background(), authKey, Principal{Key: "outage-key"})
		req := httptest.NewRequest("POST", "/v1/search", strings.NewReader("{}")).WithContext(ctx)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	if got := call(); got != http.StatusOK {
		t.Fatalf("call 1 status = %d", got)
	}
	if got := call(); got != http.StatusOK {
		t.Fatalf("call 2 status = %d", got)
	}
	if got := call(); got != http.StatusOK {
		t.Fatalf("call 3 status = %d, want fail-open 200", got)
	}
}

func TestRateLimiter_RedisTokenBucketRefillsByRate(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	newHandler := func() http.Handler {
		return RateLimiter(0.5, 2, rdb)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
	}
	call := func(h http.Handler) int {
		ctx := context.WithValue(context.Background(), authKey, Principal{Key: "slow-key"})
		req := httptest.NewRequest("POST", "/v1/search", strings.NewReader("{}")).WithContext(ctx)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	h := newHandler()

	if got := call(h); got != http.StatusOK {
		t.Fatalf("call 1 status = %d", got)
	}
	if got := call(h); got != http.StatusOK {
		t.Fatalf("call 2 status = %d", got)
	}
	if got := call(newHandler()); got != http.StatusTooManyRequests {
		t.Fatalf("call 3 status = %d, want 429", got)
	}
	time.Sleep(1100 * time.Millisecond)
	if got := call(newHandler()); got != http.StatusTooManyRequests {
		t.Fatalf("call after 1.1s status = %d, want 429", got)
	}
	time.Sleep(1 * time.Second)
	if got := call(newHandler()); got != http.StatusOK {
		t.Fatalf("call after 2.1s status = %d, want 200", got)
	}
}
