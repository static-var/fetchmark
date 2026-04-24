package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
