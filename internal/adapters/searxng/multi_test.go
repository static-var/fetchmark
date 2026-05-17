package searxng

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/search"
)

func TestMultiClient_FailsOverToHealthyInstance(t *testing.T) {
	var badHits, goodHits int64

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&badHits, 1)
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(bad.Close)

	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&goodHits, 1)
		if strings.HasPrefix(r.URL.Path, "/search") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(fixture))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(good.Close)

	mc, err := NewMulti([]string{bad.URL, good.URL}, bad.Client())
	if err != nil {
		t.Fatalf("NewMulti: %v", err)
	}

	hits, err := mc.Search(context.Background(), search.Query{Q: "golang"})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) == 0 {
		t.Fatalf("want hits from healthy instance, got 0")
	}
	if atomic.LoadInt64(&badHits) == 0 {
		t.Errorf("bad instance was never tried")
	}
	if atomic.LoadInt64(&goodHits) == 0 {
		t.Errorf("good instance was never tried")
	}

	// Second call within the cooldown window should skip the bad
	// instance entirely.
	before := atomic.LoadInt64(&badHits)
	if _, err := mc.Search(context.Background(), search.Query{Q: "golang"}); err != nil {
		t.Fatalf("second Search: %v", err)
	}
	if after := atomic.LoadInt64(&badHits); after != before {
		t.Errorf("bad instance hit during cooldown: before=%d after=%d", before, after)
	}
}

func TestMultiClient_RoundRobinsBetweenHealthy(t *testing.T) {
	var a, b int64
	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&a, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fixture))
	}))
	t.Cleanup(srvA.Close)
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&b, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fixture))
	}))
	t.Cleanup(srvB.Close)

	mc, err := NewMulti([]string{srvA.URL, srvB.URL}, srvA.Client())
	if err != nil {
		t.Fatalf("NewMulti: %v", err)
	}
	for i := 0; i < 6; i++ {
		if _, err := mc.Search(context.Background(), search.Query{Q: fmt.Sprintf("q%d", i)}); err != nil {
			t.Fatalf("Search: %v", err)
		}
	}
	ga, gb := atomic.LoadInt64(&a), atomic.LoadInt64(&b)
	if ga == 0 || gb == 0 {
		t.Fatalf("round-robin imbalance: a=%d b=%d", ga, gb)
	}
}

func TestMultiClient_DoesNotCooldownOnClientSearchError(t *testing.T) {
	var badRequestHits int64
	badRequest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&badRequestHits, 1)
		http.Error(w, "bad engine", http.StatusBadRequest)
	}))
	t.Cleanup(badRequest.Close)

	var healthyHits int64
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&healthyHits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fixture))
	}))
	t.Cleanup(healthy.Close)

	mc, err := NewMultiWithCooldown([]string{badRequest.URL, healthy.URL}, badRequest.Client(), time.Minute)
	if err != nil {
		t.Fatalf("NewMultiWithCooldown: %v", err)
	}

	if _, err := mc.Search(context.Background(), search.Query{Q: "golang"}); err == nil {
		t.Fatal("expected client error from first SearXNG instance")
	}
	if got := atomic.LoadInt64(&healthyHits); got != 0 {
		t.Fatalf("non-retryable client error should not fail over; healthy hits = %d", got)
	}

	_, _ = mc.Search(context.Background(), search.Query{Q: "golang"})
	_, _ = mc.Search(context.Background(), search.Query{Q: "golang"})
	if got := atomic.LoadInt64(&badRequestHits); got != 2 {
		t.Fatalf("client-error instance should not be cooled down; hits = %d, want 2", got)
	}
}

func TestMultiClient_PingAnyHealthy(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	t.Cleanup(bad.Close)
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(good.Close)

	mc, err := NewMulti([]string{bad.URL, good.URL}, bad.Client())
	if err != nil {
		t.Fatalf("NewMulti: %v", err)
	}
	if err := mc.Ping(context.Background()); err != nil {
		t.Fatalf("Ping should pass with one healthy instance: %v", err)
	}
}

func TestMultiClient_RejectsEmpty(t *testing.T) {
	if _, err := NewMulti(nil, nil); err == nil {
		t.Fatal("expected error on empty bases")
	}
}

// TestMultiClient_CooldownExpiresAfterDuration proves the cooldown
// window honors the value passed to NewMultiWithCooldown. With a 50ms
// cooldown, a failed instance must be retried on a subsequent Search
// after the window elapses.
func TestMultiClient_CooldownExpiresAfterDuration(t *testing.T) {
	var attempts int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&attempts, 1)
		if n == 1 {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/search") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(fixture))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	mc, err := NewMultiWithCooldown([]string{srv.URL}, http.DefaultClient, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("ctor: %v", err)
	}

	// First call fails; instance cools down.
	if _, err := mc.Search(context.Background(), search.Query{Q: "x"}); err == nil {
		t.Fatal("expected first call to fail")
	}

	requireEventually(t, time.Second, func() bool {
		_, err := mc.Search(context.Background(), search.Query{Q: "x"})
		return err == nil
	})
}

// TestMultiClient_NonPositiveCooldownFallsBack guards the safety net
// that prevents FM_SEARXNG_COOLDOWN=0 (or negative) from silently
// disabling failover — the ctor clamps up to the default instead.
func TestMultiClient_NonPositiveCooldownFallsBack(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	mc, err := NewMultiWithCooldown([]string{srv.URL}, http.DefaultClient, 0)
	if err != nil {
		t.Fatalf("ctor: %v", err)
	}
	if mc.cooldownDur != defaultInstanceCooldown {
		t.Fatalf("cooldownDur = %v, want %v", mc.cooldownDur, defaultInstanceCooldown)
	}
}
