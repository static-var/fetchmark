package searxng

import (
	"context"
	"errors"
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

func TestMultiClient_FailsOverOnDegradedEmpty(t *testing.T) {
	var degradedHits, healthyHits int64
	degraded := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&degradedHits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[],"unresponsive_engines":[["duckduckgo","Suspended: CAPTCHA"]]}`))
	}))
	t.Cleanup(degraded.Close)
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&healthyHits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fixture))
	}))
	t.Cleanup(healthy.Close)

	mc, err := NewMulti([]string{degraded.URL, healthy.URL}, degraded.Client())
	if err != nil {
		t.Fatalf("NewMulti: %v", err)
	}
	batch, err := mc.SearchBatch(context.Background(), search.Query{Q: "golang"})
	if err != nil {
		t.Fatalf("SearchBatch: %v", err)
	}
	if batch.Status != search.BatchPartial || len(batch.Hits) == 0 {
		t.Fatalf("batch = %+v, want partial fallback hits with prior diagnostics", batch)
	}
	if len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Source != "duckduckgo" {
		t.Fatalf("fallback diagnostics = %+v", batch.Diagnostics)
	}
	if atomic.LoadInt64(&degradedHits) != 1 || atomic.LoadInt64(&healthyHits) != 1 {
		t.Fatalf("attempts degraded=%d healthy=%d, want 1 each", degradedHits, healthyHits)
	}
}

func TestMultiClient_PriorDegradationPreventsAuthoritativeEmptyStatus(t *testing.T) {
	degraded := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[],"unresponsive_engines":[["duckduckgo","timeout"]]}`))
	}))
	t.Cleanup(degraded.Close)
	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[],"unresponsive_engines":[]}`))
	}))
	t.Cleanup(empty.Close)

	mc, err := NewMulti([]string{degraded.URL, empty.URL}, degraded.Client())
	if err != nil {
		t.Fatalf("NewMulti: %v", err)
	}
	batch, err := mc.SearchBatch(context.Background(), search.Query{Q: "obscure query"})
	if err != nil {
		t.Fatalf("SearchBatch: %v", err)
	}
	if batch.Status != search.BatchDegradedEmpty || len(batch.Hits) != 0 || len(batch.Diagnostics) != 1 {
		t.Fatalf("batch = %+v, want degraded empty with prior diagnostics", batch)
	}
}

func TestMultiClient_PriorFailurePreventsAuthoritativeEmptyStatus(t *testing.T) {
	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusBadGateway)
	}))
	t.Cleanup(failed.Close)
	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[],"unresponsive_engines":[]}`))
	}))
	t.Cleanup(empty.Close)

	mc, err := NewMulti([]string{failed.URL, empty.URL}, failed.Client())
	if err != nil {
		t.Fatalf("NewMulti: %v", err)
	}
	batch, err := mc.SearchBatch(context.Background(), search.Query{Q: "obscure query"})
	if err != nil {
		t.Fatalf("SearchBatch: %v", err)
	}
	if batch.Status != search.BatchDegradedEmpty || len(batch.Hits) != 0 {
		t.Fatalf("batch = %+v, want degraded empty after incomplete pool coverage", batch)
	}
	if len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Instance != failed.URL || batch.Diagnostics[0].Source != "instance" || batch.Diagnostics[0].Reason != "http_502" {
		t.Fatalf("failure diagnostics = %+v", batch.Diagnostics)
	}
}

func TestMultiClient_PriorFailureMakesFallbackHitsPartial(t *testing.T) {
	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":`))
	}))
	t.Cleanup(failed.Close)
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fixture))
	}))
	t.Cleanup(healthy.Close)

	mc, err := NewMulti([]string{failed.URL, healthy.URL}, failed.Client())
	if err != nil {
		t.Fatalf("NewMulti: %v", err)
	}
	batch, err := mc.SearchBatch(context.Background(), search.Query{Q: "golang"})
	if err != nil {
		t.Fatalf("SearchBatch: %v", err)
	}
	if batch.Status != search.BatchPartial || len(batch.Hits) == 0 {
		t.Fatalf("batch = %+v, want partial fallback hits", batch)
	}
	if len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Reason != "malformed_response" {
		t.Fatalf("failure diagnostics = %+v", batch.Diagnostics)
	}
}

func TestMultiClient_AcceptsAuthoritativeEmptyWithoutFailover(t *testing.T) {
	var fallbackHits int64
	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[],"unresponsive_engines":[]}`))
	}))
	t.Cleanup(empty.Close)
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&fallbackHits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fixture))
	}))
	t.Cleanup(fallback.Close)

	mc, err := NewMulti([]string{empty.URL, fallback.URL}, empty.Client())
	if err != nil {
		t.Fatalf("NewMulti: %v", err)
	}
	batch, err := mc.SearchBatch(context.Background(), search.Query{Q: "intentionally empty"})
	if err != nil {
		t.Fatalf("SearchBatch: %v", err)
	}
	if batch.Status != search.BatchAuthoritativeEmpty || len(batch.Hits) != 0 {
		t.Fatalf("batch = %+v", batch)
	}
	if atomic.LoadInt64(&fallbackHits) != 0 {
		t.Fatalf("fallback hits = %d, want 0", fallbackHits)
	}
}

func TestMultiClient_AllDegradedPreservesCompatibleEmptyResult(t *testing.T) {
	newDegraded := func(engine string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"results":[],"unresponsive_engines":[[%q,"timeout"]]}`, engine)
		}))
	}
	a := newDegraded("mwmbl")
	b := newDegraded("wiby")
	t.Cleanup(a.Close)
	t.Cleanup(b.Close)

	mc, err := NewMulti([]string{a.URL, b.URL}, a.Client())
	if err != nil {
		t.Fatalf("NewMulti: %v", err)
	}
	batch, err := mc.SearchBatch(context.Background(), search.Query{Q: "obscure query"})
	if err != nil {
		t.Fatalf("SearchBatch: %v", err)
	}
	if batch.Status != search.BatchDegradedEmpty || len(batch.Hits) != 0 || len(batch.Diagnostics) != 2 {
		t.Fatalf("batch = %+v", batch)
	}

	hits, err := mc.Search(context.Background(), search.Query{Q: "obscure query"})
	if err != nil || len(hits) != 0 {
		t.Fatalf("legacy Search hits=%+v err=%v, want compatible empty success", hits, err)
	}
}

func TestMultiClient_PartialResultsStopFailover(t *testing.T) {
	var fallbackHits int64
	partial := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"url":"https://mwmbl.org","title":"Mwmbl"}],"unresponsive_engines":[["brave","timeout"]]}`))
	}))
	t.Cleanup(partial.Close)
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&fallbackHits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fixture))
	}))
	t.Cleanup(fallback.Close)

	mc, err := NewMulti([]string{partial.URL, fallback.URL}, partial.Client())
	if err != nil {
		t.Fatalf("NewMulti: %v", err)
	}
	batch, err := mc.SearchBatch(context.Background(), search.Query{Q: "open search"})
	if err != nil {
		t.Fatalf("SearchBatch: %v", err)
	}
	if batch.Status != search.BatchPartial || len(batch.Hits) != 1 {
		t.Fatalf("batch = %+v", batch)
	}
	if atomic.LoadInt64(&fallbackHits) != 0 {
		t.Fatalf("fallback hits = %d, want 0", fallbackHits)
	}
}

func TestMultiClient_DoesNotCooldownNonRetryableSearchStatus(t *testing.T) {
	var badHits int64
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&badHits, 1)
		http.Error(w, "bad query", http.StatusBadRequest)
	}))
	t.Cleanup(bad.Close)
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fixture))
	}))
	t.Cleanup(good.Close)

	mc, err := NewMultiWithCooldown([]string{bad.URL, good.URL}, bad.Client(), time.Minute)
	if err != nil {
		t.Fatalf("NewMultiWithCooldown: %v", err)
	}

	mc.failures[0] = 2
	mc.cooldown[0] = time.Now().Add(-time.Second)
	hits, err := mc.Search(context.Background(), search.Query{Q: "golang"})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("want hits from fallback instance")
	}
	if atomic.LoadInt64(&badHits) != 1 {
		t.Fatalf("bad instance hits = %d, want 1", badHits)
	}
	if !mc.cooldown[0].IsZero() {
		t.Fatalf("4xx search status set cooldown until %s", mc.cooldown[0])
	}
	if mc.failures[0] != 0 {
		t.Fatalf("responsive 4xx left transport failure streak = %d", mc.failures[0])
	}
}

func TestMultiClient_CooldownsRetryableSearchStatus(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	t.Cleanup(bad.Close)
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fixture))
	}))
	t.Cleanup(good.Close)

	mc, err := NewMultiWithCooldown([]string{bad.URL, good.URL}, bad.Client(), time.Minute)
	if err != nil {
		t.Fatalf("NewMultiWithCooldown: %v", err)
	}

	if _, err := mc.Search(context.Background(), search.Query{Q: "golang"}); err != nil {
		t.Fatalf("Search: %v", err)
	}
	if !mc.cooldown[0].After(time.Now()) {
		t.Fatalf("5xx search status did not set a future cooldown: %s", mc.cooldown[0])
	}
}

func TestMultiClient_CooldownsForbiddenStatus(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	t.Cleanup(bad.Close)
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fixture))
	}))
	t.Cleanup(good.Close)

	mc, err := NewMultiWithCooldown([]string{bad.URL, good.URL}, bad.Client(), time.Minute)
	if err != nil {
		t.Fatalf("NewMultiWithCooldown: %v", err)
	}
	if _, err := mc.Search(context.Background(), search.Query{Q: "golang"}); err != nil {
		t.Fatalf("Search: %v", err)
	}
	if !mc.cooldown[0].After(time.Now()) {
		t.Fatalf("403 search status did not set a future cooldown: %s", mc.cooldown[0])
	}
}

func TestMultiClient_AllCoolingDoesNotBypassCooldown(t *testing.T) {
	var attempts int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&attempts, 1)
		w.Header().Set("Retry-After", "120")
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	t.Cleanup(server.Close)

	mc, err := NewMultiWithCooldown([]string{server.URL}, server.Client(), time.Minute)
	if err != nil {
		t.Fatalf("NewMultiWithCooldown: %v", err)
	}
	if _, err := mc.SearchBatch(context.Background(), search.Query{Q: "golang"}); err == nil {
		t.Fatal("first search should fail")
	}
	_, err = mc.SearchBatch(context.Background(), search.Query{Q: "golang"})
	var cooling *PoolCoolingError
	if !errors.As(err, &cooling) {
		t.Fatalf("second error = %T %v, want *PoolCoolingError", err, err)
	}
	if cooling.RetryAfter < 119*time.Second || cooling.RetryAfter > 120*time.Second {
		t.Fatalf("RetryAfter = %v, want about 120s", cooling.RetryAfter)
	}
	if got := atomic.LoadInt64(&attempts); got != 1 {
		t.Fatalf("attempts = %d, want 1 while circuit is open", got)
	}
}

func TestMultiClient_TransportBackoffGrowsAfterRepeatedFailures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)

	mc, err := NewMultiWithCooldown([]string{server.URL}, server.Client(), 10*time.Second)
	if err != nil {
		t.Fatalf("NewMultiWithCooldown: %v", err)
	}
	now := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	mc.now = func() time.Time { return now }
	mc.jitter = func(duration time.Duration, _ string, _ int) time.Duration { return duration }

	if _, err := mc.SearchBatch(context.Background(), search.Query{Q: "first"}); err == nil {
		t.Fatal("first search should fail")
	}
	if got := mc.cooldown[0].Sub(now); got != 10*time.Second {
		t.Fatalf("first cooldown = %v, want 10s", got)
	}
	now = now.Add(11 * time.Second)
	if _, err := mc.SearchBatch(context.Background(), search.Query{Q: "second"}); err == nil {
		t.Fatal("second search should fail")
	}
	if got := mc.cooldown[0].Sub(now); got != 20*time.Second {
		t.Fatalf("second cooldown = %v, want 20s", got)
	}
}

func TestMultiClient_RepeatedDegradedEmptyOpensQualityCircuit(t *testing.T) {
	var degradedAttempts, healthyAttempts int64
	var recovered atomic.Bool
	degraded := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&degradedAttempts, 1)
		w.Header().Set("Content-Type", "application/json")
		if recovered.Load() {
			_, _ = w.Write([]byte(fixture))
			return
		}
		_, _ = w.Write([]byte(`{"results":[],"unresponsive_engines":[["brave","Suspended: CAPTCHA"]]}`))
	}))
	t.Cleanup(degraded.Close)
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&healthyAttempts, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fixture))
	}))
	t.Cleanup(healthy.Close)

	mc, err := NewMultiWithCooldown([]string{degraded.URL, healthy.URL}, degraded.Client(), time.Minute)
	if err != nil {
		t.Fatalf("NewMultiWithCooldown: %v", err)
	}
	now := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	mc.now = func() time.Time { return now }
	mc.jitter = func(duration time.Duration, _ string, _ int) time.Duration { return duration }
	for attempt := 0; attempt < 2; attempt++ {
		atomic.StoreUint64(&mc.rr, 0)
		batch, err := mc.SearchBatch(context.Background(), search.Query{Q: "open search"})
		if err != nil || batch.Status != search.BatchPartial || len(batch.Hits) == 0 {
			t.Fatalf("attempt %d batch=%+v err=%v", attempt+1, batch, err)
		}
	}
	if got := mc.qualityCooldown[0].Sub(now); got != time.Minute {
		t.Fatalf("quality cooldown = %v, want 1m", got)
	}
	if score := mc.qualityEWMA[0]; score <= 0 || score >= 1 {
		t.Fatalf("quality EWMA = %v, want 0..1", score)
	}

	atomic.StoreUint64(&mc.rr, 0)
	batch, err := mc.SearchBatch(context.Background(), search.Query{Q: "open search"})
	if err != nil || batch.Status != search.BatchHealthy || len(batch.Hits) == 0 {
		t.Fatalf("circuit-skip batch=%+v err=%v", batch, err)
	}
	if got := atomic.LoadInt64(&degradedAttempts); got != 2 {
		t.Fatalf("degraded attempts = %d, want 2 while quality circuit is open", got)
	}
	if got := atomic.LoadInt64(&healthyAttempts); got != 3 {
		t.Fatalf("healthy attempts = %d, want 3", got)
	}

	recovered.Store(true)
	now = now.Add(time.Minute + time.Second)
	atomic.StoreUint64(&mc.rr, 0)
	batch, err = mc.SearchBatch(context.Background(), search.Query{Q: "open search"})
	if err != nil || batch.Status != search.BatchHealthy || len(batch.Hits) == 0 {
		t.Fatalf("half-open recovery batch=%+v err=%v", batch, err)
	}
	if !mc.qualityCooldown[0].IsZero() || mc.degradedStreak[0] != 0 {
		t.Fatalf("quality state did not recover: until=%v degraded=%d", mc.qualityCooldown[0], mc.degradedStreak[0])
	}
}

func TestMultiClient_DistinctAuthoritativeEmptiesRemainAuthoritative(t *testing.T) {
	var emptyAttempts int64
	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&emptyAttempts, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[],"unresponsive_engines":[]}`))
	}))
	t.Cleanup(empty.Close)

	mc, err := NewMultiWithCooldown([]string{empty.URL}, empty.Client(), time.Minute)
	if err != nil {
		t.Fatalf("NewMultiWithCooldown: %v", err)
	}
	for attempt := 0; attempt < 4; attempt++ {
		batch, err := mc.SearchBatch(context.Background(), search.Query{Q: fmt.Sprintf("legitimate empty %d", attempt)})
		if err != nil || batch.Status != search.BatchAuthoritativeEmpty {
			t.Fatalf("attempt %d batch=%+v err=%v", attempt+1, batch, err)
		}
	}
	if got := atomic.LoadInt64(&emptyAttempts); got != 4 {
		t.Fatalf("empty attempts = %d, want 4", got)
	}
	if !mc.qualityCooldown[0].IsZero() {
		t.Fatalf("authoritative empties opened quality circuit until %v", mc.qualityCooldown[0])
	}
}

func TestMultiClient_CallerCancellationDoesNotPoisonTransport(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)
	mc, err := NewMultiWithCooldown([]string{server.URL}, server.Client(), time.Minute)
	if err != nil {
		t.Fatalf("NewMultiWithCooldown: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := mc.SearchBatch(ctx, search.Query{Q: "cancel me"})
		done <- err
	}()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("SearchBatch error = %v, want context.Canceled", err)
	}
	if !mc.cooldown[0].IsZero() || mc.failures[0] != 0 {
		t.Fatalf("cancellation poisoned transport: cooldown=%v failures=%d", mc.cooldown[0], mc.failures[0])
	}
}

func TestMultiClient_LaterPageCallerCancellationWinsWithoutCooling(t *testing.T) {
	pageTwoStarted := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("pageno") == "1" {
			_, _ = w.Write([]byte(`{"results":[{"url":"https://example.com/first","title":"First"}]}`))
			return
		}
		close(pageTwoStarted)
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)
	mc, err := NewMultiWithCooldown([]string{server.URL}, server.Client(), time.Minute)
	if err != nil {
		t.Fatalf("NewMultiWithCooldown: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct {
		batch search.SearchBatch
		err   error
	}, 1)
	go func() {
		batch, err := mc.SearchBatch(ctx, search.Query{Q: "cancel page two", MaxResults: 2})
		done <- struct {
			batch search.SearchBatch
			err   error
		}{batch: batch, err: err}
	}()
	<-pageTwoStarted
	cancel()
	result := <-done
	if !errors.Is(result.err, context.Canceled) {
		t.Fatalf("SearchBatch error = %v, want context.Canceled", result.err)
	}
	if result.batch.Status != search.BatchFailed || len(result.batch.Hits) != 0 {
		t.Fatalf("canceled SearchBatch = %+v, want failed without partial hits", result.batch)
	}
	if !mc.cooldown[0].IsZero() || mc.failures[0] != 0 {
		t.Fatalf("later-page cancellation poisoned transport: cooldown=%v failures=%d", mc.cooldown[0], mc.failures[0])
	}
}

func TestMultiClient_CanceledContextWinsOverOpenCircuits(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(fixture))
	}))
	t.Cleanup(server.Close)
	mc, err := NewMultiWithCooldown([]string{server.URL}, server.Client(), time.Minute)
	if err != nil {
		t.Fatalf("NewMultiWithCooldown: %v", err)
	}
	mc.cooldown[0] = time.Now().Add(time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = mc.SearchBatch(ctx, search.Query{Q: "cancelled"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SearchBatch error = %T %v, want context.Canceled", err, err)
	}
}

func TestMultiClient_CancellationDuringPoolSelectionWinsOverOpenCircuits(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(fixture))
	}))
	t.Cleanup(server.Close)
	mc, err := NewMultiWithCooldown([]string{server.URL}, server.Client(), time.Minute)
	if err != nil {
		t.Fatalf("NewMultiWithCooldown: %v", err)
	}
	mc.cooldown[0] = time.Now().Add(time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	mc.now = func() time.Time {
		cancel()
		return time.Now()
	}
	_, err = mc.SearchBatch(ctx, search.Query{Q: "cancelled during selection"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SearchBatch error = %T %v, want context.Canceled", err, err)
	}
}

func TestMultiClient_RealTimeoutStillCoolsTransport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)
	httpClient := server.Client()
	httpClient.Timeout = 25 * time.Millisecond
	mc, err := NewMultiWithCooldown([]string{server.URL}, httpClient, time.Minute)
	if err != nil {
		t.Fatalf("NewMultiWithCooldown: %v", err)
	}
	_, err = mc.SearchBatch(context.Background(), search.Query{Q: "timeout"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SearchBatch error = %v, want deadline exceeded", err)
	}
	if !mc.cooldown[0].After(time.Now()) || mc.failures[0] != 1 {
		t.Fatalf("timeout did not cool transport: cooldown=%v failures=%d", mc.cooldown[0], mc.failures[0])
	}
}

func TestMultiClient_CanceledPingDoesNotPoisonTransport(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)
	mc, err := NewMultiWithCooldown([]string{server.URL}, server.Client(), time.Minute)
	if err != nil {
		t.Fatalf("NewMultiWithCooldown: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- mc.Ping(ctx) }()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Ping error = %v, want context.Canceled", err)
	}
	if !mc.cooldown[0].IsZero() || mc.failures[0] != 0 {
		t.Fatalf("canceled Ping poisoned transport: cooldown=%v failures=%d", mc.cooldown[0], mc.failures[0])
	}
}

func TestMultiClient_LaterPageRateLimitReturnsPartialAndOpensTransportCircuit(t *testing.T) {
	var primaryAttempts, fallbackAttempts int64
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempt := atomic.AddInt64(&primaryAttempts, 1)
		if attempt%2 == 1 {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"results":[{"url":"https://mwmbl.org","title":"Mwmbl"}]}`))
			return
		}
		w.Header().Set("Retry-After", "120")
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	t.Cleanup(primary.Close)
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&fallbackAttempts, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fixture))
	}))
	t.Cleanup(fallback.Close)

	mc, err := NewMultiWithCooldown([]string{primary.URL, fallback.URL}, primary.Client(), time.Minute)
	if err != nil {
		t.Fatalf("NewMultiWithCooldown: %v", err)
	}
	batch, err := mc.SearchBatch(context.Background(), search.Query{Q: "open search", MaxResults: 5})
	if err != nil || batch.Status != search.BatchPartial || len(batch.Hits) != 1 {
		t.Fatalf("first batch=%+v err=%v", batch, err)
	}
	if delay := time.Until(mc.cooldown[0]); delay < 119*time.Second || delay > 120*time.Second {
		t.Fatalf("transport cooldown = %v, want about 120s", delay)
	}

	atomic.StoreUint64(&mc.rr, 0)
	batch, err = mc.SearchBatch(context.Background(), search.Query{Q: "open search", MaxResults: 5})
	if err != nil || len(batch.Hits) == 0 {
		t.Fatalf("fallback batch=%+v err=%v", batch, err)
	}
	if got := atomic.LoadInt64(&primaryAttempts); got != 2 {
		t.Fatalf("primary attempts = %d, want 2 while Retry-After is active", got)
	}
	if got := atomic.LoadInt64(&fallbackAttempts); got != 2 {
		t.Fatalf("fallback HTTP requests = %d, want 2 (page plus repeated-page stop)", got)
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
