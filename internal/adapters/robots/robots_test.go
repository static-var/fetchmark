package robots

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestChecker_AllowDisallow(t *testing.T) {
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		if got := r.Header.Get("User-Agent"); got != "Fetchmark" {
			t.Errorf("robots User-Agent = %q", got)
		}
		_, _ = w.Write([]byte("User-agent: Fetchmark\nDisallow: /private\n"))
	}))
	t.Cleanup(srv.Close)

	c := New(srv.Client(), time.Hour, 0)
	ok, err := c.Allowed(context.Background(), "Fetchmark", srv.URL+"/public/page")
	if err != nil || !ok {
		t.Fatalf("public allowed err=%v ok=%v", err, ok)
	}
	ok, _ = c.Allowed(context.Background(), "Fetchmark", srv.URL+"/private/page")
	if ok {
		t.Fatal("private should be disallowed")
	}
	// Second check must hit cache (still 1 upstream request).
	_, _ = c.Allowed(context.Background(), "Fetchmark", srv.URL+"/another")
	if n := atomic.LoadInt64(&hits); n != 1 {
		t.Fatalf("expected 1 upstream hit, got %d", n)
	}
}

func TestChecker_4xxAllowsAll(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	c := New(srv.Client(), time.Hour, 0)
	ok, err := c.Allowed(context.Background(), "Fetchmark", srv.URL+"/anywhere")
	if err != nil || !ok {
		t.Fatalf("4xx should allow all, err=%v ok=%v", err, ok)
	}
}

func TestChecker_CoalescesConcurrentColdFetchesPerOrigin(t *testing.T) {
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write([]byte("User-agent: Fetchmark\nDisallow: /private\n"))
	}))
	t.Cleanup(srv.Close)

	c := New(srv.Client(), time.Hour, 0)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := c.Allowed(context.Background(), "Fetchmark", srv.URL+"/public")
			if err != nil || !ok {
				t.Errorf("Allowed err=%v ok=%v", err, ok)
			}
		}()
	}
	wg.Wait()
	if got := atomic.LoadInt64(&hits); got != 1 {
		t.Fatalf("robots fetches = %d, want 1", got)
	}
}

func TestChecker_FetchReportsOversizedRobots(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("User-agent: Fetchmark\nDisallow: /private\n" + strings.Repeat("x", 64)))
	}))
	t.Cleanup(srv.Close)

	c := New(srv.Client(), time.Hour, 16)
	data, err := c.fetch(context.Background(), srv.URL, "Fetchmark")
	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("err = %v, want too-large error", err)
	}
	if data != nil {
		t.Fatalf("oversized robots data should not be parsed: %+v", data)
	}
}

func TestChecker_FetchFailureIsNotCachedAsAllow(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			http.Error(w, "temporary", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte("User-agent: Fetchmark\nDisallow: /private\n"))
	}))
	t.Cleanup(srv.Close)
	c := New(srv.Client(), time.Hour, 0)

	if ok, _ := c.Allowed(context.Background(), "Fetchmark", srv.URL+"/private"); !ok {
		t.Fatal("first transient failure should fail open")
	}
	if ok, _ := c.Allowed(context.Background(), "Fetchmark", srv.URL+"/private"); ok {
		t.Fatal("second request should retry robots and observe disallow")
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("robots hits = %d, want 2", got)
	}
}

func TestChecker_UnreachableFailsOpen(t *testing.T) {
	c := New(&http.Client{Timeout: 100 * time.Millisecond}, time.Hour, 0)
	// Port 1 is reserved; connect refuses fast.
	ok, err := c.Allowed(context.Background(), "Fetchmark", "http://127.0.0.1:1/page")
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !ok {
		t.Fatal("unreachable robots.txt should fail open")
	}
}

func TestChecker_SweepExpiredRemovesOldEntries(t *testing.T) {
	c := New(http.DefaultClient, time.Minute, 0)
	now := time.Now()
	c.cache["http://expired.example"] = entry{fetched: now.Add(-2 * time.Minute)}
	c.cache["http://fresh.example"] = entry{fetched: now.Add(-30 * time.Second)}

	c.sweepExpired(now)

	if _, ok := c.cache["http://expired.example"]; ok {
		t.Fatal("expired cache entry was not removed")
	}
	if _, ok := c.cache["http://fresh.example"]; !ok {
		t.Fatal("fresh cache entry was removed")
	}
}
