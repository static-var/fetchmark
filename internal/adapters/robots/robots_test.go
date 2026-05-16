package robots

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestChecker_AllowDisallow(t *testing.T) {
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
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
