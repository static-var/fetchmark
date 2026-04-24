package searxng

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

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
