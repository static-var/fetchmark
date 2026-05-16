package fetcher

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/egress"
)

func newFetcher(t *testing.T, b Budgets) *Fetcher {
	t.Helper()
	if b.MaxBodyBytes == 0 {
		b.MaxBodyBytes = 1 << 20
	}
	if b.MaxDecompressedBytes == 0 {
		b.MaxDecompressedBytes = 4 << 20
	}
	f, err := New(Options{
		Policy:        egress.DefaultInternal(),
		Budgets:       b,
		DefaultUA:     "Fetchmark-Test/1",
		RespectRobots: false,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return f
}

func TestFetch_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got != "Fetchmark-Test/1" {
			t.Errorf("ua = %q", got)
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html><body>hello</body></html>"))
	}))
	t.Cleanup(srv.Close)
	f := newFetcher(t, Budgets{})
	res := f.Fetch(context.Background(), Request{URL: srv.URL})
	if res.Err != nil || res.Unsupported != "" {
		t.Fatalf("got err=%v unsup=%q", res.Err, res.Unsupported)
	}
	if !strings.Contains(string(res.Body), "hello") {
		t.Fatalf("body = %q", res.Body)
	}
	if res.Status != 200 {
		t.Fatalf("status = %d", res.Status)
	}
}

func TestFetch_MIMEBlocked(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		_, _ = w.Write([]byte("%PDF-1.4\n%\xe2\xe3\xcf\xd3"))
	}))
	t.Cleanup(srv.Close)
	f := newFetcher(t, Budgets{AllowedMIME: []string{"text/html", "application/xhtml+xml"}})
	res := f.Fetch(context.Background(), Request{URL: srv.URL})
	if res.Unsupported != ReasonNonHTML {
		t.Fatalf("unsup = %q err=%v", res.Unsupported, res.Err)
	}
}

func TestFetch_BodyTooLarge(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(strings.Repeat("A", 2000)))
	}))
	t.Cleanup(srv.Close)
	f := newFetcher(t, Budgets{MaxBodyBytes: 1000, MaxDecompressedBytes: 4000})
	res := f.Fetch(context.Background(), Request{URL: srv.URL})
	if res.Unsupported != ReasonTooLarge {
		t.Fatalf("unsup = %q err=%v", res.Unsupported, res.Err)
	}
}

func TestFetch_Retries5xxThenSuccess(t *testing.T) {
	var count int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&count, 1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html>ok</html>"))
	}))
	t.Cleanup(srv.Close)
	f := newFetcher(t, Budgets{Retries: 2})
	res := f.Fetch(context.Background(), Request{URL: srv.URL})
	if res.Err != nil || res.Status != 200 {
		t.Fatalf("err=%v status=%d", res.Err, res.Status)
	}
	if atomic.LoadInt32(&count) != 2 {
		t.Fatalf("attempts = %d", count)
	}
}

func TestFetch_NonRetriedClientError(t *testing.T) {
	var count int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)
	f := newFetcher(t, Budgets{Retries: 3})
	res := f.Fetch(context.Background(), Request{URL: srv.URL})
	if res.Status != 400 || res.Err != nil {
		t.Fatalf("status=%d err=%v", res.Status, res.Err)
	}
	if atomic.LoadInt32(&count) != 1 {
		t.Fatalf("should not retry 4xx, attempts=%d", count)
	}
}

// Retries exhausted against a persistent 5xx must surface a terminal
// error so the pipeline marks the result fetch_failed rather than
// silently emitting a non-2xx "success". Guards against a regression
// where the retry loop's synthetic error was dropped on the floor.
func TestFetch_RetriesExhausted_TerminalError(t *testing.T) {
	var count int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	f := newFetcher(t, Budgets{Retries: 2})
	res := f.Fetch(context.Background(), Request{URL: srv.URL})
	if res.Err == nil {
		t.Fatalf("expected terminal error after exhausted retries; status=%d", res.Status)
	}
	if res.Status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", res.Status)
	}
	if got, want := atomic.LoadInt32(&count), int32(3); got != want {
		t.Fatalf("attempts = %d, want %d (initial + 2 retries)", got, want)
	}
	if !strings.Contains(res.Err.Error(), "after 2 retries") {
		t.Fatalf("error = %q; want mention of 'after 2 retries'", res.Err)
	}
}

func TestFetch_EgressBlockedByPolicy(t *testing.T) {
	// Use external policy + loopback URL -> must be rejected pre-connect.
	f, err := New(Options{
		Policy:    egress.DefaultExternal(),
		Budgets:   Budgets{MaxBodyBytes: 1 << 20, MaxDecompressedBytes: 4 << 20},
		DefaultUA: "Fetchmark-Test/1",
	})
	if err != nil {
		t.Fatal(err)
	}
	res := f.Fetch(context.Background(), Request{URL: "http://127.0.0.1:1/"})
	if res.Unsupported != ReasonEgress {
		t.Fatalf("unsup = %q err=%v", res.Unsupported, res.Err)
	}
}

func TestFetchMany_Parallelism(t *testing.T) {
	var inflight, peak int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := atomic.AddInt32(&inflight, 1)
		for {
			p := atomic.LoadInt32(&peak)
			if cur <= p || atomic.CompareAndSwapInt32(&peak, p, cur) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt32(&inflight, -1)
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html>ok</html>"))
	}))
	t.Cleanup(srv.Close)

	// Global=3, per-host=3: server serves a single host; expect peak<=3.
	f := newFetcher(t, Budgets{GlobalConcurrency: 3, PerHostConcurrency: 3})
	reqs := make([]Request, 10)
	for i := range reqs {
		reqs[i] = Request{URL: srv.URL + fmt.Sprintf("/p/%d", i)}
	}
	out := f.FetchMany(context.Background(), reqs)
	for i, r := range out {
		if r.Err != nil || r.Status != 200 {
			t.Fatalf("res[%d] err=%v status=%d", i, r.Err, r.Status)
		}
	}
	if atomic.LoadInt32(&peak) > 3 {
		t.Fatalf("peak inflight = %d, expected <= 3", peak)
	}
}

func TestClientFor_BadProxy(t *testing.T) {
	f := newFetcher(t, Budgets{})
	_, err := f.clientFor(":::not a url")
	if err == nil {
		t.Fatal("expected parse error")
	}
}

func TestGateFor_BoundsHostGateGrowth(t *testing.T) {
	f := newFetcher(t, Budgets{PerHostConcurrency: 1})

	for i := 0; i < maxHostGates*2; i++ {
		_ = f.gateFor(fmt.Sprintf("host-%d.example", i))
	}

	if got := f.hostGateCount(); got > maxHostGates {
		t.Fatalf("host gate count = %d, want <= %d", got, maxHostGates)
	}
}

func TestGateFor_UsesOverflowWhenAllGatesBusyAtCap(t *testing.T) {
	f := newFetcher(t, Budgets{PerHostConcurrency: 1})

	for i := 0; i < maxHostGates; i++ {
		release, err := f.acquireHostGate(context.Background(), fmt.Sprintf("busy-%d.example", i))
		if err != nil {
			t.Fatalf("acquireHostGate: %v", err)
		}
		defer release()
	}

	release, err := f.acquireHostGate(context.Background(), "overflow.example")
	if err != nil {
		t.Fatalf("acquireHostGate overflow: %v", err)
	}
	defer release()
	if got := f.hostGateCount(); got != maxHostGates {
		t.Fatalf("host gate count = %d, want %d", got, maxHostGates)
	}
}

func TestAcquireHostGate_DoesNotEvictReservedGate(t *testing.T) {
	f := newFetcher(t, Budgets{PerHostConcurrency: 1})

	releaseHeld, err := f.acquireHostGate(context.Background(), "reserved.example")
	if err != nil {
		t.Fatalf("acquire held gate: %v", err)
	}

	reservedStarted := make(chan struct{})
	reservedAcquired := make(chan func())
	go func() {
		close(reservedStarted)
		release, err := f.acquireHostGate(context.Background(), "reserved.example")
		if err != nil {
			t.Errorf("reserve second gate: %v", err)
			close(reservedAcquired)
			return
		}
		reservedAcquired <- release
	}()
	<-reservedStarted
	deadline := time.Now().Add(time.Second)
	for {
		f.hostsMu.Lock()
		active := f.hosts["reserved.example"].active
		f.hostsMu.Unlock()
		if active == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for blocked acquire to reserve host gate")
		}
		time.Sleep(time.Millisecond)
	}

	for i := 0; i < maxHostGates-1; i++ {
		_ = f.gateFor(fmt.Sprintf("idle-%d.example", i))
	}
	_ = f.gateFor("new.example")

	f.hostsMu.Lock()
	_, ok := f.hosts["reserved.example"]
	f.hostsMu.Unlock()
	if !ok {
		t.Fatal("reserved gate was evicted while an acquire was blocked on its semaphore")
	}

	releaseHeld()
	releaseSecond := <-reservedAcquired
	if releaseSecond != nil {
		releaseSecond()
	}
}
