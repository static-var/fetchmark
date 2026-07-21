package fetcher

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/egress"
	"github.com/staticvar/fetchmark/internal/adapters/robots"
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

func TestFetchConditional304IsBodylessSuccess(t *testing.T) {
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		if got := r.Header.Get("If-None-Match"); got != `W/"version-1"` {
			t.Errorf("If-None-Match = %q", got)
		}
		if got := r.Header.Get("If-Modified-Since"); got != "Fri, 17 Jul 2026 10:00:00 GMT" {
			t.Errorf("If-Modified-Since = %q", got)
		}
		w.Header().Set("ETag", `W/"version-1"`)
		w.Header().Set("Last-Modified", "Fri, 17 Jul 2026 10:00:00 GMT")
		w.Header().Set("X-Robots-Tag", "noarchive")
		w.WriteHeader(http.StatusNotModified)
	}))
	t.Cleanup(srv.Close)
	f := newFetcher(t, Budgets{Retries: 2, AllowedMIME: []string{"text/html"}})
	res := f.Fetch(context.Background(), Request{
		URL: srv.URL, IfNoneMatch: `W/"version-1"`, IfModifiedSince: "Fri, 17 Jul 2026 10:00:00 GMT",
	})
	if res.Err != nil || res.Unsupported != "" || !res.NotModified || res.Status != http.StatusNotModified {
		t.Fatalf("result = %+v", res)
	}
	if res.Body != nil || res.BytesRead != 0 || attempts.Load() != 1 {
		t.Fatalf("body=%q bytes=%d attempts=%d", res.Body, res.BytesRead, attempts.Load())
	}
	if res.ETag != `W/"version-1"` || res.LastModified != "Fri, 17 Jul 2026 10:00:00 GMT" || len(res.XRobotsTag) != 1 {
		t.Fatalf("validators/signals = %+v", res)
	}
}

func TestFetchUnexpected304IsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	}))
	t.Cleanup(srv.Close)
	res := newFetcher(t, Budgets{}).Fetch(context.Background(), Request{URL: srv.URL})
	if res.Err == nil || res.NotModified || res.Unsupported != "" {
		t.Fatalf("result = %+v", res)
	}
}

func TestFetchRetryPreservesConditionalValidators(t *testing.T) {
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") != `"stable"` {
			t.Errorf("attempt %d lost validator", attempts.Load()+1)
		}
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("retry"))
			return
		}
		w.WriteHeader(http.StatusNotModified)
	}))
	t.Cleanup(srv.Close)
	f := newFetcher(t, Budgets{Retries: 2})
	res := f.Fetch(context.Background(), Request{URL: srv.URL, IfNoneMatch: `"stable"`})
	if res.Err != nil || !res.NotModified || attempts.Load() != 2 {
		t.Fatalf("result=%+v attempts=%d", res, attempts.Load())
	}
	if res.BytesRead != int64(len("retry")) {
		t.Fatalf("bytes = %d", res.BytesRead)
	}
}

func TestFetchDoesNotForwardValidatorsAcrossOriginRedirect(t *testing.T) {
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("If-None-Match"); got != "" {
			t.Errorf("cross-origin If-None-Match = %q", got)
		}
		if got := r.Header.Get("If-Modified-Since"); got != "" {
			t.Errorf("cross-origin If-Modified-Since = %q", got)
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html>redirected</html>"))
	}))
	t.Cleanup(destination.Close)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", destination.URL)
		w.WriteHeader(http.StatusFound)
	}))
	t.Cleanup(source.Close)
	f := newFetcher(t, Budgets{})
	res := f.Fetch(context.Background(), Request{
		URL: source.URL, IfNoneMatch: `"origin-one"`, IfModifiedSince: "Fri, 17 Jul 2026 10:00:00 GMT",
	})
	if res.Err != nil || res.Status != http.StatusOK || !strings.Contains(string(res.Body), "redirected") {
		t.Fatalf("result = %+v", res)
	}
}

func TestFetchFocusedRedirectScope(t *testing.T) {
	var escapedTargetHits atomic.Int64
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		escapedTargetHits.Add(1)
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html>escaped</html>"))
	}))
	t.Cleanup(destination.Close)

	var source *httptest.Server
	source = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/docs/allowed-start":
			http.Redirect(w, r, "/docs/allowed-final", http.StatusFound)
		case "/docs/allowed-final":
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte("<html>allowed</html>"))
		case "/docs/outside-start":
			http.Redirect(w, r, "/private/final", http.StatusFound)
		case "/docs/origin-start":
			http.Redirect(w, r, destination.URL+"/stolen", http.StatusFound)
		case "/docs/encoded-start":
			w.Header().Set("Location", "/docs/%2e%2e/private")
			w.WriteHeader(http.StatusFound)
		case "/docs/encoded-deny-start":
			w.Header().Set("Location", "/docs/%73ecret/page")
			w.WriteHeader(http.StatusFound)
		default:
			escapedTargetHits.Add(1)
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte("<html>unexpected</html>"))
		}
	}))
	t.Cleanup(source.Close)

	f := newFetcher(t, Budgets{})
	allowed := f.Fetch(context.Background(), Request{
		URL: source.URL + "/docs/allowed-start", FocusedRedirectScope: &RedirectScope{AllowedPathPrefixes: []string{"/docs/"}},
	})
	if allowed.Err != nil || allowed.Unsupported != "" || allowed.FinalURL != source.URL+"/docs/allowed-final" {
		t.Fatalf("allowed result = %+v", allowed)
	}
	for _, path := range []string{"/docs/outside-start", "/docs/origin-start", "/docs/encoded-start", "/docs/encoded-deny-start"} {
		result := f.Fetch(context.Background(), Request{
			URL: source.URL + path, FocusedRedirectScope: &RedirectScope{
				AllowedPathPrefixes: []string{"/docs/"}, DeniedPathPrefixes: []string{"/docs/secret/"},
			},
		})
		if result.Err != nil || result.Unsupported != ReasonRedirectScope || len(result.Body) != 0 {
			t.Fatalf("path %s result = %+v", path, result)
		}
	}
	if escapedTargetHits.Load() != 0 {
		t.Fatalf("out-of-scope redirect target received %d requests", escapedTargetHits.Load())
	}
}

func TestFetchFocusedScopeRejectsEncodedDeniedSeedBeforeNetwork(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html>must not be fetched</html>"))
	}))
	t.Cleanup(server.Close)
	result := newFetcher(t, Budgets{}).Fetch(context.Background(), Request{
		URL: server.URL + "/docs/%73ecret/page",
		FocusedRedirectScope: &RedirectScope{
			AllowedPathPrefixes: []string{"/docs/"}, DeniedPathPrefixes: []string{"/docs/secret/"},
		},
	})
	if result.Err != nil || result.Unsupported != ReasonRedirectScope || hits.Load() != 0 {
		t.Fatalf("result=%+v hits=%d", result, hits.Load())
	}
}

func TestFetchFocusedScopeNormalizesExactDeniedURLPathEscapes(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html>must not be fetched</html>"))
	}))
	t.Cleanup(server.Close)
	result := newFetcher(t, Budgets{}).Fetch(context.Background(), Request{
		URL: server.URL + "/docs/%73ecret",
		FocusedRedirectScope: &RedirectScope{
			AllowedPathPrefixes: []string{"/docs/"}, DeniedURLs: []string{server.URL + "/docs/secret"},
		},
	})
	if result.Err != nil || result.Unsupported != ReasonRedirectScope || hits.Load() != 0 {
		t.Fatalf("result=%+v hits=%d", result, hits.Load())
	}
}

func TestFocusedRedirectScopeEnforcesExplicitAllowAndDenyPaths(t *testing.T) {
	original, err := url.Parse("https://example.com/docs/page")
	if err != nil {
		t.Fatal(err)
	}
	scope, err := newFocusedRedirectScope(original, RedirectScope{
		AllowedPathPrefixes: []string{"/docs/"}, DeniedPathPrefixes: []string{"/docs/private/"},
		DeniedURLs: []string{"https://example.com/docs/blocked"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"https://example.com/docs/next", "https://example.com:443/docs/elsewhere"} {
		candidate, parseErr := url.Parse(raw)
		if parseErr != nil || !scope.allows(candidate) {
			t.Fatalf("scope rejected %q (parse=%v)", raw, parseErr)
		}
	}
	for _, raw := range []string{"https://example.com/private", "https://example.com/docs/private/secret", "https://example.com/docs/blocked"} {
		candidate, parseErr := url.Parse(raw)
		if parseErr != nil || scope.allows(candidate) {
			t.Fatalf("scope allowed %q (parse=%v)", raw, parseErr)
		}
	}
}

func TestFetchFocusedRedirectScopeRejectsAmbiguousOriginalBeforeNetwork(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html>must not be fetched</html>"))
	}))
	t.Cleanup(server.Close)
	result := newFetcher(t, Budgets{}).Fetch(context.Background(), Request{
		URL: server.URL + "/docs/%2e%2e/private", FocusedRedirectScope: &RedirectScope{AllowedPathPrefixes: []string{"/docs/"}},
	})
	if result.Err != nil || result.Unsupported != ReasonRedirectScope || hits.Load() != 0 {
		t.Fatalf("result=%+v hits=%d", result, hits.Load())
	}
}

func TestFocusedRedirectScopeAllowsExplicitRootPolicyForEmptyPath(t *testing.T) {
	original, err := url.Parse("https://example.com")
	if err != nil {
		t.Fatal(err)
	}
	scope, err := newFocusedRedirectScope(original, RedirectScope{AllowedPathPrefixes: []string{"/"}})
	if err != nil || !scope.allows(original) {
		t.Fatalf("scope=%+v error=%v", scope, err)
	}
}

func TestFetchEvaluatesRobotsForRedirectDestination(t *testing.T) {
	var destinationPageHits atomic.Int64
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			_, _ = w.Write([]byte("User-agent: Fetchmark-Test\nDisallow: /private\n"))
			return
		}
		destinationPageHits.Add(1)
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html>must not be fetched</html>"))
	}))
	t.Cleanup(destination.Close)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			_, _ = w.Write([]byte("User-agent: Fetchmark-Test\nAllow: /\n"))
			return
		}
		w.Header().Set("Location", destination.URL+"/private/page")
		w.WriteHeader(http.StatusFound)
	}))
	t.Cleanup(source.Close)
	policy := egress.DefaultInternal()
	checker := robots.New(policy.HTTPClient(time.Second), time.Hour, 0)
	f, err := New(Options{
		Policy: policy, Budgets: Budgets{MaxBodyBytes: 1 << 20, MaxDecompressedBytes: 1 << 20, PerHostConcurrency: 1},
		Robots: checker, DefaultUA: "Fetchmark-Test/1", RespectRobots: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := f.Fetch(context.Background(), Request{URL: source.URL + "/start", RespectRobots: true})
	if result.Unsupported != ReasonRobots || result.Err != nil {
		t.Fatalf("result = %+v", result)
	}
	if destinationPageHits.Load() != 0 {
		t.Fatalf("destination page hits = %d", destinationPageHits.Load())
	}
}

func TestFetchReciprocalRedirectsDoNotInvertHostGates(t *testing.T) {
	for _, respectRobots := range []bool{false, true} {
		t.Run(fmt.Sprintf("robots_%v", respectRobots), func(t *testing.T) {
			ready := make(chan struct{}, 2)
			release := make(chan struct{})
			var aURL, bURL string
			handler := func(target *string) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/robots.txt" {
						_, _ = w.Write([]byte("User-agent: Fetchmark-Test\nAllow: /\n"))
						return
					}
					if r.URL.Path == "/start" {
						ready <- struct{}{}
						<-release
						http.Redirect(w, r, *target+"/final", http.StatusFound)
						return
					}
					w.Header().Set("Content-Type", "text/html")
					_, _ = w.Write([]byte("<html>ok</html>"))
				}
			}
			a := httptest.NewServer(handler(&bURL))
			t.Cleanup(a.Close)
			b := httptest.NewServer(handler(&aURL))
			t.Cleanup(b.Close)
			aURL = "http://a.test"
			bURL = "http://b.test"
			addresses := map[string]string{
				"a.test": a.Listener.Addr().String(),
				"b.test": b.Listener.Addr().String(),
			}
			transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				host, _, err := net.SplitHostPort(address)
				if err != nil {
					return nil, err
				}
				return (&net.Dialer{}).DialContext(ctx, network, addresses[host])
			}}
			policy := egress.DefaultInternal()
			client := &http.Client{Transport: transport, CheckRedirect: policy.HTTPClient(0).CheckRedirect}
			var checker *robots.Checker
			if respectRobots {
				checker = robots.New(client, time.Hour, 0)
			}
			f, err := New(Options{
				Policy: policy, Budgets: Budgets{MaxBodyBytes: 1 << 20, MaxDecompressedBytes: 1 << 20, PerHostConcurrency: 1, GlobalConcurrency: 2},
				Robots: checker, DefaultUA: "Fetchmark-Test/1", RespectRobots: respectRobots,
			})
			if err != nil {
				t.Fatal(err)
			}
			f.clients[""] = client
			ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
			defer cancel()
			results := make([]Result, 2)
			var wait sync.WaitGroup
			wait.Add(2)
			go func() {
				defer wait.Done()
				results[0] = f.Fetch(ctx, Request{URL: aURL + "/start", RespectRobots: respectRobots})
			}()
			go func() {
				defer wait.Done()
				results[1] = f.Fetch(ctx, Request{URL: bURL + "/start", RespectRobots: respectRobots})
			}()
			<-ready
			<-ready
			close(release)
			wait.Wait()
			for index, result := range results {
				if result.Err != nil || result.Status != http.StatusOK {
					t.Fatalf("result[%d] = %+v", index, result)
				}
			}
		})
	}
}

func TestFetch_PreservesAuthoritativeRobotsAndXRobotsTagSignals(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			_, _ = w.Write([]byte("User-agent: Fetchmark-Test\nAllow: /\n"))
			return
		}
		w.Header().Add("X-Robots-Tag", "noarchive")
		w.Header().Add("X-Robots-Tag", "Fetchmark-Test: noindex")
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html><body>retention controls</body></html>"))
	}))
	t.Cleanup(srv.Close)
	f, err := New(Options{
		Policy: egress.DefaultInternal(), Budgets: Budgets{MaxBodyBytes: 1 << 20, MaxDecompressedBytes: 1 << 20},
		Robots: robots.New(srv.Client(), time.Hour, 0), DefaultUA: "Fetchmark-Test/1", RespectRobots: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	res := f.Fetch(context.Background(), Request{URL: srv.URL + "/page", RespectRobots: true})
	if res.Err != nil || !res.RobotsAllowed || !res.RobotsAuthoritative {
		t.Fatalf("result = %+v", res)
	}
	if len(res.XRobotsTag) != 2 || res.XRobotsTag[1] != "Fetchmark-Test: noindex" {
		t.Fatalf("X-Robots-Tag = %#v", res.XRobotsTag)
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

func TestFetchBoundsResponseHeadersBeforeRetentionMetadata(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; "+strings.Repeat("x", 128<<10))
		_, _ = w.Write([]byte("<html>unreachable body</html>"))
	}))
	t.Cleanup(srv.Close)
	result := newFetcher(t, Budgets{MaxResponseHeaderBytes: 16 << 10}).Fetch(context.Background(), Request{URL: srv.URL})
	if result.Err == nil || result.Body != nil {
		t.Fatalf("result = %+v", result)
	}
}

func TestFetch_RequestBodyLimitOverridesConfiguredMaximum(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html>" + strings.Repeat("x", 100) + "</html>"))
	}))
	t.Cleanup(srv.Close)
	f := newFetcher(t, Budgets{MaxBodyBytes: 1000, MaxDecompressedBytes: 1000})
	res := f.Fetch(context.Background(), Request{URL: srv.URL, MaxBodyBytes: 16, MaxDecompressedBytes: 16})
	if res.Unsupported != ReasonTooLarge {
		t.Fatalf("unsupported = %q, want %q", res.Unsupported, ReasonTooLarge)
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

func TestFetch_RetryWorkStopsAtTotalByteBudget(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("12345678"))
	}))
	t.Cleanup(srv.Close)
	f := newFetcher(t, Budgets{Retries: 5, MaxBodyBytes: 100, MaxDecompressedBytes: 100})
	res := f.Fetch(context.Background(), Request{URL: srv.URL, MaxTotalBytes: 16})
	if res.Unsupported != ReasonRequestBudget {
		t.Fatalf("unsupported = %q, want %q (err=%v)", res.Unsupported, ReasonRequestBudget, res.Err)
	}
	if res.BytesRead != 16 || hits.Load() != 2 {
		t.Fatalf("bytes=%d hits=%d, want 16 bytes and 2 attempts", res.BytesRead, hits.Load())
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

func TestFetch_RetriesRetryableStatusBeforeMIMERejection(t *testing.T) {
	var count int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&count, 1) == 1 {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("temporary outage"))
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html>ok</html>"))
	}))
	t.Cleanup(srv.Close)

	f := newFetcher(t, Budgets{Retries: 1, AllowedMIME: []string{"text/html"}})
	res := f.Fetch(context.Background(), Request{URL: srv.URL})
	if res.Err != nil || res.Unsupported != "" {
		t.Fatalf("err=%v unsupported=%q status=%d", res.Err, res.Unsupported, res.Status)
	}
	if res.Status != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.Status)
	}
	if got := atomic.LoadInt32(&count); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
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

func TestFetch_PerRequestTimeoutCanExceedDefaultClientTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(30 * time.Millisecond)
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html>slow ok</html>"))
	}))
	t.Cleanup(srv.Close)

	f := newFetcher(t, Budgets{FetchTimeout: 5 * time.Millisecond})
	res := f.Fetch(context.Background(), Request{URL: srv.URL, Timeout: 100 * time.Millisecond})
	if res.Err != nil || res.Unsupported != "" {
		t.Fatalf("per-request timeout should govern request: err=%v unsupported=%q", res.Err, res.Unsupported)
	}
	if res.Status != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.Status)
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
	entered := make(chan struct{}, 10)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := atomic.AddInt32(&inflight, 1)
		entered <- struct{}{}
		for {
			p := atomic.LoadInt32(&peak)
			if cur <= p || atomic.CompareAndSwapInt32(&peak, p, cur) {
				break
			}
		}
		<-release
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

	done := make(chan []Result, 1)
	go func() {
		done <- f.FetchMany(context.Background(), reqs)
	}()

	for i := 0; i < 3; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			close(release)
			t.Fatalf("timed out waiting for request %d to enter handler", i+1)
		}
	}
	select {
	case <-entered:
		close(release)
		t.Fatalf("observed more than 3 concurrent requests; peak inflight = %d", atomic.LoadInt32(&peak))
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	var out []Result
	select {
	case out = <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for FetchMany to complete")
	}
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

func TestClientFor_BoundsProxyClientCache(t *testing.T) {
	f := newFetcher(t, Budgets{})
	for i := 0; i < maxProxyClients*2; i++ {
		if _, err := f.clientFor(fmt.Sprintf("http://127.0.0.1:1/?proxy=%d", i)); err != nil {
			t.Fatalf("clientFor(%d): %v", i, err)
		}
	}
	if got := len(f.clients); got > maxProxyClients+1 {
		t.Fatalf("client cache size = %d, want <= %d", got, maxProxyClients+1)
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
	requireEventually(t, time.Second, func() bool {
		f.hostsMu.Lock()
		active := f.hosts["reserved.example"].active
		f.hostsMu.Unlock()
		return active == 2
	})

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
