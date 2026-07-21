package crawlsource

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/egress"
	"github.com/staticvar/fetchmark/internal/adapters/robots"
)

var fixedNow = time.Date(2026, 7, 18, 10, 30, 0, 0, time.UTC)

type recordingRobots struct {
	mu        sync.Mutex
	decisions map[string]robots.Decision
	urls      []string
}

func (r *recordingRobots) Evaluate(_ context.Context, _, rawURL string) robots.Decision {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.urls = append(r.urls, rawURL)
	if decision, ok := r.decisions[rawURL]; ok {
		return decision
	}
	return robots.Decision{Allowed: true, Authoritative: true}
}

func TestNewAcceptsCrawlerUserAgentWithMaximumContactURI(t *testing.T) {
	userAgent := "FetchmarkCrawler/dev (+https://operator.example/" + strings.Repeat("a", 2048-len("https://operator.example/")) + ")"
	if _, err := New(Options{Policy: egress.DefaultInternal(), Robots: &recordingRobots{}, UserAgent: userAgent}); err != nil {
		t.Fatalf("valid configured contact did not fit crawler User-Agent: %v", err)
	}
}

func (r *recordingRobots) observed() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.urls...)
}

func newTestClient(t *testing.T, policy egress.Policy, evaluator RobotsEvaluator, mutate func(*Options)) *Client {
	t.Helper()
	options := Options{
		Policy: policy, Robots: evaluator, UserAgent: "FetchmarkCrawler/1.0 (+https://example.test/crawler)",
		Timeout: time.Second, Now: func() time.Time { return fixedNow },
	}
	if mutate != nil {
		mutate(&options)
	}
	client, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func gzipBytes(t *testing.T, body []byte) []byte {
	t.Helper()
	var encoded bytes.Buffer
	writer := gzip.NewWriter(&encoded)
	if _, err := writer.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func assertReason(t *testing.T, err error, want Reason) {
	t.Helper()
	var fetchErr *Error
	if !errors.As(err, &fetchErr) || fetchErr.Reason != want {
		t.Fatalf("error = %v, want reason %q", err, want)
	}
}

func TestFetchReturnsVerifiedXMLAndMetadata(t *testing.T) {
	const document = `<?xml version="1.0"?><urlset><url><loc>https://example.test/a</loc></url></urlset>`
	var sourceURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sources/sitemap.xml" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		if got := r.Header.Get("User-Agent"); got != "FetchmarkCrawler/1.0 (+https://example.test/crawler)" {
			t.Errorf("user-agent = %q", got)
		}
		if r.Header.Get("If-None-Match") != `"previous"` || r.Header.Get("If-Modified-Since") != "Thu, 17 Jul 2026 10:30:00 GMT" {
			t.Errorf("conditionals = %q / %q", r.Header.Get("If-None-Match"), r.Header.Get("If-Modified-Since"))
		}
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.Header().Set("ETag", `"current"`)
		w.Header().Set("Last-Modified", "Fri, 18 Jul 2026 09:30:00 GMT")
		w.Header().Set("Cache-Control", "public, max-age=120")
		w.Header().Set("Date", fixedNow.Format(http.TimeFormat))
		_, _ = w.Write([]byte(document))
	}))
	t.Cleanup(server.Close)
	sourceURL = server.URL + "/sources/sitemap.xml"
	evaluator := &recordingRobots{}
	client := newTestClient(t, egress.DefaultInternal(), evaluator, nil)

	result, err := client.Fetch(context.Background(), Request{
		URL: sourceURL, AllowedPathPrefixes: []string{"/sources/"},
		IfNoneMatch: `"previous"`, IfModifiedSince: "Thu, 17 Jul 2026 10:30:00 GMT",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != http.StatusOK || result.FinalURL != sourceURL || result.ContentType != "application/xml" || string(result.Body) != document {
		t.Fatalf("result = %+v", result)
	}
	if result.ETag != `"current"` || result.LastModified != "Fri, 18 Jul 2026 09:30:00 GMT" || result.XMLRoot != "urlset" {
		t.Fatalf("validators/root = %+v", result)
	}
	if result.WireBytes != int64(len(document)) || result.DecompressedBytes != int64(len(document)) || !result.ObservedAt.Equal(fixedNow) {
		t.Fatalf("accounting = %+v", result)
	}
	if !result.Freshness.FreshUntil.Equal(fixedNow.Add(2*time.Minute)) || result.Freshness.NoStore || result.Freshness.Revalidate {
		t.Fatalf("freshness = %+v", result.Freshness)
	}
	if got := evaluator.observed(); !reflect.DeepEqual(got, []string{sourceURL}) {
		t.Fatalf("robots URLs = %#v", got)
	}
}

func TestFetchChecksScopeEgressAndRobotsBeforeEachRedirect(t *testing.T) {
	var redirectedConditionals atomic.Bool
	var destinationHits atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sources/current.xml":
			http.Redirect(w, r, server.URL+"/sources/archive/v2.xml", http.StatusFound)
		case "/sources/archive/v2.xml":
			destinationHits.Add(1)
			redirectedConditionals.Store(r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "")
			w.Header().Set("Content-Type", "application/atom+xml")
			w.Header().Set("ETag", `"redirect-target-only"`)
			w.Header().Set("Last-Modified", "Fri, 18 Jul 2026 09:30:00 GMT")
			_, _ = w.Write([]byte(`<feed xmlns="http://www.w3.org/2005/Atom"><entry/></feed>`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	original := server.URL + "/sources/current.xml"
	final := server.URL + "/sources/archive/v2.xml"
	evaluator := &recordingRobots{}
	client := newTestClient(t, egress.DefaultInternal(), evaluator, nil)

	result, err := client.Fetch(context.Background(), Request{
		URL: original, AllowedPathPrefixes: []string{"/sources/"}, IfNoneMatch: `"v1"`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.FinalURL != final || destinationHits.Load() != 1 || redirectedConditionals.Load() {
		t.Fatalf("result=%+v destinationHits=%d redirectedConditionals=%v", result, destinationHits.Load(), redirectedConditionals.Load())
	}
	if result.ETag != "" || result.LastModified != "" {
		t.Fatalf("redirect target validators leaked into original source state: %+v", result)
	}
	if got := evaluator.observed(); !reflect.DeepEqual(got, []string{original, final}) {
		t.Fatalf("robots URLs = %#v", got)
	}
}

func TestFetchRejectsRedirectOutsideOriginalOriginOrPathScope(t *testing.T) {
	var outsideHits atomic.Int32
	outside := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		outsideHits.Add(1)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<urlset/>`))
	}))
	t.Cleanup(outside.Close)

	for _, test := range []struct {
		name     string
		location func(string) string
	}{
		{name: "different authority", location: func(string) string { return outside.URL + "/sources/final.xml" }},
		{name: "outside path", location: func(origin string) string { return origin + "/private/final.xml" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/sources/start.xml" {
					http.Redirect(w, r, test.location(server.URL), http.StatusFound)
					return
				}
				outsideHits.Add(1)
			}))
			t.Cleanup(server.Close)
			client := newTestClient(t, egress.DefaultInternal(), &recordingRobots{}, nil)
			_, err := client.Fetch(context.Background(), Request{URL: server.URL + "/sources/start.xml", AllowedPathPrefixes: []string{"/sources/"}})
			assertReason(t, err, ReasonRedirectScope)
		})
	}
	if outsideHits.Load() != 0 {
		t.Fatalf("outside redirect target was requested %d times", outsideHits.Load())
	}
}

func TestFetchAllowsLiteralDoubleSlashButKeepsExactPrefixScope(t *testing.T) {
	var escapedScopeHits atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sources//direct.xml":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<urlset/>`))
		case "/sources//redirect.xml":
			http.Redirect(w, r, server.URL+"/sources/escaped.xml", http.StatusFound)
		case "/sources/escaped.xml":
			escapedScopeHits.Add(1)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	client := newTestClient(t, egress.DefaultInternal(), &recordingRobots{}, nil)
	if _, err := client.Fetch(context.Background(), Request{
		URL: server.URL + "/sources//direct.xml", AllowedPathPrefixes: []string{"/sources//"},
	}); err != nil {
		t.Fatalf("literal double slash source rejected: %v", err)
	}
	_, err := client.Fetch(context.Background(), Request{
		URL: server.URL + "/sources//redirect.xml", AllowedPathPrefixes: []string{"/sources//"},
	})
	assertReason(t, err, ReasonRedirectScope)
	if escapedScopeHits.Load() != 0 {
		t.Fatal("single-slash redirect escaped the exact configured prefix")
	}
}

func TestFetchFailsClosedForRobotsUnreachableAndDisallowed(t *testing.T) {
	for _, test := range []struct {
		name     string
		decision robots.Decision
		want     Reason
	}{
		{name: "unreachable", decision: robots.Decision{Allowed: false, Authoritative: true, Err: errors.New("robots upstream 503")}, want: ReasonRobotsUnreachable},
		{name: "disallowed", decision: robots.Decision{Allowed: false, Authoritative: true}, want: ReasonRobotsDisallowed},
	} {
		t.Run(test.name, func(t *testing.T) {
			var hits atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				w.Header().Set("Content-Type", "application/xml")
				_, _ = w.Write([]byte(`<urlset/>`))
			}))
			t.Cleanup(server.Close)
			evaluator := &recordingRobots{decisions: map[string]robots.Decision{server.URL + "/sitemap.xml": test.decision}}
			client := newTestClient(t, egress.DefaultInternal(), evaluator, nil)
			_, err := client.Fetch(context.Background(), Request{URL: server.URL + "/sitemap.xml", AllowedPathPrefixes: []string{"/"}})
			assertReason(t, err, test.want)
			if hits.Load() != 0 {
				t.Fatalf("source requested after robots rejection")
			}
		})
	}
}

func TestFetchSupportsOneBoundedGzipLayer(t *testing.T) {
	document := []byte(`<sitemapindex><sitemap><loc>https://example.test/one.xml</loc></sitemap></sitemapindex>`)
	compressed := gzipBytes(t, document)
	for _, test := range []struct {
		name            string
		contentType     string
		contentEncoding string
		body            []byte
	}{
		{name: "HTTP gzip", contentType: "application/xml", contentEncoding: "gzip", body: compressed},
		{name: "gzip sitemap payload", contentType: "application/gzip", body: compressed},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", test.contentType)
				if test.contentEncoding != "" {
					w.Header().Set("Content-Encoding", test.contentEncoding)
				}
				_, _ = w.Write(test.body)
			}))
			t.Cleanup(server.Close)
			client := newTestClient(t, egress.DefaultInternal(), &recordingRobots{}, nil)
			result, err := client.Fetch(context.Background(), Request{URL: server.URL + "/sources/site.xml.gz", AllowedPathPrefixes: []string{"/sources/"}})
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(result.Body, document) || result.WireBytes != int64(len(compressed)) || result.DecompressedBytes != int64(len(document)) {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}

func TestFetchRejectsNestedGzipAndByteLimitOverruns(t *testing.T) {
	document := []byte(`<urlset>` + strings.Repeat(`<url/>`, 128) + `</urlset>`)
	compressed := gzipBytes(t, document)
	nested := gzipBytes(t, compressed)
	multiMember := append(gzipBytes(t, []byte(`<urlset/>`)), gzipBytes(t, []byte(`<feed/>`))...)
	for _, test := range []struct {
		name        string
		body        []byte
		contentType string
		encoding    string
		wire        int64
		decomp      int64
		want        Reason
	}{
		{name: "nested gzip", body: nested, contentType: "application/gzip", wire: 1 << 20, decomp: 1 << 20, want: ReasonNestedEncoding},
		{name: "multiple gzip members", body: multiMember, contentType: "application/gzip", wire: 1 << 20, decomp: 1 << 20, want: ReasonNestedEncoding},
		{name: "wire bytes", body: document, contentType: "application/xml", wire: 16, decomp: 1 << 20, want: ReasonWireTooLarge},
		{name: "decompressed bytes", body: compressed, contentType: "application/xml", encoding: "gzip", wire: 1 << 20, decomp: 32, want: ReasonDecompressedTooLarge},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", test.contentType)
				if test.encoding != "" {
					w.Header().Set("Content-Encoding", test.encoding)
				}
				_, _ = w.Write(test.body)
			}))
			t.Cleanup(server.Close)
			client := newTestClient(t, egress.DefaultInternal(), &recordingRobots{}, func(options *Options) {
				options.MaxWireBytes = 1 << 20
				options.MaxDecompressedBytes = 1 << 20
			})
			_, err := client.Fetch(context.Background(), Request{
				URL: server.URL + "/sources/site.xml.gz", AllowedPathPrefixes: []string{"/sources/"},
				MaxWireBytes: test.wire, MaxDecompressedBytes: test.decomp,
			})
			assertReason(t, err, test.want)
		})
	}
}

func TestFetchRequiresXMLMIMEAndSupportedRoot(t *testing.T) {
	for _, test := range []struct {
		name        string
		contentType string
		body        string
		want        Reason
	}{
		{name: "HTML MIME", contentType: "text/html", body: `<urlset/>`, want: ReasonContentType},
		{name: "HTML masquerading as XML", contentType: "application/xml", body: `<html><body>challenge</body></html>`, want: ReasonXMLRoot},
		{name: "malformed XML", contentType: "application/rss+xml", body: `<rss><channel>`, want: ReasonMalformedXML},
		{name: "multiple XML roots", contentType: "application/xml", body: `<urlset/><feed/>`, want: ReasonMalformedXML},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", test.contentType)
				_, _ = w.Write([]byte(test.body))
			}))
			t.Cleanup(server.Close)
			client := newTestClient(t, egress.DefaultInternal(), &recordingRobots{}, nil)
			_, err := client.Fetch(context.Background(), Request{URL: server.URL + "/source.xml", AllowedPathPrefixes: []string{"/"}})
			assertReason(t, err, test.want)
		})
	}
}

func TestFetchAcceptsOnlyOriginalConditionalNotModified(t *testing.T) {
	t.Run("matching original", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotModified) }))
		t.Cleanup(server.Close)
		client := newTestClient(t, egress.DefaultInternal(), &recordingRobots{}, nil)
		result, err := client.Fetch(context.Background(), Request{URL: server.URL + "/source.xml", AllowedPathPrefixes: []string{"/"}, IfNoneMatch: `"v1"`})
		if err != nil || !result.NotModified || result.Status != http.StatusNotModified || len(result.Body) != 0 {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	})

	t.Run("no-store clears returned validators", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("ETag", `"must-not-persist"`)
			w.Header().Set("Last-Modified", "Fri, 18 Jul 2026 09:30:00 GMT")
			w.WriteHeader(http.StatusNotModified)
		}))
		t.Cleanup(server.Close)
		client := newTestClient(t, egress.DefaultInternal(), &recordingRobots{}, nil)
		result, err := client.Fetch(context.Background(), Request{URL: server.URL + "/source.xml", AllowedPathPrefixes: []string{"/"}, IfNoneMatch: `"v1"`})
		if err != nil {
			t.Fatal(err)
		}
		if !result.NotModified || !result.Freshness.NoStore || result.ETag != "" || result.LastModified != "" {
			t.Fatalf("result = %+v", result)
		}
	})

	t.Run("unsolicited", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotModified) }))
		t.Cleanup(server.Close)
		client := newTestClient(t, egress.DefaultInternal(), &recordingRobots{}, nil)
		_, err := client.Fetch(context.Background(), Request{URL: server.URL + "/source.xml", AllowedPathPrefixes: []string{"/"}})
		assertReason(t, err, ReasonUnexpectedNotModified)
	})

	t.Run("redirected", func(t *testing.T) {
		var server *httptest.Server
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/source.xml" {
				http.Redirect(w, r, server.URL+"/next.xml", http.StatusFound)
				return
			}
			if r.Header.Get("If-None-Match") != "" {
				t.Errorf("conditional leaked to redirect")
			}
			w.WriteHeader(http.StatusNotModified)
		}))
		t.Cleanup(server.Close)
		client := newTestClient(t, egress.DefaultInternal(), &recordingRobots{}, nil)
		_, err := client.Fetch(context.Background(), Request{URL: server.URL + "/source.xml", AllowedPathPrefixes: []string{"/"}, IfNoneMatch: `"v1"`})
		assertReason(t, err, ReasonUnexpectedNotModified)
	})
}

func TestFetchParsesBoundedRetryAfterAndConservativeFreshness(t *testing.T) {
	for _, test := range []struct {
		name         string
		cacheControl string
		expires      string
		date         string
		age          string
		ageAgain     string
		retryAfter   string
		wantFresh    time.Time
		noStore      bool
		revalidate   bool
		wantRetry    time.Duration
	}{
		{name: "bounded seconds", cacheControl: "max-age=300", retryAfter: "999999", wantFresh: fixedNow.Add(5 * time.Minute), wantRetry: 6 * time.Hour},
		{name: "HTTP date", expires: fixedNow.Add(10 * time.Minute).Format(http.TimeFormat), retryAfter: fixedNow.Add(90 * time.Second).Format(http.TimeFormat), wantFresh: fixedNow.Add(10 * time.Minute), wantRetry: 90 * time.Second},
		{name: "far future Expires is bounded", expires: fixedNow.Add(10 * 365 * 24 * time.Hour).Format(http.TimeFormat), wantFresh: fixedNow.Add(365 * 24 * time.Hour)},
		{name: "no store", cacheControl: "no-store, max-age=300", wantFresh: fixedNow, noStore: true},
		{name: "no cache", cacheControl: "no-cache, max-age=300", wantFresh: fixedNow, revalidate: true},
		{name: "malformed max age", cacheControl: "max-age=garbage", expires: fixedNow.Add(time.Hour).Format(http.TimeFormat), wantFresh: fixedNow},
		{name: "response age reduces freshness", cacheControl: "max-age=300", date: fixedNow.Add(-4 * time.Minute).Format(http.TimeFormat), wantFresh: fixedNow.Add(time.Minute)},
		{name: "ancient Date safely expires bounded max age", cacheControl: "max-age=31536000", date: time.Date(1601, 1, 1, 0, 0, 0, 0, time.UTC).Format(http.TimeFormat), wantFresh: fixedNow},
		{name: "Age takes conservative maximum", cacheControl: "max-age=300", date: fixedNow.Add(-time.Minute).Format(http.TimeFormat), age: "240", wantFresh: fixedNow.Add(time.Minute)},
		{name: "Age at max age is stale", cacheControl: "max-age=300", age: "300", wantFresh: fixedNow},
		{name: "malformed Age is immediately stale", cacheControl: "max-age=300", age: "invalid", wantFresh: fixedNow},
		{name: "duplicate Age is immediately stale", cacheControl: "max-age=300", age: "1", ageAgain: "2", wantFresh: fixedNow},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Cache-Control", test.cacheControl)
				w.Header().Set("Expires", test.expires)
				responseDate := test.date
				if responseDate == "" {
					responseDate = fixedNow.Format(http.TimeFormat)
				}
				w.Header().Set("Date", responseDate)
				if test.age != "" {
					w.Header().Set("Age", test.age)
				}
				if test.ageAgain != "" {
					w.Header().Add("Age", test.ageAgain)
				}
				w.Header().Set("Retry-After", test.retryAfter)
				w.WriteHeader(http.StatusTooManyRequests)
			}))
			t.Cleanup(server.Close)
			client := newTestClient(t, egress.DefaultInternal(), &recordingRobots{}, func(options *Options) { options.MaxRetryAfter = 6 * time.Hour })
			result, err := client.Fetch(context.Background(), Request{URL: server.URL + "/source.xml", AllowedPathPrefixes: []string{"/"}})
			if err != nil {
				t.Fatal(err)
			}
			if !result.Freshness.FreshUntil.Equal(test.wantFresh) || result.Freshness.NoStore != test.noStore || result.Freshness.Revalidate != test.revalidate || result.RetryAfter != test.wantRetry {
				t.Fatalf("freshness=%+v retry=%s", result.Freshness, result.RetryAfter)
			}
		})
	}
}

func TestFetchEnforcesHeaderCapsAndDoesNotRetry(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("ETag", strings.Repeat("x", 65))
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)
	client := newTestClient(t, egress.DefaultInternal(), &recordingRobots{}, func(options *Options) {
		options.MaxHeaderValueBytes = 64
	})
	_, err := client.Fetch(context.Background(), Request{URL: server.URL + "/source.xml", AllowedPathPrefixes: []string{"/"}})
	assertReason(t, err, ReasonHeadersTooLarge)
	if hits.Load() != 1 {
		t.Fatalf("requests = %d, want exactly one", hits.Load())
	}
}

func TestFetchClassifiesTruncatedResponseAsTransportWithoutRetry(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		connection, buffered, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		defer connection.Close()
		_, _ = buffered.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: 100\r\n\r\n<urlset>")
		_ = buffered.Flush()
	}))
	t.Cleanup(server.Close)
	client := newTestClient(t, egress.DefaultInternal(), &recordingRobots{}, nil)
	_, err := client.Fetch(context.Background(), Request{URL: server.URL + "/source.xml", AllowedPathPrefixes: []string{"/"}})
	assertReason(t, err, ReasonTransport)
	if hits.Load() != 1 {
		t.Fatalf("requests = %d, want exactly one", hits.Load())
	}
}

func TestFetchRejectsInvalidRequestAndExternalPolicyBlocksLoopback(t *testing.T) {
	client := newTestClient(t, egress.DefaultInternal(), &recordingRobots{}, nil)
	for _, request := range []Request{
		{URL: "https://example.test/source.xml"},
		{URL: "https://user:secret@example.test/source.xml", AllowedPathPrefixes: []string{"/"}},
		{URL: "https://example.test/private/source.xml", AllowedPathPrefixes: []string{"/sources/"}},
		{URL: "https://example.test/source.xml", AllowedPathPrefixes: []string{"/../"}},
	} {
		_, err := client.Fetch(context.Background(), request)
		assertReason(t, err, ReasonInvalidRequest)
	}

	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	t.Cleanup(server.Close)
	external := newTestClient(t, egress.DefaultExternal(), &recordingRobots{}, nil)
	_, err := external.Fetch(context.Background(), Request{URL: server.URL + "/source.xml", AllowedPathPrefixes: []string{"/"}})
	assertReason(t, err, ReasonEgress)
	if hits.Load() != 0 {
		t.Fatal("blocked loopback request reached server")
	}
}

func TestFetchRejectsRequestBudgetsThatWidenOrInvalidateClientLimits(t *testing.T) {
	client := newTestClient(t, egress.DefaultInternal(), &recordingRobots{}, func(options *Options) {
		options.MaxWireBytes = 64
		options.MaxDecompressedBytes = 128
	})
	for _, request := range []Request{
		{URL: "https://example.test/source.xml", AllowedPathPrefixes: []string{"/"}, MaxWireBytes: -1},
		{URL: "https://example.test/source.xml", AllowedPathPrefixes: []string{"/"}, MaxDecompressedBytes: -1},
		{URL: "https://example.test/source.xml", AllowedPathPrefixes: []string{"/"}, MaxWireBytes: 65},
		{URL: "https://example.test/source.xml", AllowedPathPrefixes: []string{"/"}, MaxDecompressedBytes: 129},
	} {
		_, err := client.Fetch(context.Background(), request)
		assertReason(t, err, ReasonInvalidRequest)
	}
}

func TestNewRejectsUnboundedPolicyBudgets(t *testing.T) {
	base := Options{
		Policy: egress.DefaultInternal(), Robots: &recordingRobots{}, UserAgent: "FetchmarkCrawler/1.0",
	}
	for _, mutate := range []func(*Options){
		func(options *Options) { options.Timeout = 10*time.Minute + time.Nanosecond },
		func(options *Options) { options.MaxWireBytes = (50 << 20) + 1 },
		func(options *Options) { options.MaxDecompressedBytes = (50 << 20) + 1 },
		func(options *Options) { options.MaxResponseHeaderBytes = (1 << 20) + 1 },
		func(options *Options) { options.MaxHeaderValueBytes = (64 << 10) + 1 },
		func(options *Options) { options.MaxRetryAfter = 7*24*time.Hour + time.Nanosecond },
	} {
		options := base
		mutate(&options)
		_, err := New(options)
		assertReason(t, err, ReasonInvalidOptions)
	}
}
