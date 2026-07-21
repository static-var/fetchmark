package packevidence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/ccindex"
	"github.com/staticvar/fetchmark/internal/adapters/packevidencegate"
	"github.com/staticvar/fetchmark/internal/core/indexpackselection"
)

func TestObserverCollectsBodyFreeEvidenceAndCachesRobots(t *testing.T) {
	var robotsHits atomic.Int64
	var pageHits atomic.Int64
	var requestTimesMu sync.Mutex
	var requestTimes []time.Time
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestTimesMu.Lock()
		requestTimes = append(requestTimes, time.Now())
		requestTimesMu.Unlock()
		if got := request.Header.Get("User-Agent"); got != "FetchmarkPackEvidence/1 (+mailto:operator@example.org)" {
			t.Errorf("User-Agent = %q", got)
		}
		switch request.URL.Path {
		case "/robots.txt":
			robotsHits.Add(1)
			writer.Header().Set("Content-Type", "text/plain")
			_, _ = io.WriteString(writer, "User-agent: FetchmarkPackEvidence\nAllow: /\n")
		case "/page", "/second":
			pageHits.Add(1)
			writer.Header().Set("Content-Type", "text/html; charset=utf-8")
			writer.Header().Set("X-Robots-Tag", "noarchive")
			_, _ = io.WriteString(writer, `<html><head><meta name="robots" content="index"></head><body>secret page body</body></html>`)
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)

	const interval = 20 * time.Millisecond
	observer := testObserver(t, server, interval)
	rights := validRightsDecision()
	first, err := observer.Observe(context.Background(), 1, normalizedCandidate("https://example.org/page"), rights)
	if err != nil {
		t.Fatal(err)
	}
	second, err := observer.Observe(context.Background(), 2, normalizedCandidate("https://example.org/second"), rights)
	if err != nil {
		t.Fatal(err)
	}
	if first.Observation.Outcome != "admitted" || second.Observation.Outcome != "admitted" ||
		first.Candidate.Admission.Robots.Outcome != "allowed" || first.Candidate.Admission.Indexing.Outcome != "indexable" {
		t.Fatalf("first=%#v second=%#v", first, second)
	}
	if robotsHits.Load() != 1 || pageHits.Load() != 2 {
		t.Fatalf("robots hits=%d page hits=%d", robotsHits.Load(), pageHits.Load())
	}
	if strings.Contains(string(mustJSON(t, first.Observation)), "secret page body") {
		t.Fatal("observation retained page body")
	}
	wantPolicy := "User-agent: FetchmarkPackEvidence\nAllow: /\n"
	if string(first.PolicyBody) != wantPolicy || string(second.PolicyBody) != wantPolicy {
		t.Fatalf("policy bodies differ: %q %q", first.PolicyBody, second.PolicyBody)
	}
	wantPolicyDigest := sha256.Sum256([]byte(wantPolicy))
	if first.Candidate.Admission.Robots.BodySHA256 != hex.EncodeToString(wantPolicyDigest[:]) {
		t.Fatalf("policy digest = %q", first.Candidate.Admission.Robots.BodySHA256)
	}
	requestTimesMu.Lock()
	defer requestTimesMu.Unlock()
	for index := 1; index < len(requestTimes); index++ {
		if elapsed := requestTimes[index].Sub(requestTimes[index-1]); elapsed < interval-5*time.Millisecond {
			t.Fatalf("request %d started after %s, want at least %s", index, elapsed, interval)
		}
	}
}

func TestObserverRejectsNoIndexAndIncompleteRepresentations(t *testing.T) {
	tests := []struct {
		name       string
		page       http.HandlerFunc
		wantReason string
		wantIndex  string
	}{
		{
			name: "noindex header",
			page: func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "text/html")
				writer.Header().Set("X-Robots-Tag", "noindex")
				_, _ = io.WriteString(writer, "<p>body</p>")
			},
			wantReason: "indexing_not_permitted", wantIndex: "noindex",
		},
		{
			name: "truncated body",
			page: func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "text/html")
				writer.Header().Set("Content-Length", "100")
				_, _ = io.WriteString(writer, "short")
			},
			wantReason: "indexing_fetch_failed", wantIndex: "fetch_failed",
		},
		{
			name: "unsupported compression",
			page: func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "text/html")
				writer.Header().Set("Content-Encoding", "br")
				_, _ = io.WriteString(writer, "encoded")
			},
			wantReason: "indexing_fetch_failed", wantIndex: "fetch_failed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path == "/robots.txt" {
					_, _ = io.WriteString(writer, "User-agent: *\nAllow: /\n")
					return
				}
				test.page(writer, request)
			}))
			t.Cleanup(server.Close)
			observer := testObserver(t, server, 0)
			result, err := observer.Observe(context.Background(), 1, normalizedCandidate("https://example.org/page"), validRightsDecision())
			if err != nil {
				t.Fatal(err)
			}
			if result.Observation.Outcome != "rejected" || result.Observation.Reason != test.wantReason || result.Candidate.Admission.Indexing.Outcome != test.wantIndex {
				t.Fatalf("result = %#v", result)
			}
		})
	}
}

func TestObserverRobotsFailuresAreFailClosed(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		body        string
		wantAllow   bool
		wantOutcome string
	}{
		{name: "not found allows", status: http.StatusNotFound, body: strings.Repeat("ignored", 100_000), wantAllow: true, wantOutcome: "allowed"},
		{name: "server error rejects", status: http.StatusServiceUnavailable, body: "temporary", wantOutcome: "failed"},
		{name: "explicit disallow", status: http.StatusOK, body: "User-agent: *\nDisallow: /\n", wantOutcome: "disallowed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var pageHits atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path == "/robots.txt" {
					writer.WriteHeader(test.status)
					_, _ = io.WriteString(writer, test.body)
					return
				}
				pageHits.Add(1)
				writer.Header().Set("Content-Type", "text/html")
				_, _ = io.WriteString(writer, "<p>ok</p>")
			}))
			t.Cleanup(server.Close)
			observer := testObserver(t, server, 0)
			result, err := observer.Observe(context.Background(), 1, normalizedCandidate("https://example.org/page"), validRightsDecision())
			if err != nil {
				t.Fatal(err)
			}
			if result.Candidate.Admission.Robots.Outcome != test.wantOutcome {
				t.Fatalf("robots outcome = %q", result.Candidate.Admission.Robots.Outcome)
			}
			if (pageHits.Load() == 1) != test.wantAllow {
				t.Fatalf("page hits = %d", pageHits.Load())
			}
		})
	}
}

func TestObserverHonorsRobotsRetryAfterWithoutContactingPage(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		header string
		want   uint64
	}{
		{name: "delta seconds", header: "13", want: 13},
		{name: "HTTP date", header: now.Add(17 * time.Second).Format(http.TimeFormat), want: 17},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var pageHits atomic.Int64
			base := roundTripFunc(func(request *http.Request) (*http.Response, error) {
				if request.URL.Path != "/robots.txt" {
					pageHits.Add(1)
				}
				header := make(http.Header)
				header.Set("Retry-After", test.header)
				return responseFor(request, http.StatusTooManyRequests, header, ""), nil
			})
			observer := observerWithTransport(t, base, 0, 0, func() time.Time { return now })
			result, err := observer.Observe(context.Background(), 1, normalizedCandidate("https://example.org/page"), validRightsDecision())
			if err != nil || pageHits.Load() != 0 || result.Observation.Robots.Outcome != "failed" ||
				result.Observation.Robots.FailureReason != "robots_rate_limited" || result.Observation.Robots.RetryAfterSeconds != test.want {
				t.Fatalf("result=%#v page_hits=%d error=%v", result, pageHits.Load(), err)
			}
		})
	}
}

func TestObserverAppliesRobotsRedirectRetryAfterToRespondingHost(t *testing.T) {
	var targetRequests atomic.Int64
	base := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Hostname() == "origin.example.org" {
			return responseFor(request, http.StatusFound, http.Header{"Location": []string{"https://policy.example.net/limited"}}, ""), nil
		}
		targetRequests.Add(1)
		return responseFor(request, http.StatusTooManyRequests, http.Header{"Retry-After": []string{"2"}}, ""), nil
	})
	observer := observerWithTransport(t, base, 0, 0, func() time.Time {
		return time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	})
	first := normalizedCandidate("https://origin.example.org/page")
	first.URLKey = "org,example,origin)/page"
	result, err := observer.Observe(context.Background(), 1, first, validRightsDecision())
	if err != nil || result.Observation.Robots.RetryAfterSeconds != 2 || targetRequests.Load() != 1 {
		t.Fatalf("result=%#v target_requests=%d error=%v", result, targetRequests.Load(), err)
	}
	second := normalizedCandidate("https://policy.example.net/page")
	second.URLKey = "net,example,policy)/page"
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := observer.Observe(ctx, 2, second, validRightsDecision()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second error = %v", err)
	}
	if targetRequests.Load() != 1 {
		t.Fatalf("rate-limited host received %d requests, want 1", targetRequests.Load())
	}
}

func TestObserverAppliesPageRetryAfterBeforeNextCandidate(t *testing.T) {
	var pageRequests atomic.Int64
	base := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/robots.txt" {
			return responseFor(request, http.StatusNotFound, nil, ""), nil
		}
		pageRequests.Add(1)
		return responseFor(request, http.StatusTooManyRequests, http.Header{"Retry-After": []string{"2"}, "Content-Type": []string{"text/html"}}, ""), nil
	})
	observer := observerWithTransport(t, base, 0, 0, func() time.Time {
		return time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	})
	first, err := observer.Observe(context.Background(), 1, normalizedCandidate("https://example.org/page"), validRightsDecision())
	if err != nil || first.Observation.Indexing.RetryAfterSeconds != 2 || pageRequests.Load() != 1 {
		t.Fatalf("first=%#v page_requests=%d error=%v", first, pageRequests.Load(), err)
	}
	second := normalizedCandidate("https://example.org/second")
	second.URLKey = "org,example)/second"
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := observer.Observe(ctx, 2, second, validRightsDecision()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second error = %v", err)
	}
	if pageRequests.Load() != 1 {
		t.Fatalf("rate-limited host received %d page requests, want 1", pageRequests.Load())
	}
}

func TestCoordinatedTransportRecordsResponseBeforeBodyRelease(t *testing.T) {
	coordinator := &recordingCoordinator{}
	base := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return responseFor(request, http.StatusTooManyRequests, http.Header{"Retry-After": []string{"30"}}, ""), nil
	})
	observer := observerWithTransport(t, base, 0, time.Second, time.Now)
	observer.coordinator = coordinator
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.org/page", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := (&pacedTransport{base: base, observer: observer}).RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	if coordinator.recorded.Load() != 1 || coordinator.released.Load() != 0 {
		t.Fatalf("before close: recorded=%d released=%d", coordinator.recorded.Load(), coordinator.released.Load())
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if coordinator.released.Load() != 1 {
		t.Fatalf("release count = %d, want 1", coordinator.released.Load())
	}
}

func TestCoordinatedTransportFailsClosedWhenResponseCannotBeRecorded(t *testing.T) {
	coordinator := &recordingCoordinator{recordErr: errors.New("coordinator lost")}
	baseBody := &closeRecordingBody{Reader: strings.NewReader("")}
	base := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		response := responseFor(request, http.StatusOK, nil, "")
		response.Body = baseBody
		return response, nil
	})
	observer := observerWithTransport(t, base, 0, time.Second, time.Now)
	observer.coordinator = coordinator
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.org/page", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := (&pacedTransport{base: base, observer: observer}).RoundTrip(request)
	if response != nil || !errors.Is(err, ErrCoordination) {
		t.Fatalf("response=%v error=%v, want coordination failure", response, err)
	}
	if !baseBody.closed.Load() || coordinator.released.Load() != 1 {
		t.Fatalf("body_closed=%v released=%d", baseBody.closed.Load(), coordinator.released.Load())
	}
}

func TestObserverPacingQueueDoesNotConsumeNetworkTimeout(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		timeout  time.Duration
	}{
		{name: "interval greater than timeout", interval: 25 * time.Millisecond, timeout: 5 * time.Millisecond},
		{name: "interval equals timeout", interval: 15 * time.Millisecond, timeout: 15 * time.Millisecond},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := roundTripFunc(func(request *http.Request) (*http.Response, error) {
				if request.URL.Path == "/robots.txt" {
					return responseFor(request, http.StatusNotFound, nil, ""), nil
				}
				return responseFor(request, http.StatusOK, http.Header{"Content-Type": []string{"text/html; charset=utf-8"}}, "<p>ok</p>"), nil
			})
			observer := observerWithTransport(t, base, test.interval, test.timeout, func() time.Time {
				return time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
			})
			result, err := observer.Observe(context.Background(), 1, normalizedCandidate("https://example.org/page"), validRightsDecision())
			if err != nil || result.Observation.Outcome != "admitted" {
				t.Fatalf("result=%#v error=%v", result, err)
			}
		})
	}
}

func TestObserverRejectsUserinfoBeforeNetwork(t *testing.T) {
	var requests atomic.Int64
	observer := observerWithTransport(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests.Add(1)
		return responseFor(request, http.StatusOK, nil, ""), nil
	}), 0, 0, time.Now)
	result, err := observer.Observe(context.Background(), 1, normalizedCandidate("https://user:secret@example.org/page"), validRightsDecision())
	if err != nil || result.Observation.Reason != "non_public_url" || requests.Load() != 0 {
		t.Fatalf("result=%#v requests=%d error=%v", result, requests.Load(), err)
	}
}

func TestObserverRecordsExactEffectiveRobotsURI(t *testing.T) {
	const effective = "https://example.org/policy?utm_source=x&b=2&a=1"
	base := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case "/robots.txt":
			return responseFor(request, http.StatusFound, http.Header{"Location": []string{"/policy?utm_source=x&b=2&a=1"}}, ""), nil
		case "/policy":
			return responseFor(request, http.StatusOK, nil, "User-agent: *\nAllow: /\n"), nil
		default:
			return responseFor(request, http.StatusOK, http.Header{"Content-Type": []string{"text/html"}}, "<p>ok</p>"), nil
		}
	})
	observer := observerWithTransport(t, base, 0, 0, func() time.Time {
		return time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	})
	result, err := observer.Observe(context.Background(), 1, normalizedCandidate("https://example.org/page"), validRightsDecision())
	if err != nil || result.Observation.Outcome != "admitted" || result.Observation.Robots.FinalURI != effective {
		t.Fatalf("result=%#v error=%v", result, err)
	}
}

func TestObserverFollowsRobotsRedirectButDoesNotContactPageRedirectTarget(t *testing.T) {
	var policyHits atomic.Int64
	var targetHits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/robots.txt":
			http.Redirect(writer, request, "/policy", http.StatusFound)
		case "/policy":
			policyHits.Add(1)
			_, _ = io.WriteString(writer, "User-agent: *\nAllow: /\n")
		case "/page":
			http.Redirect(writer, request, "/target", http.StatusFound)
		case "/target":
			targetHits.Add(1)
			writer.Header().Set("Content-Type", "text/html")
			_, _ = io.WriteString(writer, "<p>must not be contacted</p>")
		}
	}))
	t.Cleanup(server.Close)
	observer := testObserver(t, server, 0)
	result, err := observer.Observe(context.Background(), 1, normalizedCandidate("https://example.org/page"), validRightsDecision())
	if err != nil {
		t.Fatal(err)
	}
	if policyHits.Load() != 1 || targetHits.Load() != 0 {
		t.Fatalf("policy hits=%d target hits=%d", policyHits.Load(), targetHits.Load())
	}
	if result.Observation.Robots.FinalURI != "https://example.org/policy" || result.Observation.Indexing.Status != http.StatusFound ||
		result.Observation.Indexing.RedirectLocation != "/target" || result.Observation.Reason != "indexing_not_permitted" {
		t.Fatalf("result = %#v", result)
	}
}

func TestObserverRejectsNoncanonicalURLBeforeNetwork(t *testing.T) {
	var requests atomic.Int64
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return nil, errors.New("must not run")
	})}
	observer, err := newObserver(client, "FetchmarkPackEvidence/1", time.Hour, 0, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	result, err := observer.Observe(context.Background(), 1, normalizedCandidate("https://EXAMPLE.org/page"), validRightsDecision())
	if err != nil || result.Observation.Reason != "noncanonical_url" || requests.Load() != 0 {
		t.Fatalf("result=%#v requests=%d error=%v", result, requests.Load(), err)
	}
}

func TestObserverDoesNotArchiveIncompleteRobotsRepresentation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/robots.txt" {
			writer.Header().Set("Content-Length", "100")
			_, _ = io.WriteString(writer, "short")
			return
		}
		t.Fatal("page contacted after incomplete robots response")
	}))
	t.Cleanup(server.Close)
	observer := testObserver(t, server, 0)
	result, err := observer.Observe(context.Background(), 1, normalizedCandidate("https://example.org/page"), validRightsDecision())
	if err != nil || result.Observation.Robots.BodyComplete || result.PolicyBodyComplete || result.Observation.Reason != "robots_not_allowed" {
		t.Fatalf("result=%#v error=%v", result, err)
	}
}

func TestReadCompleteRequiresCleanEOFAndExactLength(t *testing.T) {
	if body, complete, err := readComplete(strings.NewReader("okay"), 4, 4); err != nil || !complete || string(body) != "okay" {
		t.Fatalf("body=%q complete=%v err=%v", body, complete, err)
	}
	for name, test := range map[string]struct {
		reader  io.Reader
		length  int64
		maximum int
	}{
		"too large": {strings.NewReader("12345"), -1, 4},
		"short":     {strings.NewReader("123"), 4, 4},
	} {
		t.Run(name, func(t *testing.T) {
			if _, complete, err := readComplete(test.reader, test.length, test.maximum); err == nil || complete {
				t.Fatalf("complete=%v err=%v", complete, err)
			}
		})
	}
}

func TestRobotsCacheHasHardEntryAndByteBounds(t *testing.T) {
	observer, err := newObserver(&http.Client{}, "FetchmarkPackEvidence/1", time.Hour, 0, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 20<<10)
	for index := 0; index < 2*MaxRobotsCacheEntries; index++ {
		origin := fmt.Sprintf("https://host-%04d.example.org", index)
		observer.cacheRobots(origin, robotsResult{body: append([]byte(nil), body...)})
	}
	if len(observer.cache) > MaxRobotsCacheEntries || observer.cacheBytes > MaxRobotsCacheBytes || len(observer.cacheOrder) > MaxRobotsCacheEntries {
		t.Fatalf("cache entries=%d bytes=%d order=%d", len(observer.cache), observer.cacheBytes, len(observer.cacheOrder))
	}
}

type rewriteTransport struct {
	base   http.RoundTripper
	target *url.URL
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

type recordingCoordinator struct {
	recorded  atomic.Int64
	released  atomic.Int64
	recordErr error
}

func (coordinator *recordingCoordinator) Acquire(context.Context, string) (packevidencegate.Lease, error) {
	return packevidencegate.Lease{Host: "example.org", AttemptID: "attempt-1", Generation: 1, ExpiresAt: time.Now().Add(time.Hour)}, nil
}

func (coordinator *recordingCoordinator) RecordResponse(packevidencegate.Lease, int, string) (time.Duration, error) {
	coordinator.recorded.Add(1)
	return 0, coordinator.recordErr
}

func (coordinator *recordingCoordinator) Release(packevidencegate.Lease) error {
	coordinator.released.Add(1)
	return nil
}

type closeRecordingBody struct {
	io.Reader
	closed atomic.Bool
}

func (body *closeRecordingBody) Close() error {
	body.closed.Store(true)
	return nil
}

func (transport rewriteTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	rewritten := request.Clone(request.Context())
	rewritten.URL = new(url.URL)
	*rewritten.URL = *request.URL
	rewritten.URL.Scheme = transport.target.Scheme
	rewritten.URL.Host = transport.target.Host
	rewritten.Host = request.URL.Host
	response, err := transport.base.RoundTrip(rewritten)
	if response != nil {
		response.Request = request
	}
	return response, err
}

func testObserver(t *testing.T, server *httptest.Server, interval time.Duration) *Observer {
	t.Helper()
	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: rewriteTransport{base: server.Client().Transport, target: target}}
	now := func() time.Time { return time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC) }
	observer, err := newObserver(client, "FetchmarkPackEvidence/1 (+mailto:operator@example.org)", 12*time.Hour, interval, now)
	if err != nil {
		t.Fatal(err)
	}
	client.Transport = &pacedTransport{base: client.Transport, observer: observer}
	return observer
}

func observerWithTransport(t *testing.T, base http.RoundTripper, interval, timeout time.Duration, now func() time.Time) *Observer {
	t.Helper()
	client := &http.Client{Transport: base, Timeout: timeout}
	observer, err := newObserver(client, "FetchmarkPackEvidence/1 (+mailto:operator@example.org)", 12*time.Hour, interval, now)
	if err != nil {
		t.Fatal(err)
	}
	client.Transport = &pacedTransport{base: base, observer: observer}
	return observer
}

func responseFor(request *http.Request, status int, header http.Header, body string) *http.Response {
	if header == nil {
		header = make(http.Header)
	}
	return &http.Response{
		StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)), Header: header,
		Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)), Request: request,
	}
}

func normalizedCandidate(rawURL string) ccindex.Candidate {
	return ccindex.Candidate{
		Version: 1, URLKey: "org,example)/page", Timestamp: "20260718120000", URL: rawURL,
		MIME: "text/html", MIMEDetected: "text/html", Status: "200",
		Digest: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", Length: "100", Offset: "0",
		Filename: "crawl-data/CC-MAIN-2026-30/segments/x/warc/file.warc.gz", Languages: "en",
	}
}

func validRightsDecision() indexpackselection.RightsDecision {
	return indexpackselection.RightsDecision{
		Outcome: "permitted", AllowedFields: []string{"url_metadata"}, Basis: "url_metadata_policy",
		EvidenceURI: "https://commoncrawl.org/terms-of-use", EvidenceSHA256: strings.Repeat("a", 64),
		ObservedAt: "2026-07-19T12:00:00Z", ValidUntil: "2026-07-20T00:00:00Z", RightsNotice: "URL metadata only",
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
