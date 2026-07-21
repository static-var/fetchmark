package corpusclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewValidatesFixedFetchmarkOrigin(t *testing.T) {
	t.Parallel()

	invalid := []string{
		"",
		"ftp://fetchmark.example",
		"https://",
		"https://user:secret@fetchmark.example",
		"https://fetchmark.example/base",
		"https://fetchmark.example?mode=curated",
		"https://fetchmark.example#fragment",
	}
	for _, endpoint := range invalid {
		endpoint := endpoint
		t.Run(endpoint, func(t *testing.T) {
			t.Parallel()
			if _, err := New(Options{BaseURL: endpoint, APIKey: "key"}); err == nil {
				t.Fatalf("New(%q) succeeded", endpoint)
			}
		})
	}

	for _, endpoint := range []string{"http://fetchmark.example", "https://fetchmark.example/"} {
		client, err := New(Options{BaseURL: endpoint, APIKey: "key"})
		if err != nil {
			t.Fatalf("New(%q): %v", endpoint, err)
		}
		if client.maxResponseBytes != 1<<20 {
			t.Fatalf("default max response bytes = %d", client.maxResponseBytes)
		}
	}
}

func TestAdmitSendsExactURLAndPolicyRequest(t *testing.T) {
	t.Parallel()

	var gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/admin/corpus/focused-admissions" || r.URL.RawQuery != "" {
			t.Errorf("request = %s %s", r.Method, r.URL.String())
		}
		if got := r.Header.Get("X-API-Key"); got != "admin-secret" {
			t.Errorf("X-API-Key = %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q", got)
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		gotBody = string(raw)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"count":1,"results":[{"url":"https://one.example/","status":"admitted","outbound_links":["https://reference.example/one","https://reference.example/two"]}]}`)
	}))
	defer server.Close()

	client, err := New(Options{BaseURL: server.URL, APIKey: "admin-secret"})
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Admit(context.Background(), FocusedAdmission{
		URL: "https://one.example/", AllowedPathPrefixes: []string{"/docs/"},
		DeniedPathPrefixes: []string{"/docs/private/"}, DeniedURLs: []string{"https://one.example/docs/blocked"},
		MaxOutboundLinks: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotBody != `{"url":"https://one.example/","allowed_path_prefixes":["/docs/"],"denied_path_prefixes":["/docs/private/"],"denied_urls":["https://one.example/docs/blocked"],"max_outbound_links":2}` {
		t.Fatalf("body = %s", gotBody)
	}
	if response.Count != 1 || len(response.Results) != 1 || response.Results[0].OutboundLinks == nil || len(*response.Results[0].OutboundLinks) != 2 {
		t.Fatalf("response = %+v", response)
	}
}

func TestAdmitPreservesAuthoritativeEmptyOutboundLinkSnapshot(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"count":1,"results":[{"url":"https://example.com/","status":"admitted","outbound_links":[]}]}`)
	}))
	defer server.Close()
	client, err := New(Options{BaseURL: server.URL, APIKey: "key"})
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Admit(context.Background(), FocusedAdmission{
		URL: "https://example.com/", AllowedPathPrefixes: []string{"/"}, MaxOutboundLinks: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Results[0].OutboundLinks == nil || *response.Results[0].OutboundLinks == nil || len(*response.Results[0].OutboundLinks) != 0 {
		t.Fatalf("outbound links = %#v", response.Results[0].OutboundLinks)
	}
}

func TestAdmitRefusesRedirectWithoutLeakingAPIKey(t *testing.T) {
	t.Parallel()

	var targetCalls atomic.Int32
	var targetKey atomic.Value
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls.Add(1)
		targetKey.Store(r.Header.Get("X-API-Key"))
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/stolen", http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()

	client, err := New(Options{BaseURL: redirector.URL, APIKey: "never-forward"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Admit(context.Background(), FocusedAdmission{URL: "https://example.com/", AllowedPathPrefixes: []string{"/"}})
	assertClass(t, err, FailurePermanent, http.StatusTemporaryRedirect)
	if got := targetCalls.Load(); got != 0 {
		t.Fatalf("redirect target received %d requests, key=%v", got, targetKey.Load())
	}
}

func TestAdmitClassifiesHTTPAndRetryAfter(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 18, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		status     int
		retryAfter string
		wantClass  FailureClass
		wantRetry  time.Duration
	}{
		{name: "unauthorized", status: http.StatusUnauthorized, wantClass: FailureFatal},
		{name: "forbidden", status: http.StatusForbidden, wantClass: FailureFatal},
		{name: "conflict", status: http.StatusConflict, wantClass: FailureFatal},
		{name: "bad request", status: http.StatusBadRequest, wantClass: FailurePermanent},
		{name: "not found", status: http.StatusNotFound, wantClass: FailurePermanent},
		{name: "rate limited seconds", status: http.StatusTooManyRequests, retryAfter: "45", wantClass: FailureTransient, wantRetry: 45 * time.Second},
		{name: "rate limited date", status: http.StatusTooManyRequests, retryAfter: now.Add(2 * time.Hour).Format(http.TimeFormat), wantClass: FailureTransient, wantRetry: 2 * time.Hour},
		{name: "server error", status: http.StatusBadGateway, wantClass: FailureTransient},
		{name: "bounded retry", status: http.StatusServiceUnavailable, retryAfter: "999999999999", wantClass: FailureTransient, wantRetry: 24 * time.Hour},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Retry-After", test.retryAfter)
				w.WriteHeader(test.status)
			}))
			defer server.Close()
			client, err := New(Options{BaseURL: server.URL, APIKey: "key"})
			if err != nil {
				t.Fatal(err)
			}
			client.now = func() time.Time { return now }
			_, err = client.Admit(context.Background(), FocusedAdmission{URL: "https://example.com/", AllowedPathPrefixes: []string{"/"}})
			admissionErr := assertClass(t, err, test.wantClass, test.status)
			if admissionErr.RetryAfter != test.wantRetry {
				t.Fatalf("RetryAfter = %v, want %v", admissionErr.RetryAfter, test.wantRetry)
			}
		})
	}
}

func TestAdmitClassifiesTransportFailureAsTransient(t *testing.T) {
	t.Parallel()

	client, err := New(Options{
		BaseURL: "https://fetchmark.example",
		APIKey:  "key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("dial failed")
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Admit(context.Background(), FocusedAdmission{URL: "https://example.com/", AllowedPathPrefixes: []string{"/"}})
	assertClass(t, err, FailureTransient, 0)
}

func TestAdmitAppliesRequestTimeout(t *testing.T) {
	t.Parallel()

	const timeout = 20 * time.Millisecond
	client, err := New(Options{
		BaseURL: "https://fetchmark.example",
		APIKey:  "key",
		Timeout: timeout,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			deadline, ok := request.Context().Deadline()
			if !ok || time.Until(deadline) > timeout+10*time.Millisecond {
				return nil, errors.New("request deadline missing or too late")
			}
			<-request.Context().Done()
			return nil, request.Context().Err()
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Admit(context.Background(), FocusedAdmission{URL: "https://example.com/", AllowedPathPrefixes: []string{"/"}})
	assertClass(t, err, FailureTransient, 0)
}

func TestAdmitRejectsOversizedAndMalformedResponses(t *testing.T) {
	t.Parallel()
	manyLinks := make([]string, 0, maxOutboundLinks+1)
	for index := 0; index <= maxOutboundLinks; index++ {
		manyLinks = append(manyLinks, `"https://example.com/`+strconv.Itoa(index)+`"`)
	}

	tests := []struct {
		name             string
		body             string
		max              int64
		maxOutboundLinks int
	}{
		{name: "oversized", body: strings.Repeat("x", 33), max: 32},
		{name: "unknown field", body: `{"count":0,"results":[],"extra":true}`, max: 1024},
		{name: "trailing value", body: `{"count":0,"results":[]} {}`, max: 1024},
		{name: "count mismatch", body: `{"count":2,"results":[{"url":"https://example.com/","status":"admitted"}]}`, max: 1024},
		{name: "unknown status", body: `{"count":1,"results":[{"url":"https://example.com/","status":"queued"}]}`, max: 1024},
		{name: "invalid result URL", body: `{"count":1,"results":[{"url":"https://user:secret@example.com/","status":"admitted"}]}`, max: 1024},
		{name: "different result URL", body: `{"count":1,"results":[{"url":"https://other.example/","status":"admitted"}]}`, max: 1024},
		{name: "unrequested outbound links", body: `{"count":1,"results":[{"url":"https://example.com/","status":"admitted","outbound_links":[]}]}`, max: 1024},
		{name: "over requested outbound links", body: `{"count":1,"results":[{"url":"https://example.com/","status":"admitted","outbound_links":["https://example.com/one","https://example.com/two"]}]}`, max: 1024, maxOutboundLinks: 1},
		{name: "over absolute outbound link cap", body: `{"count":1,"results":[{"url":"https://example.com/","status":"admitted","outbound_links":[` + strings.Join(manyLinks, ",") + `]}]}`, max: 16 << 10, maxOutboundLinks: maxOutboundLinks},
		{name: "duplicate outbound link", body: `{"count":1,"results":[{"url":"https://example.com/","status":"admitted","outbound_links":["https://example.com/one","https://example.com/one"]}]}`, max: 1024, maxOutboundLinks: 2},
		{name: "noncanonical outbound link", body: `{"count":1,"results":[{"url":"https://example.com/","status":"admitted","outbound_links":["https://EXAMPLE.com:443/one#fragment"]}]}`, max: 1024, maxOutboundLinks: 1},
		{name: "credentialed outbound link", body: `{"count":1,"results":[{"url":"https://example.com/","status":"admitted","outbound_links":["https://user:secret@example.com/one"]}]}`, max: 1024, maxOutboundLinks: 1},
		{name: "non HTTP outbound link", body: `{"count":1,"results":[{"url":"https://example.com/","status":"admitted","outbound_links":["ftp://example.com/one"]}]}`, max: 1024, maxOutboundLinks: 1},
		{name: "oversized outbound link", body: `{"count":1,"results":[{"url":"https://example.com/","status":"admitted","outbound_links":["https://example.com/` + strings.Repeat("x", 2049) + `"]}]}`, max: 4096, maxOutboundLinks: 1},
		{name: "rejected result with outbound snapshot", body: `{"count":1,"results":[{"url":"https://example.com/","status":"rejected","outbound_links":[]}]}`, max: 1024, maxOutboundLinks: 1},
		{name: "admitted result with null outbound snapshot", body: `{"count":1,"results":[{"url":"https://example.com/","status":"admitted","outbound_links":null}]}`, max: 1024, maxOutboundLinks: 1},
		{name: "rejected result with null outbound snapshot", body: `{"count":1,"results":[{"url":"https://example.com/","status":"rejected","outbound_links":null}]}`, max: 1024, maxOutboundLinks: 1},
		{name: "failed result with null outbound snapshot", body: `{"count":1,"results":[{"url":"https://example.com/","status":"failed","outbound_links":null}]}`, max: 1024, maxOutboundLinks: 1},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			client, err := New(Options{BaseURL: server.URL, APIKey: "key", MaxResponseBytes: test.max})
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.Admit(context.Background(), FocusedAdmission{
				URL: "https://example.com/", AllowedPathPrefixes: []string{"/"}, MaxOutboundLinks: test.maxOutboundLinks,
			})
			assertClass(t, err, FailurePermanent, http.StatusOK)
		})
	}
}

func TestAdmitRejectsOversizedPolicyBeforeNetwork(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	client, err := New(Options{
		BaseURL: "https://fetchmark.example", APIKey: "key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return nil, errors.New("must not be called")
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	policy := FocusedAdmission{URL: "https://example.com/", AllowedPathPrefixes: []string{"/" + strings.Repeat("x", maxAdmissionBytes)}}
	if _, err := client.Admit(context.Background(), policy); err == nil || calls.Load() != 0 {
		t.Fatalf("error=%v calls=%d", err, calls.Load())
	}
}

func TestAdmitRejectsEmptyURLs(t *testing.T) {
	t.Parallel()

	client, err := New(Options{BaseURL: "https://fetchmark.example", APIKey: "key"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Admit(context.Background(), FocusedAdmission{}); err == nil {
		t.Fatal("Admit(nil) succeeded")
	}
}

func TestAdmitRejectsInvalidOutboundLinkCapsBeforeNetwork(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	client, err := New(Options{
		BaseURL: "https://fetchmark.example", APIKey: "key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return nil, errors.New("must not be called")
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{-1, maxOutboundLinks + 1} {
		_, err := client.Admit(context.Background(), FocusedAdmission{
			URL: "https://example.com/", AllowedPathPrefixes: []string{"/"}, MaxOutboundLinks: limit,
		})
		if err == nil {
			t.Fatalf("Admit(max_outbound_links=%d) succeeded", limit)
		}
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("network calls = %d", got)
	}
}

func assertClass(t *testing.T, err error, want FailureClass, status int) *AdmissionError {
	t.Helper()
	if err == nil {
		t.Fatal("expected error")
	}
	var admissionErr *AdmissionError
	if !errors.As(err, &admissionErr) {
		t.Fatalf("error %T is not *AdmissionError: %v", err, err)
	}
	if admissionErr.Class != want || admissionErr.StatusCode != status {
		t.Fatalf("error = %+v, want class=%q status=%d", admissionErr, want, status)
	}
	return admissionErr
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}
