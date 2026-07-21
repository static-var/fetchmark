package yacy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/search"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestClientSearchBatchMapsBoundedTextResults(t *testing.T) {
	var observed *http.Request
	client := mustNew(t, testOptions(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		observed = request.Clone(request.Context())
		return jsonResponse(http.StatusOK, `{"channels":[{"link":"http://yacy:8090/yacysearch.html?query=open&amp;resource=global","items":[
			{"title":"Independent search","link":"https://example.org/search","description":"An <b>open</b> index","pubDate":"Mon, 13 Jul 2026 10:30:00 GMT","host":"example.org","ranking":"0.91"},
			{"title":"Second result","link":"https://example.net/two","description":"Two"},
			{"title":"Beyond cap","link":"https://example.com/three","description":"Three"}
		]}]}`), nil
	})))
	client.maxResults = 2

	batch, err := client.SearchBatch(context.Background(), search.Query{Q: `Open "search" OR resource:global`, MaxResults: 50})
	if err != nil {
		t.Fatal(err)
	}
	if observed == nil || observed.Method != http.MethodGet || observed.URL.Scheme != "http" || observed.URL.Host != "yacy:8090" || observed.URL.Path != "/yacysearch.json" {
		t.Fatalf("request = %v", observed)
	}
	values := observed.URL.Query()
	if values.Get("query") != "open search resource global" || values.Get("resource") != "global" || values.Get("contentdom") != "text" || values.Get("maximumRecords") != "2" || values.Get("startRecord") != "0" || values.Get("verify") != "false" || values.Get("nav") != "none" || values.Get("urlmaskfilter") != ".*" {
		t.Fatalf("query values = %v", values)
	}
	if observed.Header.Get("Accept") != "application/json" || !strings.Contains(observed.Header.Get("User-Agent"), "Fetchmark-Test") {
		t.Fatalf("headers = %v", observed.Header)
	}
	if batch.Provider != "yacy" || batch.Instance != "yacy:8090" || batch.Status != search.BatchHealthy || len(batch.Hits) != 2 {
		t.Fatalf("batch = %+v", batch)
	}
	first := batch.Hits[0]
	if first.Title != "Independent search" || first.Snippet != "An open index" || first.Metadata["source"] != "YaCy" || first.Metadata["yacy_resource"] != "global" || first.Metadata["yacy_ranking"] != "0.91" || first.PublishedAt == nil || len(first.Engines) != 1 || first.Engines[0] != "yacy" {
		t.Fatalf("first hit = %+v", first)
	}
}

func TestClientClassifiesLocalAndGlobalEmptyDifferently(t *testing.T) {
	for _, test := range []struct {
		resource string
		want     search.BatchStatus
		reason   string
	}{
		{resource: "local", want: search.BatchAuthoritativeEmpty},
		{resource: "global", want: search.BatchDegradedEmpty, reason: "global_empty"},
	} {
		t.Run(test.resource, func(t *testing.T) {
			options := testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
				return jsonResponse(http.StatusOK, `{"channels":[{"link":"http://yacy:8090/yacysearch.html?resource=`+test.resource+`","items":[]}]}`), nil
			}))
			options.Resource = test.resource
			client := mustNew(t, options)
			batch, err := client.SearchBatch(context.Background(), search.Query{Q: "missing result"})
			if err != nil || batch.Status != test.want {
				t.Fatalf("batch = %+v, err = %v", batch, err)
			}
			if test.reason == "" && len(batch.Diagnostics) != 0 {
				t.Fatalf("diagnostics = %+v", batch.Diagnostics)
			}
			if test.reason != "" && (len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Reason != test.reason) {
				t.Fatalf("diagnostics = %+v", batch.Diagnostics)
			}
		})
	}
}

func TestClientReportsGlobalDowngradeAndKeepsValidHits(t *testing.T) {
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{"channels":[{"link":"http://yacy:8090/yacysearch.html?resource=local","items":[{"title":"Local fallback","link":"https://example.org/"}]}]}`), nil
	})))
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "peer query"})
	if err != nil || batch.Status != search.BatchPartial || len(batch.Hits) != 1 || len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Reason != "global_downgraded" {
		t.Fatalf("batch = %+v, err = %v", batch, err)
	}
	if batch.Hits[0].Metadata["yacy_requested_resource"] != "global" || batch.Hits[0].Metadata["yacy_effective_resource"] != "local" || batch.Hits[0].Metadata["yacy_resource"] != "local" {
		t.Fatalf("resource metadata = %+v", batch.Hits[0].Metadata)
	}
}

func TestClientDegradesUnverifiedResourceEvidence(t *testing.T) {
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{"channels":[{"items":[]}]}`), nil
	})))
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "unverified"})
	if err != nil || batch.Status != search.BatchDegradedEmpty || len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Reason != "resource_unverified" {
		t.Fatalf("batch = %+v, err = %v", batch, err)
	}
}

func TestClientAppliesDomainFiltersAndRejectsMalformedRows(t *testing.T) {
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{"channels":[{"link":"http://yacy:8090/yacysearch.html?resource=global","items":[
			{"title":"Keep","link":"https://docs.example.org/guide"},
			{"title":"Excluded","link":"https://other.example.net/"},
			{"title":"Bad","link":"javascript:alert(1)"}
		]}]}`), nil
	})))
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "domain", IncludeDomains: []string{"example.org"}})
	if err != nil || batch.Status != search.BatchPartial || len(batch.Hits) != 1 || batch.Hits[0].URL != "https://docs.example.org/guide" || len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Reason != "malformed_results" {
		t.Fatalf("batch = %+v, err = %v", batch, err)
	}
}

func TestClientRejectsUnsupportedControlsBeforeNetwork(t *testing.T) {
	var calls atomic.Int32
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return jsonResponse(http.StatusOK, `{}`), nil
	})))
	strict := 2
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "query", SafeSearch: &strict})
	var unsupported *search.UnsupportedControlError
	if !errors.As(err, &unsupported) || unsupported.Control != "safe_search" || batch.Status != search.BatchFailed || calls.Load() != 0 {
		t.Fatalf("batch = %+v, err = %v, calls = %d", batch, err, calls.Load())
	}
}

func TestNewRequiresExplicitBoundedOriginAndResource(t *testing.T) {
	valid := testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{}`), nil
	}))
	for _, test := range []struct {
		name   string
		mutate func(*Options)
	}{
		{name: "missing endpoint", mutate: func(options *Options) { options.Endpoint = "" }},
		{name: "path", mutate: func(options *Options) { options.Endpoint = "https://search.example/yacy" }},
		{name: "userinfo", mutate: func(options *Options) { options.Endpoint = "https://user:pass@search.example" }},
		{name: "query", mutate: func(options *Options) { options.Endpoint = "https://search.example?token=secret" }},
		{name: "insecure not allowed", mutate: func(options *Options) { options.AllowInsecureHTTP = false }},
		{name: "bad resource", mutate: func(options *Options) { options.Resource = "remote" }},
		{name: "missing client", mutate: func(options *Options) { options.HTTPClient = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := valid
			test.mutate(&options)
			if _, err := New(options); err == nil {
				t.Fatal("New succeeded")
			}
		})
	}
}

func TestNewCapsBudgetsAndAllowsHTTPSWithoutInsecureOptIn(t *testing.T) {
	options := testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{}`), nil
	}))
	options.Endpoint = "https://search.example"
	options.AllowInsecureHTTP = false
	options.MaxResults = 1000
	client := mustNew(t, options)
	if client.maxResults != maxProviderResults || client.budget.Burst() != 1 || client.budget.MaxConcurrency() != 1 {
		t.Fatalf("caps = results %d burst %d concurrency %d", client.maxResults, client.budget.Burst(), client.budget.MaxConcurrency())
	}
}

func TestClientRefusesRedirectWithoutFollowing(t *testing.T) {
	var calls atomic.Int32
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		response := jsonResponse(http.StatusFound, `{}`)
		response.Header.Set("Location", "http://other.internal/yacysearch.json")
		return response, nil
	})))
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "redirect"})
	var statusError *StatusError
	if !errors.As(err, &statusError) || statusError.StatusCode != http.StatusFound || batch.Status != search.BatchFailed || calls.Load() != 1 {
		t.Fatalf("batch = %+v, err = %v, calls = %d", batch, err, calls.Load())
	}
}

func TestClientTransportRejectsOffOriginRequestBeforeBase(t *testing.T) {
	var calls atomic.Int32
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return jsonResponse(http.StatusOK, `{}`), nil
	})))
	request, err := http.NewRequest(http.MethodGet, "http://other.internal/yacysearch.json", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.httpClient.Transport.RoundTrip(request); err == nil || !strings.Contains(err.Error(), "outside configured origin") {
		t.Fatalf("RoundTrip error = %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("base transport calls = %d", calls.Load())
	}
}

func TestClientHonorsRetryAfterAndCooldown(t *testing.T) {
	var calls atomic.Int32
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		response := jsonResponse(http.StatusTooManyRequests, `{}`)
		response.Header.Set("Retry-After", "3")
		return response, nil
	})))
	client.now = func() time.Time { return time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC) }
	for range 2 {
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "limited"})
		var statusError *StatusError
		if !errors.As(err, &statusError) || statusError.RetryAfter != 3*time.Second || len(batch.Diagnostics) != 1 || batch.Diagnostics[0].RetryAfter != 3*time.Second {
			t.Fatalf("batch = %+v, err = %v", batch, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}

func testOptions(transport http.RoundTripper) Options {
	return Options{
		Endpoint: "http://yacy:8090", HTTPClient: &http.Client{Transport: transport},
		UserAgent: "Fetchmark-Test/1.0 (+https://example.test/contact)", Resource: "global",
		AllowInsecureHTTP: true, MaxResults: 10, MaxBodyBytes: 1 << 20,
		RatePerSecond: 100, Burst: 10, MaxConcurrency: 10,
	}
}

func mustNew(t *testing.T, options Options) *Client {
	t.Helper()
	client, err := New(options)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return client
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}
