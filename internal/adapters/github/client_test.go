package github

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

const testUserAgent = "Fetchmark/0.1 (+https://fetchmark.example/contact)"

func TestSearchBatchBuildsPinnedRepositoryRequestAndMapsMetadata(t *testing.T) {
	var observed *http.Request
	client := newTestClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		observed = request.Clone(request.Context())
		return jsonResponse(http.StatusOK, `{
  "total_count": 2,
  "incomplete_results": false,
  "items": [
    {
      "id": 123,
      "full_name": "golang/go",
      "html_url": "https://github.com/golang/go",
      "description": "The Go programming language",
      "topics": ["go", "compiler"],
      "license": {"spdx_id": "BSD-3-Clause", "name": "BSD 3-Clause"},
      "language": "Go",
      "updated_at": "2026-07-18T12:00:00Z",
      "pushed_at": "2026-07-18T10:00:00Z",
      "stargazers_count": 130000,
      "forks_count": 18000,
      "archived": false,
      "disabled": false,
      "visibility": "public",
      "score": 42.5,
      "owner": {"login": "golang"}
    },
    {
      "id": 456,
      "full_name": "rust-lang/rust",
      "html_url": "https://github.com/rust-lang/rust",
      "description": null,
      "topics": [],
      "license": null,
      "language": "Rust",
      "updated_at": "2026-07-17T12:00:00Z",
      "pushed_at": "2026-07-17T10:00:00Z",
      "stargazers_count": 100000,
      "forks_count": 13000,
      "archived": false,
      "disabled": false,
      "visibility": "public",
      "score": 40,
      "owner": {"login": "rust-lang"}
    }
  ]
}`), nil
	}))

	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "How do errors.Is and errors.As work in Go?", MaxResults: 2})
	if err != nil {
		t.Fatal(err)
	}
	if observed == nil {
		t.Fatal("request was not observed")
	}
	if observed.URL.Scheme != "https" || observed.URL.Host != "api.github.com" || observed.URL.Path != "/search/repositories" {
		t.Fatalf("request URL = %s", observed.URL)
	}
	if got := observed.URL.Query().Get("q"); strings.Contains(got, "?") || strings.Contains(got, "How do") || !strings.Contains(got, "errors.Is") || !strings.Contains(got, "is:public") {
		t.Fatalf("projected q = %q", got)
	}
	if observed.URL.Query().Get("per_page") != "2" || observed.URL.Query().Get("page") != "1" {
		t.Fatalf("paging query = %s", observed.URL.RawQuery)
	}
	if got := observed.Header.Get("Accept"); got != "application/vnd.github+json" {
		t.Fatalf("Accept = %q", got)
	}
	if got := observed.Header.Get("X-GitHub-Api-Version"); got != apiVersion {
		t.Fatalf("API version = %q", got)
	}
	if got := observed.Header.Get("User-Agent"); got != testUserAgent {
		t.Fatalf("User-Agent = %q", got)
	}
	if batch.Status != search.BatchHealthy || batch.Provider != "github" || batch.Instance != "api.github.com" || len(batch.Hits) != 2 {
		t.Fatalf("batch = %+v", batch)
	}
	hit := batch.Hits[0]
	if hit.URL != "https://github.com/golang/go" || hit.Title != "golang/go" || hit.Snippet != "The Go programming language" {
		t.Fatalf("hit = %+v", hit)
	}
	if hit.PublishedAt != nil {
		t.Fatalf("repository activity must not be publication time: %v", hit.PublishedAt)
	}
	if hit.Metadata["source"] != "GitHub" || hit.Metadata["license_spdx"] != "BSD-3-Clause" || hit.Metadata["topics"] != "compiler,go" || hit.Metadata["pushed_at"] != "2026-07-18T10:00:00Z" {
		t.Fatalf("metadata = %#v", hit.Metadata)
	}
}

func TestSearchBatchClassifiesEmptyAndIncompleteResponses(t *testing.T) {
	tests := []struct {
		name       string
		resource   string
		wantStatus search.BatchStatus
		wantError  bool
	}{
		{name: "authoritative empty", resource: `{"total_count":0,"incomplete_results":false,"items":[]}`, wantStatus: search.BatchAuthoritativeEmpty},
		{name: "incomplete empty", resource: `{"total_count":10,"incomplete_results":true,"items":[]}`, wantStatus: search.BatchDegradedEmpty},
		{name: "incomplete with hit", resource: `{"total_count":10,"incomplete_results":true,"items":[{"id":1,"full_name":"owner/repo","html_url":"https://github.com/owner/repo","owner":{"login":"owner"},"visibility":"public"}]}`, wantStatus: search.BatchPartial},
		{name: "positive count missing items", resource: `{"total_count":10,"incomplete_results":false,"items":[]}`, wantStatus: search.BatchDegradedEmpty},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newTestClient(t, staticJSONTransport(test.resource))
			batch, err := client.SearchBatch(context.Background(), search.Query{Q: "compiler"})
			if (err != nil) != test.wantError {
				t.Fatalf("error = %v", err)
			}
			if batch.Status != test.wantStatus {
				t.Fatalf("status = %q, want %q", batch.Status, test.wantStatus)
			}
		})
	}
}

func TestSearchBatchRetainsUsableRowsAsPartial(t *testing.T) {
	client := newTestClient(t, staticJSONTransport(`{
  "total_count": 2,
  "incomplete_results": false,
  "items": [
    {"id":1,"full_name":"owner/repo","html_url":"https://github.com/owner/repo","owner":{"login":"owner"},"visibility":"public"},
    {"id":2,"full_name":"bad/repo","html_url":"https://github.com:444/bad/repo","owner":{"login":"bad"},"visibility":"public"}
  ]
}`))
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "repo"})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Status != search.BatchPartial || len(batch.Hits) != 1 || len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Reason != "malformed_results" {
		t.Fatalf("batch = %+v", batch)
	}
}

func TestSearchBatchRejectsUnsupportedControlsWithoutNetwork(t *testing.T) {
	var calls atomic.Int32
	client := newTestClient(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("unexpected network call")
	}))
	one := 1
	queries := []search.Query{
		{Q: "x", Engines: []string{"github"}},
		{Q: "x", Categories: []string{"code"}},
		{Q: "x", Language: "en"},
		{Q: "x", SafeSearch: &one},
		{Q: "x", ExactMatch: true},
	}
	for _, query := range queries {
		batch, err := client.SearchBatch(context.Background(), query)
		var unsupported *search.UnsupportedControlError
		if !errors.As(err, &unsupported) || batch.Status != search.BatchFailed {
			t.Fatalf("query %+v: batch=%+v err=%v", query, batch, err)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("network calls = %d", calls.Load())
	}
}

func TestSearchBatchMapsFreshnessToRepositoryActivity(t *testing.T) {
	var observed *http.Request
	client := newTestClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		observed = request.Clone(request.Context())
		return jsonResponse(http.StatusOK, `{"total_count":0,"incomplete_results":false,"items":[]}`), nil
	}))
	client.now = func() time.Time { return time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC) }
	if _, err := client.SearchBatch(context.Background(), search.Query{Q: "release", TimeRange: "week"}); err != nil {
		t.Fatal(err)
	}
	if got := observed.URL.Query().Get("q"); !strings.Contains(got, "pushed:>=2026-07-12") {
		t.Fatalf("projected q = %q", got)
	}
}

func TestProjectQueryCannotInjectBooleanOrFieldOperators(t *testing.T) {
	projected, err := projectQuery(`errors.Is OR repo:attacker/private NOT archived:true`)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(projected, " OR ") || strings.Contains(projected, " NOT ") || strings.Contains(projected, "repo:") || strings.Contains(projected, "archived:true archived:false") {
		t.Fatalf("projected query retained caller operators: %q", projected)
	}
	if !strings.Contains(projected, "errors.Is") || !strings.Contains(projected, "is:public archived:false mirror:false") {
		t.Fatalf("projected query lost bounded terms or adapter qualifiers: %q", projected)
	}
}

func TestSearchBatchHonorsRateLimitResetAndCooldown(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	var calls atomic.Int32
	client := newTestClient(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		response := jsonResponse(http.StatusForbidden, `{}`)
		response.Header.Set("X-RateLimit-Remaining", "0")
		response.Header.Set("X-RateLimit-Reset", "1784462460") // now + 60s
		return response, nil
	}))
	client.now = func() time.Time { return now }
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "compiler"})
	var statusErr *StatusError
	if !errors.As(err, &statusErr) || statusErr.RetryAfter != time.Minute || batch.Diagnostics[0].RetryAfter != time.Minute {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
	if _, err := client.SearchBatch(context.Background(), search.Query{Q: "other"}); !errors.As(err, &statusErr) || statusErr.RetryAfter != time.Minute {
		t.Fatalf("cooldown error = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("network calls = %d", calls.Load())
	}
}

func TestSearchBatchRefusesRedirect(t *testing.T) {
	client := newTestClient(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
		response := jsonResponse(http.StatusFound, `{}`)
		response.Header.Set("Location", "https://example.com/steal")
		return response, nil
	}))
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "compiler"})
	var statusErr *StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusFound || batch.Status != search.BatchFailed {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
}

func TestNewPinsOfficialEndpointAndHardBudgets(t *testing.T) {
	for _, endpoint := range []string{
		"http://api.github.com/search/repositories",
		"https://example.com/search/repositories",
		"https://api.github.com/search/repositories/extra",
		"https://user@api.github.com/search/repositories",
		"https://api.github.com/search/repositories?q=x",
	} {
		if _, err := New(Options{Endpoint: endpoint, HTTPClient: &http.Client{}, UserAgent: testUserAgent, MaxResults: 10, MaxBodyBytes: 1024, RatePerSecond: 0.1, Burst: 1, MaxConcurrency: 1}); err == nil {
			t.Fatalf("New accepted endpoint %q", endpoint)
		}
	}
	client, err := New(Options{HTTPClient: &http.Client{}, UserAgent: testUserAgent, MaxResults: 100, MaxBodyBytes: 1024, RatePerSecond: 10, Burst: 10, MaxConcurrency: 10})
	if err != nil {
		t.Fatal(err)
	}
	if client.maxResults != maxProviderResults {
		t.Fatalf("maxResults = %d", client.maxResults)
	}
}

func TestNewMakesEveryRateTokenOneHTTPAttempt(t *testing.T) {
	transport := &http.Transport{}
	original := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error {
		return nil
	}}
	client, err := New(Options{
		HTTPClient: original, UserAgent: testUserAgent, MaxResults: 10,
		MaxBodyBytes: 1024, RatePerSecond: 0.1, Burst: 1, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := client.httpClient.Transport.(*http.Transport)
	if !ok || got == transport || !got.DisableKeepAlives || got.ForceAttemptHTTP2 || got.TLSNextProto == nil {
		t.Fatalf("single-attempt transport = %#v", client.httpClient.Transport)
	}
	if transport.DisableKeepAlives || original.CheckRedirect == nil {
		t.Fatal("New mutated the caller-owned client or transport")
	}
}

func newTestClient(t *testing.T, transport http.RoundTripper) *Client {
	t.Helper()
	client, err := New(Options{
		HTTPClient: &http.Client{Transport: transport}, UserAgent: testUserAgent,
		MaxResults: 10, MaxBodyBytes: 1 << 20, RatePerSecond: maxProviderRate,
		Burst: 1, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func staticJSONTransport(body string) http.RoundTripper {
	return roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, body), nil
	})
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
