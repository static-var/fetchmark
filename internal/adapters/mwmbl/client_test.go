package mwmbl

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

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestClientSearchBatchMapsSegmentedResultsAndBoundsCount(t *testing.T) {
	var request *http.Request
	client := mustNew(t, testOptions(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		request = req.Clone(req.Context())
		return jsonResponse(http.StatusOK, `[
			{"url":"https://example.com/one","title":[{"value":"Open","is_bold":true},{"value":" search"}],"extract":[{"value":"A free "},{"value":"index"}],"source":"mwmbl"},
			{"url":"https://example.com/two","title":[{"value":"Second"}],"extract":[],"source":"user"},
			{"url":"https://example.com/three","title":[{"value":"Third"}],"extract":[],"source":"mwmbl"}
		]`), nil
	})))
	client.maxResults = 2

	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "open search", MaxResults: 50})
	if err != nil {
		t.Fatal(err)
	}
	if request == nil || request.Method != http.MethodGet || request.URL.String() != "https://api.mwmbl.org/api/v1/search/?s=open+search" {
		t.Fatalf("request = %v", request)
	}
	if request.Header.Get("Accept") != "application/json" || !strings.Contains(request.Header.Get("User-Agent"), "FetchmarkBot") {
		t.Fatalf("headers = %v", request.Header)
	}
	if batch.Provider != "mwmbl" || batch.Instance != "api.mwmbl.org" || batch.Status != search.BatchHealthy || len(batch.Hits) != 2 {
		t.Fatalf("batch = %+v", batch)
	}
	if hit := batch.Hits[0]; hit.Title != "Open search" || hit.Snippet != "A free index" || hit.Metadata["mwmbl_source"] != "mwmbl" || len(hit.Engines) != 1 || hit.Engines[0] != "mwmbl" {
		t.Fatalf("first hit = %+v", hit)
	}
}

func TestClientSearchBatchClassifiesEmptyAndPartialMalformedRows(t *testing.T) {
	responses := []string{`[]`, `[{"url":"javascript:alert(1)","title":[{"value":"bad"}]},{"url":"https://example.com/good","title":[{"value":"good"}]}]`}
	var calls atomic.Int32
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, responses[int(calls.Add(1))-1]), nil
	})))
	empty, err := client.SearchBatch(context.Background(), search.Query{Q: "none"})
	if err != nil || empty.Status != search.BatchAuthoritativeEmpty {
		t.Fatalf("empty batch=%+v err=%v", empty, err)
	}
	partial, err := client.SearchBatch(context.Background(), search.Query{Q: "mixed"})
	if err != nil || partial.Status != search.BatchPartial || len(partial.Hits) != 1 || len(partial.Diagnostics) != 1 || partial.Diagnostics[0].Reason != "malformed_results" {
		t.Fatalf("partial batch=%+v err=%v", partial, err)
	}
}

func TestClientRejectsNullTopLevelPayloadAsMalformed(t *testing.T) {
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `null`), nil
	})))
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "unexpected shape"})
	if err == nil || batch.Status != search.BatchFailed || len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Reason != "malformed" {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
}

func TestClientDomainFiltersMatchPublicHostPathGrammar(t *testing.T) {
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `[
			{"url":"https://www.go.dev/doc/tutorial","title":[{"value":"keep path"}]},
			{"url":"https://go.dev/blog/release","title":[{"value":"wrong path"}]},
			{"url":"https://docs.example.com/guide/start","title":[{"value":"excluded wildcard"}]}
		]`), nil
	})))
	batch, err := client.SearchBatch(context.Background(), search.Query{
		Q: "domain grammar", IncludeDomains: []string{"go.dev/doc", "*.example.com/guide"},
		ExcludeDomains: []string{"www.docs.example.com/guide"},
	})
	if err != nil || len(batch.Hits) != 1 || batch.Hits[0].URL != "https://www.go.dev/doc/tutorial" {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
}

func TestClientRejectsUnsupportedControlsBeforeNetwork(t *testing.T) {
	var calls atomic.Int32
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return jsonResponse(http.StatusOK, `[]`), nil
	})))
	strict := 2
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "birds", SafeSearch: &strict})
	var unsupported *search.UnsupportedControlError
	if !errors.As(err, &unsupported) || unsupported.Control != "safesearch" || batch.Status != search.BatchFailed || calls.Load() != 0 {
		t.Fatalf("batch=%+v err=%v calls=%d", batch, err, calls.Load())
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
	client.now = func() time.Time { return time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC) }
	for range 2 {
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "limited"})
		var statusErr *StatusError
		if !errors.As(err, &statusErr) || statusErr.RetryAfter != 3*time.Second || batch.Diagnostics[0].RetryAfter != 3*time.Second {
			t.Fatalf("batch=%+v err=%v", batch, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("HTTP calls = %d, want cooldown to suppress second request", calls.Load())
	}
}

func TestClientHugeRetryAfterClampsAndOpensCooldown(t *testing.T) {
	var calls atomic.Int32
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		response := jsonResponse(http.StatusTooManyRequests, `{}`)
		response.Header.Set("Retry-After", "9223372036854775807")
		return response, nil
	})))
	client.now = func() time.Time { return time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC) }
	for range 2 {
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "limited"})
		var statusErr *StatusError
		if !errors.As(err, &statusErr) || statusErr.RetryAfter != maxRetryAfter || batch.Diagnostics[0].RetryAfter != maxRetryAfter {
			t.Fatalf("batch=%+v err=%v", batch, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("HTTP calls = %d, want cooldown to suppress second request", calls.Load())
	}
}

func TestNewRequiresFixedOfficialEndpointIdentityAndBudgets(t *testing.T) {
	valid := testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `[]`), nil
	}))
	tests := []struct {
		name   string
		mutate func(*Options)
	}{
		{name: "wrong endpoint", mutate: func(o *Options) { o.Endpoint = "https://example.com/api/v1/search/" }},
		{name: "missing contact", mutate: func(o *Options) { o.UserAgent = "FetchmarkBot" }},
		{name: "missing client", mutate: func(o *Options) { o.HTTPClient = nil }},
		{name: "unbounded results", mutate: func(o *Options) { o.MaxResults = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := valid
			test.mutate(&options)
			if _, err := New(options); err == nil {
				t.Fatal("New succeeded")
			}
		})
	}
}

func testOptions(transport http.RoundTripper) Options {
	return Options{
		HTTPClient: &http.Client{Transport: transport},
		UserAgent:  "FetchmarkBot/0.1 (https://github.com/staticvar/fetchmark)",
		MaxResults: 10, MaxBodyBytes: 1 << 20, RatePerSecond: 100, Burst: 1, MaxConcurrency: 1,
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
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
