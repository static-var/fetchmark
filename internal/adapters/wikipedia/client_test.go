package wikipedia

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/search"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestClientSearchBatchMapsResultsAndIdentity(t *testing.T) {
	var request *http.Request
	client := mustNew(t, Options{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			request = req.Clone(req.Context())
			return jsonResponse(http.StatusOK, `{"query":{"search":[{"pageid":123,"title":"Emmy Noether","snippet":"<span class=\"searchmatch\">Emmy</span> was a mathematician &amp; physicist","timestamp":"2026-07-01T12:00:00Z","wordcount":1234}]}}`), nil
		})},
		UserAgent:      "FetchmarkBot/0.1 (https://github.com/staticvar/fetchmark)",
		MaxResults:     10,
		MaxBodyBytes:   1 << 20,
		RatePerSecond:  100,
		Burst:          2,
		MaxConcurrency: 1,
	})

	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "Emmy Noether", Language: "de", MaxResults: 50})
	if err != nil {
		t.Fatal(err)
	}
	if request == nil || request.URL.Host != "de.wikipedia.org" {
		t.Fatalf("request URL = %v", request)
	}
	query := request.URL.Query()
	if query.Get("action") != "query" || query.Get("list") != "search" || query.Get("srsearch") != "Emmy Noether" || query.Get("srlimit") != "10" || query.Get("formatversion") != "2" {
		t.Fatalf("query = %v", query)
	}
	if got := request.Header.Get("User-Agent"); !strings.Contains(got, "FetchmarkBot") {
		t.Fatalf("User-Agent = %q", got)
	}
	if request.Header.Get("Api-User-Agent") == "" {
		t.Fatal("Api-User-Agent is missing")
	}
	if batch.Provider != "wikipedia" || batch.Status != search.BatchHealthy || len(batch.Hits) != 1 {
		t.Fatalf("batch = %+v", batch)
	}
	hit := batch.Hits[0]
	if hit.URL != "https://de.wikipedia.org/wiki/Emmy_Noether" || hit.Title != "Emmy Noether" || hit.Snippet != "Emmy was a mathematician & physicist" {
		t.Fatalf("hit = %+v", hit)
	}
	if hit.Metadata["page_id"] != "123" || hit.Metadata["word_count"] != "1234" || hit.Metadata["license"] != "CC-BY-SA" || hit.Metadata["source"] != "Wikimedia Foundation" {
		t.Fatalf("metadata = %+v", hit.Metadata)
	}
}

func TestClientSearchBatchUsesCompactNaturalLanguageQuestion(t *testing.T) {
	var request *http.Request
	client := mustNew(t, testOptions(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		request = req.Clone(req.Context())
		return jsonResponse(http.StatusOK, `{"query":{"search":[]}}`), nil
	})))

	_, err := client.SearchBatch(context.Background(), search.Query{
		Q: "  How does the Svalbard Global Seed Vault accession system work?  ",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := request.URL.Query().Get("srsearch"); got != "Svalbard Global Seed Vault accession system" {
		t.Fatalf("srsearch = %q", got)
	}
}

func TestClientSearchBatchPreservesExplicitExactMatchQuery(t *testing.T) {
	var request *http.Request
	client := mustNew(t, testOptions(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		request = req.Clone(req.Context())
		return jsonResponse(http.StatusOK, `{"query":{"search":[]}}`), nil
	})))

	_, err := client.SearchBatch(context.Background(), search.Query{
		Q:          "  How does the Svalbard Global Seed Vault accession system work?  ",
		ExactMatch: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := request.URL.Query().Get("srsearch"); got != "How does the Svalbard Global Seed Vault accession system work?" {
		t.Fatalf("srsearch = %q", got)
	}
}

func TestClientSearchBatchClassifiesAuthoritativeEmpty(t *testing.T) {
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{"query":{"search":[]}}`), nil
	})))
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "nothing"})
	if err != nil || batch.Status != search.BatchAuthoritativeEmpty || len(batch.Hits) != 0 {
		t.Fatalf("batch = %+v err=%v", batch, err)
	}
}

func TestClientArticleURLIsEscapedExactlyOnce(t *testing.T) {
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{"query":{"search":[{"pageid":1,"title":"100% Pure","snippet":"clean","timestamp":"2026-07-01T00:00:00Z","wordcount":1}]}}`), nil
	})))
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "pure"})
	if err != nil {
		t.Fatal(err)
	}
	if got := batch.Hits[0].URL; got != "https://en.wikipedia.org/wiki/100%25_Pure" {
		t.Fatalf("article URL = %q", got)
	}
}

func TestClientSearchBatchHonorsRetryAfter(t *testing.T) {
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		response := jsonResponse(http.StatusTooManyRequests, `{}`)
		response.Header.Set("Retry-After", "3")
		return response, nil
	})))
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "limited"})
	var statusErr *StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusTooManyRequests || statusErr.RetryAfter != 3*time.Second {
		t.Fatalf("error = %#v", err)
	}
	if batch.Status != search.BatchFailed || len(batch.Diagnostics) != 1 || !batch.Diagnostics[0].Retryable || batch.Diagnostics[0].RetryAfter != 3*time.Second {
		t.Fatalf("batch = %+v", batch)
	}
}

func TestClientHeaderlessRateLimitOpensDefaultCooldown(t *testing.T) {
	var calls atomic.Int32
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return jsonResponse(http.StatusTooManyRequests, `{}`), nil
	})))
	client.now = func() time.Time { return time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC) }
	for range 2 {
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "limited"})
		var statusErr *StatusError
		if !errors.As(err, &statusErr) || batch.Diagnostics[0].RetryAfter != defaultBlockBackoff {
			t.Fatalf("batch=%+v err=%v", batch, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("HTTP calls = %d, want second request suppressed", calls.Load())
	}
}

func TestClientRejectsInvalidLanguageBeforeRequest(t *testing.T) {
	var calls atomic.Int32
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return jsonResponse(http.StatusOK, `{"query":{"search":[]}}`), nil
	})))
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "test", Language: "en.evil.example"})
	if err == nil || batch.Status != search.BatchFailed || calls.Load() != 0 {
		t.Fatalf("batch = %+v err=%v calls=%d", batch, err, calls.Load())
	}
}

func TestClientLanguageAutoUsesDefaultWikipedia(t *testing.T) {
	var host string
	client := mustNew(t, testOptions(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		host = request.URL.Hostname()
		return jsonResponse(http.StatusOK, `{"query":{"search":[]}}`), nil
	})))
	if _, err := client.SearchBatch(context.Background(), search.Query{Q: "test", Language: "auto"}); err != nil {
		t.Fatal(err)
	}
	if host != "en.wikipedia.org" {
		t.Fatalf("host = %q, want default language host", host)
	}
}

func TestClientRejectsOversizedAndMalformedResponses(t *testing.T) {
	tests := []struct {
		name string
		body string
		max  int64
	}{
		{name: "oversized", body: strings.Repeat("x", 65), max: 64},
		{name: "malformed", body: `{`, max: 1024},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
				return jsonResponse(http.StatusOK, tt.body), nil
			}))
			options.MaxBodyBytes = tt.max
			client := mustNew(t, options)
			batch, err := client.SearchBatch(context.Background(), search.Query{Q: "test"})
			if err == nil || batch.Status != search.BatchFailed {
				t.Fatalf("batch = %+v err=%v", batch, err)
			}
		})
	}
}

func TestClientBoundsGlobalConcurrency(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var active, maximum atomic.Int32
	client := mustNew(t, testOptions(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		current := active.Add(1)
		for {
			prior := maximum.Load()
			if current <= prior || maximum.CompareAndSwap(prior, current) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-release:
			active.Add(-1)
			return jsonResponse(http.StatusOK, `{"query":{"search":[]}}`), nil
		case <-req.Context().Done():
			active.Add(-1)
			return nil, req.Context().Err()
		}
	})))
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = client.SearchBatch(context.Background(), search.Query{Q: "same"})
		}()
	}
	<-started
	select {
	case <-started:
		t.Fatal("second request started before the concurrency slot was released")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	wg.Wait()
	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrency = %d", maximum.Load())
	}
}

func TestClientQueuedRequestObservesNewCooldown(t *testing.T) {
	started := make(chan struct{})
	secondQueued := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	options := testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		close(started)
		<-release
		response := jsonResponse(http.StatusTooManyRequests, `{}`)
		response.Header.Set("Retry-After", "60")
		return response, nil
	}))
	options.RatePerSecond = 0.01
	options.Burst = 1
	options.MaxConcurrency = 1
	client := mustNew(t, options)
	var arrivals atomic.Int32
	client.beforeSemaphore = func() {
		if arrivals.Add(1) == 2 {
			close(secondQueued)
		}
	}
	firstErr := make(chan error, 1)
	go func() {
		_, err := client.SearchBatch(context.Background(), search.Query{Q: "first"})
		firstErr <- err
	}()
	<-started
	secondErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		_, err := client.SearchBatch(ctx, search.Query{Q: "second"})
		secondErr <- err
	}()
	<-secondQueued
	close(release)
	if err := <-firstErr; err == nil {
		t.Fatal("first request unexpectedly succeeded")
	}
	var statusErr *StatusError
	if err := <-secondErr; !errors.As(err, &statusErr) || statusErr.RetryAfter <= 0 {
		t.Fatalf("queued request error = %v, want active cooldown", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("HTTP calls = %d, want queued request suppressed", calls.Load())
	}
}

func TestClientCanceledRateWaitDoesNotIssueRequest(t *testing.T) {
	var calls atomic.Int32
	options := testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return jsonResponse(http.StatusOK, `{"query":{"search":[]}}`), nil
	}))
	options.RatePerSecond = 0.01
	options.Burst = 1
	client := mustNew(t, options)
	if _, err := client.SearchBatch(context.Background(), search.Query{Q: "first"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.SearchBatch(ctx, search.Query{Q: "second"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("HTTP calls = %d, want 1", calls.Load())
	}
}

func TestNewRejectsUnsafeConfiguration(t *testing.T) {
	tests := []Options{
		{HTTPClient: &http.Client{}, UserAgent: "Go-http-client/1.1", MaxResults: 10, MaxBodyBytes: 1024, RatePerSecond: 1, Burst: 1, MaxConcurrency: 1},
		{HTTPClient: &http.Client{}, UserAgent: "ExampleBot/1.0 (none)", MaxResults: 10, MaxBodyBytes: 1024, RatePerSecond: 1, Burst: 1, MaxConcurrency: 1},
		{Endpoint: "http://evil.example/api", HTTPClient: &http.Client{}, UserAgent: "FetchmarkBot/0.1 (https://example.com)", MaxResults: 10, MaxBodyBytes: 1024, RatePerSecond: 1, Burst: 1, MaxConcurrency: 1},
	}
	for _, options := range tests {
		if _, err := New(options); err == nil {
			t.Fatal("New accepted unsafe configuration")
		}
	}
}

func TestNewCapsSharedServiceBudgets(t *testing.T) {
	options := testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{"query":{"search":[]}}`), nil
	}))
	options.RatePerSecond = 100
	options.Burst = 100
	options.MaxConcurrency = 100
	client := mustNew(t, options)
	if got := client.budget.MaxConcurrency(); got != 2 {
		t.Fatalf("concurrency capacity = %d, want 2", got)
	}
	if got := client.budget.Burst(); got != 2 {
		t.Fatalf("rate burst = %d, want 2", got)
	}
}

func testOptions(transport http.RoundTripper) Options {
	return Options{
		HTTPClient:     &http.Client{Transport: transport},
		UserAgent:      "FetchmarkBot/0.1 (https://github.com/staticvar/fetchmark)",
		MaxResults:     10,
		MaxBodyBytes:   1 << 20,
		RatePerSecond:  100,
		Burst:          2,
		MaxConcurrency: 1,
	}
}

func mustNew(t *testing.T, options Options) *Client {
	t.Helper()
	client, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
