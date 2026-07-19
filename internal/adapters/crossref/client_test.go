package crossref

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

func TestClientSearchBatchMapsBibliographicMetadata(t *testing.T) {
	var request *http.Request
	client := mustNew(t, Options{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			request = req.Clone(req.Context())
			return jsonResponse(http.StatusOK, `{"status":"ok","message":{"items":[{"DOI":"10.5555/example.1","title":["A useful paper"],"author":[{"given":"Ada","family":"Lovelace"},{"family":"Noether"}],"published":{"date-parts":[[2026,7,2]]},"type":"journal-article","publisher":"Example Press","container-title":["Journal of Examples"],"license":[{"URL":"https://creativecommons.org/licenses/by/4.0/"}],"abstract":"must not be copied"}]}}`), nil
		})},
		UserAgent: "FetchmarkBot/0.1 (https://github.com/staticvar/fetchmark)", Mailto: "ops@example.com",
		MaxResults: 20, MaxBodyBytes: 1 << 20, RatePerSecond: 100, Burst: 100, MaxConcurrency: 100,
	})
	client.now = func() time.Time { return time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC) }
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "useful", ExactMatch: true, TimeRange: "week", MaxResults: 50})
	if err != nil {
		t.Fatal(err)
	}
	query := request.URL.Query()
	if query.Get("query.title") != "useful" || query.Get("query.bibliographic") != "" || query.Get("rows") != "20" || query.Get("mailto") != "ops@example.com" || query.Get("filter") != "from-pub-date:2026-07-11,until-pub-date:2026-07-18" {
		t.Fatalf("query = %v", query)
	}
	if batch.Provider != "crossref" || batch.Status != search.BatchHealthy || len(batch.Hits) != 1 {
		t.Fatalf("batch = %+v", batch)
	}
	hit := batch.Hits[0]
	if hit.URL != "https://doi.org/10.5555/example.1" || hit.Title != "A useful paper" || strings.Contains(hit.Snippet, "must not be copied") {
		t.Fatalf("hit = %+v", hit)
	}
	if hit.Snippet != "Ada Lovelace, Noether · Journal of Examples · Example Press" || hit.Metadata["doi"] != "10.5555/example.1" || hit.Metadata["license_url"] == "" {
		t.Fatalf("hit = %+v", hit)
	}
	if hit.PublishedAt == nil || hit.PublishedAt.Format("2006-01-02") != "2026-07-02" {
		t.Fatalf("published = %v", hit.PublishedAt)
	}
}

func TestClientSearchBatchClassifiesEmptyAndFailure(t *testing.T) {
	t.Run("authoritative empty", func(t *testing.T) {
		client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusOK, `{"status":"ok","message":{"items":[]}}`), nil
		})))
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "nothing"})
		if err != nil || batch.Status != search.BatchAuthoritativeEmpty {
			t.Fatalf("batch=%+v err=%v", batch, err)
		}
	})
	t.Run("retry after", func(t *testing.T) {
		client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
			response := jsonResponse(http.StatusTooManyRequests, `{}`)
			response.Header.Set("Retry-After", "3")
			return response, nil
		})))
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "limited"})
		var statusErr *StatusError
		if !errors.As(err, &statusErr) || batch.Status != search.BatchFailed || batch.Diagnostics[0].RetryAfter != 3*time.Second {
			t.Fatalf("batch=%+v err=%v", batch, err)
		}
	})
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

func TestClientRejectsMalformedOversizedAndEmptyQuery(t *testing.T) {
	for _, tt := range []struct {
		name, body string
		limit      int64
	}{
		{name: "malformed", body: `{`, limit: 1024},
		{name: "oversized", body: strings.Repeat("x", 65), limit: 64},
	} {
		t.Run(tt.name, func(t *testing.T) {
			options := testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) { return jsonResponse(http.StatusOK, tt.body), nil }))
			options.MaxBodyBytes = tt.limit
			client := mustNew(t, options)
			if batch, err := client.SearchBatch(context.Background(), search.Query{Q: "test"}); err == nil || batch.Status != search.BatchFailed {
				t.Fatalf("batch=%+v err=%v", batch, err)
			}
		})
	}
	var calls atomic.Int32
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return jsonResponse(http.StatusOK, `{}`), nil
	})))
	if _, err := client.SearchBatch(context.Background(), search.Query{Q: "  "}); err == nil || calls.Load() != 0 {
		t.Fatalf("empty query err=%v calls=%d", err, calls.Load())
	}
}

func TestDatePartsRejectsImpossibleDate(t *testing.T) {
	if got := (dateParts{Parts: [][]int{{2026, 2, 31}}}).time(); got != nil {
		t.Fatalf("impossible date normalized to %v", got)
	}
}

func TestClientCapsPublicConcurrency(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var active, maximum atomic.Int32
	options := testOptions(roundTripFunc(func(req *http.Request) (*http.Response, error) {
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
			return jsonResponse(http.StatusOK, `{"status":"ok","message":{"items":[]}}`), nil
		case <-req.Context().Done():
			active.Add(-1)
			return nil, req.Context().Err()
		}
	}))
	options.MaxConcurrency = 9
	options.RatePerSecond = 100
	options.Burst = 9
	client := mustNew(t, options)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = client.SearchBatch(context.Background(), search.Query{Q: "test"}) }()
	}
	<-started
	select {
	case <-started:
		t.Fatal("second public-pool request started concurrently")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	wg.Wait()
	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrency=%d", maximum.Load())
	}
}

func TestNewRejectsUnsafeConfiguration(t *testing.T) {
	for _, options := range []Options{
		{HTTPClient: &http.Client{}, UserAgent: "curl/1", MaxResults: 10, MaxBodyBytes: 1024, RatePerSecond: 1, Burst: 1, MaxConcurrency: 1},
		{HTTPClient: &http.Client{}, UserAgent: "ExampleBot/1.0 (none)", MaxResults: 10, MaxBodyBytes: 1024, RatePerSecond: 1, Burst: 1, MaxConcurrency: 1},
		{Endpoint: "https://evil.example/v1/works", HTTPClient: &http.Client{}, UserAgent: "Fetchmark/1 (https://example.com)", MaxResults: 10, MaxBodyBytes: 1024, RatePerSecond: 1, Burst: 1, MaxConcurrency: 1},
		{HTTPClient: &http.Client{}, UserAgent: "Fetchmark/1 (https://example.com)", Mailto: "not an email", MaxResults: 10, MaxBodyBytes: 1024, RatePerSecond: 1, Burst: 1, MaxConcurrency: 1},
	} {
		if _, err := New(options); err == nil {
			t.Fatal("New accepted unsafe configuration")
		}
	}
}

func testOptions(transport http.RoundTripper) Options {
	return Options{HTTPClient: &http.Client{Transport: transport}, UserAgent: "FetchmarkBot/0.1 (https://github.com/staticvar/fetchmark)", MaxResults: 10, MaxBodyBytes: 1 << 20, RatePerSecond: 100, Burst: 10, MaxConcurrency: 10}
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
	return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}
