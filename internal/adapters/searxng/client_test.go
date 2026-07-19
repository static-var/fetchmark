package searxng

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/staticvar/fetchmark/internal/core/search"
	"github.com/staticvar/fetchmark/internal/obs"
)

const fixture = `{
  "query": "golang",
  "number_of_results": 2,
  "results": [
    {"url": "https://go.dev", "title": "The Go Programming Language", "content": "Go is...", "engine": "google", "engines": ["google","bing"], "category": "general"},
    {"url": "https://golang.org", "title": "golang.org", "content": "redirect", "engine": "duckduckgo"}
  ],
  "unresponsive_engines": []
}`

func newStub(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c, err := New(srv.URL, srv.Client())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestSearch_ControlsAndMetadata(t *testing.T) {
	c := newStub(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("q") != `"bird species" site:a.example -site:b.example` {
			t.Errorf("q = %q", q.Get("q"))
		}
		if q.Get("categories") != "general,news" {
			t.Errorf("categories = %q", q.Get("categories"))
		}
		if q.Get("language") != "en" {
			t.Errorf("language = %q", q.Get("language"))
		}
		if q.Get("time_range") != "year" {
			t.Errorf("time_range = %q", q.Get("time_range"))
		}
		if q.Get("safesearch") != "1" {
			t.Errorf("safesearch = %q", q.Get("safesearch"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"url":"https://a.example","title":"A","content":"one","category":"general"},{"url":"https://b.example","title":"B","content":"two","category":"news"}]}`))
	})

	safeSearch := 1
	hits, err := c.Search(context.Background(), search.Query{
		Q:              "bird species",
		Categories:     []string{"general", "news"},
		Language:       "en",
		TimeRange:      "year",
		SafeSearch:     &safeSearch,
		IncludeDomains: []string{"a.example"},
		ExcludeDomains: []string{"b.example"},
		ExactMatch:     true,
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("hits = %d", len(hits))
	}
	if hits[0].Metadata["category"] != "general" || hits[0].Metadata["original_rank"] != "1" {
		t.Fatalf("first metadata = %#v", hits[0].Metadata)
	}
	if hits[1].Metadata["category"] != "news" || hits[1].Metadata["original_rank"] != "2" {
		t.Fatalf("second metadata = %#v", hits[1].Metadata)
	}
}

func TestSearch_FetchesAdditionalPagesUntilMaxResults(t *testing.T) {
	var pages []string
	c := newStub(t, func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("pageno")
		pages = append(pages, page)
		w.Header().Set("Content-Type", "application/json")
		switch page {
		case "1":
			_, _ = w.Write([]byte(`{"results":[{"url":"https://a.example","title":"A"},{"url":"https://b.example","title":"B"}]}`))
		case "2":
			_, _ = w.Write([]byte(`{"results":[{"url":"https://c.example","title":"C"},{"url":"https://d.example","title":"D"}]}`))
		case "3":
			_, _ = w.Write([]byte(`{"results":[{"url":"https://e.example","title":"E"}]}`))
		default:
			t.Fatalf("unexpected page %q", page)
		}
	})

	hits, err := c.Search(context.Background(), search.Query{Q: "bird species", MaxResults: 5})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if got, want := strings.Join(pages, ","), "1,2,3"; got != want {
		t.Fatalf("pages = %q, want %q", got, want)
	}
	if len(hits) != 5 {
		t.Fatalf("hits = %d, want 5", len(hits))
	}
	if hits[4].URL != "https://e.example" || hits[4].Metadata["original_rank"] != "5" {
		t.Fatalf("last hit = %+v", hits[4])
	}
}

func TestSearch_StopsOnRepeatedPage(t *testing.T) {
	requests := 0
	c := newStub(t, func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"url":"https://same.example","title":"Same"}]}`))
	})

	hits, err := c.Search(context.Background(), search.Query{Q: "birds", MaxResults: 5})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2 (first page plus repeated-page detection)", requests)
	}
	if len(hits) != 1 {
		t.Fatalf("hits = %d, want one unique page", len(hits))
	}
}

func TestSearch_RecordsUnresponsiveEnginesFromEmptyPage(t *testing.T) {
	c := newStub(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[],"unresponsive_engines":[["google","timeout"]]}`))
	})

	_, err := c.Search(context.Background(), search.Query{Q: "birds", MaxResults: 5})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	c.mu.Lock()
	_, ok := c.engineQuality["google"]
	c.mu.Unlock()
	if !ok {
		t.Fatalf("unresponsive engine from empty page was not recorded")
	}
}

func TestSearchBatch_ClassifiesDegradedEmptyAndPropagatesDiagnostics(t *testing.T) {
	c := newStub(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[],"unresponsive_engines":[["duckduckgo","Suspended: CAPTCHA"],["brave","too many requests"]]}`))
	})

	metric := obs.DiscoveryBatchTotal.WithLabelValues("searxng", c.instanceID(), string(search.BatchDegradedEmpty))
	before := counterValue(t, metric)
	batch, err := c.SearchBatch(context.Background(), search.Query{Q: "birds"})
	if err != nil {
		t.Fatalf("SearchBatch: %v", err)
	}
	if batch.Status != search.BatchDegradedEmpty {
		t.Fatalf("status = %q, want %q", batch.Status, search.BatchDegradedEmpty)
	}
	if len(batch.Hits) != 0 || batch.Partial() {
		t.Fatalf("degraded empty batch = %+v", batch)
	}
	if batch.Provider != "searxng" || batch.Instance == "" {
		t.Fatalf("provider identity = %q / %q", batch.Provider, batch.Instance)
	}
	if batch.Duration <= 0 {
		t.Fatalf("duration = %v, want positive", batch.Duration)
	}
	if len(batch.Diagnostics) != 2 {
		t.Fatalf("diagnostics = %+v", batch.Diagnostics)
	}
	if got := batch.Diagnostics[0]; got.Source != "duckduckgo" || got.Reason != "Suspended: CAPTCHA" {
		t.Fatalf("first diagnostic = %+v", got)
	}
	if got := counterValue(t, metric); got != before+1 {
		t.Fatalf("degraded batch metric = %v, want %v", got, before+1)
	}
}

func TestSearchBatch_TracksPerEngineQualityEWMA(t *testing.T) {
	requests := 0
	c := newStub(t, func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		if requests == 1 {
			_, _ = w.Write([]byte(`{"results":[],"unresponsive_engines":[["mwmbl","timeout"]]}`))
			return
		}
		_, _ = w.Write([]byte(`{"results":[{"url":"https://mwmbl.org","title":"Mwmbl","engine":"mwmbl"}],"unresponsive_engines":[]}`))
	})

	if _, err := c.SearchBatch(context.Background(), search.Query{Q: "first"}); err != nil {
		t.Fatalf("first SearchBatch: %v", err)
	}
	first := c.engineQuality["mwmbl"]
	if first <= 0 || first >= 1 {
		t.Fatalf("first engine quality = %v, want 0..1", first)
	}
	if _, err := c.SearchBatch(context.Background(), search.Query{Q: "second"}); err != nil {
		t.Fatalf("second SearchBatch: %v", err)
	}
	second := c.engineQuality["mwmbl"]
	if second <= first || second >= 1 {
		t.Fatalf("second engine quality = %v, want %v..1", second, first)
	}
}

func TestUpdateEngineHealthBoundsUnknownEngineState(t *testing.T) {
	c, err := New("http://searxng.example", &http.Client{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	entries := make([][]any, 0, maxTrackedEngines+100)
	for i := 0; i < maxTrackedEngines+100; i++ {
		entries = append(entries, []any{fmt.Sprintf("engine-%04d", i), "timeout"})
	}
	c.updateEngineHealth(nil, entries)
	if got := len(c.engineQuality); got != maxTrackedEngines+1 {
		t.Fatalf("tracked engine series = %d, want %d individual + overflow", got, maxTrackedEngines)
	}
	if _, ok := c.engineQuality[engineOverflowLabel]; !ok {
		t.Fatalf("missing overflow engine quality: %+v", c.engineQuality)
	}
	if _, ok := c.engineQuality["engine-0000"]; !ok {
		t.Fatal("stable known engine was not tracked")
	}
	if _, ok := c.engineQuality[fmt.Sprintf("engine-%04d", maxTrackedEngines+99)]; ok {
		t.Fatal("engine beyond the budget received an unbounded label")
	}
}

func counterValue(t *testing.T, counter prometheus.Counter) float64 {
	t.Helper()
	metric := &dto.Metric{}
	if err := counter.Write(metric); err != nil {
		t.Fatalf("write metric: %v", err)
	}
	return metric.GetCounter().GetValue()
}

func TestSearchBatch_ClassifiesPartialAndAuthoritativeEmpty(t *testing.T) {
	t.Run("partial", func(t *testing.T) {
		c := newStub(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"results":[{"url":"https://wiby.me","title":"Wiby"}],"unresponsive_engines":[["brave","timeout"]]}`))
		})

		batch, err := c.SearchBatch(context.Background(), search.Query{Q: "small web"})
		if err != nil {
			t.Fatalf("SearchBatch: %v", err)
		}
		if batch.Status != search.BatchPartial || !batch.Partial() || len(batch.Hits) != 1 || len(batch.Diagnostics) != 1 {
			t.Fatalf("partial batch = %+v", batch)
		}
	})

	t.Run("authoritative empty", func(t *testing.T) {
		c := newStub(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"results":[],"unresponsive_engines":[]}`))
		})

		batch, err := c.SearchBatch(context.Background(), search.Query{Q: "no such thing"})
		if err != nil {
			t.Fatalf("SearchBatch: %v", err)
		}
		if batch.Status != search.BatchAuthoritativeEmpty || batch.Partial() || len(batch.Diagnostics) != 0 {
			t.Fatalf("authoritative empty batch = %+v", batch)
		}
	})
}

func TestSearchBatch_UsesTerminalPageDiagnosticsAndToleratesMalformedEntries(t *testing.T) {
	requests := 0
	c := newStub(t, func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		if requests == 1 {
			_, _ = w.Write([]byte(`{"results":[{"url":"https://mwmbl.org","title":"Mwmbl"}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"results":[],"unresponsive_engines":[[],[17,{"unexpected":true}],["brave",17]]}`))
	})

	batch, err := c.SearchBatch(context.Background(), search.Query{Q: "open search", MaxResults: 5})
	if err != nil {
		t.Fatalf("SearchBatch: %v", err)
	}
	if batch.Status != search.BatchPartial || len(batch.Hits) != 1 {
		t.Fatalf("batch = %+v, want partial with first-page hit", batch)
	}
	if len(batch.Diagnostics) != 3 {
		t.Fatalf("diagnostics = %+v, want malformed entries preserved", batch.Diagnostics)
	}
	if batch.Diagnostics[0].Source != "unknown" || batch.Diagnostics[0].Reason != "malformed diagnostic" {
		t.Fatalf("malformed diagnostic = %+v", batch.Diagnostics[0])
	}
}

func TestSearchBatch_RetainsFirstPageWhenLaterPageFails(t *testing.T) {
	tests := []struct {
		name       string
		secondPage func(http.ResponseWriter)
		reason     string
	}{
		{
			name: "upstream status",
			secondPage: func(w http.ResponseWriter) {
				http.Error(w, "unavailable", http.StatusBadGateway)
			},
			reason: "http_502",
		},
		{
			name: "malformed response",
			secondPage: func(w http.ResponseWriter) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"results":`))
			},
			reason: "malformed_response",
		},
		{
			name: "oversized response",
			secondPage: func(w http.ResponseWriter) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"results":[{"content":"` + strings.Repeat("x", maxResponseBytes+1) + `"}]}`))
			},
			reason: "response_too_large",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requests := 0
			c := newStub(t, func(w http.ResponseWriter, _ *http.Request) {
				requests++
				if requests == 1 {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"results":[{"url":"https://mwmbl.org","title":"Mwmbl"}]}`))
					return
				}
				tt.secondPage(w)
			})

			batch, err := c.SearchBatch(context.Background(), search.Query{Q: "open search", MaxResults: 5})
			if err != nil {
				t.Fatalf("SearchBatch: %v", err)
			}
			if batch.Status != search.BatchPartial || len(batch.Hits) != 1 || batch.Hits[0].URL != "https://mwmbl.org" {
				t.Fatalf("batch = %+v, want retained first-page hit", batch)
			}
			if len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Source != "page_2" || batch.Diagnostics[0].Reason != tt.reason {
				t.Fatalf("diagnostics = %+v", batch.Diagnostics)
			}
		})
	}
}

func TestSearchBatch_RetainsFirstPageWhenLaterPageTimesOut(t *testing.T) {
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests == 1 {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"results":[{"url":"https://mwmbl.org","title":"Mwmbl"}]}`))
			return
		}
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	httpClient := srv.Client()
	httpClient.Timeout = 25 * time.Millisecond
	c, err := New(srv.URL, httpClient)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	batch, err := c.SearchBatch(context.Background(), search.Query{Q: "open search", MaxResults: 5})
	if err != nil {
		t.Fatalf("SearchBatch: %v", err)
	}
	if batch.Status != search.BatchPartial || len(batch.Hits) != 1 {
		t.Fatalf("batch = %+v, want retained first-page hit", batch)
	}
	if len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Source != "page_2" || batch.Diagnostics[0].Reason != "timeout" {
		t.Fatalf("diagnostics = %+v", batch.Diagnostics)
	}
}

func TestSearch_LegacyContractReturnsFirstPageWhenLaterPageFails(t *testing.T) {
	requests := 0
	c := newStub(t, func(w http.ResponseWriter, _ *http.Request) {
		requests++
		if requests == 1 {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"results":[{"url":"https://wiby.me","title":"Wiby"}]}`))
			return
		}
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	})

	hits, err := c.Search(context.Background(), search.Query{Q: "small web", MaxResults: 5})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 1 || hits[0].URL != "https://wiby.me" {
		t.Fatalf("hits = %+v", hits)
	}
}

func TestSearchBatch_ClassifiesHTTPFailure(t *testing.T) {
	c := newStub(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	})

	batch, err := c.SearchBatch(context.Background(), search.Query{Q: "birds"})
	if err == nil {
		t.Fatal("expected error")
	}
	if batch.Status != search.BatchFailed || batch.Provider != "searxng" || batch.Duration <= 0 {
		t.Fatalf("failed batch = %+v", batch)
	}
}

func TestSearch_PreservesPublishedAtMetadata(t *testing.T) {
	c := newStub(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"url":"https://a.example","title":"A","content":"one","publishedDate":"2025-01-02T03:04:05Z","date":"2025-01-02"},{"url":"https://b.example","title":"B","content":"two","date":"not a date"}]}`))
	})

	hits, err := c.Search(context.Background(), search.Query{Q: "birds"})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("hits = %d", len(hits))
	}
	want := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	if hits[0].PublishedAt == nil || !hits[0].PublishedAt.Equal(want) {
		t.Fatalf("publishedAt = %+v, want %s", hits[0].PublishedAt, want)
	}
	if hits[0].Metadata["published_at"] != "2025-01-02T03:04:05Z" || hits[0].Metadata["date"] != "2025-01-02" {
		t.Fatalf("published metadata = %#v", hits[0].Metadata)
	}
	if hits[1].PublishedAt != nil || hits[1].Metadata["published_at"] != "not a date" || hits[1].Metadata["date"] != "not a date" {
		t.Fatalf("invalid date should be preserved as metadata only: %+v", hits[1])
	}
}

func TestSearch_Success(t *testing.T) {
	var gotQuery string
	c := newStub(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/search" {
			t.Errorf("path = %q", r.URL.Path)
		}
		gotQuery = r.URL.Query().Get("q")
		if r.URL.Query().Get("format") != "json" {
			t.Errorf("format missing")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fixture))
	})
	hits, err := c.Search(context.Background(), search.Query{Q: "golang", Engines: []string{"google", "bing"}})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if gotQuery != "golang" {
		t.Errorf("query = %q", gotQuery)
	}
	if len(hits) != 2 {
		t.Fatalf("hits = %d", len(hits))
	}
	if hits[0].URL != "https://go.dev" || len(hits[0].Engines) != 2 {
		t.Errorf("first hit = %+v", hits[0])
	}
	if hits[1].Engines[0] != "duckduckgo" {
		t.Errorf("second hit engines = %v", hits[1].Engines)
	}
}

func TestSearch_EmptyQuery(t *testing.T) {
	c := newStub(t, func(w http.ResponseWriter, r *http.Request) {})
	if _, err := c.Search(context.Background(), search.Query{Q: "   "}); err == nil {
		t.Fatal("expected error for empty query")
	}
}

func TestSearch_UpstreamError(t *testing.T) {
	c := newStub(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	})
	_, err := c.Search(context.Background(), search.Query{Q: "x"})
	if err == nil || !strings.Contains(err.Error(), "502") {
		t.Fatalf("err = %v", err)
	}
}

func TestSearch_ReturnsTypedStatusError(t *testing.T) {
	c := newStub(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	})

	_, err := c.Search(context.Background(), search.Query{Q: "x"})
	var statusErr *StatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("err = %T %v, want *StatusError", err, err)
	}
	if statusErr.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", statusErr.StatusCode)
	}
	if !statusErr.Retryable() {
		t.Fatal("429 status should be retryable")
	}
}

func TestSearch_ParsesRetryAfterDeltaAndHTTPDate(t *testing.T) {
	tests := []struct {
		name        string
		header      string
		wantAtLeast time.Duration
		wantAtMost  time.Duration
	}{
		{name: "delta seconds", header: "120", wantAtLeast: 120 * time.Second, wantAtMost: 120 * time.Second},
		{name: "http date", header: time.Now().UTC().Add(90 * time.Second).Format(http.TimeFormat), wantAtLeast: 88 * time.Second, wantAtMost: 90 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newStub(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", tt.header)
				http.Error(w, "rate limited", http.StatusTooManyRequests)
			})

			_, err := c.Search(context.Background(), search.Query{Q: "x"})
			var statusErr *StatusError
			if !errors.As(err, &statusErr) {
				t.Fatalf("err = %T %v, want *StatusError", err, err)
			}
			if statusErr.RetryAfter < tt.wantAtLeast || statusErr.RetryAfter > tt.wantAtMost {
				t.Fatalf("RetryAfter = %v, want %v..%v", statusErr.RetryAfter, tt.wantAtLeast, tt.wantAtMost)
			}
		})
	}
}

func TestParseRetryAfterBoundsUntrustedValues(t *testing.T) {
	now := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	if got := parseRetryAfter("999999999999999999999999999999999999", now); got != maxRetryAfter {
		t.Fatalf("huge Retry-After = %v, want %v", got, maxRetryAfter)
	}
	for _, raw := range []string{"-1", "0", "invalid", now.Add(-time.Minute).Format(http.TimeFormat)} {
		if got := parseRetryAfter(raw, now); got != 0 {
			t.Fatalf("Retry-After %q = %v, want 0", raw, got)
		}
	}
}

func TestSearch_Non2xxStatusSkipsOversizedJSONDecode(t *testing.T) {
	c := newStub(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"results":[{"content":"` + strings.Repeat("x", maxResponseBytes+1) + `"}]}`))
	})

	_, err := c.Search(context.Background(), search.Query{Q: "x"})
	var statusErr *StatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("err = %T %v, want *StatusError", err, err)
	}
	if statusErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", statusErr.StatusCode)
	}
	if errors.Is(err, errResponseTooLarge) {
		t.Fatalf("non-2xx response should not decode oversized JSON: %v", err)
	}
}

func TestSearch_OversizedJSONResponse(t *testing.T) {
	c := newStub(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"content":"` + strings.Repeat("x", maxResponseBytes+1) + `"}]}`))
	})

	_, err := c.Search(context.Background(), search.Query{Q: "x"})
	if !errors.Is(err, errResponseTooLarge) {
		t.Fatalf("err = %v, want errResponseTooLarge", err)
	}
}

func TestPing_Non2xxStatusIsUnready(t *testing.T) {
	c := newStub(t, func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})

	err := c.Ping(context.Background())
	if err == nil {
		t.Fatal("expected ping error for 404")
	}
}

func TestNew_BadURL(t *testing.T) {
	if _, err := New("notaurl", nil); err == nil {
		t.Fatal("expected error")
	}
}
