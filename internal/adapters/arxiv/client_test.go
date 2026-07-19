package arxiv

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

func TestClientSearchBatchMapsDescriptiveMetadataOnly(t *testing.T) {
	var observed *http.Request
	client := mustNew(t, Options{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			observed = request.Clone(request.Context())
			return atomResponse(http.StatusOK, strings.Replace(testFeed(`
				<entry>
					<id>http://arxiv.org/abs/2401.01234v2</id>
					<updated>2026-07-18T03:04:05Z</updated>
					<published>2026-07-12T01:02:03Z</published>
					<title> Retrieval   augmented generation </title>
					<summary> A bounded abstract with
					useful details. </summary>
					<author><name>Ada Lovelace</name></author>
					<author><name>Emmy Noether</name></author>
					<category term="cs.IR"/>
					<category term="cs.CL"/>
					<arxiv:primary_category term="cs.IR"/>
					<arxiv:doi>10.5555/example.1</arxiv:doi>
					<arxiv:journal_ref>Journal of Examples 42</arxiv:journal_ref>
					<arxiv:license>https://creativecommons.org/licenses/by/4.0/</arxiv:license>
					<link href="https://arxiv.org/pdf/2401.01234v2" rel="related" type="application/pdf"/>
				</entry>`), ">0</opensearch:totalResults>", ">1</opensearch:totalResults>", 1)), nil
		})},
		UserAgent:  "FetchmarkBot/0.1 (contact: mailto:ops@example.com)",
		MaxResults: 20, MaxBodyBytes: 1 << 20, RatePerSecond: 100, Burst: 100, MaxConcurrency: 100,
	})
	client.now = func() time.Time { return time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC) }

	batch, err := client.SearchBatch(context.Background(), search.Query{
		Q: "the retrieval, augmented generation?", ExactMatch: true, TimeRange: "week", MaxResults: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	query := observed.URL.Query()
	if query.Get("search_query") != `all:"the retrieval augmented generation" AND submittedDate:[202607121200 TO 202607191200]` ||
		query.Get("start") != "0" || query.Get("max_results") != "20" || query.Get("sortBy") != "submittedDate" || query.Get("sortOrder") != "descending" {
		t.Fatalf("query = %v", query)
	}
	if observed.Header.Get("Accept") != "application/atom+xml" || observed.Header.Get("User-Agent") == "" {
		t.Fatalf("headers = %v", observed.Header)
	}
	if batch.Provider != "arxiv" || batch.Status != search.BatchHealthy || len(batch.Hits) != 1 {
		t.Fatalf("batch = %+v", batch)
	}
	hit := batch.Hits[0]
	if hit.URL != "https://arxiv.org/abs/2401.01234v2" || hit.Title != "Retrieval augmented generation" ||
		hit.Snippet != "A bounded abstract with useful details." || hit.ProviderDocument != nil {
		t.Fatalf("hit = %+v", hit)
	}
	if hit.PublishedAt == nil || !hit.PublishedAt.Equal(time.Date(2026, 7, 12, 1, 2, 3, 0, time.UTC)) {
		t.Fatalf("published = %v", hit.PublishedAt)
	}
	wantMetadata := map[string]string{
		"source": "arXiv", "arxiv_id": "2401.01234v2", "authors": "Ada Lovelace, Emmy Noether",
		"categories": "cs.IR, cs.CL", "primary_category": "cs.IR", "doi": "10.5555/example.1",
		"journal_ref": "Journal of Examples 42", "updated_at": "2026-07-18T03:04:05Z",
		"article_license_url":  "https://creativecommons.org/licenses/by/4.0/",
		"metadata_license":     "CC0-1.0",
		"metadata_license_url": "https://creativecommons.org/publicdomain/zero/1.0/",
		"attribution":          "Thank you to arXiv for use of its open access interoperability.",
	}
	for key, want := range wantMetadata {
		if got := hit.Metadata[key]; got != want {
			t.Fatalf("metadata[%q] = %q, want %q; metadata=%v", key, got, want, hit.Metadata)
		}
	}
	for _, value := range hit.Metadata {
		if strings.Contains(value, "/pdf/") {
			t.Fatalf("PDF link escaped metadata-only boundary: %v", hit.Metadata)
		}
	}
}

func TestAtomLicenseUsesCharacterDataAndRejectsConflicts(t *testing.T) {
	entry := atomEntry{
		ID: "https://arxiv.org/abs/2401.01234", Title: "Example",
		Published: "2026-07-12T01:02:03Z",
	}
	tests := []struct {
		name    string
		license atomLicense
		want    string
	}{
		{name: "character data", license: atomLicense{Text: " https://creativecommons.org/licenses/by/4.0/ "}, want: "https://creativecommons.org/licenses/by/4.0/"},
		{name: "attribute fallback", license: atomLicense{Href: "https://creativecommons.org/licenses/by/3.0/"}, want: "https://creativecommons.org/licenses/by/3.0/"},
		{name: "matching forms", license: atomLicense{Text: "https://creativecommons.org/licenses/by/4.0/", Href: "https://creativecommons.org/licenses/by/4.0/"}, want: "https://creativecommons.org/licenses/by/4.0/"},
		{name: "conflicting forms", license: atomLicense{Text: "https://creativecommons.org/licenses/by/4.0/", Href: "https://creativecommons.org/licenses/by/3.0/"}},
		{name: "malformed", license: atomLicense{Text: "javascript:alert(1)"}},
		{name: "absent"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entry.License = test.license
			hit, ok := entry.hit()
			if !ok {
				t.Fatal("valid entry was rejected")
			}
			if got := hit.Metadata["article_license_url"]; got != test.want {
				t.Fatalf("article_license_url = %q, want %q", got, test.want)
			}
		})
	}
}

func TestNewClonesHTTPBoundaryAndDisablesKeepAlives(t *testing.T) {
	transport := &http.Transport{}
	original := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error {
		return nil
	}}
	client := mustNew(t, testOptionsWithClient(original))
	got, ok := client.httpClient.Transport.(*http.Transport)
	if !ok || got == transport || !got.DisableKeepAlives || got.ForceAttemptHTTP2 || got.TLSNextProto == nil {
		t.Fatalf("transport=%T %#v original=%#v", client.httpClient.Transport, got, transport)
	}
	if transport.DisableKeepAlives {
		t.Fatal("New mutated the caller's transport")
	}
	if client.httpClient == original {
		t.Fatal("New retained the caller's mutable HTTP client")
	}
}

func TestClientRefusesRedirectWithoutSecondAttempt(t *testing.T) {
	var calls atomic.Int32
	client := mustNew(t, testOptions(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		call := calls.Add(1)
		if call == 1 {
			response := atomResponse(http.StatusFound, "")
			response.Header.Set("Location", "https://export.arxiv.org/api/query?redirected=1")
			return response, nil
		}
		return atomResponse(http.StatusOK, testFeed("")), nil
	})))
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "redirect boundary"})
	var statusErr *StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusFound ||
		batch.Status != search.BatchFailed || batch.Diagnostics[0].Reason != "http_302" || calls.Load() != 1 {
		t.Fatalf("batch=%+v err=%v calls=%d", batch, err, calls.Load())
	}
}

func TestClientSearchBatchTreatsQuerySyntaxAsLiteralTerms(t *testing.T) {
	var searchQuery string
	client := mustNew(t, testOptions(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		searchQuery = request.URL.Query().Get("search_query")
		return atomResponse(http.StatusOK, testFeed("")), nil
	})))
	_, err := client.SearchBatch(context.Background(), search.Query{Q: `quantum OR cat:cs.AI "quoted"`})
	if err != nil {
		t.Fatal(err)
	}
	if searchQuery != "all:quantum AND all:cat AND all:cs AND all:ai AND all:quoted" {
		t.Fatalf("search_query = %q", searchQuery)
	}
}

func TestClientSearchBatchClassifiesEmptyPartialAndDegradedFeeds(t *testing.T) {
	tests := []struct {
		name       string
		feed       string
		wantStatus search.BatchStatus
		wantHits   int
		wantReason string
	}{
		{name: "authoritative empty", feed: testFeed(""), wantStatus: search.BatchAuthoritativeEmpty},
		{name: "missing total", feed: strings.Replace(testFeed(""), "<opensearch:totalResults>0</opensearch:totalResults>", "", 1), wantStatus: search.BatchDegradedEmpty, wantReason: "missing_total_results"},
		{name: "invalid only", feed: strings.Replace(testFeed(`<entry><id>https://evil.example/abs/1</id><published>2026-01-01T00:00:00Z</published><title>Bad</title></entry>`), ">0</opensearch:totalResults>", ">1</opensearch:totalResults>", 1), wantStatus: search.BatchDegradedEmpty, wantReason: "malformed_results"},
		{name: "partial", feed: strings.Replace(testFeed(`<entry><id>https://arxiv.org/abs/2401.00001</id><published>2026-01-01T00:00:00Z</published><title>Good</title></entry><entry><id>https://evil.example/abs/1</id><published>2026-01-01T00:00:00Z</published><title>Bad</title></entry>`), ">0</opensearch:totalResults>", ">2</opensearch:totalResults>", 1), wantStatus: search.BatchPartial, wantHits: 1, wantReason: "malformed_results"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
				return atomResponse(http.StatusOK, test.feed), nil
			})))
			batch, err := client.SearchBatch(context.Background(), search.Query{Q: "bounded research"})
			if err != nil || batch.Status != test.wantStatus || len(batch.Hits) != test.wantHits {
				t.Fatalf("batch=%+v err=%v", batch, err)
			}
			if test.wantReason != "" && (len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Reason != test.wantReason) {
				t.Fatalf("diagnostics = %+v", batch.Diagnostics)
			}
		})
	}
}

func TestClientSearchBatchReportsHTTPAndBodyFailures(t *testing.T) {
	t.Run("retry after", func(t *testing.T) {
		client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
			response := atomResponse(http.StatusTooManyRequests, "")
			response.Header.Set("Retry-After", "7")
			return response, nil
		})))
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "limited"})
		var statusErr *StatusError
		if !errors.As(err, &statusErr) || batch.Status != search.BatchFailed || batch.Diagnostics[0].RetryAfter != 7*time.Second {
			t.Fatalf("batch=%+v err=%v", batch, err)
		}
	})
	for _, test := range []struct {
		name string
		body string
		max  int64
	}{
		{name: "malformed", body: `<feed>`, max: 1024},
		{name: "oversized", body: strings.Repeat("x", 65), max: 64},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
				return atomResponse(http.StatusOK, test.body), nil
			}))
			options.MaxBodyBytes = test.max
			client := mustNew(t, options)
			if batch, err := client.SearchBatch(context.Background(), search.Query{Q: "test"}); err == nil || batch.Status != search.BatchFailed {
				t.Fatalf("batch=%+v err=%v", batch, err)
			}
		})
	}
}

func TestClientEnforcesArxivGlobalPacingAndSingleConnection(t *testing.T) {
	var calls atomic.Int32
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return atomResponse(http.StatusOK, testFeed("")), nil
	})))
	if client.budget.Burst() != 1 || client.budget.MaxConcurrency() != 1 {
		t.Fatalf("budget burst=%d concurrency=%d", client.budget.Burst(), client.budget.MaxConcurrency())
	}
	if _, err := client.SearchBatch(context.Background(), search.Query{Q: "first"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	batch, err := client.SearchBatch(ctx, search.Query{Q: "second"})
	if err == nil || batch.Diagnostics[0].Reason != "canceled" || calls.Load() != 1 {
		t.Fatalf("batch=%+v err=%v calls=%d", batch, err, calls.Load())
	}
}

func TestClientRejectsUnsupportedControlsAndUnsafeConfiguration(t *testing.T) {
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("unsupported request reached upstream")
		return nil, nil
	})))
	for _, query := range []search.Query{
		{Q: "x", Engines: []string{"arxiv"}},
		{Q: "x", Categories: []string{"science"}},
		{Q: "x", Language: "fr"},
		{Q: "x", Language: "en", SafeSearch: intPointer(1)},
		{Q: "x", SafeSearch: intPointer(1)},
		{Q: "x", IncludeDomains: []string{"example.com"}},
		{Q: "  "},
	} {
		batch, err := client.SearchBatch(context.Background(), query)
		if err == nil || batch.Status != search.BatchFailed {
			t.Fatalf("query=%+v batch=%+v err=%v", query, batch, err)
		}
	}

	for _, options := range []Options{
		{HTTPClient: &http.Client{}, UserAgent: "curl/1", MaxResults: 10, MaxBodyBytes: 1024, RatePerSecond: 1, Burst: 1, MaxConcurrency: 1},
		{Endpoint: "https://evil.example/api/query", HTTPClient: &http.Client{}, UserAgent: "Fetchmark/1 (https://example.com)", MaxResults: 10, MaxBodyBytes: 1024, RatePerSecond: 1, Burst: 1, MaxConcurrency: 1},
		{Endpoint: "https://export.arxiv.org/api/query?x=1", HTTPClient: &http.Client{}, UserAgent: "Fetchmark/1 (https://example.com)", MaxResults: 10, MaxBodyBytes: 1024, RatePerSecond: 1, Burst: 1, MaxConcurrency: 1},
	} {
		if _, err := New(options); err == nil {
			t.Fatalf("New accepted unsafe options: %+v", options)
		}
	}
}

func testOptions(transport http.RoundTripper) Options {
	return testOptionsWithClient(&http.Client{Transport: transport})
}

func testOptionsWithClient(httpClient *http.Client) Options {
	return Options{
		HTTPClient: httpClient, UserAgent: "FetchmarkBot/0.1 (https://github.com/staticvar/fetchmark)",
		MaxResults: 20, MaxBodyBytes: 1 << 20, RatePerSecond: 100, Burst: 100, MaxConcurrency: 100,
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

func atomResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status, Status: http.StatusText(status), Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(body)),
	}
}

func testFeed(entries string) string {
	return `<?xml version="1.0" encoding="UTF-8"?>
		<feed xmlns="http://www.w3.org/2005/Atom"
			xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/"
			xmlns:arxiv="http://arxiv.org/schemas/atom">
			<opensearch:totalResults>0</opensearch:totalResults>` + entries + `</feed>`
}

func intPointer(value int) *int { return &value }
