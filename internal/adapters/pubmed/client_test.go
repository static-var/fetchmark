package pubmed

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/search"
)

func TestClientSearchBatchMapsBoundedMetadataFlow(t *testing.T) {
	transport := &scriptedTransport{responses: []scriptedResponse{
		{body: `{"header":{"type":"esearch","version":"0.3"},"esearchresult":{"count":"2","retmax":"2","retstart":"0","idlist":["42","43"],"querytranslation":"night shift circadian metabolic biomarkers"}}`},
		{body: `{"header":{"type":"esummary","version":"0.3"},"result":{"uids":["42","43"],"42":{"uid":"42","pubdate":"2025 Nov","source":"J Open Med","authors":[{"name":"Ada A"},{"name":"Turing B"}],"title":"Night-shift work and metabolic biomarkers.","lang":["eng"],"pubtype":["Journal Article"],"articleids":[{"idtype":"pubmed","value":"42"},{"idtype":"doi","value":"10.1000/example"}],"fulljournalname":"Journal of Open Medicine","sortpubdate":"2025/11/03 00:00"},"43":{"uid":"43","pubdate":"2024","source":"Chronobiology","authors":[{"name":"River C"}],"title":"Circadian disruption in longitudinal cohorts.","lang":["eng"],"pubtype":["Meta-Analysis"],"articleids":[{"idtype":"pubmed","value":"43"}],"fulljournalname":"Chronobiology Reports","sortpubdate":"2024/01/01 00:00"}}}`},
	}}
	client := mustNew(t, Options{
		HTTPClient: &http.Client{Transport: transport},
		UserAgent:  "Fetchmark-Test/1.0 (+https://example.test/contact)",
		Email:      "operator@example.test", MaxResults: 10, MaxBodyBytes: 1 << 20,
		RatePerSecond: 100, Burst: 10, MaxConcurrency: 10,
	})
	batch, err := client.SearchBatch(context.Background(), search.Query{
		Q: "night-shift work, circadian disruption, and metabolic biomarkers?", MaxResults: 2,
	})
	if err != nil {
		t.Fatalf("SearchBatch: %v", err)
	}
	if batch.Status != search.BatchHealthy || batch.Provider != "pubmed" || batch.Instance != "eutils.ncbi.nlm.nih.gov" || len(batch.Hits) != 2 {
		t.Fatalf("batch = %+v", batch)
	}
	hit := batch.Hits[0]
	if hit.URL != "https://pubmed.ncbi.nlm.nih.gov/42/" || hit.Title != "Night-shift work and metabolic biomarkers." {
		t.Fatalf("first hit identity = %+v", hit)
	}
	if hit.Snippet != "Ada A, Turing B · Journal of Open Medicine · 2025" {
		t.Fatalf("snippet = %q", hit.Snippet)
	}
	if hit.PublishedAt == nil || !hit.PublishedAt.Equal(time.Date(2025, 11, 3, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("published = %v", hit.PublishedAt)
	}
	if hit.Metadata["pmid"] != "42" || hit.Metadata["doi"] != "10.1000/example" || hit.Metadata["source"] != "PubMed / NLM" || hit.Metadata["staleness"] != "observed_at_query_time" {
		t.Fatalf("metadata = %+v", hit.Metadata)
	}
	if hit.ProviderDocument == nil || len(hit.ProviderDocument.HTML) == 0 || hit.ProviderDocument.SiteName != "PubMed" {
		t.Fatalf("provider document = %+v", hit.ProviderDocument)
	}
	providerHTML := string(hit.ProviderDocument.HTML)
	if !strings.Contains(providerHTML, "Journal of Open Medicine") || strings.Contains(strings.ToLower(providerHTML), "abstract") {
		t.Fatalf("metadata-only provider HTML = %q", providerHTML)
	}

	requests := transport.Requests()
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want 2", len(requests))
	}
	assertCommonRequest(t, requests[0])
	assertCommonRequest(t, requests[1])
	if requests[0].URL.Path != "/entrez/eutils/esearch.fcgi" || requests[0].URL.Query().Get("retmax") != "2" || requests[0].URL.Query().Get("sort") != "relevance" || requests[0].URL.Query().Get("retstart") != "0" {
		t.Fatalf("ESearch request = %s", requests[0].URL)
	}
	if got := requests[0].URL.Query().Get("term"); got != "night shift work circadian disruption and metabolic biomarkers" {
		t.Fatalf("projected term = %q", got)
	}
	if requests[1].URL.Path != "/entrez/eutils/esummary.fcgi" || requests[1].URL.Query().Get("id") != "42,43" {
		t.Fatalf("ESummary request = %s", requests[1].URL)
	}
}

func TestTitleOnlyRecordStillCarriesNoFetchMetadataDocument(t *testing.T) {
	transport := &scriptedTransport{responses: []scriptedResponse{
		{body: `{"esearchresult":{"count":"1","idlist":["42"]}}`},
		{body: `{"result":{"uids":["42"],"42":{"uid":"42","title":"A title-only citation."}}}`},
	}}
	client := newTestClient(t, transport)
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "title only citation"})
	if err != nil || len(batch.Hits) != 1 {
		t.Fatalf("SearchBatch = %+v, %v", batch, err)
	}
	document := batch.Hits[0].ProviderDocument
	if document == nil || len(document.HTML) == 0 {
		t.Fatalf("title-only citation would fall through to page fetch: %+v", document)
	}
}

func TestClientClassifiesAuthoritativeEmptyWithoutSummaryRequest(t *testing.T) {
	transport := &scriptedTransport{responses: []scriptedResponse{{body: `{"esearchresult":{"count":"0","idlist":[]}}`}}}
	client := newTestClient(t, transport)
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "no matching biomedical citation"})
	if err != nil || batch.Status != search.BatchAuthoritativeEmpty || len(batch.Hits) != 0 {
		t.Fatalf("SearchBatch = %+v, %v", batch, err)
	}
	if got := len(transport.Requests()); got != 1 {
		t.Fatalf("request count = %d, want ESearch only", got)
	}
}

func TestClientRetainsValidRowsAsPartial(t *testing.T) {
	transport := &scriptedTransport{responses: []scriptedResponse{
		{body: `{"esearchresult":{"count":"2","idlist":["42","43"]}}`},
		{body: `{"result":{"uids":["42"],"42":{"uid":"42","title":"One valid citation."}}}`},
	}}
	client := newTestClient(t, transport)
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "partially malformed result"})
	if err != nil || batch.Status != search.BatchPartial || len(batch.Hits) != 1 || len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Reason != "malformed_results" {
		t.Fatalf("SearchBatch = %+v, %v", batch, err)
	}
}

func TestClientBoundsProviderIDsBeforeSummaryRequest(t *testing.T) {
	transport := &scriptedTransport{responses: []scriptedResponse{
		{body: `{"esearchresult":{"count":"3","idlist":["41","42","43"]}}`},
		{body: `{"result":{"uids":["41","42"],"41":{"uid":"41","title":"First bounded citation."},"42":{"uid":"42","title":"Second bounded citation."}}}`},
	}}
	client := newTestClient(t, transport)
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "bounded provider IDs", MaxResults: 2})
	if err != nil || batch.Status != search.BatchPartial || len(batch.Hits) != 2 {
		t.Fatalf("SearchBatch = %+v, %v", batch, err)
	}
	requests := transport.Requests()
	if len(requests) != 2 || requests[1].URL.Query().Get("id") != "41,42" {
		t.Fatalf("summary request exceeded local result cap: %+v", requests)
	}
}

func TestJSONRateLimitOpensCooldownWithoutSummaryRequest(t *testing.T) {
	transport := &scriptedTransport{responses: []scriptedResponse{{body: `{"error":"API rate limit exceeded","count":"11"}`}}}
	client := newTestClient(t, transport)
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "rate limited query"})
	if err == nil || batch.Status != search.BatchFailed || len(batch.Diagnostics) != 1 || !batch.Diagnostics[0].Retryable || batch.Diagnostics[0].RetryAfter != time.Minute {
		t.Fatalf("first SearchBatch = %+v, %v", batch, err)
	}
	batch, err = client.SearchBatch(context.Background(), search.Query{Q: "second query"})
	if err == nil || len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Reason != "cooldown" {
		t.Fatalf("cooldown SearchBatch = %+v, %v", batch, err)
	}
	if got := len(transport.Requests()); got != 1 {
		t.Fatalf("cooldown made %d requests, want 1 total", got)
	}
}

func TestClientRejectsUnsupportedControlsAndExcludedDomainBeforeEgress(t *testing.T) {
	transport := &scriptedTransport{}
	client := newTestClient(t, transport)
	_, err := client.SearchBatch(context.Background(), search.Query{Q: "citation", ExactMatch: true})
	var unsupported *search.UnsupportedControlError
	if !errors.As(err, &unsupported) || unsupported.Control != "exact_match" {
		t.Fatalf("exact-match error = %T %v", err, err)
	}
	moderate := 1
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "citation", Language: "en", SafeSearch: &moderate})
	if !errors.As(err, &unsupported) || unsupported.Control != "language" || batch.Status != search.BatchFailed {
		t.Fatalf("Brave-default controls = %+v, %T %v", batch, err, err)
	}
	batch, err = client.SearchBatch(context.Background(), search.Query{Q: "citation", ExcludeDomains: []string{"pubmed.ncbi.nlm.nih.gov"}})
	if err != nil || batch.Status != search.BatchAuthoritativeEmpty {
		t.Fatalf("excluded domain = %+v, %v", batch, err)
	}
	if got := len(transport.Requests()); got != 0 {
		t.Fatalf("preflight made %d requests", got)
	}
}

func TestClientAppliesDomainControlsToCanonicalResultHost(t *testing.T) {
	for _, test := range []struct {
		name           string
		includeDomains []string
		excludeDomains []string
	}{
		{name: "exact result host included", includeDomains: []string{"pubmed.ncbi.nlm.nih.gov"}},
		{name: "API host exclusion does not exclude results", excludeDomains: []string{"eutils.ncbi.nlm.nih.gov"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &scriptedTransport{responses: []scriptedResponse{{body: `{"esearchresult":{"count":"0","idlist":[]}}`}}}
			client := newTestClient(t, transport)
			batch, err := client.SearchBatch(context.Background(), search.Query{
				Q: "citation", IncludeDomains: test.includeDomains, ExcludeDomains: test.excludeDomains,
			})
			if err != nil || batch.Status != search.BatchAuthoritativeEmpty {
				t.Fatalf("SearchBatch = %+v, %v", batch, err)
			}
			if got := len(transport.Requests()); got != 1 {
				t.Fatalf("request count = %d, want 1", got)
			}
		})
	}
}

func TestClientRefusesRedirectWithoutFollowing(t *testing.T) {
	header := make(http.Header)
	header.Set("Location", providerOrigin+searchPath+"?db=pubmed")
	transport := &scriptedTransport{responses: []scriptedResponse{{status: http.StatusFound, header: header}}}
	client := newTestClient(t, transport)
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "redirected query"})
	var statusError *StatusError
	if !errors.As(err, &statusError) || statusError.StatusCode != http.StatusFound || batch.Status != search.BatchFailed {
		t.Fatalf("redirect SearchBatch = %+v, %T %v", batch, err, err)
	}
	if got := len(transport.Requests()); got != 1 {
		t.Fatalf("redirect request count = %d, want 1", got)
	}
}

func TestProjectQueryStripsCallerSyntaxButPreservesNaturalAnd(t *testing.T) {
	got, err := projectQuery(`Cancer[Title] OR "night-shift" and NOT cohort*`)
	if err != nil || got != "cancer night shift and cohort" {
		t.Fatalf("projectQuery = %q, %v", got, err)
	}
}

func TestNewRequiresPlainOperatorEmailAndCapsSharedServiceBudget(t *testing.T) {
	base := Options{HTTPClient: &http.Client{}, UserAgent: "Fetchmark-Test/1.0 (+https://example.test/contact)", MaxResults: 100, MaxBodyBytes: 1024, RatePerSecond: 100, Burst: 10, MaxConcurrency: 10}
	if _, err := New(base); err == nil {
		t.Fatal("missing operator email accepted")
	}
	base.Email = "Fetchmark Operator <operator@example.test>"
	if _, err := New(base); err == nil {
		t.Fatal("display-name email accepted")
	}
	base.Email = "operator@example.test"
	client, err := New(base)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if client.maxResults != maxProviderResults || client.budget.Burst() != 1 || client.budget.MaxConcurrency() != 1 {
		t.Fatalf("caps = results %d burst %d concurrency %d", client.maxResults, client.budget.Burst(), client.budget.MaxConcurrency())
	}
	base.Email = strings.Repeat("a", 244) + "@example.test"
	if _, err := New(base); err == nil {
		t.Fatal("oversized operator email accepted")
	}
}

func assertCommonRequest(t *testing.T, request *http.Request) {
	t.Helper()
	query := request.URL.Query()
	if request.Method != http.MethodGet || query.Get("db") != "pubmed" || query.Get("retmode") != "json" || query.Get("tool") != "fetchmark" || query.Get("email") != "operator@example.test" {
		t.Fatalf("common request contract = %s %s", request.Method, request.URL)
	}
	if request.Header.Get("Accept") != "application/json" || request.Header.Get("User-Agent") != "Fetchmark-Test/1.0 (+https://example.test/contact)" {
		t.Fatalf("headers = %+v", request.Header)
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

func newTestClient(t *testing.T, transport http.RoundTripper) *Client {
	t.Helper()
	return mustNew(t, Options{
		HTTPClient: &http.Client{Transport: transport},
		UserAgent:  "Fetchmark-Test/1.0 (+https://example.test/contact)", Email: "operator@example.test",
		MaxResults: 10, MaxBodyBytes: 1 << 20, RatePerSecond: 100, Burst: 10, MaxConcurrency: 10,
	})
}

type scriptedResponse struct {
	status int
	body   string
	header http.Header
}

type scriptedTransport struct {
	mu        sync.Mutex
	responses []scriptedResponse
	requests  []*http.Request
}

func (transport *scriptedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	clone := request.Clone(context.Background())
	clone.URL = cloneURL(request.URL)
	transport.requests = append(transport.requests, clone)
	if len(transport.responses) == 0 {
		return &http.Response{StatusCode: http.StatusInternalServerError, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("unexpected request")), Request: request}, nil
	}
	response := transport.responses[0]
	transport.responses = transport.responses[1:]
	status := response.status
	if status == 0 {
		status = http.StatusOK
	}
	header := response.header
	if header == nil {
		header = make(http.Header)
	}
	return &http.Response{StatusCode: status, Header: header.Clone(), Body: io.NopCloser(strings.NewReader(response.body)), Request: request}, nil
}

func (transport *scriptedTransport) Requests() []*http.Request {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return append([]*http.Request(nil), transport.requests...)
}

func cloneURL(value *url.URL) *url.URL {
	cloned := *value
	return &cloned
}
