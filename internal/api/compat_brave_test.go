package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/docindex"
	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/pipeline"
	"github.com/staticvar/fetchmark/internal/core/search"
)

type docIndexCompatPipeline struct {
	index *docindex.Index
}

func (p *docIndexCompatPipeline) Search(ctx context.Context, options pipeline.Options) ([]model.SearchResult, error) {
	batch, err := p.index.SearchBatch(ctx, search.Query{
		Q: options.Query, Engines: options.Engines, Categories: options.Categories,
		Language: options.Language, TimeRange: options.TimeRange, SafeSearch: options.SafeSearch,
		IncludeDomains: options.IncludeDomains, ExcludeDomains: options.ExcludeDomains,
		ExactMatch: options.ExactMatch, SearchDepth: options.SearchDepth, MaxResults: options.MaxResults,
	})
	if err != nil {
		return nil, err
	}
	results := make([]model.SearchResult, 0, len(batch.Hits))
	for _, hit := range batch.Hits {
		results = append(results, model.SearchResult{
			URL: hit.URL, Title: hit.Title, Snippet: hit.Snippet, Metadata: hit.Metadata,
			Provenance: hit.Provenance, Content: &model.Content{Language: hit.Metadata["language"]},
		})
	}
	return results, nil
}

func (*docIndexCompatPipeline) Parse(context.Context, pipeline.Options) []model.SearchResult {
	return nil
}

func TestBraveCompatSearchMapsQueryPaginationAndResults(t *testing.T) {
	pipe := &fakePipeline{results: []model.SearchResult{
		{URL: "https://example.com/one", Title: "One", Snippet: "first"},
		{URL: "https://example.com/two", Title: "Two", Snippet: "second"},
		{URL: "https://example.com/three", Title: "Three", Snippet: "third", Content: &model.Content{Language: "en"}},
		{URL: "https://example.com/four", Title: "Four", Snippet: "fourth"},
	}}
	router := compatTestRouter(pipe)
	request := httptest.NewRequest(http.MethodGet, "/compat/brave/res/v1/web/search?q=open+search&count=2&offset=1&freshness=pm&safesearch=strict&search_lang=en", nil)
	request.Header.Set("X-Subscription-Token", "k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if pipe.lastOpts.MaxResults != 5 || pipe.lastOpts.TimeRange != "month" || pipe.lastOpts.Language != "en" || pipe.lastOpts.SafeSearch == nil || *pipe.lastOpts.SafeSearch != 2 {
		t.Fatalf("options = %+v", pipe.lastOpts)
	}
	var response braveSearchResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Type != "search" || response.Query.Original != "open search" || len(response.Web.Results) != 2 || response.Web.Results[0].URL != "https://example.com/three" || response.Web.Results[0].Language != "en" {
		t.Fatalf("response = %+v", response)
	}
}

func TestBraveCompatRejectsPageBeyondInstanceCap(t *testing.T) {
	router := compatTestRouter(&fakePipeline{})
	request := httptest.NewRequest(http.MethodGet, "/compat/brave/res/v1/web/search?q=x&count=20&offset=2", nil)
	request.Header.Set("X-Subscription-Token", "k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnprocessableEntity || !strings.Contains(recorder.Body.String(), `"type":"ErrorResponse"`) || !strings.Contains(recorder.Body.String(), "instance result limit") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestBraveCompatRejectsUnsupportedControls(t *testing.T) {
	router := compatTestRouter(&fakePipeline{})
	request := httptest.NewRequest(http.MethodGet, "/compat/brave/res/v1/web/search?q=x&result_filter=web", nil)
	request.Header.Set("X-Subscription-Token", "k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnprocessableEntity || !strings.Contains(recorder.Body.String(), "result_filter") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestBraveCompatUsesVendorAuthShape(t *testing.T) {
	router := compatTestRouter(&fakePipeline{})
	request := httptest.NewRequest(http.MethodGet, "/compat/brave/res/v1/web/search?q=x", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized || !strings.Contains(recorder.Body.String(), `"code":"SUBSCRIPTION_TOKEN_INVALID"`) || strings.Contains(recorder.Body.String(), `"id":""`) {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestBraveCompatAppliesDocumentedDefaults(t *testing.T) {
	pipe := &fakePipeline{}
	router := compatTestRouter(pipe)
	request := httptest.NewRequest(http.MethodGet, "/compat/brave/res/v1/web/search?q=x", nil)
	request.Header.Set("X-Subscription-Token", "k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if pipe.lastOpts.Language != "en" || pipe.lastOpts.SafeSearch == nil || *pipe.lastOpts.SafeSearch != 1 || pipe.lastOpts.MaxResults != 21 {
		t.Fatalf("options = %+v", pipe.lastOpts)
	}
}

func TestBraveCompatDefaultsAdmitSafeEnglishOfficialDocs(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("..", "adapters", "docindex", "testdata", "official-docs.json"))
	if err != nil {
		t.Fatal(err)
	}
	index, err := docindex.Open(docindex.Options{Path: path, Now: time.Date(2026, 7, 20, 1, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })
	router := compatTestRouter(&docIndexCompatPipeline{index: index})
	request := httptest.NewRequest(http.MethodGet, "/compat/brave/res/v1/web/search?q=Kotlin+coroutine+cancellation", nil)
	request.Header.Set("X-Subscription-Token", "k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response braveSearchResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Web.Results) == 0 || response.Web.Results[0].URL != "https://kotlinlang.org/docs/cancellation-and-timeouts.html" || response.Web.Results[0].Language != "en" {
		t.Fatalf("response = %+v", response)
	}
}
