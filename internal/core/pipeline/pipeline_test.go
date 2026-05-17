package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/adapters/fetcher"
	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/rank"
	"github.com/staticvar/fetchmark/internal/core/search"
)

type stubSearcher struct {
	hits []search.Hit
	err  error
	last *search.Query
}

func (s stubSearcher) Search(_ context.Context, q search.Query) ([]search.Hit, error) {
	if s.last != nil {
		*s.last = q
	}
	return s.hits, s.err
}
func (s stubSearcher) Ping(_ context.Context) error { return nil }

type stubFetcher struct {
	resp map[string]fetcher.Result
}

func (f stubFetcher) Fetch(_ context.Context, r fetcher.Request) fetcher.Result {
	if v, ok := f.resp[r.URL]; ok {
		v.URL = r.URL
		return v
	}
	return fetcher.Result{URL: r.URL, Err: errors.New("no stub")}
}

type stubExtractor struct{}

func (stubExtractor) Extract(raw []byte, url string) (*model.Content, error) {
	return &model.Content{URL: url, Title: "T", MainText: string(raw), Markdown: "# " + string(raw)}, nil
}

type formattedExtractor struct{}

func (formattedExtractor) Extract(raw []byte, url string) (*model.Content, error) {
	return &model.Content{
		URL:         url,
		Title:       "T",
		MainText:    string(raw),
		Markdown:    "# " + string(raw),
		CleanedHTML: "<main>" + string(raw) + "</main>",
	}, nil
}

type countingFetcher struct {
	calls atomic.Int64
}

func (f *countingFetcher) Fetch(_ context.Context, r fetcher.Request) fetcher.Result {
	f.calls.Add(1)
	return fetcher.Result{URL: r.URL, Status: 200, Body: []byte(r.URL)}
}

type countingExtractor struct {
	calls atomic.Int64
}

func (e *countingExtractor) Extract(raw []byte, url string) (*model.Content, error) {
	e.calls.Add(1)
	return &model.Content{URL: url, Title: "T", MainText: string(raw), Markdown: string(raw)}, nil
}

type sharedOutcomeCache struct {
	out any
}

func (c sharedOutcomeCache) Get(context.Context, string) ([]byte, error) { return nil, nil }
func (c sharedOutcomeCache) Set(context.Context, string, []byte) error   { return nil }
func (c sharedOutcomeCache) Do(string, func() (any, error)) (any, error, bool) {
	return c.out, nil, true
}
func (c sharedOutcomeCache) WithLock(ctx context.Context, _ string, _ cache.LockOptions, fn func(context.Context) ([]byte, error)) ([]byte, error) {
	return fn(ctx)
}

func TestFetchAndExtractAppliesSharedSingleflightRaw(t *testing.T) {
	content := model.Content{URL: "https://a.example/x", Title: "Shared", MainText: "shared body", Markdown: "# shared"}
	raw, err := json.Marshal(content)
	if err != nil {
		t.Fatal(err)
	}
	p := &Pipeline{
		Fetcher:   stubFetcher{resp: map[string]fetcher.Result{}},
		Extractor: stubExtractor{},
		Cache:     sharedOutcomeCache{out: fetchOutcome{raw: raw, fromCache: true}},
	}
	r := &model.SearchResult{URL: "https://a.example/x"}

	p.fetchAndExtract(context.Background(), Options{}, r, false)

	if r.Title != "Shared" || r.Content == nil || r.Content.MainText != "shared body" || r.Markdown != "# shared" || !r.FromCache {
		t.Fatalf("shared singleflight raw was not applied to result: %+v", r)
	}
}

func TestFetchAndExtractAppliesSharedSingleflightUnsupported(t *testing.T) {
	p := &Pipeline{
		Fetcher:   stubFetcher{resp: map[string]fetcher.Result{}},
		Extractor: stubExtractor{},
		Cache:     sharedOutcomeCache{out: fetchOutcome{unsupported: fetcher.ReasonRobots, fetchMS: 12}},
	}
	r := &model.SearchResult{URL: "https://a.example/x"}

	p.fetchAndExtract(context.Background(), Options{}, r, false)

	if r.Unsupported != fetcher.ReasonRobots || r.FetchMS != 12 {
		t.Fatalf("shared unsupported outcome was not applied to result: %+v", r)
	}
}

func TestFetchAndExtractAppliesSharedSingleflightFetchFailure(t *testing.T) {
	p := &Pipeline{
		Fetcher:   stubFetcher{resp: map[string]fetcher.Result{}},
		Extractor: stubExtractor{},
		Cache:     sharedOutcomeCache{out: fetchOutcome{unsupported: "fetch_failed", fetchMS: 34}},
	}
	r := &model.SearchResult{URL: "https://a.example/x"}

	p.fetchAndExtract(context.Background(), Options{}, r, false)

	if r.Unsupported != "fetch_failed" || r.FetchMS != 34 {
		t.Fatalf("shared fetch failure was not applied to result: %+v", r)
	}
}

func TestSearchPropagatesControlsAndMetadata(t *testing.T) {
	publishedAt := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	var got search.Query
	p := &Pipeline{
		Searcher:  stubSearcher{hits: []search.Hit{{URL: "https://a.example/1", PublishedAt: &publishedAt, Metadata: map[string]string{"category": "news", "original_rank": "1"}}}, last: &got},
		Fetcher:   stubFetcher{resp: map[string]fetcher.Result{"https://a.example/1": {Status: 200, Body: []byte("body")}}},
		Extractor: stubExtractor{},
		Cache:     cache.New(nil, 0),
	}

	out, err := p.Search(context.Background(), Options{Query: "birds", Categories: []string{"general", "news"}, Language: "en", TimeRange: "year", MaxResults: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Categories) != 2 || got.Categories[0] != "general" || got.Categories[1] != "news" || got.Language != "en" || got.TimeRange != "year" {
		t.Fatalf("query controls not propagated: %+v", got)
	}
	if len(out) != 1 {
		t.Fatalf("got %d results, want 1", len(out))
	}
	if out[0].Metadata["category"] != "news" || out[0].Metadata["original_rank"] != "1" {
		t.Fatalf("metadata not copied: %+v", out)
	}
	if out[0].PublishedAt == nil || !out[0].PublishedAt.Equal(publishedAt) {
		t.Fatalf("published_at not copied: %+v", out[0].PublishedAt)
	}
}

func TestSearchCapsCandidatesBeforeFetch(t *testing.T) {
	hits := []search.Hit{
		{URL: "https://a.example/1"},
		{URL: "https://b.example/2"},
		{URL: "https://c.example/3"},
	}
	fetcher := &countingFetcher{}
	extractor := &countingExtractor{}
	p := &Pipeline{
		Searcher:  stubSearcher{hits: hits},
		Fetcher:   fetcher,
		Extractor: extractor,
		Cache:     cache.New(nil, 0),
	}

	out, err := p.Search(context.Background(), Options{Query: "hello", MaxResults: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("got %d results, want 1", len(out))
	}
	if got := fetcher.calls.Load(); got != 1 {
		t.Fatalf("fetched %d URLs, want 1", got)
	}
	if got := extractor.calls.Load(); got != 1 {
		t.Fatalf("extracted %d URLs, want 1", got)
	}
}

func TestPipeline_Search_EndToEnd(t *testing.T) {
	hits := []search.Hit{
		{URL: "https://a.example/x", Title: "Go BM25", Engines: []string{"g"}},
		{URL: "https://b.example/y", Title: "irrelevant", Engines: []string{"g", "b"}},
	}
	body := map[string]fetcher.Result{
		"https://a.example/x": {URL: "https://a.example/x", Status: 200, Body: []byte("go bm25 guide")},
		"https://b.example/y": {URL: "https://b.example/y", Status: 200, Body: []byte("cooking recipes")},
	}
	p := &Pipeline{
		Searcher:  stubSearcher{hits: hits},
		Fetcher:   stubFetcher{resp: body},
		Extractor: stubExtractor{},
		Cache:     cache.New(nil, 0),
		Ranker:    rank.New(),
	}
	out, err := p.Search(context.Background(), Options{Query: "bm25", MaxResults: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("got %d results", len(out))
	}
	if out[0].URL != "https://a.example/x" {
		t.Fatalf("ranking wrong; first=%v", out[0].URL)
	}
}

func TestPipeline_ParseFormatsMarkdownOnlyClearsDuplicateAndUnrequestedFields(t *testing.T) {
	p := formattedPipelineForParse()

	out := p.Parse(context.Background(), Options{URLs: []string{"https://a.example/x"}, Formats: []string{" Markdown "}})
	if len(out) != 1 {
		t.Fatalf("got %d results, want 1", len(out))
	}
	if out[0].Markdown == "" {
		t.Fatalf("top-level markdown was not preserved: %+v", out[0])
	}
	if out[0].Content == nil {
		t.Fatal("content was cleared")
	}
	if out[0].Content.Markdown != "" {
		t.Fatalf("duplicate content markdown was not cleared: %+v", out[0].Content)
	}
	if out[0].HTML != "" || out[0].Content.CleanedHTML != "" {
		t.Fatalf("html fields were not cleared: %+v", out[0])
	}
	if out[0].Content.MainText != "" {
		t.Fatalf("structured main text was not cleared: %+v", out[0].Content)
	}
}

func TestPipeline_ParseFormatsHTMLOnlyClearsDuplicateAndUnrequestedFields(t *testing.T) {
	p := formattedPipelineForParse()

	out := p.Parse(context.Background(), Options{URLs: []string{"https://a.example/x"}, Formats: []string{"html"}})
	if len(out) != 1 {
		t.Fatalf("got %d results, want 1", len(out))
	}
	if out[0].HTML == "" {
		t.Fatalf("top-level html was not preserved: %+v", out[0])
	}
	if out[0].Content == nil {
		t.Fatal("content was cleared")
	}
	if out[0].Content.CleanedHTML != "" {
		t.Fatalf("duplicate content html was not cleared: %+v", out[0].Content)
	}
	if out[0].Markdown != "" || out[0].Content.Markdown != "" {
		t.Fatalf("markdown fields were not cleared: %+v", out[0])
	}
	if out[0].Content.MainText != "" {
		t.Fatalf("structured main text was not cleared: %+v", out[0].Content)
	}
}

func TestPipeline_ParseUnknownOnlyFormatsPreserveContent(t *testing.T) {
	p := formattedPipelineForParse()

	out := p.Parse(context.Background(), Options{URLs: []string{"https://a.example/x"}, Formats: []string{"pdf", "unknown"}})
	if len(out) != 1 {
		t.Fatalf("got %d results, want 1", len(out))
	}
	assertFullFormattedContent(t, out[0])
}

func TestPipeline_SearchFormatsMarkdownOnlyFiltersResults(t *testing.T) {
	p := &Pipeline{
		Searcher: stubSearcher{hits: []search.Hit{{URL: "https://a.example/x"}}},
		Fetcher: stubFetcher{resp: map[string]fetcher.Result{
			"https://a.example/x": {URL: "https://a.example/x", Status: 200, Body: []byte("article")},
		}},
		Extractor: formattedExtractor{},
		Cache:     cache.New(nil, 0),
	}

	out, err := p.Search(context.Background(), Options{Formats: []string{"markdown"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("got %d results, want 1", len(out))
	}
	if out[0].Markdown == "" || out[0].Content == nil {
		t.Fatalf("markdown result missing content: %+v", out[0])
	}
	if out[0].Content.Markdown != "" || out[0].HTML != "" || out[0].Content.CleanedHTML != "" || out[0].Content.MainText != "" {
		t.Fatalf("search result was not format-filtered: %+v", out[0])
	}
}

func formattedPipelineForParse() *Pipeline {
	return &Pipeline{
		Fetcher: stubFetcher{resp: map[string]fetcher.Result{
			"https://a.example/x": {URL: "https://a.example/x", Status: 200, Body: []byte("article")},
		}},
		Extractor: formattedExtractor{},
		Cache:     cache.New(nil, 0),
	}
}

func assertFullFormattedContent(t *testing.T, got model.SearchResult) {
	t.Helper()
	if got.Markdown == "" || got.HTML == "" || got.Content == nil {
		t.Fatalf("top-level/content fields missing: %+v", got)
	}
	if got.Content.Markdown == "" || got.Content.CleanedHTML == "" || got.Content.MainText == "" {
		t.Fatalf("content was not preserved: %+v", got.Content)
	}
}

func TestPipeline_CacheHitSkipsFetch(t *testing.T) {
	c := cache.New(nil, 0)
	u := "https://a.example/x"
	canon, _ := cache.CanonicalURL(u)
	blob := []byte(`{"url":"` + canon + `","title":"Cached","text":"content","markdown":"# c"}`)
	if err := c.Set(context.Background(), cache.ArtifactKey(canon), blob); err != nil {
		t.Fatal(err)
	}
	p := &Pipeline{
		Fetcher:   stubFetcher{resp: map[string]fetcher.Result{}},
		Extractor: stubExtractor{},
		Cache:     c,
		Ranker:    rank.New(),
	}
	out := p.Parse(context.Background(), Options{URLs: []string{u}, Query: "cached"})
	if len(out) != 1 || !out[0].FromCache || out[0].Title != "Cached" {
		t.Fatalf("cache path failed: %+v", out)
	}
}

func TestPipeline_URLDedupeMergesEngines(t *testing.T) {
	hits := []search.Hit{
		{URL: "https://a.example/x?utm_source=1", Engines: []string{"g"}},
		{URL: "https://A.example/x", Engines: []string{"b"}},
	}
	p := &Pipeline{
		Searcher:  stubSearcher{hits: hits},
		Fetcher:   stubFetcher{resp: map[string]fetcher.Result{"https://a.example/x": {Status: 200, Body: []byte("x")}}},
		Extractor: stubExtractor{},
		Cache:     cache.New(nil, 0),
	}
	out, _ := p.Search(context.Background(), Options{Query: ""})
	if len(out) != 1 {
		t.Fatalf("expected 1 deduped result, got %d", len(out))
	}
	if len(out[0].Engines) != 2 {
		t.Fatalf("expected 2 engines merged, got %v", out[0].Engines)
	}
}

func TestPipeline_ContentSHADedupes(t *testing.T) {
	hits := []search.Hit{
		{URL: "https://a.example/1"},
		{URL: "https://b.example/2"},
	}
	p := &Pipeline{
		Searcher: stubSearcher{hits: hits},
		Fetcher: stubFetcher{resp: map[string]fetcher.Result{
			"https://a.example/1": {Body: []byte("identical body")},
			"https://b.example/2": {Body: []byte("identical body")},
		}},
		Extractor: stubExtractor{},
		Cache:     cache.New(nil, 0),
	}
	out, _ := p.Search(context.Background(), Options{})
	if len(out) != 1 {
		t.Fatalf("content dedupe failed, got %d", len(out))
	}
}
