package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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

type emptyExtractor struct{}

func (emptyExtractor) Extract(_ []byte, url string) (*model.Content, error) {
	return &model.Content{URL: url, Title: "T"}, nil
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
	safeSearch := 1
	p := &Pipeline{
		Searcher:  stubSearcher{hits: []search.Hit{{URL: "https://a.example/1", PublishedAt: &publishedAt, Metadata: map[string]string{"category": "news", "original_rank": "1"}}}, last: &got},
		Fetcher:   stubFetcher{resp: map[string]fetcher.Result{"https://a.example/1": {Status: 200, Body: []byte("body")}}},
		Extractor: stubExtractor{},
		Cache:     cache.New(nil, 0),
	}

	out, err := p.Search(context.Background(), Options{
		Query:          "birds",
		Categories:     []string{"general", "news"},
		Language:       "en",
		TimeRange:      "year",
		SafeSearch:     &safeSearch,
		IncludeDomains: []string{"a.example"},
		ExcludeDomains: []string{"b.example"},
		ExactMatch:     true,
		MaxResults:     10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Categories) != 2 || got.Categories[0] != "general" || got.Categories[1] != "news" || got.Language != "en" || got.TimeRange != "year" {
		t.Fatalf("query controls not propagated: %+v", got)
	}
	if got.SafeSearch == nil || *got.SafeSearch != 1 || len(got.IncludeDomains) != 1 || got.IncludeDomains[0] != "a.example" || len(got.ExcludeDomains) != 1 || got.ExcludeDomains[0] != "b.example" || !got.ExactMatch {
		t.Fatalf("advanced query controls not propagated: %+v", got)
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

func TestSearchFiltersDomainsBeforeFetch(t *testing.T) {
	fetcher := &countingFetcher{}
	p := &Pipeline{
		Searcher: stubSearcher{hits: []search.Hit{
			{URL: "https://docs.example.com/keep"},
			{URL: "https://blog.example.com/drop"},
			{URL: "https://blocked.example.com/keep-looking"},
			{URL: "https://other.test/also-drop"},
		}},
		Fetcher:   fetcher,
		Extractor: stubExtractor{},
		Cache:     cache.New(nil, 0),
	}

	out, err := p.Search(context.Background(), Options{
		Query:          "keep",
		IncludeDomains: []string{"example.com"},
		ExcludeDomains: []string{"blog.example.com", "blocked.example.com"},
		MaxResults:     10,
		CandidateCap:   10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].URL != "https://docs.example.com/keep" {
		t.Fatalf("domain-filtered results = %+v", out)
	}
	if got := fetcher.calls.Load(); got != 1 {
		t.Fatalf("fetched %d URLs, want only the included non-excluded URL", got)
	}
}

func TestSearchAttachesQueryFocusedChunks(t *testing.T) {
	p := &Pipeline{
		Searcher: stubSearcher{hits: []search.Hit{
			{URL: "https://a.example/article", Title: "Bird migration"},
		}},
		Fetcher: stubFetcher{resp: map[string]fetcher.Result{
			"https://a.example/article": {Status: 200, Body: []byte("Intro paragraph about unrelated weather.\n\nDetailed migration routes show birds crossing oceans every year.\n\nAnother migration paragraph mentions birds and route timing.")},
		}},
		Extractor: stubExtractor{},
		Cache:     cache.New(nil, 0),
	}

	out, err := p.Search(context.Background(), Options{Query: "bird migration routes", MaxResults: 5, CandidateCap: 5, ChunksPerSource: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("got %d results, want 1", len(out))
	}
	if len(out[0].Chunks) != 2 {
		t.Fatalf("chunks = %+v, want 2", out[0].Chunks)
	}
	if !strings.Contains(out[0].Chunks[0].Text, "migration routes") {
		t.Fatalf("top chunk should be the most query-focused paragraph: %+v", out[0].Chunks)
	}
	if out[0].Chunks[0].Score <= out[0].Chunks[1].Score {
		t.Fatalf("chunks should be sorted by score desc: %+v", out[0].Chunks)
	}
}

func TestSearchOmitsZeroScoreChunks(t *testing.T) {
	p := &Pipeline{
		Searcher: stubSearcher{hits: []search.Hit{
			{URL: "https://a.example/article", Title: "Weather"},
		}},
		Fetcher: stubFetcher{resp: map[string]fetcher.Result{
			"https://a.example/article": {Status: 200, Body: []byte("Clouds formed over the city.\n\nRain moved across the coastline.")},
		}},
		Extractor: stubExtractor{},
		Cache:     cache.New(nil, 0),
	}

	out, err := p.Search(context.Background(), Options{Query: "bird migration routes", MaxResults: 5, CandidateCap: 5, ChunksPerSource: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("got %d results, want 1", len(out))
	}
	if len(out[0].Chunks) != 0 {
		t.Fatalf("zero-score chunks should be omitted: %+v", out[0].Chunks)
	}
}

func TestSearchOmitsChunksWithoutExtractedContent(t *testing.T) {
	p := &Pipeline{
		Searcher: stubSearcher{hits: []search.Hit{
			{URL: "https://a.example/article", Snippet: "bird migration routes"},
		}},
		Fetcher:   stubFetcher{resp: map[string]fetcher.Result{"https://a.example/article": {Status: 200, Body: []byte("body")}}},
		Extractor: emptyExtractor{},
		Cache:     cache.New(nil, 0),
	}

	out, err := p.Search(context.Background(), Options{Query: "bird migration routes", MaxResults: 5, CandidateCap: 5, ChunksPerSource: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("got %d results, want 1", len(out))
	}
	if len(out[0].Chunks) != 0 {
		t.Fatalf("chunks should require extracted page text, got: %+v", out[0].Chunks)
	}
}

func TestSearchCapsCandidatesBeforeFetch(t *testing.T) {
	hits := make([]search.Hit, 20)
	resp := make(map[string]fetcher.Result, len(hits))
	for i := range hits {
		u := "https://example.com/" + string(rune('a'+i))
		hits[i] = search.Hit{URL: u}
		resp[u] = fetcher.Result{Status: 200, Body: []byte(u)}
	}
	fetcher := &countingFetcher{}
	extractor := &countingExtractor{}
	var got search.Query
	p := &Pipeline{
		Searcher:  stubSearcher{hits: hits, last: &got},
		Fetcher:   fetcher,
		Extractor: extractor,
		Cache:     cache.New(nil, 0),
	}

	out, err := p.Search(context.Background(), Options{Query: "hello", MaxResults: 5, CandidateCap: 15})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) > 5 {
		t.Fatalf("got %d results, want at most 5", len(out))
	}
	if got.MaxResults != 15 {
		t.Fatalf("search query max results = %d, want candidate cap 15", got.MaxResults)
	}
	if got := fetcher.calls.Load(); got != 15 {
		t.Fatalf("fetched %d URLs, want 15", got)
	}
	if got := extractor.calls.Load(); got != 15 {
		t.Fatalf("extracted %d URLs, want 15", got)
	}
}

func TestSearchDuplicateCandidatesDoNotStarveLaterUniqueResults(t *testing.T) {
	hits := []search.Hit{
		{URL: "https://a.example/same?utm_source=one"},
		{URL: "https://a.example/same?utm_source=two"},
		{URL: "https://b.example/unique"},
	}
	p := &Pipeline{
		Searcher: stubSearcher{hits: hits},
		Fetcher: stubFetcher{resp: map[string]fetcher.Result{
			"https://a.example/same":   {Status: 200, Body: []byte("first")},
			"https://b.example/unique": {Status: 200, Body: []byte("later relevant unique")},
		}},
		Extractor: stubExtractor{},
		Cache:     cache.New(nil, 0),
		Ranker:    rank.New(),
	}

	out, err := p.Search(context.Background(), Options{Query: "later relevant unique", MaxResults: 1, CandidateCap: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("got %d results, want 1", len(out))
	}
	if out[0].URL != "https://b.example/unique" {
		t.Fatalf("duplicate-before-cap starved later unique candidate: %+v", out)
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
