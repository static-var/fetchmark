package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/adapters/fetcher"
	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/rank"
	"github.com/staticvar/fetchmark/internal/core/search"
)

type stubSearcher struct {
	hits []search.Hit
	err  error
}

func (s stubSearcher) Search(_ context.Context, _ search.Query) ([]search.Hit, error) {
	return s.hits, s.err
}
func (s stubSearcher) Ping(_ context.Context) error { return nil }

type stubFetcher struct {
	resp map[string]fetcher.Result
}

func (f stubFetcher) FetchMany(_ context.Context, reqs []fetcher.Request) []fetcher.Result {
	out := make([]fetcher.Result, len(reqs))
	for i, r := range reqs {
		if v, ok := f.resp[r.URL]; ok {
			out[i] = v
		} else {
			out[i] = fetcher.Result{URL: r.URL, Err: errors.New("no stub")}
		}
	}
	return out
}

type stubExtractor struct{}

func (stubExtractor) Extract(raw []byte, url string) (*model.Content, error) {
	return &model.Content{URL: url, Title: "T", MainText: string(raw), Markdown: "# " + string(raw)}, nil
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
