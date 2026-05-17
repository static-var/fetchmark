package pipeline

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/core/search"
)

type recordingExpansionSearcher struct {
	queries   []search.Query
	responses func(search.Query) []search.Hit
}

func (s *recordingExpansionSearcher) Search(_ context.Context, q search.Query) ([]search.Hit, error) {
	s.queries = append(s.queries, q)
	if s.responses == nil {
		return nil, nil
	}
	return s.responses(q), nil
}

func TestExpansionNonAdvancedUsesSingleOriginalSearch(t *testing.T) {
	searcher := &recordingExpansionSearcher{}
	p := &Pipeline{Searcher: searcher}

	_, err := p.Search(context.Background(), Options{
		Query:        "golang api",
		MaxResults:   2,
		CandidateCap: 7,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(searcher.queries) != 1 {
		t.Fatalf("search calls = %d, want exactly 1", len(searcher.queries))
	}
	got := searcher.queries[0]
	if got.Q != "golang api" {
		t.Fatalf("query = %q, want original", got.Q)
	}
	if got.MaxResults != 7 {
		t.Fatalf("max results = %d, want candidate cap 7", got.MaxResults)
	}
}

func TestAdvancedExpansionBuildsDeterministicVariantsWithControls(t *testing.T) {
	safeSearch := 1
	searcher := &recordingExpansionSearcher{}
	p := &Pipeline{Searcher: searcher}

	_, err := p.Search(context.Background(), Options{
		Query:          "latest golang api 2026",
		Engines:        []string{"google"},
		Categories:     []string{"general"},
		Language:       "en",
		TimeRange:      "month",
		SafeSearch:     &safeSearch,
		IncludeDomains: []string{"go.dev"},
		ExcludeDomains: []string{"example.com"},
		SearchDepth:    "advanced",
		MaxResults:     3,
		CandidateCap:   9,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(searcher.queries) < 4 {
		t.Fatalf("advanced search calls = %d, want original, exact, freshness, and docs variants: %+v", len(searcher.queries), searcher.queries)
	}
	if searcher.queries[0].Q != "latest golang api 2026" || searcher.queries[0].ExactMatch {
		t.Fatalf("first variant should be the original non-exact query: %+v", searcher.queries[0])
	}

	var sawExact, sawFresh, sawDocs bool
	seen := map[string]bool{}
	for _, q := range searcher.queries {
		key := fmt.Sprintf("%s|%t", q.Q, q.ExactMatch)
		if seen[key] {
			t.Fatalf("duplicate variant %q", key)
		}
		seen[key] = true
		if q.MaxResults != 9 || q.Language != "en" || q.TimeRange != "month" || q.SafeSearch == nil || *q.SafeSearch != 1 {
			t.Fatalf("query controls not preserved: %+v", q)
		}
		if len(q.Engines) != 1 || q.Engines[0] != "google" || len(q.Categories) != 1 || q.Categories[0] != "general" {
			t.Fatalf("query list controls not preserved: %+v", q)
		}
		if len(q.IncludeDomains) != 1 || q.IncludeDomains[0] != "go.dev" || len(q.ExcludeDomains) != 1 || q.ExcludeDomains[0] != "example.com" {
			t.Fatalf("domain controls not preserved: %+v", q)
		}
		if q.Q == "latest golang api 2026" && q.ExactMatch {
			sawExact = true
		}
		if q.Q != "latest golang api 2026" && strings.Contains(strings.ToLower(q.Q), "news") {
			sawFresh = true
		}
		lower := strings.ToLower(q.Q)
		if q.Q != "latest golang api 2026" && (strings.Contains(lower, "documentation") || strings.Contains(lower, "official docs")) {
			sawDocs = true
		}
	}
	if !sawExact || !sawFresh || !sawDocs {
		t.Fatalf("missing expected variants: exact=%t fresh=%t docs=%t queries=%+v", sawExact, sawFresh, sawDocs, searcher.queries)
	}
}

func TestAdvancedRRFFusesDuplicateCanonicalURLsBeforeFetch(t *testing.T) {
	searcher := &recordingExpansionSearcher{
		responses: func(q search.Query) []search.Hit {
			switch {
			case q.ExactMatch:
				return []search.Hit{
					{URL: "https://b.example/doc?utm_source=exact", Title: "B exact", Snippet: "exact", Engines: []string{"bing"}, Metadata: map[string]string{"original_rank": "1", "exact_meta": "yes"}},
					{URL: "https://c.example/doc", Title: "C exact", Engines: []string{"bing"}},
				}
			case strings.Contains(q.Q, "official docs"):
				return []search.Hit{
					{URL: "https://b.example/doc", Title: "B docs", Engines: []string{"duckduckgo"}},
					{URL: "https://d.example/doc", Title: "D docs", Engines: []string{"duckduckgo"}},
				}
			default:
				return []search.Hit{
					{URL: "https://a.example/doc", Title: "A original", Engines: []string{"google"}, Metadata: map[string]string{"original_rank": "1"}},
					{URL: "https://b.example/doc?utm_source=original", Title: "B original", Snippet: "original", Engines: []string{"google"}, Metadata: map[string]string{"original_rank": "2"}},
				}
			}
		},
	}
	fetcher := &countingFetcher{}
	p := &Pipeline{
		Searcher:  searcher,
		Fetcher:   fetcher,
		Extractor: stubExtractor{},
		Cache:     cache.New(nil, 0),
	}

	out, err := p.Search(context.Background(), Options{
		Query:        "golang api docs",
		SearchDepth:  "advanced",
		MaxResults:   2,
		CandidateCap: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(out) != 2 {
		t.Fatalf("got %d results, want candidate-capped 2", len(out))
	}
	if out[0].URL != "https://b.example/doc" {
		t.Fatalf("RRF should promote repeated canonical URL first, got %+v", out)
	}
	if got := fetcher.calls.Load(); got != 2 {
		t.Fatalf("fetched %d URLs, want only fused candidate cap", got)
	}
	if len(out[0].Engines) != 3 {
		t.Fatalf("engines were not merged across duplicate fused hits: %+v", out[0].Engines)
	}
	if out[0].Title != "B original" || out[0].Snippet != "original" {
		t.Fatalf("first useful metadata should come from first encountered duplicate: %+v", out[0])
	}
	if out[0].Metadata["original_rank"] != "2" || out[0].Metadata["exact_meta"] != "yes" {
		t.Fatalf("metadata was not preserved and merged: %+v", out[0].Metadata)
	}
	if out[0].Metadata["rrf_score"] == "" || out[0].Metadata["rrf_variants"] == "" || out[0].Metadata["rrf_original_rank"] != "2" {
		t.Fatalf("missing RRF debug metadata: %+v", out[0].Metadata)
	}
}
