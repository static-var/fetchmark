package pipeline

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/core/search"
)

func TestRelevanceVariantsPreferSpecialtyProjectionForBasicSearch(t *testing.T) {
	searcher := &recordingExpansionSearcher{}
	p := &Pipeline{
		Searcher: searcher,
		DiscoverySources: []DiscoverySource{
			{ID: "searxng-default", ProviderID: "searxng", Searcher: searcher, Variants: []string{"original"}},
			{ID: "searxng-docs", ProviderID: "searxng", Searcher: searcher, Variants: []string{"docs"}, Engines: []string{"stackoverflow"}},
		},
	}
	const query = "How do SQLite WAL checkpoints interact with readers?"
	if _, err := p.searchCandidates(context.Background(), Options{Query: query}, 10); err != nil {
		t.Fatal(err)
	}
	queries := searcher.recordedQueries()
	if len(queries) != 1 || !strings.Contains(strings.ToLower(queries[0].Q), "official docs") || !reflect.DeepEqual(queries[0].Engines, []string{"stackoverflow"}) {
		t.Fatalf("basic specialty query = %+v", queries)
	}
}

func TestRelevanceVariantsUseConceptOnlyForDeclaredAdvancedLane(t *testing.T) {
	mwmbl := &recordingExpansionSearcher{}
	wiby := &recordingExpansionSearcher{}
	p := &Pipeline{
		Searcher: mwmbl,
		DiscoverySources: []DiscoverySource{
			{ID: "mwmbl-general", ProviderID: "mwmbl", Searcher: mwmbl, Variants: []string{"original", "concept"}},
			{ID: "wiby-general", ProviderID: "wiby", Searcher: wiby, Variants: []string{"original"}},
		},
		AdvancedSearchConcurrency: 2,
	}
	const query = "How do urban trees reduce neighborhood temperatures?"
	if _, err := p.searchCandidates(context.Background(), Options{Query: query, SearchDepth: "advanced"}, 10); err != nil {
		t.Fatal(err)
	}
	assertRelevanceQueries(t, mwmbl.recordedQueries(), []string{query, "urban trees reduce neighborhood temperatures"})
	assertRelevanceQueries(t, wiby.recordedQueries(), []string{query})
}

func TestRelevanceVariantsKeepExactMatchUnchanged(t *testing.T) {
	searcher := &recordingExpansionSearcher{}
	p := &Pipeline{
		Searcher: searcher,
		DiscoverySources: []DiscoverySource{
			{ID: "searxng-default", ProviderID: "searxng", Searcher: searcher, Variants: []string{"original", "concept"}},
			{ID: "searxng-docs", ProviderID: "searxng", Searcher: searcher, Variants: []string{"docs"}},
		},
	}
	const query = "current SQLite WAL behavior"
	if _, err := p.searchCandidates(context.Background(), Options{Query: query, SearchDepth: "advanced", ExactMatch: true}, 10); err != nil {
		t.Fatal(err)
	}
	queries := searcher.recordedQueries()
	if len(queries) != 1 || queries[0].Q != query || !queries[0].ExactMatch {
		t.Fatalf("exact-match query changed: %+v", queries)
	}
}

func assertRelevanceQueries(t *testing.T, queries []search.Query, want []string) {
	t.Helper()
	got := make(map[string]int, len(queries))
	for _, query := range queries {
		got[query.Q]++
	}
	for _, query := range want {
		if got[query] != 1 {
			t.Fatalf("queries = %+v, want one %q", queries, query)
		}
		delete(got, query)
	}
	if len(got) != 0 {
		t.Fatalf("unexpected queries = %+v", got)
	}
}
