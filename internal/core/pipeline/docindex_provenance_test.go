package pipeline

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/adapters/docindex"
	"github.com/staticvar/fetchmark/internal/adapters/fetcher"
	"github.com/staticvar/fetchmark/internal/core/model"
)

func TestOfficialDocIndexFullPipelineUsesOnlyConfiguredBrokerLane(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("..", "..", "adapters", "docindex", "testdata", "official-docs.json"))
	if err != nil {
		t.Fatal(err)
	}
	index, err := docindex.Open(docindex.Options{Path: path, Now: time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })
	url := "https://kotlinlang.org/docs/cancellation-and-timeouts.html"
	p := &Pipeline{
		Searcher: index,
		DiscoverySources: []DiscoverySource{{
			ID: "official-developer-docs", ProviderID: "docindex", ProviderKind: "docindex",
			Searcher: index, Weight: 1, Variants: []string{"original"}, MaxResults: 20,
		}},
		Fetcher:   stubFetcher{resp: map[string]fetcher.Result{url: {Status: 200, Body: []byte("Kotlin coroutine cancellation is cooperative.")}}},
		Extractor: stubExtractor{},
		Cache:     cache.New(nil, 0),
	}
	moderate := 1
	results, err := p.Search(context.Background(), Options{
		Query: "Kotlin coroutine cancellation", Language: "en", SafeSearch: &moderate, MaxResults: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 || results[0].Metadata["document_source"] != "kotlin" {
		t.Fatalf("results = %#v", results)
	}
	if _, found := results[0].Metadata["provenance"]; found {
		t.Fatalf("pipeline exposed adapter metadata provenance = %#v", results[0].Metadata)
	}
	want := []model.DiscoveryProvenance{{Provider: "docindex", Lane: "official-developer-docs", Variant: "original"}}
	if !reflect.DeepEqual(results[0].Provenance, want) {
		t.Fatalf("provenance = %#v, want %#v", results[0].Provenance, want)
	}
}
