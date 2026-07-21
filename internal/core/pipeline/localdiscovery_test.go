package pipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/localartifact"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
	"github.com/staticvar/fetchmark/internal/core/search"
)

type countingSearch struct {
	hits  []search.Hit
	calls atomic.Int64
}

func (source *countingSearch) Search(_ context.Context, _ search.Query) ([]search.Hit, error) {
	source.calls.Add(1)
	return append([]search.Hit(nil), source.hits...), nil
}

func TestBasicDiscoveryFusesLocalIndexWithLiveSource(t *testing.T) {
	live := &countingSearch{hits: []search.Hit{{URL: "https://live.example/page", Title: "Live"}}}
	local := &countingSearch{hits: []search.Hit{{URL: "https://local.example/page", Title: "Local"}}}
	pipeline := &Pipeline{Searcher: live, LocalSearcher: local, AdvancedSearchConcurrency: 2}

	hits, err := pipeline.searchCandidates(context.Background(), Options{Query: "query"}, 10)
	if err != nil || len(hits) != 2 {
		t.Fatalf("hits=%+v err=%v", hits, err)
	}
	if live.calls.Load() != 1 || local.calls.Load() != 1 {
		t.Fatalf("live calls=%d local calls=%d", live.calls.Load(), local.calls.Load())
	}
}

func TestExplicitEngineBypassesLocalIndex(t *testing.T) {
	live := &countingSearch{hits: []search.Hit{{URL: "https://live.example/page"}}}
	local := &countingSearch{hits: []search.Hit{{URL: "https://local.example/page"}}}
	pipeline := &Pipeline{Searcher: live, LocalSearcher: local}

	hits, err := pipeline.searchCandidates(context.Background(), Options{Query: "query", Engines: []string{"mwmbl"}}, 10)
	if err != nil || len(hits) != 1 || hits[0].URL != "https://live.example/page" {
		t.Fatalf("hits=%+v err=%v", hits, err)
	}
	if local.calls.Load() != 0 {
		t.Fatalf("local calls=%d, want 0", local.calls.Load())
	}
}

func TestPersistentLocalDiscoveryRejectsProjectionThatDoesNotMatchArtifact(t *testing.T) {
	const rawURL = "https://local.example/stale-safe"
	observedAt := time.Now().UTC().Add(-time.Minute)
	index := openPipelineIndex(t)
	artifacts := openCuratedArtifacts(t)
	if err := artifacts.Put(context.Background(), localartifact.Version{
		URL: rawURL, EffectiveURL: rawURL, Body: []byte("same retained body"), MIME: "text/html",
		PolicyAgent: "Fetchmark-Test", FetchedAt: observedAt, ObservedAt: observedAt,
		ValidatedAt: observedAt, SafetyClassification: localcorpus.SafetyUnsafe,
		IndexingDisposition: localcorpus.DispositionPermitted,
	}); err != nil {
		t.Fatal(err)
	}
	artifact, ok, err := artifacts.Current(context.Background(), rawURL)
	if err != nil || !ok {
		t.Fatalf("artifact=%+v ok=%t err=%v", artifact, ok, err)
	}
	if err := index.Reconcile(context.Background(), localcorpus.Document{
		URL: rawURL, Body: "same retained body", FetchedAt: observedAt, ContentHash: artifact.ContentHash,
		SafetyClassification: localcorpus.SafetySafe, IndexingDisposition: localcorpus.DispositionPermitted,
	}); err != nil {
		t.Fatal(err)
	}
	strict := 2
	direct, err := index.SearchBatch(context.Background(), search.Query{Q: "retained body", SafeSearch: &strict, MaxResults: 10})
	if err != nil || len(direct.Hits) != 1 {
		t.Fatalf("direct stale projection=%+v err=%v", direct, err)
	}
	pipeline := &Pipeline{Searcher: &countingSearch{}, LocalSearcher: index, LocalArtifacts: artifacts, AdvancedSearchConcurrency: 2}
	hits, err := pipeline.searchCandidates(context.Background(), Options{Query: "retained body", SafeSearch: &strict}, 10)
	if err != nil || len(hits) != 0 {
		t.Fatalf("verified hits=%+v err=%v", hits, err)
	}
}
