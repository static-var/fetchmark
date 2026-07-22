package pipeline

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/core/discovery"
	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/search"
	"github.com/staticvar/fetchmark/internal/obs"
)

type recordingExpansionSearcher struct {
	mu        sync.Mutex
	queries   []search.Query
	responses func(search.Query) []search.Hit
}

func (s *recordingExpansionSearcher) Search(_ context.Context, q search.Query) ([]search.Hit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queries = append(s.queries, q)
	if s.responses == nil {
		return nil, nil
	}
	return s.responses(q), nil
}

func (s *recordingExpansionSearcher) recordedQueries() []search.Query {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]search.Query(nil), s.queries...)
}

type expansionSearchFunc func(context.Context, search.Query) ([]search.Hit, error)

func (f expansionSearchFunc) Search(ctx context.Context, q search.Query) ([]search.Hit, error) {
	return f(ctx, q)
}

type expansionBatchFunc func(context.Context, search.Query) (search.SearchBatch, error)

type expansionPlannerFunc func(search.Query) []discovery.Source

func (f expansionPlannerFunc) Sources(query search.Query) []discovery.Source { return f(query) }

type primaryExpansionPlanner struct {
	primary discovery.Source
	sources []discovery.Source
}

func (planner primaryExpansionPlanner) Sources(search.Query) []discovery.Source {
	return append([]discovery.Source(nil), planner.sources...)
}

func (planner primaryExpansionPlanner) PrimarySource() discovery.Source { return planner.primary }

func (f expansionBatchFunc) Search(ctx context.Context, q search.Query) ([]search.Hit, error) {
	batch, err := f(ctx, q)
	return batch.Hits, err
}

func (f expansionBatchFunc) SearchBatch(ctx context.Context, q search.Query) (search.SearchBatch, error) {
	return f(ctx, q)
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

	queries := searcher.recordedQueries()
	if len(queries) != 1 {
		t.Fatalf("search calls = %d, want exactly 1", len(queries))
	}
	got := queries[0]
	if got.Q != "golang api" {
		t.Fatalf("query = %q, want original", got.Q)
	}
	if got.MaxResults != 7 {
		t.Fatalf("max results = %d, want candidate cap 7", got.MaxResults)
	}
}

func TestSearchDepthReachesDiscoveryPlanner(t *testing.T) {
	searcher := &recordingExpansionSearcher{}
	var planned search.Query
	p := &Pipeline{
		Searcher: searcher,
		DiscoveryPlanner: expansionPlannerFunc(func(query search.Query) []discovery.Source {
			planned = query
			return []discovery.Source{{ID: "primary", ProviderID: "primary", Searcher: searcher, Variants: []string{"original"}}}
		}),
	}

	if _, err := p.Search(context.Background(), Options{Query: "tidal bores", SearchDepth: "advanced", MaxResults: 2}); err != nil {
		t.Fatal(err)
	}
	if planned.SearchDepth != "advanced" {
		t.Fatalf("planner search depth = %q, want advanced", planned.SearchDepth)
	}
}

func TestBasicSearchUsesOneOriginalLanePerPlannedProvider(t *testing.T) {
	var primaryCalls, extraCalls atomic.Int32
	primary := expansionSearchFunc(func(context.Context, search.Query) ([]search.Hit, error) {
		primaryCalls.Add(1)
		return nil, nil
	})
	extra := expansionSearchFunc(func(context.Context, search.Query) ([]search.Hit, error) {
		extraCalls.Add(1)
		return nil, nil
	})
	p := &Pipeline{
		Searcher: primary,
		DiscoveryPlanner: expansionPlannerFunc(func(search.Query) []discovery.Source {
			return []discovery.Source{
				{ID: "primary-default", ProviderID: "primary", Searcher: primary, Weight: 1, Variants: []string{"original", "exact"}},
				{ID: "primary-open", ProviderID: "primary", Searcher: primary, Weight: 0.9, Variants: []string{"original"}},
				{ID: "extra-original", ProviderID: "extra", Searcher: extra, Weight: 1, Variants: []string{"original", "exact"}},
			}
		}),
		AdvancedSearchConcurrency: 2,
	}
	if _, err := p.Search(context.Background(), Options{Query: "birds", MaxResults: 5}); err != nil {
		t.Fatal(err)
	}
	if primaryCalls.Load() != 1 || extraCalls.Load() != 1 {
		t.Fatalf("basic calls primary=%d extra=%d, want one original lane for each provider", primaryCalls.Load(), extraCalls.Load())
	}
}

func TestBasicSearchSkipsSecondaryProvidersWhenPrimaryHasRelevantResults(t *testing.T) {
	var secondaryCalls atomic.Int32
	primary := expansionSearchFunc(func(context.Context, search.Query) ([]search.Hit, error) {
		return []search.Hit{{
			URL: "https://go.dev/doc/", Title: "Go concurrency patterns", Snippet: "Goroutines and channels",
		}}, nil
	})
	secondary := expansionSearchFunc(func(context.Context, search.Query) ([]search.Hit, error) {
		secondaryCalls.Add(1)
		return []search.Hit{{URL: "https://secondary.example/result"}}, nil
	})
	primarySource := discovery.Source{ID: "scrapling-general", ProviderID: "scrapling", ProviderKind: "scrapling", Searcher: primary, Variants: []string{"original"}}
	p := &Pipeline{
		DiscoveryPlanner: primaryExpansionPlanner{
			primary: primarySource,
			sources: []discovery.Source{
				primarySource,
				{ID: "searxng-open", ProviderID: "searxng", ProviderKind: "searxng", Searcher: secondary, Variants: []string{"original"}},
			},
		},
		AdvancedSearchConcurrency: 2,
	}

	candidates, err := p.searchCandidateSet(context.Background(), Options{Query: "Go concurrency patterns"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if secondaryCalls.Load() != 0 || len(candidates.hits) != 1 || len(candidates.lanes) != 1 || candidates.lanes[0].Provider != "scrapling" {
		t.Fatalf("candidates=%+v secondary_calls=%d", candidates, secondaryCalls.Load())
	}
}

func TestBasicSearchUsesSecondaryProvidersWhenPrimaryUnderfillsCandidateWindow(t *testing.T) {
	var secondaryCalls atomic.Int32
	primary := expansionSearchFunc(func(context.Context, search.Query) ([]search.Hit, error) {
		return []search.Hit{{
			URL: "https://go.dev/doc/", Title: "Go concurrency patterns", Snippet: "Goroutines and channels",
		}}, nil
	})
	secondary := expansionSearchFunc(func(context.Context, search.Query) ([]search.Hit, error) {
		secondaryCalls.Add(1)
		return []search.Hit{{URL: "https://secondary.example/result", Title: "More Go concurrency patterns"}}, nil
	})
	primarySource := discovery.Source{ID: "scrapling-general", ProviderID: "scrapling", ProviderKind: "scrapling", Searcher: primary, Variants: []string{"original"}}
	p := &Pipeline{
		DiscoveryPlanner: primaryExpansionPlanner{
			primary: primarySource,
			sources: []discovery.Source{
				primarySource,
				{ID: "searxng-open", ProviderID: "searxng", ProviderKind: "searxng", Searcher: secondary, Variants: []string{"original"}},
			},
		},
		AdvancedSearchConcurrency: 2,
	}

	candidates, err := p.searchCandidateSet(context.Background(), Options{Query: "Go concurrency patterns"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if secondaryCalls.Load() != 1 || len(candidates.hits) != 2 || len(candidates.lanes) != 2 {
		t.Fatalf("candidates=%+v secondary_calls=%d", candidates, secondaryCalls.Load())
	}
}

func TestBasicSearchObservesUnderfilledPrimaryLaneOnce(t *testing.T) {
	const laneID = "scrapling-underfill-observation"
	primaryCounter := obs.DiscoveryLaneTotal.WithLabelValues("scrapling", laneID, "original", string(search.BatchHealthy))
	before := metricCounterValue(t, primaryCounter)
	primary := expansionSearchFunc(func(context.Context, search.Query) ([]search.Hit, error) {
		return []search.Hit{{URL: "https://go.dev/doc/", Title: "Go concurrency patterns"}}, nil
	})
	secondary := expansionSearchFunc(func(context.Context, search.Query) ([]search.Hit, error) {
		return []search.Hit{{URL: "https://secondary.example/result", Title: "More Go concurrency patterns"}}, nil
	})
	primarySource := discovery.Source{ID: laneID, ProviderID: "scrapling", ProviderKind: "scrapling", Searcher: primary, Variants: []string{"original"}}
	p := &Pipeline{DiscoveryPlanner: primaryExpansionPlanner{
		primary: primarySource,
		sources: []discovery.Source{
			primarySource,
			{ID: "searxng-observation", ProviderID: "searxng", ProviderKind: "searxng", Searcher: secondary, Variants: []string{"original"}},
		},
	}}

	if _, err := p.searchCandidateSet(context.Background(), Options{Query: "Go concurrency patterns", MaxResults: 2}, 2); err != nil {
		t.Fatal(err)
	}
	if delta := metricCounterValue(t, primaryCounter) - before; delta != 1 {
		t.Fatalf("primary lane observation delta = %v, want 1", delta)
	}
}

func TestBasicSearchDoesNotExposeDefaultQueryForCandidateHeadroom(t *testing.T) {
	var secondaryCalls atomic.Int32
	primary := expansionSearchFunc(func(context.Context, search.Query) ([]search.Hit, error) {
		hits := make([]search.Hit, 20)
		for i := range hits {
			hits[i] = search.Hit{URL: fmt.Sprintf("https://go.dev/doc/%d", i), Title: "Go concurrency patterns"}
		}
		return hits, nil
	})
	secondary := expansionSearchFunc(func(context.Context, search.Query) ([]search.Hit, error) {
		secondaryCalls.Add(1)
		return []search.Hit{{URL: "https://secondary.example/result"}}, nil
	})
	primarySource := discovery.Source{
		ID: "scrapling-general", ProviderID: "scrapling", ProviderKind: "scrapling", Searcher: primary,
		Variants: []string{"original"}, MaxResults: 20,
	}
	p := &Pipeline{DiscoveryPlanner: primaryExpansionPlanner{
		primary: primarySource,
		sources: []discovery.Source{
			primarySource,
			{ID: "searxng-open", ProviderID: "searxng", ProviderKind: "searxng", Searcher: secondary, Variants: []string{"original"}},
		},
	}}

	candidates, err := p.searchCandidateSet(context.Background(), Options{Query: "Go concurrency patterns", MaxResults: 10}, 30)
	if err != nil {
		t.Fatal(err)
	}
	if secondaryCalls.Load() != 0 || len(candidates.hits) != 20 {
		t.Fatalf("candidates=%d secondary_calls=%d", len(candidates.hits), secondaryCalls.Load())
	}
}

func TestBasicSearchUsesSecondaryProvidersWhenPrimaryResultsAreUnrelated(t *testing.T) {
	var secondaryCalls atomic.Int32
	primary := expansionSearchFunc(func(context.Context, search.Query) ([]search.Hit, error) {
		return []search.Hit{{URL: "https://example.com/cooking", Title: "Cooking pasta"}}, nil
	})
	secondary := expansionSearchFunc(func(context.Context, search.Query) ([]search.Hit, error) {
		secondaryCalls.Add(1)
		return []search.Hit{{URL: "https://go.dev/doc/", Title: "Go concurrency patterns"}}, nil
	})
	primarySource := discovery.Source{ID: "scrapling-general", ProviderID: "scrapling", ProviderKind: "scrapling", Searcher: primary, Variants: []string{"original"}}
	p := &Pipeline{
		DiscoveryPlanner: primaryExpansionPlanner{
			primary: primarySource,
			sources: []discovery.Source{
				primarySource,
				{ID: "searxng-open", ProviderID: "searxng", ProviderKind: "searxng", Searcher: secondary, Variants: []string{"original"}},
			},
		},
		AdvancedSearchConcurrency: 2,
	}

	candidates, err := p.searchCandidateSet(context.Background(), Options{Query: "Go concurrency patterns"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if secondaryCalls.Load() != 1 || len(candidates.hits) != 2 || len(candidates.lanes) != 2 {
		t.Fatalf("candidates=%+v secondary_calls=%d", candidates, secondaryCalls.Load())
	}
}

func metricCounterValue(t *testing.T, metric interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()
	value := new(dto.Metric)
	if err := metric.Write(value); err != nil {
		t.Fatal(err)
	}
	return value.GetCounter().GetValue()
}

func TestBasicSearchFallsBackToPrimaryWhenPlannedLanesAreAdvancedOnly(t *testing.T) {
	for _, test := range []struct {
		pack  string
		query string
	}{
		{pack: "developer", query: "golang api"},
		{pack: "fresh", query: "latest security news 2026"},
	} {
		t.Run(test.pack, func(t *testing.T) {
			primary := &recordingExpansionSearcher{responses: func(search.Query) []search.Hit {
				return []search.Hit{{URL: "https://primary.example/result"}}
			}}
			spec, err := discovery.DefaultSpec()
			if err != nil {
				t.Fatal(err)
			}
			registry, err := discovery.NewRegistryFromSpec(
				spec,
				map[string]search.Searcher{"searxng": primary},
				"searxng",
				[]string{test.pack},
				[]string{"searxng"},
			)
			if err != nil {
				t.Fatal(err)
			}
			p := &Pipeline{
				Searcher:                  primary,
				DiscoveryPlanner:          registry,
				AdvancedSearchConcurrency: 2,
			}

			hits, err := p.searchCandidates(context.Background(), Options{Query: test.query}, 7)
			if err != nil {
				t.Fatalf("basic search: %v", err)
			}
			if len(hits) != 1 || hits[0].URL != "https://primary.example/result" {
				t.Fatalf("hits = %+v, want primary fallback result", hits)
			}
			wantProvenance := []model.DiscoveryProvenance{{Provider: "searxng", Lane: "searxng", Variant: "original"}}
			if !reflect.DeepEqual(hits[0].Provenance, wantProvenance) || hits[0].Metadata["rrf_sources"] != "searxng:original" {
				t.Fatalf("fallback provenance = %#v metadata=%#v, want %#v", hits[0].Provenance, hits[0].Metadata, wantProvenance)
			}
			queries := primary.recordedQueries()
			if len(queries) != 1 {
				t.Fatalf("primary calls = %d, want one original request", len(queries))
			}
			if got := queries[0]; got.Q != test.query || got.ExactMatch || got.MaxResults != 7 {
				t.Fatalf("primary query = %+v, want unchanged basic query", got)
			}
		})
	}
}

func TestBasicSearchRunsFederationLikeLaneOnlyWhenOriginalAllowed(t *testing.T) {
	for _, test := range []struct {
		name      string
		variants  []string
		wantCalls int32
	}{
		{name: "original enabled", variants: []string{"original"}, wantCalls: 1},
		{name: "advanced only", variants: []string{"docs"}, wantCalls: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			primary := expansionSearchFunc(func(context.Context, search.Query) ([]search.Hit, error) {
				return nil, nil
			})
			var peerCalls atomic.Int32
			peer := expansionSearchFunc(func(context.Context, search.Query) ([]search.Hit, error) {
				peerCalls.Add(1)
				return nil, nil
			})
			p := &Pipeline{
				Searcher: primary,
				DiscoverySources: []DiscoverySource{
					{ID: "primary", ProviderID: "primary", Searcher: primary, Weight: 1, Variants: []string{"original"}},
					{ID: "peer-b", ProviderID: "peer-b", Searcher: peer, Weight: 1, Variants: test.variants},
				},
				AdvancedSearchConcurrency: 2,
			}
			if _, err := p.searchCandidates(context.Background(), Options{Query: "private query"}, 5); err != nil {
				t.Fatal(err)
			}
			if got := peerCalls.Load(); got != test.wantCalls {
				t.Fatalf("peer calls = %d, want %d", got, test.wantCalls)
			}
		})
	}
}

func TestBasicSearchReturnsTypedUnsupportedEngineControlBeforeCallingNativeSource(t *testing.T) {
	var calls atomic.Int32
	native := expansionSearchFunc(func(context.Context, search.Query) ([]search.Hit, error) {
		calls.Add(1)
		return nil, nil
	})
	registry, err := discovery.NewRegistry(discovery.RegistryOptions{
		PrimarySource: "wikipedia",
		Sources:       []discovery.Source{{ID: "wikipedia", Searcher: native, Weight: 1}},
		Packs:         []discovery.Pack{{ID: "general-open", Always: true, Sources: []discovery.SourceRef{{ID: "wikipedia"}}}},
		EnabledPacks:  []string{"general-open"},
	})
	if err != nil {
		t.Fatal(err)
	}
	p := &Pipeline{Searcher: native, DiscoveryPlanner: registry, AdvancedSearchConcurrency: 2}
	_, err = p.searchCandidates(context.Background(), Options{Query: "birds", Engines: []string{"mwmbl"}}, 10)
	var unsupported *search.UnsupportedControlError
	if !errors.As(err, &unsupported) || unsupported.Control != "engines" {
		t.Fatalf("search error = %v, want typed engines unsupported-control error", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("native search calls = %d, want zero", calls.Load())
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

	queries := searcher.recordedQueries()
	if len(queries) < 4 {
		t.Fatalf("advanced search calls = %d, want original, exact, freshness, and docs variants: %+v", len(queries), queries)
	}

	var sawExact, sawFresh, sawDocs bool
	var sawOriginal bool
	seen := map[string]bool{}
	for _, q := range queries {
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
		if q.Q == "latest golang api 2026" && !q.ExactMatch {
			sawOriginal = true
		}
		if q.Q != "latest golang api 2026" && strings.Contains(strings.ToLower(q.Q), "news") {
			sawFresh = true
		}
		lower := strings.ToLower(q.Q)
		if q.Q != "latest golang api 2026" && (strings.Contains(lower, "documentation") || strings.Contains(lower, "official docs")) {
			sawDocs = true
		}
	}
	if !sawOriginal || !sawExact || !sawFresh || !sawDocs {
		t.Fatalf("missing expected variants: original=%t exact=%t fresh=%t docs=%t queries=%+v", sawOriginal, sawExact, sawFresh, sawDocs, queries)
	}
}

func TestAdvancedExpansionBuildsFreshnessVariantForExplicitTimeRange(t *testing.T) {
	variants := advancedQueryVariants(search.Query{Q: "bird flu", TimeRange: "day"})
	labels := make([]string, len(variants))
	for i, variant := range variants {
		labels[i] = variant.label
		if variant.query.TimeRange != "day" {
			t.Fatalf("variant lost time range: %+v", variant)
		}
	}
	if !slices.Contains(labels, "freshness") {
		t.Fatalf("variant labels = %v, want freshness", labels)
	}
}

func TestAdvancedFanoutRunsLanesConcurrentlyWithinLimit(t *testing.T) {
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	var calls, active, maximum atomic.Int32
	searcher := expansionSearchFunc(func(ctx context.Context, _ search.Query) ([]search.Hit, error) {
		calls.Add(1)
		current := active.Add(1)
		for {
			prior := maximum.Load()
			if current <= prior || maximum.CompareAndSwap(prior, current) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-release:
			active.Add(-1)
			return nil, nil
		case <-ctx.Done():
			active.Add(-1)
			return nil, ctx.Err()
		}
	})
	p := &Pipeline{Searcher: searcher, AdvancedSearchConcurrency: 2}
	done := make(chan error, 1)
	go func() {
		_, err := p.Search(context.Background(), Options{Query: "latest golang api 2026", SearchDepth: "advanced", MaxResults: 5})
		done <- err
	}()
	<-started
	<-started
	select {
	case <-started:
		t.Fatal("third lane started while the concurrency limit was full")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 4 {
		t.Fatalf("calls = %d, want four planned lanes", got)
	}
	if got := maximum.Load(); got != 2 {
		t.Fatalf("maximum concurrency = %d, want 2", got)
	}
}

func TestAdvancedFanoutPreservesSuccessfulLaneWhenAnotherFails(t *testing.T) {
	wantErr := errors.New("exact unavailable")
	searcher := expansionSearchFunc(func(_ context.Context, q search.Query) ([]search.Hit, error) {
		if q.ExactMatch {
			return nil, wantErr
		}
		if q.Q == "golang api" {
			return []search.Hit{{URL: "https://go.dev/doc"}}, nil
		}
		return nil, nil
	})
	p := &Pipeline{Searcher: searcher, AdvancedSearchConcurrency: 3}
	hits, err := p.searchCandidates(context.Background(), Options{Query: "golang api", SearchDepth: "advanced"}, 10)
	if err != nil {
		t.Fatalf("partial advanced search returned error: %v", err)
	}
	if len(hits) != 1 || hits[0].URL != "https://go.dev/doc" {
		t.Fatalf("hits = %+v, want successful lane retained", hits)
	}
}

func TestAdvancedFanoutAcceptsPartialBatchWithHits(t *testing.T) {
	searcher := expansionBatchFunc(func(_ context.Context, q search.Query) (search.SearchBatch, error) {
		if q.Q == "golang api" && !q.ExactMatch {
			return search.SearchBatch{
				Hits:        []search.Hit{{URL: "https://go.dev/ref/spec"}},
				Provider:    "searxng",
				Status:      search.BatchPartial,
				Diagnostics: []search.ProviderDiagnostic{{Source: "blocked"}},
			}, nil
		}
		return search.SearchBatch{Provider: "searxng", Status: search.BatchAuthoritativeEmpty}, nil
	})
	p := &Pipeline{Searcher: searcher, AdvancedSearchConcurrency: 3}
	hits, err := p.searchCandidates(context.Background(), Options{Query: "golang api", SearchDepth: "advanced"}, 10)
	if err != nil || len(hits) != 1 {
		t.Fatalf("partial batch hits = %+v err=%v", hits, err)
	}
}

func TestAdvancedFanoutEmptyAndFailureReturnsEmpty(t *testing.T) {
	wantErr := errors.New("exact unavailable")
	searcher := expansionSearchFunc(func(_ context.Context, q search.Query) ([]search.Hit, error) {
		if q.ExactMatch {
			return nil, wantErr
		}
		return nil, nil
	})
	p := &Pipeline{Searcher: searcher, AdvancedSearchConcurrency: 3}
	hits, err := p.searchCandidates(context.Background(), Options{Query: "golang api", SearchDepth: "advanced"}, 10)
	if err != nil || len(hits) != 0 {
		t.Fatalf("empty plus failure = %+v err=%v, want authoritative empty", hits, err)
	}
}

func TestAdvancedFanoutDegradedAndFailureRetainsAggregateStatus(t *testing.T) {
	wantErr := errors.New("exact unavailable")
	searcher := expansionBatchFunc(func(_ context.Context, q search.Query) (search.SearchBatch, error) {
		if q.ExactMatch {
			return search.SearchBatch{Provider: "searxng", Status: search.BatchFailed}, wantErr
		}
		return search.SearchBatch{
			Provider:    "searxng",
			Status:      search.BatchDegradedEmpty,
			Diagnostics: []search.ProviderDiagnostic{{Provider: "searxng", Source: "mwmbl", Reason: "timeout"}},
		}, nil
	})
	p := &Pipeline{Searcher: searcher, AdvancedSearchConcurrency: 3}
	candidates, err := p.searchCandidateSet(context.Background(), Options{Query: "golang api", SearchDepth: "advanced"}, 10)
	if err != nil {
		t.Fatalf("degraded aggregate public behavior should remain empty success: %v", err)
	}
	if candidates.status != search.BatchDegradedEmpty {
		t.Fatalf("aggregate status = %q, want degraded_empty", candidates.status)
	}
	if len(candidates.hits) != 0 {
		t.Fatalf("degraded aggregate hits = %+v", candidates.hits)
	}
}

func TestSearchDetailedPreservesOrderedBoundedLaneEvidence(t *testing.T) {
	diagnostics := []search.ProviderDiagnostic{
		{Provider: "untrusted-provider-value", Instance: "http://private.internal:8080", Source: "Wiby API", Reason: "Timeout!", Retryable: true, RetryAfter: 3 * time.Second},
		{Source: "official_api", Reason: "http_429", Retryable: true, RetryAfter: 48 * time.Hour},
	}
	for len(diagnostics) < maxDiscoveryDiagnosticsPerLane+1 {
		diagnostics = append(diagnostics, search.ProviderDiagnostic{Source: "official_api", Reason: "degraded"})
	}
	degraded := expansionBatchFunc(func(context.Context, search.Query) (search.SearchBatch, error) {
		return search.SearchBatch{
			Provider: "untrusted-provider-value", Instance: "http://private.internal:8080",
			Status:      search.BatchDegradedEmpty,
			Diagnostics: diagnostics,
		}, nil
	})
	failed := expansionBatchFunc(func(context.Context, search.Query) (search.SearchBatch, error) {
		return search.SearchBatch{Status: search.BatchFailed}, context.DeadlineExceeded
	})
	p := &Pipeline{
		DiscoveryPlanner: expansionPlannerFunc(func(search.Query) []discovery.Source {
			return []discovery.Source{
				{ID: "wiby-general", ProviderID: "wiby", ProviderKind: "wiby", Searcher: degraded, Weight: 1, Variants: []string{"original"}},
				{ID: "mwmbl-general", ProviderID: "mwmbl", ProviderKind: "mwmbl", Searcher: failed, Weight: 1, Variants: []string{"original"}},
			}
		}),
		AdvancedSearchConcurrency: 2,
	}

	output, err := p.SearchDetailed(context.Background(), Options{Query: "small web", MaxResults: 5})
	if err != nil {
		t.Fatalf("SearchDetailed: %v", err)
	}
	if output.Discovery.Status != search.BatchDegradedEmpty || len(output.Discovery.Lanes) != 2 {
		t.Fatalf("discovery report = %+v", output.Discovery)
	}
	first, second := output.Discovery.Lanes[0], output.Discovery.Lanes[1]
	if first.Provider != "wiby" || first.Lane != "wiby-general" || first.Variant != "original" || first.Status != search.BatchDegradedEmpty || first.CandidateCount != 0 {
		t.Fatalf("first lane = %+v", first)
	}
	if len(first.Diagnostics) != maxDiscoveryDiagnosticsPerLane || !first.DiagnosticsTruncated || first.Diagnostics[0].Source != "other" || first.Diagnostics[0].Reason != "other" || !first.Diagnostics[0].Retryable || first.Diagnostics[0].RetryAfterMS != 3000 {
		t.Fatalf("sanitized diagnostics = %+v", first.Diagnostics)
	}
	if first.Diagnostics[1].RetryAfterMS != maxDiscoveryRetryAfter.Milliseconds() {
		t.Fatalf("retry delay was not clamped: %+v", first.Diagnostics[1])
	}
	if second.Provider != "mwmbl" || second.Status != search.BatchFailed || len(second.Diagnostics) != 1 || second.Diagnostics[0].Reason != "timeout" {
		t.Fatalf("failed lane = %+v", second)
	}
}

func TestSearchDetailedRetainsFailedLaneEvidenceWithError(t *testing.T) {
	failed := expansionBatchFunc(func(context.Context, search.Query) (search.SearchBatch, error) {
		return search.SearchBatch{
			Status:      search.BatchFailed,
			Diagnostics: []search.ProviderDiagnostic{{Source: "official_api", Reason: "http_503", Retryable: true}},
		}, errors.New("provider unavailable")
	})
	p := &Pipeline{DiscoveryPlanner: expansionPlannerFunc(func(search.Query) []discovery.Source {
		return []discovery.Source{{
			ID: "wiby-general", ProviderID: "wiby", ProviderKind: "wiby",
			Searcher: failed, Weight: 1, Variants: []string{"original"},
		}}
	})}

	output, err := p.SearchDetailed(context.Background(), Options{Query: "small web", MaxResults: 5})
	if err == nil || output.Discovery.Status != search.BatchFailed || len(output.Discovery.Lanes) != 1 {
		t.Fatalf("output=%+v err=%v", output, err)
	}
	lane := output.Discovery.Lanes[0]
	if lane.Provider != "wiby" || lane.Status != search.BatchFailed || len(lane.Diagnostics) != 1 || lane.Diagnostics[0].Reason != "http_503" {
		t.Fatalf("failed lane = %+v", lane)
	}
}

func TestSearchDetailedRetainsOrderedLaneEvidenceWhenParentIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	completed := expansionBatchFunc(func(context.Context, search.Query) (search.SearchBatch, error) {
		cancel()
		return search.SearchBatch{Status: search.BatchHealthy, Hits: []search.Hit{{URL: "https://example.com"}}}, nil
	})
	queued := expansionBatchFunc(func(context.Context, search.Query) (search.SearchBatch, error) {
		t.Fatal("queued lane should not execute after cancellation")
		return search.SearchBatch{}, nil
	})
	p := &Pipeline{
		DiscoveryPlanner: expansionPlannerFunc(func(search.Query) []discovery.Source {
			return []discovery.Source{
				{ID: "wiby-general", ProviderID: "wiby", ProviderKind: "wiby", Searcher: completed, Weight: 1, Variants: []string{"original"}},
				{ID: "mwmbl-general", ProviderID: "mwmbl", ProviderKind: "mwmbl", Searcher: queued, Weight: 1, Variants: []string{"original"}},
			}
		}),
		AdvancedSearchConcurrency: 1,
	}

	output, err := p.SearchDetailed(ctx, Options{Query: "small web", MaxResults: 5})
	if !errors.Is(err, context.Canceled) || output.Discovery.Status != search.BatchPartial || len(output.Discovery.Lanes) != 2 {
		t.Fatalf("output=%+v err=%v", output, err)
	}
	if output.Discovery.Lanes[0].Status != search.BatchHealthy || output.Discovery.Lanes[1].Status != search.BatchFailed || output.Discovery.Lanes[1].Diagnostics[0].Reason != "canceled" {
		t.Fatalf("lanes=%+v", output.Discovery.Lanes)
	}
}

func TestDiscoveryLaneReportClampsDuration(t *testing.T) {
	report := discoveryLaneReport(laneOutcome{
		lane:  discoveryLane{provider: "wiby", lane: "wiby-general", variant: "original"},
		batch: search.SearchBatch{Status: search.BatchAuthoritativeEmpty}, duration: 25 * time.Hour,
	})
	if report.DurationMS != search.MaxDiscoveryDuration.Milliseconds() {
		t.Fatalf("duration_ms=%d", report.DurationMS)
	}
}

func TestAdvancedFanoutSuccessfulHitsAndFailureAreAggregatePartial(t *testing.T) {
	searcher := expansionSearchFunc(func(_ context.Context, q search.Query) ([]search.Hit, error) {
		if q.ExactMatch {
			return nil, errors.New("exact unavailable")
		}
		if q.Q == "golang api" {
			return []search.Hit{{URL: "https://go.dev/doc"}}, nil
		}
		return nil, nil
	})
	p := &Pipeline{Searcher: searcher, AdvancedSearchConcurrency: 3}
	candidates, err := p.searchCandidateSet(context.Background(), Options{Query: "golang api", SearchDepth: "advanced"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if candidates.status != search.BatchPartial {
		t.Fatalf("aggregate status = %q, want partial", candidates.status)
	}
}

func TestAdvancedFanoutAllFailuresReturnsFirstPlannerError(t *testing.T) {
	originalErr := errors.New("original failed")
	searcher := expansionSearchFunc(func(_ context.Context, q search.Query) ([]search.Hit, error) {
		if q.Q == "golang api" && !q.ExactMatch {
			return nil, originalErr
		}
		return nil, fmt.Errorf("later failed: %s", q.Q)
	})
	p := &Pipeline{Searcher: searcher, AdvancedSearchConcurrency: 3}
	_, err := p.searchCandidates(context.Background(), Options{Query: "golang api", SearchDepth: "advanced"}, 10)
	if !errors.Is(err, originalErr) {
		t.Fatalf("error = %v, want first planner error %v", err, originalErr)
	}
}

func TestAdvancedFanoutParentCancellationWinsOverPartialResults(t *testing.T) {
	started := make(chan struct{}, 3)
	searcher := expansionSearchFunc(func(ctx context.Context, q search.Query) ([]search.Hit, error) {
		started <- struct{}{}
		if q.Q == "golang api" && !q.ExactMatch {
			return []search.Hit{{URL: "https://go.dev/doc"}}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	p := &Pipeline{Searcher: searcher, AdvancedSearchConcurrency: 3}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := p.searchCandidates(ctx, Options{Query: "golang api", SearchDepth: "advanced"}, 10)
		done <- err
	}()
	for range 3 {
		<-started
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestWeightedRRFUsesLaneWeights(t *testing.T) {
	hits := fuseHitsRRF([]variantHits{
		{label: "original", provider: "primary", lane: "primary", weight: 1, hits: []search.Hit{{URL: "https://a.example"}}},
		{label: "docs", provider: "docs", lane: "docs", weight: 2, hits: []search.Hit{{URL: "https://b.example"}}},
	}, 2)
	if len(hits) != 2 || hits[0].URL != "https://b.example" {
		t.Fatalf("weighted hits = %+v, want higher-weight source first", hits)
	}
}

func TestWeightedRRFRecordsSourceAndVariantProvenance(t *testing.T) {
	hits := fuseHitsRRF([]variantHits{
		{label: "original", provider: "searxng", lane: "searxng-open", weight: 1, hits: []search.Hit{{URL: "https://example.com/doc?utm_source=sx"}}},
		{label: "docs", provider: "openpack", lane: "open-docs", weight: 1, hits: []search.Hit{{URL: "https://example.com/doc"}}},
	}, 2)
	if len(hits) != 1 {
		t.Fatalf("hits = %+v, want canonical cross-source dedupe", hits)
	}
	want := []model.DiscoveryProvenance{
		{Provider: "openpack", Lane: "open-docs", Variant: "docs"},
		{Provider: "searxng", Lane: "searxng-open", Variant: "original"},
	}
	if !reflect.DeepEqual(hits[0].Provenance, want) {
		t.Fatalf("provenance = %#v, want %#v", hits[0].Provenance, want)
	}
	if got := hits[0].Metadata["rrf_sources"]; got != "searxng-open:original,open-docs:docs" {
		t.Fatalf("rrf_sources = %q", got)
	}
}

func TestBasicLanesKeepProviderAndLaneIdentitySeparate(t *testing.T) {
	searcher := expansionSearchFunc(func(context.Context, search.Query) ([]search.Hit, error) { return nil, nil })
	p := &Pipeline{DiscoverySources: []DiscoverySource{
		{ID: "searxng-default", ProviderID: "searxng", Searcher: searcher, Weight: 1, Variants: []string{"original"}},
		{ID: "searxng-open", ProviderID: "searxng", Searcher: searcher, Weight: 1, Variants: []string{"original"}},
	}}
	lanes, err := p.basicLanes(search.Query{Q: "birds"})
	if err != nil {
		t.Fatal(err)
	}
	if len(lanes) != 1 || lanes[0].provider != "searxng" || lanes[0].lane != "searxng-default" {
		t.Fatalf("basic lanes = %+v", lanes)
	}
}

func TestAdvancedLanesKeepOneProviderWithDistinctLaneIDs(t *testing.T) {
	searcher := expansionSearchFunc(func(context.Context, search.Query) ([]search.Hit, error) { return nil, nil })
	p := &Pipeline{DiscoverySources: []DiscoverySource{
		{ID: "searxng-default", ProviderID: "searxng", ProviderKind: "searxng", Searcher: searcher, Weight: 1, Variants: []string{"original"}},
		{ID: "searxng-open", ProviderID: "searxng", ProviderKind: "searxng", Searcher: searcher, Weight: 1, Variants: []string{"original"}},
	}}
	base := search.Query{Q: "birds"}
	lanes, err := p.advancedLanes(base, []queryVariant{{label: "original", weight: 1, query: base}})
	if err != nil {
		t.Fatal(err)
	}
	if len(lanes) != 2 || lanes[0].provider != "searxng" || lanes[1].provider != "searxng" || lanes[0].lane != "searxng-default" || lanes[1].lane != "searxng-open" {
		t.Fatalf("advanced lanes = %+v", lanes)
	}
}

func TestFederationProvenanceRedactsPrivateBindingIdentity(t *testing.T) {
	searcher := expansionSearchFunc(func(context.Context, search.Query) ([]search.Hit, error) {
		return []search.Hit{{URL: "https://example.com/doc"}}, nil
	})
	p := &Pipeline{DiscoverySources: []DiscoverySource{{
		ID: "peer-one-original", ProviderID: "peer-one", Searcher: searcher,
		Weight: 1, Variants: []string{"original"}, ProviderKind: "federation",
	}}}
	hits, err := p.searchCandidates(context.Background(), Options{Query: "birds", SearchDepth: "basic"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	want := []model.DiscoveryProvenance{{Provider: "federation", Lane: "federation", Variant: "original"}}
	if len(hits) != 1 || !reflect.DeepEqual(hits[0].Provenance, want) {
		t.Fatalf("hits = %#v, want redacted provenance %#v", hits, want)
	}
}

func TestAdvancedFanoutUsesConfiguredDiscoverySources(t *testing.T) {
	primary := expansionSearchFunc(func(_ context.Context, q search.Query) ([]search.Hit, error) {
		if q.Q == "golang api" && !q.ExactMatch {
			return []search.Hit{{URL: "https://general.example/doc"}}, nil
		}
		return nil, nil
	})
	docs := expansionSearchFunc(func(_ context.Context, q search.Query) ([]search.Hit, error) {
		if q.Q == "golang api" && !q.ExactMatch {
			return []search.Hit{{URL: "https://docs.example/doc"}}, nil
		}
		return nil, nil
	})
	p := &Pipeline{
		Searcher: primary,
		DiscoverySources: []DiscoverySource{
			{ID: "searxng", Searcher: primary, Weight: 1},
			{ID: "open_docs", Searcher: docs, Weight: 1},
		},
		AdvancedSearchConcurrency: 4,
	}
	hits, err := p.searchCandidates(context.Background(), Options{Query: "golang api", SearchDepth: "advanced"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 {
		t.Fatalf("hits = %+v, want both configured discovery sources", hits)
	}
	if hits[0].Metadata["rrf_sources"] != "searxng:original" || hits[1].Metadata["rrf_sources"] != "open_docs:original" {
		t.Fatalf("source provenance = %q, %q", hits[0].Metadata["rrf_sources"], hits[1].Metadata["rrf_sources"])
	}
}

func TestAdvancedFanoutStoresOutcomesInPlannerOrder(t *testing.T) {
	firstStarted := make(chan struct{})
	secondFinished := make(chan struct{})
	releaseFirst := make(chan struct{})
	first := expansionSearchFunc(func(ctx context.Context, _ search.Query) ([]search.Hit, error) {
		close(firstStarted)
		select {
		case <-releaseFirst:
			return []search.Hit{{URL: "https://example.com/doc", Title: "planner first"}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	second := expansionSearchFunc(func(context.Context, search.Query) ([]search.Hit, error) {
		close(secondFinished)
		return []search.Hit{{URL: "https://example.com/doc", Title: "completed first"}}, nil
	})
	p := &Pipeline{
		Searcher: first,
		DiscoverySources: []DiscoverySource{
			{ID: "planner_first", Searcher: first, Weight: 1},
			{ID: "planner_second", Searcher: second, Weight: 1},
		},
		AdvancedSearchConcurrency: 2,
	}
	done := make(chan struct {
		hits []search.Hit
		err  error
	}, 1)
	go func() {
		hits, err := p.searchCandidates(context.Background(), Options{Query: "birds", SearchDepth: "advanced"}, 10)
		done <- struct {
			hits []search.Hit
			err  error
		}{hits: hits, err: err}
	}()
	<-firstStarted
	<-secondFinished
	close(releaseFirst)
	result := <-done
	if result.err != nil || len(result.hits) != 1 {
		t.Fatalf("hits = %+v err=%v", result.hits, result.err)
	}
	if result.hits[0].Title != "planner first" {
		t.Fatalf("title = %q, completion order affected metadata", result.hits[0].Title)
	}
	if got := result.hits[0].Metadata["rrf_sources"]; got != "planner_first:original,planner_second:original" {
		t.Fatalf("rrf_sources = %q, want planner order", got)
	}
}

func TestTopKContributionUsesFinalPostDedupeResults(t *testing.T) {
	summary := summarizeTopKContribution([]model.SearchResult{
		{URL: "https://a.example/doc", Provenance: []model.DiscoveryProvenance{
			{Provider: "searxng", Lane: "searxng-open", Variant: "original"},
			{Provider: "openpack", Lane: "open-docs", Variant: "docs"},
		}},
		{URL: "https://b.example/doc", Provenance: []model.DiscoveryProvenance{
			{Provider: "openpack", Lane: "open-docs", Variant: "docs"},
		}},
	})
	if got := summary.contributions[model.DiscoveryProvenance{Provider: "searxng", Lane: "searxng-open", Variant: "original"}]; got != 1 {
		t.Fatalf("searxng contribution = %d, want 1", got)
	}
	openDocs := model.DiscoveryProvenance{Provider: "openpack", Lane: "open-docs", Variant: "docs"}
	if got := summary.contributions[openDocs]; got != 2 {
		t.Fatalf("open docs contribution = %d, want 2", got)
	}
	if summary.uniqueDomains != 2 {
		t.Fatalf("unique domains = %d, want 2", summary.uniqueDomains)
	}
	if got := summary.domainsBySource[openDocs]; got != 2 {
		t.Fatalf("open docs unique domains = %d, want 2", got)
	}
}

func TestMetricLabelsCollapseInvalidSourcesAndVariants(t *testing.T) {
	if got := normalizeDiscoverySource("raw/provider url"); got != "other" {
		t.Fatalf("invalid source = %q, want other", got)
	}
	if got := normalizeDiscoveryVariant("user supplied"); got != "other" {
		t.Fatalf("invalid variant = %q, want other", got)
	}
	if got := normalizeDiscoverySource("open_docs"); got != "open_docs" {
		t.Fatalf("trusted source = %q", got)
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
