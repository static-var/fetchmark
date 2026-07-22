package discovery

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/staticvar/fetchmark/internal/core/search"
)

type stubSearcher struct{}

func (stubSearcher) Search(context.Context, search.Query) ([]search.Hit, error) { return nil, nil }

func TestClassifyIntents(t *testing.T) {
	tests := []struct {
		name string
		q    search.Query
		want []Intent
	}{
		{name: "general", q: search.Query{Q: "how do tidal bores form"}, want: []Intent{IntentGeneral}},
		{name: "advanced exploration", q: search.Query{Q: "how do tidal bores form", SearchDepth: "advanced"}, want: []Intent{IntentGeneral, IntentExplore}},
		{name: "developer", q: search.Query{Q: "Kotlin coroutine API documentation"}, want: []Intent{IntentGeneral, IntentDeveloper}},
		{name: "research", q: search.Query{Q: "peer reviewed CRISPR DOI systematic review"}, want: []Intent{IntentGeneral, IntentResearch}},
		{name: "knowledge", q: search.Query{Q: "who was Emmy Noether"}, want: []Intent{IntentGeneral, IntentKnowledge}},
		{name: "historical knowledge with year", q: search.Query{Q: "What was the 2020 census?"}, want: []Intent{IntentGeneral, IntentFresh, IntentKnowledge}},
		{name: "knowledge with ambiguous current term", q: search.Query{Q: "What is electric current?"}, want: []Intent{IntentGeneral, IntentFresh, IntentKnowledge}},
		{name: "fresh and research", q: search.Query{Q: "latest climate research papers 2026"}, want: []Intent{IntentGeneral, IntentFresh, IntentResearch}},
		{name: "explicit time range", q: search.Query{Q: "bird flu", TimeRange: "day"}, want: []Intent{IntentGeneral, IntentFresh}},
		{name: "new marker", q: search.Query{Q: "new bird flu guidance"}, want: []Intent{IntentGeneral, IntentFresh}},
		{name: "this week marker", q: search.Query{Q: "bird flu this week"}, want: []Intent{IntentGeneral, IntentFresh}},
		{name: "this month marker", q: search.Query{Q: "bird flu this month"}, want: []Intent{IntentGeneral, IntentFresh}},
		{name: "phrase prefix is not fresh", q: search.Query{Q: "this weekend hiking routes"}, want: []Intent{IntentGeneral}},
		{name: "category research", q: search.Query{Q: "graph neural networks", Categories: []string{"science"}}, want: []Intent{IntentGeneral, IntentResearch}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyIntents(tt.q); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ClassifyIntents() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClassifyIntentsCoversDeveloperTechnologyFamilies(t *testing.T) {
	queries := []string{
		"How does nested scrolling work between Jetpack Compose and Android Views?",
		"Why do self-referential Rust types require Pin?",
		"How can AbortController cancel a fetch request?",
		"How can Docker BuildKit cache mounts use a secret?",
		"How do Kubernetes startup and readiness probes interact?",
		"What is the difference between Git partial clone and sparse checkout?",
		"How do SQLite WAL checkpoints interact with readers?",
		"What are the locking caveats of PostgreSQL SKIP LOCKED?",
		"How are secrets passed to a reusable GitHub Actions workflow?",
		"When should OpenTelemetry baggage be used?",
		"How are gRPC retries restricted?",
		"How do I diagnose a Gradle configuration cache problem?",
		"How does Swift actor reentrancy affect state across await?",
		"How do WASI component-model resource handles cross boundaries?",
	}
	for _, query := range queries {
		if got := ClassifyIntents(search.Query{Q: query}); !containsIntent(got, IntentDeveloper) {
			t.Errorf("query was not classified as developer: %q -> %v", query, got)
		}
	}
}

func TestClassifyIntentsCoversFixedSuiteResearchQueries(t *testing.T) {
	queries := []string{
		"What do systematic reviews find about agroforestry effects on crop yield in tropical smallholder systems?",
		"What do meta-analyses estimate about urban green space effects on heat-island intensity?",
		"What do controlled studies show about spaced repetition for adult second-language vocabulary learning?",
		"Which open datasets document glacial lake outburst floods in the Himalayas?",
		"What have longitudinal studies found about night-shift work, circadian disruption, and metabolic biomarkers?",
		"Which methods are recommended for detecting publication bias in network meta-analysis?",
		"What peer-reviewed evidence links Indigenous fire stewardship with biodiversity outcomes?",
		"What does comparative research show about voter error rates under ranked-choice voting?",
		"What evidence supports long-range atmospheric transport of microplastics to remote regions?",
		"Which open historical ship-log datasets are used to reconstruct past climate?",
		"What have preregistered and cross-cultural replication studies found about ego depletion?",
		"How accurately does passive acoustic monitoring estimate biodiversity in tropical forests?",
		"Which methods quantify uncertainty in satellite estimates of methane emissions?",
		"What do systematic reviews conclude about AI tutoring systems and learning outcomes in primary and secondary education?",
		"What have isotope studies revealed about human mobility in Bronze Age Europe?",
		"What corpus methods are used to study code-switching in multilingual social media text?",
		"How do biodegradable polymers perform in controlled marine-degradation experiments?",
		"What do meta-analyses find about daylight exposure and cognitive performance in offices?",
		"Which open benchmark datasets evaluate OCR for low-resource writing systems?",
		"What evidence connects community-managed fisheries with ecological and livelihood outcomes in the Global South?",
	}
	for _, query := range queries {
		if got := ClassifyIntents(search.Query{Q: query}); !containsIntent(got, IntentResearch) {
			t.Errorf("query was not classified as research: %q -> %v", query, got)
		}
	}
}

func TestClassifyIntentsDoesNotTreatGenericEvidenceMethodsOrEstimatesAsResearch(t *testing.T) {
	queries := []string{
		"How do cooking methods affect the flavor of tea?",
		"How can I estimate the cost of renovating a kitchen?",
		"What evidence do I need for a passport application?",
	}
	for _, query := range queries {
		if got := ClassifyIntents(search.Query{Q: query}); containsIntent(got, IntentResearch) {
			t.Errorf("generic query was classified as research: %q -> %v", query, got)
		}
	}
}

func containsIntent(values []Intent, want Intent) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestRegistryRoutesBuiltinPacks(t *testing.T) {
	primary, wikipedia, crossref, arxiv := stubSearcher{}, stubSearcher{}, stubSearcher{}, stubSearcher{}
	registry, err := NewRegistry(RegistryOptions{
		PrimarySource: "searxng",
		Sources: []Source{
			{ID: "searxng", Searcher: primary, Weight: 1},
			{ID: "wikipedia", Searcher: wikipedia, Weight: 0.8},
			{ID: "crossref", Searcher: crossref, Weight: 0.9},
			{ID: "arxiv", Searcher: arxiv, Weight: 1},
		},
		Packs: BuiltinPacks(BuiltinOptions{
			PrimarySource:   "searxng",
			EnableWikipedia: true,
			EnableCrossref:  true,
			EnableArxiv:     true,
		}),
		EnabledPacks: []string{"general-open", "developer", "research", "knowledge", "fresh"},
	})
	if err != nil {
		t.Fatal(err)
	}

	assertSourceIDs(t, registry.Sources(search.Query{Q: "peer reviewed battery research"}), "searxng", "crossref", "arxiv")
	assertSourceIDs(t, registry.Sources(search.Query{Q: "who was Emmy Noether"}), "searxng", "wikipedia")
	assertSourceIDs(t, registry.Sources(search.Query{Q: "how do tidal bores form", SearchDepth: "advanced"}), "searxng", "wikipedia")
	assertSourceIDs(t, registry.Sources(search.Query{Q: "latest Kotlin API news"}), "searxng")
}

func TestBuiltinWikipediaExplorationLaneDoesNotDuplicateExactQuery(t *testing.T) {
	packs := BuiltinPacks(BuiltinOptions{PrimarySource: "searxng", EnableWikipedia: true})
	for _, pack := range packs {
		if pack.ID != "knowledge" {
			continue
		}
		for _, source := range pack.Sources {
			if source.ID == "wikipedia" {
				if want := []string{"original"}; !reflect.DeepEqual(source.Variants, want) {
					t.Fatalf("Wikipedia variants = %v, want %v", source.Variants, want)
				}
				return
			}
		}
	}
	t.Fatal("built-in knowledge pack has no Wikipedia lane")
}

func TestRegistryExplicitEnginesUseOnlyPrimarySource(t *testing.T) {
	registry, err := NewRegistry(RegistryOptions{
		PrimarySource: "searxng",
		EngineSource:  "searxng",
		Sources: []Source{
			{ID: "searxng", Searcher: stubSearcher{}, Weight: 1},
			{ID: "crossref", Searcher: stubSearcher{}, Weight: 1},
		},
		Packs: []Pack{{
			ID:      "research",
			Intents: []Intent{IntentGeneral, IntentResearch},
			Sources: []SourceRef{{ID: "searxng"}, {ID: "crossref"}},
		}},
		EnabledPacks: []string{"research"},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertSourceIDs(t, registry.Sources(search.Query{Q: "research", Engines: []string{"mwmbl"}}), "searxng")
}

func TestRegistryExplicitEnginesUseSearxNGWhenAnotherSourceIsPrimary(t *testing.T) {
	registry, err := NewRegistry(RegistryOptions{
		PrimarySource: "wikipedia",
		EngineSource:  "searxng",
		Sources: []Source{
			{ID: "wikipedia", Searcher: stubSearcher{}, Weight: 1},
			{ID: "searxng", Searcher: stubSearcher{}, Weight: 1},
		},
		Packs: []Pack{{
			ID:      "general-open",
			Intents: []Intent{IntentGeneral},
			Sources: []SourceRef{{ID: "wikipedia"}, {ID: "searxng"}},
		}},
		EnabledPacks: []string{"general-open"},
	})
	if err != nil {
		t.Fatal(err)
	}
	sources, err := registry.Plan(search.Query{Q: "research", Engines: []string{"mwmbl"}})
	if err != nil {
		t.Fatal(err)
	}
	assertSourceIDs(t, sources, "searxng")
}

func TestRegistryRejectsExplicitEnginesWithoutSearxNG(t *testing.T) {
	registry, err := NewRegistry(RegistryOptions{
		PrimarySource: "wikipedia",
		Sources:       []Source{{ID: "wikipedia", Searcher: stubSearcher{}, Weight: 1}},
		Packs:         []Pack{{ID: "general-open", Always: true, Sources: []SourceRef{{ID: "wikipedia"}}}},
		EnabledPacks:  []string{"general-open"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = registry.Plan(search.Query{Q: "research", Engines: []string{"mwmbl"}})
	var unsupported *search.UnsupportedControlError
	if !errors.As(err, &unsupported) || unsupported.Control != "engines" {
		t.Fatalf("Plan error = %v, want typed engines unsupported-control error", err)
	}
}

func TestRegistryPlansConfiguredPrimaryBeforeOpportunisticSearxNG(t *testing.T) {
	registry, err := NewRegistry(RegistryOptions{
		PrimarySource: "wikipedia",
		EngineSource:  "searxng",
		Sources: []Source{
			{ID: "searxng", Searcher: stubSearcher{}, Weight: 1},
			{ID: "wikipedia", Searcher: stubSearcher{}, Weight: 0.9},
		},
		Packs: []Pack{
			{ID: "general-open", Always: true, Sources: []SourceRef{{ID: "searxng", LaneID: "searxng-default", Variants: []string{"original"}}}},
			{ID: "knowledge", Intents: []Intent{IntentKnowledge}, Sources: []SourceRef{{ID: "wikipedia", LaneID: "wikipedia-knowledge", Variants: []string{"original"}}}},
		},
		EnabledPacks: []string{"general-open", "knowledge"},
	})
	if err != nil {
		t.Fatal(err)
	}
	general, err := registry.Plan(search.Query{Q: "tidal bores"})
	if err != nil {
		t.Fatal(err)
	}
	assertSourceIDs(t, general, "wikipedia", "searxng-default")
	knowledge, err := registry.Plan(search.Query{Q: "what is a tidal bore"})
	if err != nil {
		t.Fatal(err)
	}
	assertSourceIDs(t, knowledge, "wikipedia-knowledge", "searxng-default")
}

func TestRegistryDedupesBySourceAtHighestWeightInStableOrder(t *testing.T) {
	registry, err := NewRegistry(RegistryOptions{
		PrimarySource: "searxng",
		Sources: []Source{
			{ID: "searxng", Searcher: stubSearcher{}, Weight: 1},
			{ID: "wikipedia", Searcher: stubSearcher{}, Weight: 0.8},
		},
		Packs: []Pack{
			{ID: "general-open", Intents: []Intent{IntentGeneral}, Sources: []SourceRef{{ID: "searxng", Weight: 1}, {ID: "wikipedia", Weight: 0.5}}},
			{ID: "knowledge", Intents: []Intent{IntentKnowledge}, Sources: []SourceRef{{ID: "wikipedia", Weight: 1.25}, {ID: "searxng", Weight: 0.7}}},
		},
		EnabledPacks: []string{"general-open", "knowledge"},
	})
	if err != nil {
		t.Fatal(err)
	}
	sources := registry.Sources(search.Query{Q: "what is a tidal bore"})
	assertSourceIDs(t, sources, "searxng", "wikipedia")
	if sources[0].Weight != 1 || sources[1].Weight != 1 {
		t.Fatalf("weights = %v, want source base multiplied by highest pack weights", []float64{sources[0].Weight, sources[1].Weight})
	}
}

func TestRegistryDisabledPacksFallBackToPrimary(t *testing.T) {
	registry, err := NewRegistry(RegistryOptions{
		PrimarySource: "searxng",
		Sources:       []Source{{ID: "searxng", Searcher: stubSearcher{}, Weight: 1}},
		Packs:         []Pack{{ID: "general-open", Intents: []Intent{IntentGeneral}, Sources: []SourceRef{{ID: "searxng"}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertSourceIDs(t, registry.Sources(search.Query{Q: "anything"}), "searxng")
}

func TestRegistryRejectsInvalidDefinitions(t *testing.T) {
	tests := []struct {
		name string
		opts RegistryOptions
	}{
		{name: "missing primary", opts: RegistryOptions{PrimarySource: "missing"}},
		{name: "duplicate source", opts: RegistryOptions{PrimarySource: "searxng", Sources: []Source{{ID: "searxng", Searcher: stubSearcher{}}, {ID: "searxng", Searcher: stubSearcher{}}}}},
		{name: "unknown pack source", opts: RegistryOptions{PrimarySource: "searxng", Sources: []Source{{ID: "searxng", Searcher: stubSearcher{}}}, Packs: []Pack{{ID: "bad", Intents: []Intent{IntentGeneral}, Sources: []SourceRef{{ID: "missing"}}}}}},
		{name: "unknown enabled pack", opts: RegistryOptions{PrimarySource: "searxng", Sources: []Source{{ID: "searxng", Searcher: stubSearcher{}}}, EnabledPacks: []string{"missing"}}},
		{name: "invalid source id", opts: RegistryOptions{PrimarySource: "raw url", Sources: []Source{{ID: "raw url", Searcher: stubSearcher{}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewRegistry(tt.opts); err == nil {
				t.Fatal("NewRegistry succeeded, want error")
			}
		})
	}
}

func assertSourceIDs(t *testing.T, sources []Source, want ...string) {
	t.Helper()
	got := make([]string, len(sources))
	for i := range sources {
		got[i] = sources[i].ID
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("source IDs = %v, want %v", got, want)
	}
}
