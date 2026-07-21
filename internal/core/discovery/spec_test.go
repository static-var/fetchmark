package discovery

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/core/search"
)

func TestDefaultSpecIsStrictAndComplete(t *testing.T) {
	spec, err := DefaultSpec()
	if err != nil {
		t.Fatal(err)
	}
	if spec.Version != 1 || len(spec.Sources) != 12 || len(spec.Packs) != 5 {
		t.Fatalf("default spec version=%d sources=%d packs=%d", spec.Version, len(spec.Sources), len(spec.Packs))
	}
	if spec.Sources[1].ID != "wikipedia" || spec.Sources[2].ID != "crossref" || spec.Sources[3].ID != "arxiv" || spec.Sources[3].Kind != "arxiv" || spec.Sources[4].ID != "mwmbl" || spec.Sources[5].ID != "wiby" || spec.Sources[6].ID != "stackexchange" || spec.Sources[7].ID != "github" || spec.Sources[8].ID != "pubmed" || spec.Sources[8].Kind != "pubmed" || spec.Sources[9].ID != "yacy" || spec.Sources[9].Kind != "yacy" || spec.Sources[10].ID != "scrapling" || spec.Sources[10].Kind != "scrapling" || spec.Sources[11].ID != "feedindex" || spec.Sources[11].Kind != "feedindex" {
		t.Fatalf("default native sources = %+v", spec.Sources)
	}
}

func TestDefaultSpecEnforcesScraplingBoundedBrowserPoolCeiling(t *testing.T) {
	spec, err := DefaultSpec()
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range spec.Sources {
		if source.ID != "scrapling" {
			continue
		}
		if source.RatePerSecond != 2 || source.Burst != 4 || source.MaxConcurrency != 4 || source.MaxResults > 20 || source.TimeoutMS < 90000 {
			t.Fatalf("Scrapling provider safeguards changed: %+v", source)
		}
		return
	}
	t.Fatal("missing Scrapling source")
}

func TestDefaultSpecEnforcesYaCyConservativeServiceCeiling(t *testing.T) {
	spec, err := DefaultSpec()
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range spec.Sources {
		if source.ID != "yacy" {
			continue
		}
		if source.RatePerSecond > 0.2 || source.Burst != 1 || source.MaxConcurrency != 1 || source.MaxResults > 100 || source.TimeoutMS < 10000 {
			t.Fatalf("YaCy provider safeguards changed: %+v", source)
		}
		return
	}
	t.Fatal("missing YaCy source")
}

func TestDefaultSpecEnforcesPubMedAnonymousServiceCeiling(t *testing.T) {
	spec, err := DefaultSpec()
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range spec.Sources {
		if source.ID != "pubmed" {
			continue
		}
		if source.RatePerSecond > 2 || source.Burst != 1 || source.MaxConcurrency != 1 || source.MaxResults > 20 || source.TimeoutMS < 10000 {
			t.Fatalf("PubMed provider safeguards changed: %+v", source)
		}
		return
	}
	t.Fatal("missing PubMed source")
}

func TestDefaultSpecEnforcesGitHubAnonymousSearchCeiling(t *testing.T) {
	spec, err := DefaultSpec()
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range spec.Sources {
		if source.ID != "github" {
			continue
		}
		if source.RatePerSecond > 0.1 || source.Burst != 1 || source.MaxConcurrency != 1 || source.TimeoutMS < 20000 || source.MaxResults > 20 {
			t.Fatalf("GitHub provider safeguards changed: %+v", source)
		}
		return
	}
	t.Fatal("missing GitHub source")
}

func TestDefaultSpecLeavesQueueHeadroomForLowRatePublicProviders(t *testing.T) {
	spec, err := DefaultSpec()
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"mwmbl", "wiby"} {
		var source *SourceSpec
		for index := range spec.Sources {
			if spec.Sources[index].ID == id {
				source = &spec.Sources[index]
				break
			}
		}
		if source == nil {
			t.Fatalf("missing %s source", id)
		}
		if source.RatePerSecond != 0.2 || source.Burst != 1 || source.MaxConcurrency != 1 {
			t.Fatalf("%s provider safeguards changed: %+v", id, *source)
		}
		if source.TimeoutMS < 30000 {
			t.Fatalf("%s timeout_ms = %d, want at least 30000 for bounded admission queueing", id, source.TimeoutMS)
		}
	}
}

func TestDefaultSpecEnforcesArxivPublicServiceCeiling(t *testing.T) {
	spec, err := DefaultSpec()
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range spec.Sources {
		if source.ID != "arxiv" {
			continue
		}
		if source.RatePerSecond > 1.0/3.0 || source.Burst != 1 || source.MaxConcurrency != 1 || source.TimeoutMS < 9000 {
			t.Fatalf("arXiv provider safeguards changed: %+v", source)
		}
		return
	}
	t.Fatal("missing arXiv source")
}

func TestLoadSpecRejectsUnknownFieldsAndTrailingJSON(t *testing.T) {
	base := `{"version":1,"sources":[{"id":"searxng","kind":"searxng","weight":1,"max_results":10,"timeout_ms":1000,"max_concurrency":1,"rate_per_second":1,"burst":1}],"packs":[{"id":"general-open","always":true,"sources":[{"id":"main","source":"searxng","weight":1,"variants":["original"]}]}]}`
	for _, raw := range []string{
		strings.Replace(base, `"version":1`, `"version":1,"unknown":true`, 1),
		base + `{}`,
	} {
		if _, err := LoadSpec(strings.NewReader(raw)); err == nil {
			t.Fatal("LoadSpec accepted non-strict JSON")
		}
	}
}

func TestLoadSpecRejectsOversizedInput(t *testing.T) {
	if _, err := LoadSpec(strings.NewReader(strings.Repeat(" ", MaxSpecBytes+1))); err == nil {
		t.Fatal("LoadSpec accepted oversized input")
	}
}

func TestLoadSpecRejectsInvalidDefinitions(t *testing.T) {
	validSource := `{"id":"searxng","kind":"searxng","weight":1,"max_results":10,"timeout_ms":1000,"max_concurrency":1,"rate_per_second":1,"burst":1}`
	tests := []string{
		`{"version":2,"sources":[` + validSource + `],"packs":[]}`,
		`{"version":1,"sources":[` + validSource + `,` + validSource + `],"packs":[]}`,
		`{"version":1,"sources":[{"id":"searxng","kind":"paid","weight":1,"max_results":10,"timeout_ms":1000,"max_concurrency":1,"rate_per_second":1,"burst":1}],"packs":[]}`,
		`{"version":1,"sources":[{"id":"searxng","kind":"searxng","weight":0,"max_results":10,"timeout_ms":1000,"max_concurrency":1,"rate_per_second":1,"burst":1}],"packs":[]}`,
		`{"version":1,"sources":[` + validSource + `],"packs":[{"id":"bad","intents":["unknown"],"sources":[{"source":"missing","variants":["bad"]}]}]}`,
	}
	for _, raw := range tests {
		if _, err := LoadSpec(strings.NewReader(raw)); err == nil {
			t.Fatalf("LoadSpec accepted invalid definition: %s", raw)
		}
	}
}

func TestLoadSpecRejectsAliasedMwmblSource(t *testing.T) {
	raw := `{"version":1,"sources":[{"id":"mwmbl-copy","kind":"mwmbl","weight":1,"max_results":10,"timeout_ms":1000,"max_concurrency":1,"rate_per_second":1,"burst":1}],"packs":[{"id":"general-open","always":true,"sources":[{"id":"mwmbl-copy-original","source":"mwmbl-copy","weight":1,"variants":["original"]}]}]}`
	if _, err := LoadSpec(strings.NewReader(raw)); err == nil || !strings.Contains(err.Error(), `mwmbl source must use id "mwmbl"`) {
		t.Fatalf("LoadSpec error = %v", err)
	}
}

func TestLoadSpecRejectsAliasedFixedPublicSources(t *testing.T) {
	for _, kind := range []string{"wikipedia", "crossref", "arxiv", "wiby", "stackexchange", "github", "pubmed", "yacy", "feedindex"} {
		raw := fmt.Sprintf(`{"version":1,"sources":[{"id":"%s-copy","kind":"%s","weight":1,"max_results":10,"timeout_ms":1000,"max_concurrency":1,"rate_per_second":1,"burst":1}],"packs":[{"id":"general-open","always":true,"sources":[{"id":"copy-original","source":"%s-copy","weight":1,"variants":["original"]}]}]}`, kind, kind, kind)
		if _, err := LoadSpec(strings.NewReader(raw)); err == nil || !strings.Contains(err.Error(), fmt.Sprintf(`%s source must use id %q`, kind, kind)) {
			t.Fatalf("LoadSpec(%s) error = %v", kind, err)
		}
	}
}

func TestDefaultSpecRoutesOptInFeedIndexOnlyForFreshIntent(t *testing.T) {
	spec, err := DefaultSpec()
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewRegistryFromSpec(spec, map[string]search.Searcher{
		"searxng":   namedSearcher("searxng"),
		"feedindex": namedSearcher("feedindex"),
	}, "searxng", []string{"general-open", "fresh"}, []string{"searxng", "feedindex"})
	if err != nil {
		t.Fatal(err)
	}
	assertSourceIDs(t, registry.Sources(search.Query{Q: "stable Python release", TimeRange: "day"}), "searxng-default", "searxng-open", "searxng-fresh", "official-fresh-feeds")
	assertSourceIDs(t, registry.Sources(search.Query{Q: "stable Python release"}), "searxng-default", "searxng-open")
}

func TestLoadSpecAcceptsExplicitOpenPackSource(t *testing.T) {
	raw := `{"version":1,"sources":[{"id":"searxng","kind":"searxng","weight":1,"max_results":10,"timeout_ms":1000,"max_concurrency":1,"rate_per_second":1,"burst":1},{"id":"openpack-developer","kind":"openpack","weight":1,"max_results":10,"timeout_ms":1000,"max_concurrency":1,"rate_per_second":1,"burst":1}],"packs":[{"id":"developer","intents":["developer"],"sources":[{"id":"openpack-developer-lane","source":"openpack-developer","weight":1,"variants":["original"]}]}]}`
	if _, err := LoadSpec(strings.NewReader(raw)); err != nil {
		t.Fatalf("LoadSpec rejected openpack source: %v", err)
	}
}

func TestLoadSpecAcceptsExplicitFederationSource(t *testing.T) {
	raw := `{"version":1,"sources":[{"id":"searxng","kind":"searxng","weight":1,"max_results":10,"timeout_ms":1000,"max_concurrency":1,"rate_per_second":1,"burst":1},{"id":"peer-one","kind":"federation","weight":0.8,"max_results":10,"timeout_ms":2000,"max_concurrency":1,"rate_per_second":1,"burst":1}],"packs":[{"id":"peer-developer","intents":["developer"],"sources":[{"id":"peer-one-original","source":"peer-one","weight":1,"variants":["original"]}]}]}`
	if _, err := LoadSpec(strings.NewReader(raw)); err != nil {
		t.Fatalf("LoadSpec rejected federation source: %v", err)
	}
}

func TestSourceSpecAppliesGlobalBudgets(t *testing.T) {
	spec := SourceSpec{ID: "wikipedia", Kind: "wikipedia", Weight: 1.1, MaxResults: 10, TimeoutMS: 5000}
	source := spec.Bind(stubSearcher{})
	if source.ID != "wikipedia" || source.ProviderKind != "wikipedia" || source.Weight != 1.1 || source.MaxResults != 10 || source.Timeout.String() != "5s" {
		t.Fatalf("bound source = %+v", source)
	}
}

type namedSearcher string

func (namedSearcher) Search(context.Context, search.Query) ([]search.Hit, error) { return nil, nil }

func TestNewRegistryFromSpecAppliesSourceAndPackAllowlists(t *testing.T) {
	spec, err := DefaultSpec()
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewRegistryFromSpec(spec, map[string]search.Searcher{
		"searxng":  namedSearcher("searxng"),
		"crossref": namedSearcher("crossref"),
	}, "searxng", []string{"general-open", "research", "fresh"}, []string{"searxng", "crossref"})
	if err != nil {
		t.Fatal(err)
	}
	sources := registry.Sources(search.Query{Q: "peer reviewed paper"})
	assertSourceIDs(t, sources, "searxng-default", "searxng-open", "crossref-research", "searxng-research")
	for _, source := range sources {
		if source.ProviderID == "wikipedia" {
			t.Fatal("disabled Wikipedia source was planned")
		}
	}
	assertSourceIDs(t, registry.Sources(search.Query{Q: "bird flu", TimeRange: "day"}), "searxng-default", "searxng-open", "searxng-fresh")
}

func TestDefaultSpecRoutesAdvancedExplorationToWikipedia(t *testing.T) {
	spec, err := DefaultSpec()
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewRegistryFromSpec(spec, map[string]search.Searcher{
		"searxng":   namedSearcher("searxng"),
		"wikipedia": namedSearcher("wikipedia"),
	}, "searxng", []string{"general-open", "knowledge"}, []string{"searxng", "wikipedia"})
	if err != nil {
		t.Fatal(err)
	}
	assertSourceIDs(t, registry.Sources(search.Query{Q: "how do tidal bores form"}), "searxng-default", "searxng-open")
	advanced := registry.Sources(search.Query{Q: "how do tidal bores form", SearchDepth: "advanced"})
	assertSourceIDs(t, advanced, "searxng-default", "searxng-open", "wikipedia-knowledge", "searxng-knowledge")
	for _, source := range advanced {
		if source.ID == "wikipedia-knowledge" {
			if want := []string{"original"}; !reflect.DeepEqual(source.Variants, want) {
				t.Fatalf("Wikipedia variants = %v, want %v", source.Variants, want)
			}
			return
		}
	}
	t.Fatal("advanced plan has no Wikipedia knowledge lane")
}

func TestNewRegistryFromSpecAllowsExplicitNonSearxPrimary(t *testing.T) {
	spec, err := DefaultSpec()
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewRegistryFromSpec(spec, map[string]search.Searcher{
		"wikipedia": namedSearcher("wikipedia"),
		"crossref":  namedSearcher("crossref"),
	}, "wikipedia", []string{"general-open", "research", "knowledge"}, []string{"wikipedia", "crossref"})
	if err != nil {
		t.Fatal(err)
	}
	sources, err := registry.Plan(search.Query{Q: "peer reviewed paper"})
	if err != nil {
		t.Fatal(err)
	}
	assertSourceIDs(t, sources, "wikipedia", "crossref-research")
	_, err = registry.Plan(search.Query{Q: "paper", Engines: []string{"mwmbl"}})
	var unsupported *search.UnsupportedControlError
	if !errors.As(err, &unsupported) {
		t.Fatalf("engine plan error = %v, want typed unsupported control", err)
	}
}

func TestNewRegistryFromSpecRejectsUnknownOrUnboundAllowlists(t *testing.T) {
	spec, err := DefaultSpec()
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		adapters map[string]search.Searcher
		packs    []string
		sources  []string
	}{
		{name: "unknown source", adapters: map[string]search.Searcher{"searxng": namedSearcher("searxng")}, packs: []string{"general-open"}, sources: []string{"searxng", "missing"}},
		{name: "unbound source", adapters: map[string]search.Searcher{"searxng": namedSearcher("searxng")}, packs: []string{"general-open"}, sources: []string{"searxng", "wikipedia"}},
		{name: "primary disabled", adapters: map[string]search.Searcher{"crossref": namedSearcher("crossref")}, packs: []string{"research"}, sources: []string{"crossref"}},
		{name: "unknown pack", adapters: map[string]search.Searcher{"searxng": namedSearcher("searxng")}, packs: []string{"missing"}, sources: []string{"searxng"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewRegistryFromSpec(spec, tt.adapters, "searxng", tt.packs, tt.sources); err == nil {
				t.Fatal("NewRegistryFromSpec succeeded")
			}
		})
	}
}
