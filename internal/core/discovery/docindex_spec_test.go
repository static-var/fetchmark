package discovery

import (
	"context"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/core/search"
)

func TestDefaultSpecRoutesOptInOfficialDocIndexOnlyForDeveloperIntent(t *testing.T) {
	spec, err := DefaultSpec()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, source := range spec.Sources {
		if source.ID == "docindex" && source.Kind == "docindex" {
			found = true
		}
	}
	if !found {
		t.Fatal("default spec does not define docindex")
	}
	registry, err := NewRegistryFromSpec(spec, map[string]search.Searcher{
		"searxng":  namedDocIndexTestSearcher("searxng"),
		"docindex": namedDocIndexTestSearcher("docindex"),
	}, "searxng", []string{"general-open", "developer"}, []string{"searxng", "docindex"})
	if err != nil {
		t.Fatal(err)
	}
	developer := registry.Sources(search.Query{Q: "Kotlin coroutine API documentation"})
	if !containsLane(developer, "official-developer-docs") {
		t.Fatalf("developer sources = %+v", developer)
	}
	general := registry.Sources(search.Query{Q: "medieval manuscript pigments"})
	if containsLane(general, "official-developer-docs") {
		t.Fatalf("general sources = %+v", general)
	}
}

func TestLoadSpecRejectsAliasedOfficialDocIndexSource(t *testing.T) {
	raw := `{"version":1,"sources":[{"id":"docindex-copy","kind":"docindex","weight":1,"max_results":10,"timeout_ms":1000,"max_concurrency":1,"rate_per_second":1,"burst":1}],"packs":[{"id":"developer","intents":["developer"],"sources":[{"id":"copy-original","source":"docindex-copy","weight":1,"variants":["original"]}]}]}`
	if _, err := LoadSpec(strings.NewReader(raw)); err == nil || !strings.Contains(err.Error(), `docindex source must use id "docindex"`) {
		t.Fatalf("LoadSpec error = %v", err)
	}
}

type namedDocIndexTestSearcher string

func (namedDocIndexTestSearcher) Search(context.Context, search.Query) ([]search.Hit, error) {
	return nil, nil
}

func containsLane(sources []Source, id string) bool {
	for _, source := range sources {
		if source.ID == id {
			return true
		}
	}
	return false
}
