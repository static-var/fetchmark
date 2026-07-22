package discovery

import (
	"context"
	"testing"

	"github.com/staticvar/fetchmark/internal/core/search"
)

func TestRelevanceProfileSuppressesBroadKnowledgeForSpecialtyQueries(t *testing.T) {
	primary, wikipedia, crossref := stubSearcher{}, stubSearcher{}, stubSearcher{}
	registry, err := NewRegistry(RegistryOptions{
		PrimarySource: "searxng",
		Sources: []Source{
			{ID: "searxng", Searcher: primary, Weight: 1},
			{ID: "wikipedia", Searcher: wikipedia, Weight: 1},
			{ID: "crossref", Searcher: crossref, Weight: 1},
		},
		Packs: BuiltinPacks(BuiltinOptions{
			PrimarySource: "searxng", EnableWikipedia: true, EnableCrossref: true,
		}),
		EnabledPacks: []string{"general-open", "developer", "research", "knowledge", "fresh"},
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, query := range []string{
		"What is AbortController cancellation?",
		"systematic review of urban heat islands",
	} {
		for _, source := range registry.Sources(search.Query{Q: query, SearchDepth: "advanced"}) {
			if source.ProviderID == "wikipedia" {
				t.Errorf("advanced specialty query %q planned Wikipedia lane %q", query, source.ID)
			}
		}
	}
}

func TestRelevanceProfileAcceptsConceptVariant(t *testing.T) {
	_, err := NewRegistry(RegistryOptions{
		PrimarySource: "mwmbl",
		Sources:       []Source{{ID: "mwmbl", Searcher: profileTestSearcher{}}},
		Packs: []Pack{{
			ID: "general-open", Always: true,
			Sources: []SourceRef{{ID: "mwmbl", Variants: []string{"original", "concept"}}},
		}},
		EnabledPacks: []string{"general-open"},
	})
	if err != nil {
		t.Fatalf("concept variant rejected: %v", err)
	}
}

type profileTestSearcher struct{}

func (profileTestSearcher) Search(context.Context, search.Query) ([]search.Hit, error) {
	return nil, nil
}
