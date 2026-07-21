package api

import (
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/core/model"
)

func TestDiscoveryAttributionHeadersPreserveAllRequiredSources(t *testing.T) {
	header := make(http.Header)
	addDiscoveryAttributionHeaders(header, []model.SearchResult{
		{Provenance: []model.DiscoveryProvenance{
			{Provider: "stackexchange", Lane: "stackexchange-developer", Variant: "original"},
			{Provider: "wiby", Lane: "wiby-general", Variant: "original"},
			{Provider: "arxiv", Lane: "arxiv-research", Variant: "original"},
			{Provider: "pubmed", Lane: "pubmed-research", Variant: "original"},
		}},
		{Provenance: []model.DiscoveryProvenance{{Provider: "stackexchange", Lane: "stackexchange-developer", Variant: "original"}}},
	})
	want := []string{stackExchangeAttributionLink, wibyAttributionLink, arxivAttributionLink, pubMedAttributionLink}
	if got := header.Values("Link"); !reflect.DeepEqual(got, want) {
		t.Fatalf("Link headers = %v, want %v", got, want)
	}
	if strings.Contains(strings.Join(header.Values("Link"), ","), `rel="license"; title="CC0 1.0 metadata"`) {
		t.Fatalf("arXiv metadata license escaped into response context: %v", header.Values("Link"))
	}
}

func TestStackExchangeAttributionHeadersPreserveExactLicenseAndAuthor(t *testing.T) {
	header := make(http.Header)
	addDiscoveryAttributionHeaders(header, []model.SearchResult{{
		Metadata:   map[string]string{"license": "CC BY-SA 4.0", "owner_id": "42"},
		Provenance: []model.DiscoveryProvenance{{Provider: "stackexchange", Lane: "stackexchange-developer", Variant: "original"}},
	}})
	want := []string{stackExchangeAttributionLink, stackExchangeLicenseLinks["CC BY-SA 4.0"], `<https://stackoverflow.com/users/42>; rel="author"`}
	if got := header.Values("Link"); !reflect.DeepEqual(got, want) {
		t.Fatalf("Link headers = %v, want %v", got, want)
	}
}
