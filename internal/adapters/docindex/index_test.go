package docindex

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/search"
)

func TestOpenSearchesImmutableOfficialMetadataWithProvenance(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("testdata", "official-docs.json"))
	if err != nil {
		t.Fatal(err)
	}
	index, err := Open(Options{Path: path, Now: time.Date(2026, 7, 20, 1, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })

	batch, err := index.SearchBatch(context.Background(), search.Query{
		Q: "How do Kotlin coroutines handle cooperative cancellation?", MaxResults: 5,
		Language: "en", SafeSearch: intPointer(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Provider != providerID || batch.Status != search.BatchPartial || len(batch.Hits) != 2 {
		t.Fatalf("batch = %#v", batch)
	}
	if got := batch.Hits[0].URL; got != "https://kotlinlang.org/docs/cancellation-and-timeouts.html" {
		t.Fatalf("first URL = %q", got)
	}
	hit := batch.Hits[0]
	if hit.Metadata["provider"] != providerID || hit.Metadata["document_source"] != "kotlin" ||
		hit.Metadata["source_owner"] != "JetBrains" || hit.Metadata["license"] != "Apache-2.0" ||
		hit.Metadata["operator"] != "Fetchmark test operator" || hit.Metadata["policy_url"] == "" ||
		len(hit.Metadata["snapshot_sha256"]) != 64 || !strings.HasPrefix(batch.Instance, "snapshot@") {
		t.Fatalf("metadata = %#v", hit.Metadata)
	}
	if len(hit.Provenance) != 0 {
		t.Fatalf("adapter emitted public provenance = %#v", hit.Provenance)
	}
	if _, found := hit.Metadata["provenance"]; found {
		t.Fatalf("adapter emitted metadata provenance = %#v", hit.Metadata)
	}

	empty, err := index.SearchBatch(context.Background(), search.Query{Q: "photosynthesis chlorophyll", MaxResults: 5})
	if err != nil {
		t.Fatal(err)
	}
	if empty.Status != search.BatchDegradedEmpty || len(empty.Diagnostics) != 1 || empty.Diagnostics[0].Reason != "bounded_snapshot_no_match" {
		t.Fatalf("empty batch = %#v", empty)
	}
}

func TestOpenRejectsUnsafeOrUnprovenSnapshots(t *testing.T) {
	now := time.Date(2026, 7, 20, 1, 0, 0, 0, time.UTC)
	tests := map[string]func(map[string]any){
		"unknown field":          func(snapshot map[string]any) { snapshot["surprise"] = true },
		"missing operator":       func(snapshot map[string]any) { delete(snapshot, "operator") },
		"noncanonical source id": func(snapshot map[string]any) { source(snapshot)["id"] = " Kotlin " },
		"missing source owner":   func(snapshot map[string]any) { source(snapshot)["owner"] = "" },
		"missing license":        func(snapshot map[string]any) { source(snapshot)["license"] = "" },
		"missing language":       func(snapshot map[string]any) { delete(document(snapshot), "language") },
		"unknown language":       func(snapshot map[string]any) { document(snapshot)["language"] = "English" },
		"missing safety evidence": func(snapshot map[string]any) {
			delete(document(snapshot), "safety_classification")
		},
		"unknown safety evidence": func(snapshot map[string]any) {
			document(snapshot)["safety_classification"] = "unclassified"
		},
		"invalid public hostname": func(snapshot map[string]any) {
			source(snapshot)["allowed_hosts"] = []string{"-bad.example"}
			source(snapshot)["source_url"] = "https://-bad.example/sitemap.xml"
			for _, item := range source(snapshot)["documents"].([]any) {
				doc := item.(map[string]any)
				doc["url"] = "https://-bad.example/" + strings.TrimPrefix(doc["url"].(string), "https://kotlinlang.org/")
			}
		},
		"noncanonical URL": func(snapshot map[string]any) {
			document(snapshot)["url"] = "HTTPS://KOTLINLANG.ORG:443/docs/cancellation-and-timeouts.html#section"
		},
		"URL userinfo": func(snapshot map[string]any) {
			document(snapshot)["url"] = "https://user@kotlinlang.org/docs/cancellation-and-timeouts.html"
		},
		"URL outside allowed hosts": func(snapshot map[string]any) {
			document(snapshot)["url"] = "https://example.com/docs/cancellation-and-timeouts.html"
		},
		"single label host": func(snapshot map[string]any) {
			source(snapshot)["allowed_hosts"] = []string{"intranet"}
			source(snapshot)["source_url"] = "https://intranet/sitemap.xml"
			for _, item := range source(snapshot)["documents"].([]any) {
				doc := item.(map[string]any)
				doc["url"] = "https://intranet/" + strings.TrimPrefix(doc["url"].(string), "https://kotlinlang.org/")
			}
		},
		"reserved host": func(snapshot map[string]any) {
			source(snapshot)["allowed_hosts"] = []string{"docs.example"}
			source(snapshot)["source_url"] = "https://docs.example/sitemap.xml"
			for _, item := range source(snapshot)["documents"].([]any) {
				doc := item.(map[string]any)
				doc["url"] = "https://docs.example/" + strings.TrimPrefix(doc["url"].(string), "https://kotlinlang.org/")
			}
		},
		"numeric alternate host": func(snapshot map[string]any) {
			source(snapshot)["allowed_hosts"] = []string{"127.1"}
			source(snapshot)["source_url"] = "https://127.1/sitemap.xml"
			for _, item := range source(snapshot)["documents"].([]any) {
				doc := item.(map[string]any)
				doc["url"] = "https://127.1/" + strings.TrimPrefix(doc["url"].(string), "https://kotlinlang.org/")
			}
		},
		"robots denied":            func(snapshot map[string]any) { document(snapshot)["robots_allowed"] = false },
		"source robots denied":     func(snapshot map[string]any) { source(snapshot)["robots_allowed"] = false },
		"missing noindex evidence": func(snapshot map[string]any) { delete(document(snapshot), "noindex") },
		"missing source noindex evidence": func(snapshot map[string]any) {
			delete(source(snapshot), "noindex")
		},
		"noindex": func(snapshot map[string]any) { document(snapshot)["noindex"] = true },
		"too many headings": func(snapshot map[string]any) {
			headings := make([]string, maximumHeadingsPerDocument+1)
			for index := range headings {
				headings[index] = "heading"
			}
			document(snapshot)["headings"] = headings
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			snapshot := fixtureSnapshot(t)
			mutate(snapshot)
			path := writeSnapshot(t, snapshot)
			opened, err := Open(Options{Path: path, Now: now})
			if err == nil {
				_ = opened.Close()
				t.Fatal("Open succeeded")
			}
		})
	}
}

func TestEvidenceAgeBoundaryAndRuntimeExpiry(t *testing.T) {
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	boundary := now.Add(-defaultMaxEvidenceAge)
	snapshot := fixtureSnapshot(t)
	snapshot["generated_at"] = boundary.Format(time.RFC3339Nano)
	source(snapshot)["observed_at"] = boundary.Format(time.RFC3339Nano)
	source(snapshot)["robots_observed_at"] = boundary.Format(time.RFC3339Nano)
	for _, item := range source(snapshot)["documents"].([]any) {
		doc := item.(map[string]any)
		doc["observed_at"] = boundary.Format(time.RFC3339Nano)
		doc["robots_observed_at"] = boundary.Format(time.RFC3339Nano)
	}
	index, err := Open(Options{Path: writeSnapshot(t, snapshot), Now: now})
	if err != nil {
		t.Fatalf("boundary evidence rejected: %v", err)
	}
	t.Cleanup(func() { _ = index.Close() })
	query := search.Query{Q: "Kotlin coroutine cancellation", Language: "en", SafeSearch: intPointer(1)}
	batch, err := index.SearchBatch(context.Background(), query)
	if err != nil || len(batch.Hits) == 0 {
		t.Fatalf("boundary search = %#v err=%v", batch, err)
	}
	index.now = func() time.Time { return now.Add(time.Nanosecond) }
	stale, err := index.SearchBatch(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if len(stale.Hits) != 0 || stale.Status != search.BatchDegradedEmpty || len(stale.Diagnostics) != 1 || stale.Diagnostics[0].Reason != "stale_policy_evidence" {
		t.Fatalf("stale search = %#v", stale)
	}
}

func TestOpenRejectsStaleEvidenceAtEveryLevel(t *testing.T) {
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	stale := now.Add(-defaultMaxEvidenceAge - time.Nanosecond).Format(time.RFC3339Nano)
	tests := map[string]func(map[string]any){
		"snapshot":        func(snapshot map[string]any) { snapshot["generated_at"] = stale },
		"source":          func(snapshot map[string]any) { source(snapshot)["observed_at"] = stale },
		"source robots":   func(snapshot map[string]any) { source(snapshot)["robots_observed_at"] = stale },
		"document":        func(snapshot map[string]any) { document(snapshot)["observed_at"] = stale },
		"document robots": func(snapshot map[string]any) { document(snapshot)["robots_observed_at"] = stale },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			snapshot := fixtureSnapshot(t)
			mutate(snapshot)
			if index, err := Open(Options{Path: writeSnapshot(t, snapshot), Now: now}); err == nil {
				_ = index.Close()
				t.Fatal("Open accepted stale evidence")
			}
		})
	}
}

func TestSearchHonorsEngineAndDomainControls(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("testdata", "official-docs.json"))
	if err != nil {
		t.Fatal(err)
	}
	index, err := Open(Options{Path: path, Now: time.Date(2026, 7, 20, 1, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })

	for _, query := range []search.Query{
		{Q: "Kotlin coroutine cancellation", Engines: []string{"wikipedia"}},
		{Q: "Kotlin coroutine cancellation", IncludeDomains: []string{"example.com"}},
		{Q: "Kotlin coroutine cancellation", ExcludeDomains: []string{"kotlinlang.org"}},
	} {
		batch, err := index.SearchBatch(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch.Hits) != 0 {
			t.Fatalf("query %#v returned %#v", query, batch.Hits)
		}
	}
}

func fixtureSnapshot(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "official-docs.json"))
	if err != nil {
		t.Fatal(err)
	}
	var snapshot map[string]any
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func source(snapshot map[string]any) map[string]any {
	return snapshot["sources"].([]any)[0].(map[string]any)
}

func document(snapshot map[string]any) map[string]any {
	return source(snapshot)["documents"].([]any)[0].(map[string]any)
}

func writeSnapshot(t *testing.T, snapshot map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "official-docs.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestOpenRequiresCleanAbsolutePath(t *testing.T) {
	if _, err := Open(Options{Path: "relative.json"}); err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("Open error = %v", err)
	}
}

func intPointer(value int) *int { return &value }
