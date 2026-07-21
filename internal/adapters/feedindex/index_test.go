package feedindex

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/search"
)

func TestOpenSearchRoutesTopicsAndRanksLatestStableRelease(t *testing.T) {
	now := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	path := writeSnapshot(t, makeSnapshot(now, "https://blog.python.org/rss.xml", []string{"python", "cpython"}, []map[string]any{
		feedDocument(now, "https://blog.python.org/2025/10/python-3140-final-is-here/", "Python 3.14.0 final is here", "The stable release is available.", now.Add(-286*24*time.Hour)),
		feedDocument(now, "https://blog.python.org/2026/06/python-3146-31314/", "Python 3.14.6 and 3.13.14 are now available", "A pair of bug fix releases.", now.Add(-40*24*time.Hour)),
		feedDocument(now, "https://blog.python.org/2026/07/python-3150-beta-4/", "Python 3.15.0 beta 4 is here", "The final beta is available.", now.Add(-2*24*time.Hour)),
	}))

	index, err := Open(Options{Path: path, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })

	batch, err := index.SearchBatch(context.Background(), search.Query{Q: "What is the latest stable Python release and what changed in it?", MaxResults: 10})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Status != search.BatchHealthy || batch.Provider != providerID || len(batch.Hits) < 2 {
		t.Fatalf("batch = %#v", batch)
	}
	if got := batch.Hits[0].URL; got != "https://blog.python.org/2026/06/python-3146-31314" {
		t.Fatalf("first URL = %q", got)
	}
	if batch.Hits[0].Metadata["feed_source"] != "official" || batch.Hits[0].Metadata["license"] != "CC-BY-SA-4.0" {
		t.Fatalf("metadata = %#v", batch.Hits[0].Metadata)
	}

	empty, err := index.SearchBatch(context.Background(), search.Query{Q: "global sea ice extent", MaxResults: 10})
	if err != nil {
		t.Fatal(err)
	}
	if empty.Status != search.BatchAuthoritativeEmpty || len(empty.Hits) != 0 {
		t.Fatalf("unrouted batch = %#v", empty)
	}
}

func TestOpenFailsClosedOnUnsafeOrUnprovenSnapshot(t *testing.T) {
	now := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	tests := map[string]func(map[string]any){
		"private feed URL": func(snapshot map[string]any) {
			snapshot["sources"].([]map[string]any)[0]["feed_url"] = "https://127.0.0.1/feed.xml"
		},
		"zoned link-local feed URL": func(snapshot map[string]any) {
			snapshot["sources"].([]map[string]any)[0]["feed_url"] = "https://[fe80::1%25en0]/feed.xml"
		},
		"polling faster than budget": func(snapshot map[string]any) {
			snapshot["sources"].([]map[string]any)[0]["min_poll_interval_seconds"] = 60
		},
		"robots denied": func(snapshot map[string]any) {
			snapshot["sources"].([]map[string]any)[0]["documents"].([]map[string]any)[0]["robots_allowed"] = false
		},
		"html noindex": func(snapshot map[string]any) {
			snapshot["sources"].([]map[string]any)[0]["documents"].([]map[string]any)[0]["noindex"] = true
		},
		"x-robots noindex": func(snapshot map[string]any) {
			snapshot["sources"].([]map[string]any)[0]["documents"].([]map[string]any)[0]["x_robots_noindex"] = true
		},
		"missing noindex observation": func(snapshot map[string]any) {
			delete(snapshot["sources"].([]map[string]any)[0]["documents"].([]map[string]any)[0], "noindex")
		},
		"missing license": func(snapshot map[string]any) {
			snapshot["sources"].([]map[string]any)[0]["license"] = ""
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			snapshot := makeSnapshot(now, "https://updates.example/feed.xml", []string{"example"}, []map[string]any{
				feedDocument(now, "https://updates.example/item", "Example update", "Relevant summary.", now.Add(-time.Hour)),
			})
			mutate(snapshot)
			if index, err := Open(Options{Path: writeSnapshot(t, snapshot), Now: now}); err == nil {
				_ = index.Close()
				t.Fatal("Open succeeded")
			}
		})
	}
}

func makeSnapshot(now time.Time, feedURL string, topics []string, documents []map[string]any) map[string]any {
	return map[string]any{
		"version": 1, "generated_at": now.Add(-time.Hour).Format(time.RFC3339),
		"sources": []map[string]any{{
			"id": "official", "feed_url": feedURL, "topics": topics,
			"license": "CC-BY-SA-4.0", "license_url": "https://creativecommons.org/licenses/by-sa/4.0/",
			"fetched_at":              now.Add(-time.Hour).Format(time.RFC3339),
			"feed_robots_observed_at": now.Add(-time.Hour).Format(time.RFC3339), "feed_robots_allowed": true,
			"feed_noindex": false, "feed_x_robots_noindex": false,
			"min_poll_interval_seconds": 3600, "max_items_per_fetch": 50,
			"documents": documents,
		}},
	}
}

func feedDocument(now time.Time, url, title, summary string, published time.Time) map[string]any {
	return map[string]any{
		"url": url, "title": title, "summary": summary,
		"published_at": published.Format(time.RFC3339), "observed_at": now.Add(-time.Hour).Format(time.RFC3339),
		"robots_observed_at": now.Add(-time.Hour).Format(time.RFC3339), "robots_allowed": true,
		"noindex": false, "x_robots_noindex": false,
	}
}

func writeSnapshot(t *testing.T, snapshot map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
