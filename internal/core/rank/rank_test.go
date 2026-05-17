package rank

import (
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/model"
)

func TestScore_TitleWeightBeatsBodyMatch(t *testing.T) {
	results := []model.SearchResult{
		{URL: "a", Title: "Not Relevant Page", Snippet: "machine learning tutorials inside"},
		{URL: "b", Title: "Machine Learning Guide", Snippet: "nothing else here"},
	}
	r := New().Score("machine learning", results)
	if r[0].URL != "b" {
		t.Fatalf("title-match should win, got order=%v %v", r[0].URL, r[1].URL)
	}
	if r[0].Score <= r[1].Score {
		t.Fatalf("scores not strictly ordered: %v %v", r[0].Score, r[1].Score)
	}
}

func TestScore_EngineDiversityBonus(t *testing.T) {
	// Same textual relevance; multi-engine should rank ahead.
	base := []model.SearchResult{
		{URL: "a", Title: "BM25 Explained", Engines: []string{"google"}},
		{URL: "b", Title: "BM25 Explained", Engines: []string{"google", "bing", "duckduckgo"}},
	}
	r := New().Score("BM25", base)
	if r[0].URL != "b" {
		t.Fatalf("engine-diversity bonus should break tie, got %v first", r[0].URL)
	}
}

func TestRankerPenalizesUnsupportedResults(t *testing.T) {
	results := []model.SearchResult{
		{URL: "https://example.com/good", Title: "Bird species discovery", Snippet: "recent bird species discovery evidence"},
		{URL: "https://example.com/file.pdf", Title: "Bird species discovery", Snippet: "recent bird species discovery evidence", Unsupported: "pdf"},
	}

	r := New().Score("recent bird species discovery", results)
	if r[0].Unsupported != "" {
		t.Fatalf("supported result should rank ahead of unsupported result; got %q first", r[0].URL)
	}
}

func TestRankerPenalizesSocialResultsForFreshnessQueries(t *testing.T) {
	results := []model.SearchResult{
		{URL: "https://x.com/birder/status/123", Title: "Recent bird species discovery", Snippet: "recent bird species discovery"},
		{URL: "https://news.example.org/birds/new-species-discovery-2026", Title: "Recent bird species discovery", Snippet: "recent bird species discovery"},
	}

	r := New().Score("recent bird species discovery", results)
	if r[0].URL == "https://x.com/birder/status/123" {
		t.Fatalf("social result should be penalized for freshness query; got %q first", r[0].URL)
	}
}

func TestRankerDoesNotTreatFreshnessSubstringsAsFreshnessQueries(t *testing.T) {
	results := []model.SearchResult{
		{URL: "https://x.com/energy/status/123", Title: "Renewable energy trends", Snippet: "renewable energy trends"},
		{URL: "https://example.org/renewable-energy-trends", Title: "Renewable energy trends", Snippet: "renewable energy trends"},
	}

	r := New().Score("renewable energy trends", results)
	if r[0].URL != "https://x.com/energy/status/123" {
		t.Fatalf("renewable should not trigger freshness penalties; got %q first", r[0].URL)
	}
}

func TestRankerPenalizesHomepagesForFreshnessQueries(t *testing.T) {
	results := []model.SearchResult{
		{URL: "https://birds.example.org/", Title: "Recent bird species discovery", Snippet: "recent bird species discovery"},
		{URL: "https://birds.example.org/news/recent-bird-species-discovery", Title: "Recent bird species discovery", Snippet: "recent bird species discovery"},
	}

	r := New().Score("recent bird species discovery", results)
	if r[0].URL == "https://birds.example.org/" {
		t.Fatalf("homepage should be penalized for freshness query; got %q first", r[0].URL)
	}
}

func TestRankerBoostsRecentArticleLikeResults(t *testing.T) {
	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	recent := now.AddDate(0, 0, -7)
	results := []model.SearchResult{
		{URL: "https://example.org/archive/bird-species-discovery", Title: "Recent bird species discovery", Snippet: "recent bird species discovery"},
		{URL: "https://example.org/2026/05/10/recent-bird-species-discovery", Title: "Recent bird species discovery", Snippet: "recent bird species discovery", PublishedAt: &recent},
	}

	r := (&Ranker{now: func() time.Time { return now }}).Score("recent bird species discovery", results)
	if r[0].PublishedAt == nil {
		t.Fatalf("recent article-like result should receive boost; got %q first", r[0].URL)
	}
}

func TestRankerDoesNotBoostFuturePublishedAt(t *testing.T) {
	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	future := now.AddDate(0, 0, 7)
	results := []model.SearchResult{
		{URL: "https://example.org/archive/recent-bird-species-discovery", Title: "Recent bird species discovery", Snippet: "recent bird species discovery"},
		{URL: "https://example.org/story/recent-bird-species-discovery", Title: "Recent bird species discovery", Snippet: "recent bird species discovery", PublishedAt: &future},
	}

	r := (&Ranker{now: func() time.Time { return now }}).Score("recent bird species discovery", results)
	if r[0].PublishedAt != nil {
		t.Fatalf("future PublishedAt should not receive recency boost; got %q first", r[0].URL)
	}
}

func TestQualityAdjustmentRequiresRecentPublishedAtForArticleBoost(t *testing.T) {
	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	recent := now.AddDate(0, 0, -7)
	stale := now.AddDate(0, 0, -180)

	recentArticle := model.SearchResult{URL: "https://example.org/news/recent-bird-species-discovery", Title: "Recent bird species discovery", PublishedAt: &recent}
	staleArticle := model.SearchResult{URL: "https://example.org/news/recent-bird-species-discovery", Title: "Recent bird species discovery", PublishedAt: &stale}
	staleDatedURL := model.SearchResult{URL: "https://example.org/2026/05/10/recent-bird-species-discovery", Title: "Recent bird species discovery", PublishedAt: &stale}

	query := "recent bird species discovery"
	if qualityAdjustmentAt(query, staleArticle, now) >= qualityAdjustmentAt(query, recentArticle, now) {
		t.Fatalf("stale PublishedAt with article URL must not beat recent article")
	}
	if qualityAdjustmentAt(query, staleDatedURL, now) >= qualityAdjustmentAt(query, recentArticle, now) {
		t.Fatalf("stale dated URL must not beat recent article")
	}
}

func TestRankerKeepsReferenceUsefulForEvergreenQueries(t *testing.T) {
	results := []model.SearchResult{
		{URL: "https://example.org/blog/bird-species-taxonomy", Title: "Bird species taxonomy", Snippet: "bird species taxonomy"},
		{URL: "https://en.wikipedia.org/wiki/Bird_species_taxonomy", Title: "Bird species taxonomy", Snippet: "bird species taxonomy"},
	}

	r := New().Score("bird species taxonomy", results)
	if r[0].URL != "https://en.wikipedia.org/wiki/Bird_species_taxonomy" {
		t.Fatalf("reference result should remain useful for evergreen query; got %q first", r[0].URL)
	}
}

func TestScore_NoQueryOrEmptyResults(t *testing.T) {
	r := New()
	if out := r.Score("", []model.SearchResult{{URL: "a"}}); len(out) != 1 {
		t.Fatal("empty query should not drop results")
	}
	if out := r.Score("q", nil); out != nil {
		t.Fatal("nil results should pass through")
	}
}

func TestTokenize_DropsStopPunct(t *testing.T) {
	got := tokenize("Hello, world! It's 2024.")
	want := []string{"hello", "world", "it", "2024"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}
