package rank

import (
	"strings"
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

func TestScore_FocusedPassageBeatsRepeatedWholeBodyTerms(t *testing.T) {
	results := []model.SearchResult{
		{
			URL:   "https://example.org/archive",
			Title: "Migration archive",
			Content: &model.Content{MainText: strings.Repeat(
				"Bird reports discuss routes. Migration records are indexed separately. ", 80,
			)},
		},
		{
			URL:     "https://example.org/field-study",
			Title:   "Coastal field study",
			Snippet: "Researchers mapped seasonal movement.",
			Content: &model.Content{
				Headings: []string{"Bird migration routes"},
				MainText: "Field teams mapped bird migration routes across two coastal corridors and compared route timing.",
			},
		},
	}

	got := New().Score("bird migration routes", results)
	if got[0].URL != "https://example.org/field-study" {
		t.Fatalf("focused passage should beat repeated dispersed body terms; got %q first (scores %.3f, %.3f)", got[0].URL, got[0].Score, got[1].Score)
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

func TestRankerDoesNotTreatFreshnessSubstringsOrProperNounsAsFreshnessQueries(t *testing.T) {
	for _, query := range []string{
		"renewable energy trends",
		"this weekend hiking routes",
		"New York subway map",
		"New Zealand visa requirements",
		"New Balance shoes",
	} {
		t.Run(query, func(t *testing.T) {
			results := []model.SearchResult{
				{URL: "https://x.com/hiking/status/123", Title: query, Snippet: query},
				{URL: "https://example.org/hiking-routes", Title: query, Snippet: query},
			}
			r := New().Score(query, results)
			if r[0].URL != "https://x.com/hiking/status/123" {
				t.Fatalf("freshness substring should not trigger penalties; got %q first", r[0].URL)
			}
		})
	}
}

func TestRankerTreatsAnyTwentyXXYearAsFreshnessQuery(t *testing.T) {
	social := model.SearchResult{URL: "https://x.com/birder/status/123", Title: "Bird species discovery 2027"}
	article := model.SearchResult{URL: "https://news.example.org/birds/species-discovery-2027", Title: "Bird species discovery 2027"}

	if qualityAdjustment("bird species discovery 2027", social) >= qualityAdjustment("bird species discovery 2027", article) {
		t.Fatal("20xx year query should use freshness penalties consistently")
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

func TestRankerTreatsCurrentAsFreshnessQuery(t *testing.T) {
	now := time.Now().UTC()
	recent := now.Add(-24 * time.Hour)
	results := []model.SearchResult{
		{URL: "https://en.wikipedia.org/wiki/Bird_flu", Title: "Current bird flu outlook", Snippet: "current bird flu outlook"},
		{URL: "https://example.org/news/bird-flu-outlook", Title: "Current bird flu outlook", Snippet: "current bird flu outlook", PublishedAt: &recent},
	}

	got := New().Score("current bird flu outlook", results)
	if got[0].PublishedAt == nil {
		t.Fatalf("current query should prefer recent article over reference result; got %q first", got[0].URL)
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

func TestFilterLowConfidenceKeepsMorphologicalAndAcronymMatches(t *testing.T) {
	results := []model.SearchResult{
		{URL: "https://en.wikipedia.org/wiki/Qanat", Title: "Qanat", Score: 3},
		{URL: "https://sqlite.org/wal.html", Title: "Write-Ahead Logging", Score: 3},
	}

	qanat := FilterLowConfidence("How were qanats and windcatchers combined to cool buildings?", results[:1])
	if len(qanat) != 1 {
		t.Fatalf("plural entity match was dropped: %+v", qanat)
	}
	wal := FilterLowConfidence("How do SQLite WAL checkpoints interact with readers?", results[1:])
	if len(wal) != 1 {
		t.Fatalf("query acronym match was dropped: %+v", wal)
	}
}

func TestFilterLowConfidenceAbstainsFromUnrelatedResults(t *testing.T) {
	results := []model.SearchResult{
		{URL: "https://en.wikipedia.org/wiki/Western_African_Ebola_epidemic", Title: "Western African Ebola epidemic", Score: 12},
		{URL: "https://en.wikipedia.org/wiki/Sodium-ion_battery", Title: "Sodium-ion battery", Score: 5},
	}

	got := FilterLowConfidence("What are the latest global measles trend updates from the WHO?", results)
	if len(got) != 0 {
		t.Fatalf("unrelated results were retained: %+v", got)
	}
}

func TestFilterLowConfidencePreservesEmptyArrayShapeForUnsupportedQuery(t *testing.T) {
	results := []model.SearchResult{{URL: "https://example.org/", Title: "Example"}}

	got := FilterLowConfidence("who are the", results)
	if got == nil || len(got) != 0 {
		t.Fatalf("unsupported query result = %#v, want a non-nil empty slice", got)
	}
}

func TestFilterLowConfidenceUsesCrossProviderAgreementOnlyWithLexicalSupport(t *testing.T) {
	results := []model.SearchResult{
		{
			URL: "https://example.org/heat-pump-guide", Title: "Building efficiency guide", Score: 1,
			Provenance: []model.DiscoveryProvenance{
				{Provider: "mwmbl", Lane: "general", Variant: "original"},
				{Provider: "wiby", Lane: "general", Variant: "original"},
			},
		},
		{
			URL: "https://example.org/unrelated", Title: "Completely unrelated page", Score: 20,
			Provenance: []model.DiscoveryProvenance{
				{Provider: "mwmbl", Lane: "general", Variant: "original"},
				{Provider: "wiby", Lane: "general", Variant: "original"},
			},
		},
	}

	got := FilterLowConfidence("How do heat pumps move heat into a building?", results)
	if len(got) != 1 || got[0].URL != "https://example.org/heat-pump-guide" {
		t.Fatalf("provenance-aware filter = %+v", got)
	}
}

func TestFilterLowConfidenceKeepsStrongFocusedPassageEvidence(t *testing.T) {
	results := []model.SearchResult{{
		URL:     "https://database.example.org/concurrency",
		Title:   "Database concurrency notes",
		Snippet: "A practical explanation for application developers.",
		Content: &model.Content{
			Headings: []string{"Checkpoint behavior"},
			MainText: "SQLite WAL checkpoints can wait for readers because an active reader pins the end mark needed by the checkpoint.",
		},
	}}

	ranked := New().Score("How do SQLite WAL checkpoints interact with readers?", results)
	got := FilterLowConfidence("How do SQLite WAL checkpoints interact with readers?", ranked)
	if len(got) != 1 {
		t.Fatalf("strong focused passage evidence was dropped: %+v", ranked)
	}
}

func TestTopicalTokensPreservesConceptSuffixesAndNormalizesSafePlurals(t *testing.T) {
	got := TopicalTokens("analysis status physics series species news routes batteries classes")
	want := []string{"analysis", "status", "physics", "series", "species", "news", "route", "battery", "class"}
	if len(got) != len(want) {
		t.Fatalf("TopicalTokens() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("TopicalTokens() = %v, want %v", got, want)
		}
	}
}

func TestTopicalTokensPreservesPunctuationBearingIdentifiers(t *testing.T) {
	got := TopicalTokens("C++ C# http.Client")
	want := []string{"c++", "c#", "http", "client"}
	if len(got) != len(want) {
		t.Fatalf("TopicalTokens() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("TopicalTokens() = %v, want %v", got, want)
		}
	}
}

func TestScanTopicalTokensStopsAtRequestedLimit(t *testing.T) {
	text := strings.Repeat("alpha ", maxTopicalBodyTokens+1) + strings.Repeat("unscanned ", maxTopicalBodyTokens*8)

	tokens, scannedBytes := scanTopicalTokens(text, maxTopicalBodyTokens)

	if len(tokens) != maxTopicalBodyTokens {
		t.Fatalf("tokens = %d, want %d", len(tokens), maxTopicalBodyTokens)
	}
	if scannedBytes >= len(text)/2 {
		t.Fatalf("scanner consumed %d/%d bytes after reaching token limit", scannedBytes, len(text))
	}
	for _, token := range tokens {
		if token != "alpha" {
			t.Fatalf("scanner crossed limit into tail token %q", token)
		}
	}

	stopWords := strings.Repeat("the ", maxTopicalBodyTokens+1) + strings.Repeat("unscanned ", maxTopicalBodyTokens*8)
	tokens, scannedBytes = scanTopicalTokens(stopWords, maxTopicalBodyTokens)
	if len(tokens) != 0 {
		t.Fatalf("stop-word scan emitted tokens: %v", tokens)
	}
	if scannedBytes >= len(stopWords)/2 {
		t.Fatalf("stop-word scanner consumed %d/%d bytes after reaching work limit", scannedBytes, len(stopWords))
	}
}

func TestTopicalEvidenceUsesClosestCoveringSpan(t *testing.T) {
	query := newTopicalQuery("alpha beta gamma")
	tokens := append([]string{"alpha"}, strings.Fields(strings.Repeat("filler ", 40))...)
	tokens = append(tokens, "beta", "gamma", "alpha")

	evidence := measureTopicalEvidence(query, tokens)
	if evidence.proximity != 1 {
		t.Fatalf("proximity = %.3f, want closest three-term span score 1", evidence.proximity)
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
