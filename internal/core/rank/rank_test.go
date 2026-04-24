package rank

import (
	"testing"

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
