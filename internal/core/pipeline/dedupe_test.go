package pipeline

import (
	"testing"

	"github.com/staticvar/fetchmark/internal/core/model"
)

func TestJaccard_IdenticalSets(t *testing.T) {
	a := shingleSet("the quick brown fox jumps over the lazy dog", 3)
	b := shingleSet("the quick brown fox jumps over the lazy dog", 3)
	if got := jaccard(a, b); got != 1.0 {
		t.Fatalf("identical Jaccard = %v, want 1.0", got)
	}
}

func TestJaccard_Disjoint(t *testing.T) {
	a := shingleSet("red green blue yellow", 3)
	b := shingleSet("mercury venus earth mars", 3)
	if got := jaccard(a, b); got != 0 {
		t.Fatalf("disjoint Jaccard = %v, want 0", got)
	}
}

func TestDedupeNearDuplicates_DropsReposts(t *testing.T) {
	// Two syndicated copies with tiny edits + one unrelated article.
	article := "This post explains how BM25 ranking works over a corpus " +
		"of documents. It covers term frequency saturation, inverse " +
		"document frequency, and length normalization with practical " +
		"worked examples drawn from common search pipelines."
	// Realistic syndicated repost: identical body plus a short credit line.
	syndicated := article + " Reprinted with permission."
	unrelated := "A beginner's guide to sourdough starters covering flour, " +
		"hydration, and bulk fermentation for home bakers exploring " +
		"fermented breads for the first time."

	in := []model.SearchResult{
		{URL: "https://a/x", Score: 0.4, Content: &model.Content{MainText: article}},
		{URL: "https://b/x", Score: 0.9, Content: &model.Content{MainText: syndicated}},
		{URL: "https://c/y", Score: 0.7, Content: &model.Content{MainText: unrelated}},
	}
	out := dedupeNearDuplicates(in)
	if len(out) != 2 {
		t.Fatalf("expected 2 results after near-dup dedupe, got %d", len(out))
	}
	// Higher-scoring duplicate (b) should survive over a.
	seen := map[string]bool{}
	for _, r := range out {
		seen[r.URL] = true
	}
	if seen["https://a/x"] {
		t.Errorf("lower-scoring duplicate kept; survivors=%v", seen)
	}
	if !seen["https://b/x"] || !seen["https://c/y"] {
		t.Errorf("expected b and c to survive; survivors=%v", seen)
	}
}

func TestDedupeNearDuplicates_PassesThroughWithoutContent(t *testing.T) {
	in := []model.SearchResult{{URL: "https://a"}, {URL: "https://b"}}
	out := dedupeNearDuplicates(in)
	if len(out) != 2 {
		t.Fatalf("results without Content.MainText should be kept; got %d", len(out))
	}
}

func TestShingleSet_ShortText(t *testing.T) {
	s := shingleSet("hi there", 3)
	if len(s) != 2 {
		t.Fatalf("expected fallback unigram set of size 2, got %d", len(s))
	}
}
