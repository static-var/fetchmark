package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/rank"
)

type relevanceControlRecorder struct {
	query      string
	controls   rank.SearchControls
	legacyCall bool
}

func (r *relevanceControlRecorder) Score(_ string, results []model.SearchResult) []model.SearchResult {
	r.legacyCall = true
	return results
}

func (r *relevanceControlRecorder) ScoreWithControls(query string, results []model.SearchResult, controls rank.SearchControls) []model.SearchResult {
	r.query = query
	r.controls = controls
	return results
}

func TestRelevanceControlsPassExplicitFreshnessToRanker(t *testing.T) {
	recorder := &relevanceControlRecorder{}
	p := &Pipeline{Ranker: recorder}
	p.process(context.Background(), Options{TimeRange: "day"}, nil, "bird flu outlook")

	if recorder.legacyCall || !recorder.controls.Fresh || recorder.query != "bird flu outlook" {
		t.Fatalf("ranker call legacy=%v controls=%+v query=%q", recorder.legacyCall, recorder.controls, recorder.query)
	}
}

func TestRelevanceControlsPreserveEveryTextFreshnessMarker(t *testing.T) {
	for _, query := range []string{"new bird flu guidance", "bird flu this week", "bird flu this month"} {
		t.Run(query, func(t *testing.T) {
			recorder := &relevanceControlRecorder{}
			p := &Pipeline{Ranker: recorder}
			p.process(context.Background(), Options{}, nil, query)
			if recorder.legacyCall || !recorder.controls.Fresh {
				t.Fatalf("ranker call legacy=%v controls=%+v query=%q", recorder.legacyCall, recorder.controls, recorder.query)
			}
		})
	}
}

func TestRelevanceControlsRejectFreshnessPhrasePrefixes(t *testing.T) {
	recorder := &relevanceControlRecorder{}
	p := &Pipeline{Ranker: recorder}
	p.process(context.Background(), Options{}, nil, "this weekend hiking routes")
	if recorder.legacyCall || recorder.controls.Fresh {
		t.Fatalf("ranker call legacy=%v controls=%+v query=%q", recorder.legacyCall, recorder.controls, recorder.query)
	}
}

func TestRelevanceControlsRankRecentArticleFirstForTimeRange(t *testing.T) {
	now := time.Now().UTC()
	recent := now.Add(-24 * time.Hour)
	stale := now.Add(-180 * 24 * time.Hour)
	providerDocument := &model.ProviderDocument{HTML: []byte("bird flu outlook")}
	seed := []model.SearchResult{
		{URL: "https://en.wikipedia.org/wiki/Bird_flu", Title: "Bird flu outlook", Snippet: "bird flu outlook", ProviderDocument: providerDocument},
		{URL: "https://example.org/news/stale-bird-flu-outlook", Title: "Bird flu outlook", Snippet: "bird flu outlook", PublishedAt: &stale, ProviderDocument: providerDocument},
		{URL: "https://example.org/news/recent-bird-flu-outlook", Title: "Bird flu outlook", Snippet: "bird flu outlook", PublishedAt: &recent, ProviderDocument: providerDocument},
	}
	p := &Pipeline{Extractor: stubExtractor{}, Ranker: rank.New()}

	got := p.process(context.Background(), Options{TimeRange: "day", PreserveURLResults: true}, seed, "bird flu outlook")
	if len(got) != 3 || got[0].PublishedAt == nil || !got[0].PublishedAt.Equal(recent) {
		t.Fatalf("time-range ranking = %+v, want recent article first", got)
	}
}
