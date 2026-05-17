package pipeline

import (
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/core/model"
)

func TestAttachQueryChunksPhraseMatchOutranksBagOfTerms(t *testing.T) {
	results := []model.SearchResult{{
		Content: &model.Content{MainText: strings.Join([]string{
			"Routes can shift when birds adapt their migration timing across regions.",
			"Field teams mapped bird migration routes across two coastal corridors.",
		}, "\n\n")},
	}}

	attachQueryChunks(results, "bird migration routes", 2)

	if len(results[0].Chunks) != 2 {
		t.Fatalf("chunks = %+v, want 2", results[0].Chunks)
	}
	if !strings.Contains(results[0].Chunks[0].Text, "bird migration routes") {
		t.Fatalf("exact phrase chunk should outrank bag-of-terms chunk: %+v", results[0].Chunks)
	}
}

func TestAttachQueryChunksTermFrequencyBeatsOneOffLongChunk(t *testing.T) {
	results := []model.SearchResult{{
		Content: &model.Content{MainText: strings.Join([]string{
			"Neural search ranking appears once, then this paragraph drifts into broad product history, release planning, office notes, meeting summaries, roadmap language, and unrelated filler that should not dominate just because it is long.",
			"Neural search ranking improves when neural search systems combine ranking features with calibrated lexical ranking signals.",
		}, "\n\n")},
	}}

	attachQueryChunks(results, "neural search ranking", 2)

	if len(results[0].Chunks) != 2 {
		t.Fatalf("chunks = %+v, want 2", results[0].Chunks)
	}
	if !strings.Contains(results[0].Chunks[0].Text, "calibrated lexical ranking signals") {
		t.Fatalf("repeated focused terms should beat one-off long chunk: %+v", results[0].Chunks)
	}
}

func TestAttachQueryChunksUsesTitleSnippetForScoringOnly(t *testing.T) {
	results := []model.SearchResult{{
		Title:   "Freshwater pearl farming guide",
		Snippet: "Freshwater freshwater pond systems for pearl growers; snippet-only words stay out of chunks.",
		Content: &model.Content{MainText: strings.Join([]string{
			"Pearl markets changed after export demand softened.",
			"Freshwater ponds need oxygen control before stocking begins.",
		}, "\n\n")},
	}}

	attachQueryChunks(results, "freshwater pearl", 2)

	if len(results[0].Chunks) != 2 {
		t.Fatalf("chunks = %+v, want 2", results[0].Chunks)
	}
	if !strings.Contains(results[0].Chunks[0].Text, "Freshwater ponds") {
		t.Fatalf("title/snippet context should influence chunk order: %+v", results[0].Chunks)
	}
	if strings.Contains(results[0].Chunks[0].Text, "snippet-only") {
		t.Fatalf("snippet text must not become returned chunk text: %+v", results[0].Chunks)
	}
}

func TestAttachQueryChunksSuppressesZeroScoreChunksEvenWithContext(t *testing.T) {
	results := []model.SearchResult{{
		Title:   "Bird migration routes",
		Snippet: "Bird migration routes appear only in search metadata.",
		Content: &model.Content{MainText: strings.Join([]string{
			"Clouds formed over the city.",
			"Rain moved across the coastline.",
		}, "\n\n")},
	}}

	attachQueryChunks(results, "bird migration routes", 2)

	if len(results[0].Chunks) != 0 {
		t.Fatalf("zero-score content chunks should be omitted: %+v", results[0].Chunks)
	}
}

func TestAttachQueryChunksPhraseBoostRequiresTokenBoundaries(t *testing.T) {
	results := []model.SearchResult{{
		Content: &model.Content{MainText: strings.Join([]string{
			"Go developer tooling mentions developer workflows but not the short dev token.",
			"Teams use the go dev command during module setup.",
		}, "\n\n")},
	}}

	attachQueryChunks(results, "go dev", 2)

	if len(results[0].Chunks) != 2 {
		t.Fatalf("chunks = %+v, want 2", results[0].Chunks)
	}
	if !strings.Contains(results[0].Chunks[0].Text, "go dev command") {
		t.Fatalf("phrase boost should require exact token boundaries: %+v", results[0].Chunks)
	}
}
