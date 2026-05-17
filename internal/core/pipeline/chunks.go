package pipeline

import (
	"sort"
	"strings"
	"unicode"

	"github.com/staticvar/fetchmark/internal/core/model"
)

const maxChunkChars = 500

func attachQueryChunks(results []model.SearchResult, query string, perSource int) {
	if perSource <= 0 {
		return
	}
	if perSource > 3 {
		perSource = 3
	}
	queryTerms := chunkTerms(query)
	if len(queryTerms) == 0 {
		return
	}
	for i := range results {
		chunks := rankChunks(queryTerms, chunkSourceText(results[i]))
		if len(chunks) > perSource {
			chunks = chunks[:perSource]
		}
		results[i].Chunks = chunks
	}
}

func chunkSourceText(r model.SearchResult) string {
	if r.Content != nil {
		if strings.TrimSpace(r.Content.MainText) != "" {
			return r.Content.MainText
		}
		if strings.TrimSpace(r.Content.Markdown) != "" {
			return r.Content.Markdown
		}
	}
	return ""
}

func rankChunks(queryTerms map[string]struct{}, text string) []model.ContentChunk {
	parts := splitChunks(text)
	out := make([]model.ContentChunk, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		score := scoreChunk(queryTerms, part)
		if score <= 0 {
			continue
		}
		out = append(out, model.ContentChunk{Text: part, Score: score})
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Score > out[j].Score
	})
	return out
}

func splitChunks(text string) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	paragraphs := strings.Split(text, "\n\n")
	chunks := make([]string, 0, len(paragraphs))
	for _, paragraph := range paragraphs {
		paragraph = strings.TrimSpace(paragraph)
		if paragraph == "" {
			continue
		}
		for len(paragraph) > maxChunkChars {
			cut := strings.LastIndexAny(paragraph[:maxChunkChars], ".!? ")
			if cut < maxChunkChars/2 {
				cut = maxChunkChars
			}
			chunks = append(chunks, strings.TrimSpace(paragraph[:cut]))
			paragraph = strings.TrimSpace(paragraph[cut:])
		}
		if paragraph != "" {
			chunks = append(chunks, paragraph)
		}
	}
	return chunks
}

func scoreChunk(queryTerms map[string]struct{}, text string) float64 {
	tokens := chunkTerms(text)
	if len(tokens) == 0 {
		return 0
	}
	var matches int
	for term := range queryTerms {
		if _, ok := tokens[term]; ok {
			matches++
		}
	}
	return float64(matches) / float64(len(queryTerms))
}

func chunkTerms(s string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, token := range strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	}) {
		if len(token) < 2 {
			continue
		}
		out[token] = struct{}{}
		if strings.HasSuffix(token, "s") && len(token) > 3 {
			out[strings.TrimSuffix(token, "s")] = struct{}{}
		}
	}
	return out
}
