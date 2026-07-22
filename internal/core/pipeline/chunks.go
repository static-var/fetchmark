package pipeline

import (
	"math"
	"sort"
	"strings"
	"unicode"

	"github.com/staticvar/fetchmark/internal/core/model"
	corerank "github.com/staticvar/fetchmark/internal/core/rank"
)

const maxChunkChars = 500

type chunkQuery struct {
	terms          map[string]struct{}
	ordered        []string
	phrases        []string
	useBroadTokens bool
}

func attachQueryChunks(results []model.SearchResult, query string, perSource int) {
	if perSource <= 0 {
		return
	}
	if perSource > 3 {
		perSource = 3
	}
	queryProfile := newChunkQuery(query)
	if len(queryProfile.terms) == 0 {
		return
	}
	for i := range results {
		chunks := rankChunks(queryProfile, chunkSourceText(results[i]), chunkScoringContext(results[i]))
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

func chunkScoringContext(r model.SearchResult) string {
	parts := []string{r.Title, r.Snippet}
	if r.Content != nil {
		parts = append(parts, r.Content.Title, r.Content.Description)
	}
	return strings.Join(parts, " ")
}

func rankChunks(query chunkQuery, text, context string) []model.ContentChunk {
	parts := splitChunks(text)
	out := make([]model.ContentChunk, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		score := scoreChunk(query, part, context)
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
		for runeLen(paragraph) > maxChunkChars {
			cut := chunkCutIndex(paragraph, maxChunkChars)
			chunks = append(chunks, strings.TrimSpace(paragraph[:cut]))
			paragraph = strings.TrimSpace(paragraph[cut:])
		}
		if paragraph != "" {
			chunks = append(chunks, paragraph)
		}
	}
	return chunks
}

func chunkCutIndex(s string, maxRunes int) int {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return len(s)
	}
	cutRunes := maxRunes
	for i := maxRunes - 1; i >= maxRunes/2; i-- {
		switch runes[i] {
		case '.', '!', '?', ' ':
			cutRunes = i
			i = -1
		}
	}
	if cutRunes <= 0 {
		cutRunes = maxRunes
	}
	return len(string(runes[:cutRunes]))
}

func runeLen(s string) int {
	n := 0
	for range s {
		n++
	}
	return n
}

func scoreChunk(query chunkQuery, text, context string) float64 {
	tokens := chunkTokensForQuery(query, text)
	if len(tokens) == 0 {
		return 0
	}
	counts := tokenCounts(tokens)
	var matches, totalHits int
	for term := range query.terms {
		if count := counts[term]; count > 0 {
			matches++
			totalHits += count
		}
	}
	if matches == 0 {
		return 0
	}

	termCount := float64(len(query.terms))
	coverage := float64(matches) / termCount
	frequency := math.Log1p(float64(totalHits)) / termCount
	score := coverage + 0.35*frequency

	for _, phrase := range query.phrases {
		if containsTokenPhrase(tokens, phrase) {
			score += chunkPhraseBoost(phrase, len(query.ordered))
		}
	}

	if context != "" {
		contextCounts := tokenCounts(chunkTokensForQuery(query, context))
		var contextBoost float64
		for term, count := range counts {
			if _, ok := query.terms[term]; !ok || count == 0 {
				continue
			}
			if contextCount := contextCounts[term]; contextCount > 0 {
				contextBoost += 0.06 + 0.02*math.Log1p(float64(contextCount))
			}
		}
		score += math.Min(contextBoost, 0.18)
	}

	return score * chunkLengthMultiplier(len(tokens))
}

func chunkTerms(tokens []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, token := range tokens {
		out[token] = struct{}{}
	}
	return out
}

func newChunkQuery(s string) chunkQuery {
	tokens := chunkTokens(s)
	useBroadTokens := len(tokens) == 0
	if useBroadTokens {
		tokens = broadChunkTokens(s)
	}
	ordered := dedupeOrderedTokens(tokens)
	return chunkQuery{
		terms:          chunkTerms(ordered),
		ordered:        ordered,
		phrases:        chunkPhrases(ordered),
		useBroadTokens: useBroadTokens,
	}
}

func chunkTokens(s string) []string {
	return corerank.TopicalTokens(s)
}

func chunkTokensForQuery(query chunkQuery, s string) []string {
	if query.useBroadTokens {
		return broadChunkTokens(s)
	}
	return chunkTokens(s)
}

func broadChunkTokens(s string) []string {
	fields := strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	out := make([]string, 0, len(fields))
	for _, token := range fields {
		token = normalizeBroadChunkToken(token)
		if token != "" {
			out = append(out, token)
		}
	}
	return out
}

func normalizeBroadChunkToken(token string) string {
	if len(token) < 2 {
		return ""
	}
	if strings.HasSuffix(token, "s") && len(token) > 3 {
		token = strings.TrimSuffix(token, "s")
	}
	return token
}

func tokenCounts(tokens []string) map[string]int {
	out := make(map[string]int, len(tokens))
	for _, token := range tokens {
		out[token]++
	}
	return out
}

func dedupeOrderedTokens(tokens []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		out = append(out, token)
	}
	return out
}

func chunkPhrases(tokens []string) []string {
	if len(tokens) < 2 {
		return nil
	}
	seen := map[string]struct{}{}
	phrases := make([]string, 0, len(tokens))
	maxLen := min(len(tokens), 4)
	for size := maxLen; size >= 2; size-- {
		for start := 0; start+size <= len(tokens); start++ {
			phrase := strings.Join(tokens[start:start+size], " ")
			if _, ok := seen[phrase]; ok {
				continue
			}
			seen[phrase] = struct{}{}
			phrases = append(phrases, phrase)
		}
	}
	return phrases
}

func chunkPhraseBoost(phrase string, queryLen int) float64 {
	terms := strings.Count(phrase, " ") + 1
	if terms == queryLen {
		return 0.75
	}
	return 0.18 * float64(terms-1)
}

func containsTokenPhrase(tokens []string, phrase string) bool {
	phraseTokens := strings.Fields(phrase)
	if len(phraseTokens) == 0 || len(phraseTokens) > len(tokens) {
		return false
	}
	for i := 0; i+len(phraseTokens) <= len(tokens); i++ {
		matches := true
		for j, token := range phraseTokens {
			if tokens[i+j] != token {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

func chunkLengthMultiplier(tokenCount int) float64 {
	switch {
	case tokenCount <= 0:
		return 0
	case tokenCount < 6:
		return 0.85
	case tokenCount <= 90:
		return 1
	default:
		return math.Max(0.72, 1-float64(tokenCount-90)/220)
	}
}
