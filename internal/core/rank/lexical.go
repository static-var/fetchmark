package rank

import (
	"html"
	"math"
	"strings"
	"unicode"

	"github.com/staticvar/fetchmark/internal/core/model"
)

const (
	maxTopicalBodyTokens = 8_192
	maxTopicalTokenRunes = 128
	passageTokenLimit    = 160
	passageTokenOverlap  = 32
)

type topicalQuery struct {
	ordered []string
	terms   []string
	set     map[string]struct{}
}

type topicalEvidence struct {
	matches     int
	coverage    float64
	phraseTerms int
	proximity   float64
}

type lexicalDocument struct {
	title    []string
	headings []string
	snippet  []string
	passage  []string
	evidence topicalEvidence
}

// TopicalTokens returns dependency-free lexical terms shared by result ranking,
// confidence admission, and passage selection. It removes question scaffolding
// and applies only conservative plural normalization; it is not a language-
// specific stemmer.
func TopicalTokens(value string) []string {
	tokens, _ := scanTopicalTokens(html.UnescapeString(value), 0)
	return tokens
}

// scanTopicalTokens stops reading as soon as maxTokens lexical words have been
// inspected, including stop words that are not emitted.
// A non-positive limit keeps the public TopicalTokens behavior unbounded for
// short query and metadata fields. scannedBytes makes the body-work bound
// directly testable without timing or allocation heuristics.
func scanTopicalTokens(value string, maxTokens int) ([]string, int) {
	capacity := 32
	if maxTokens > 0 {
		capacity = min(maxTokens, 256)
	}
	out := make([]string, 0, capacity)
	word := make([]rune, 0, 24)
	scannedWords := 0
	flush := func() bool {
		if len(word) == 0 {
			return false
		}
		scannedWords++
		value := string(word)
		word = word[:0]
		if len([]rune(value)) <= 1 && !strings.ContainsAny(value, "+#") {
			return maxTokens > 0 && scannedWords >= maxTokens
		}
		if _, stop := confidenceStopWords[value]; stop {
			return maxTokens > 0 && scannedWords >= maxTokens
		}
		out = append(out, singularConfidenceTerm(value))
		return maxTokens > 0 && scannedWords >= maxTokens
	}

	for offset, r := range value {
		if maxTokens > 0 && offset >= maxTokens*maxTopicalTokenRunes {
			flush()
			return out, offset
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) || ((r == '+' || r == '#') && len(word) > 0) {
			if len(word) < maxTopicalTokenRunes {
				word = append(word, unicode.ToLower(r))
			}
			continue
		}
		if flush() {
			return out, offset
		}
	}
	flush()
	return out, len(value)
}

func newTopicalQuery(value string) topicalQuery {
	ordered := TopicalTokens(value)
	terms := uniqueTerms(ordered)
	return topicalQuery{ordered: ordered, terms: terms, set: termSet(terms)}
}

func newLexicalDocument(query topicalQuery, result model.SearchResult) lexicalDocument {
	document := lexicalDocument{
		title:   TopicalTokens(confidenceTitle(result)),
		snippet: TopicalTokens(result.Snippet),
	}
	if result.Content != nil {
		document.headings = TopicalTokens(strings.Join(result.Content.Headings, " "))
		document.passage, _ = bestTopicalPassage(query, result.Content.MainText)
	}
	document.evidence = bestTopicalEvidence(query,
		document.title,
		document.headings,
		document.snippet,
		document.passage,
	)
	return document
}

func bestTopicalPassage(query topicalQuery, text string) ([]string, topicalEvidence) {
	if len(query.terms) == 0 || strings.TrimSpace(text) == "" {
		return nil, topicalEvidence{}
	}

	all, _ := scanTopicalTokens(text, maxTopicalBodyTokens)
	if len(all) == 0 {
		return nil, topicalEvidence{}
	}

	step := passageTokenLimit - passageTokenOverlap
	var best []string
	var bestEvidence topicalEvidence
	for start := 0; start < len(all); start += step {
		end := min(start+passageTokenLimit, len(all))
		passage := all[start:end]
		evidence := measureTopicalEvidence(query, passage)
		if topicalEvidenceScore(evidence) > topicalEvidenceScore(bestEvidence) {
			best = passage
			bestEvidence = evidence
		}
		if end == len(all) {
			break
		}
	}
	return best, bestEvidence
}

func bestTopicalEvidence(query topicalQuery, fields ...[]string) topicalEvidence {
	var best topicalEvidence
	for _, field := range fields {
		evidence := measureTopicalEvidence(query, field)
		if topicalEvidenceScore(evidence) > topicalEvidenceScore(best) {
			best = evidence
		}
	}
	return best
}

func measureTopicalEvidence(query topicalQuery, tokens []string) topicalEvidence {
	if len(query.terms) == 0 || len(tokens) == 0 {
		return topicalEvidence{}
	}

	positions := make(map[string][]int, len(query.terms))
	for i, token := range tokens {
		if _, wanted := query.set[token]; wanted {
			positions[token] = append(positions[token], i)
		}
	}
	matches := len(positions)
	if matches == 0 {
		return topicalEvidence{}
	}

	return topicalEvidence{
		matches:     matches,
		coverage:    float64(matches) / float64(len(query.terms)),
		phraseTerms: longestQueryPhrase(query.ordered, tokens),
		proximity:   matchProximity(positions, tokens),
	}
}

func topicalEvidenceScore(evidence topicalEvidence) float64 {
	if evidence.matches == 0 {
		return 0
	}
	// Coverage is primary. Phrase and proximity are bounded tie-break evidence,
	// so repetition of one term can never mimic broad topical support.
	return evidence.coverage +
		0.35*math.Min(float64(evidence.phraseTerms)/float64(max(evidence.matches, 1)), 1) +
		0.25*evidence.proximity
}

func longestQueryPhrase(query, text []string) int {
	best := 0
	for qi := range query {
		for ti := range text {
			length := 0
			for qi+length < len(query) && ti+length < len(text) && query[qi+length] == text[ti+length] {
				length++
			}
			if length > best {
				best = length
			}
		}
	}
	return best
}

func matchProximity(positions map[string][]int, tokens []string) float64 {
	if len(positions) <= 1 {
		return 0
	}
	counts := make(map[string]int, len(positions))
	covered, left, bestSpan := 0, 0, math.MaxInt
	for right, token := range tokens {
		if _, wanted := positions[token]; wanted {
			counts[token]++
			if counts[token] == 1 {
				covered++
			}
		}
		for covered == len(positions) && left <= right {
			bestSpan = min(bestSpan, right-left+1)
			leftToken := tokens[left]
			if _, wanted := positions[leftToken]; wanted {
				counts[leftToken]--
				if counts[leftToken] == 0 {
					covered--
				}
			}
			left++
		}
	}
	if bestSpan == math.MaxInt {
		return 0
	}
	return math.Min(float64(len(positions))/float64(bestSpan), 1)
}

func lexicalDocumentContains(document lexicalDocument, term string) bool {
	return containsTerm(document.title, term) ||
		containsTerm(document.headings, term) ||
		containsTerm(document.snippet, term) ||
		containsTerm(document.passage, term)
}

func averageFieldLength(documents []lexicalDocument, field func(lexicalDocument) []string) float64 {
	if len(documents) == 0 {
		return 1
	}
	var total int
	for _, document := range documents {
		total += len(field(document))
	}
	average := float64(total) / float64(len(documents))
	if average < 1 {
		return 1
	}
	return average
}

func bm25Field(tokens []string, term string, averageLength float64) float64 {
	tf := min(countTerm(tokens, term), 2)
	if tf == 0 {
		return 0
	}
	length := float64(max(len(tokens), 1))
	frequency := float64(tf)
	return (frequency * (k1 + 1)) /
		(frequency + k1*(1-b+b*length/averageLength))
}
