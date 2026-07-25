// Package rank implements Okapi BM25 with a title-weight multiplier and
// a small additive bonus for engine diversity (hits that multiple
// engines returned rank higher than single-engine hits).
//
// This is an intentionally compact, dependency-free implementation sized
// for the 10–50 document reranking the API performs per request. A full
// lexical index (e.g. bleve) would be overkill and opaque for this use
// case — see plan.md §2 for the decision.
package rank

import (
	"html"
	"math"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/staticvar/fetchmark/internal/core/discovery"
	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/search"
)

// BM25 parameters (Okapi defaults).
const (
	k1 = 1.5
	b  = 0.75
	// Field weights encode the reliability order of page-owned metadata. The
	// bounded passage remains useful but cannot overpower title or headings by
	// repeating query terms.
	titleWeight             = 2.2
	headingWeight           = 1.6
	snippetWeight           = 1.0
	passageWeight           = 0.85
	topicalEvidenceWeight   = 0.6
	engineBonusMax          = 0.25
	confidenceScoreFloor    = 3.0
	confidenceCoverageFloor = 0.15
)

var confidenceStopWords = map[string]struct{}{
	"a": {}, "about": {}, "across": {}, "an": {}, "and": {}, "are": {}, "as": {}, "at": {},
	"be": {}, "been": {}, "between": {}, "by": {}, "can": {}, "could": {}, "did": {}, "do": {},
	"does": {}, "during": {}, "for": {}, "from": {}, "had": {}, "has": {}, "have": {}, "how": {},
	"in": {}, "into": {}, "is": {}, "it": {}, "latest": {}, "new": {}, "of": {}, "on": {},
	"or": {}, "over": {}, "recent": {}, "should": {}, "the": {}, "their": {}, "this": {}, "to": {},
	"under": {}, "was": {}, "were": {}, "what": {}, "when": {}, "where": {}, "which": {},
	"who": {}, "why": {}, "with": {}, "would": {},
}

// Ranker scores a slice of SearchResults against a query.
type Ranker struct {
	now func() time.Time
}

// SearchControls carries request policy that cannot be inferred reliably from
// query text. The pipeline derives Fresh from the same discovery profile used
// to select source packs.
type SearchControls struct {
	Fresh bool
}

// New constructs a Ranker.
func New() *Ranker { return &Ranker{now: time.Now} }

func (r *Ranker) currentTime() time.Time {
	if r != nil && r.now != nil {
		return r.now()
	}
	return time.Now()
}

// Score mutates results in place by assigning .Score and returns the
// slice sorted in descending score order. Results with empty extracted
// content still receive a score derived from title + snippet.
func (r *Ranker) Score(query string, results []model.SearchResult) []model.SearchResult {
	return r.ScoreWithControls(query, results, SearchControls{Fresh: isFreshnessQuery(query)})
}

// ScoreWithControls scores results using explicit request policy while keeping
// Score available for existing callers and test fakes.
func (r *Ranker) ScoreWithControls(query string, results []model.SearchResult, controls SearchControls) []model.SearchResult {
	results, _ = r.scoreWithControls(query, results, controls)
	return results
}

// ScoreAndFilterWithControls shares the field analysis between scoring and the
// confidence floor, avoiding a second scan of extracted page bodies.
func (r *Ranker) ScoreAndFilterWithControls(query string, results []model.SearchResult, controls SearchControls) []model.SearchResult {
	results, documents := r.scoreWithControls(query, results, controls)
	return filterLowConfidence(query, results, documents)
}

func (r *Ranker) scoreWithControls(query string, results []model.SearchResult, controls SearchControls) ([]model.SearchResult, []lexicalDocument) {
	q := newTopicalQuery(query)
	if len(q.terms) == 0 || len(results) == 0 {
		return results, nil
	}

	documents := make([]lexicalDocument, len(results))
	for i := range results {
		documents[i] = newLexicalDocument(q, results[i])
	}
	averageTitleLength := averageFieldLength(documents, func(document lexicalDocument) []string { return document.title })
	averageHeadingLength := averageFieldLength(documents, func(document lexicalDocument) []string { return document.headings })
	averageSnippetLength := averageFieldLength(documents, func(document lexicalDocument) []string { return document.snippet })
	averagePassageLength := averageFieldLength(documents, func(document lexicalDocument) []string { return document.passage })

	idf := make(map[string]float64, len(q.terms))
	for _, term := range q.terms {
		var df int
		for _, document := range documents {
			if lexicalDocumentContains(document, term) {
				df++
			}
		}
		idf[term] = math.Log(1 + (float64(len(results))-float64(df)+0.5)/(float64(df)+0.5))
	}

	for i := range results {
		score := 0.0
		document := documents[i]
		for _, term := range q.terms {
			score += idf[term] * (titleWeight*bm25Field(document.title, term, averageTitleLength) +
				headingWeight*bm25Field(document.headings, term, averageHeadingLength) +
				snippetWeight*bm25Field(document.snippet, term, averageSnippetLength) +
				passageWeight*bm25Field(document.passage, term, averagePassageLength))
		}
		score += topicalEvidenceWeight * topicalEvidenceScore(document.evidence)
		// Engine-diversity additive bonus: up to +engineBonusMax for
		// 3+ distinct engines. Small, additive, documented.
		n := len(results[i].Engines)
		if n > 1 {
			bonus := engineBonusMax * math.Min(float64(n-1)/2.0, 1.0)
			score += bonus
		}
		score += qualityAdjustmentForFreshnessAt(results[i], r.currentTime(), controls.Fresh)
		results[i].Score = score
	}

	// Stable insertion sort (n is small, <= 50).
	for i := 1; i < len(results); i++ {
		for j := i; j > 0 && results[j].Score > results[j-1].Score; j-- {
			results[j], results[j-1] = results[j-1], results[j]
			documents[j], documents[j-1] = documents[j-1], documents[j]
		}
	}
	return results, documents
}

// FilterLowConfidence keeps only results with deterministic lexical evidence
// that they address the query. It intentionally prefers an honest empty set to
// returning a high-scoring document whose title, URL, and independent source
// provenance provide no query support. The input order is preserved.
func FilterLowConfidence(query string, results []model.SearchResult) []model.SearchResult {
	return filterLowConfidence(query, results, nil)
}

func filterLowConfidence(query string, results []model.SearchResult, documents []lexicalDocument) []model.SearchResult {
	queryProfile := newTopicalQuery(query)
	queryTerms := queryProfile.terms
	if len(queryTerms) == 0 {
		return results[:0]
	}
	querySet := termSet(queryTerms)
	queryAcronyms := queryAcronymSet(query)
	kept := results[:0]
	for index, result := range results {
		title := confidenceTitle(result)
		titleTerms := uniqueTerms(confidenceTokens(title))
		titleSet := termSet(titleTerms)
		urlSet := termSet(confidenceURLTokens(result.URL))
		titleHits := intersectCount(querySet, titleSet)
		urlHits := intersectCount(querySet, urlSet)
		anyHits := unionMatchCount(querySet, titleSet, urlSet)
		titleCoverage := ratio(titleHits, len(titleTerms))
		queryCoverage := ratio(anyHits, len(queryTerms))
		providers := distinctProviderCount(result.Provenance)

		lexicallySupported := titleHits >= 2 ||
			titleHasQueryAcronym(title, queryAcronyms) ||
			(titleHits >= 1 && titleCoverage >= 0.5 && result.Score >= confidenceScoreFloor) ||
			(titleHits >= 1 && urlHits >= 1 && queryCoverage >= confidenceCoverageFloor) ||
			(providers >= 2 && queryCoverage >= confidenceCoverageFloor)
		if !lexicallySupported {
			topicalEvidence := topicalEvidence{}
			if len(documents) == len(results) {
				topicalEvidence = documents[index].evidence
			} else {
				topicalEvidence = newLexicalDocument(queryProfile, result).evidence
			}
			lexicallySupported = topicalEvidence.matches >= 2 &&
				topicalEvidence.coverage >= 0.6 &&
				(topicalEvidence.phraseTerms >= 2 || topicalEvidence.proximity >= 0.6)
		}
		if lexicallySupported {
			kept = append(kept, result)
		}
	}
	return kept
}

func confidenceTitle(result model.SearchResult) string {
	primary := html.UnescapeString(pickTitle(result))
	discovered := html.UnescapeString(result.Title)
	if discovered == "" || discovered == primary {
		return primary
	}
	return primary + " " + discovered
}

func confidenceTokens(value string) []string {
	return boundedTopicalTokens(value, maxTopicalFieldTokens)
}

func singularConfidenceTerm(word string) string {
	runes := []rune(word)
	if len(runes) > 5 && strings.HasSuffix(word, "ies") && word != "series" && word != "species" {
		return string(runes[:len(runes)-3]) + "y"
	}
	if len(runes) > 5 && strings.HasSuffix(word, "sses") {
		return string(runes[:len(runes)-2])
	}
	if len(runes) > 4 && strings.HasSuffix(word, "s") &&
		!strings.HasSuffix(word, "ss") &&
		!strings.HasSuffix(word, "is") &&
		!strings.HasSuffix(word, "us") &&
		!strings.HasSuffix(word, "ics") &&
		word != "news" && word != "series" && word != "species" {
		return string(runes[:len(runes)-1])
	}
	return word
}

func confidenceURLTokens(rawURL string) []string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil
	}
	path, err := url.PathUnescape(parsed.EscapedPath())
	if err != nil {
		path = parsed.Path
	}
	return uniqueTerms(confidenceTokens(path))
}

func uniqueTerms(terms []string) []string {
	seen := make(map[string]struct{}, len(terms))
	out := make([]string, 0, len(terms))
	for _, term := range terms {
		if _, duplicate := seen[term]; duplicate {
			continue
		}
		seen[term] = struct{}{}
		out = append(out, term)
	}
	return out
}

func termSet(terms []string) map[string]struct{} {
	set := make(map[string]struct{}, len(terms))
	for _, term := range terms {
		set[term] = struct{}{}
	}
	return set
}

func intersectCount(left, right map[string]struct{}) int {
	count := 0
	for term := range left {
		if _, ok := right[term]; ok {
			count++
		}
	}
	return count
}

func unionMatchCount(query, left, right map[string]struct{}) int {
	count := 0
	for term := range query {
		_, inLeft := left[term]
		_, inRight := right[term]
		if inLeft || inRight {
			count++
		}
	}
	return count
}

func ratio(numerator, denominator int) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func distinctProviderCount(provenance []model.DiscoveryProvenance) int {
	providers := make(map[string]struct{}, len(provenance))
	for _, source := range provenance {
		if source.Provider != "" {
			providers[source.Provider] = struct{}{}
		}
	}
	return len(providers)
}

func queryAcronymSet(query string) map[string]struct{} {
	set := map[string]struct{}{}
	for _, word := range rawWords(query) {
		runes := []rune(word)
		if len(runes) < 2 || len(runes) > 8 {
			continue
		}
		hasLetter := false
		valid := true
		for _, r := range runes {
			switch {
			case unicode.IsUpper(r):
				hasLetter = true
			case unicode.IsDigit(r):
			default:
				valid = false
			}
		}
		if valid && hasLetter {
			set[strings.ToLower(word)] = struct{}{}
		}
	}
	return set
}

func titleHasQueryAcronym(title string, queryAcronyms map[string]struct{}) bool {
	if len(queryAcronyms) == 0 {
		return false
	}
	var initials strings.Builder
	for _, word := range rawWords(title) {
		lower := strings.ToLower(word)
		if _, stop := confidenceStopWords[lower]; stop {
			continue
		}
		initials.WriteRune([]rune(lower)[0])
	}
	value := initials.String()
	for acronym := range queryAcronyms {
		if strings.Contains(value, acronym) {
			return true
		}
	}
	return false
}

func rawWords(value string) []string {
	return strings.FieldsFunc(html.UnescapeString(value), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}

func qualityAdjustment(query string, result model.SearchResult) float64 {
	return qualityAdjustmentAt(query, result, time.Now())
}

func qualityAdjustmentAt(query string, result model.SearchResult, now time.Time) float64 {
	return qualityAdjustmentForFreshnessAt(result, now, isFreshnessQuery(query))
}

func qualityAdjustmentForFreshnessAt(result model.SearchResult, now time.Time, fresh bool) float64 {
	adj := 0.0
	u, err := url.Parse(result.URL)
	host := ""
	if err == nil {
		host = strings.ToLower(u.Hostname())
	}

	if result.Unsupported != "" {
		adj -= 1.0
	}
	if fresh {
		if isSocialHost(host) {
			adj -= 0.7
		}
		if isHomepage(result.URL) {
			adj -= 0.45
		}
		if isReferenceURL(result.URL) {
			adj -= 0.25
		}
		if hasRecentPublishedAt(result, now) && isArticleLike(result) {
			adj += 0.35
		}
	} else if isReferenceURL(result.URL) {
		adj += 0.15
	}
	if adj > 0.5 {
		return 0.5
	}
	if adj < -1.5 {
		return -1.5
	}
	return adj
}

func hasRecentPublishedAt(r model.SearchResult, now time.Time) bool {
	if r.PublishedAt == nil {
		return false
	}
	age := now.Sub(*r.PublishedAt)
	return age >= 0 && age <= 90*24*time.Hour
}

func isFreshnessQuery(query string) bool {
	return discovery.ProfileQuery(search.Query{Q: query}).Fresh
}

func isSocialHost(host string) bool {
	host = strings.TrimPrefix(strings.ToLower(host), "www.")
	social := []string{"x.com", "twitter.com", "facebook.com", "instagram.com", "tiktok.com", "reddit.com", "linkedin.com", "threads.net", "bsky.app"}
	for _, s := range social {
		if host == s || strings.HasSuffix(host, "."+s) {
			return true
		}
	}
	return false
}

func isHomepage(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	p := strings.Trim(u.EscapedPath(), "/")
	return p == "" && u.RawQuery == "" && u.Fragment == ""
}

func isReferenceURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := strings.TrimPrefix(strings.ToLower(u.Hostname()), "www.")
	if host == "wikipedia.org" || strings.HasSuffix(host, ".wikipedia.org") || host == "britannica.com" || strings.HasSuffix(host, ".britannica.com") {
		return true
	}
	path := strings.ToLower(u.EscapedPath())
	return strings.Contains(path, "/wiki/") || strings.Contains(path, "/reference/") || strings.Contains(path, "/encyclopedia/")
}

func isArticleLike(r model.SearchResult) bool {
	u, err := url.Parse(r.URL)
	if err != nil {
		return false
	}
	path := strings.ToLower(u.EscapedPath())
	articleMarkers := []string{"/news/", "/article/", "/articles/", "/blog/", "/posts/", "/2024/", "/2025/", "/2026/"}
	for _, marker := range articleMarkers {
		if strings.Contains(path, marker) {
			return true
		}
	}
	return false
}

func pickTitle(r model.SearchResult) string {
	if r.Content != nil && r.Content.Title != "" {
		return r.Content.Title
	}
	return r.Title
}

func tokenize(s string) []string {
	if s == "" {
		return nil
	}
	s = strings.ToLower(s)
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	out := fields[:0]
	for _, f := range fields {
		if len(f) > 1 {
			out = append(out, f)
		}
	}
	return out
}

func containsTerm(tokens []string, term string) bool {
	for _, t := range tokens {
		if t == term {
			return true
		}
	}
	return false
}

func countTerm(tokens []string, term string) int {
	n := 0
	for _, t := range tokens {
		if t == term {
			n++
		}
	}
	return n
}
