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
	"math"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/staticvar/fetchmark/internal/core/model"
)

// BM25 parameters (Okapi defaults).
const (
	k1             = 1.5
	b              = 0.75
	titleWeight    = 2.0
	engineBonusMax = 0.25
)

// Ranker scores a slice of SearchResults against a query.
type Ranker struct {
	now func() time.Time
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
	q := tokenize(query)
	if len(q) == 0 || len(results) == 0 {
		return results
	}

	docs := make([][]string, len(results))
	titles := make([][]string, len(results))
	var totalLen int
	for i := range results {
		text := results[i].Snippet
		if c := results[i].Content; c != nil {
			text = text + " " + c.MainText
		}
		docs[i] = tokenize(text)
		titles[i] = tokenize(pickTitle(results[i]))
		totalLen += len(docs[i]) + len(titles[i])
	}
	avgLen := float64(totalLen) / float64(len(results))
	if avgLen == 0 {
		avgLen = 1
	}

	idf := make(map[string]float64, len(q))
	for _, term := range q {
		var df int
		for i := range results {
			if containsTerm(docs[i], term) || containsTerm(titles[i], term) {
				df++
			}
		}
		idf[term] = math.Log(1 + (float64(len(results))-float64(df)+0.5)/(float64(df)+0.5))
	}

	for i := range results {
		score := 0.0
		docLen := float64(len(docs[i]) + len(titles[i]))
		for _, term := range q {
			tf := float64(countTerm(docs[i], term)) + titleWeight*float64(countTerm(titles[i], term))
			if tf == 0 {
				continue
			}
			score += idf[term] * (tf * (k1 + 1)) / (tf + k1*(1-b+b*docLen/avgLen))
		}
		// Engine-diversity additive bonus: up to +engineBonusMax for
		// 3+ distinct engines. Small, additive, documented.
		n := len(results[i].Engines)
		if n > 1 {
			bonus := engineBonusMax * math.Min(float64(n-1)/2.0, 1.0)
			score += bonus
		}
		score += qualityAdjustmentAt(query, results[i], r.currentTime())
		results[i].Score = score
	}

	// Stable insertion sort (n is small, <= 50).
	for i := 1; i < len(results); i++ {
		for j := i; j > 0 && results[j].Score > results[j-1].Score; j-- {
			results[j], results[j-1] = results[j-1], results[j]
		}
	}
	return results
}

func qualityAdjustment(query string, result model.SearchResult) float64 {
	return qualityAdjustmentAt(query, result, time.Now())
}

func qualityAdjustmentAt(query string, result model.SearchResult, now time.Time) float64 {
	adj := 0.0
	fresh := isFreshnessQuery(query)
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
	q := strings.ToLower(query)
	phraseMarkers := []string{"this week", "this month"}
	for _, marker := range phraseMarkers {
		if strings.Contains(q, marker) {
			return true
		}
	}

	tokens := tokenize(query)
	markers := map[string]struct{}{
		"latest": {},
		"recent": {},
		"new":    {},
		"news":   {},
		"today":  {},
		"2024":   {},
		"2025":   {},
		"2026":   {},
	}
	for _, token := range tokens {
		if _, ok := markers[token]; ok {
			return true
		}
	}
	return false
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
