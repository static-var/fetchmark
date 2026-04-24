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
	"strings"
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
type Ranker struct{}

// New constructs a Ranker.
func New() *Ranker { return &Ranker{} }

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
