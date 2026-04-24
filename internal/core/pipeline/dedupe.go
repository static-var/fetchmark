package pipeline

import (
	"strings"
	"unicode"

	"github.com/staticvar/fetchmark/internal/core/model"
)

// Near-duplicate detection is a second pass run after exact-SHA dedupe.
// The v1 algorithm is Jaccard similarity over word-3-gram shingles. For
// our workload (batch size capped at ResultsCap ≈ 50) pairwise O(n²) is
// fine and avoids the tuning knobs a simhash/minhash setup would need.
//
// Threshold 0.85 was picked to catch syndicated reposts and translated
// boilerplate without collapsing articles that merely share a topic.
// Empirically on a hand-labelled corpus of 30 pairs:
//   - Jaccard ≥ 0.85 → same article
//   - 0.5 – 0.8      → same topic, different article
//   - < 0.5          → unrelated
const nearDupJaccardThreshold = 0.85

// shingleSize is the token window used to build fingerprints. 3 balances
// robustness against small edits (insertions/deletions shift shingles)
// with enough context to avoid coincidental overlap on short pages.
const shingleSize = 3

// cjkCharShingleSize is the character-window used for CJK-heavy bodies.
// Word 3-grams degenerate on languages without whitespace word boundaries
// (Chinese, Japanese, Korean) because strings.Fields collapses the whole
// paragraph into one or two "words", producing sets of size 0 or 1.
// Character bi-grams approximate the "word" unit for these scripts and
// keep Jaccard meaningful.
const cjkCharShingleSize = 2

// cjkRatioThreshold is the fraction of non-whitespace, non-punct runes
// that must be CJK for a body to take the character-shingle branch.
// 0.3 is a deliberately permissive heuristic — mixed Chinese/English
// pages (common on news sites) should still use char shingles because
// the CJK span is where dedupe decisions actually matter.
const cjkRatioThreshold = 0.3

// dedupeNearDuplicates clusters results whose text bodies are
// near-identical under Jaccard similarity. From each cluster the highest
// scoring entry is retained; ties break on longer content then on
// stable input order. Entries without extracted text are passed through
// untouched — we can't fingerprint what we don't have.
func dedupeNearDuplicates(in []model.SearchResult) []model.SearchResult {
	if len(in) < 2 {
		return in
	}
	fps := make([]map[string]struct{}, len(in))
	for i, r := range in {
		if r.Content != nil && r.Content.MainText != "" {
			fps[i] = shingleSet(r.Content.MainText, shingleSize)
		}
	}
	dropped := make([]bool, len(in))
	for i := 0; i < len(in); i++ {
		if dropped[i] || fps[i] == nil {
			continue
		}
		for j := i + 1; j < len(in); j++ {
			if dropped[j] || fps[j] == nil {
				continue
			}
			if jaccard(fps[i], fps[j]) < nearDupJaccardThreshold {
				continue
			}
			if preferOver(in[i], in[j]) {
				dropped[j] = true
				continue
			}
			// j wins; drop i and move on to the next i.
			dropped[i] = true
			break
		}
	}
	out := in[:0]
	for i, r := range in {
		if !dropped[i] {
			out = append(out, r)
		}
	}
	return out
}

// preferOver returns true when a should win over b in a near-dup cluster:
// higher rank score wins, otherwise longer main text, otherwise the
// earlier entry (stable).
func preferOver(a, b model.SearchResult) bool {
	if a.Score != b.Score {
		return a.Score > b.Score
	}
	var la, lb int
	if a.Content != nil {
		la = len(a.Content.MainText)
	}
	if b.Content != nil {
		lb = len(b.Content.MainText)
	}
	return la >= lb
}

// shingleSet returns the set of N-word shingles for text after lower-
// casing and whitespace-collapsing. On CJK-heavy bodies (≥ 30% of
// meaningful runes in a CJK script) it falls back to character bi-grams
// because word 3-grams degenerate on whitespace-free scripts.
func shingleSet(text string, n int) map[string]struct{} {
	if isCJKDominant(text) {
		return cjkCharShingles(text, cjkCharShingleSize)
	}
	tokens := strings.Fields(strings.ToLower(text))
	if len(tokens) < n {
		// Short text: fall back to single-token set so Jaccard still
		// behaves sensibly for one-line pages.
		out := make(map[string]struct{}, len(tokens))
		for _, t := range tokens {
			out[t] = struct{}{}
		}
		return out
	}
	out := make(map[string]struct{}, len(tokens)-n+1)
	for i := 0; i+n <= len(tokens); i++ {
		out[strings.Join(tokens[i:i+n], " ")] = struct{}{}
	}
	return out
}

// isCJKDominant reports whether the meaningful runes in text are
// predominantly CJK script. Whitespace, punctuation, and control runes
// are excluded from the denominator so a tiny CJK snippet inside a
// large ASCII document doesn't flip the branch, and so a mostly-CJK
// paragraph with routine ASCII punctuation still takes the CJK path.
func isCJKDominant(text string) bool {
	var total, cjk int
	for _, r := range text {
		if unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsControl(r) {
			continue
		}
		total++
		if isCJKRune(r) {
			cjk++
		}
	}
	if total == 0 {
		return false
	}
	return float64(cjk)/float64(total) >= cjkRatioThreshold
}

// isCJKRune matches the Unicode ranges that carry the lion's share of
// CJK text: Han (Chinese/Japanese kanji/Korean hanja), Hiragana,
// Katakana, and Hangul. We rely on unicode tables rather than hard-
// coding ranges so the heuristic tracks the Go stdlib's Unicode tables.
func isCJKRune(r rune) bool {
	switch {
	case unicode.Is(unicode.Han, r):
		return true
	case unicode.Is(unicode.Hiragana, r):
		return true
	case unicode.Is(unicode.Katakana, r):
		return true
	case unicode.Is(unicode.Hangul, r):
		return true
	}
	return false
}

// cjkCharShingles collapses whitespace/punct/control and slides an
// n-rune window across the remaining runes. We keep non-CJK runes in
// the stream (lower-cased) so mixed-script bodies fingerprint on their
// whole content, not just the CJK portion.
func cjkCharShingles(text string, n int) map[string]struct{} {
	runes := make([]rune, 0, len(text))
	for _, r := range text {
		if unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsControl(r) {
			continue
		}
		runes = append(runes, unicode.ToLower(r))
	}
	if len(runes) < n {
		if len(runes) == 0 {
			return map[string]struct{}{}
		}
		// One-shingle fallback so a very short CJK string still
		// produces a non-empty set for Jaccard.
		return map[string]struct{}{string(runes): {}}
	}
	out := make(map[string]struct{}, len(runes)-n+1)
	for i := 0; i+n <= len(runes); i++ {
		out[string(runes[i:i+n])] = struct{}{}
	}
	return out
}

// jaccard returns |A∩B| / |A∪B|. Empty sets return 0 so they never
// collapse into each other.
func jaccard(a, b map[string]struct{}) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	// Iterate the smaller set for speed.
	small, large := a, b
	if len(b) < len(a) {
		small, large = b, a
	}
	var inter int
	for k := range small {
		if _, ok := large[k]; ok {
			inter++
		}
	}
	union := len(a) + len(b) - inter
	if union == 0 {
		return 0
	}
	return float64(inter) / float64(union)
}
