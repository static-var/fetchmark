package pipeline

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/core/search"
)

const rrfRankConstant = 60

type queryVariant struct {
	label string
	query search.Query
}

type variantHits struct {
	label string
	hits  []search.Hit
}

func (p *Pipeline) searchCandidates(ctx context.Context, o Options, candidateCap int) ([]search.Hit, error) {
	base := search.Query{
		Q:              o.Query,
		Engines:        o.Engines,
		Categories:     o.Categories,
		Language:       o.Language,
		TimeRange:      o.TimeRange,
		SafeSearch:     o.SafeSearch,
		IncludeDomains: o.IncludeDomains,
		ExcludeDomains: o.ExcludeDomains,
		ExactMatch:     o.ExactMatch,
		MaxResults:     candidateCap,
	}
	if !isAdvancedSearchDepth(o.SearchDepth) {
		return p.Searcher.Search(ctx, base)
	}

	variants := advancedQueryVariants(base)
	all := make([]variantHits, 0, len(variants))
	for _, variant := range variants {
		hits, err := p.Searcher.Search(ctx, variant.query)
		if err != nil {
			return nil, err
		}
		all = append(all, variantHits{label: variant.label, hits: hits})
	}
	return fuseHitsRRF(all, candidateCap), nil
}

func isAdvancedSearchDepth(depth string) bool {
	return strings.EqualFold(strings.TrimSpace(depth), "advanced")
}

func advancedQueryVariants(base search.Query) []queryVariant {
	variants := make([]queryVariant, 0, 4)
	seen := map[string]struct{}{}
	add := func(label string, q search.Query) {
		q.Q = strings.Join(strings.Fields(q.Q), " ")
		key := q.Q + "\x00" + strconv.FormatBool(q.ExactMatch)
		if q.Q == "" {
			return
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		variants = append(variants, queryVariant{label: label, query: q})
	}

	add("original", base)
	if len(queryTerms(base.Q)) > 1 {
		q := base
		q.ExactMatch = true
		add("exact", q)
	}
	if isFreshnessQuery(base.Q) {
		q := base
		q.Q = freshnessVariant(base.Q)
		add("freshness", q)
	}
	if isDeveloperDocsQuery(base.Q) {
		q := base
		q.Q = docsVariant(base.Q)
		add("docs", q)
	}
	return variants
}

func queryTerms(q string) []string {
	return strings.Fields(strings.TrimSpace(q))
}

var yearTokenPattern = regexp.MustCompile(`\b20[0-9]{2}\b`)

func isFreshnessQuery(q string) bool {
	lower := strings.ToLower(q)
	for _, token := range []string{"latest", "recent", "news", "today"} {
		if containsWord(lower, token) {
			return true
		}
	}
	return yearTokenPattern.MatchString(lower)
}

func freshnessVariant(q string) string {
	lower := strings.ToLower(q)
	switch {
	case !containsWord(lower, "news"):
		return q + " news"
	case !containsWord(lower, "latest"):
		return q + " latest"
	default:
		return q + " recent updates"
	}
}

func isDeveloperDocsQuery(q string) bool {
	lower := strings.ToLower(q)
	for _, token := range []string{
		"api", "sdk", "docs", "documentation", "error", "install", "configure",
		"config", "golang", "python", "kotlin", "java", "javascript",
		"typescript", "node", "react", "cli",
	} {
		if containsWord(lower, token) {
			return true
		}
	}
	return false
}

func docsVariant(q string) string {
	lower := strings.ToLower(q)
	if strings.Contains(lower, "official docs") || strings.Contains(lower, "official documentation") {
		return q
	}
	if containsWord(lower, "documentation") {
		return q + " official"
	}
	return q + " official docs"
}

func containsWord(s, word string) bool {
	for _, field := range strings.FieldsFunc(s, func(r rune) bool {
		return !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9')
	}) {
		if field == word {
			return true
		}
	}
	return false
}

type fusedHit struct {
	hit       search.Hit
	score     float64
	variants  []string
	seenRank  int
	firstSeen int
}

func fuseHitsRRF(all []variantHits, candidateCap int) []search.Hit {
	byURL := map[string]*fusedHit{}
	order := 0
	for _, variant := range all {
		for i, hit := range variant.hits {
			canonicalURL, err := cache.CanonicalURL(hit.URL)
			if err != nil {
				canonicalURL = hit.URL
			}
			rank := i + 1
			fused, ok := byURL[canonicalURL]
			if !ok {
				hit.URL = canonicalURL
				hit.Metadata = cloneMetadata(hit.Metadata)
				fused = &fusedHit{hit: hit, seenRank: rank, firstSeen: order}
				byURL[canonicalURL] = fused
				order++
			} else {
				mergeHit(&fused.hit, hit)
			}
			fused.score += 1 / float64(rrfRankConstant+rank)
			fused.variants = appendUnique(fused.variants, variant.label)
		}
	}

	fused := make([]*fusedHit, 0, len(byURL))
	for _, hit := range byURL {
		annotateRRF(hit)
		fused = append(fused, hit)
	}
	sort.SliceStable(fused, func(i, j int) bool {
		if fused[i].score != fused[j].score {
			return fused[i].score > fused[j].score
		}
		if fused[i].firstSeen != fused[j].firstSeen {
			return fused[i].firstSeen < fused[j].firstSeen
		}
		return fused[i].hit.URL < fused[j].hit.URL
	})
	if candidateCap > 0 && len(fused) > candidateCap {
		fused = fused[:candidateCap]
	}

	out := make([]search.Hit, len(fused))
	for i, hit := range fused {
		out[i] = hit.hit
	}
	return out
}

func mergeHit(dst *search.Hit, src search.Hit) {
	if dst.Title == "" {
		dst.Title = src.Title
	}
	if dst.Snippet == "" {
		dst.Snippet = src.Snippet
	}
	if dst.PublishedAt == nil {
		dst.PublishedAt = src.PublishedAt
	}
	dst.Engines = mergeEnginesStable(dst.Engines, src.Engines)
	if len(src.Metadata) > 0 {
		if dst.Metadata == nil {
			dst.Metadata = map[string]string{}
		}
		for k, v := range src.Metadata {
			if _, exists := dst.Metadata[k]; !exists {
				dst.Metadata[k] = v
			}
		}
	}
}

func annotateRRF(hit *fusedHit) {
	if hit.hit.Metadata == nil {
		hit.hit.Metadata = map[string]string{}
	}
	hit.hit.Metadata["rrf_score"] = fmt.Sprintf("%.6f", hit.score)
	hit.hit.Metadata["rrf_variants"] = strings.Join(hit.variants, ",")
	hit.hit.Metadata["rrf_original_rank"] = strconv.Itoa(hit.seenRank)
}

func cloneMetadata(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func mergeEnginesStable(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, engine := range append(a, b...) {
		if _, ok := seen[engine]; ok {
			continue
		}
		seen[engine] = struct{}{}
		out = append(out, engine)
	}
	return out
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
