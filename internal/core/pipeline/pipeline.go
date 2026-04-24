// Package pipeline composes the domain operations for the Fetchmark API:
// SearXNG search, parallel fetch, extraction, cache, dedupe, and
// re-rank. Handlers depend on Pipeline rather than on the individual
// adapters so orchestration can be tested in isolation and so each
// adapter can be swapped without touching the HTTP layer.
package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/adapters/fetcher"
	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/search"
	"github.com/staticvar/fetchmark/internal/obs"
)

// Fetcher is the subset of fetcher.Fetcher the pipeline needs.
type Fetcher interface {
	FetchMany(ctx context.Context, reqs []fetcher.Request) []fetcher.Result
}

// Extractor turns raw HTML into a domain Content value.
type Extractor interface {
	Extract(raw []byte, pageURL string) (*model.Content, error)
}

// Ranker scores and orders results.
type Ranker interface {
	Score(query string, results []model.SearchResult) []model.SearchResult
}

// Cache is the subset of cache.Cache the pipeline uses.
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, val []byte) error
}

// Options adjust a single pipeline call.
type Options struct {
	Query         string
	URLs          []string
	Engines       []string
	MaxResults    int
	RespectRobots bool
	ProxyURL      string
	UserAgent     string
	Timeout       time.Duration
	Formats       []string
	AdminRequest  bool
}

// Pipeline wires search, fetch, extract, cache, rank.
type Pipeline struct {
	Searcher  search.Searcher
	Fetcher   Fetcher
	Extractor Extractor
	Cache     Cache
	Ranker    Ranker
}

// Search runs the full search pipeline: hit SearXNG, parallel fetch,
// extract, dedupe, rank.
func (p *Pipeline) Search(ctx context.Context, o Options) ([]model.SearchResult, error) {
	hits, err := p.Searcher.Search(ctx, search.Query{
		Q:          o.Query,
		Engines:    o.Engines,
		MaxResults: o.MaxResults,
	})
	if err != nil {
		return nil, err
	}
	return p.process(ctx, o, hitsToResults(hits), o.Query), nil
}

// Parse runs the fetch+extract+rank portion on a caller-supplied URL
// list.
func (p *Pipeline) Parse(ctx context.Context, o Options) []model.SearchResult {
	seed := make([]model.SearchResult, 0, len(o.URLs))
	for _, u := range o.URLs {
		seed = append(seed, model.SearchResult{URL: u})
	}
	return p.process(ctx, o, seed, o.Query)
}

func hitsToResults(hits []search.Hit) []model.SearchResult {
	out := make([]model.SearchResult, len(hits))
	for i, h := range hits {
		out[i] = model.SearchResult{
			URL:     h.URL,
			Title:   h.Title,
			Snippet: h.Snippet,
			Engines: h.Engines,
		}
	}
	return out
}

func (p *Pipeline) process(ctx context.Context, o Options, seed []model.SearchResult, query string) []model.SearchResult {
	seen := make(map[string]int, len(seed))
	results := make([]model.SearchResult, 0, len(seed))
	for _, r := range seed {
		canon, err := cache.CanonicalURL(r.URL)
		if err != nil {
			continue
		}
		if idx, ok := seen[canon]; ok {
			results[idx].Engines = mergeEngines(results[idx].Engines, r.Engines)
			continue
		}
		seen[canon] = len(results)
		r.URL = canon
		results = append(results, r)
	}

	cacheBypass := o.ProxyURL != ""
	needIdx := []int{}
	fetchReqs := []fetcher.Request{}
	for i, r := range results {
		if !cacheBypass && p.Cache != nil {
			if raw, _ := p.Cache.Get(ctx, cache.ArtifactKey(r.URL)); raw != nil {
				var c model.Content
				if err := json.Unmarshal(raw, &c); err == nil {
					applyContent(&results[i], &c)
					results[i].FromCache = true
					obs.CacheEvents.WithLabelValues("fa", "hit").Inc()
					continue
				}
			}
			obs.CacheEvents.WithLabelValues("fa", "miss").Inc()
		}
		needIdx = append(needIdx, i)
		fetchReqs = append(fetchReqs, fetcher.Request{
			URL:           r.URL,
			ProxyURL:      o.ProxyURL,
			UserAgent:     o.UserAgent,
			RespectRobots: o.RespectRobots,
			Timeout:       o.Timeout,
		})
	}

	if len(fetchReqs) > 0 {
		fetched := p.Fetcher.FetchMany(ctx, fetchReqs)
		for i, fr := range fetched {
			idx := needIdx[i]
			results[idx].FetchMS = fr.FetchMS
			if fr.Err != nil {
				results[idx].Unsupported = "fetch_failed"
				obs.FetchOutcome.WithLabelValues("error").Inc()
				continue
			}
			if fr.Unsupported != "" {
				results[idx].Unsupported = fr.Unsupported
				obs.FetchOutcome.WithLabelValues(fr.Unsupported).Inc()
				continue
			}
			obs.FetchOutcome.WithLabelValues("ok").Inc()
			obs.FetchDuration.Observe(float64(fr.FetchMS) / 1000.0)
			c, err := p.Extractor.Extract(fr.Body, results[idx].URL)
			if err != nil || c == nil {
				results[idx].Unsupported = "extract_failed"
				obs.ExtractOutcome.WithLabelValues("error").Inc()
				continue
			}
			if c.UnsupportedReason != "" {
				obs.ExtractOutcome.WithLabelValues(c.UnsupportedReason).Inc()
			} else {
				obs.ExtractOutcome.WithLabelValues("ok").Inc()
			}
			applyContent(&results[idx], c)
			if !cacheBypass && p.Cache != nil {
				if blob, mErr := json.Marshal(c); mErr == nil {
					if sErr := p.Cache.Set(ctx, cache.ArtifactKey(results[idx].URL), blob); sErr == nil {
						obs.CacheEvents.WithLabelValues("fa", "write").Inc()
					}
				}
			}
		}
	}

	results = dedupeByContentSHA(results)
	results = dedupeNearDuplicates(results)

	if p.Ranker != nil && query != "" {
		results = p.Ranker.Score(query, results)
	}
	if o.MaxResults > 0 && len(results) > o.MaxResults {
		results = results[:o.MaxResults]
	}
	return results
}

func applyContent(r *model.SearchResult, c *model.Content) {
	r.Content = c
	if c.Title != "" && r.Title == "" {
		r.Title = c.Title
	}
	if c.PublishedAt != nil {
		r.PublishedAt = c.PublishedAt
	}
	r.Author = c.Author
	r.Markdown = c.Markdown
	r.HTML = c.CleanedHTML
	if c.UnsupportedReason != "" && r.Unsupported == "" {
		r.Unsupported = c.UnsupportedReason
	}
}

func dedupeByContentSHA(in []model.SearchResult) []model.SearchResult {
	seen := map[string]struct{}{}
	out := in[:0]
	for _, r := range in {
		if r.Content != nil && r.Content.MainText != "" {
			h := sha256.Sum256([]byte(normalizeText(r.Content.MainText)))
			key := hex.EncodeToString(h[:])
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
		}
		out = append(out, r)
	}
	return out
}

func normalizeText(s string) string {
	return strings.Join(strings.Fields(strings.ToLower(s)), " ")
}

func mergeEngines(a, b []string) []string {
	s := map[string]struct{}{}
	for _, v := range a {
		s[v] = struct{}{}
	}
	for _, v := range b {
		s[v] = struct{}{}
	}
	out := make([]string, 0, len(s))
	for k := range s {
		out = append(out, k)
	}
	return out
}
