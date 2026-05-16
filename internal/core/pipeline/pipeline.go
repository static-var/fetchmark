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
	"sync"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/adapters/extractor"
	"github.com/staticvar/fetchmark/internal/adapters/fetcher"
	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/search"
	"github.com/staticvar/fetchmark/internal/obs"
)

// Fetcher is the subset of fetcher.Fetcher the pipeline needs.
type Fetcher interface {
	Fetch(ctx context.Context, r fetcher.Request) fetcher.Result
}

// Extractor turns raw HTML into a domain Content value.
type Extractor interface {
	Extract(raw []byte, pageURL string) (*model.Content, error)
}

// Ranker scores and orders results.
type Ranker interface {
	Score(query string, results []model.SearchResult) []model.SearchResult
}

// Renderer turns a URL into post-JS HTML. The pipeline calls it when
// the first-pass extractor flags a page as js_required, and only if
// Options.Render is true or the pipeline was configured with
// RendererAuto. A nil Renderer means the feature is disabled.
type Renderer interface {
	Render(ctx context.Context, url string) ([]byte, error)
}

// Cache is the subset of cache.Cache the pipeline uses. The three
// extra methods beyond Get/Set exist so the cold path can coalesce
// concurrent callers both within a process (Do) and across processes
// (WithLock).
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, val []byte) error
	Do(key string, fn func() (any, error)) (any, error, bool)
	WithLock(ctx context.Context, key string, opts cache.LockOptions, fn func(context.Context) ([]byte, error)) ([]byte, error)
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
	// Render forces the headless renderer to handle this call, bypassing
	// the first-pass plain fetch only when the extractor flags it as
	// js_required. It is a hint; an absent Renderer or a disabled
	// RendererAuto still results in a plain fetch.
	Render bool
}

// Pipeline wires search, fetch, extract, cache, rank.
type Pipeline struct {
	Searcher     search.Searcher
	Fetcher      Fetcher
	Extractor    Extractor
	Cache        Cache
	Ranker       Ranker
	Renderer     Renderer
	RendererAuto bool
	// RendererTimeout is the worst-case wall time a single renderer call
	// can take. Used to size the Redis stampede-lock TTL and wait budget
	// on the render path so a slow headless fetch doesn't lose the lock
	// mid-call or cause waiters to give up too early. Zero means the
	// pipeline falls back to Options.Timeout.
	RendererTimeout time.Duration
	// EgressValidate, when non-nil, is consulted before a URL is handed
	// to the Renderer. Returning a non-nil error marks the result
	// unsupported with "egress_reject" and skips the render call. This
	// closes the SSRF hole on the render path, which would otherwise
	// bypass the fetcher's dial-time validation.
	EgressValidate func(ctx context.Context, rawURL string) error
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
	if o.MaxResults > 0 && len(hits) > o.MaxResults {
		hits = hits[:o.MaxResults]
	}
	results := p.process(ctx, o, hitsToResults(hits), o.Query)
	filterResultsByFormats(results, o.Formats)
	return results, nil
}

// Parse runs the fetch+extract+rank portion on a caller-supplied URL
// list.
func (p *Pipeline) Parse(ctx context.Context, o Options) []model.SearchResult {
	seed := make([]model.SearchResult, 0, len(o.URLs))
	for _, u := range o.URLs {
		seed = append(seed, model.SearchResult{URL: u})
	}
	results := p.process(ctx, o, seed, o.Query)
	filterResultsByFormats(results, o.Formats)
	return results
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
	renderMode := o.Render && p.Renderer != nil

	// Cold path: for each result in need of extraction, run a bounded
	// goroutine that does get-recheck → local singleflight → Redis lock
	// → get-recheck → fetch → extract → cache.Set. The fetcher already
	// enforces global and per-host concurrency internally, so we spawn
	// one goroutine per URL without additional limiting here.
	var wg sync.WaitGroup
	for i := range results {
		i := i
		r := &results[i]
		// Try cache first synchronously — a hit avoids spawning a worker
		// and keeps the common path allocation-free. Render requests
		// consult the rendered key space so a plain-fetched
		// js_required placeholder never shadows a later render.
		if !cacheBypass && p.Cache != nil {
			primaryKey := cache.ArtifactKey(r.URL)
			if renderMode {
				primaryKey = cache.RenderedArtifactKey(r.URL)
			}
			if raw, _ := p.Cache.Get(ctx, primaryKey); raw != nil {
				var c model.Content
				if err := json.Unmarshal(raw, &c); err == nil {
					applyContent(r, &c)
					r.FromCache = true
					obs.CacheEvents.WithLabelValues("fa", "hit").Inc()
					continue
				}
			}
			obs.CacheEvents.WithLabelValues("fa", "miss").Inc()
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			p.fetchAndExtract(ctx, o, r, cacheBypass)
		}()
	}
	wg.Wait()

	results = dedupeByContentSHA(results)
	// Rank first, then near-dup collapse. The cluster-winner tiebreak
	// in dedupeNearDuplicates uses SearchResult.Score, which is only
	// meaningful after the ranker has run — otherwise every winner is
	// picked by MainText length and input order, which can drop the
	// more relevant duplicate.
	if p.Ranker != nil && query != "" {
		results = p.Ranker.Score(query, results)
	}
	results = dedupeNearDuplicates(results)
	if o.MaxResults > 0 && len(results) > o.MaxResults {
		results = results[:o.MaxResults]
	}
	return results
}

func filterResultsByFormats(results []model.SearchResult, formats []string) {
	if len(formats) == 0 {
		return
	}

	requested := map[string]bool{}
	for _, format := range formats {
		format = strings.ToLower(strings.TrimSpace(format))
		switch format {
		case "markdown", "html", "json":
			requested[format] = true
		}
	}
	if len(requested) == 0 {
		return
	}

	keepMarkdown := requested["markdown"]
	keepHTML := requested["html"]
	keepJSON := requested["json"]

	for i := range results {
		if !keepMarkdown {
			results[i].Markdown = ""
		}
		if !keepHTML {
			results[i].HTML = ""
		}
		if results[i].Content != nil {
			results[i].Content.Markdown = ""
			results[i].Content.CleanedHTML = ""
			if !keepJSON {
				results[i].Content.MainText = ""
			}
		}
	}
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

// fetchAndExtract populates r by fetching, extracting, and caching a
// single URL. Concurrent callers of the same URL within a process are
// coalesced via Cache.Do; across processes a Redis-backed WithLock
// further suppresses duplicated work. Every path re-checks the cache
// on entry to avoid redundant fetches after another caller populated
// it while this caller was queued.
func (p *Pipeline) fetchAndExtract(ctx context.Context, o Options, r *model.SearchResult, cacheBypass bool) {
	// Render mode picks a separate cache key space so a plain-fetched
	// js_required placeholder never shadows a later render=true call,
	// and a rendered blob never masks the cheap path for callers who
	// did not ask for it.
	renderMode := o.Render && p.Renderer != nil
	key := cache.ArtifactKey(r.URL)
	if renderMode {
		key = cache.RenderedArtifactKey(r.URL)
	}

	// doFetch is the critical section: a single fetch+extract that
	// ultimately produces a JSON-serialised Content blob in the cache.
	doFetch := func(ctx context.Context) ([]byte, error) {
		// Re-check once more inside the lock — another worker/process
		// may have populated the entry while we waited for the lock.
		if !cacheBypass && p.Cache != nil {
			if raw, _ := p.Cache.Get(ctx, key); raw != nil {
				return raw, nil
			}
		}

		// Explicit render: skip the plain fetch entirely and go
		// straight to the headless service. The extractor still runs
		// on the rendered HTML so the output shape matches the plain
		// path (Content with markdown/metadata/etc.).
		if renderMode {
			return p.renderAndExtract(ctx, o, r, key, cacheBypass)
		}

		req := fetcher.Request{
			URL:           r.URL,
			ProxyURL:      o.ProxyURL,
			UserAgent:     o.UserAgent,
			RespectRobots: o.RespectRobots,
			Timeout:       o.Timeout,
		}
		fr := p.Fetcher.Fetch(ctx, req)
		// Record fetch-side outcome on the result; Err + Unsupported
		// cases short-circuit the rest of the pipeline for this URL.
		if fr.Err != nil {
			r.Unsupported = "fetch_failed"
			r.FetchMS = fr.FetchMS
			obs.FetchOutcome.WithLabelValues("error").Inc()
			return nil, fr.Err
		}
		if fr.Unsupported != "" {
			r.Unsupported = fr.Unsupported
			r.FetchMS = fr.FetchMS
			obs.FetchOutcome.WithLabelValues(fr.Unsupported).Inc()
			return nil, nil
		}
		r.FetchMS = fr.FetchMS
		obs.FetchOutcome.WithLabelValues("ok").Inc()
		obs.FetchDuration.Observe(float64(fr.FetchMS) / 1000.0)

		c, err := p.Extractor.Extract(fr.Body, r.URL)
		if err != nil || c == nil {
			r.Unsupported = "extract_failed"
			obs.ExtractOutcome.WithLabelValues("error").Inc()
			return nil, err
		}
		if c.UnsupportedReason != "" {
			obs.ExtractOutcome.WithLabelValues(c.UnsupportedReason).Inc()
		} else {
			obs.ExtractOutcome.WithLabelValues("ok").Inc()
		}
		blob, mErr := json.Marshal(c)
		if mErr != nil {
			applyContent(r, c)
			return nil, mErr
		}
		applyContent(r, c)
		if !cacheBypass && p.Cache != nil {
			if sErr := p.Cache.Set(ctx, key, blob); sErr == nil {
				obs.CacheEvents.WithLabelValues("fa", "write").Inc()
			}
		}

		// Automatic render upgrade: when the first-pass extractor
		// flagged js_required and the operator has opted into auto
		// rendering, try the headless service. The plain blob is kept
		// under the plain key so future non-render requests hit cache;
		// the rendered blob is stored under a separate key so it does
		// not clobber the cheap path.
		if p.Renderer != nil && p.RendererAuto &&
			c.UnsupportedReason == extractor.ReasonJSRequired {
			if _, rerr := p.tryAutoRender(ctx, o, r, cacheBypass); rerr == nil {
				// r already updated in place by tryAutoRender.
			}
		}
		return blob, nil
	}

	// Fast path: no cache configured (bypass or nil) just runs
	// directly.
	if cacheBypass || p.Cache == nil {
		_, _ = doFetch(ctx)
		return
	}

	// Local singleflight by key suppresses duplicate in-flight workers
	// inside this process. Cross-instance suppression happens inside
	// doFetch via WithLock.
	_, _, _ = p.Cache.Do(key, func() (any, error) {
		// Recheck cache once more — singleflight may have raced us.
		if raw, _ := p.Cache.Get(ctx, key); raw != nil {
			if err := applyRaw(r, raw); err == nil {
				r.FromCache = true
			}
			return raw, nil
		}
		// Cross-process lock. LockTTL is generous relative to our
		// fetch+extract budget so a slow fetcher doesn't lose the lock
		// mid-request.
		raw, err := p.Cache.WithLock(ctx, key, cache.LockOptions{
			LockTTL:      p.lockTTL(o),
			WaitMax:      p.lockWait(o),
			PollInterval: 100 * time.Millisecond,
		}, doFetch)
		if err != nil {
			return nil, err
		}
		// If another worker/process populated the cache while we
		// waited, the lock path returned that blob without calling our
		// fetcher — reflect it on r.
		if r.Content == nil && raw != nil {
			if err := applyRaw(r, raw); err == nil {
				r.FromCache = true
			}
		}
		return raw, nil
	})
}

// renderAndExtract runs the renderer as the primary source of HTML,
// extracts content, and writes the result to the supplied cache key.
// Used for explicit render=true requests.
func (p *Pipeline) renderAndExtract(ctx context.Context, o Options, r *model.SearchResult, key string, cacheBypass bool) ([]byte, error) {
	// The fetcher's DialControl is not on this path — validate the URL
	// against the egress policy before handing it to the renderer so
	// render=true can't be used to reach RFC1918/link-local targets.
	if p.EgressValidate != nil {
		if err := p.EgressValidate(ctx, r.URL); err != nil {
			r.Unsupported = "egress_reject"
			obs.RendererOutcome.WithLabelValues("skipped").Inc()
			return nil, err
		}
	}
	start := time.Now()
	raw, err := p.Renderer.Render(ctx, r.URL)
	if err != nil {
		r.Unsupported = "render_failed"
		obs.RendererOutcome.WithLabelValues("error").Inc()
		return nil, err
	}
	obs.RendererOutcome.WithLabelValues("ok").Inc()
	obs.RendererDuration.Observe(time.Since(start).Seconds())
	r.FetchMS = int64(time.Since(start) / time.Millisecond)

	c, err := p.Extractor.Extract(raw, r.URL)
	if err != nil || c == nil {
		r.Unsupported = "extract_failed"
		obs.ExtractOutcome.WithLabelValues("error").Inc()
		return nil, err
	}
	if c.UnsupportedReason != "" {
		obs.ExtractOutcome.WithLabelValues(c.UnsupportedReason).Inc()
	} else {
		obs.ExtractOutcome.WithLabelValues("ok").Inc()
	}
	blob, mErr := json.Marshal(c)
	applyContent(r, c)
	if mErr != nil {
		return nil, mErr
	}
	_ = o // reserved for future per-request renderer knobs
	// Never cache a rendered artifact that still flags js_required:
	// doing so would make subsequent explicit-render requests a cache
	// hit on a useless placeholder until TTL expiry, defeating the
	// entire point of the render path.
	if c.UnsupportedReason == extractor.ReasonJSRequired {
		return blob, nil
	}
	if !cacheBypass && p.Cache != nil {
		if sErr := p.Cache.Set(ctx, key, blob); sErr == nil {
			obs.CacheEvents.WithLabelValues("fa", "write").Inc()
		}
	}
	return blob, nil
}

// applyRenderedContent applies a rendered-artifact blob onto r,
// overwriting any js_required placeholder populated by an earlier
// plain fetch. Unlike applyContent (fill-only), this unconditionally
// clears placeholder Title/Unsupported before applying so the rendered
// upgrade is guaranteed to surface on r. Used at every rendered-cache-
// hit site in tryAutoRender (pre-lock check, in-lock re-check, and
// post-lock peer-populated path).
func applyRenderedContent(r *model.SearchResult, c *model.Content) {
	r.Title = ""
	r.Unsupported = ""
	applyContent(r, c)
}

// tryAutoRender upgrades a js_required plain result by running the
// headless renderer and replacing r's content when the second pass
// produces a real extraction. The rendered blob is cached under the
// rendered key; the plain key is left untouched so cheap paths stay
// cheap.
//
// When a cache is configured, the renderer call is guarded by a
// Redis-backed stampede lock on the rendered key so concurrent auto-
// render upgrades for the same URL collapse to a single renderer
// invocation. The lock TTL is sized against a render budget (not the
// caller's fetch timeout) because auto-render runs with o.Render=false.
func (p *Pipeline) tryAutoRender(ctx context.Context, o Options, r *model.SearchResult, cacheBypass bool) ([]byte, error) {
	renderedKey := cache.RenderedArtifactKey(r.URL)
	if !cacheBypass && p.Cache != nil {
		if raw, _ := p.Cache.Get(ctx, renderedKey); raw != nil {
			var c model.Content
			if err := json.Unmarshal(raw, &c); err == nil && c.UnsupportedReason != extractor.ReasonJSRequired {
				applyRenderedContent(r, &c)
				r.FromCache = true
				obs.CacheEvents.WithLabelValues("fa", "hit").Inc()
				return raw, nil
			}
		}
	}
	if cacheBypass || p.Cache == nil {
		return p.renderAndExtract(ctx, o, r, renderedKey, cacheBypass)
	}

	// lockTTL/lockWait read o.Render to size against the renderer
	// timeout. Auto-render arrives with o.Render=false, so derive the
	// budget from a Render=true clone to avoid lock expiry mid-render.
	ro := o
	ro.Render = true

	raw, err := p.Cache.WithLock(ctx, renderedKey, cache.LockOptions{
		LockTTL:      p.lockTTL(ro),
		WaitMax:      p.lockWait(ro),
		PollInterval: 100 * time.Millisecond,
	}, func(ctx context.Context) ([]byte, error) {
		// Re-check inside the lock — a peer may have populated the
		// rendered cache while we were waiting. If so, reuse it.
		if raw, _ := p.Cache.Get(ctx, renderedKey); raw != nil {
			var c model.Content
			if err := json.Unmarshal(raw, &c); err == nil && c.UnsupportedReason != extractor.ReasonJSRequired {
				applyRenderedContent(r, &c)
				r.FromCache = true
				obs.CacheEvents.WithLabelValues("fa", "hit").Inc()
				return raw, nil
			}
		}
		return p.renderAndExtract(ctx, o, r, renderedKey, cacheBypass)
	})
	if err != nil {
		return nil, err
	}
	// The WithLock path returns either the blob produced by our fn or
	// one populated by a peer while we waited. In the peer-populated
	// case our fn did not run, so r still carries the js_required plain
	// content from the earlier fetch — unconditionally re-apply so the
	// caller sees the rendered upgrade.
	if raw != nil && r.FromCache == false {
		var c model.Content
		if err := json.Unmarshal(raw, &c); err == nil && c.UnsupportedReason != extractor.ReasonJSRequired {
			applyRenderedContent(r, &c)
			r.FromCache = true
		}
	}
	return raw, nil
}

// applyRaw decodes a cached artifact blob onto r. It is the symmetric
// counterpart of the Cache.Set call inside doFetch.
func applyRaw(r *model.SearchResult, raw []byte) error {
	var c model.Content
	if err := json.Unmarshal(raw, &c); err != nil {
		return err
	}
	applyContent(r, &c)
	return nil
}

// lockTTL derives the Redis-lock TTL from the worst-case work time so
// the lock is guaranteed to outlive the critical section, with a small
// cushion for extraction and serialisation. On the render path the
// worst case is the renderer's own timeout, not the fetch timeout.
func (p *Pipeline) lockTTL(o Options) time.Duration {
	base := p.criticalBudget(o)
	return base + 5*time.Second
}

// lockWait caps how long a second caller blocks before giving up and
// running the work without the lock. Match lockTTL so a waiter only
// falls back to unlocked (duplicate) work when the lock is genuinely
// stale, not while the leader is still legitimately inside its
// critical section. Caller cancellation is still governed by ctx.
func (p *Pipeline) lockWait(o Options) time.Duration {
	return p.lockTTL(o)
}

// criticalBudget returns the worst-case wall time for the work the
// stampede lock is guarding, picking the larger of the caller's fetch
// timeout and the configured renderer timeout when the renderer is in
// play. Callers that set neither fall back to 10s.
func (p *Pipeline) criticalBudget(o Options) time.Duration {
	base := o.Timeout
	if base <= 0 {
		base = 10 * time.Second
	}
	if o.Render && p.Renderer != nil && p.RendererTimeout > base {
		base = p.RendererTimeout
	}
	return base
}

// removed duplicate applyContent below this point

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
