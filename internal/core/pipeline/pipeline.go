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
	"errors"
	"html"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/adapters/extractor"
	"github.com/staticvar/fetchmark/internal/adapters/fetcher"
	"github.com/staticvar/fetchmark/internal/core/discovery"
	"github.com/staticvar/fetchmark/internal/core/localartifact"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
	"github.com/staticvar/fetchmark/internal/core/model"
	corerank "github.com/staticvar/fetchmark/internal/core/rank"
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

// RobotsChecker decides whether a user agent may fetch a URL.
type RobotsChecker interface {
	Allowed(ctx context.Context, userAgent, rawURL string) (bool, error)
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

type boundedRenderer interface {
	RenderBounded(ctx context.Context, url string, maxBody int64) ([]byte, error)
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
	Query           string
	URLs            []string
	Engines         []string
	Categories      []string
	Language        string
	TimeRange       string
	SafeSearch      *int
	IncludeDomains  []string
	ExcludeDomains  []string
	ExactMatch      bool
	SearchDepth     string
	ChunksPerSource int
	MaxResults      int
	CandidateCap    int
	RespectRobots   bool
	ProxyURL        string
	UserAgent       string
	Timeout         time.Duration
	Formats         []string
	AdminRequest    bool
	// PreserveURLResults disables content-based deduplication so callers that
	// require one outcome per supplied URL (such as Exa /contents) can map every
	// successful extraction back to its input. Canonical duplicate URLs are
	// still fetched once and may be projected to multiple wire results.
	PreserveURLResults bool
	// Render forces the headless renderer to handle this call, bypassing
	// the first-pass plain fetch only when the extractor flags it as
	// js_required. It is a hint; an absent Renderer or a disabled
	// RendererAuto still results in a plain fetch.
	Render bool
	// forceFresh and retentionIntent are deliberately package-private. Public
	// request translators cannot opt into the operator-controlled curated path.
	forceFresh            bool
	skipOutputReservation bool
	applyRelevanceFloor   bool
	focusedRedirectScope  *fetcher.RedirectScope
	retentionIntent       retentionIntent
	safetyClassification  localcorpus.SafetyClassification
}

type retentionIntent uint8

type existingCorpusWriter interface {
	ReconcileExisting(context.Context, localcorpus.Document) (bool, error)
}

type existingArtifactRevoker interface {
	RevokeExisting(context.Context, string, localcorpus.IndexingDisposition, time.Time) (bool, error)
}

const (
	retentionAutomatic retentionIntent = iota
	retentionCurated
)

// Pipeline wires search, fetch, extract, cache, rank.
type Pipeline struct {
	Searcher search.Searcher
	// DiscoverySources is the trusted, process-configured registry used by
	// basic and advanced search. Basic search runs only original-capable lanes;
	// an empty or advanced-only plan falls back to Searcher as source "primary".
	DiscoverySources          []DiscoverySource
	DiscoveryPlanner          discovery.Planner
	AdvancedSearchConcurrency int
	Fetcher                   Fetcher
	Extractor                 Extractor
	Cache                     Cache
	Ranker                    Ranker
	// LocalCorpus is an opt-in retention sink. Only cold live retrievals are
	// reconciled: cached artifacts are never promoted without fresh robots and
	// X-Robots-Tag evidence.
	LocalCorpus localcorpus.Writer
	// LocalRetentionPolicy distinguishes automatic personal/archive feeding
	// from curated admission. A zero policy with a non-nil writer preserves the
	// pre-policy embedding contract as personal mode.
	LocalRetentionPolicy localcorpus.Policy
	// LocalArtifacts is the durable personal/archive source store used for
	// conditional revalidation. It is intentionally separate from the Bleve
	// discovery projection and the expiring response cache.
	LocalArtifacts localartifact.Store
	// LocalSearcher is the read side of LocalCorpus. When configured, it runs
	// alongside live discovery for both basic and advanced searches.
	LocalSearcher   search.Searcher
	Renderer        Renderer
	RendererAuto    bool
	Robots          RobotsChecker
	RobotsUserAgent string
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

	// ArtifactConcurrency bounds the complete cold artifact lifecycle across
	// concurrent requests: fetch/render, extract, marshal, and cache admission.
	// Zero preserves the historical unbounded behavior for custom/test callers.
	ArtifactConcurrency          int
	MaxArtifactBodyBytes         int64
	MaxArtifactDecompressedBytes int64
	MaxRendererSourceBytes       int64
	MaxRequestSourceBytes        int64
	MaxRequestOutputBytes        int64
	artifactOnce                 sync.Once
	artifactSem                  chan struct{}
	curatedMutationLocks         urlMutationLocks
	localCorpusQuarantined       atomic.Bool
}

// ReasonRequestByteBudget marks an artifact omitted because retaining or
// processing it would exceed the aggregate per-request byte ceiling.
const ReasonRequestByteBudget = "request_byte_budget"

type requestBudgetKey struct{}

var nextRequestBudgetID atomic.Uint64

type requestBudget struct {
	mu        sync.Mutex
	id        uint64
	source    int64
	output    int64
	maxSource int64
	maxOutput int64
	exhausted bool
}

func (b *requestBudget) claimSource(max int64) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if max <= 0 || b.exhausted {
		return 0
	}
	if b.maxSource > 0 {
		remaining := b.maxSource - b.source
		if remaining <= 0 {
			return 0
		}
		if max > remaining {
			max = remaining
		}
	}
	b.source += max
	return max
}

func (b *requestBudget) finishSource(claimed, actual int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if actual > claimed {
		return false
	}
	b.source -= claimed - actual
	return true
}

func (b *requestBudget) addOutput(n int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.maxOutput > 0 && n > b.maxOutput-b.output {
		b.exhausted = true
		return false
	}
	b.output += n
	return true
}

func budgetFromContext(ctx context.Context) *requestBudget {
	budget, _ := ctx.Value(requestBudgetKey{}).(*requestBudget)
	return budget
}

func claimSource(ctx context.Context, max int64) int64 {
	budget := budgetFromContext(ctx)
	if budget == nil {
		return max
	}
	return budget.claimSource(max)
}

func finishSource(ctx context.Context, claimed int64, actual int) bool {
	budget := budgetFromContext(ctx)
	return budget == nil || budget.finishSource(claimed, int64(actual))
}

func minPositive(a, b int64) int64 {
	if a <= 0 {
		return b
	}
	if b <= 0 || a < b {
		return a
	}
	return b
}

func contentBytes(c *model.Content, formats []string) int64 {
	if c == nil {
		return 0
	}
	requested := map[string]bool{}
	for _, format := range formats {
		switch strings.ToLower(strings.TrimSpace(format)) {
		case "markdown", "html", "json":
			requested[strings.ToLower(strings.TrimSpace(format))] = true
		}
	}
	if len(requested) == 0 {
		return int64(len(c.MainText) + len(c.Markdown) + len(c.CleanedHTML))
	}
	var size int
	if requested["json"] {
		size += len(c.MainText)
	}
	if requested["markdown"] {
		size += len(c.Markdown)
	}
	if requested["html"] {
		size += len(c.CleanedHTML)
	}
	return int64(size)
}

func reserveContent(ctx context.Context, c *model.Content, formats []string) bool {
	budget := budgetFromContext(ctx)
	return budget == nil || budget.addOutput(contentBytes(c, formats))
}

func (p *Pipeline) acquireArtifact(ctx context.Context) (func(), error) {
	if p.ArtifactConcurrency <= 0 {
		return func() {}, nil
	}
	p.artifactOnce.Do(func() {
		p.artifactSem = make(chan struct{}, p.ArtifactConcurrency)
	})
	select {
	case p.artifactSem <- struct{}{}:
		return func() { <-p.artifactSem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// SearchOutput keeps discovery evidence separate from post-discovery fetch and
// extraction results while letting native callers retain both.
type SearchOutput struct {
	Results   []model.SearchResult
	Discovery search.DiscoveryReport
}

// Search preserves the original compatibility contract.
func (p *Pipeline) Search(ctx context.Context, o Options) ([]model.SearchResult, error) {
	output, err := p.SearchDetailed(ctx, o)
	return output.Results, err
}

// SearchDetailed runs the full search pipeline and retains the aggregate and
// per-lane discovery report even when discovery returns no results or fails.
func (p *Pipeline) SearchDetailed(ctx context.Context, o Options) (SearchOutput, error) {
	candidateCap := o.CandidateCap
	if candidateCap <= 0 {
		candidateCap = o.MaxResults
	}
	candidates, err := p.searchCandidateSet(ctx, o, candidateCap)
	discoveryReport := search.DiscoveryReport{Status: candidates.status, Lanes: candidates.lanes}
	if discoveryReport.Status == "" {
		if err != nil {
			discoveryReport.Status = search.BatchFailed
		} else if len(candidates.hits) == 0 {
			discoveryReport.Status = search.BatchAuthoritativeEmpty
		} else {
			discoveryReport.Status = search.BatchHealthy
		}
	}
	if err != nil {
		return SearchOutput{Discovery: discoveryReport}, err
	}
	hits := candidates.hits
	if candidateCap > 0 && len(hits) > candidateCap {
		hits = hits[:candidateCap]
	}
	o.applyRelevanceFloor = true
	results := p.process(ctx, o, hitsToResults(hits), o.Query)
	if len(hits) > 0 {
		observeTopKContribution(results)
	}
	filterResultsByFormats(results, o.Formats)
	return SearchOutput{Results: results, Discovery: discoveryReport}, nil
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
			URL:              h.URL,
			Title:            h.Title,
			Snippet:          h.Snippet,
			Engines:          h.Engines,
			PublishedAt:      h.PublishedAt,
			Metadata:         h.Metadata,
			Provenance:       mergeDiscoveryProvenance(nil, h.Provenance),
			ProviderDocument: h.ProviderDocument,
		}
	}
	return out
}

func (p *Pipeline) process(ctx context.Context, o Options, seed []model.SearchResult, query string) []model.SearchResult {
	if p.MaxRequestSourceBytes > 0 || p.MaxRequestOutputBytes > 0 {
		ctx = context.WithValue(ctx, requestBudgetKey{}, &requestBudget{
			id:        nextRequestBudgetID.Add(1),
			maxSource: p.MaxRequestSourceBytes,
			maxOutput: p.MaxRequestOutputBytes,
		})
	}
	seen := make(map[string]int, len(seed))
	results := make([]model.SearchResult, 0, len(seed))
	for _, r := range seed {
		canon, err := cache.CanonicalURL(r.URL)
		if err != nil {
			continue
		}
		if idx, ok := seen[canon]; ok {
			results[idx].Engines = mergeEngines(results[idx].Engines, r.Engines)
			results[idx].Provenance = mergeDiscoveryProvenance(results[idx].Provenance, r.Provenance)
			if results[idx].ProviderDocument == nil {
				results[idx].ProviderDocument = r.ProviderDocument
			}
			syncLegacyRRFProvenance(&results[idx])
			continue
		}
		seen[canon] = len(results)
		r.URL = canon
		results = append(results, r)
	}
	results = filterResultsByDomains(results, o.IncludeDomains, o.ExcludeDomains)

	cacheBypass := o.ProxyURL != "" || o.forceFresh
	renderMode := o.Render && p.Renderer != nil

	// Process each URL independently. In curated mode processOne holds a bounded
	// per-URL mutation lock across robots evaluation, retrieval, and persistence,
	// preserving policy-observation order without serializing unrelated URLs.
	var wg sync.WaitGroup
	for i := range results {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.processOne(ctx, o, &results[i], cacheBypass, renderMode)
		}()
	}
	wg.Wait()

	// Apply retained-output admission once, in stable result order and outside
	// shared cache/singleflight work, so concurrent requests never share budget
	// decisions and completion timing cannot change which results are retained.
	if !o.skipOutputReservation {
		for i := range results {
			if results[i].Content != nil && !reserveContent(ctx, results[i].Content, o.Formats) {
				results[i].Content = nil
				results[i].Markdown = ""
				results[i].HTML = ""
				results[i].Author = ""
				results[i].PublishedAt = nil
				results[i].Chunks = nil
				results[i].Unsupported = ReasonRequestByteBudget
			}
		}
	}

	if !o.PreserveURLResults {
		results = dedupeByContentSHA(results)
	}
	// Rank first, then near-dup collapse. The cluster-winner tiebreak
	// in dedupeNearDuplicates uses SearchResult.Score, which is only
	// meaningful after the ranker has run — otherwise every winner is
	// picked by MainText length and input order, which can drop the
	// more relevant duplicate.
	if p.Ranker != nil && query != "" {
		results = p.Ranker.Score(query, results)
		if o.applyRelevanceFloor {
			results = corerank.FilterLowConfidence(query, results)
		}
	}
	if query != "" && o.ChunksPerSource > 0 {
		attachQueryChunks(results, query, o.ChunksPerSource)
	}
	if !o.PreserveURLResults {
		results = dedupeNearDuplicates(results)
	}
	if o.MaxResults > 0 && len(results) > o.MaxResults {
		results = results[:o.MaxResults]
	}
	return results
}

func (p *Pipeline) processOne(ctx context.Context, o Options, result *model.SearchResult, cacheBypass, renderMode bool) {
	releaseMutation, err := p.acquireCuratedMutation(ctx, result.URL)
	if err != nil {
		result.Unsupported = "fetch_failed"
		return
	}
	defer releaseMutation()

	if result.ProviderDocument != nil {
		p.extractProviderDocument(ctx, result)
		return
	}

	if o.RespectRobots && p.Robots != nil {
		userAgent := o.UserAgent
		if userAgent == "" {
			userAgent = p.RobotsUserAgent
		}
		releaseArtifact, err := p.acquireArtifact(ctx)
		if err != nil {
			result.Unsupported = "fetch_failed"
			return
		}
		allowed, _ := p.Robots.Allowed(ctx, userAgent, result.URL)
		releaseArtifact()
		if !allowed {
			result.Unsupported = fetcher.ReasonRobots
			obs.RobotsBlocks.Inc()
			if o.retentionIntent == retentionCurated {
				if err := p.reconcileCuratedRevocation(ctx, result.URL, localcorpus.DispositionRobotsBlocked, time.Now().UTC()); err != nil {
					result.Unsupported = CuratedReasonStorageFailed
				}
			} else {
				p.tombstoneLocalCorpus(ctx, result.URL, localcorpus.DispositionRobotsBlocked, time.Now().UTC())
			}
			return
		}
	}

	if result.Unsupported != "" {
		return
	}
	// Render requests consult a separate key space so a plain js_required
	// placeholder never shadows a later render.
	if !cacheBypass && p.Cache != nil {
		primaryKey := cache.ArtifactKey(result.URL)
		if renderMode {
			primaryKey = cache.RenderedArtifactKey(result.URL)
		}
		if raw, _ := p.Cache.Get(ctx, primaryKey); raw != nil {
			var content model.Content
			if err := json.Unmarshal(raw, &content); err == nil {
				applyContent(result, &content)
				result.FromCache = true
				obs.CacheEvents.WithLabelValues("fa", "hit").Inc()
				return
			}
		}
		obs.CacheEvents.WithLabelValues("fa", "miss").Inc()
	}
	p.fetchAndExtract(ctx, o, result, cacheBypass)
}

func (p *Pipeline) extractProviderDocument(ctx context.Context, result *model.SearchResult) {
	document := result.ProviderDocument
	result.ProviderDocument = nil
	releaseArtifact, err := p.acquireArtifact(ctx)
	if err != nil {
		result.Unsupported = "extract_failed"
		obs.ExtractOutcome.WithLabelValues("error").Inc()
		return
	}
	defer releaseArtifact()
	if document == nil || len(document.HTML) == 0 || p.Extractor == nil {
		result.Unsupported = "extract_failed"
		obs.ExtractOutcome.WithLabelValues("error").Inc()
		return
	}
	claimed := claimSource(ctx, int64(len(document.HTML)))
	if claimed < int64(len(document.HTML)) {
		_ = finishSource(ctx, claimed, int(claimed))
		result.Unsupported = ReasonRequestByteBudget
		return
	}
	_ = finishSource(ctx, claimed, len(document.HTML))

	var source strings.Builder
	source.Grow(len(document.HTML) + len(result.Title) + 96)
	source.WriteString("<!doctype html><html><head><title>")
	source.WriteString(html.EscapeString(result.Title))
	source.WriteString("</title></head><body><article>")
	source.Write(document.HTML)
	source.WriteString("</article></body></html>")
	content, err := p.Extractor.Extract([]byte(source.String()), result.URL)
	if err != nil || content == nil {
		result.Unsupported = "extract_failed"
		obs.ExtractOutcome.WithLabelValues("error").Inc()
		return
	}
	content.URL = result.URL
	if content.Title == "" {
		content.Title = result.Title
	}
	if content.PublishedAt == nil {
		content.PublishedAt = result.PublishedAt
	}
	if content.Author == "" {
		content.Author = document.Author
	}
	if content.SiteName == "" {
		content.SiteName = document.SiteName
	}
	applyContent(result, content)
	if content.UnsupportedReason != "" {
		obs.ExtractOutcome.WithLabelValues(content.UnsupportedReason).Inc()
		return
	}
	obs.ExtractOutcome.WithLabelValues("ok").Inc()
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
		// Plain text is valid Markdown. If structural Markdown conversion was
		// unavailable but extraction produced a body, preserve useful content
		// instead of returning metadata-only output for a markdown request.
		if keepMarkdown && results[i].Markdown == "" && results[i].Content != nil &&
			strings.TrimSpace(results[i].Content.MainText) != "" {
			results[i].Markdown = results[i].Content.MainText
		}
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

type fetchOutcome struct {
	raw         []byte
	fromCache   bool
	unsupported string
	fetchMS     int64
}

func (p *Pipeline) revalidationCandidate(ctx context.Context, options Options, rawURL string, maxBody int64) (localartifact.Version, bool) {
	if p.LocalArtifacts == nil || options.ProxyURL != "" || options.Render || !options.RespectRobots {
		return localartifact.Version{}, false
	}
	policy := p.LocalRetentionPolicy
	if policy.Mode == "" {
		policy.Mode = localcorpus.ModePersonal
	}
	if policy.Mode != localcorpus.ModePersonal && policy.Mode != localcorpus.ModeArchive {
		return localartifact.Version{}, false
	}
	version, ok, err := p.LocalArtifacts.Current(ctx, rawURL)
	if err != nil || !ok || (version.ETag == "" && version.LastModified == "") {
		return localartifact.Version{}, false
	}
	expectedAgent := options.UserAgent
	if expectedAgent == "" {
		expectedAgent = p.RobotsUserAgent
	}
	if version.PolicyAgent == "" || version.PolicyAgent != expectedAgent {
		return localartifact.Version{}, false
	}
	canonicalEffective, err := cache.CanonicalURL(version.EffectiveURL)
	if err != nil || canonicalEffective != rawURL {
		return localartifact.Version{}, false
	}
	if maxBody > 0 && int64(len(version.Body)) > maxBody {
		return localartifact.Version{}, false
	}
	return version, true
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

		sourceLimit := p.MaxArtifactDecompressedBytes
		if p.MaxArtifactBodyBytes > sourceLimit {
			sourceLimit = p.MaxArtifactBodyBytes
		}
		if sourceLimit <= 0 {
			sourceLimit = p.MaxRequestSourceBytes
		}
		claimed := int64(0)
		if budgetFromContext(ctx) != nil {
			claimed = claimSource(ctx, sourceLimit)
			if claimed <= 0 {
				r.Unsupported = ReasonRequestByteBudget
				return nil, nil
			}
		}
		req := fetcher.Request{
			URL:                  r.URL,
			ProxyURL:             o.ProxyURL,
			UserAgent:            o.UserAgent,
			RespectRobots:        o.RespectRobots,
			Timeout:              o.Timeout,
			MaxBodyBytes:         minPositive(p.MaxArtifactBodyBytes, claimed),
			MaxDecompressedBytes: minPositive(p.MaxArtifactDecompressedBytes, claimed),
			MaxTotalBytes:        claimed,
			FocusedRedirectScope: o.focusedRedirectScope,
		}
		prior, hasPrior := p.revalidationCandidate(ctx, o, r.URL, minPositive(sourceLimit, claimed))
		if hasPrior {
			req.IfNoneMatch = prior.ETag
			req.IfModifiedSince = prior.LastModified
		}
		fr := p.Fetcher.Fetch(ctx, req)
		if fr.NotModified && hasPrior {
			latest, ok, currentErr := p.LocalArtifacts.Current(ctx, r.URL)
			if currentErr != nil || !ok || latest.ContentHash != prior.ContentHash {
				hasPrior = false
			}
		}
		if fr.NotModified && !hasPrior {
			// An unsolicited or stale 304 has no usable representation. Retry
			// exactly once without validators; extraction must never see an
			// empty 304 body.
			conditionalBytes := fr.BytesRead
			req.IfNoneMatch = ""
			req.IfModifiedSince = ""
			if req.MaxTotalBytes > 0 {
				if conditionalBytes >= req.MaxTotalBytes {
					finishSource(ctx, claimed, int(conditionalBytes))
					r.Unsupported = ReasonRequestByteBudget
					return nil, nil
				}
				req.MaxTotalBytes -= conditionalBytes
			}
			fr = p.Fetcher.Fetch(ctx, req)
			fr.BytesRead += conditionalBytes
		}
		observedAt := fetchObservationTime(fr)
		headerDisposition := localcorpus.EvaluateDisposition(fr.RobotsAllowed, fr.RobotsAuthoritative, fr.XRobotsTag, nil, fr.UAUsed)
		headerRevokesRetention := headerDisposition == localcorpus.DispositionNoIndexHeader ||
			headerDisposition == localcorpus.DispositionNoArchiveHeader
		// Record fetch-side outcome on the result; Err + Unsupported
		// cases short-circuit the rest of the pipeline for this URL.
		if fr.Err != nil {
			finishSource(ctx, claimed, int(fr.BytesRead))
			r.Unsupported = "fetch_failed"
			r.FetchMS = fr.FetchMS
			obs.FetchOutcome.WithLabelValues("error").Inc()
			return nil, fr.Err
		}
		if fr.Status == 404 || fr.Status == 410 {
			disposition := localcorpus.DispositionUnknown
			if headerRevokesRetention {
				disposition = headerDisposition
			}
			if o.retentionIntent == retentionCurated {
				if err := p.reconcileCuratedRevocation(ctx, r.URL, disposition, observedAt); err != nil {
					r.Unsupported = CuratedReasonStorageFailed
				}
			} else {
				p.tombstoneLocalCorpus(ctx, r.URL, disposition, observedAt)
			}
			finishSource(ctx, claimed, int(fr.BytesRead))
			if r.Unsupported == "" {
				r.Unsupported = "fetch_failed"
			}
			r.FetchMS = fr.FetchMS
			obs.FetchOutcome.WithLabelValues("error").Inc()
			return nil, errors.New("upstream returned a terminal missing status")
		}
		if fr.Unsupported != "" {
			finishSource(ctx, claimed, int(fr.BytesRead))
			r.Unsupported = fr.Unsupported
			r.FetchMS = fr.FetchMS
			obs.FetchOutcome.WithLabelValues(fr.Unsupported).Inc()
			revocation := localcorpus.IndexingDisposition("")
			if fr.Unsupported == fetcher.ReasonRobots {
				revocation = localcorpus.DispositionRobotsBlocked
			} else if fr.Status == 200 && headerRevokesRetention {
				revocation = headerDisposition
			} else if fr.Unsupported != fetcher.ReasonRequestBudget && fr.Status == 200 {
				revocation = localcorpus.DispositionUnknown
			}
			if revocation != "" {
				if o.retentionIntent == retentionCurated {
					if err := p.reconcileCuratedRevocation(ctx, r.URL, revocation, observedAt); err != nil {
						r.Unsupported = CuratedReasonStorageFailed
					}
				} else {
					p.tombstoneLocalCorpus(ctx, r.URL, revocation, observedAt)
				}
			}
			return nil, nil
		}
		if fr.NotModified {
			if headerDisposition != localcorpus.DispositionPermitted {
				p.tombstoneLocalCorpus(ctx, r.URL, headerDisposition, observedAt)
				finishSource(ctx, claimed, int(fr.BytesRead))
				r.Unsupported = retentionUnsupportedReason(headerDisposition)
				return nil, nil
			}
			if !hasPrior || len(prior.Body) == 0 {
				finishSource(ctx, claimed, int(fr.BytesRead))
				r.Unsupported = "fetch_failed"
				return nil, errors.New("conditional response has no retained representation")
			}
			if !finishSource(ctx, claimed, int(fr.BytesRead)+len(prior.Body)) {
				r.Unsupported = ReasonRequestByteBudget
				return nil, nil
			}
			reused := fr
			reused.Body = append([]byte(nil), prior.Body...)
			reused.ContentType = prior.MIME
			reused.FinalURL = prior.EffectiveURL
			disposition := localcorpus.EvaluateDisposition(
				reused.RobotsAllowed, reused.RobotsAuthoritative, reused.XRobotsTag, reused.Body, reused.UAUsed,
			)
			if disposition != localcorpus.DispositionPermitted {
				p.tombstoneLocalCorpus(ctx, r.URL, disposition, observedAt)
				r.Unsupported = retentionUnsupportedReason(disposition)
				return nil, nil
			}
			c, err := p.Extractor.Extract(reused.Body, extractionBaseURL(r.URL, reused.FinalURL))
			if c != nil {
				// EffectiveURL is extraction context for relative metadata only;
				// the public content identity remains the URL the caller supplied.
				c.URL = r.URL
				if !localcorpus.AllowsLinkFollowing(reused.XRobotsTag, reused.Body, reused.UAUsed) {
					c.OutboundLinks = nil
				}
			}
			if err != nil || c == nil {
				r.Unsupported = "extract_failed"
				obs.ExtractOutcome.WithLabelValues("error").Inc()
				return nil, err
			}
			if c.UnsupportedReason != "" || strings.TrimSpace(c.MainText) == "" {
				p.tombstoneLocalCorpus(ctx, r.URL, localcorpus.DispositionUnknown, observedAt)
				r.Unsupported = c.UnsupportedReason
				return nil, nil
			}
			document, retain := p.automaticRetention(localDocument(r, c, reused, localcorpus.DispositionPermitted))
			if !retain {
				return nil, errors.New("conditional response is not permitted by local retention policy")
			}
			artifactCtx, artifactCancel := localArtifactMutationContext(ctx)
			err = p.LocalArtifacts.Revalidate(artifactCtx, localartifact.Observation{
				URL: r.URL, ObservedAt: observedAt, ValidatedAt: time.Now().UTC(),
				ETag: fr.ETag, LastModified: fr.LastModified, ExpiresAt: document.ExpiresAt,
			})
			artifactCancel()
			if err != nil {
				obs.LocalArtifactMutationTotal.WithLabelValues("error").Inc()
				return nil, err
			}
			obs.LocalArtifactMutationTotal.WithLabelValues("revalidated").Inc()
			if p.LocalCorpus != nil {
				mutationCtx, mutationCancel := localCorpusMutationContext(ctx)
				err = p.LocalCorpus.Reconcile(mutationCtx, document)
				mutationCancel()
				if err != nil {
					obs.LocalIndexMutationTotal.WithLabelValues("error").Inc()
					return nil, err
				}
			}
			blob, marshalErr := json.Marshal(c)
			if marshalErr != nil {
				return nil, marshalErr
			}
			applyContent(r, c)
			r.FromCache = true
			r.FetchMS = fr.FetchMS
			if !cacheBypass && p.Cache != nil {
				if setErr := p.Cache.Set(ctx, key, blob); setErr == nil {
					obs.CacheEvents.WithLabelValues("fa", "write").Inc()
				}
			}
			obs.FetchOutcome.WithLabelValues("not_modified").Inc()
			obs.ExtractOutcome.WithLabelValues("ok").Inc()
			return blob, nil
		}
		if fr.Status != 0 && fr.Status != 200 {
			finishSource(ctx, claimed, int(fr.BytesRead))
			r.Unsupported = "fetch_failed"
			r.FetchMS = fr.FetchMS
			obs.FetchOutcome.WithLabelValues("error").Inc()
			return nil, errors.New("upstream returned an unsupported status")
		}
		if headerRevokesRetention {
			if o.retentionIntent == retentionCurated {
				if err := p.reconcileCuratedRevocation(ctx, r.URL, headerDisposition, observedAt); err != nil {
					r.Unsupported = CuratedReasonStorageFailed
				} else {
					r.Unsupported = retentionUnsupportedReason(headerDisposition)
				}
				finishSource(ctx, claimed, int(fr.BytesRead))
				return nil, nil
			}
			p.tombstoneLocalCorpus(ctx, r.URL, headerDisposition, observedAt)
		}
		consumed := fr.BytesRead
		if consumed <= 0 {
			// Preserve compatibility with custom Fetcher implementations that
			// predate byte accounting while never undercharging a body.
			consumed = int64(len(fr.Body))
		}
		if !finishSource(ctx, claimed, int(consumed)) {
			r.Unsupported = ReasonRequestByteBudget
			return nil, nil
		}
		r.FetchMS = fr.FetchMS
		obs.FetchOutcome.WithLabelValues("ok").Inc()
		obs.FetchDuration.Observe(float64(fr.FetchMS) / 1000.0)

		c, err := p.Extractor.Extract(fr.Body, extractionBaseURL(r.URL, fr.FinalURL))
		if c != nil {
			c.URL = r.URL
			if !localcorpus.AllowsLinkFollowing(fr.XRobotsTag, fr.Body, fr.UAUsed) {
				c.OutboundLinks = nil
			}
		}
		if err != nil || c == nil {
			if !headerRevokesRetention {
				if o.retentionIntent == retentionCurated {
					if mutationErr := p.reconcileCuratedRevocation(ctx, r.URL, localcorpus.DispositionUnknown, observedAt); mutationErr != nil {
						r.Unsupported = CuratedReasonStorageFailed
					}
				} else {
					p.tombstoneLocalCorpus(ctx, r.URL, localcorpus.DispositionUnknown, observedAt)
				}
			}
			if r.Unsupported == "" {
				r.Unsupported = "extract_failed"
			}
			obs.ExtractOutcome.WithLabelValues("error").Inc()
			return nil, err
		}
		if c.UnsupportedReason != "" {
			obs.ExtractOutcome.WithLabelValues(c.UnsupportedReason).Inc()
		} else {
			obs.ExtractOutcome.WithLabelValues("ok").Inc()
		}
		if headerDisposition == localcorpus.DispositionPermitted || headerDisposition == localcorpus.DispositionUnknown {
			disposition, retained, retentionErr := p.reconcileLocalCorpus(ctx, o.retentionIntent, o.safetyClassification, r, c, fr)
			if o.retentionIntent == retentionCurated &&
				(retentionErr != nil || disposition != localcorpus.DispositionPermitted || !retained) {
				r.Unsupported = curatedUnsupportedReason(disposition, c.UnsupportedReason, retentionErr)
				return nil, retentionErr
			}
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

		// Automatic render upgrade: when the first-pass extractor flags
		// js_required or retains metadata without any usable body, try the
		// headless service. The plain blob is kept
		// under the plain key so future non-render requests hit cache;
		// the rendered blob is stored under a separate key so it does
		// not clobber the cheap path.
		if o.retentionIntent != retentionCurated && p.Renderer != nil && p.RendererAuto &&
			contentNeedsBrowser(c) {
			if _, rerr := p.tryAutoRender(ctx, o, r, cacheBypass); rerr == nil {
				// r already updated in place by tryAutoRender.
			}
		}
		return blob, nil
	}

	// Fast path: no cache configured (bypass or nil) just runs
	// directly.
	if cacheBypass || p.Cache == nil {
		releaseArtifact, err := p.acquireArtifact(ctx)
		if err != nil {
			return
		}
		defer releaseArtifact()
		_, _ = doFetch(ctx)
		return
	}

	// Local singleflight by key suppresses duplicate in-flight workers
	// inside this process. Cross-instance suppression happens inside
	// doFetch via WithLock.
	flightKey := key
	if budget := budgetFromContext(ctx); budget != nil {
		// Budget decisions are request-local and must never be shared with a
		// concurrent request that has different remaining capacity.
		flightKey += ":request:" + strconv.FormatUint(budget.id, 10)
	}
	v, _, _ := p.Cache.Do(flightKey, func() (any, error) {
		// Recheck cache once more — singleflight may have raced us.
		if raw, _ := p.Cache.Get(ctx, key); raw != nil {
			if err := applyRaw(ctx, r, raw); err == nil {
				r.FromCache = true
			}
			return fetchOutcome{raw: raw, fromCache: true}, nil
		}
		// Acquire process capacity before the distributed lock so queued work
		// cannot hold a Redis lock until it expires without doing useful work.
		releaseArtifact, err := p.acquireArtifact(ctx)
		if err != nil {
			return fetchOutcome{unsupported: r.Unsupported, fetchMS: r.FetchMS}, nil
		}
		defer releaseArtifact()
		// Cross-process lock. LockTTL is generous relative to our
		// fetch+extract budget so a slow fetcher doesn't lose the lock
		// mid-request.
		raw, err := p.Cache.WithLock(ctx, key, cache.LockOptions{
			LockTTL:      p.lockTTL(o),
			WaitMax:      p.lockWait(o),
			PollInterval: 100 * time.Millisecond,
		}, doFetch)
		if err != nil {
			return fetchOutcome{unsupported: r.Unsupported, fetchMS: r.FetchMS}, nil
		}
		// If another worker/process populated the cache while we
		// waited, the lock path returned that blob without calling our
		// fetcher — reflect it on r.
		if r.Content == nil && raw != nil {
			if err := applyRaw(ctx, r, raw); err == nil {
				r.FromCache = true
			}
		}
		return fetchOutcome{raw: raw, fromCache: r.FromCache, unsupported: r.Unsupported, fetchMS: r.FetchMS}, nil
	})
	applyFetchOutcome(ctx, r, v)
}

func contentNeedsBrowser(content *model.Content) bool {
	if content == nil {
		return true
	}
	if content.UnsupportedReason == extractor.ReasonJSRequired {
		return true
	}
	return content.UnsupportedReason == "" && strings.TrimSpace(content.MainText) == "" &&
		strings.TrimSpace(content.Markdown) == ""
}

func applyFetchOutcome(ctx context.Context, r *model.SearchResult, v any) {
	out, ok := v.(fetchOutcome)
	if !ok {
		if raw, ok := v.([]byte); ok {
			out.raw = raw
		} else {
			return
		}
	}
	if out.unsupported != "" && r.Unsupported == "" && r.Content == nil {
		r.Unsupported = out.unsupported
		r.FetchMS = out.fetchMS
		return
	}
	if out.raw != nil && r.Content == nil && r.Unsupported == "" {
		if err := applyRaw(ctx, r, out.raw); err == nil {
			r.FromCache = out.fromCache
		}
	}
}

func (p *Pipeline) reconcileLocalCorpus(ctx context.Context, intent retentionIntent, classification localcorpus.SafetyClassification, result *model.SearchResult, content *model.Content, fetched fetcher.Result) (localcorpus.IndexingDisposition, bool, error) {
	if p.LocalCorpus == nil && p.LocalArtifacts == nil {
		return localcorpus.DispositionUnknown, false, errors.New("local corpus is not configured")
	}
	disposition := localcorpus.EvaluateDisposition(
		fetched.RobotsAllowed, fetched.RobotsAuthoritative, fetched.XRobotsTag, fetched.Body, fetched.UAUsed,
	)
	if disposition == localcorpus.DispositionPermitted && (content.UnsupportedReason != "" || strings.TrimSpace(content.MainText) == "") {
		disposition = localcorpus.DispositionUnknown
	}
	document := localDocument(result, content, fetched, disposition)
	document.SafetyClassification = classification
	document, retain := p.retention(document, intent)
	if !retain {
		return disposition, false, nil
	}
	if intent == retentionAutomatic && p.LocalRetentionPolicy.Mode == localcorpus.ModeCurated &&
		disposition != localcorpus.DispositionPermitted {
		// Ordinary traffic may revoke retained curated state, but it must not
		// create durable tombstones for attacker-selected denied URLs. Reconcile
		// the two stores independently so artifact-only crash/rebuild state is
		// still purged when the index has no matching document.
		_ = p.reconcileExistingCuratedRevocation(ctx, document)
		return disposition, false, nil
	}
	if intent == retentionCurated && disposition == localcorpus.DispositionPermitted &&
		(p.LocalCorpus == nil || p.LocalArtifacts == nil) {
		return disposition, false, errors.New("curated retention requires both local stores")
	}
	if disposition == localcorpus.DispositionPermitted && p.LocalArtifacts != nil {
		artifactCtx, artifactCancel := localArtifactMutationContext(ctx)
		defer artifactCancel()
		if err := p.LocalArtifacts.Put(artifactCtx, localartifact.Version{
			URL: result.URL, EffectiveURL: fetched.FinalURL, Body: append([]byte(nil), fetched.Body...),
			ETag: fetched.ETag, LastModified: fetched.LastModified, MIME: fetched.ContentType,
			PolicyAgent: fetched.UAUsed, FetchedAt: document.FetchedAt, ObservedAt: document.FetchedAt,
			ValidatedAt: document.FetchedAt, ExpiresAt: document.ExpiresAt,
			SafetyClassification: document.SafetyClassification,
			IndexingDisposition:  disposition,
		}); err != nil {
			obs.LocalArtifactMutationTotal.WithLabelValues("error").Inc()
			if intent == retentionCurated {
				rollbackErr := p.failClosedCuratedRetention(ctx, result.URL, document.FetchedAt)
				return disposition, false, errors.Join(err, rollbackErr)
			}
			return disposition, false, err
		}
		obs.LocalArtifactMutationTotal.WithLabelValues("stored").Inc()
	}
	if p.LocalCorpus == nil {
		return disposition, disposition == localcorpus.DispositionPermitted, nil
	}
	mutationCtx, cancel := localCorpusMutationContext(ctx)
	defer cancel()
	reconcileErr := p.LocalCorpus.Reconcile(mutationCtx, document)
	if reconcileErr != nil {
		obs.LocalIndexMutationTotal.WithLabelValues("error").Inc()
		if disposition == localcorpus.DispositionPermitted {
			if intent == retentionCurated {
				// The artifact is written first so the index never points at a
				// missing source. If indexing fails, fail closed by replacing any
				// older searchable projection with an unknown tombstone and
				// revoking the just-written source.
				rollbackErr := p.failClosedCuratedRetention(ctx, result.URL, document.FetchedAt)
				return disposition, false, errors.Join(reconcileErr, rollbackErr)
			}
			return disposition, false, reconcileErr
		}
	}
	if disposition != localcorpus.DispositionPermitted {
		p.revokeLocalArtifact(ctx, result.URL, disposition, document.FetchedAt)
		if intent == retentionCurated && reconcileErr != nil {
			return disposition, false, reconcileErr
		}
		if reconcileErr == nil {
			obs.LocalIndexMutationTotal.WithLabelValues("removed").Inc()
		}
		return disposition, false, nil
	}
	obs.LocalIndexMutationTotal.WithLabelValues("indexed").Inc()
	return disposition, true, nil
}

func localDocument(result *model.SearchResult, content *model.Content, fetched fetcher.Result, disposition localcorpus.IndexingDisposition) localcorpus.Document {
	document := localcorpus.Document{
		URL: result.URL, FetchedAt: fetchObservationTime(fetched), IndexingDisposition: disposition,
	}
	if disposition != localcorpus.DispositionPermitted {
		return document
	}
	document.Title = content.Title
	document.Headings = append([]string(nil), content.Headings...)
	document.Body = content.MainText
	document.Language = content.Language
	document.Author = content.Author
	document.PublishedAt = content.PublishedAt
	contentHash := sha256.Sum256(fetched.Body)
	document.ContentHash = hex.EncodeToString(contentHash[:])
	document.OutboundLinks = append([]string(nil), content.OutboundLinks...)
	document.Provenance = append([]string(nil), result.Engines...)
	document.MIME = fetched.ContentType
	document.ExtractionStatus = "ok"
	return document
}

func (p *Pipeline) tombstoneLocalCorpus(ctx context.Context, rawURL string, disposition localcorpus.IndexingDisposition, observedAt time.Time) {
	if p.LocalCorpus == nil && p.LocalArtifacts == nil {
		return
	}
	document, retain := p.automaticRetention(localcorpus.Document{
		URL: rawURL, FetchedAt: observedAt, IndexingDisposition: disposition,
	})
	if !retain {
		return
	}
	if p.LocalRetentionPolicy.Mode == localcorpus.ModeCurated {
		_ = p.reconcileExistingCuratedRevocation(ctx, document)
		return
	}
	if p.LocalCorpus != nil {
		mutationCtx, cancel := localCorpusMutationContext(ctx)
		defer cancel()
		err := p.LocalCorpus.Reconcile(mutationCtx, document)
		if err != nil {
			obs.LocalIndexMutationTotal.WithLabelValues("error").Inc()
			p.revokeLocalArtifact(ctx, rawURL, disposition, observedAt)
			return
		}
		obs.LocalIndexMutationTotal.WithLabelValues("removed").Inc()
	}
	p.revokeLocalArtifact(ctx, rawURL, disposition, observedAt)
}

func (p *Pipeline) revokeLocalArtifact(ctx context.Context, rawURL string, disposition localcorpus.IndexingDisposition, observedAt time.Time) {
	if p.LocalArtifacts == nil {
		return
	}
	mutationCtx, cancel := localArtifactMutationContext(ctx)
	defer cancel()
	if err := p.LocalArtifacts.Revoke(mutationCtx, rawURL, disposition, observedAt); err != nil {
		obs.LocalArtifactMutationTotal.WithLabelValues("error").Inc()
		return
	}
	obs.LocalArtifactMutationTotal.WithLabelValues("revoked").Inc()
}

func localArtifactMutationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
}

func (p *Pipeline) automaticRetention(document localcorpus.Document) (localcorpus.Document, bool) {
	policy := p.LocalRetentionPolicy
	if policy.Mode == "" && p.LocalCorpus != nil {
		policy = localcorpus.Policy{Mode: localcorpus.ModePersonal, MaxAge: localcorpus.DefaultPersonalMaxAge}
	}
	return policy.ApplyAutomatic(document)
}

func (p *Pipeline) retention(document localcorpus.Document, intent retentionIntent) (localcorpus.Document, bool) {
	policy := p.LocalRetentionPolicy
	if intent == retentionCurated {
		return policy.ApplyExplicit(document)
	}
	return p.automaticRetention(document)
}

func fetchObservationTime(fetched fetcher.Result) time.Time {
	if fetched.ObservedAt.IsZero() {
		return time.Now().UTC()
	}
	return fetched.ObservedAt.UTC()
}

func extractionBaseURL(requestedURL, effectiveURL string) string {
	candidate := strings.TrimSpace(effectiveURL)
	parsed, err := url.Parse(candidate)
	if candidate == "" || err != nil || parsed.Hostname() == "" || parsed.User != nil ||
		(!strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https")) {
		return requestedURL
	}
	return candidate
}

func retentionUnsupportedReason(disposition localcorpus.IndexingDisposition) string {
	if disposition == localcorpus.DispositionNoArchiveHeader || disposition == localcorpus.DispositionNoArchiveMetadata {
		return "noarchive"
	}
	if disposition == localcorpus.DispositionRobotsBlocked {
		return fetcher.ReasonRobots
	}
	return "noindex"
}

func localCorpusMutationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
}

// renderAndExtract runs the renderer as the primary source of HTML,
// extracts content, and writes the result to the supplied cache key.
// Used for explicit render=true requests.
func (p *Pipeline) renderAndExtract(ctx context.Context, o Options, r *model.SearchResult, key string, cacheBypass bool) ([]byte, error) {
	observedAt := time.Now().UTC()
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
	sourceLimit := p.MaxRendererSourceBytes
	if sourceLimit <= 0 {
		sourceLimit = p.MaxRequestSourceBytes
	}
	claimed := int64(0)
	if budgetFromContext(ctx) != nil {
		claimed = claimSource(ctx, sourceLimit)
		if claimed <= 0 {
			r.Unsupported = ReasonRequestByteBudget
			return nil, nil
		}
	}
	var raw []byte
	var err error
	if bounded, ok := p.Renderer.(boundedRenderer); ok && claimed > 0 {
		raw, err = bounded.RenderBounded(ctx, r.URL, claimed)
	} else {
		raw, err = p.Renderer.Render(ctx, r.URL)
	}
	if err != nil {
		// The renderer adapter cannot report partial browser/network work, so
		// conservatively consume the full claim on failure.
		finishSource(ctx, claimed, int(claimed))
		r.Unsupported = "render_failed"
		obs.RendererOutcome.WithLabelValues("error").Inc()
		return nil, err
	}
	if !finishSource(ctx, claimed, len(raw)) {
		r.Unsupported = ReasonRequestByteBudget
		return nil, nil
	}
	obs.RendererOutcome.WithLabelValues("ok").Inc()
	obs.RendererDuration.Observe(time.Since(start).Seconds())
	r.FetchMS = int64(time.Since(start) / time.Millisecond)
	// Renderer results can never grant retention because response headers are
	// unavailable. Explicit HTML noindex remains sufficient to revoke a prior
	// document; absence of that signal leaves existing state unchanged.
	metadataDisposition := localcorpus.EvaluateDisposition(true, true, nil, raw, o.UserAgent)
	if metadataDisposition == localcorpus.DispositionNoIndexMetadata || metadataDisposition == localcorpus.DispositionNoArchiveMetadata {
		p.tombstoneLocalCorpus(ctx, r.URL, metadataDisposition, observedAt)
	}

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
	applyCached := func(raw []byte) bool {
		var c model.Content
		if err := json.Unmarshal(raw, &c); err != nil || c.UnsupportedReason == extractor.ReasonJSRequired {
			return false
		}
		applyRenderedContent(r, &c)
		r.FromCache = true
		return true
	}
	if !cacheBypass && p.Cache != nil {
		if raw, _ := p.Cache.Get(ctx, renderedKey); raw != nil && applyCached(raw) {
			obs.CacheEvents.WithLabelValues("fa", "hit").Inc()
			return raw, nil
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

	renderedLocally := false
	raw, err := p.Cache.WithLock(ctx, renderedKey, cache.LockOptions{
		LockTTL:      p.lockTTL(ro),
		WaitMax:      p.lockWait(ro),
		PollInterval: 100 * time.Millisecond,
	}, func(ctx context.Context) ([]byte, error) {
		// Re-check inside the lock — a peer may have populated the
		// rendered cache while we were waiting. If so, reuse it.
		if raw, _ := p.Cache.Get(ctx, renderedKey); raw != nil && applyCached(raw) {
			obs.CacheEvents.WithLabelValues("fa", "hit").Inc()
			return raw, nil
		}
		rendered, renderErr := p.renderAndExtract(ctx, o, r, renderedKey, cacheBypass)
		if renderErr == nil {
			renderedLocally = true
		}
		return rendered, renderErr
	})
	if err != nil {
		return nil, err
	}
	// The WithLock path returns either the blob produced by our fn or
	// one populated by a peer while we waited. In the peer-populated
	// case our fn did not run, so r still carries the js_required plain
	// content from the earlier fetch — unconditionally re-apply so the
	// caller sees the rendered upgrade.
	if raw != nil && !renderedLocally && !r.FromCache {
		applyCached(raw)
	}
	return raw, nil
}

// applyRaw decodes a cached artifact blob onto r. It is the symmetric
// counterpart of the Cache.Set call inside doFetch.
func applyRaw(ctx context.Context, r *model.SearchResult, raw []byte) error {
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
	seen := map[string]int{}
	out := in[:0]
	for _, r := range in {
		if r.Content != nil && r.Content.MainText != "" {
			h := sha256.Sum256([]byte(normalizeText(r.Content.MainText)))
			key := hex.EncodeToString(h[:])
			if winner, dup := seen[key]; dup {
				out[winner].Provenance = mergeDiscoveryProvenance(out[winner].Provenance, r.Provenance)
				syncLegacyRRFProvenance(&out[winner])
				continue
			}
			seen[key] = len(out)
		}
		out = append(out, r)
	}
	return out
}

func normalizeText(s string) string {
	return strings.Join(strings.Fields(strings.ToLower(s)), " ")
}

func mergeEngines(a, b []string) []string {
	return mergeEnginesStable(a, b)
}
