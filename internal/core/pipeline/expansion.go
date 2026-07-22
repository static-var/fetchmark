package pipeline

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/core/discovery"
	"github.com/staticvar/fetchmark/internal/core/model"
	corerank "github.com/staticvar/fetchmark/internal/core/rank"
	"github.com/staticvar/fetchmark/internal/core/search"
	"github.com/staticvar/fetchmark/internal/obs"
)

const rrfRankConstant = 60

const (
	maxDiscoveryDiagnosticsPerLane = search.MaxDiscoveryDiagnosticsPerLane
	maxDiscoveryRetryAfter         = search.MaxDiscoveryRetryAfter
)

type queryVariant struct {
	label  string
	weight float64
	query  search.Query
}

type variantHits struct {
	label    string
	provider string
	lane     string
	weight   float64
	hits     []search.Hit
}

// DiscoverySource remains an alias for callers constructing a static advanced
// registry. New wiring should prefer discovery.Planner source packs.
type DiscoverySource = discovery.Source

type discoveryLane struct {
	provider string
	lane     string
	variant  string
	weight   float64
	query    search.Query
	searcher search.Searcher
	timeout  time.Duration
}

type laneOutcome struct {
	lane     discoveryLane
	batch    search.SearchBatch
	err      error
	duration time.Duration
}

type candidateSet struct {
	hits   []search.Hit
	status search.BatchStatus
	lanes  []search.DiscoveryLaneReport
}

func (p *Pipeline) searchCandidates(ctx context.Context, o Options, candidateCap int) ([]search.Hit, error) {
	candidates, err := p.searchCandidateSet(ctx, o, candidateCap)
	return candidates.hits, err
}

func (p *Pipeline) searchCandidateSet(ctx context.Context, o Options, candidateCap int) (candidateSet, error) {
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
		SearchDepth:    o.SearchDepth,
		MaxResults:     candidateCap,
	}
	if !isAdvancedSearchDepth(o.SearchDepth) {
		if p.DiscoveryPlanner != nil || len(p.DiscoverySources) > 0 || p.availableLocalSearcher() != nil {
			lanes, planErr := p.basicLanes(base)
			if planErr != nil {
				return candidateSet{}, planErr
			}
			concurrency := p.AdvancedSearchConcurrency
			if concurrency <= 0 {
				concurrency = 1
			}
			return p.executeDiscoveryPlan(ctx, lanes, concurrency, candidateCap, base.Q)
		}
		started := time.Now()
		hits, err := p.Searcher.Search(ctx, base)
		status := search.BatchHealthy
		if err != nil {
			status = search.BatchFailed
		} else if len(hits) == 0 {
			status = search.BatchAuthoritativeEmpty
		}
		outcome := laneOutcome{
			lane:  discoveryLane{provider: "primary", lane: "primary", variant: "original"},
			batch: search.SearchBatch{Hits: hits, Status: status}, err: err, duration: time.Since(started),
		}
		return candidateSet{hits: hits, status: status, lanes: []search.DiscoveryLaneReport{discoveryLaneReport(outcome)}}, err
	}

	lanes, err := p.advancedLanes(base, advancedQueryVariants(base))
	if err != nil {
		return candidateSet{}, err
	}
	return p.executeDiscoveryPlan(ctx, lanes, p.AdvancedSearchConcurrency, candidateCap, base.Q)
}

// executeDiscoveryPlan gives the configured Scrapling primary the first chance
// to answer. Existing open providers remain a fallback when the primary is
// degraded, empty, or has no result that passes Fetchmark's lexical confidence
// policy. Other primary providers retain the existing parallel broker behavior.
func (p *Pipeline) executeDiscoveryPlan(ctx context.Context, lanes []discoveryLane, concurrency, candidateCap int, query string) (candidateSet, error) {
	primaryProvider := ""
	if planner, ok := p.DiscoveryPlanner.(interface{ PrimarySource() discovery.Source }); ok {
		primary := planner.PrimarySource()
		if provenanceProvider(primary) == "scrapling" {
			primaryProvider = "scrapling"
		}
	}
	if primaryProvider == "" {
		return collectLaneOutcomes(ctx, executeDiscoveryLanes(ctx, lanes, concurrency), candidateCap)
	}

	primaryLanes := make([]discoveryLane, 0, len(lanes))
	secondaryLanes := make([]discoveryLane, 0, len(lanes))
	for _, lane := range lanes {
		if lane.provider == primaryProvider {
			primaryLanes = append(primaryLanes, lane)
		} else {
			secondaryLanes = append(secondaryLanes, lane)
		}
	}
	if len(primaryLanes) == 0 || len(secondaryLanes) == 0 {
		return collectLaneOutcomes(ctx, executeDiscoveryLanes(ctx, lanes, concurrency), candidateCap)
	}

	primaryOutcomes := executeDiscoveryLanes(ctx, primaryLanes, concurrency)
	primaryCandidates, primaryErr := collectLaneOutcomes(ctx, primaryOutcomes, candidateCap)
	primaryFillsWindow := candidateCap <= 0 || len(primaryCandidates.hits) >= candidateCap
	if primaryErr == nil && primaryFillsWindow && primaryCandidatesRelevant(query, primaryCandidates) {
		return primaryCandidates, nil
	}
	secondaryOutcomes := executeDiscoveryLanes(ctx, secondaryLanes, concurrency)
	return collectLaneOutcomes(ctx, append(primaryOutcomes, secondaryOutcomes...), candidateCap)
}

func primaryCandidatesRelevant(query string, candidates candidateSet) bool {
	if len(candidates.hits) == 0 || (candidates.status != search.BatchHealthy && candidates.status != search.BatchPartial) {
		return false
	}
	results := make([]model.SearchResult, 0, len(candidates.hits))
	for _, hit := range candidates.hits {
		results = append(results, model.SearchResult{
			URL: hit.URL, Title: hit.Title, Snippet: hit.Snippet, Engines: append([]string(nil), hit.Engines...),
			Provenance: append([]model.DiscoveryProvenance(nil), hit.Provenance...),
		})
	}
	results = corerank.New().Score(query, results)
	return len(corerank.FilterLowConfidence(query, results)) > 0
}

func collectLaneOutcomes(ctx context.Context, outcomes []laneOutcome, candidateCap int) (candidateSet, error) {
	contextErr := ctx.Err()
	all := make([]variantHits, 0, len(outcomes))
	reports := make([]search.DiscoveryLaneReport, 0, len(outcomes))
	var firstErr error
	degraded := false
	authoritative := false
	failed := false
	for _, outcome := range outcomes {
		observeLaneOutcome(outcome)
		reports = append(reports, discoveryLaneReport(outcome))
		if outcome.err != nil {
			failed = true
			if firstErr == nil {
				firstErr = outcome.err
			}
			continue
		}
		switch outcome.batch.Status {
		case search.BatchPartial, search.BatchDegradedEmpty:
			degraded = true
		case search.BatchAuthoritativeEmpty:
			authoritative = true
		case search.BatchHealthy:
			if len(outcome.batch.Hits) == 0 {
				authoritative = true
			}
		}
		if len(outcome.batch.Hits) == 0 {
			continue
		}
		if outcome.batch.Status != search.BatchHealthy && outcome.batch.Status != search.BatchPartial {
			continue
		}
		all = append(all, variantHits{
			label:    outcome.lane.variant,
			provider: outcome.lane.provider,
			lane:     outcome.lane.lane,
			weight:   outcome.lane.weight,
			hits:     outcome.batch.Hits,
		})
	}
	if len(all) > 0 {
		candidates := fuseCandidateSet(all, candidateCap)
		candidates.status = search.BatchHealthy
		if degraded || failed {
			candidates.status = search.BatchPartial
		}
		candidates.lanes = reports
		return candidates, contextErr
	}
	if degraded || (authoritative && failed) {
		return candidateSet{status: search.BatchDegradedEmpty, lanes: reports}, contextErr
	}
	if authoritative {
		return candidateSet{status: search.BatchAuthoritativeEmpty, lanes: reports}, contextErr
	}
	if contextErr != nil {
		return candidateSet{status: search.BatchFailed, lanes: reports}, contextErr
	}
	if firstErr == nil {
		firstErr = errors.New("pipeline: all advanced discovery lanes failed")
	}
	return candidateSet{status: search.BatchFailed, lanes: reports}, firstErr
}

func discoveryLaneReport(outcome laneOutcome) search.DiscoveryLaneReport {
	status := outcome.batch.Status
	if outcome.err != nil || status == "" {
		status = search.BatchFailed
	} else if len(outcome.batch.Hits) == 0 {
		switch status {
		case search.BatchHealthy:
			status = search.BatchAuthoritativeEmpty
		case search.BatchPartial:
			status = search.BatchDegradedEmpty
		}
	}
	duration := outcome.duration
	if duration < 0 {
		duration = 0
	} else if duration > search.MaxDiscoveryDuration {
		duration = search.MaxDiscoveryDuration
	}
	report := search.DiscoveryLaneReport{
		Provider:   normalizeDiscoverySource(outcome.lane.provider),
		Lane:       normalizeDiscoverySource(outcome.lane.lane),
		Variant:    normalizeDiscoveryVariant(outcome.lane.variant),
		Status:     status,
		DurationMS: duration.Milliseconds(),
	}
	if outcome.err == nil && (status == search.BatchHealthy || status == search.BatchPartial) {
		report.CandidateCount = uniqueCanonicalCandidateCount(outcome.batch.Hits)
		if report.CandidateCount > search.MaxDiscoveryCandidateCount {
			report.CandidateCount = search.MaxDiscoveryCandidateCount
		}
	}
	diagnostics := outcome.batch.Diagnostics
	if len(diagnostics) > maxDiscoveryDiagnosticsPerLane {
		report.DiagnosticsTruncated = true
		diagnostics = diagnostics[:maxDiscoveryDiagnosticsPerLane]
	}
	fallbackCount := 0
	for _, diagnostic := range diagnostics {
		retryAfter := diagnostic.RetryAfter
		if retryAfter < 0 {
			retryAfter = 0
		} else if retryAfter > maxDiscoveryRetryAfter {
			retryAfter = maxDiscoveryRetryAfter
		}
		mapped := search.DiscoveryDiagnostic{
			Source:       normalizeDiscoverySource(diagnostic.Source),
			Reason:       normalizeDiscoverySource(diagnostic.Reason),
			Retryable:    diagnostic.Retryable,
			RetryAfterMS: retryAfter.Milliseconds(),
		}
		if fallback := diagnostic.Fallback; fallback != nil && fallbackCount < search.MaxDiscoveryFallbackCount &&
			fallback.Format == "cleaned_dom" && fallback.Content != "" && len(fallback.Content) <= search.MaxDiscoveryFallbackBytes &&
			utf8.ValidString(fallback.Content) && !strings.ContainsRune(fallback.Content, '\x00') {
			copied := *fallback
			mapped.Fallback = &copied
			fallbackCount++
		}
		report.Diagnostics = append(report.Diagnostics, mapped)
	}
	if outcome.err != nil && len(report.Diagnostics) == 0 {
		reason := "request_failed"
		retryable := false
		switch {
		case errors.Is(outcome.err, context.DeadlineExceeded):
			reason, retryable = "timeout", true
		case errors.Is(outcome.err, context.Canceled):
			reason = "canceled"
		}
		report.Diagnostics = []search.DiscoveryDiagnostic{{Reason: reason, Retryable: retryable}}
	}
	return report
}

func (p *Pipeline) basicLanes(base search.Query) ([]discoveryLane, error) {
	sources, err := p.plannedSources(base)
	if err != nil {
		return nil, err
	}
	hasOriginal := false
	for _, source := range sources {
		if source.Searcher != nil && sourceAllowsVariant(source, "original") {
			hasOriginal = true
			break
		}
	}
	if !hasOriginal && p.Searcher != nil {
		// Packs may intentionally contain only advanced variants such as docs or
		// freshness. Basic search still needs one unchanged request, so fall back
		// to the operator-selected primary without changing the advanced plan.
		fallback := DiscoverySource{
			ID: "primary", ProviderID: "primary", Searcher: p.Searcher,
			Weight: 1, Variants: []string{"original"},
		}
		if planner, ok := p.DiscoveryPlanner.(interface{ PrimarySource() discovery.Source }); ok {
			fallback = planner.PrimarySource()
			fallback.Variants = []string{"original"}
			fallback.Engines = nil
			fallback.Categories = nil
		}
		sources = append([]DiscoverySource{fallback}, sources...)
	}
	localSearcher := p.availableLocalSearcher()
	if localSearcher != nil && len(base.Engines) == 0 {
		sources = append(sources, DiscoverySource{
			ID: "local-index", ProviderID: "local-index", Searcher: localSearcher,
			Weight: 1, Variants: []string{"original"},
		})
	}
	lanes := make([]discoveryLane, 0, len(sources))
	seenProviders := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		if source.Searcher == nil || !sourceAllowsVariant(source, "original") {
			continue
		}
		provider := source.ProviderID
		if provider == "" {
			provider = source.ID
		}
		provider = normalizeDiscoverySource(provider)
		if _, duplicate := seenProviders[provider]; duplicate {
			continue
		}
		seenProviders[provider] = struct{}{}
		query := applySourceControls(base, source)
		weight := source.Weight
		if weight <= 0 {
			weight = 1
		}
		lanes = append(lanes, discoveryLane{
			provider: provenanceProvider(source), lane: provenanceLane(source),
			variant: "original", weight: weight,
			query: query, searcher: source.Searcher, timeout: source.Timeout,
		})
	}
	return lanes, nil
}

func (p *Pipeline) plannedSources(base search.Query) ([]DiscoverySource, error) {
	if p.DiscoveryPlanner != nil {
		if planner, ok := p.DiscoveryPlanner.(discovery.QueryPlanner); ok {
			return planner.Plan(base)
		}
		return p.DiscoveryPlanner.Sources(base), nil
	}
	return p.DiscoverySources, nil
}

func (p *Pipeline) advancedLanes(base search.Query, variants []queryVariant) ([]discoveryLane, error) {
	sources, err := p.plannedSources(base)
	if err != nil {
		return nil, err
	}
	localSearcher := p.availableLocalSearcher()
	normalized := make([]DiscoverySource, 0, len(sources)+1)
	localPresent := false
	for _, source := range sources {
		isLocal := source.ProviderID == "local-index" || source.ID == "local-index"
		if isLocal {
			if localSearcher == nil || len(base.Engines) != 0 {
				continue
			}
			source.Searcher = localSearcher
			localPresent = true
		}
		normalized = append(normalized, source)
	}
	sources = normalized
	if localSearcher != nil && len(base.Engines) == 0 {
		if !localPresent {
			sources = append(sources, DiscoverySource{
				ID: "local-index", ProviderID: "local-index", Searcher: localSearcher,
				Weight: 1, Variants: []string{"original"},
			})
		}
	}
	if len(sources) == 0 {
		sources = []DiscoverySource{{ID: "primary", Searcher: p.Searcher, Weight: 1}}
	}
	lanes := make([]discoveryLane, 0, len(sources)*len(variants))
	for _, variant := range variants {
		for _, source := range sources {
			if source.Searcher == nil {
				continue
			}
			if !sourceAllowsVariant(source, variant.label) {
				continue
			}
			weight := source.Weight * variant.weight
			if weight <= 0 {
				weight = 1
			}
			query := applySourceControls(variant.query, source)
			lanes = append(lanes, discoveryLane{
				provider: provenanceProvider(source),
				lane:     provenanceLane(source),
				variant:  normalizeDiscoveryVariant(variant.label),
				weight:   weight,
				query:    query,
				searcher: source.Searcher,
				timeout:  source.Timeout,
			})
		}
	}
	return lanes, nil
}

func applySourceControls(query search.Query, source DiscoverySource) search.Query {
	if len(query.Engines) == 0 && len(source.Engines) > 0 {
		query.Engines = append([]string(nil), source.Engines...)
	}
	if len(query.Categories) == 0 && len(source.Categories) > 0 {
		query.Categories = append([]string(nil), source.Categories...)
	}
	if source.MaxResults > 0 && (query.MaxResults <= 0 || source.MaxResults < query.MaxResults) {
		query.MaxResults = source.MaxResults
	}
	return query
}

func sourceAllowsVariant(source DiscoverySource, variant string) bool {
	if len(source.Variants) == 0 {
		return true
	}
	for _, allowed := range source.Variants {
		if allowed == variant {
			return true
		}
	}
	return false
}

func executeDiscoveryLanes(ctx context.Context, lanes []discoveryLane, concurrency int) []laneOutcome {
	outcomes := make([]laneOutcome, len(lanes))
	if len(lanes) == 0 {
		return outcomes
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > len(lanes) {
		concurrency = len(lanes)
	}
	jobs := make(chan int, len(lanes))
	for i := range lanes {
		jobs <- i
	}
	close(jobs)
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				if ctx.Err() != nil {
					outcomes[index] = laneOutcome{lane: lanes[index], err: ctx.Err()}
					continue
				}
				outcomes[index] = executeDiscoveryLane(ctx, lanes[index])
			}
		}()
	}
	wg.Wait()
	return outcomes
}

func executeDiscoveryLane(ctx context.Context, lane discoveryLane) laneOutcome {
	started := time.Now()
	if lane.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, lane.timeout)
		defer cancel()
	}
	var batch search.SearchBatch
	var err error
	if rich, ok := lane.searcher.(search.BatchSearcher); ok {
		batch, err = rich.SearchBatch(ctx, lane.query)
	} else {
		batch.Hits, err = lane.searcher.Search(ctx, lane.query)
		batch.Provider = "legacy"
	}
	if err == nil {
		switch batch.Status {
		case search.BatchHealthy:
			if len(batch.Hits) == 0 {
				batch.Status = search.BatchAuthoritativeEmpty
			}
		case search.BatchPartial:
			if len(batch.Hits) == 0 {
				batch.Status = search.BatchDegradedEmpty
			}
		case search.BatchDegradedEmpty, search.BatchAuthoritativeEmpty:
		case search.BatchFailed:
			err = errors.New("pipeline: discovery lane returned failed status")
		default:
			if len(batch.Hits) > 0 {
				batch.Status = search.BatchHealthy
			} else {
				batch.Status = search.BatchAuthoritativeEmpty
			}
		}
	}
	return laneOutcome{lane: lane, batch: batch, err: err, duration: time.Since(started)}
}

func observeLaneOutcome(outcome laneOutcome) {
	status := "failed"
	if outcome.err == nil {
		switch outcome.batch.Status {
		case search.BatchHealthy, search.BatchPartial, search.BatchDegradedEmpty, search.BatchAuthoritativeEmpty:
			status = string(outcome.batch.Status)
		}
	}
	obs.DiscoveryLaneTotal.WithLabelValues(outcome.lane.provider, outcome.lane.lane, outcome.lane.variant, status).Inc()
	obs.DiscoveryLaneDuration.WithLabelValues(outcome.lane.provider, outcome.lane.lane, outcome.lane.variant).Observe(outcome.duration.Seconds())
	count := 0
	if outcome.err == nil && (outcome.batch.Status == search.BatchHealthy || outcome.batch.Status == search.BatchPartial) {
		count = uniqueCanonicalCandidateCount(outcome.batch.Hits)
	}
	obs.DiscoverySourceCandidates.WithLabelValues(outcome.lane.provider, outcome.lane.lane, outcome.lane.variant).Observe(float64(count))
}

func provenanceProvider(source DiscoverySource) string {
	if source.ProviderKind != "" {
		return normalizeDiscoverySource(source.ProviderKind)
	}
	provider := source.ProviderID
	if provider == "" {
		provider = source.ID
	}
	return normalizeDiscoverySource(provider)
}

func provenanceLane(source DiscoverySource) string {
	if source.ProviderKind == "federation" {
		return "federation"
	}
	return normalizeDiscoverySource(source.ID)
}

func uniqueCanonicalCandidateCount(hits []search.Hit) int {
	seen := make(map[string]struct{}, len(hits))
	for _, hit := range hits {
		canonical, err := cache.CanonicalURL(hit.URL)
		if err != nil {
			canonical = hit.URL
		}
		seen[canonical] = struct{}{}
	}
	return len(seen)
}

func normalizeDiscoverySource(source string) string {
	source = strings.ToLower(strings.TrimSpace(source))
	if source == "" || len(source) > 48 {
		return "other"
	}
	for _, r := range source {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' && r != '-' {
			return "other"
		}
	}
	return source
}

func normalizeDiscoveryVariant(variant string) string {
	switch variant {
	case "original", "exact", "freshness", "docs":
		return variant
	default:
		return "other"
	}
}

func normalizeDiscoveryProvenance(value model.DiscoveryProvenance) model.DiscoveryProvenance {
	return model.DiscoveryProvenance{
		Provider: normalizeDiscoverySource(value.Provider),
		Lane:     normalizeDiscoverySource(value.Lane),
		Variant:  normalizeDiscoveryVariant(value.Variant),
	}
}

// mergeDiscoveryProvenance returns a unique, deterministic tuple set. Sorting
// makes response JSON and evaluation artifacts independent of goroutine or
// provider completion order.
func mergeDiscoveryProvenance(left, right []model.DiscoveryProvenance) []model.DiscoveryProvenance {
	seen := make(map[model.DiscoveryProvenance]struct{}, len(left)+len(right))
	merged := make([]model.DiscoveryProvenance, 0, len(left)+len(right))
	for _, value := range append(append([]model.DiscoveryProvenance(nil), left...), right...) {
		value = normalizeDiscoveryProvenance(value)
		if _, duplicate := seen[value]; duplicate {
			continue
		}
		seen[value] = struct{}{}
		merged = append(merged, value)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Provider != merged[j].Provider {
			return merged[i].Provider < merged[j].Provider
		}
		if merged[i].Lane != merged[j].Lane {
			return merged[i].Lane < merged[j].Lane
		}
		return merged[i].Variant < merged[j].Variant
	})
	return merged
}

// syncLegacyRRFProvenance keeps the temporary lane/variant projection aligned
// with typed attribution after content-equivalent results merge. Existing
// valid winner order is preserved; newly absorbed tuples append in typed order.
func syncLegacyRRFProvenance(result *model.SearchResult) {
	if result == nil || len(result.Provenance) == 0 {
		return
	}
	allowed := make(map[string]struct{}, len(result.Provenance))
	for _, value := range result.Provenance {
		value = normalizeDiscoveryProvenance(value)
		allowed[value.Lane+":"+value.Variant] = struct{}{}
	}
	projection := make([]string, 0, len(allowed))
	seen := make(map[string]struct{}, len(allowed))
	if result.Metadata != nil {
		for _, token := range strings.Split(result.Metadata["rrf_sources"], ",") {
			token = strings.TrimSpace(token)
			if _, valid := allowed[token]; !valid {
				continue
			}
			if _, duplicate := seen[token]; duplicate {
				continue
			}
			seen[token] = struct{}{}
			projection = append(projection, token)
		}
	}
	for _, value := range result.Provenance {
		value = normalizeDiscoveryProvenance(value)
		token := value.Lane + ":" + value.Variant
		if _, duplicate := seen[token]; duplicate {
			continue
		}
		seen[token] = struct{}{}
		projection = append(projection, token)
	}
	if result.Metadata == nil {
		result.Metadata = make(map[string]string, 1)
	}
	result.Metadata["rrf_sources"] = strings.Join(projection, ",")
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
		variants = append(variants, queryVariant{label: label, weight: 1, query: q})
	}

	add("original", base)
	if len(queryTerms(base.Q)) > 1 {
		q := base
		q.ExactMatch = true
		add("exact", q)
	}
	if strings.TrimSpace(base.TimeRange) != "" || isFreshnessQuery(base.Q) {
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
	sources   []model.DiscoveryProvenance
	seenRank  int
	firstSeen int
}

func fuseHitsRRF(all []variantHits, candidateCap int) []search.Hit {
	return fuseCandidateSet(all, candidateCap).hits
}

func fuseCandidateSet(all []variantHits, candidateCap int) candidateSet {
	byURL := map[string]*fusedHit{}
	order := 0
	for _, variant := range all {
		weight := variant.weight
		if weight <= 0 {
			weight = 1
		}
		provider := normalizeDiscoverySource(variant.provider)
		lane := normalizeDiscoverySource(variant.lane)
		if provider == "other" && strings.TrimSpace(variant.provider) == "" {
			provider = lane
		}
		if lane == "other" && strings.TrimSpace(variant.lane) == "" {
			lane = provider
		}
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
				hit.Provenance = mergeDiscoveryProvenance(nil, hit.Provenance)
				fused = &fusedHit{hit: hit, seenRank: rank, firstSeen: order}
				byURL[canonicalURL] = fused
				order++
			} else {
				mergeHit(&fused.hit, hit)
			}
			fused.score += weight / float64(rrfRankConstant+rank)
			fused.variants = appendUnique(fused.variants, variant.label)
			fused.sources = appendDiscoveryProvenanceStable(fused.sources, model.DiscoveryProvenance{
				Provider: provider, Lane: lane, Variant: normalizeDiscoveryVariant(variant.label),
			})
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

	out := candidateSet{hits: make([]search.Hit, len(fused))}
	for i, hit := range fused {
		out.hits[i] = hit.hit
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
	if dst.ProviderDocument == nil {
		dst.ProviderDocument = src.ProviderDocument
	}
	dst.Engines = mergeEnginesStable(dst.Engines, src.Engines)
	dst.Provenance = mergeDiscoveryProvenance(dst.Provenance, src.Provenance)
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
	hit.hit.Provenance = mergeDiscoveryProvenance(hit.hit.Provenance, hit.sources)
	sources := make([]string, len(hit.sources))
	for i, source := range hit.sources {
		sources[i] = source.Lane + ":" + source.Variant
	}
	hit.hit.Metadata["rrf_sources"] = strings.Join(sources, ",")
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

func appendDiscoveryProvenanceStable(values []model.DiscoveryProvenance, value model.DiscoveryProvenance) []model.DiscoveryProvenance {
	value = normalizeDiscoveryProvenance(value)
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

type topKContributionSummary struct {
	contributions   map[model.DiscoveryProvenance]int
	domainsBySource map[model.DiscoveryProvenance]int
	uniqueDomains   int
}

func summarizeTopKContribution(results []model.SearchResult) topKContributionSummary {
	contributions := make(map[model.DiscoveryProvenance]int)
	domainSets := make(map[model.DiscoveryProvenance]map[string]struct{})
	allDomains := make(map[string]struct{})
	for _, result := range results {
		refs := result.Provenance
		if len(refs) == 0 {
			continue
		}
		host := hostname(result.URL)
		if host != "" {
			allDomains[host] = struct{}{}
		}
		for _, ref := range refs {
			ref = normalizeDiscoveryProvenance(ref)
			contributions[ref]++
			if host == "" {
				continue
			}
			if domainSets[ref] == nil {
				domainSets[ref] = make(map[string]struct{})
			}
			domainSets[ref][host] = struct{}{}
		}
	}
	domainsBySource := make(map[model.DiscoveryProvenance]int, len(domainSets))
	for ref, domains := range domainSets {
		domainsBySource[ref] = len(domains)
	}
	return topKContributionSummary{
		contributions:   contributions,
		domainsBySource: domainsBySource,
		uniqueDomains:   len(allDomains),
	}
}

func observeTopKContribution(results []model.SearchResult) {
	summary := summarizeTopKContribution(results)
	for ref, count := range summary.contributions {
		obs.DiscoverySourceTopKContribution.WithLabelValues(ref.Provider, ref.Lane, ref.Variant).Add(float64(count))
	}
	for ref, domains := range summary.domainsBySource {
		obs.DiscoverySourceTopKUniqueDomains.WithLabelValues(ref.Provider, ref.Lane, ref.Variant).Observe(float64(domains))
	}
	obs.DiscoveryTopKUniqueDomains.Observe(float64(summary.uniqueDomains))
}

func hostname(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return strings.ToLower(parsed.Hostname())
}
