// Package discoverycache provides bounded in-process discovery result caching
// and identical-query coalescing. It wraps, rather than replaces, the domain
// Searcher contract so providers and the future broker remain independent.
package discoverycache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/search"
	"github.com/staticvar/fetchmark/internal/obs"
)

// Options bounds cache lifetime, background work, and process memory.
// StaleTTL is an additional stale-while-revalidate window after FreshTTL.
type Options struct {
	FreshTTL       time.Duration
	StaleTTL       time.Duration
	RefreshTimeout time.Duration
	MaxEntries     int
	MaxBytes       int64
	MaxEntryBytes  int64
	MaxInflight    int
}

type entry struct {
	batch      search.SearchBatch
	freshUntil time.Time
	staleUntil time.Time
	size       int64
	sequence   uint64
}

type flight struct {
	done      chan struct{}
	cancel    context.CancelFunc
	waiters   int
	keepAlive bool
	batch     search.SearchBatch
	err       error
}

type acquisition struct {
	cached   search.SearchBatch
	cacheHit bool
	stale    bool
	work     *flight
	shared   bool
	accepted bool
	waiter   bool
}

// Searcher wraps a discovery provider with bounded stale-while-revalidate and
// process-local query coalescing. Only healthy batches with hits are cacheable;
// partial, empty, failed, canceled, and oversized outcomes are never admitted.
type Searcher struct {
	inner search.Searcher
	batch search.BatchSearcher
	opts  Options

	mu       sync.Mutex
	entries  map[string]entry
	bytes    int64
	sequence uint64
	inflight map[string]*flight
	now      func() time.Time

	reportedEntries int
	reportedBytes   int64

	// Test seams for deterministic handoff and metric-ordering regressions.
	afterLookupMiss      func()
	afterStaleLookup     func()
	beforeFlightPublish  func()
	beforeInflightMetric func(int)
	beforeUsageMetric    func(int, int64)
}

var _ search.Searcher = (*Searcher)(nil)
var _ search.BatchSearcher = (*Searcher)(nil)

// New constructs a bounded cached searcher. Invalid or zero limits are
// rejected rather than silently creating an unbounded or inert cache.
func New(inner search.Searcher, options Options) (*Searcher, error) {
	if inner == nil {
		return nil, errors.New("discoverycache: inner searcher is required")
	}
	if options.FreshTTL <= 0 || options.StaleTTL <= 0 || options.RefreshTimeout <= 0 {
		return nil, errors.New("discoverycache: durations must be > 0")
	}
	if options.MaxEntries <= 0 || options.MaxBytes <= 0 || options.MaxEntryBytes <= 0 || options.MaxInflight <= 0 {
		return nil, errors.New("discoverycache: limits must be > 0")
	}
	if options.MaxEntryBytes > options.MaxBytes {
		return nil, errors.New("discoverycache: max entry bytes must be <= max bytes")
	}
	cached := &Searcher{
		inner:    inner,
		opts:     options,
		entries:  make(map[string]entry),
		inflight: make(map[string]*flight),
		now:      time.Now,
	}
	if batch, ok := inner.(search.BatchSearcher); ok {
		cached.batch = batch
	}
	return cached, nil
}

// Search preserves the compatibility contract and delegates to SearchBatch.
func (s *Searcher) Search(ctx context.Context, q search.Query) ([]search.Hit, error) {
	batch, err := s.SearchBatch(ctx, q)
	if err != nil {
		return nil, err
	}
	return batch.Hits, nil
}

// SearchBatch returns fresh cached hits immediately, serves stale hits while
// one detached bounded refresh runs, or waits for one coalesced cold fill.
// Canceling one waiter does not interrupt other waiters, but the final cold
// waiter releases provider work that can no longer serve a caller. Each waiter
// still observes its own context.
func (s *Searcher) SearchBatch(ctx context.Context, q search.Query) (search.SearchBatch, error) {
	if err := ctx.Err(); err != nil {
		return failedBatch(), err
	}
	q = cloneQuery(q)
	key := queryKey(q)
	acquired := s.acquire(key, q, s.now())
	if acquired.cacheHit {
		if acquired.stale {
			obs.DiscoveryCacheEvents.WithLabelValues("stale_hit").Inc()
			if s.afterStaleLookup != nil {
				s.afterStaleLookup()
			}
			switch {
			case acquired.shared:
				obs.DiscoveryCacheEvents.WithLabelValues("coalesced").Inc()
			case acquired.accepted:
				obs.DiscoveryCacheEvents.WithLabelValues("refresh_started").Inc()
			default:
				obs.DiscoveryCacheEvents.WithLabelValues("refresh_skipped_limit").Inc()
			}
		} else {
			obs.DiscoveryCacheEvents.WithLabelValues("fresh_hit").Inc()
		}
		if err := ctx.Err(); err != nil {
			return failedBatch(), err
		}
		return acquired.cached, nil
	}

	obs.DiscoveryCacheEvents.WithLabelValues("miss").Inc()
	if !acquired.accepted {
		obs.DiscoveryCacheEvents.WithLabelValues("bypass_limit").Inc()
		return s.searchDirect(ctx, q)
	}
	if acquired.shared {
		obs.DiscoveryCacheEvents.WithLabelValues("coalesced").Inc()
	}
	if acquired.waiter {
		defer s.releaseWaiter(key, acquired.work)
	}
	select {
	case <-ctx.Done():
		obs.DiscoveryCacheEvents.WithLabelValues("caller_canceled").Inc()
		return failedBatch(), ctx.Err()
	case <-acquired.work.done:
		if err := ctx.Err(); err != nil {
			obs.DiscoveryCacheEvents.WithLabelValues("caller_canceled").Inc()
			return failedBatch(), err
		}
		if acquired.work.err != nil {
			return cloneBatch(acquired.work.batch), acquired.work.err
		}
		return cloneBatch(acquired.work.batch), nil
	}
}

// acquire atomically resolves a cache entry and joins or starts any required
// provider work. In particular, a stale result cannot be observed without the
// same transaction also joining the refresh that was current at that instant.
func (s *Searcher) acquire(key string, q search.Query, now time.Time) acquisition {
	missHookCalled := false
	for {
		s.mu.Lock()
		if cached, ok := s.entries[key]; ok {
			if now.Before(cached.staleUntil) {
				s.sequence++
				cached.sequence = s.sequence
				s.entries[key] = cached
				acquired := acquisition{
					cached:   cloneBatch(cached.batch),
					cacheHit: true,
					stale:    !now.Before(cached.freshUntil),
				}
				if !acquired.stale {
					s.mu.Unlock()
					return acquired
				}
				flight := s.startFlightLocked(key, q, true)
				acquired.work = flight.work
				acquired.shared = flight.shared
				acquired.accepted = flight.accepted
				return acquired
			}
			s.deleteEntryLocked(key, cached)
			s.setUsageMetricsLocked()
		}
		if !missHookCalled && s.afterLookupMiss != nil {
			missHookCalled = true
			hook := s.afterLookupMiss
			s.mu.Unlock()
			hook()
			continue
		}
		return s.startFlightLocked(key, q, false)
	}
}

// startFlightLocked consumes and releases s.mu on every path.
func (s *Searcher) startFlightLocked(key string, q search.Query, keepAlive bool) acquisition {
	if existing := s.inflight[key]; existing != nil {
		if keepAlive {
			existing.keepAlive = true
		} else {
			existing.waiters++
		}
		s.mu.Unlock()
		return acquisition{work: existing, shared: true, accepted: true, waiter: !keepAlive}
	}
	if len(s.inflight) >= s.opts.MaxInflight {
		s.mu.Unlock()
		return acquisition{}
	}
	workCtx, cancel := context.WithTimeout(context.Background(), s.opts.RefreshTimeout)
	work := &flight{done: make(chan struct{}), cancel: cancel, keepAlive: keepAlive}
	if !keepAlive {
		work.waiters = 1
	}
	s.inflight[key] = work
	inflight := len(s.inflight)
	obs.DiscoveryCacheInflight.Inc()
	s.mu.Unlock()
	if s.beforeInflightMetric != nil {
		s.beforeInflightMetric(inflight)
	}
	go s.runFlight(key, q, work, workCtx)
	return acquisition{work: work, accepted: true, waiter: !keepAlive}
}

func (s *Searcher) runFlight(key string, q search.Query, work *flight, ctx context.Context) {
	defer work.cancel()
	batch, err := s.searchProvider(ctx, q)
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		if size, ok := s.cacheable(batch); ok {
			s.store(key, batch, size, s.now())
			obs.DiscoveryCacheEvents.WithLabelValues("admitted").Inc()
		} else {
			obs.DiscoveryCacheEvents.WithLabelValues("rejected").Inc()
		}
	}
	if err != nil {
		batch = failureBatch(batch)
	}
	if s.beforeFlightPublish != nil {
		s.beforeFlightPublish()
	}
	s.mu.Lock()
	work.batch = cloneBatch(batch)
	work.err = err
	close(work.done)
	removed := s.inflight[key] == work
	if removed {
		delete(s.inflight, key)
		obs.DiscoveryCacheInflight.Dec()
	}
	inflight := len(s.inflight)
	s.mu.Unlock()
	if removed && s.beforeInflightMetric != nil {
		s.beforeInflightMetric(inflight)
	}
}

// releaseWaiter cancels a cold fill only after its final waiting caller has
// gone away. Stale refreshes remain detached, and one canceled caller cannot
// disrupt an identical request that is still waiting on the shared work.
func (s *Searcher) releaseWaiter(key string, work *flight) {
	if work == nil {
		return
	}
	var cancel context.CancelFunc
	removed := false
	inflight := 0
	s.mu.Lock()
	if work.waiters > 0 {
		work.waiters--
	}
	if work.waiters == 0 && !work.keepAlive && s.inflight[key] == work {
		delete(s.inflight, key)
		obs.DiscoveryCacheInflight.Dec()
		inflight = len(s.inflight)
		cancel = work.cancel
		removed = true
	}
	s.mu.Unlock()
	if removed && s.beforeInflightMetric != nil {
		s.beforeInflightMetric(inflight)
	}
	if cancel != nil {
		cancel()
	}
}

// searchDirect is the overload path when all detached-flight slots are used.
// It honors the caller context and deliberately avoids cache admission.
func (s *Searcher) searchDirect(ctx context.Context, q search.Query) (search.SearchBatch, error) {
	batch, err := s.searchProvider(ctx, q)
	if err != nil {
		return failureBatch(batch), err
	}
	if err := ctx.Err(); err != nil {
		return failureBatch(batch), err
	}
	return cloneBatch(batch), nil
}

func (s *Searcher) searchProvider(ctx context.Context, q search.Query) (search.SearchBatch, error) {
	if s.batch != nil {
		return s.batch.SearchBatch(ctx, q)
	}
	hits, err := s.inner.Search(ctx, q)
	if err != nil {
		return search.SearchBatch{Status: search.BatchFailed}, err
	}
	status := search.BatchAuthoritativeEmpty
	if len(hits) > 0 {
		status = search.BatchHealthy
	}
	return search.SearchBatch{Hits: hits, Provider: "legacy", Status: status}, nil
}

func (s *Searcher) cacheable(batch search.SearchBatch) (int64, bool) {
	if len(batch.Hits) == 0 || batch.Status != search.BatchHealthy {
		return 0, false
	}
	size, ok := batchSizeWithin(batch, s.opts.MaxEntryBytes)
	return size, ok
}

func (s *Searcher) store(key string, batch search.SearchBatch, size int64, now time.Time) {
	s.mu.Lock()
	if old, ok := s.entries[key]; ok {
		s.deleteEntryLocked(key, old)
	}
	s.sequence++
	s.entries[key] = entry{
		batch:      cloneBatch(batch),
		freshUntil: now.Add(s.opts.FreshTTL),
		staleUntil: now.Add(s.opts.FreshTTL + s.opts.StaleTTL),
		size:       size,
		sequence:   s.sequence,
	}
	s.bytes += size
	for len(s.entries) > s.opts.MaxEntries || s.bytes > s.opts.MaxBytes {
		oldestKey, oldest := s.oldestEntryLocked()
		s.deleteEntryLocked(oldestKey, oldest)
		obs.DiscoveryCacheEvents.WithLabelValues("evicted").Inc()
	}
	entries, bytes := len(s.entries), s.bytes
	s.setUsageMetricsLocked()
	s.mu.Unlock()
	if s.beforeUsageMetric != nil {
		s.beforeUsageMetric(entries, bytes)
	}
}

func (s *Searcher) oldestEntryLocked() (string, entry) {
	var oldestKey string
	var oldest entry
	first := true
	for key, cached := range s.entries {
		if first || cached.sequence < oldest.sequence {
			oldestKey, oldest, first = key, cached, false
		}
	}
	return oldestKey, oldest
}

func (s *Searcher) deleteEntryLocked(key string, cached entry) {
	delete(s.entries, key)
	s.bytes -= cached.size
	if s.bytes < 0 {
		s.bytes = 0
	}
}

func (s *Searcher) setUsageMetricsLocked() {
	entries := len(s.entries)
	obs.DiscoveryCacheEntries.Add(float64(entries - s.reportedEntries))
	obs.DiscoveryCacheBytes.Add(float64(s.bytes - s.reportedBytes))
	s.reportedEntries = entries
	s.reportedBytes = s.bytes
}

func (s *Searcher) entryCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

func failedBatch() search.SearchBatch {
	return search.SearchBatch{Status: search.BatchFailed}
}

func failureBatch(batch search.SearchBatch) search.SearchBatch {
	batch.Hits = nil
	batch.Status = search.BatchFailed
	return batch
}

func queryKey(q search.Query) string {
	type key struct {
		Version        int      `json:"version"`
		Query          string   `json:"query"`
		Engines        []string `json:"engines"`
		Categories     []string `json:"categories"`
		Language       string   `json:"language"`
		TimeRange      string   `json:"time_range"`
		SafeSearch     *int     `json:"safe_search"`
		IncludeDomains []string `json:"include_domains"`
		ExcludeDomains []string `json:"exclude_domains"`
		ExactMatch     bool     `json:"exact_match"`
		MaxResults     int      `json:"max_results"`
	}
	hash := sha256.New()
	_ = json.NewEncoder(hash).Encode(key{
		Version:        1,
		Query:          q.Q,
		Engines:        q.Engines,
		Categories:     q.Categories,
		Language:       q.Language,
		TimeRange:      q.TimeRange,
		SafeSearch:     q.SafeSearch,
		IncludeDomains: q.IncludeDomains,
		ExcludeDomains: q.ExcludeDomains,
		ExactMatch:     q.ExactMatch,
		MaxResults:     q.MaxResults,
	})
	return hex.EncodeToString(hash.Sum(nil))
}

func cloneQuery(q search.Query) search.Query {
	q.Engines = append([]string(nil), q.Engines...)
	q.Categories = append([]string(nil), q.Categories...)
	q.IncludeDomains = append([]string(nil), q.IncludeDomains...)
	q.ExcludeDomains = append([]string(nil), q.ExcludeDomains...)
	if q.SafeSearch != nil {
		value := *q.SafeSearch
		q.SafeSearch = &value
	}
	return q
}

func cloneBatch(batch search.SearchBatch) search.SearchBatch {
	batch.Hits = append([]search.Hit(nil), batch.Hits...)
	for i := range batch.Hits {
		batch.Hits[i].Engines = append([]string(nil), batch.Hits[i].Engines...)
		batch.Hits[i].Provenance = append([]model.DiscoveryProvenance(nil), batch.Hits[i].Provenance...)
		if batch.Hits[i].PublishedAt != nil {
			published := *batch.Hits[i].PublishedAt
			batch.Hits[i].PublishedAt = &published
		}
		if batch.Hits[i].Metadata != nil {
			metadata := make(map[string]string, len(batch.Hits[i].Metadata))
			for key, value := range batch.Hits[i].Metadata {
				metadata[key] = value
			}
			batch.Hits[i].Metadata = metadata
		}
		if batch.Hits[i].ProviderDocument != nil {
			batch.Hits[i].ProviderDocument = &model.ProviderDocument{
				HTML:     append([]byte(nil), batch.Hits[i].ProviderDocument.HTML...),
				Author:   batch.Hits[i].ProviderDocument.Author,
				SiteName: batch.Hits[i].ProviderDocument.SiteName,
			}
		}
	}
	batch.Diagnostics = append([]search.ProviderDiagnostic(nil), batch.Diagnostics...)
	return batch
}

// batchSizeWithin estimates retained payload plus conservative per-element
// overhead and aborts before integer overflow or the configured entry cap.
func batchSizeWithin(batch search.SearchBatch, limit int64) (int64, bool) {
	size := int64(256)
	add := func(amount int64) bool {
		if amount < 0 || size > limit-amount {
			return false
		}
		size += amount
		return true
	}
	if !add(int64(len(batch.Provider) + len(batch.Instance))) {
		return 0, false
	}
	for _, hit := range batch.Hits {
		if !add(128 + int64(len(hit.URL)+len(hit.Title)+len(hit.Snippet))) {
			return 0, false
		}
		if hit.ProviderDocument != nil && !add(32+int64(len(hit.ProviderDocument.HTML)+len(hit.ProviderDocument.Author)+len(hit.ProviderDocument.SiteName))) {
			return 0, false
		}
		for _, engine := range hit.Engines {
			if !add(16 + int64(len(engine))) {
				return 0, false
			}
		}
		for key, value := range hit.Metadata {
			if !add(64 + int64(len(key)+len(value))) {
				return 0, false
			}
		}
		for _, provenance := range hit.Provenance {
			if !add(64 + int64(len(provenance.Provider)+len(provenance.Lane)+len(provenance.Variant))) {
				return 0, false
			}
		}
	}
	for _, diagnostic := range batch.Diagnostics {
		if !add(64 + int64(len(diagnostic.Provider)+len(diagnostic.Instance)+len(diagnostic.Source)+len(diagnostic.Reason))) {
			return 0, false
		}
	}
	return size, true
}
