package discoverycache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/search"
	"github.com/staticvar/fetchmark/internal/obs"
)

type batchFunc func(context.Context, search.Query) (search.SearchBatch, error)

func (f batchFunc) SearchBatch(ctx context.Context, q search.Query) (search.SearchBatch, error) {
	return f(ctx, q)
}

func (f batchFunc) Search(ctx context.Context, q search.Query) ([]search.Hit, error) {
	batch, err := f(ctx, q)
	return batch.Hits, err
}

type legacyFunc func(context.Context, search.Query) ([]search.Hit, error)

func (f legacyFunc) Search(ctx context.Context, q search.Query) ([]search.Hit, error) {
	return f(ctx, q)
}

func testOptions() Options {
	return Options{
		FreshTTL:       time.Minute,
		StaleTTL:       5 * time.Minute,
		RefreshTimeout: 5 * time.Second,
		MaxEntries:     8,
		MaxBytes:       8 << 20,
		MaxEntryBytes:  1 << 20,
		MaxInflight:    4,
	}
}

func mustNew(t *testing.T, inner search.Searcher, options Options) *Searcher {
	t.Helper()
	cached, err := New(inner, options)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return cached
}

func healthyBatch(title string) search.SearchBatch {
	return search.SearchBatch{
		Provider: "test",
		Instance: "test-1",
		Status:   search.BatchHealthy,
		Hits: []search.Hit{{
			URL:        "https://example.com/" + title,
			Title:      title,
			Metadata:   map[string]string{"version": title},
			Provenance: []model.DiscoveryProvenance{{Provider: "test", Lane: "test-default", Variant: "original"}},
		}},
	}
}

func TestSearcherCoalescesIdenticalColdQueries(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	inner := batchFunc(func(ctx context.Context, _ search.Query) (search.SearchBatch, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
			return healthyBatch("shared"), nil
		case <-ctx.Done():
			return search.SearchBatch{}, ctx.Err()
		}
	})
	cached := mustNew(t, inner, testOptions())

	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			batch, err := cached.SearchBatch(context.Background(), search.Query{Q: "same"})
			if err == nil && (len(batch.Hits) != 1 || batch.Hits[0].Title != "shared") {
				err = fmt.Errorf("batch = %+v", batch)
			}
			results <- err
		}()
	}
	close(start)
	<-started
	assertRemains(t, 25*time.Millisecond, func() bool { return calls.Load() == 1 }, "provider call count while coalesced")
	close(release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider calls = %d, want 1", got)
	}
}

func TestSearcherRechecksCacheBeforeStartingFlight(t *testing.T) {
	var calls atomic.Int32
	inner := batchFunc(func(context.Context, search.Query) (search.SearchBatch, error) {
		calls.Add(1)
		return healthyBatch("duplicate"), nil
	})
	cached := mustNew(t, inner, testOptions())
	query := search.Query{Q: "handoff"}
	missed := make(chan struct{})
	release := make(chan struct{})
	cached.afterLookupMiss = func() {
		close(missed)
		<-release
	}
	type result struct {
		batch search.SearchBatch
		err   error
	}
	done := make(chan result, 1)
	go func() {
		batch, err := cached.SearchBatch(context.Background(), query)
		done <- result{batch: batch, err: err}
	}()
	<-missed
	winner := healthyBatch("winner")
	size, ok := cached.cacheable(winner)
	if !ok {
		t.Fatal("winner batch should be cacheable")
	}
	cached.store(queryKey(query), winner, size, cached.now())
	close(release)
	got := <-done
	if got.err != nil || len(got.batch.Hits) != 1 || got.batch.Hits[0].Title != "winner" {
		t.Fatalf("handoff batch = %+v err=%v", got.batch, got.err)
	}
	if providerCalls := calls.Load(); providerCalls != 0 {
		t.Fatalf("provider calls = %d, want 0 after cache handoff", providerCalls)
	}
}

func TestSearcherPublishesFlightBeforeRemovingSameKey(t *testing.T) {
	var calls atomic.Int32
	inner := batchFunc(func(context.Context, search.Query) (search.SearchBatch, error) {
		calls.Add(1)
		return search.SearchBatch{Provider: "test", Status: search.BatchAuthoritativeEmpty}, nil
	})
	cached := mustNew(t, inner, testOptions())
	publishBlocked := make(chan struct{})
	releasePublish := make(chan struct{})
	var once sync.Once
	cached.beforeFlightPublish = func() {
		once.Do(func() {
			close(publishBlocked)
			<-releasePublish
		})
	}
	query := search.Query{Q: "completion handoff"}
	results := make(chan error, 2)
	go func() {
		_, err := cached.SearchBatch(context.Background(), query)
		results <- err
	}()
	<-publishBlocked
	go func() {
		_, err := cached.SearchBatch(context.Background(), query)
		results <- err
	}()
	deadline := time.Now().Add(50 * time.Millisecond)
	for calls.Load() == 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	duplicate := calls.Load() != 1
	close(releasePublish)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("SearchBatch: %v", err)
		}
	}
	if duplicate {
		t.Fatalf("provider calls reached %d before first flight was published", calls.Load())
	}
}

func TestSearcherFreshHitIsClonedAndDoesNotCallProvider(t *testing.T) {
	var calls atomic.Int32
	inner := batchFunc(func(context.Context, search.Query) (search.SearchBatch, error) {
		calls.Add(1)
		batch := healthyBatch("original")
		batch.Hits[0].ProviderDocument = &model.ProviderDocument{HTML: []byte("original document"), Author: "Ada", SiteName: "Stack Overflow"}
		return batch, nil
	})
	cached := mustNew(t, inner, testOptions())

	first, err := cached.SearchBatch(context.Background(), search.Query{Q: "cached"})
	if err != nil {
		t.Fatalf("first SearchBatch: %v", err)
	}
	first.Hits[0].Title = "mutated"
	first.Hits[0].Metadata["version"] = "mutated"
	first.Hits[0].Provenance[0].Provider = "mutated"
	first.Hits[0].ProviderDocument.HTML[0] = 'X'
	second, err := cached.SearchBatch(context.Background(), search.Query{Q: "cached"})
	if err != nil {
		t.Fatalf("second SearchBatch: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider calls = %d, want 1", got)
	}
	if second.Hits[0].Title != "original" || second.Hits[0].Metadata["version"] != "original" || second.Hits[0].Provenance[0].Provider != "test" || string(second.Hits[0].ProviderDocument.HTML) != "original document" || second.Hits[0].ProviderDocument.Author != "Ada" || second.Hits[0].ProviderDocument.SiteName != "Stack Overflow" {
		t.Fatalf("cached batch was mutated through caller alias: %+v", second)
	}
}

func TestBatchSizeIncludesProviderDocument(t *testing.T) {
	batch := healthyBatch("sized")
	without, ok := batchSizeWithin(batch, 1<<20)
	if !ok {
		t.Fatal("baseline batch unexpectedly exceeded limit")
	}
	batch.Hits[0].ProviderDocument = &model.ProviderDocument{HTML: []byte("provider body"), Author: "Ada", SiteName: "Stack Overflow"}
	withDocument, ok := batchSizeWithin(batch, 1<<20)
	if !ok {
		t.Fatal("provider batch unexpectedly exceeded limit")
	}
	if delta := withDocument - without; delta < int64(len("provider body")+len("Ada")+len("Stack Overflow")) {
		t.Fatalf("provider document size delta = %d", delta)
	}
}

func TestSearcherServesStaleWhileOneRefreshRuns(t *testing.T) {
	var calls atomic.Int32
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	inner := batchFunc(func(ctx context.Context, _ search.Query) (search.SearchBatch, error) {
		switch calls.Add(1) {
		case 1:
			return healthyBatch("old"), nil
		case 2:
			close(refreshStarted)
			select {
			case <-releaseRefresh:
				return healthyBatch("new"), nil
			case <-ctx.Done():
				return search.SearchBatch{}, ctx.Err()
			}
		default:
			return healthyBatch("unexpected"), nil
		}
	})
	options := testOptions()
	options.FreshTTL = time.Second
	cached := mustNew(t, inner, options)
	var nowNanos atomic.Int64
	nowNanos.Store(time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC).UnixNano())
	cached.now = func() time.Time { return time.Unix(0, nowNanos.Load()) }
	query := search.Query{Q: "stale"}

	if _, err := cached.SearchBatch(context.Background(), query); err != nil {
		t.Fatalf("prime SearchBatch: %v", err)
	}
	nowNanos.Add(int64(2 * time.Second))
	stale, err := cached.SearchBatch(context.Background(), query)
	if err != nil || stale.Hits[0].Title != "old" {
		t.Fatalf("stale SearchBatch = %+v err=%v", stale, err)
	}
	<-refreshStarted
	for range 4 {
		batch, err := cached.SearchBatch(context.Background(), query)
		if err != nil || batch.Hits[0].Title != "old" {
			t.Fatalf("concurrent stale SearchBatch = %+v err=%v", batch, err)
		}
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("provider calls during refresh = %d, want 2", got)
	}
	cached.mu.Lock()
	refresh := cached.inflight[queryKey(query)]
	cached.mu.Unlock()
	if refresh == nil {
		t.Fatal("missing in-flight stale refresh")
	}
	close(releaseRefresh)
	<-refresh.done
	refreshed, err := cached.SearchBatch(context.Background(), query)
	if err != nil || len(refreshed.Hits) != 1 || refreshed.Hits[0].Title != "new" {
		t.Fatalf("refreshed batch = %+v err=%v calls=%d", refreshed, err, calls.Load())
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("provider calls after refresh = %d, want 2", got)
	}
}

func TestSearcherStaleLookupJoinsCompletingRefresh(t *testing.T) {
	var calls atomic.Int32
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	inner := batchFunc(func(ctx context.Context, _ search.Query) (search.SearchBatch, error) {
		switch calls.Add(1) {
		case 1:
			return healthyBatch("old"), nil
		case 2:
			close(refreshStarted)
			select {
			case <-releaseRefresh:
				return healthyBatch("new"), nil
			case <-ctx.Done():
				return search.SearchBatch{}, ctx.Err()
			}
		default:
			return healthyBatch("duplicate"), nil
		}
	})
	options := testOptions()
	options.FreshTTL = time.Second
	cached := mustNew(t, inner, options)
	var nowNanos atomic.Int64
	nowNanos.Store(time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC).UnixNano())
	cached.now = func() time.Time { return time.Unix(0, nowNanos.Load()) }
	query := search.Query{Q: "stale handoff"}
	if _, err := cached.SearchBatch(context.Background(), query); err != nil {
		t.Fatalf("prime SearchBatch: %v", err)
	}
	nowNanos.Add(int64(2 * time.Second))
	if _, err := cached.SearchBatch(context.Background(), query); err != nil {
		t.Fatalf("start refresh SearchBatch: %v", err)
	}
	<-refreshStarted

	staleObserved := make(chan struct{})
	releaseStale := make(chan struct{})
	var once sync.Once
	cached.afterStaleLookup = func() {
		once.Do(func() {
			close(staleObserved)
			<-releaseStale
		})
	}
	second := make(chan error, 1)
	go func() {
		_, err := cached.SearchBatch(context.Background(), query)
		second <- err
	}()
	<-staleObserved
	cached.mu.Lock()
	refresh := cached.inflight[queryKey(query)]
	cached.mu.Unlock()
	if refresh == nil {
		t.Fatal("missing first refresh")
	}
	close(releaseRefresh)
	<-refresh.done
	close(releaseStale)
	if err := <-second; err != nil {
		t.Fatalf("second stale SearchBatch: %v", err)
	}
	assertRemains(t, 25*time.Millisecond, func() bool { return calls.Load() == 2 }, "provider call count after stale handoff")
	if got := calls.Load(); got != 2 {
		t.Fatalf("provider calls = %d, want prime plus one refresh", got)
	}
}

func TestSearcherDoesNotCacheEmptyFailedOrOversizedOutcomes(t *testing.T) {
	tests := []struct {
		name       string
		batch      search.SearchBatch
		err        error
		entryBytes int64
	}{
		{name: "degraded empty", batch: search.SearchBatch{Status: search.BatchDegradedEmpty, Diagnostics: []search.ProviderDiagnostic{{Reason: "blocked"}}}},
		{name: "authoritative empty", batch: search.SearchBatch{Status: search.BatchAuthoritativeEmpty}},
		{name: "partial hits", batch: search.SearchBatch{Status: search.BatchPartial, Hits: []search.Hit{{URL: "https://example.com/partial"}}}},
		{name: "failed status", batch: search.SearchBatch{Status: search.BatchFailed}},
		{name: "provider error", err: errors.New("provider failed")},
		{name: "cancellation", err: context.Canceled},
		{name: "oversized", batch: healthyBatch("payload-too-large"), entryBytes: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			inner := batchFunc(func(context.Context, search.Query) (search.SearchBatch, error) {
				calls.Add(1)
				return tt.batch, tt.err
			})
			options := testOptions()
			if tt.entryBytes > 0 {
				options.MaxEntryBytes = tt.entryBytes
			}
			cached := mustNew(t, inner, options)
			for range 2 {
				_, _ = cached.SearchBatch(context.Background(), search.Query{Q: "not cacheable"})
			}
			if got := calls.Load(); got != 2 {
				t.Fatalf("provider calls = %d, want 2", got)
			}
		})
	}
}

func TestSearcherCallerCancellationDoesNotCancelSharedFill(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	providerCanceled := make(chan struct{})
	release := make(chan struct{})
	inner := batchFunc(func(ctx context.Context, _ search.Query) (search.SearchBatch, error) {
		calls.Add(1)
		close(started)
		select {
		case <-release:
			return healthyBatch("shared"), nil
		case <-ctx.Done():
			close(providerCanceled)
			return search.SearchBatch{}, ctx.Err()
		}
	})
	cached := mustNew(t, inner, testOptions())
	query := search.Query{Q: "shared cancellation"}
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		_, err := cached.SearchBatch(ctx, query)
		first <- err
	}()
	<-started
	second := make(chan error, 1)
	go func() {
		_, err := cached.SearchBatch(context.Background(), query)
		second <- err
	}()
	assertRemains(t, 25*time.Millisecond, func() bool { return calls.Load() == 1 }, "shared provider call count")
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("first error = %v, want context.Canceled", err)
	}
	select {
	case <-providerCanceled:
		t.Fatal("caller cancellation canceled shared provider fill")
	default:
	}
	close(release)
	if err := <-second; err != nil {
		t.Fatalf("second error = %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider calls = %d, want 1", got)
	}
}

func TestSearcherLastWaiterCancellationStopsOrphanedColdFill(t *testing.T) {
	started := make(chan struct{})
	providerCanceled := make(chan struct{})
	inner := batchFunc(func(ctx context.Context, _ search.Query) (search.SearchBatch, error) {
		close(started)
		<-ctx.Done()
		close(providerCanceled)
		return search.SearchBatch{}, ctx.Err()
	})
	options := testOptions()
	options.RefreshTimeout = 5 * time.Second
	cached := mustNew(t, inner, options)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := cached.SearchBatch(ctx, search.Query{Q: "orphaned cold fill"})
		done <- err
	}()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("caller error = %v, want context.Canceled", err)
	}
	select {
	case <-providerCanceled:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("last waiter cancellation left the cold provider fill running")
	}
}

func TestSearcherCanceledFlightCannotDeleteReplacement(t *testing.T) {
	var calls atomic.Int32
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	releaseSecond := make(chan struct{})
	inner := batchFunc(func(ctx context.Context, _ search.Query) (search.SearchBatch, error) {
		switch calls.Add(1) {
		case 1:
			close(firstStarted)
			<-releaseFirst // Simulate an upstream that notices cancellation late.
			return healthyBatch("obsolete"), nil
		case 2:
			close(secondStarted)
			select {
			case <-releaseSecond:
				return healthyBatch("replacement"), nil
			case <-ctx.Done():
				return search.SearchBatch{}, ctx.Err()
			}
		default:
			return search.SearchBatch{}, errors.New("unexpected provider call")
		}
	})
	options := testOptions()
	options.MaxInflight = 1
	cached := mustNew(t, inner, options)
	query := search.Query{Q: "replacement race"}

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, err := cached.SearchBatch(firstCtx, query)
		firstDone <- err
	}()
	<-firstStarted
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first error = %v, want context.Canceled", err)
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := cached.SearchBatch(context.Background(), query)
		secondDone <- err
	}()
	<-secondStarted
	close(releaseFirst)

	thirdDone := make(chan error, 1)
	go func() {
		_, err := cached.SearchBatch(context.Background(), query)
		thirdDone <- err
	}()
	assertRemains(t, 25*time.Millisecond, func() bool { return calls.Load() == 2 }, "replacement coalescing after obsolete flight completion")
	close(releaseSecond)
	for _, done := range []<-chan error{secondDone, thirdDone} {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestSearcherWorkerDeadlineIsReturnedAndNotCached(t *testing.T) {
	var calls atomic.Int32
	inner := batchFunc(func(ctx context.Context, _ search.Query) (search.SearchBatch, error) {
		calls.Add(1)
		<-ctx.Done()
		return search.SearchBatch{Provider: "test", Status: search.BatchFailed}, ctx.Err()
	})
	options := testOptions()
	options.RefreshTimeout = 20 * time.Millisecond
	cached := mustNew(t, inner, options)
	for range 2 {
		batch, err := cached.SearchBatch(context.Background(), search.Query{Q: "timeout"})
		if !errors.Is(err, context.DeadlineExceeded) || batch.Status != search.BatchFailed || len(batch.Hits) != 0 {
			t.Fatalf("timeout batch = %+v err=%v", batch, err)
		}
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("provider calls = %d, want 2 without timeout admission", got)
	}
}

func TestSearcherClonesQueryBeforeDetachedExecution(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	observed := make(chan search.Query, 1)
	inner := batchFunc(func(ctx context.Context, q search.Query) (search.SearchBatch, error) {
		close(started)
		select {
		case <-release:
			observed <- q
			return healthyBatch("cloned"), nil
		case <-ctx.Done():
			return search.SearchBatch{}, ctx.Err()
		}
	})
	cached := mustNew(t, inner, testOptions())
	safe := 1
	query := search.Query{Q: "clone", Engines: []string{"mwmbl"}, SafeSearch: &safe}
	done := make(chan error, 1)
	go func() {
		_, err := cached.SearchBatch(context.Background(), query)
		done <- err
	}()
	<-started
	query.Engines[0] = "mutated"
	*query.SafeSearch = 2
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("SearchBatch: %v", err)
	}
	got := <-observed
	if len(got.Engines) != 1 || got.Engines[0] != "mwmbl" || got.SafeSearch == nil || *got.SafeSearch != 1 {
		t.Fatalf("provider observed aliased query: %+v", got)
	}
}

func TestSearcherFailedRefreshKeepsOriginalStaleDeadline(t *testing.T) {
	var calls atomic.Int32
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	inner := batchFunc(func(ctx context.Context, _ search.Query) (search.SearchBatch, error) {
		if calls.Add(1) == 1 {
			return healthyBatch("old"), nil
		}
		close(refreshStarted)
		select {
		case <-releaseRefresh:
			return search.SearchBatch{Status: search.BatchFailed}, errors.New("refresh failed")
		case <-ctx.Done():
			return search.SearchBatch{}, ctx.Err()
		}
	})
	options := testOptions()
	options.FreshTTL = time.Second
	cached := mustNew(t, inner, options)
	var nowNanos atomic.Int64
	nowNanos.Store(time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC).UnixNano())
	cached.now = func() time.Time { return time.Unix(0, nowNanos.Load()) }
	query := search.Query{Q: "keep stale"}
	if _, err := cached.SearchBatch(context.Background(), query); err != nil {
		t.Fatalf("prime SearchBatch: %v", err)
	}
	key := queryKey(query)
	cached.mu.Lock()
	originalDeadline := cached.entries[key].staleUntil
	cached.mu.Unlock()
	nowNanos.Add(int64(2 * time.Second))
	stale, err := cached.SearchBatch(context.Background(), query)
	if err != nil || stale.Hits[0].Title != "old" {
		t.Fatalf("stale SearchBatch = %+v err=%v", stale, err)
	}
	<-refreshStarted
	cached.mu.Lock()
	refresh := cached.inflight[key]
	cached.mu.Unlock()
	if refresh == nil {
		t.Fatal("missing failed refresh flight")
	}
	close(releaseRefresh)
	<-refresh.done
	cached.mu.Lock()
	retained := cached.entries[key]
	cached.mu.Unlock()
	if !retained.staleUntil.Equal(originalDeadline) || retained.batch.Hits[0].Title != "old" {
		t.Fatalf("failed refresh changed stale entry: %+v", retained)
	}
}

func TestSearcherBoundsEntriesWithLRUEviction(t *testing.T) {
	var mu sync.Mutex
	calls := map[string]int{}
	inner := batchFunc(func(_ context.Context, q search.Query) (search.SearchBatch, error) {
		mu.Lock()
		calls[q.Q]++
		mu.Unlock()
		return healthyBatch(q.Q), nil
	})
	options := testOptions()
	options.MaxEntries = 2
	cached := mustNew(t, inner, options)

	for _, query := range []string{"one", "two", "one", "three", "two"} {
		if _, err := cached.SearchBatch(context.Background(), search.Query{Q: query}); err != nil {
			t.Fatalf("SearchBatch(%q): %v", query, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if calls["one"] != 1 || calls["two"] != 2 || calls["three"] != 1 {
		t.Fatalf("provider calls = %+v, want one=1 two=2 three=1", calls)
	}
	if got := cached.entryCount(); got != 2 {
		t.Fatalf("cache entries = %d, want 2", got)
	}
}

func TestSearcherBoundsTotalBytes(t *testing.T) {
	var mu sync.Mutex
	calls := map[string]int{}
	inner := batchFunc(func(_ context.Context, q search.Query) (search.SearchBatch, error) {
		mu.Lock()
		calls[q.Q]++
		mu.Unlock()
		return healthyBatch(q.Q + strings.Repeat("x", 1000)), nil
	})
	options := testOptions()
	options.MaxEntries = 8
	options.MaxEntryBytes = 4 << 10
	options.MaxBytes = 5 << 10
	cached := mustNew(t, inner, options)

	for _, query := range []string{"one", "two", "one"} {
		if _, err := cached.SearchBatch(context.Background(), search.Query{Q: query}); err != nil {
			t.Fatalf("SearchBatch(%q): %v", query, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if calls["one"] != 2 || calls["two"] != 1 {
		t.Fatalf("provider calls = %+v, want byte-bound eviction of one", calls)
	}
	if got := cached.entryCount(); got != 1 {
		t.Fatalf("cache entries = %d, want 1 after byte-bound eviction", got)
	}
}

func TestSearcherBoundsDetachedFlightsAndBypassesWithCallerContext(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var mu sync.Mutex
	calls := map[string]int{}
	inner := batchFunc(func(ctx context.Context, q search.Query) (search.SearchBatch, error) {
		mu.Lock()
		calls[q.Q]++
		mu.Unlock()
		if q.Q == "first" {
			close(firstStarted)
			select {
			case <-releaseFirst:
				return healthyBatch("first"), nil
			case <-ctx.Done():
				return search.SearchBatch{}, ctx.Err()
			}
		}
		return healthyBatch(q.Q), nil
	})
	options := testOptions()
	options.MaxInflight = 1
	cached := mustNew(t, inner, options)
	firstDone := make(chan error, 1)
	go func() {
		_, err := cached.SearchBatch(context.Background(), search.Query{Q: "first"})
		firstDone <- err
	}()
	<-firstStarted

	for range 2 {
		batch, err := cached.SearchBatch(context.Background(), search.Query{Q: "second"})
		if err != nil || len(batch.Hits) != 1 || batch.Hits[0].Title != "second" {
			t.Fatalf("bypass batch = %+v err=%v", batch, err)
		}
	}
	mu.Lock()
	secondCalls := calls["second"]
	mu.Unlock()
	if secondCalls != 2 {
		t.Fatalf("bypass provider calls = %d, want 2 without admission", secondCalls)
	}
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first SearchBatch: %v", err)
	}
}

func TestSearcherInflightGaugeCannotPublishOlderSnapshot(t *testing.T) {
	obs.DiscoveryCacheInflight.Set(0)
	t.Cleanup(func() { obs.DiscoveryCacheInflight.Set(0) })
	providerStarted := make(chan struct{}, 2)
	releaseProviders := make(chan struct{})
	inner := batchFunc(func(ctx context.Context, q search.Query) (search.SearchBatch, error) {
		providerStarted <- struct{}{}
		select {
		case <-releaseProviders:
			return healthyBatch(q.Q), nil
		case <-ctx.Done():
			return search.SearchBatch{}, ctx.Err()
		}
	})
	cached := mustNew(t, inner, testOptions())
	firstMetricBlocked := make(chan struct{})
	releaseMetric := make(chan struct{})
	metricReleased := make(chan struct{})
	var once sync.Once
	cached.beforeInflightMetric = func(count int) {
		if count != 1 {
			return
		}
		once.Do(func() {
			close(firstMetricBlocked)
			<-releaseMetric
			close(metricReleased)
		})
	}
	results := make(chan error, 2)
	go func() {
		_, err := cached.SearchBatch(context.Background(), search.Query{Q: "metric-one"})
		results <- err
	}()
	<-firstMetricBlocked
	go func() {
		_, err := cached.SearchBatch(context.Background(), search.Query{Q: "metric-two"})
		results <- err
	}()
	<-providerStarted
	close(releaseMetric)
	<-metricReleased
	<-providerStarted
	cached.mu.Lock()
	actual := len(cached.inflight)
	cached.mu.Unlock()
	if got := gaugeValue(t, obs.DiscoveryCacheInflight); got != float64(actual) || actual != 2 {
		t.Fatalf("inflight gauge = %v actual = %d, want 2", got, actual)
	}
	close(releaseProviders)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("SearchBatch: %v", err)
		}
	}
}

func TestSearcherUsageGaugesCannotPublishOlderSnapshot(t *testing.T) {
	obs.DiscoveryCacheEntries.Set(0)
	obs.DiscoveryCacheBytes.Set(0)
	t.Cleanup(func() {
		obs.DiscoveryCacheEntries.Set(0)
		obs.DiscoveryCacheBytes.Set(0)
	})
	cached := mustNew(t, legacyFunc(func(context.Context, search.Query) ([]search.Hit, error) { return nil, nil }), testOptions())
	firstMetricBlocked := make(chan struct{})
	releaseMetric := make(chan struct{})
	metricReleased := make(chan struct{})
	var once sync.Once
	cached.beforeUsageMetric = func(entries int, _ int64) {
		if entries != 1 {
			return
		}
		once.Do(func() {
			close(firstMetricBlocked)
			<-releaseMetric
			close(metricReleased)
		})
	}
	first := healthyBatch("usage-one")
	firstSize, _ := cached.cacheable(first)
	second := healthyBatch("usage-two")
	secondSize, _ := cached.cacheable(second)
	firstDone := make(chan struct{})
	go func() {
		cached.store("one", first, firstSize, cached.now())
		close(firstDone)
	}()
	<-firstMetricBlocked
	cached.store("two", second, secondSize, cached.now())
	close(releaseMetric)
	<-metricReleased
	<-firstDone
	cached.mu.Lock()
	actualEntries, actualBytes := len(cached.entries), cached.bytes
	cached.mu.Unlock()
	if got := gaugeValue(t, obs.DiscoveryCacheEntries); got != float64(actualEntries) || actualEntries != 2 {
		t.Fatalf("entry gauge = %v actual = %d, want 2", got, actualEntries)
	}
	if got := gaugeValue(t, obs.DiscoveryCacheBytes); got != float64(actualBytes) {
		t.Fatalf("byte gauge = %v actual = %d", got, actualBytes)
	}
}

func TestSearcherUsageGaugesAggregateIndependentProviderCaches(t *testing.T) {
	obs.DiscoveryCacheEntries.Set(0)
	obs.DiscoveryCacheBytes.Set(0)
	t.Cleanup(func() {
		obs.DiscoveryCacheEntries.Set(0)
		obs.DiscoveryCacheBytes.Set(0)
	})
	firstCache := mustNew(t, legacyFunc(func(context.Context, search.Query) ([]search.Hit, error) { return nil, nil }), testOptions())
	secondCache := mustNew(t, legacyFunc(func(context.Context, search.Query) ([]search.Hit, error) { return nil, nil }), testOptions())
	first := healthyBatch("provider-one")
	firstSize, _ := firstCache.cacheable(first)
	second := healthyBatch("provider-two")
	secondSize, _ := secondCache.cacheable(second)
	firstCache.store("one", first, firstSize, firstCache.now())
	secondCache.store("two", second, secondSize, secondCache.now())
	if got := gaugeValue(t, obs.DiscoveryCacheEntries); got != 2 {
		t.Fatalf("aggregate entry gauge = %v, want 2", got)
	}
	if got := gaugeValue(t, obs.DiscoveryCacheBytes); got != float64(firstSize+secondSize) {
		t.Fatalf("aggregate byte gauge = %v, want %d", got, firstSize+secondSize)
	}
}

func TestQueryKeyIsFixedAndCoversQueryFields(t *testing.T) {
	safe := 1
	base := search.Query{
		Q:              "query",
		Engines:        []string{"a", "b"},
		Categories:     []string{"general"},
		Language:       "en",
		TimeRange:      "day",
		SafeSearch:     &safe,
		IncludeDomains: []string{"example.com"},
		ExcludeDomains: []string{"blocked.example"},
		ExactMatch:     true,
		MaxResults:     10,
	}
	baseKey := queryKey(base)
	if len(baseKey) != 64 {
		t.Fatalf("key length = %d, want 64", len(baseKey))
	}
	mutations := []func(*search.Query){
		func(q *search.Query) { q.Q += "!" },
		func(q *search.Query) { q.Engines = []string{"b", "a"} },
		func(q *search.Query) { q.Categories = []string{"news"} },
		func(q *search.Query) { q.Language = "de" },
		func(q *search.Query) { q.TimeRange = "week" },
		func(q *search.Query) { value := 2; q.SafeSearch = &value },
		func(q *search.Query) { q.IncludeDomains = []string{"other.example"} },
		func(q *search.Query) { q.ExcludeDomains = nil },
		func(q *search.Query) { q.ExactMatch = false },
		func(q *search.Query) { q.MaxResults++ },
	}
	for i, mutate := range mutations {
		candidate := cloneQuery(base)
		mutate(&candidate)
		if got := queryKey(candidate); got == baseKey {
			t.Fatalf("mutation %d did not change query key", i)
		}
	}
	large := base
	large.Q = strings.Repeat("x", 1<<20)
	if got := len(queryKey(large)); got != 64 {
		t.Fatalf("large query key length = %d, want 64", got)
	}
}

func TestSearcherSupportsLegacySearcher(t *testing.T) {
	var calls atomic.Int32
	inner := legacyFunc(func(context.Context, search.Query) ([]search.Hit, error) {
		calls.Add(1)
		return []search.Hit{{URL: "https://example.com", Title: "legacy"}}, nil
	})
	cached := mustNew(t, inner, testOptions())
	for range 2 {
		batch, err := cached.SearchBatch(context.Background(), search.Query{Q: "legacy"})
		if err != nil || batch.Status != search.BatchHealthy || len(batch.Hits) != 1 {
			t.Fatalf("legacy batch = %+v err=%v", batch, err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("legacy provider calls = %d, want 1", got)
	}
}

func TestNewRejectsInvalidOptions(t *testing.T) {
	inner := legacyFunc(func(context.Context, search.Query) ([]search.Hit, error) { return nil, nil })
	tests := []Options{
		{},
		{FreshTTL: time.Second, StaleTTL: time.Second, RefreshTimeout: time.Second, MaxEntries: 1, MaxBytes: 1, MaxEntryBytes: 0, MaxInflight: 1},
		{FreshTTL: time.Second, StaleTTL: time.Second, RefreshTimeout: time.Second, MaxEntries: 1, MaxBytes: 1, MaxEntryBytes: 2, MaxInflight: 1},
	}
	for i, options := range tests {
		if _, err := New(inner, options); err == nil {
			t.Fatalf("case %d: expected validation error", i)
		}
	}
	if _, err := New(nil, testOptions()); err == nil {
		t.Fatal("nil inner: expected validation error")
	}
}

func assertRemains(t *testing.T, duration time.Duration, condition func() bool, label string) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		if !condition() {
			t.Fatalf("%s changed before %v", label, duration)
		}
		time.Sleep(time.Millisecond)
	}
}

func gaugeValue(t *testing.T, gauge interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()
	metric := &dto.Metric{}
	if err := gauge.Write(metric); err != nil {
		t.Fatalf("write metric: %v", err)
	}
	return metric.GetGauge().GetValue()
}
