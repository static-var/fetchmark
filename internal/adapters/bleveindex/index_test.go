package bleveindex

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
	"github.com/staticvar/fetchmark/internal/core/search"
)

func TestExpiredDocumentsAreExcludedFromSearchAndCount(t *testing.T) {
	index := openMemoryIndex(t, 0)
	now := time.Now().UTC()
	expired := now.Add(-time.Minute)
	future := now.Add(time.Hour)
	for _, document := range []localcorpus.Document{
		{URL: "https://example.com/expired", Body: "retention expiry phrase", ExpiresAt: &expired, IndexingDisposition: localcorpus.DispositionPermitted},
		{URL: "https://example.com/current", Body: "retention expiry phrase", ExpiresAt: &future, IndexingDisposition: localcorpus.DispositionPermitted},
	} {
		if err := index.Index(context.Background(), document); err != nil {
			t.Fatal(err)
		}
	}
	hits, err := index.Search(context.Background(), search.Query{Q: "retention expiry", MaxResults: 10})
	if err != nil || len(hits) != 1 || hits[0].URL != "https://example.com/current" {
		t.Fatalf("hits=%+v err=%v", hits, err)
	}
	count, err := index.Count()
	if err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestOpenRejectsIndexWithoutFetchmarkSchemaMarker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.bleve")
	legacy, err := bleve.New(path, bleve.NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Options{Path: path}); !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("Open error = %v, want ErrSchemaMismatch", err)
	}
}

func TestOpenRejectsSymlinkedPersistentIndexPath(t *testing.T) {
	parent := t.TempDir()
	outside := filepath.Join(parent, "outside")
	if err := os.MkdirAll(outside, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(parent, "index")
	if err := os.Symlink(outside, path); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Options{Path: path}); err == nil {
		t.Fatal("Open accepted symlinked index path")
	}
}

func TestIndexBoundsPersistentDocumentCardinalityAndLogicalBytes(t *testing.T) {
	index, err := Open(Options{InMemory: true, MaxDocuments: 1, MaxBytes: 512, MaxDocumentBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })
	first := localcorpus.Document{URL: "https://example.com/one", Body: "first", IndexingDisposition: localcorpus.DispositionPermitted}
	if err := index.Reconcile(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := localcorpus.Document{URL: "https://example.com/two", Body: "second", IndexingDisposition: localcorpus.DispositionPermitted}
	if err := index.Reconcile(context.Background(), second); !errors.Is(err, ErrIndexBudgetExceeded) {
		t.Fatalf("second document error = %v", err)
	}
	if count, err := index.Count(); err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestExpiredIndexEntryReleasesAggregateCapacity(t *testing.T) {
	index, err := Open(Options{InMemory: true, MaxDocuments: 1, MaxBytes: 512, MaxDocumentBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })
	expired := time.Now().UTC().Add(-time.Minute)
	if err := index.Reconcile(context.Background(), localcorpus.Document{
		URL: "https://example.com/expired-capacity", Body: "old", ExpiresAt: &expired,
		IndexingDisposition: localcorpus.DispositionPermitted,
	}); err != nil {
		t.Fatal(err)
	}
	if err := index.Reconcile(context.Background(), localcorpus.Document{
		URL: "https://example.com/replacement-capacity", Body: "new",
		IndexingDisposition: localcorpus.DispositionPermitted,
	}); err != nil {
		t.Fatalf("replacement after expiry = %v", err)
	}
	if count, err := index.Count(); err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestSweepRemovesExactBoundaryAndReleasesCapacity(t *testing.T) {
	sweepAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	future := sweepAt.Add(time.Second)
	documents := []localcorpus.Document{
		{URL: "https://example.com/exact-boundary", Body: "exact expiry", ExpiresAt: &sweepAt, IndexingDisposition: localcorpus.DispositionPermitted},
		{URL: "https://example.com/future-boundary", Body: "future expiry", ExpiresAt: &future, IndexingDisposition: localcorpus.DispositionPermitted},
		{URL: "https://example.com/unbounded", Body: "unbounded expiry", IndexingDisposition: localcorpus.DispositionPermitted},
	}
	maxBytes := int64(0)
	for _, document := range documents {
		maxBytes += int64(documentSize(document))
	}
	index, err := Open(Options{InMemory: true, MaxDocuments: len(documents), MaxBytes: maxBytes, MaxDocumentBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })
	for _, document := range documents {
		if err := index.Reconcile(context.Background(), document); err != nil {
			t.Fatal(err)
		}
	}

	nonUTC := sweepAt.In(time.FixedZone("test-offset", 5*60*60+30*60))
	removed, err := index.Sweep(context.Background(), nonUTC)
	if err != nil || removed != 1 {
		t.Fatalf("Sweep removed=%d err=%v, want 1, nil", removed, err)
	}
	if count, err := index.Count(); err != nil || count != 2 {
		t.Fatalf("count after sweep=%d err=%v, want 2, nil", count, err)
	}
	for _, query := range []string{"future expiry", "unbounded expiry"} {
		hits, err := index.Search(context.Background(), search.Query{Q: query, ExactMatch: true})
		if err != nil || len(hits) != 1 {
			t.Fatalf("Search(%q) hits=%+v err=%v", query, hits, err)
		}
	}
	if err := index.Reconcile(context.Background(), localcorpus.Document{
		URL: "https://x.io/r", Body: "r",
		IndexingDisposition: localcorpus.DispositionPermitted,
	}); err != nil {
		t.Fatalf("replacement after sweep = %v", err)
	}
}

func TestSweepRejectsZeroTimeAndHonorsCancellation(t *testing.T) {
	index := openMemoryIndex(t, 0)
	if _, err := index.Sweep(context.Background(), time.Time{}); err == nil {
		t.Fatal("Sweep accepted a zero time")
	}

	future := time.Now().UTC().Add(time.Hour)
	if err := index.Reconcile(context.Background(), localcorpus.Document{
		URL: "https://example.com/canceled-sweep", Body: "retained after canceled sweep", ExpiresAt: &future,
		IndexingDisposition: localcorpus.DispositionPermitted,
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if removed, err := index.Sweep(ctx, future.Add(time.Hour)); !errors.Is(err, context.Canceled) || removed != 0 {
		t.Fatalf("canceled Sweep removed=%d err=%v", removed, err)
	}
	if count, err := index.Count(); err != nil || count != 1 {
		t.Fatalf("count after canceled sweep=%d err=%v, want 1, nil", count, err)
	}
}

func TestSweepReturnsErrClosed(t *testing.T) {
	index, err := Open(Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Close(); err != nil {
		t.Fatal(err)
	}
	if removed, err := index.Sweep(context.Background(), time.Now()); !errors.Is(err, ErrClosed) || removed != 0 {
		t.Fatalf("closed Sweep removed=%d err=%v", removed, err)
	}
}

func TestPersistentIndexAggregateCapacitySurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bounded.bleve")
	options := Options{Path: path, MaxDocuments: 1, MaxBytes: 512, MaxDocumentBytes: 256}
	index, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Reconcile(context.Background(), localcorpus.Document{
		URL: "https://example.com/persisted-capacity", Body: "first", IndexingDisposition: localcorpus.DispositionPermitted,
	}); err != nil {
		t.Fatal(err)
	}
	if err := index.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if err := reopened.Reconcile(context.Background(), localcorpus.Document{
		URL: "https://example.com/second-capacity", Body: "second", IndexingDisposition: localcorpus.DispositionPermitted,
	}); !errors.Is(err, ErrIndexBudgetExceeded) {
		t.Fatalf("second document after reopen error = %v", err)
	}
}

func TestPersistentIndexRejectsLoweredDocumentCapOnReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lowered-cap.bleve")
	index, err := Open(Options{Path: path, MaxDocuments: 2, MaxBytes: 1024, MaxDocumentBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	for _, rawURL := range []string{"https://example.com/cap-one", "https://example.com/cap-two"} {
		if err := index.Reconcile(context.Background(), localcorpus.Document{
			URL: rawURL, Body: "bounded", IndexingDisposition: localcorpus.DispositionPermitted,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := index.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Options{Path: path, MaxDocuments: 1, MaxBytes: 1024, MaxDocumentBytes: 256}); !errors.Is(err, ErrIndexBudgetExceeded) {
		t.Fatalf("Open with lowered cap error = %v", err)
	}
}

func TestPersistentExpiredEntryReleasesCapacityOnReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "expired-capacity.bleve")
	options := Options{Path: path, MaxDocuments: 1, MaxBytes: 512, MaxDocumentBytes: 256}
	index, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	expired := time.Now().UTC().Add(-time.Minute)
	if err := index.Reconcile(context.Background(), localcorpus.Document{
		URL: "https://example.com/expired-reopen", Body: "expired", ExpiresAt: &expired,
		IndexingDisposition: localcorpus.DispositionPermitted,
	}); err != nil {
		t.Fatal(err)
	}
	if err := index.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if err := reopened.Reconcile(context.Background(), localcorpus.Document{
		URL: "https://example.com/fresh-reopen", Body: "fresh", IndexingDisposition: localcorpus.DispositionPermitted,
	}); err != nil {
		t.Fatalf("fresh document after expired reopen = %v", err)
	}
}

func TestIndexRejectsAnythingWithoutExplicitPermission(t *testing.T) {
	index := openMemoryIndex(t, 0)
	for _, disposition := range []localcorpus.IndexingDisposition{
		"", localcorpus.DispositionUnknown, localcorpus.DispositionRobotsBlocked,
		localcorpus.DispositionNoIndexHeader, localcorpus.DispositionNoIndexMetadata,
		localcorpus.DispositionNoArchiveHeader, localcorpus.DispositionNoArchiveMetadata,
		localcorpus.DispositionTakedown,
	} {
		err := index.Index(context.Background(), localcorpus.Document{
			URL: "https://example.com/", Body: "must not persist", IndexingDisposition: disposition,
		})
		if !errors.Is(err, ErrNotPermitted) {
			t.Fatalf("disposition %q error = %v", disposition, err)
		}
	}
	count, err := index.Count()
	if err != nil || count != 0 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestIndexSearchesStoredMetadataAndFilters(t *testing.T) {
	index := openMemoryIndex(t, 0)
	now := time.Now().UTC().Truncate(time.Second)
	old := now.AddDate(-2, 0, 0)
	documents := []localcorpus.Document{
		{
			URL: "https://go.dev/doc/effective_go?utm_source=test", Title: "Effective Go", Body: "Idiomatic concurrency with goroutines and channels.",
			Language: "en", Author: "Go team", PublishedAt: &now, FetchedAt: now,
			Provenance: []string{"searxng", "local-use"}, MIME: "text/html", ExtractionStatus: "ok",
			IndexingDisposition: localcorpus.DispositionPermitted,
		},
		{
			URL: "https://example.org/old", Title: "Old concurrency note", Body: "Concurrency notes from an old archive.",
			Language: "en", PublishedAt: &old, FetchedAt: now, IndexingDisposition: localcorpus.DispositionPermitted,
		},
	}
	for _, document := range documents {
		if err := index.Index(context.Background(), document); err != nil {
			t.Fatal(err)
		}
	}
	batch, err := index.SearchBatch(context.Background(), search.Query{
		Q: "idiomatic concurrency", IncludeDomains: []string{"go.dev"}, Language: "en", TimeRange: "year", MaxResults: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Status != search.BatchHealthy || batch.Provider != providerID || len(batch.Hits) != 1 {
		t.Fatalf("batch = %+v", batch)
	}
	hit := batch.Hits[0]
	if hit.URL != "https://go.dev/doc/effective_go" || hit.Title != "Effective Go" || hit.PublishedAt == nil || hit.Metadata["content_hash"] == "" || !strings.Contains(hit.Metadata["provenance"], "searxng") {
		t.Fatalf("hit = %+v", hit)
	}
}

func TestIndexReplacesCanonicalURL(t *testing.T) {
	index := openMemoryIndex(t, 0)
	for _, document := range []localcorpus.Document{
		{URL: "https://EXAMPLE.com:443/page#old", Title: "Old", Body: "alpha only", IndexingDisposition: localcorpus.DispositionPermitted},
		{URL: "https://example.com/page", Title: "New", Body: "beta replacement", IndexingDisposition: localcorpus.DispositionPermitted},
	} {
		if err := index.Index(context.Background(), document); err != nil {
			t.Fatal(err)
		}
	}
	count, err := index.Count()
	if err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	hits, err := index.Search(context.Background(), search.Query{Q: "beta", MaxResults: 2})
	if err != nil || len(hits) != 1 || hits[0].Title != "New" {
		t.Fatalf("hits=%+v err=%v", hits, err)
	}
}

func TestReconcileExistingDoesNotCreateAbsentTombstone(t *testing.T) {
	index := openMemoryIndex(t, 0)
	mutated, err := index.ReconcileExisting(context.Background(), localcorpus.Document{
		URL: "https://example.com/absent", FetchedAt: time.Now().UTC(),
		IndexingDisposition: localcorpus.DispositionRobotsBlocked,
	})
	if err != nil || mutated {
		t.Fatalf("mutated=%t err=%v", mutated, err)
	}
	if index.documents != 0 || index.usedBytes != 0 {
		t.Fatalf("usage=(%d documents, %d bytes)", index.documents, index.usedBytes)
	}
}

func TestReconcileExistingAtomicallyRevokesRetainedDocument(t *testing.T) {
	index := openMemoryIndex(t, 0)
	base := time.Now().UTC().Add(-time.Minute)
	if err := index.Reconcile(context.Background(), localcorpus.Document{
		URL: "https://example.com/retained", Body: "retained lexical body", FetchedAt: base,
		IndexingDisposition: localcorpus.DispositionPermitted,
	}); err != nil {
		t.Fatal(err)
	}
	mutated, err := index.ReconcileExisting(context.Background(), localcorpus.Document{
		URL: "https://example.com/retained", FetchedAt: base.Add(time.Second),
		IndexingDisposition: localcorpus.DispositionNoIndexHeader,
	})
	if err != nil || !mutated {
		t.Fatalf("mutated=%t err=%v", mutated, err)
	}
	batch, err := index.SearchBatch(context.Background(), search.Query{Q: "retained", MaxResults: 1})
	if err != nil || len(batch.Hits) != 0 {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
	if index.documents != 1 {
		t.Fatalf("documents=%d, want one bounded tombstone", index.documents)
	}
}

func TestIndexRemovesPreviouslyPermittedDocumentOnRevocation(t *testing.T) {
	index := openMemoryIndex(t, 0)
	document := localcorpus.Document{
		URL: "https://example.com/revoked", Body: "previously searchable text", IndexingDisposition: localcorpus.DispositionPermitted,
	}
	if err := index.Index(context.Background(), document); err != nil {
		t.Fatal(err)
	}
	document.Body = ""
	document.IndexingDisposition = localcorpus.DispositionNoIndexMetadata
	if err := index.Index(context.Background(), document); !errors.Is(err, ErrNotPermitted) {
		t.Fatalf("revocation error = %v", err)
	}
	count, err := index.Count()
	if err != nil || count != 0 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestNewerRevocationCannotBeOverwrittenByStalePermittedObservation(t *testing.T) {
	index := openMemoryIndex(t, 0)
	base := time.Now().UTC().Add(-time.Hour)
	permitted := localcorpus.Document{
		URL: "https://example.com/ordered", Body: "searchable before revocation", FetchedAt: base,
		IndexingDisposition: localcorpus.DispositionPermitted,
	}
	if err := index.Index(context.Background(), permitted); err != nil {
		t.Fatal(err)
	}
	revoked := permitted
	revoked.Body = ""
	revoked.FetchedAt = base.Add(2 * time.Minute)
	revoked.IndexingDisposition = localcorpus.DispositionNoIndexHeader
	if err := index.Index(context.Background(), revoked); !errors.Is(err, ErrNotPermitted) {
		t.Fatalf("revocation error = %v", err)
	}
	permitted.FetchedAt = base.Add(time.Minute)
	if err := index.Index(context.Background(), permitted); !errors.Is(err, ErrStaleObservation) {
		t.Fatalf("stale permitted error = %v", err)
	}
	count, err := index.Count()
	if err != nil || count != 0 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	hits, err := index.Search(context.Background(), search.Query{Q: "searchable"})
	if err != nil || len(hits) != 0 {
		t.Fatalf("hits=%+v err=%v", hits, err)
	}
}

func TestRevocationOrderingPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ordered.bleve")
	index, err := Open(Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Add(-time.Hour)
	document := localcorpus.Document{
		URL: "https://example.com/persisted-revocation", Body: "must stay revoked", FetchedAt: base,
		IndexingDisposition: localcorpus.DispositionPermitted,
	}
	if err := index.Reconcile(context.Background(), document); err != nil {
		t.Fatal(err)
	}
	document.Body = ""
	document.FetchedAt = base.Add(2 * time.Minute)
	document.IndexingDisposition = localcorpus.DispositionNoIndexMetadata
	if err := index.Reconcile(context.Background(), document); err != nil {
		t.Fatal(err)
	}
	if err := index.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	document.Body = "stale permitted retry"
	document.FetchedAt = base.Add(time.Minute)
	document.IndexingDisposition = localcorpus.DispositionPermitted
	if err := reopened.Reconcile(context.Background(), document); !errors.Is(err, ErrStaleObservation) {
		t.Fatalf("stale reconcile error = %v", err)
	}
	count, err := reopened.Count()
	if err != nil || count != 0 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestTakedownRemainsStickyAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "takedown.bleve")
	index, err := Open(Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Index(context.Background(), localcorpus.Document{
		URL: "https://example.com/takedown", Body: "remove this", IndexingDisposition: localcorpus.DispositionPermitted,
	}); err != nil {
		t.Fatal(err)
	}
	if err := index.Delete(context.Background(), "https://example.com/takedown"); err != nil {
		t.Fatal(err)
	}
	if err := index.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if err := reopened.Reconcile(context.Background(), localcorpus.Document{
		URL: "https://example.com/takedown", Body: "later routine fetch", FetchedAt: time.Now().UTC().Add(time.Hour),
		IndexingDisposition: localcorpus.DispositionPermitted,
	}); !errors.Is(err, ErrStaleObservation) {
		t.Fatalf("post-takedown reconcile error = %v", err)
	}
	count, err := reopened.Count()
	if err != nil || count != 0 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestSearchAppliesFiltersBeforeResultLimit(t *testing.T) {
	index := openMemoryIndex(t, 0)
	for i := 0; i < 50; i++ {
		host := "noise.example"
		path := "/irrelevant"
		if i == 49 {
			host = "docs.example"
			path = "/guide/retained"
		}
		if err := index.Index(context.Background(), localcorpus.Document{
			URL: "https://" + host + path + "?id=" + strconv.Itoa(i), Body: "common filtered phrase", Language: "en",
			IndexingDisposition: localcorpus.DispositionPermitted,
		}); err != nil {
			t.Fatal(err)
		}
	}
	hits, err := index.Search(context.Background(), search.Query{
		Q: "common filtered phrase", IncludeDomains: []string{"docs.example/guide"}, Language: "en", MaxResults: 1,
	})
	if err != nil || len(hits) != 1 || !strings.Contains(hits[0].URL, "docs.example/guide/retained") {
		t.Fatalf("hits=%+v err=%v", hits, err)
	}
}

func TestIndexPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fetchmark.bleve")
	index, err := Open(Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Index(context.Background(), localcorpus.Document{
		URL: "https://example.com/persist", Title: "Persistent", Body: "durable lexical corpus",
		IndexingDisposition: localcorpus.DispositionPermitted,
	}); err != nil {
		t.Fatal(err)
	}
	if err := index.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	hits, err := reopened.Search(context.Background(), search.Query{Q: "durable corpus"})
	if err != nil || len(hits) != 1 || hits[0].URL != "https://example.com/persist" {
		t.Fatalf("hits=%+v err=%v", hits, err)
	}
}

func TestIndexEnforcesDocumentBudgetAndCancellation(t *testing.T) {
	index := openMemoryIndex(t, 8)
	err := index.Index(context.Background(), localcorpus.Document{
		URL: "https://example.com/large", Body: "123456789", IndexingDisposition: localcorpus.DispositionPermitted,
	})
	if err == nil || !strings.Contains(err.Error(), "exceeds 8 bytes") {
		t.Fatalf("budget error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = index.Index(ctx, localcorpus.Document{
		URL: "https://example.com/cancel", Body: "small", IndexingDisposition: localcorpus.DispositionPermitted,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
}

func TestIndexBudgetIncludesMetadataAndListCardinality(t *testing.T) {
	index := openMemoryIndex(t, 64)
	err := index.Index(context.Background(), localcorpus.Document{
		URL: "https://example.com/meta", Title: strings.Repeat("x", 100), Body: "tiny",
		IndexingDisposition: localcorpus.DispositionPermitted,
	})
	if err == nil || !strings.Contains(err.Error(), "exceeds 64 bytes") {
		t.Fatalf("metadata budget error = %v", err)
	}

	index = openMemoryIndex(t, 1<<20)
	err = index.Index(context.Background(), localcorpus.Document{
		URL: "https://example.com/lists", Body: "tiny", Headings: make([]string, maxDocumentListItems+1),
		IndexingDisposition: localcorpus.DispositionPermitted,
	})
	if err == nil || !strings.Contains(err.Error(), "list items") {
		t.Fatalf("list budget error = %v", err)
	}
}

func TestSafeSearchExcludesUnclassifiedDocuments(t *testing.T) {
	index := openMemoryIndex(t, 0)
	for _, document := range []localcorpus.Document{
		{URL: "https://example.com/unknown", Body: "safety classification example", IndexingDisposition: localcorpus.DispositionPermitted},
		{URL: "https://example.com/safe", Body: "safety classification example", SafetyClassification: "safe", IndexingDisposition: localcorpus.DispositionPermitted},
	} {
		if err := index.Index(context.Background(), document); err != nil {
			t.Fatal(err)
		}
	}
	strict := 2
	hits, err := index.Search(context.Background(), search.Query{Q: "safety classification", SafeSearch: &strict, MaxResults: 10})
	if err != nil || len(hits) != 1 || hits[0].URL != "https://example.com/safe" {
		t.Fatalf("hits=%+v err=%v", hits, err)
	}
}

func TestIndexRejectsUnknownSafetyClassification(t *testing.T) {
	index := openMemoryIndex(t, 0)
	err := index.Reconcile(context.Background(), localcorpus.Document{
		URL: "https://example.com/unknown-safety", Body: "body", SafetyClassification: "guessed",
		IndexingDisposition: localcorpus.DispositionPermitted,
	})
	if err == nil || !strings.Contains(err.Error(), "unknown safety classification") {
		t.Fatalf("error = %v", err)
	}
}

func TestEmptyIndexReturnsAuthoritativeEmpty(t *testing.T) {
	index := openMemoryIndex(t, 0)
	batch, err := index.SearchBatch(context.Background(), search.Query{Q: "nothing"})
	if err != nil || batch.Status != search.BatchAuthoritativeEmpty || batch.Provider != providerID || len(batch.Hits) != 0 {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
}

func openMemoryIndex(t *testing.T, maxDocumentBytes int) *Index {
	t.Helper()
	index, err := Open(Options{InMemory: true, MaxDocumentBytes: maxDocumentBytes})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := index.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return index
}
