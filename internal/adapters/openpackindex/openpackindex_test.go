package openpackindex

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/klauspost/compress/zstd"

	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/search"
)

type packFixture struct {
	bundle        string
	root          string
	manifest      indexpack.Manifest
	manifestRaw   []byte
	signatureRaw  []byte
	publicKey     ed25519.PublicKey
	privateKey    ed25519.PrivateKey
	acceptance    indexpack.Acceptance
	compressedRaw []byte
}

func TestInstallOpenSearchAllFieldsAndMetadata(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	fixture := newPackFixture(t, []indexpack.Record{
		testRecord(now, "https://docs.example.com/guide/one", "titletoken", []string{"headingtoken"}, []string{"anchortoken"}, "sketchtoken first"),
		testRecord(now, "https://other.example.net/second", "another", []string{"Elsewhere"}, []string{"reference"}, "second document"),
	}, nil)
	installed := installFixture(t, fixture)
	if installed.ManifestSHA256 != fixture.acceptance.ExpectedManifestSHA256 || installed.RecordCount != 2 {
		t.Fatalf("Install = %#v", installed)
	}
	if installed.ProjectionBytes == 0 || installed.ProjectionBytes > MaxProjectionBytes {
		t.Fatalf("projection bytes = %d", installed.ProjectionBytes)
	}
	index := openInstalled(t, installed, fixture, "pack-provider")
	t.Cleanup(func() { _ = index.Close() })

	for _, term := range []string{"titletoken", "headingtoken", "anchortoken", "sketchtoken"} {
		batch, err := index.SearchBatch(context.Background(), search.Query{Q: term, MaxResults: 5})
		if err != nil {
			t.Fatalf("SearchBatch(%q): %v", term, err)
		}
		if len(batch.Hits) != 1 || batch.Hits[0].URL != "https://docs.example.com/guide/one" {
			t.Fatalf("SearchBatch(%q) = %#v", term, batch)
		}
		if batch.Provider != "pack-provider" || batch.Instance != "developer-en@1" || batch.Status != search.BatchHealthy {
			t.Fatalf("SearchBatch(%q) identity = %#v", term, batch)
		}
		hit := batch.Hits[0]
		if hit.Snippet != "sketchtoken first" || hit.Metadata["manifest_sha256"] != installed.ManifestSHA256 ||
			hit.Metadata["provider"] != "pack-provider" || hit.Metadata["source_id"] != "pack-provider" ||
			!strings.Contains(hit.Metadata["provenance"], "fixture-source") {
			t.Fatalf("SearchBatch(%q) metadata = %#v", term, hit)
		}
	}
	if err := index.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := index.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	batch, err := index.SearchBatch(context.Background(), search.Query{Q: "titletoken"})
	if !errors.Is(err, ErrClosed) || batch.Status != search.BatchFailed || len(batch.Diagnostics) != 1 {
		t.Fatalf("closed SearchBatch = %#v, %v", batch, err)
	}
}

func TestInstallSearchURLOnlyV2RecordFromLocalURLTerms(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	record := testRecord(now, "https://docs.example.com/guides/open-search-v2", "", nil, nil, "")
	record.PublishedAt = ""
	record.ContentSHA256 = ""
	record.AuthorityScore = 0
	fixture := newPackFixture(t, []indexpack.Record{record}, func(manifest *indexpack.Manifest, _ *[]byte) {
		manifest.Version = indexpack.VersionURLMetadata
		manifest.Build.CandidateSHA256 = strings.Repeat("8", 64)
	})
	installed := installFixture(t, fixture)
	index := openInstalled(t, installed, fixture, "url-pack")
	defer index.Close()

	for _, query := range []string{"docs", "guides", "search", "v2"} {
		batch, err := index.SearchBatch(context.Background(), search.Query{Q: query})
		if err != nil || len(batch.Hits) != 1 || batch.Hits[0].URL != record.URL || batch.Hits[0].Title != "" {
			t.Fatalf("URL-only SearchBatch(%q) = %#v, %v", query, batch, err)
		}
	}
}

func TestApplyDeltaMaterializesNewProjectionWithoutMutatingParent(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	parent := newPackFixture(t, []indexpack.Record{
		testRecord(now, "https://example.com/a", "oldtoken alpha", nil, nil, "oldtoken alpha"),
		testRecord(now, "https://example.com/b", "stable beta", nil, nil, "stable beta"),
		testRecord(now, "https://example.com/c", "remove gamma", nil, nil, "remove gamma"),
	}, nil)
	parentInstalled := installFixture(t, parent)
	parentBefore := directorySnapshot(t, parentInstalled.Path)
	replacement := testRecord(now, "https://example.com/a", "newtoken alpha", nil, nil, "newtoken alpha")
	addition := testRecord(now, "https://example.com/d", "added delta", nil, nil, "added delta")
	delta := newDeltaPackFixture(t, parent, []indexpack.Record{
		replacement,
		{Operation: indexpack.OperationTombstone, URL: "https://example.com/c"},
		addition,
	}, nil)
	installed, err := ApplyDelta(context.Background(), DeltaInstallOptions{
		ParentBundleDir: parent.bundle, BundleDir: delta.bundle, Root: parent.root,
		TrustedKeys:     map[string]ed25519.PublicKey{indexpack.KeyID(parent.publicKey): parent.publicKey},
		DeltaAcceptance: delta.acceptance, ExpectedParentManifestSHA256: parentInstalled.ManifestSHA256,
		ExpectedProjectionRecordCount: 3,
	})
	if err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	if installed.RecordCount != 3 || installed.OperationCount != 3 || installed.Revision != 2 ||
		installed.ManifestSHA256 != delta.acceptance.ExpectedManifestSHA256 {
		t.Fatalf("ApplyDelta = %#v", installed)
	}
	if after := directorySnapshot(t, parentInstalled.Path); !reflect.DeepEqual(after, parentBefore) {
		t.Fatalf("parent projection bytes changed:\nbefore=%v\nafter=%v", parentBefore, after)
	}
	updated := openInstalled(t, installed, delta, "delta-pack")
	defer updated.Close()
	for query, want := range map[string][]string{
		"newtoken":     {"https://example.com/a"},
		"stable beta":  {"https://example.com/b"},
		"added delta":  {"https://example.com/d"},
		"oldtoken":     nil,
		"remove gamma": nil,
	} {
		batch, searchErr := updated.SearchBatch(context.Background(), search.Query{Q: query, MaxResults: 10})
		if searchErr != nil || !slices.Equal(hitURLs(batch.Hits), want) {
			t.Fatalf("updated SearchBatch(%q) = %v, %v; want %v", query, hitURLs(batch.Hits), searchErr, want)
		}
	}

	original := openInstalled(t, parentInstalled, parent, "parent-pack")
	defer original.Close()
	batch, err := original.SearchBatch(context.Background(), search.Query{Q: "oldtoken remove gamma", MaxResults: 10})
	if err != nil || len(batch.Hits) != 2 {
		t.Fatalf("parent projection changed: %#v, %v", batch, err)
	}
}

func TestApplyDeltaRejectsInvalidStateTransactionally(t *testing.T) {
	tests := []struct {
		name          string
		deltaRecords  func(time.Time) []indexpack.Record
		mutateDelta   func(*indexpack.Manifest, *[]byte)
		mutateParent  bool
		expectedCount uint64
		cancel        bool
	}{
		{name: "absent tombstone", deltaRecords: func(time.Time) []indexpack.Record {
			return []indexpack.Record{{Operation: indexpack.OperationTombstone, URL: "https://example.com/missing"}}
		}, expectedCount: 2},
		{name: "wrong projection count", deltaRecords: func(now time.Time) []indexpack.Record {
			return []indexpack.Record{testRecord(now, "https://example.com/c", "new c", nil, nil, "new c")}
		}, expectedCount: 2},
		{name: "expiry extension", deltaRecords: func(now time.Time) []indexpack.Record {
			return []indexpack.Record{testRecord(now, "https://example.com/c", "new c", nil, nil, "new c")}
		}, mutateDelta: func(manifest *indexpack.Manifest, _ *[]byte) {
			manifest.ExpiresAt = mustTime(t, manifest.ExpiresAt).Add(2 * time.Hour).Format(time.RFC3339)
		}, expectedCount: 3},
		{name: "corrupt parent shard", deltaRecords: func(now time.Time) []indexpack.Record {
			return []indexpack.Record{testRecord(now, "https://example.com/c", "new c", nil, nil, "new c")}
		}, mutateParent: true, expectedCount: 3},
		{name: "canceled", deltaRecords: func(now time.Time) []indexpack.Record {
			return []indexpack.Record{testRecord(now, "https://example.com/c", "new c", nil, nil, "new c")}
		}, expectedCount: 3, cancel: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Second)
			parent := newPackFixture(t, []indexpack.Record{
				testRecord(now, "https://example.com/a", "alpha", nil, nil, "alpha"),
				testRecord(now, "https://example.com/b", "beta", nil, nil, "beta"),
			}, nil)
			parentInstalled := installFixture(t, parent)
			parentBefore := directorySnapshot(t, parentInstalled.Path)
			delta := newDeltaPackFixture(t, parent, test.deltaRecords(now), test.mutateDelta)
			if test.mutateParent {
				path := filepath.Join(parent.bundle, filepath.FromSlash(parent.manifest.Shards[0].Path))
				raw := append([]byte(nil), parent.compressedRaw...)
				raw[len(raw)-1] ^= 1
				if err := os.WriteFile(path, raw, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			if test.cancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			_, err := ApplyDelta(ctx, DeltaInstallOptions{
				ParentBundleDir: parent.bundle, BundleDir: delta.bundle, Root: parent.root,
				TrustedKeys:     map[string]ed25519.PublicKey{indexpack.KeyID(parent.publicKey): parent.publicKey},
				DeltaAcceptance: delta.acceptance, ExpectedParentManifestSHA256: parentInstalled.ManifestSHA256,
				ExpectedProjectionRecordCount: test.expectedCount,
			})
			if err == nil {
				t.Fatal("ApplyDelta unexpectedly succeeded")
			}
			assertNoActiveProjection(t, delta)
			if after := directorySnapshot(t, parentInstalled.Path); !reflect.DeepEqual(after, parentBefore) {
				t.Fatalf("failed delta changed parent projection: before=%v after=%v", parentBefore, after)
			}
		})
	}
}

func TestApplyDeltaCancellationAfterMeasurementDoesNotActivate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	parent := newPackFixture(t, []indexpack.Record{
		testRecord(now, "https://example.com/a", "alpha", nil, nil, "alpha"),
	}, nil)
	delta := newDeltaPackFixture(t, parent, []indexpack.Record{
		testRecord(now, "https://example.com/b", "beta", nil, nil, "beta"),
	}, nil)
	parentRoot, deltaRoot := openDeltaFixtureRoots(t, parent, delta)
	ctx, cancel := context.WithCancel(context.Background())
	measure := func(ctx context.Context, path string, maximum uint64) (uint64, error) {
		measured, err := measureProjectionBytes(ctx, path, maximum)
		cancel()
		return measured, err
	}

	_, err := applyDeltaAndClose(ctx, parentRoot, deltaRoot, deltaOptions(parent, delta, 2), func() error {
		return errors.Join(deltaRoot.Close(), parentRoot.Close())
	}, os.RemoveAll, verifyClosedProjection, measure, productionInstallPolicy())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("applyDeltaAndClose error = %v, want context.Canceled", err)
	}
	assertNoActiveProjection(t, delta)
	assertNoStagingDirectories(t, parent.root, ".delta-")
}

func TestApplyDeltaCommittedRenameIsAuthoritativeOverCloseError(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	parent := newPackFixture(t, []indexpack.Record{
		testRecord(now, "https://example.com/a", "alpha", nil, nil, "alpha"),
	}, nil)
	delta := newDeltaPackFixture(t, parent, []indexpack.Record{
		testRecord(now, "https://example.com/b", "beta", nil, nil, "beta"),
	}, nil)
	parentRoot, deltaRoot := openDeltaFixtureRoots(t, parent, delta)
	installed, err := applyDeltaAndClose(context.Background(), parentRoot, deltaRoot, deltaOptions(parent, delta, 2), func() error {
		return errors.Join(deltaRoot.Close(), parentRoot.Close(), errors.New("synthetic close failure"))
	}, os.RemoveAll, verifyClosedProjection, measureProjectionBytes, productionInstallPolicy())
	if err != nil {
		t.Fatalf("committed delta reported failure: %v", err)
	}
	if installed.ManifestSHA256 != delta.acceptance.ExpectedManifestSHA256 {
		t.Fatalf("installed = %#v", installed)
	}
	if info, statErr := os.Stat(installed.Path); statErr != nil || !info.IsDir() {
		t.Fatalf("committed path: %v", statErr)
	}
}

func TestApplyDeltaRacingDestinationRemainsUntouched(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	parent := newPackFixture(t, []indexpack.Record{
		testRecord(now, "https://example.com/a", "alpha", nil, nil, "alpha"),
	}, nil)
	delta := newDeltaPackFixture(t, parent, []indexpack.Record{
		testRecord(now, "https://example.com/b", "beta", nil, nil, "beta"),
	}, nil)
	parentRoot, deltaRoot := openDeltaFixtureRoots(t, parent, delta)
	destination := filepath.Join(parent.root, "objects", delta.acceptance.ExpectedManifestSHA256)
	sentinel := filepath.Join(destination, "sentinel")
	measure := func(ctx context.Context, path string, maximum uint64) (uint64, error) {
		measured, err := measureProjectionBytes(ctx, path, maximum)
		if err != nil {
			return 0, err
		}
		if err := os.MkdirAll(destination, 0o700); err != nil {
			return 0, err
		}
		if err := os.WriteFile(sentinel, []byte("do not replace"), 0o600); err != nil {
			return 0, err
		}
		return measured, nil
	}

	_, err := applyDeltaAndClose(context.Background(), parentRoot, deltaRoot, deltaOptions(parent, delta, 2), func() error {
		return errors.Join(deltaRoot.Close(), parentRoot.Close())
	}, os.RemoveAll, verifyClosedProjection, measure, productionInstallPolicy())
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("applyDeltaAndClose error = %v, want ErrAlreadyExists", err)
	}
	raw, readErr := os.ReadFile(sentinel)
	if readErr != nil || string(raw) != "do not replace" {
		t.Fatalf("racing destination changed: %q, %v", raw, readErr)
	}
	entries, readDirErr := os.ReadDir(destination)
	if readDirErr != nil || len(entries) != 1 || entries[0].Name() != "sentinel" {
		t.Fatalf("racing destination entries = %v, %v", entries, readDirErr)
	}
	assertNoStagingDirectories(t, parent.root, ".delta-")
}

func TestApplyDeltaPreexistingDestinationRemainsUntouched(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	parent := newPackFixture(t, []indexpack.Record{
		testRecord(now, "https://example.com/a", "alpha", nil, nil, "alpha"),
	}, nil)
	delta := newDeltaPackFixture(t, parent, []indexpack.Record{
		testRecord(now, "https://example.com/b", "beta", nil, nil, "beta"),
	}, nil)
	destination := filepath.Join(parent.root, "objects", delta.acceptance.ExpectedManifestSHA256)
	sentinel := filepath.Join(destination, "sentinel")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sentinel, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := ApplyDelta(context.Background(), deltaOptions(parent, delta, 2))
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("ApplyDelta error = %v, want ErrAlreadyExists", err)
	}
	raw, readErr := os.ReadFile(sentinel)
	if readErr != nil || string(raw) != "existing" {
		t.Fatalf("preexisting destination changed: %q, %v", raw, readErr)
	}
	entries, readDirErr := os.ReadDir(destination)
	if readDirErr != nil || len(entries) != 1 || entries[0].Name() != "sentinel" {
		t.Fatalf("preexisting destination entries = %v, %v", entries, readDirErr)
	}
}

func TestSearchRechecksManifestValidityWindow(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	fixture := newPackFixture(t, []indexpack.Record{
		testRecord(now, "https://example.com/validity", "validity token", nil, nil, "validity token"),
	}, nil)
	installed := installFixture(t, fixture)
	index := openInstalled(t, installed, fixture, "pack-provider")
	defer index.Close()

	for _, current := range []time.Time{
		mustTime(t, fixture.manifest.CreatedAt).Add(-time.Second),
		mustTime(t, fixture.manifest.ExpiresAt),
		mustTime(t, fixture.manifest.ExpiresAt).Add(time.Hour),
	} {
		index.now = func() time.Time { return current }
		batch, err := index.SearchBatch(context.Background(), search.Query{Q: "validity token"})
		if !errors.Is(err, ErrOutsideValidity) || batch.Status != search.BatchFailed || len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Reason != "outside_validity" {
			t.Fatalf("SearchBatch at %v = %#v, %v", current, batch, err)
		}
	}
}

func TestOpenReadsEarlierV1ProjectionWithDocValues(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	record := testRecord(now, "https://docs.example.com/legacy", "legacy compatibility", nil, nil, "legacy compatibility")
	fixture := newPackFixture(t, []indexpack.Record{record}, nil)
	path := filepath.Join(fixture.root, "objects", fixture.acceptance.ExpectedManifestSHA256)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	legacyMapping, ok := projectionMapping().(*mapping.IndexMappingImpl)
	if !ok {
		t.Fatalf("projection mapping is %T", projectionMapping())
	}
	for _, property := range legacyMapping.DefaultMapping.Properties {
		for _, field := range property.Fields {
			field.DocValues = true
		}
	}
	projection, err := bleve.New(filepath.Join(path, projectionDirectoryName), legacyMapping)
	if err != nil {
		t.Fatal(err)
	}
	document, err := makeStoredDocument(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := projection.Index(documentID(record.URL), document); err != nil {
		t.Fatal(err)
	}
	if err := projection.Index(markerDocumentID, projectionMarker{
		MarkerKind: "open-pack-projection", SchemaVersion: "1",
		ManifestSHA256: fixture.acceptance.ExpectedManifestSHA256,
		PackID:         fixture.manifest.PackID, Revision: strconv.FormatUint(fixture.manifest.Revision, 10),
		RecordCount: "1", SigningKeyID: fixture.acceptance.ExpectedKeyID,
		CreatedAt: fixture.manifest.CreatedAt, ExpiresAt: fixture.manifest.ExpiresAt,
	}); err != nil {
		t.Fatal(err)
	}
	if err := projection.Close(); err != nil {
		t.Fatal(err)
	}

	index := openInstalled(t, Installed{
		Path: path, ManifestSHA256: fixture.acceptance.ExpectedManifestSHA256,
		PackID: fixture.manifest.PackID, Revision: fixture.manifest.Revision, RecordCount: 1,
	}, fixture, "legacy-pack")
	defer index.Close()
	batch, err := index.SearchBatch(context.Background(), search.Query{
		Q: "legacy compatibility", IncludeDomains: []string{"docs.example.com"}, Language: "en", TimeRange: "year",
	})
	if err != nil || len(batch.Hits) != 1 || batch.Hits[0].URL != record.URL || batch.Hits[0].Title != record.Title {
		t.Fatalf("legacy SearchBatch = %#v, %v", batch, err)
	}
}

func TestSearchFiltersSafeSearchExactMaxAndEngineSelection(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	old := now.AddDate(-2, 0, 0)
	records := []indexpack.Record{
		testRecord(now, "https://docs.example.com/guide/alpha", "alpha beta exact", nil, nil, "fresh english"),
		testRecord(now, "https://docs.example.com/private/alpha", "alpha gamma beta", nil, nil, "fresh private"),
		testRecord(now, "https://fr.example.org/alpha", "alpha beta french", nil, nil, "fraiche"),
		testRecord(now, "https://old.example.net/alpha", "alpha beta old", nil, nil, "old item"),
	}
	records[2].Language = "fr"
	records[3].PublishedAt = old.Format(time.RFC3339)
	fixture := newPackFixture(t, records, nil)
	installed := installFixture(t, fixture)
	index := openInstalled(t, installed, fixture, "pack-provider")
	defer index.Close()

	tests := []struct {
		name  string
		query search.Query
		want  []string
	}{
		{name: "include domain path", query: search.Query{Q: "alpha", IncludeDomains: []string{"docs.example.com/guide"}}, want: []string{"https://docs.example.com/guide/alpha"}},
		{name: "exclude path", query: search.Query{Q: "alpha", IncludeDomains: []string{"docs.example.com"}, ExcludeDomains: []string{"docs.example.com/private"}}, want: []string{"https://docs.example.com/guide/alpha"}},
		{name: "language", query: search.Query{Q: "alpha", Language: "fr"}, want: []string{"https://fr.example.org/alpha"}},
		{name: "fresh", query: search.Query{Q: "alpha", TimeRange: "year"}, want: []string{"https://docs.example.com/guide/alpha", "https://docs.example.com/private/alpha", "https://fr.example.org/alpha"}},
		{name: "exact phrase", query: search.Query{Q: "alpha beta", ExactMatch: true}, want: []string{"https://docs.example.com/guide/alpha", "https://fr.example.org/alpha", "https://old.example.net/alpha"}},
		{name: "engine mismatch", query: search.Query{Q: "alpha", Engines: []string{"some-other-provider"}}, want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			batch, err := index.SearchBatch(context.Background(), test.query)
			if err != nil {
				t.Fatalf("SearchBatch: %v", err)
			}
			got := hitURLs(batch.Hits)
			if !sameStringSet(got, test.want) {
				t.Fatalf("URLs = %v, want %v", got, test.want)
			}
			if len(test.want) == 0 && batch.Status != search.BatchAuthoritativeEmpty {
				t.Fatalf("empty status = %s", batch.Status)
			}
		})
	}

	strict := 1
	batch, err := index.SearchBatch(context.Background(), search.Query{Q: "alpha", SafeSearch: &strict})
	if err != nil || len(batch.Hits) != 0 || batch.Status != search.BatchAuthoritativeEmpty {
		t.Fatalf("safe-search fail closed = %#v, %v", batch, err)
	}
	batch, err = index.SearchBatch(context.Background(), search.Query{Q: "alpha", MaxResults: 1})
	if err != nil || len(batch.Hits) != 1 {
		t.Fatalf("max-results = %#v, %v", batch, err)
	}
}

func TestAuthorityAndFreshnessScoresDoNotAffectRanking(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	low := testRecord(now, "https://a.example.com/equal", "neutral identical terms", nil, nil, "same sketch")
	high := testRecord(now, "https://z.example.com/equal", "neutral identical terms", nil, nil, "same sketch")
	low.AuthorityScore, low.FreshnessScore = 1, 1
	high.AuthorityScore, high.FreshnessScore = 10_000, 10_000
	fixture := newPackFixture(t, []indexpack.Record{low, high}, nil)
	installed := installFixture(t, fixture)
	index := openInstalled(t, installed, fixture, "")
	defer index.Close()
	batch, err := index.SearchBatch(context.Background(), search.Query{Q: "neutral identical terms", MaxResults: 10})
	if err != nil || len(batch.Hits) != 2 {
		t.Fatalf("SearchBatch = %#v, %v", batch, err)
	}
	if batch.Hits[0].Metadata["lexical_score"] != batch.Hits[1].Metadata["lexical_score"] {
		t.Fatalf("publisher scores changed lexical rank: %#v", batch.Hits)
	}
	if batch.Hits[0].Metadata["authority_score"] == batch.Hits[1].Metadata["authority_score"] {
		t.Fatalf("authority metadata was not preserved: %#v", batch.Hits)
	}
}

func TestInstallRejectsAcceptanceMismatchesAndTimeBounds(t *testing.T) {
	fixture := newPackFixture(t, []indexpack.Record{testRecord(time.Now().UTC().Truncate(time.Second), "https://example.com/one", "one", nil, nil, "one")}, nil)
	tests := []struct {
		name   string
		mutate func(*InstallOptions)
	}{
		{name: "wrong manifest digest", mutate: func(options *InstallOptions) { options.Acceptance.ExpectedManifestSHA256 = strings.Repeat("f", 64) }},
		{name: "wrong pack", mutate: func(options *InstallOptions) { options.Acceptance.ExpectedPackID = "wrong-pack" }},
		{name: "wrong revision", mutate: func(options *InstallOptions) { options.Acceptance.ExpectedRevision = 2 }},
		{name: "wrong record count", mutate: func(options *InstallOptions) { options.Acceptance.ExpectedRecordCount++ }},
		{name: "wrong created at", mutate: func(options *InstallOptions) { options.Acceptance.ExpectedCreatedAt = "2026-07-17T00:00:00Z" }},
		{name: "wrong expires at", mutate: func(options *InstallOptions) { options.Acceptance.ExpectedExpiresAt = "2026-08-17T00:00:00Z" }},
		{name: "expired", mutate: func(options *InstallOptions) { options.Acceptance.Now = mustTime(t, fixture.manifest.ExpiresAt) }},
		{name: "future", mutate: func(options *InstallOptions) {
			options.Acceptance.Now = mustTime(t, fixture.manifest.CreatedAt).Add(-time.Second)
		}},
		{name: "wrong key", mutate: func(options *InstallOptions) {
			other, _, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			options.TrustedKeys = map[string]ed25519.PublicKey{indexpack.KeyID(other): other}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := fixture.installOptions()
			options.Root = filepath.Join(t.TempDir(), "root")
			test.mutate(&options)
			if _, err := Install(context.Background(), options); err == nil {
				t.Fatal("Install unexpectedly succeeded")
			}
			assertNoObjects(t, options.Root)
		})
	}
}

func TestInstallRejectsCorruptDigestCountAndZstdFramingTransactionally(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	record := testRecord(now, "https://example.com/one", "one", nil, nil, "one")
	tests := []struct {
		name       string
		compressed func([]byte) []byte
		manifest   func(*indexpack.Manifest)
		file       func([]byte) []byte
	}{
		{name: "compressed digest mismatch", file: func(input []byte) []byte {
			output := append([]byte(nil), input...)
			output[len(output)-1] ^= 1
			return output
		}},
		{name: "uncompressed digest mismatch", manifest: func(manifest *indexpack.Manifest) { manifest.Shards[0].UncompressedSHA256 = strings.Repeat("a", 64) }},
		{name: "record count mismatch", manifest: func(manifest *indexpack.Manifest) { manifest.Shards[0].RecordCount = 2; manifest.RecordCount = 2 }},
		{name: "corrupt zstd", compressed: func([]byte) []byte { return []byte("not a zstd stream") }},
		{name: "truncated zstd", compressed: func(input []byte) []byte { return append([]byte(nil), input[:len(input)-2]...) }},
		{name: "trailing zstd", compressed: func(input []byte) []byte { return append(append([]byte(nil), input...), 'x') }},
		{name: "concatenated zstd", compressed: func(input []byte) []byte { return append(append([]byte(nil), input...), input...) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newPackFixture(t, []indexpack.Record{record}, func(manifest *indexpack.Manifest, compressed *[]byte) {
				if test.compressed != nil {
					*compressed = test.compressed(*compressed)
				}
				refreshShardDescriptor(manifest, *compressed, encodeRecords(t, record), 1)
				if test.manifest != nil {
					test.manifest(manifest)
				}
			})
			if test.file != nil {
				writeFixtureShard(t, fixture, test.file(fixture.compressedRaw))
			}
			if _, err := Install(context.Background(), fixture.installOptions()); err == nil {
				t.Fatal("Install unexpectedly succeeded")
			}
			assertNoActiveProjection(t, fixture)
		})
	}
}

func TestInstallRejectsLimitsBeforeShardIO(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	fixture := newPackFixture(t, []indexpack.Record{testRecord(now, "https://example.com/manifest-cap", "cap", nil, nil, "cap")}, nil)
	if err := os.WriteFile(filepath.Join(fixture.bundle, ManifestFilename), make([]byte, MaxInstallManifestBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Install(context.Background(), fixture.installOptions()); !errors.Is(err, ErrInstallLimit) {
		t.Fatalf("oversized manifest Install = %v, want ErrInstallLimit", err)
	}
	assertNoActiveProjection(t, fixture)

	base := indexpack.Manifest{Kind: indexpack.KindSnapshot, RecordCount: 1, Shards: []indexpack.Shard{{RecordCount: 1, CompressedSizeBytes: 1, UncompressedSizeBytes: 1}}}
	tests := []struct {
		name   string
		mutate func(*indexpack.Manifest)
	}{
		{name: "shards", mutate: func(manifest *indexpack.Manifest) { manifest.Shards = append(manifest.Shards, manifest.Shards[0]) }},
		{name: "records", mutate: func(manifest *indexpack.Manifest) {
			manifest.RecordCount = MaxInstallRecords + 1
			manifest.Shards[0].RecordCount = MaxInstallRecords + 1
		}},
		{name: "compressed aggregate", mutate: func(manifest *indexpack.Manifest) { manifest.Shards[0].CompressedSizeBytes = MaxCompressedBytes + 1 }},
		{name: "uncompressed aggregate", mutate: func(manifest *indexpack.Manifest) {
			manifest.Shards[0].UncompressedSizeBytes = MaxUncompressedBytes + 1
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifest := base
			manifest.Shards = append([]indexpack.Shard(nil), base.Shards...)
			test.mutate(&manifest)
			if err := preflight(manifest, productionInstallPolicy()); !errors.Is(err, ErrInstallLimit) {
				t.Fatalf("preflight = %v, want ErrInstallLimit", err)
			}
		})
	}
}

func TestProductionInstallPolicyMatchesPublicLimits(t *testing.T) {
	policy := productionInstallPolicy()
	if policy.maxShards != MaxInstallShards || policy.maxRecords != MaxInstallRecords ||
		policy.maxCompressedBytes != MaxCompressedBytes || policy.maxUncompressedBytes != MaxUncompressedBytes ||
		policy.maxProjectionBytes != MaxProjectionBytes || policy.maxDecoderMemoryBytes != MaxUncompressedBytes {
		t.Fatalf("production install policy = %#v", policy)
	}
	if err := policy.validate(); err != nil {
		t.Fatalf("production install policy: %v", err)
	}
	invalid := policy
	invalid.maxRecords = 0
	if err := invalid.validate(); !errors.Is(err, ErrInstallLimit) {
		t.Fatalf("invalid install policy = %v, want ErrInstallLimit", err)
	}
}

func TestInstallEnforcesProjectionDiskBudgetTransactionally(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	fixture := newPackFixture(t, []indexpack.Record{
		testRecord(now, "https://example.com/disk-budget", "disk budget", nil, nil, "budget"),
	}, nil)
	options := fixture.installOptions()
	options.MaxProjectionBytes = 1
	if _, err := Install(context.Background(), options); !errors.Is(err, ErrInstallLimit) {
		t.Fatalf("small projection budget Install = %v, want ErrInstallLimit", err)
	}
	assertNoObjects(t, fixture.root)

	fixture = newPackFixture(t, []indexpack.Record{
		testRecord(now, "https://example.com/disk-cap", "disk cap", nil, nil, "cap"),
	}, nil)
	options = fixture.installOptions()
	options.MaxProjectionBytes = MaxProjectionBytes + 1
	if _, err := Install(context.Background(), options); !errors.Is(err, ErrInstallLimit) {
		t.Fatalf("oversized projection budget Install = %v, want ErrInstallLimit", err)
	}
	assertNoObjects(t, fixture.root)
}

func TestInstallSurfacesStagingCleanupFailure(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	fixture := newPackFixture(t, []indexpack.Record{
		testRecord(now, "https://example.com/cleanup-failure", "cleanup failure", nil, nil, "cleanup"),
	}, nil)
	options := fixture.installOptions()
	options.MaxProjectionBytes = 1
	cleanupCalls := 0
	_, err := install(context.Background(), options, func(path string) error {
		cleanupCalls++
		if removeErr := os.RemoveAll(path); removeErr != nil {
			return removeErr
		}
		return errors.New("synthetic cleanup failure")
	}, verifyClosedProjection, productionInstallPolicy())
	if !errors.Is(err, ErrInstallLimit) || !strings.Contains(err.Error(), "remove staging directory") ||
		!strings.Contains(err.Error(), "synthetic cleanup failure") {
		t.Fatalf("Install error = %v, want install limit joined with staging cleanup failure", err)
	}
	if cleanupCalls != 1 {
		t.Fatalf("cleanup calls = %d, want 1", cleanupCalls)
	}
	assertNoObjects(t, fixture.root)
}

func TestInstallReopensAndRejectsCorruptStagedProjectionBeforeActivation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	fixture := newPackFixture(t, []indexpack.Record{
		testRecord(now, "https://example.com/reopen-gate", "reopen gate", nil, nil, "reopen"),
	}, nil)
	verificationCalls := 0
	_, err := install(context.Background(), fixture.installOptions(), os.RemoveAll, func(path string, options OpenOptions, maxRecords uint64) error {
		verificationCalls++
		projection, openErr := bleve.Open(path)
		if openErr != nil {
			return openErr
		}
		if deleteErr := projection.Delete(markerDocumentID); deleteErr != nil {
			_ = projection.Close()
			return deleteErr
		}
		if closeErr := projection.Close(); closeErr != nil {
			return closeErr
		}
		return verifyClosedProjection(path, options, maxRecords)
	}, productionInstallPolicy())
	if !errors.Is(err, ErrSchemaMismatch) || !strings.Contains(err.Error(), "verify staged projection") {
		t.Fatalf("Install error = %v, want staged marker verification failure", err)
	}
	if verificationCalls != 1 {
		t.Fatalf("verification calls = %d, want 1", verificationCalls)
	}
	assertNoObjects(t, fixture.root)
}

func TestInstallRejectsFirstConsumerRecordAndFieldCaps(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	tests := []struct {
		name      string
		mutate    func(*indexpack.Record)
		wantLimit bool
	}{
		{name: "line", wantLimit: true, mutate: func(record *indexpack.Record) {
			record.Provenance = nil
			for index := range 16 {
				record.Provenance = append(record.Provenance, indexpack.Provenance{
					Source: "source-" + strconv.Itoa(index), SourceURI: "https://example.com/source/" + strconv.Itoa(index),
					RetrievedAt: now.Format(time.RFC3339), RightsNotice: strings.Repeat("r", 1_020),
				})
			}
		}},
		{name: "title", wantLimit: true, mutate: func(record *indexpack.Record) { record.Title = strings.Repeat("t", MaxInstallTitleBytes+1) }},
		{name: "heading count", wantLimit: true, mutate: func(record *indexpack.Record) {
			for index := range MaxInstallHeadings + 1 {
				record.Headings = append(record.Headings, "heading-"+strconv.Itoa(index))
			}
		}},
		{name: "heading bytes", wantLimit: true, mutate: func(record *indexpack.Record) {
			record.Headings = []string{strings.Repeat("h", MaxInstallHeadingBytes+1)}
		}},
		{name: "anchor count", wantLimit: true, mutate: func(record *indexpack.Record) {
			for index := range MaxInstallAnchorTerms + 1 {
				record.AnchorTerms = append(record.AnchorTerms, "anchor-"+strconv.Itoa(index))
			}
		}},
		{name: "anchor bytes", mutate: func(record *indexpack.Record) {
			record.AnchorTerms = []string{strings.Repeat("a", MaxInstallAnchorTermBytes+1)}
		}},
		{name: "sketch", wantLimit: true, mutate: func(record *indexpack.Record) { record.SalientSketch = strings.Repeat("s", MaxInstallSalientSketch+1) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := testRecord(now, "https://example.com/capped", "title", nil, nil, "sketch")
			test.mutate(&record)
			fixture := newPackFixture(t, []indexpack.Record{record}, nil)
			if _, err := Install(context.Background(), fixture.installOptions()); err == nil {
				t.Fatal("Install unexpectedly accepted over-cap record")
			} else if test.wantLimit && !errors.Is(err, ErrInstallLimit) {
				t.Fatalf("Install = %v, want ErrInstallLimit", err)
			}
			assertNoActiveProjection(t, fixture)
		})
	}
}

func TestInstallRejectsDuplicateURLAndCancellation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	record := testRecord(now, "https://example.com/duplicate", "duplicate", nil, nil, "duplicate")
	fixture := newPackFixture(t, []indexpack.Record{record, record}, nil)
	if _, err := Install(context.Background(), fixture.installOptions()); err == nil || !strings.Contains(err.Error(), "duplicates URL") {
		t.Fatalf("duplicate Install = %v", err)
	}
	assertNoActiveProjection(t, fixture)

	cancelFixture := newPackFixture(t, []indexpack.Record{record}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Install(ctx, cancelFixture.installOptions()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Install = %v", err)
	}
	assertNoActiveProjection(t, cancelFixture)
}

func TestInstallCancelsDuringLargeProjectionAndCleansStaging(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	records := make([]indexpack.Record, indexBatchSize*4)
	for index := range records {
		records[index] = testRecord(now,
			fmt.Sprintf("https://example.com/large-cancel/%04d", index),
			fmt.Sprintf("large cancellation record %04d", index),
			[]string{fmt.Sprintf("heading %d", index%37)},
			[]string{fmt.Sprintf("anchor %d", index%53)},
			fmt.Sprintf("varied sketch token %d", index%101),
		)
	}
	fixture := newPackFixture(t, records, nil)
	ctx := &cancelAfterErrChecksContext{Context: context.Background(), remaining: indexBatchSize + 100}
	if _, err := Install(ctx, fixture.installOptions()); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-import Install = %v, want context.Canceled", err)
	}
	assertNoObjects(t, fixture.root)
}

func TestInstallCleansProjectionAfterLateSemanticCorruption(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	records := make([]indexpack.Record, indexBatchSize*3+1)
	for index := range records {
		records[index] = testRecord(now,
			fmt.Sprintf("https://example.com/late-corruption/%04d", index),
			fmt.Sprintf("late corruption record %04d", index), nil, nil,
			fmt.Sprintf("sketch %d", index%29),
		)
	}
	records = append(records, records[0])
	fixture := newPackFixture(t, records, nil)
	if _, err := Install(context.Background(), fixture.installOptions()); err == nil || !strings.Contains(err.Error(), "duplicates URL") {
		t.Fatalf("late-corruption Install = %v", err)
	}
	assertNoObjects(t, fixture.root)
}

type cancelAfterErrChecksContext struct {
	context.Context
	remaining int
}

func (ctx *cancelAfterErrChecksContext) Err() error {
	if ctx.remaining <= 0 {
		return context.Canceled
	}
	ctx.remaining--
	return nil
}

func TestCrossShardDuplicateGuard(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	record := testRecord(now, "https://example.com/duplicate", "duplicate", nil, nil, "duplicate")
	raw := encodeRecords(t, record)
	compressed := compressRecords(t, raw)
	descriptor := descriptorFor(compressed, raw, 1)
	temporary := t.TempDir()
	path := filepath.Join(temporary, "shard.zst")
	if err := os.WriteFile(path, compressed, 0o600); err != nil {
		t.Fatal(err)
	}
	projection, err := bleve.NewMemOnly(projectionMapping())
	if err != nil {
		t.Fatal(err)
	}
	defer projection.Close()
	seen := make(map[[sha256.Size]byte]struct{})
	var imported uint64
	if err := importShard(context.Background(), projection, path, indexpack.Version, indexpack.KindSnapshot, descriptor, seen, &imported, MaxUncompressedBytes); err != nil {
		t.Fatalf("first importShard: %v", err)
	}
	if err := importShard(context.Background(), projection, path, indexpack.Version, indexpack.KindSnapshot, descriptor, seen, &imported, MaxUncompressedBytes); err == nil || !strings.Contains(err.Error(), "across pack") {
		t.Fatalf("second importShard = %v", err)
	}
}

func TestFilesystemSafetyExistingDestinationAndMarkerTamper(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	record := testRecord(now, "https://example.com/one", "one", nil, nil, "one")

	t.Run("symlink shard", func(t *testing.T) {
		fixture := newPackFixture(t, []indexpack.Record{record}, nil)
		shardPath := filepath.Join(fixture.bundle, filepath.FromSlash(fixture.manifest.Shards[0].Path))
		target := filepath.Join(t.TempDir(), "target")
		if err := os.Rename(shardPath, target); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, shardPath); err != nil {
			t.Fatal(err)
		}
		if _, err := Install(context.Background(), fixture.installOptions()); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Install = %v, want ErrUnsafePath", err)
		}
	})

	t.Run("unsafe bundle permissions", func(t *testing.T) {
		fixture := newPackFixture(t, []indexpack.Record{record}, nil)
		if err := os.Chmod(fixture.bundle, 0o777); err != nil {
			t.Fatal(err)
		}
		if _, err := Install(context.Background(), fixture.installOptions()); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Install = %v, want ErrUnsafePath", err)
		}
	})

	t.Run("arbitrary sticky writable parent", func(t *testing.T) {
		fixture := newPackFixture(t, []indexpack.Record{record}, nil)
		parent := filepath.Dir(fixture.bundle)
		if err := os.Chmod(parent, os.ModeSticky|0o777); err != nil {
			t.Fatal(err)
		}
		if _, err := Install(context.Background(), fixture.installOptions()); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Install = %v, want ErrUnsafePath", err)
		}
	})

	t.Run("existing destination", func(t *testing.T) {
		fixture := newPackFixture(t, []indexpack.Record{record}, nil)
		destination := filepath.Join(fixture.root, "objects", fixture.acceptance.ExpectedManifestSHA256)
		if err := os.MkdirAll(destination, 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := Install(context.Background(), fixture.installOptions()); !errors.Is(err, ErrAlreadyExists) {
			t.Fatalf("Install = %v, want ErrAlreadyExists", err)
		}
		entries, err := os.ReadDir(destination)
		if err != nil || len(entries) != 0 {
			t.Fatalf("existing destination modified: %v, %v", entries, err)
		}
	})

	t.Run("marker tamper", func(t *testing.T) {
		fixture := newPackFixture(t, []indexpack.Record{record}, nil)
		installed := installFixture(t, fixture)
		projection, err := bleve.Open(filepath.Join(installed.Path, projectionDirectoryName))
		if err != nil {
			t.Fatal(err)
		}
		if err := projection.Index(markerDocumentID, projectionMarker{
			MarkerKind: "open-pack-projection", SchemaVersion: "tampered", ManifestSHA256: installed.ManifestSHA256,
			PackID: installed.PackID, Revision: strconv.FormatUint(installed.Revision, 10), RecordCount: strconv.FormatUint(installed.RecordCount, 10),
			SigningKeyID: fixture.acceptance.ExpectedKeyID, CreatedAt: fixture.manifest.CreatedAt, ExpiresAt: fixture.manifest.ExpiresAt,
		}); err != nil {
			t.Fatal(err)
		}
		if err := projection.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(OpenOptions{
			Path: installed.Path, ExpectedManifestSHA256: installed.ManifestSHA256,
			ExpectedPackID: installed.PackID, ExpectedRevision: installed.Revision, ExpectedRecordCount: installed.RecordCount,
			ExpectedKeyID: fixture.acceptance.ExpectedKeyID, ExpectedCreatedAt: fixture.manifest.CreatedAt,
			ExpectedExpiresAt: fixture.manifest.ExpiresAt, Now: fixture.acceptance.Now,
		}); !errors.Is(err, ErrSchemaMismatch) {
			t.Fatalf("Open = %v, want ErrSchemaMismatch", err)
		}
	})
}

func TestOpenRejectsExpiredProjectionAndWrongKeyBinding(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	fixture := newPackFixture(t, []indexpack.Record{testRecord(now, "https://example.com/restart", "restart", nil, nil, "restart")}, nil)
	installed := installFixture(t, fixture)
	base := OpenOptions{
		Path: installed.Path, ExpectedManifestSHA256: installed.ManifestSHA256,
		ExpectedPackID: installed.PackID, ExpectedRevision: installed.Revision, ExpectedRecordCount: installed.RecordCount,
		ExpectedKeyID: fixture.acceptance.ExpectedKeyID, ExpectedCreatedAt: fixture.manifest.CreatedAt,
		ExpectedExpiresAt: fixture.manifest.ExpiresAt, Now: fixture.acceptance.Now,
	}
	expired := base
	expired.Now = mustTime(t, fixture.manifest.ExpiresAt)
	if _, err := Open(expired); !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("expired Open = %v, want ErrSchemaMismatch", err)
	}
	wrongKey := base
	wrongKey.ExpectedKeyID = strings.Repeat("f", 64)
	if _, err := Open(wrongKey); !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("wrong-key Open = %v, want ErrSchemaMismatch", err)
	}
	wrongExpiry := base
	wrongExpiry.ExpectedExpiresAt = mustTime(t, fixture.manifest.ExpiresAt).Add(time.Hour).Format(time.RFC3339)
	if _, err := Open(wrongExpiry); !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("wrong-expiry Open = %v, want ErrSchemaMismatch", err)
	}
	notYetValid := base
	notYetValid.Now = mustTime(t, fixture.manifest.CreatedAt).Add(-time.Second)
	if _, err := Open(notYetValid); !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("not-yet-valid Open = %v, want ErrSchemaMismatch", err)
	}
}

func TestVerifiedShardCopyObservesMidStreamCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	raw := bytes.Repeat([]byte("c"), 128<<10)
	descriptor := indexpack.Shard{
		Path: indexpack.ShardPath(indexpack.ShardDigest(raw)), Compression: "zstd", SHA256: indexpack.ShardDigest(raw),
		CompressedSizeBytes: uint64(len(raw)), UncompressedSHA256: strings.Repeat("a", 64),
		UncompressedSizeBytes: 1, RecordCount: 1,
	}
	source := &cancelAfterFirstRead{reader: bytes.NewReader(raw), cancel: cancel}
	var destination bytes.Buffer
	if err := copyVerifiedShard(ctx, source, &destination, descriptor); !errors.Is(err, context.Canceled) {
		t.Fatalf("copyVerifiedShard = %v, want context.Canceled", err)
	}
	if destination.Len() >= len(raw) {
		t.Fatalf("cancellation copied the entire shard: %d bytes", destination.Len())
	}
}

type cancelAfterFirstRead struct {
	reader *bytes.Reader
	cancel context.CancelFunc
	done   bool
}

func (reader *cancelAfterFirstRead) Read(buffer []byte) (int, error) {
	read, err := reader.reader.Read(buffer)
	if !reader.done && read > 0 {
		reader.done = true
		reader.cancel()
	}
	return read, err
}

func newPackFixture(t *testing.T, records []indexpack.Record, mutate func(*indexpack.Manifest, *[]byte)) *packFixture {
	t.Helper()
	if len(records) == 0 {
		t.Fatal("fixture requires records")
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rawRecords := encodeRecords(t, records...)
	compressed := compressRecords(t, rawRecords)
	now := time.Now().UTC().Truncate(time.Second)
	manifest := indexpack.Manifest{
		Version: indexpack.Version, Kind: indexpack.KindSnapshot, PackID: "developer-en", Revision: 1,
		CreatedAt: now.Add(-time.Hour).Format(time.RFC3339), ExpiresAt: now.Add(30 * 24 * time.Hour).Format(time.RFC3339),
		SigningKeyID: indexpack.KeyID(publicKey),
		Publisher: indexpack.Publisher{
			Name: "Fixture Publisher", ContactURI: "mailto:operator@example.com",
			TakedownURI: "https://example.com/takedown", RightsNotice: "fixture rights",
		},
		Policy:    indexpack.Policy{Robots: "rfc9309", NoIndex: "exclude", BodyDistribution: "forbidden"},
		Languages: []string{"en"}, RecordCount: uint64(len(records)),
		Build: indexpack.Build{
			Generator: "fixture", GeneratorVersion: "1", Analyzer: "unicode-lexical", AnalyzerVersion: "1",
			PolicySHA256: strings.Repeat("1", 64), ExclusionsSHA256: strings.Repeat("2", 64),
			Inputs: []indexpack.BuildInput{{
				Name: "fixture-input", URI: "https://example.com/input", RetrievedAt: now.Add(-2 * time.Hour).Format(time.RFC3339),
				SHA256: strings.Repeat("3", 64), RightsNotice: "fixture rights",
			}},
		},
	}
	refreshShardDescriptor(&manifest, compressed, rawRecords, uint64(len(records)))
	if mutate != nil {
		mutate(&manifest, &compressed)
	}
	manifestRaw, err := indexpack.EncodeManifest(manifest)
	if err != nil {
		t.Fatalf("EncodeManifest: %v", err)
	}
	signature, err := indexpack.SignManifest(manifestRaw, privateKey)
	if err != nil {
		t.Fatalf("SignManifest: %v", err)
	}
	signatureRaw, err := indexpack.EncodeSignature(signature)
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	bundle := filepath.Join(base, "bundle")
	root := filepath.Join(base, "root")
	if err := os.Mkdir(bundle, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, ManifestFilename), manifestRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, SignatureFilename), signatureRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	fixture := &packFixture{
		bundle: bundle, root: root, manifest: manifest, manifestRaw: manifestRaw, signatureRaw: signatureRaw,
		publicKey: publicKey, privateKey: privateKey, compressedRaw: compressed,
		acceptance: indexpack.Acceptance{
			Now: now, ExpectedPackID: manifest.PackID, ExpectedManifestSHA256: indexpack.ManifestDigest(manifestRaw),
			ExpectedKeyID: indexpack.KeyID(publicKey), MinimumRevision: 1, ExpectedRevision: manifest.Revision,
			ExpectedRecordCount: manifest.RecordCount, ExpectedCreatedAt: manifest.CreatedAt, ExpectedExpiresAt: manifest.ExpiresAt,
		},
	}
	writeFixtureShard(t, fixture, compressed)
	return fixture
}

func newDeltaPackFixture(t *testing.T, parent *packFixture, records []indexpack.Record, mutate func(*indexpack.Manifest, *[]byte)) *packFixture {
	t.Helper()
	rawRecords := encodeRecords(t, records...)
	compressed := compressRecords(t, rawRecords)
	manifest := parent.manifest
	manifest.Kind = indexpack.KindDelta
	manifest.Revision++
	manifest.ParentManifestSHA256 = parent.acceptance.ExpectedManifestSHA256
	created := mustTime(t, parent.manifest.CreatedAt).Add(30 * time.Minute)
	manifest.CreatedAt = created.Format(time.RFC3339)
	manifest.ExpiresAt = mustTime(t, parent.manifest.ExpiresAt).Add(-time.Hour).Format(time.RFC3339)
	refreshShardDescriptor(&manifest, compressed, rawRecords, uint64(len(records)))
	if mutate != nil {
		mutate(&manifest, &compressed)
	}
	manifestRaw, err := indexpack.EncodeManifest(manifest)
	if err != nil {
		t.Fatalf("EncodeManifest(delta): %v", err)
	}
	signature, err := indexpack.SignManifest(manifestRaw, parent.privateKey)
	if err != nil {
		t.Fatal(err)
	}
	signatureRaw, err := indexpack.EncodeSignature(signature)
	if err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(t.TempDir(), "delta-bundle")
	if err := os.Mkdir(bundle, 0o700); err != nil {
		t.Fatal(err)
	}
	fixture := &packFixture{
		bundle: bundle, root: parent.root, manifest: manifest, manifestRaw: manifestRaw, signatureRaw: signatureRaw,
		publicKey: parent.publicKey, privateKey: parent.privateKey, compressedRaw: compressed,
		acceptance: indexpack.Acceptance{
			Now: parent.acceptance.Now, ExpectedPackID: manifest.PackID, ExpectedManifestSHA256: indexpack.ManifestDigest(manifestRaw),
			ExpectedKeyID: indexpack.KeyID(parent.publicKey), MinimumRevision: manifest.Revision, ExpectedRevision: manifest.Revision,
			ExpectedRecordCount: manifest.RecordCount, ExpectedCreatedAt: manifest.CreatedAt, ExpectedExpiresAt: manifest.ExpiresAt,
		},
	}
	if err := os.WriteFile(filepath.Join(bundle, ManifestFilename), manifestRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, SignatureFilename), signatureRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	writeFixtureShard(t, fixture, compressed)
	return fixture
}

func openDeltaFixtureRoots(t *testing.T, parent, delta *packFixture) (*os.Root, *os.Root) {
	t.Helper()
	parentRoot, err := os.OpenRoot(parent.bundle)
	if err != nil {
		t.Fatal(err)
	}
	deltaRoot, err := os.OpenRoot(delta.bundle)
	if err != nil {
		_ = parentRoot.Close()
		t.Fatal(err)
	}
	return parentRoot, deltaRoot
}

func deltaOptions(parent, delta *packFixture, projectionCount uint64) DeltaInstallOptions {
	return DeltaInstallOptions{
		ParentBundleDir: parent.bundle, BundleDir: delta.bundle, Root: parent.root,
		TrustedKeys:     map[string]ed25519.PublicKey{indexpack.KeyID(parent.publicKey): parent.publicKey},
		DeltaAcceptance: delta.acceptance, ExpectedParentManifestSHA256: parent.acceptance.ExpectedManifestSHA256,
		ExpectedProjectionRecordCount: projectionCount,
	}
}

func refreshShardDescriptor(manifest *indexpack.Manifest, compressed, raw []byte, count uint64) {
	digest := indexpack.ShardDigest(compressed)
	manifest.RecordCount = count
	manifest.Shards = []indexpack.Shard{{
		Path: indexpack.ShardPath(digest), Compression: "zstd", SHA256: digest,
		CompressedSizeBytes: uint64(len(compressed)), UncompressedSHA256: indexpack.ShardDigest(raw),
		UncompressedSizeBytes: uint64(len(raw)), RecordCount: count,
	}}
}

func writeFixtureShard(t *testing.T, fixture *packFixture, data []byte) {
	t.Helper()
	path := filepath.Join(fixture.bundle, filepath.FromSlash(fixture.manifest.Shards[0].Path))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func encodeRecords(t *testing.T, records ...indexpack.Record) []byte {
	t.Helper()
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			t.Fatal(err)
		}
	}
	return buffer.Bytes()
}

func compressRecords(t *testing.T, raw []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	encoder, err := zstd.NewWriter(&buffer, zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func descriptorFor(compressed, raw []byte, count uint64) indexpack.Shard {
	digest := indexpack.ShardDigest(compressed)
	return indexpack.Shard{
		Path: indexpack.ShardPath(digest), Compression: "zstd", SHA256: digest,
		CompressedSizeBytes: uint64(len(compressed)), UncompressedSHA256: indexpack.ShardDigest(raw),
		UncompressedSizeBytes: uint64(len(raw)), RecordCount: count,
	}
}

func testRecord(now time.Time, rawURL, title string, headings, anchors []string, sketch string) indexpack.Record {
	return indexpack.Record{
		Operation: indexpack.OperationUpsert, URL: rawURL, Title: title, Headings: headings, AnchorTerms: anchors,
		SalientSketch: sketch, Language: "en", PublishedAt: now.Add(-time.Hour).Format(time.RFC3339),
		FetchedAt: now.Format(time.RFC3339), ContentSHA256: strings.Repeat("a", 64),
		AuthorityScore: 100, FreshnessScore: 200,
		Provenance: []indexpack.Provenance{{
			Source: "fixture-source", SourceURI: "https://example.com/input",
			RetrievedAt: now.Format(time.RFC3339), RightsNotice: "fixture rights",
		}},
	}
}

func (fixture *packFixture) installOptions() InstallOptions {
	return InstallOptions{
		BundleDir: fixture.bundle, Root: fixture.root,
		TrustedKeys: map[string]ed25519.PublicKey{indexpack.KeyID(fixture.publicKey): fixture.publicKey},
		Acceptance:  fixture.acceptance,
	}
}

func installFixture(t *testing.T, fixture *packFixture) Installed {
	t.Helper()
	installed, err := Install(context.Background(), fixture.installOptions())
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	return installed
}

func openInstalled(t *testing.T, installed Installed, fixture *packFixture, provider string) *Index {
	t.Helper()
	index, err := Open(OpenOptions{
		Path: installed.Path, ExpectedManifestSHA256: installed.ManifestSHA256,
		ExpectedPackID: installed.PackID, ExpectedRevision: installed.Revision,
		ExpectedRecordCount: installed.RecordCount, ExpectedKeyID: fixture.acceptance.ExpectedKeyID,
		ExpectedCreatedAt: fixture.manifest.CreatedAt, ExpectedExpiresAt: fixture.manifest.ExpiresAt,
		Now: fixture.acceptance.Now, ProviderID: provider,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return index
}

func assertNoActiveProjection(t *testing.T, fixture *packFixture) {
	t.Helper()
	path := filepath.Join(fixture.root, "objects", fixture.acceptance.ExpectedManifestSHA256)
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active projection exists after failed install: %v", err)
	}
}

func assertNoObjects(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "objects"))
	if err == nil && len(entries) != 0 {
		t.Fatalf("unexpected objects after failed install: %v", entries)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
}

func assertNoStagingDirectories(t *testing.T, root, prefix string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "objects"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) {
			t.Fatalf("staging directory remains: %s", entry.Name())
		}
	}
}

func hitURLs(hits []search.Hit) []string {
	urls := make([]string, len(hits))
	for index, hit := range hits {
		urls[index] = hit.URL
	}
	return urls
}

func directorySnapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	result := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(raw)
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		result[relative] = fmt.Sprintf("%x", digest)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func sameStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	seen := make(map[string]int, len(left))
	for _, value := range left {
		seen[value]++
	}
	for _, value := range right {
		seen[value]--
	}
	for _, count := range seen {
		if count != 0 {
			return false
		}
	}
	return true
}

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
