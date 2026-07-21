package localartifact

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	coreartifact "github.com/staticvar/fetchmark/internal/core/localartifact"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
)

func archiveOptions(path string) Options {
	return Options{
		Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10, MaxEntries: 10,
		Archive: true, MaxVersions: 20, MaxVersionsPerURL: 4,
	}
}

func TestArchiveRetainsDistinctVersionsAndReversionDoesNotDuplicate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	options := archiveOptions(path)
	store := openStore(t, options)
	base := time.Date(2026, time.July, 18, 12, 0, 0, 0, time.UTC)
	url := "https://example.com/archive"

	first := permittedVersion(url, "version-a", base)
	second := permittedVersion(url, "version-b", base.Add(time.Minute))
	reversion := permittedVersion(url, "version-a", base.Add(2*time.Minute))
	for _, version := range []coreartifact.Version{first, second, reversion} {
		if err := store.Put(context.Background(), version); err != nil {
			t.Fatal(err)
		}
	}

	history, ok, err := store.History(context.Background(), url)
	if err != nil || !ok {
		t.Fatalf("History ok=%t err=%v", ok, err)
	}
	if len(history.Versions) != 2 {
		t.Fatalf("versions=%d, want 2", len(history.Versions))
	}
	if history.CurrentHash != hashBytes([]byte("version-a")) {
		t.Fatalf("current hash=%q", history.CurrentHash)
	}
	for index, body := range []string{"version-a", "version-b"} {
		version, exists, err := store.Version(context.Background(), url, history.Versions[index].ContentHash)
		if err != nil || !exists || string(version.Body) != body {
			t.Fatalf("Version(%d)=%+v exists=%t err=%v", index, version, exists, err)
		}
	}
	current, exists, err := store.Current(context.Background(), url)
	if err != nil || !exists || string(current.Body) != "version-a" || !current.ObservedAt.Equal(reversion.ObservedAt) {
		t.Fatalf("Current=%+v exists=%t err=%v", current, exists, err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	history, ok, err = reopened.History(context.Background(), url)
	if err != nil || !ok || len(history.Versions) != 2 {
		t.Fatalf("reopened History=%+v ok=%t err=%v", history, ok, err)
	}
}

func TestArchiveSchemaDoesNotOpenAsPersonalOrViceVersa(t *testing.T) {
	t.Run("archive as personal", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "artifacts")
		store := openStore(t, archiveOptions(path))
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10}); err == nil {
			t.Fatal("personal Open accepted archive schema")
		}
	})
	t.Run("personal as archive", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "artifacts")
		store := openStore(t, Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(archiveOptions(path)); err == nil {
			t.Fatal("archive Open accepted personal schema")
		}
		if _, err := os.Stat(filepath.Join(path, "versions")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("failed archive Open left versions directory: %v", err)
		}
		reopened, err := Open(Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
		if err != nil {
			t.Fatalf("personal reopen after failed archive Open: %v", err)
		}
		_ = reopened.Close()
	})
}

func TestArchiveVersionQuotasRejectAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	options := archiveOptions(path)
	options.MaxVersions = 2
	options.MaxVersionsPerURL = 2
	store := openStore(t, options)
	base := time.Now().UTC()
	url := "https://example.com/quota"
	for index, body := range []string{"a", "b"} {
		if err := store.Put(context.Background(), permittedVersion(url, body, base.Add(time.Duration(index)*time.Minute))); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Put(context.Background(), permittedVersion(url, "c", base.Add(3*time.Minute))); !errors.Is(err, ErrVersionLimit) {
		t.Fatalf("third version error=%v", err)
	}
	history, ok, err := store.History(context.Background(), url)
	if err != nil || !ok || len(history.Versions) != 2 || history.CurrentHash != hashBytes([]byte("b")) {
		t.Fatalf("history after rejection=%+v ok=%t err=%v", history, ok, err)
	}
	if _, err := os.Stat(filepath.Join(path, "objects", URLID(url), hashBytes([]byte("c"))+".body")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected body stat error=%v", err)
	}
}

func TestArchiveAggregateVersionQuotaAppliesAcrossURLs(t *testing.T) {
	options := archiveOptions(filepath.Join(t.TempDir(), "artifacts"))
	options.MaxVersions = 2
	options.MaxVersionsPerURL = 2
	store := openStore(t, options)
	base := time.Now().UTC()
	for index, rawURL := range []string{"https://example.com/one", "https://example.com/two"} {
		if err := store.Put(context.Background(), permittedVersion(rawURL, rawURL, base.Add(time.Duration(index)*time.Minute))); err != nil {
			t.Fatal(err)
		}
	}
	third := permittedVersion("https://example.com/one", "third distinct body", base.Add(3*time.Minute))
	if err := store.Put(context.Background(), third); !errors.Is(err, ErrVersionLimit) {
		t.Fatalf("aggregate quota error=%v", err)
	}
	if _, ok, err := store.Current(context.Background(), third.URL); err != nil || !ok {
		t.Fatalf("original current ok=%t err=%v", ok, err)
	}
}

func TestArchiveByteQuotaRejectsNewVersionAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	store := openStore(t, archiveOptions(path))
	base := time.Now().UTC()
	url := "https://example.com/byte-quota"
	if err := store.Put(context.Background(), permittedVersion(url, "first", base)); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.maxBytes = store.usedBytes + 1
	store.mu.Unlock()
	if err := store.Put(context.Background(), permittedVersion(url, "second", base.Add(time.Minute))); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("second version error=%v", err)
	}
	history, ok, err := store.History(context.Background(), url)
	if err != nil || !ok || len(history.Versions) != 1 || history.CurrentHash != hashBytes([]byte("first")) {
		t.Fatalf("history after rejection=%+v ok=%t err=%v", history, ok, err)
	}
	rejectedHash := hashBytes([]byte("second"))
	for _, path := range []string{
		filepath.Join(path, "objects", URLID(url), rejectedHash+".body"),
		filepath.Join(path, "versions", URLID(url), rejectedHash+".json"),
	} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("rejected file %s stat error=%v", path, err)
		}
	}
}

func TestArchiveRejectsInvalidVersionLimits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	for _, mutate := range []func(*Options){
		func(options *Options) { options.MaxVersions = 0 },
		func(options *Options) { options.MaxVersionsPerURL = 0 },
		func(options *Options) { options.MaxVersions, options.MaxVersionsPerURL = 2, 3 },
	} {
		options := archiveOptions(path)
		mutate(&options)
		if _, err := Open(options); err == nil {
			t.Fatalf("Open accepted invalid limits: %+v", options)
		}
	}
}

func TestArchiveIgnoresExpiryOnPutAndRevalidation(t *testing.T) {
	store := openStore(t, archiveOptions(filepath.Join(t.TempDir(), "artifacts")))
	base := time.Now().UTC().Add(-48 * time.Hour)
	expired := base.Add(time.Hour)
	version := permittedVersion("https://example.com/no-expiry", "archive body", base)
	version.ExpiresAt = &expired
	if err := store.Put(context.Background(), version); err != nil {
		t.Fatal(err)
	}
	if err := store.Revalidate(context.Background(), coreartifact.Observation{
		URL: version.URL, ObservedAt: base.Add(2 * time.Hour), ValidatedAt: base.Add(2 * time.Hour), ExpiresAt: &expired,
	}); err != nil {
		t.Fatal(err)
	}
	current, ok, err := store.Current(context.Background(), version.URL)
	if err != nil || !ok || current.ExpiresAt != nil {
		t.Fatalf("Current=%+v ok=%t err=%v", current, ok, err)
	}
	history, ok, err := store.History(context.Background(), version.URL)
	if err != nil || !ok || len(history.Versions) != 1 || history.Versions[0].ExpiresAt != nil {
		t.Fatalf("History=%+v ok=%t err=%v", history, ok, err)
	}
}

func TestArchiveRevocationCommitsEmptyHistoryAndPurgesRepresentations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	store := openStore(t, archiveOptions(path))
	base := time.Now().UTC()
	url := "https://example.com/revoked"
	for index, body := range []string{"a", "b"} {
		if err := store.Put(context.Background(), permittedVersion(url, body, base.Add(time.Duration(index)*time.Minute))); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Revoke(context.Background(), url, localcorpus.DispositionTakedown, base.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	history, ok, err := store.History(context.Background(), url)
	if err != nil || !ok || len(history.Versions) != 0 || history.CurrentHash != "" || history.IndexingDisposition != localcorpus.DispositionTakedown {
		t.Fatalf("revoked history=%+v ok=%t err=%v", history, ok, err)
	}
	for _, directory := range []string{"objects", "versions"} {
		if _, err := os.Stat(filepath.Join(path, directory, URLID(url))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s stat error=%v", directory, err)
		}
	}
}

func TestArchiveStartupRejectsQuotaUndersizingAndPurgesUncommittedOrphans(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	options := archiveOptions(path)
	store := openStore(t, options)
	base := time.Now().UTC()
	url := "https://example.com/repair"
	for index, body := range []string{"a", "b"} {
		if err := store.Put(context.Background(), permittedVersion(url, body, base.Add(time.Duration(index)*time.Minute))); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	orphanHash := hashBytes([]byte("orphan"))
	for directory, suffix := range map[string]string{"objects": ".body", "versions": ".json"} {
		dir := filepath.Join(path, directory, URLID(url))
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, orphanHash+suffix), []byte("orphan"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	reopened, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	for directory, suffix := range map[string]string{"objects": ".body", "versions": ".json"} {
		if _, err := os.Stat(filepath.Join(path, directory, URLID(url), orphanHash+suffix)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s orphan stat error=%v", directory, err)
		}
	}
	undersized := options
	undersized.MaxVersions = 1
	if _, err := Open(undersized); !errors.Is(err, ErrVersionLimit) {
		t.Fatalf("undersized Open error=%v", err)
	}
}

func TestArchiveStartupRejectsCommittedManifestCorruption(t *testing.T) {
	for _, mutate := range []func(string) error{
		func(path string) error { return os.WriteFile(path, []byte(`{"schema_version":2}`), 0o600) },
		func(path string) error { return os.Remove(path) },
	} {
		path := filepath.Join(t.TempDir(), "artifacts")
		options := archiveOptions(path)
		store := openStore(t, options)
		version := permittedVersion("https://example.com/corrupt-manifest", "body", time.Now().UTC())
		if err := store.Put(context.Background(), version); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		manifestPath := filepath.Join(path, "versions", URLID(version.URL), hashBytes(version.Body)+".json")
		if err := mutate(manifestPath); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(options); !errors.Is(err, ErrCorruptArtifact) {
			t.Fatalf("Open error=%v", err)
		}
	}
}

func TestArchiveMutationLocalUsageMatchesFullStoreMeasurement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	store := openStore(t, archiveOptions(path))
	base := time.Now().UTC()
	assertUsage := func(stage string) {
		t.Helper()
		used, entries, err := measureStore(path)
		if err != nil {
			t.Fatal(err)
		}
		store.mu.RLock()
		cachedUsed, cachedEntries := store.usedBytes, store.entries
		store.mu.RUnlock()
		if cachedUsed != used || cachedEntries != entries {
			t.Fatalf("%s cached=(%d,%d) measured=(%d,%d)", stage, cachedUsed, cachedEntries, used, entries)
		}
	}
	url := "https://example.com/accounting"
	for index, body := range []string{"a", "b", "a"} {
		if err := store.Put(context.Background(), permittedVersion(url, body, base.Add(time.Duration(index)*time.Minute))); err != nil {
			t.Fatal(err)
		}
		assertUsage("put " + body)
	}
	if store.versions != 2 {
		t.Fatalf("versions=%d, want 2", store.versions)
	}
	if err := store.Revalidate(context.Background(), coreartifact.Observation{URL: url, ObservedAt: base.Add(3 * time.Minute), ValidatedAt: base.Add(3 * time.Minute), ETag: `"next"`}); err != nil {
		t.Fatal(err)
	}
	assertUsage("revalidate")
	if store.versions != 2 {
		t.Fatalf("versions after revalidation=%d, want 2", store.versions)
	}
	if err := store.Revoke(context.Background(), url, localcorpus.DispositionTakedown, base.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertUsage("revoke")
	if store.versions != 0 {
		t.Fatalf("versions after revoke=%d, want 0", store.versions)
	}
}

func TestArchiveSchemaMarkerIdentifiesModeAndVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	store := openStore(t, archiveOptions(path))
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(path, "schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var marker schemaMarker
	if err := json.Unmarshal(raw, &marker); err != nil || marker.Version != archiveSchemaVersion || marker.Mode != "archive" {
		t.Fatalf("marker=%+v err=%v", marker, err)
	}
}
