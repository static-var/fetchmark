package localartifact

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/localartifact"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
)

func TestStoreRejectsPointerItCouldNotReadBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	store := openStore(t, Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
	version := permittedVersion("https://example.com/metadata", "body", time.Now().UTC())
	version.MIME = strings.Repeat("x", maxPointerBytes)
	if err := store.Put(context.Background(), version); !errors.Is(err, ErrPointerTooLarge) {
		t.Fatalf("oversized pointer Put error = %v", err)
	}
	if _, ok, err := store.Current(context.Background(), version.URL); err != nil || ok {
		t.Fatalf("rejected pointer current ok=%v err=%v", ok, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
	if err != nil {
		t.Fatalf("reopen after rejected pointer: %v", err)
	}
	_ = reopened.Close()
}

func TestStoreRejectsConcurrentOwnerAndReleasesLockOnClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	options := Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10}
	first, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(options); !errors.Is(err, ErrStoreInUse) {
		t.Fatalf("concurrent Open error = %v, want %v", err, ErrStoreInUse)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(options)
	if err != nil {
		t.Fatalf("Open after owner close: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreStartupPurgesValidatedAtomicWriteTempsBeforeAccounting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	if err := os.MkdirAll(filepath.Join(path, "urls"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(path, "objects"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, temporary := range []string{
		filepath.Join(path, ".fetchmark-0123456789abcdef"),
		filepath.Join(path, "urls", ".fetchmark-fedcba9876543210"),
	} {
		if err := os.WriteFile(temporary, []byte(strings.Repeat("x", 128)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store, err := Open(Options{Path: path, MaxBytes: 64, MaxArtifactBytes: 32})
	if err != nil {
		t.Fatalf("Open with repairable crash temps: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, temporary := range []string{
		filepath.Join(path, ".fetchmark-0123456789abcdef"),
		filepath.Join(path, "urls", ".fetchmark-fedcba9876543210"),
	} {
		if _, err := os.Stat(temporary); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("temporary %s stat error = %v", temporary, err)
		}
	}
}

func TestStoreStartupRejectsUnexpectedMetadataEntriesWithoutDeletingThem(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	if err := os.MkdirAll(filepath.Join(path, "urls"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(path, "objects"), 0o700); err != nil {
		t.Fatal(err)
	}
	unexpected := filepath.Join(path, "urls", ".fetchmark-not-a-valid-temp")
	if err := os.WriteFile(unexpected, []byte("must remain"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10}); !errors.Is(err, ErrCorruptArtifact) {
		t.Fatalf("Open error = %v, want %v", err, ErrCorruptArtifact)
	}
	if body, err := os.ReadFile(unexpected); err != nil || string(body) != "must remain" {
		t.Fatalf("unexpected entry body=%q err=%v", body, err)
	}
}

func TestStorePersistsValidatedSafetyClassification(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	options := Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10}
	store := openStore(t, options)
	version := permittedVersion("https://example.com/safe", "reviewed body", time.Now().UTC())
	version.SafetyClassification = localcorpus.SafetySafe
	if err := store.Put(context.Background(), version); err != nil {
		t.Fatal(err)
	}
	current, ok, err := store.Current(context.Background(), version.URL)
	if err != nil || !ok || current.SafetyClassification != localcorpus.SafetySafe {
		t.Fatalf("current=%+v ok=%t err=%v", current, ok, err)
	}
	invalid := permittedVersion("https://example.com/invalid", "body", time.Now().UTC())
	invalid.SafetyClassification = "guessed"
	if err := store.Put(context.Background(), invalid); err == nil || !strings.Contains(err.Error(), "unknown safety classification") {
		t.Fatalf("invalid classification error = %v", err)
	}
}

func TestMutationLocalUsageMatchesFullStoreMeasurement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	store := openStore(t, Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10, MaxEntries: 10})
	base := time.Now().UTC().Add(-time.Hour)
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
	first := permittedVersion("https://example.com/accounted", "first body", base)
	if err := store.Put(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	assertUsage("first put")
	if err := store.Revalidate(context.Background(), localartifact.Observation{
		URL: first.URL, ObservedAt: base.Add(time.Minute), ValidatedAt: base.Add(time.Minute), ETag: `"one"`,
	}); err != nil {
		t.Fatal(err)
	}
	assertUsage("revalidate")
	second := permittedVersion(first.URL, "longer replacement body", base.Add(2*time.Minute))
	if err := store.Put(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	assertUsage("replacement put")
	if err := store.Revoke(context.Background(), first.URL, localcorpus.DispositionNoIndexHeader, base.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertUsage("revoke")
	if err := store.Revoke(context.Background(), "https://example.com/new-tombstone", localcorpus.DispositionTakedown, base.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertUsage("new tombstone")
}

func TestStoreRejectsSymlinkedManagedDirectories(t *testing.T) {
	for _, managed := range []string{"urls", "objects"} {
		t.Run(managed, func(t *testing.T) {
			parent := t.TempDir()
			path := filepath.Join(parent, "artifacts")
			outside := filepath.Join(parent, "outside")
			if err := os.MkdirAll(path, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(outside, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(path, managed)); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10}); err == nil {
				t.Fatal("Open accepted symlinked managed directory")
			}
			entries, err := os.ReadDir(outside)
			if err != nil || len(entries) != 0 {
				t.Fatalf("outside entries=%d err=%v", len(entries), err)
			}
		})
	}
}

func TestStoreRejectsSymlinkSwapAndPerURLObjectSymlink(t *testing.T) {
	t.Run("managed directory swapped after open", func(t *testing.T) {
		parent := t.TempDir()
		path := filepath.Join(parent, "artifacts")
		outside := filepath.Join(parent, "outside")
		if err := os.Mkdir(outside, 0o700); err != nil {
			t.Fatal(err)
		}
		store := openStore(t, Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
		if err := os.Remove(filepath.Join(path, "urls")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(path, "urls")); err != nil {
			t.Fatal(err)
		}
		if err := store.Put(context.Background(), permittedVersion("https://example.com/swap", "body", time.Now().UTC())); err == nil {
			t.Fatal("Put accepted swapped urls directory")
		}
		entries, err := os.ReadDir(outside)
		if err != nil || len(entries) != 0 {
			t.Fatalf("outside entries=%d err=%v", len(entries), err)
		}
	})

	t.Run("per URL object directory", func(t *testing.T) {
		parent := t.TempDir()
		path := filepath.Join(parent, "artifacts")
		outside := filepath.Join(parent, "outside")
		if err := os.Mkdir(outside, 0o700); err != nil {
			t.Fatal(err)
		}
		store := openStore(t, Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
		rawURL := "https://example.com/object-symlink"
		if err := os.Symlink(outside, filepath.Join(path, "objects", URLID(rawURL))); err != nil {
			t.Fatal(err)
		}
		if err := store.Put(context.Background(), permittedVersion(rawURL, "body", time.Now().UTC())); err == nil {
			t.Fatal("Put accepted symlinked per-URL object directory")
		}
		entries, err := os.ReadDir(outside)
		if err != nil || len(entries) != 0 {
			t.Fatalf("outside entries=%d err=%v", len(entries), err)
		}
	})
}

func TestStoreBoundsURLStateCardinalityAndMetadataAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	options := Options{Path: path, MaxBytes: 4096, MaxArtifactBytes: 1024, MaxEntries: 1}
	store := openStore(t, options)
	first := permittedVersion("https://example.com/one", "one", time.Now().UTC())
	if err := store.Put(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := store.Revoke(context.Background(), "https://example.com/two", localcorpus.DispositionTakedown, time.Now().UTC()); !errors.Is(err, ErrEntryLimit) {
		t.Fatalf("second URL state error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if err := reopened.Revoke(context.Background(), "https://example.com/three", localcorpus.DispositionTakedown, time.Now().UTC()); !errors.Is(err, ErrEntryLimit) {
		t.Fatalf("reopened cardinality error = %v", err)
	}
}

func TestRevokeExistingDoesNotCreateAbsentStateAndPurgesPresentArtifact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	store := openStore(t, Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10, MaxEntries: 1})
	base := time.Now().UTC().Add(-time.Minute)
	const absentURL = "https://example.com/absent"
	mutated, err := store.RevokeExisting(context.Background(), absentURL, localcorpus.DispositionNoIndexHeader, base)
	if err != nil || mutated {
		t.Fatalf("absent mutated=%t err=%v", mutated, err)
	}
	if _, err := os.Stat(filepath.Join(path, "urls", URLID(absentURL)+".json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("absent pointer stat error = %v", err)
	}

	version := permittedVersion("https://example.com/present", "retained source", base)
	if err := store.Put(context.Background(), version); err != nil {
		t.Fatal(err)
	}
	mutated, err = store.RevokeExisting(context.Background(), version.URL, localcorpus.DispositionNoIndexHeader, base.Add(time.Second))
	if err != nil || !mutated {
		t.Fatalf("present mutated=%t err=%v", mutated, err)
	}
	if current, ok, err := store.Current(context.Background(), version.URL); err != nil || ok || current.Body != nil {
		t.Fatalf("current=%+v ok=%t err=%v", current, ok, err)
	}
}

func TestPersonalStorePersistsLatestContentAddressedVersionAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	store := openStore(t, Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
	base := time.Now().UTC().Add(-time.Hour)
	first := permittedVersion("https://example.com/article?utm_source=test", "first immutable body", base)
	first.ETag = `W/"one"`
	if err := store.Put(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := permittedVersion("https://example.com/article", "second immutable body", base.Add(time.Minute))
	second.ETag = `"two"`
	if err := store.Put(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	got, ok, err := reopened.Current(context.Background(), "https://example.com/article#fragment")
	if err != nil || !ok || string(got.Body) != "second immutable body" || got.ETag != `"two"` {
		t.Fatalf("current=%+v ok=%v err=%v", got, ok, err)
	}
	entries, err := os.ReadDir(filepath.Join(path, "objects", URLID("https://example.com/article")))
	if err != nil || len(entries) != 1 {
		t.Fatalf("object entries=%d err=%v", len(entries), err)
	}
}

func TestStoreRevalidationAdvancesPointerWithoutCreatingBody(t *testing.T) {
	store := openStore(t, Options{Path: filepath.Join(t.TempDir(), "artifacts"), MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
	base := time.Now().UTC().Add(-time.Hour)
	version := permittedVersion("https://example.com/revalidate", "stable body", base)
	version.ETag = `"one"`
	if err := store.Put(context.Background(), version); err != nil {
		t.Fatal(err)
	}
	expiresAt := base.Add(48 * time.Hour)
	if err := store.Revalidate(context.Background(), localartifact.Observation{
		URL: version.URL, ObservedAt: base.Add(time.Hour), ValidatedAt: base.Add(time.Hour),
		ETag: `"two"`, ExpiresAt: &expiresAt,
	}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.Current(context.Background(), version.URL)
	if err != nil || !ok || string(got.Body) != "stable body" || got.ETag != `"two"` || !got.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("current=%+v ok=%v err=%v", got, ok, err)
	}
}

func TestStoreRevocationIsImmediatelyUnreadableAndTakedownSticky(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	store := openStore(t, Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
	base := time.Now().UTC().Add(-time.Hour)
	version := permittedVersion("https://example.com/remove", "private retained body", base)
	if err := store.Put(context.Background(), version); err != nil {
		t.Fatal(err)
	}
	if err := store.Revoke(context.Background(), version.URL, localcorpus.DispositionTakedown, base.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := store.Current(context.Background(), version.URL); err != nil || ok || got.Body != nil {
		t.Fatalf("current after takedown=%+v ok=%v err=%v", got, ok, err)
	}
	version.ObservedAt = base.Add(24 * time.Hour)
	if err := store.Put(context.Background(), version); !errors.Is(err, ErrStaleObservation) {
		t.Fatalf("post-takedown Put error = %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(path, "objects", URLID(version.URL)))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("object entries=%d err=%v", len(entries), err)
	}
}

func TestStoreRejectsExpiredCorruptAndOversizedArtifacts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	store := openStore(t, Options{Path: path, MaxBytes: 4096, MaxArtifactBytes: 32})
	base := time.Now().UTC()
	expired := base.Add(-time.Minute)
	version := permittedVersion("https://example.com/expired", "expired body", base.Add(-time.Hour))
	version.ExpiresAt = &expired
	if err := store.Put(context.Background(), version); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.Current(context.Background(), version.URL); err != nil || ok {
		t.Fatalf("expired current ok=%v err=%v", ok, err)
	}
	tooLarge := permittedVersion("https://example.com/large", string(make([]byte, 33)), base)
	if err := store.Put(context.Background(), tooLarge); !errors.Is(err, ErrArtifactTooLarge) {
		t.Fatalf("oversized Put error = %v", err)
	}

	valid := permittedVersion("https://example.com/corrupt", "valid body", base)
	if err := store.Put(context.Background(), valid); err != nil {
		t.Fatal(err)
	}
	current, ok, err := store.Current(context.Background(), valid.URL)
	if err != nil || !ok {
		t.Fatalf("seed current=%+v ok=%v err=%v", current, ok, err)
	}
	if err := os.WriteFile(filepath.Join(path, "objects", URLID(valid.URL), current.ContentHash+".body"), []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Current(context.Background(), valid.URL); !errors.Is(err, ErrCorruptArtifact) {
		t.Fatalf("corrupt Current error = %v", err)
	}
	if err := store.Put(context.Background(), valid); err != nil {
		t.Fatalf("repair Put error = %v", err)
	}
	if repaired, ok, err := store.Current(context.Background(), valid.URL); err != nil || !ok || string(repaired.Body) != "valid body" {
		t.Fatalf("repaired=%+v ok=%v err=%v", repaired, ok, err)
	}
}

func TestStoreStartupPurgesOrphansAndRejectsMissingCurrentBody(t *testing.T) {
	t.Run("orphan is purged", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "artifacts")
		store := openStore(t, Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
		version := permittedVersion("https://example.com/current", "current body", time.Now().UTC())
		if err := store.Put(context.Background(), version); err != nil {
			t.Fatal(err)
		}
		orphanDir := filepath.Join(path, "objects", URLID("https://example.com/orphan"))
		if err := os.MkdirAll(orphanDir, 0o700); err != nil {
			t.Fatal(err)
		}
		orphan := filepath.Join(orphanDir, "deadbeef.body")
		if err := os.WriteFile(orphan, []byte("orphan"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := Open(Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
		if err != nil {
			t.Fatal(err)
		}
		_ = reopened.Close()
		if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("orphan stat error = %v", err)
		}
	})

	t.Run("missing current body fails open", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "artifacts")
		store := openStore(t, Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
		version := permittedVersion("https://example.com/missing", "must remain", time.Now().UTC())
		if err := store.Put(context.Background(), version); err != nil {
			t.Fatal(err)
		}
		current, ok, err := store.Current(context.Background(), version.URL)
		if err != nil || !ok {
			t.Fatalf("current=%+v ok=%v err=%v", current, ok, err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(path, "objects", URLID(version.URL), current.ContentHash+".body")); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10}); !errors.Is(err, ErrCorruptArtifact) {
			t.Fatalf("Open error = %v", err)
		}
	})
}

func TestStoreReclaimsExpiredArtifactsUnderBudgetPressure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	store := openStore(t, Options{Path: path, MaxBytes: 800, MaxArtifactBytes: 24, MaxEntries: 1})
	now := time.Now().UTC()
	expired := now.Add(-time.Minute)
	old := permittedVersion("https://example.com/old-expired", "12345678901234567890", now.Add(-time.Hour))
	old.ExpiresAt = &expired
	if err := store.Put(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	fresh := permittedVersion("https://example.com/new-current", "abcdefghijklmnopqrst", now)
	if err := store.Put(context.Background(), fresh); err != nil {
		t.Fatalf("Put after expiry = %v", err)
	}
	if _, ok, err := store.Current(context.Background(), old.URL); err != nil || ok {
		t.Fatalf("expired current ok=%v err=%v", ok, err)
	}
	if got, ok, err := store.Current(context.Background(), fresh.URL); err != nil || !ok || string(got.Body) != "abcdefghijklmnopqrst" {
		t.Fatalf("fresh current=%+v ok=%v err=%v", got, ok, err)
	}
}

func TestStoreSweepPurgesExpiryBoundaryAndPreservesFutureAndTombstones(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifacts")
	store := openStore(t, Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
	sweepAt := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Second)

	expired := permittedVersion("https://example.com/expires-at-boundary", "expired body", sweepAt.Add(-time.Hour))
	expired.ExpiresAt = &sweepAt
	if err := store.Put(context.Background(), expired); err != nil {
		t.Fatal(err)
	}
	futureExpiry := sweepAt.Add(time.Nanosecond)
	future := permittedVersion("https://example.com/future", "future body", sweepAt.Add(-time.Hour))
	future.ExpiresAt = &futureExpiry
	if err := store.Put(context.Background(), future); err != nil {
		t.Fatal(err)
	}
	tombstoneURL := "https://example.com/tombstone"
	if err := store.Revoke(context.Background(), tombstoneURL, localcorpus.DispositionTakedown, sweepAt.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}

	beforeBytes := store.usedBytes
	localSweepAt := sweepAt.In(time.FixedZone("test", 5*60*60+30*60))
	removed, err := store.Sweep(context.Background(), localSweepAt)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	if _, ok, err := store.Current(context.Background(), expired.URL); err != nil || ok {
		t.Fatalf("expired current ok=%v err=%v", ok, err)
	}
	if got, ok, err := store.Current(context.Background(), future.URL); err != nil || !ok || string(got.Body) != "future body" {
		t.Fatalf("future current=%+v ok=%v err=%v", got, ok, err)
	}
	if _, ok, err := store.Current(context.Background(), tombstoneURL); err != nil || ok {
		t.Fatalf("tombstone current ok=%v err=%v", ok, err)
	}
	if _, err := os.Stat(store.statePath(tombstoneURL)); err != nil {
		t.Fatalf("tombstone pointer stat: %v", err)
	}
	if _, err := os.Stat(store.statePath(expired.URL)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired pointer stat error = %v", err)
	}
	if _, err := os.Stat(store.objectDir(expired.URL)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired object directory stat error = %v", err)
	}
	if store.entries != 2 {
		t.Fatalf("entries = %d, want 2", store.entries)
	}
	if store.usedBytes >= beforeBytes {
		t.Fatalf("used bytes = %d, want less than %d", store.usedBytes, beforeBytes)
	}
}

func TestStoreSweepReleasesEntryCapacity(t *testing.T) {
	store := openStore(t, Options{
		Path: filepath.Join(t.TempDir(), "artifacts"), MaxBytes: 4096, MaxArtifactBytes: 1024, MaxEntries: 1,
	})
	sweepAt := time.Date(2026, time.July, 18, 12, 0, 0, 0, time.UTC)
	expired := permittedVersion("https://example.com/expired", "expired body", sweepAt.Add(-time.Hour))
	expired.ExpiresAt = &sweepAt
	if err := store.Put(context.Background(), expired); err != nil {
		t.Fatal(err)
	}

	removed, err := store.Sweep(context.Background(), sweepAt)
	if err != nil || removed != 1 {
		t.Fatalf("Sweep removed=%d err=%v", removed, err)
	}
	if store.entries != 0 {
		t.Fatalf("entries = %d, want 0", store.entries)
	}
	if err := store.Put(context.Background(), permittedVersion("https://example.com/replacement", "replacement", sweepAt)); err != nil {
		t.Fatalf("Put after Sweep = %v", err)
	}
}

func TestStoreSweepHonorsCancellationAndClose(t *testing.T) {
	t.Run("canceled before lock", func(t *testing.T) {
		store := openStore(t, Options{Path: filepath.Join(t.TempDir(), "artifacts"), MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if removed, err := store.Sweep(ctx, time.Now()); !errors.Is(err, context.Canceled) || removed != 0 {
			t.Fatalf("Sweep removed=%d err=%v", removed, err)
		}
	})

	t.Run("canceled during deletion keeps conservative accounting", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "artifacts")
		store := openStore(t, Options{Path: path, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
		sweepAt := time.Date(2026, time.July, 18, 12, 0, 0, 0, time.UTC)
		for _, rawURL := range []string{"https://example.com/expired-one", "https://example.com/expired-two"} {
			version := permittedVersion(rawURL, "expired body", sweepAt.Add(-time.Hour))
			version.ExpiresAt = &sweepAt
			if err := store.Put(context.Background(), version); err != nil {
				t.Fatal(err)
			}
		}
		beforeBytes, beforeEntries := store.usedBytes, store.entries
		ctx := &cancelOnErrCallContext{Context: context.Background(), cancelOnCall: 4}
		removed, err := store.Sweep(ctx, sweepAt)
		if !errors.Is(err, context.Canceled) || removed != 1 {
			t.Fatalf("Sweep removed=%d err=%v", removed, err)
		}
		actualBytes, actualEntries, measureErr := measureStore(path)
		if measureErr != nil {
			t.Fatal(measureErr)
		}
		if store.usedBytes != beforeBytes || store.entries != beforeEntries {
			t.Fatalf("cached accounting bytes=%d entries=%d, want unchanged %d/%d", store.usedBytes, store.entries, beforeBytes, beforeEntries)
		}
		if store.usedBytes < actualBytes || store.entries < actualEntries {
			t.Fatalf("cached accounting %d/%d understates measured %d/%d", store.usedBytes, store.entries, actualBytes, actualEntries)
		}
		if removed, err := store.Sweep(context.Background(), sweepAt); err != nil || removed != 1 {
			t.Fatalf("follow-up Sweep removed=%d err=%v", removed, err)
		}
	})

	t.Run("closed", func(t *testing.T) {
		store := openStore(t, Options{Path: filepath.Join(t.TempDir(), "artifacts"), MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		if removed, err := store.Sweep(context.Background(), time.Now()); !errors.Is(err, ErrClosed) || removed != 0 {
			t.Fatalf("Sweep removed=%d err=%v", removed, err)
		}
	})

	t.Run("zero time", func(t *testing.T) {
		store := openStore(t, Options{Path: filepath.Join(t.TempDir(), "artifacts"), MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
		if removed, err := store.Sweep(context.Background(), time.Time{}); err == nil || removed != 0 {
			t.Fatalf("Sweep removed=%d err=%v", removed, err)
		}
	})
}

type cancelOnErrCallContext struct {
	context.Context
	calls        int
	cancelOnCall int
}

func (ctx *cancelOnErrCallContext) Err() error {
	ctx.calls++
	if ctx.calls >= ctx.cancelOnCall {
		return context.Canceled
	}
	return nil
}

func openStore(t *testing.T, options Options) *Store {
	t.Helper()
	store, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func permittedVersion(rawURL, body string, observedAt time.Time) localartifact.Version {
	expiresAt := observedAt.Add(24 * time.Hour)
	return localartifact.Version{
		URL: rawURL, EffectiveURL: rawURL, Body: []byte(body), MIME: "text/html",
		PolicyAgent: "Fetchmark-Test", FetchedAt: observedAt, ObservedAt: observedAt,
		ValidatedAt: observedAt, ExpiresAt: &expiresAt, IndexingDisposition: localcorpus.DispositionPermitted,
	}
}
