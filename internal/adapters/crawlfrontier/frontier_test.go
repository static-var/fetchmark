package crawlfrontier

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestFrontierPersistsDeterministicTransitionsAndSeedRemoval(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	f := openTestFrontier(t, path, 10)

	seeds := []Seed{
		{Key: "b", JobID: "docs", URL: "https://example.com/b"},
		{Key: "a", JobID: "docs", URL: "https://example.com/a"},
	}
	if err := f.SyncSeeds(seeds, now); err != nil {
		t.Fatalf("SyncSeeds: %v", err)
	}

	leased, err := f.LeaseDue(now, 2, time.Minute)
	if err != nil {
		t.Fatalf("LeaseDue: %v", err)
	}
	if len(leased) != 2 || leased[0].Key != "a" || leased[1].Key != "b" {
		t.Fatalf("leased order = %#v, want a then b", leased)
	}
	if leased[0].State != StateLeased || leased[0].Attempts != 1 || !leased[0].LeaseUntil.Equal(now.Add(time.Minute)) {
		t.Fatalf("first lease = %#v", leased[0])
	}
	if err := f.Complete("a", leased[0].LeaseGeneration, StateSucceeded, now.Add(24*time.Hour), "stored"); err != nil {
		t.Fatalf("Complete succeeded: %v", err)
	}
	retryAt := now.Add(time.Hour)
	if err := f.Complete("b", leased[1].LeaseGeneration, StateRetry, retryAt, "temporary failure"); err != nil {
		t.Fatalf("Complete retry: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f = openTestFrontier(t, path, 10)
	defer f.Close()
	counts, err := f.Counts()
	if err != nil {
		t.Fatalf("Counts: %v", err)
	}
	if counts.Total != 2 || counts.Succeeded != 1 || counts.Retry != 1 {
		t.Fatalf("counts after reopen = %+v", counts)
	}
	leased, err = f.LeaseDue(now.Add(30*time.Minute), 10, time.Minute)
	if err != nil || len(leased) != 0 {
		t.Fatalf("early LeaseDue = %#v, %v", leased, err)
	}
	leased, err = f.LeaseDue(retryAt, 10, time.Minute)
	if err != nil || len(leased) != 1 || leased[0].Key != "b" || leased[0].Attempts != 2 {
		t.Fatalf("retry LeaseDue = %#v, %v", leased, err)
	}
	if err := f.Complete("b", leased[0].LeaseGeneration, StateRejected, now.Add(24*time.Hour), "noindex"); err != nil {
		t.Fatalf("Complete rejected: %v", err)
	}

	if err := f.SyncSeeds(seeds[:1], now.Add(2*time.Hour)); err != nil {
		t.Fatalf("SyncSeeds removal: %v", err)
	}
	counts, err = f.Counts()
	if err != nil {
		t.Fatalf("Counts after removal: %v", err)
	}
	if counts.Succeeded != 0 || counts.Rejected != 1 || counts.Disabled != 1 {
		t.Fatalf("counts after removal = %+v", counts)
	}
	if err := f.SyncSeeds(seeds, now.Add(3*time.Hour)); err != nil {
		t.Fatalf("SyncSeeds re-add: %v", err)
	}
	counts, err = f.Counts()
	if err != nil {
		t.Fatalf("Counts after re-add: %v", err)
	}
	if counts.Queued != 1 || counts.Rejected != 1 || counts.Disabled != 0 {
		t.Fatalf("counts after re-add = %+v", counts)
	}
}

func TestLeaseDueRecoversExpiredLease(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 2)
	defer f.Close()
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	if err := f.SyncSeeds([]Seed{{Key: "a", JobID: "job", URL: "https://example.com/a"}}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := f.LeaseDue(now, 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	leased, err := f.LeaseDue(now.Add(2*time.Minute), 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 1 || leased[0].Key != "a" || leased[0].Attempts != 2 || leased[0].LastReason != ReasonLeaseExpired {
		t.Fatalf("recovered lease = %#v", leased)
	}
}

func TestCompletedEntriesLeaseOnlyWhenRefreshIsDue(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 2)
	defer f.Close()
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	if err := f.SyncSeeds([]Seed{
		{Key: "accepted", JobID: "job", URL: "https://example.com/accepted"},
		{Key: "rejected", JobID: "job", URL: "https://example.com/rejected"},
	}, now); err != nil {
		t.Fatal(err)
	}
	leased, err := f.LeaseDue(now, 2, time.Minute)
	if err != nil || len(leased) != 2 {
		t.Fatalf("initial lease = %#v, %v", leased, err)
	}
	refreshAt := now.Add(time.Hour)
	byKey := map[string]Entry{leased[0].Key: leased[0], leased[1].Key: leased[1]}
	if err := f.Complete("accepted", byKey["accepted"].LeaseGeneration, StateSucceeded, refreshAt, "stored"); err != nil {
		t.Fatal(err)
	}
	if err := f.Complete("rejected", byKey["rejected"].LeaseGeneration, StateRejected, refreshAt, "robots"); err != nil {
		t.Fatal(err)
	}
	leased, err = f.LeaseDue(now, 2, time.Minute)
	if err != nil || len(leased) != 0 {
		t.Fatalf("immediate rerun = %#v, %v", leased, err)
	}
	leased, err = f.LeaseDue(refreshAt, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 2 || leased[0].Key != "accepted" || leased[1].Key != "rejected" {
		t.Fatalf("refresh lease = %#v", leased)
	}
	if leased[0].Attempts != 1 || leased[1].Attempts != 1 {
		t.Fatalf("attempts after terminal refresh = (%d, %d), want reset then lease to 1", leased[0].Attempts, leased[1].Attempts)
	}
}

func TestCompleteRejectsStaleLeaseGeneration(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 2)
	defer f.Close()
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	if err := f.SyncSeeds([]Seed{{Key: "a", JobID: "job", URL: "https://example.com/a"}}, now); err != nil {
		t.Fatal(err)
	}
	first, err := f.LeaseDue(now, 1, time.Minute)
	if err != nil || len(first) != 1 {
		t.Fatalf("first lease = %#v, %v", first, err)
	}
	second, err := f.LeaseDue(now.Add(2*time.Minute), 1, time.Minute)
	if err != nil || len(second) != 1 {
		t.Fatalf("second lease = %#v, %v", second, err)
	}
	if second[0].LeaseGeneration <= first[0].LeaseGeneration {
		t.Fatalf("lease generations = %d then %d", first[0].LeaseGeneration, second[0].LeaseGeneration)
	}
	refreshAt := now.Add(24 * time.Hour)
	if err := f.Complete("a", first[0].LeaseGeneration, StateSucceeded, refreshAt, "stale"); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("stale Complete error = %v, want ErrLeaseMismatch", err)
	}
	if err := f.Complete("a", second[0].LeaseGeneration, StateSucceeded, refreshAt, "current"); err != nil {
		t.Fatalf("current Complete: %v", err)
	}
}

func TestLeaseGenerationDoesNotResetAfterExplicitPrune(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 2)
	defer f.Close()
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	seed := Seed{Key: "a", JobID: "job", URL: "https://example.com/a"}
	if err := f.SyncSeeds([]Seed{seed}, now); err != nil {
		t.Fatal(err)
	}
	first, err := f.LeaseDue(now, 1, time.Minute)
	if err != nil || len(first) != 1 {
		t.Fatalf("first lease = %#v, %v", first, err)
	}
	if err := f.SyncSeeds(nil, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if pruned, err := f.PruneDisabled(1); err != nil || pruned != 1 {
		t.Fatalf("PruneDisabled = %d, %v", pruned, err)
	}
	if err := f.SyncSeeds([]Seed{seed}, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	second, err := f.LeaseDue(now.Add(2*time.Minute), 1, time.Minute)
	if err != nil || len(second) != 1 {
		t.Fatalf("second lease = %#v, %v", second, err)
	}
	if second[0].LeaseGeneration <= first[0].LeaseGeneration {
		t.Fatalf("lease generations reset across prune: %d then %d", first[0].LeaseGeneration, second[0].LeaseGeneration)
	}
	if err := f.Complete("a", first[0].LeaseGeneration, StateSucceeded, now.Add(24*time.Hour), "stale"); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("stale Complete after prune error = %v, want ErrLeaseMismatch", err)
	}
}

func TestLeaseDueJobFiltersWithoutDisturbingOtherJobs(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 4)
	defer f.Close()
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	if err := f.SyncSeeds([]Seed{
		{Key: "a", JobID: "first", URL: "https://example.com/a"},
		{Key: "b", JobID: "second", URL: "https://example.com/b"},
		{Key: "c", JobID: "first", URL: "https://example.com/c"},
	}, now); err != nil {
		t.Fatal(err)
	}
	leased, err := f.LeaseDueJob("first", now, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 1 || leased[0].Key != "a" || leased[0].JobID != "first" {
		t.Fatalf("first job lease = %#v", leased)
	}
	leased, err = f.LeaseDueJob("second", now, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 1 || leased[0].Key != "b" {
		t.Fatalf("second job lease = %#v", leased)
	}
	counts, err := f.Counts()
	if err != nil {
		t.Fatal(err)
	}
	if counts.Leased != 2 || counts.Queued != 1 {
		t.Fatalf("counts = %+v", counts)
	}
}

func TestSyncSeedsCapFailureIsAtomic(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 2)
	defer f.Close()
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	if err := f.SyncSeeds([]Seed{{Key: "a", JobID: "job", URL: "https://example.com/a"}}, now); err != nil {
		t.Fatal(err)
	}
	err := f.SyncSeeds([]Seed{
		{Key: "b", JobID: "job", URL: "https://example.com/b"},
		{Key: "c", JobID: "job", URL: "https://example.com/c"},
	}, now.Add(time.Minute))
	if !errors.Is(err, ErrMaxEntries) {
		t.Fatalf("SyncSeeds error = %v, want ErrMaxEntries", err)
	}
	counts, err := f.Counts()
	if err != nil {
		t.Fatal(err)
	}
	if counts.Total != 1 || counts.Queued != 1 || counts.Disabled != 0 {
		t.Fatalf("counts after rejected sync = %+v", counts)
	}
	leased, err := f.LeaseDue(now.Add(time.Minute), 10, time.Minute)
	if err != nil || len(leased) != 1 || leased[0].Key != "a" {
		t.Fatalf("entries after rejected sync = %#v, %v", leased, err)
	}
}

func TestPruneDisabledExplicitlyRecoversCapacity(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 2)
	defer f.Close()
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	if err := f.SyncSeeds([]Seed{
		{Key: "a", JobID: "job", URL: "https://example.com/a"},
		{Key: "b", JobID: "job", URL: "https://example.com/b"},
	}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncSeeds(nil, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncSeeds([]Seed{{Key: "c", JobID: "job", URL: "https://example.com/c"}}, now.Add(2*time.Minute)); !errors.Is(err, ErrMaxEntries) {
		t.Fatalf("SyncSeeds before prune = %v, want ErrMaxEntries", err)
	}
	pruned, err := f.PruneDisabled(1)
	if err != nil || pruned != 1 {
		t.Fatalf("PruneDisabled = %d, %v", pruned, err)
	}
	if err := f.SyncSeeds([]Seed{{Key: "c", JobID: "job", URL: "https://example.com/c"}}, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("SyncSeeds after prune: %v", err)
	}
	counts, err := f.Counts()
	if err != nil {
		t.Fatal(err)
	}
	if counts.Total != 2 || counts.Queued != 1 || counts.Disabled != 1 {
		t.Fatalf("counts after prune and sync = %+v", counts)
	}
}

func TestOpenRejectsExclusiveOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	f := openTestFrontier(t, path, 2)
	defer f.Close()

	_, err := Open(path, Options{MaxEntries: 2, LockTimeout: 10 * time.Millisecond})
	if !errors.Is(err, ErrOwned) {
		t.Fatalf("second Open error = %v, want ErrOwned", err)
	}
}

func TestOpenRejectsSchemaMismatchAndCorruptEntries(t *testing.T) {
	t.Run("schema mismatch", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "frontier.db")
		f := openTestFrontier(t, path, 2)
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		mutateDB(t, path, func(tx *bolt.Tx) error {
			return tx.Bucket(metaBucket).Put(schemaKey, []byte{99})
		})
		_, err := Open(path, Options{MaxEntries: 2, LockTimeout: 10 * time.Millisecond})
		if !errors.Is(err, ErrSchemaMismatch) {
			t.Fatalf("Open error = %v, want ErrSchemaMismatch", err)
		}
	})

	t.Run("corrupt entry", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "frontier.db")
		f := openTestFrontier(t, path, 2)
		now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
		if err := f.SyncSeeds([]Seed{{Key: "a", JobID: "job", URL: "https://example.com/a"}}, now); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		mutateDB(t, path, func(tx *bolt.Tx) error {
			return tx.Bucket(entriesBucket).Put([]byte("a"), []byte("not-json"))
		})
		_, err := Open(path, Options{MaxEntries: 2, LockTimeout: 10 * time.Millisecond})
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Open error = %v, want ErrCorrupt", err)
		}
	})

	t.Run("lease sequence rollback", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "frontier.db")
		f := openTestFrontier(t, path, 2)
		now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
		if err := f.SyncSeeds([]Seed{{Key: "a", JobID: "job", URL: "https://example.com/a"}}, now); err != nil {
			t.Fatal(err)
		}
		if leased, err := f.LeaseDue(now, 1, time.Minute); err != nil || len(leased) != 1 {
			t.Fatalf("LeaseDue = %#v, %v", leased, err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		mutateDB(t, path, func(tx *bolt.Tx) error {
			return tx.Bucket(metaBucket).Put(leaseSeqKey, make([]byte, 8))
		})
		_, err := Open(path, Options{MaxEntries: 2, LockTimeout: 10 * time.Millisecond})
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Open error = %v, want ErrCorrupt", err)
		}
	})
}

func TestOpenRejectsUnsafePathsAndUsesPrivatePermissions(t *testing.T) {
	t.Run("symlink", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "target.db")
		if err := os.WriteFile(target, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(dir, "frontier.db")
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		_, err := Open(link, Options{MaxEntries: 2})
		if !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Open symlink error = %v, want ErrUnsafePath", err)
		}
	})

	t.Run("nonregular", func(t *testing.T) {
		_, err := Open(t.TempDir(), Options{MaxEntries: 2})
		if !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Open directory error = %v, want ErrUnsafePath", err)
		}
	})

	t.Run("symlink parent", func(t *testing.T) {
		dir := t.TempDir()
		realParent := filepath.Join(dir, "real")
		if err := os.Mkdir(realParent, 0o700); err != nil {
			t.Fatal(err)
		}
		linkParent := filepath.Join(dir, "linked")
		if err := os.Symlink(realParent, linkParent); err != nil {
			t.Fatal(err)
		}
		_, err := Open(filepath.Join(linkParent, "frontier.db"), Options{MaxEntries: 2})
		if !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Open through symlink parent error = %v, want ErrUnsafePath", err)
		}
	})

	t.Run("writable parent", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), "shared")
		if err := os.Mkdir(parent, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(parent, 0o777); err != nil {
			t.Fatal(err)
		}
		_, err := Open(filepath.Join(parent, "frontier.db"), Options{MaxEntries: 2})
		if !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Open in writable parent error = %v, want ErrUnsafePath", err)
		}
	})

	t.Run("permissions", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "frontier.db")
		f := openTestFrontier(t, path, 2)
		defer f.Close()
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("mode = %04o, want 0600", got)
		}
	})
}

func TestCompleteRejectsInvalidTransitions(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 2)
	defer f.Close()
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	if err := f.SyncSeeds([]Seed{{Key: "a", JobID: "job", URL: "https://example.com/a"}}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.Complete("a", 1, StateSucceeded, time.Time{}, ""); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("complete unleased error = %v, want ErrInvalidTransition", err)
	}
	if _, err := f.LeaseDue(now, 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := f.Complete("a", 1, StateQueued, time.Time{}, ""); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("complete queued error = %v, want ErrInvalidTransition", err)
	}
	leased, err := f.LeaseDue(now, 1, time.Minute)
	if err != nil || len(leased) != 0 {
		t.Fatalf("second lease while active = %#v, %v", leased, err)
	}
	if err := f.Complete("a", 1, StateDisabled, time.Time{}, "operator disabled"); err != nil {
		t.Fatalf("complete disabled: %v", err)
	}
	counts, err := f.Counts()
	if err != nil {
		t.Fatal(err)
	}
	if counts.Disabled != 1 {
		t.Fatalf("counts after disabled completion = %+v", counts)
	}
	if err := f.Complete("missing", 1, StateSucceeded, now.Add(time.Hour), ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("complete missing error = %v, want ErrNotFound", err)
	}
}

func TestConcurrentCloseIsRaceSafe(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 4)
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	seeds := []Seed{{Key: "a", JobID: "job", URL: "https://example.com/a"}}
	if err := f.SyncSeeds(seeds, now); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 3)
	var wg sync.WaitGroup
	operations := []func() error{
		func() error { _, err := f.Counts(); return err },
		func() error { _, err := f.LeaseDue(now, 1, time.Minute); return err },
		func() error { return f.SyncSeeds(seeds, now) },
	}
	for _, operation := range operations {
		operation := operation
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 50 {
				if err := operation(); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	close(start)
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("concurrent operation error = %v", err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func openTestFrontier(t *testing.T, path string, maxEntries int) *Frontier {
	t.Helper()
	f, err := Open(path, Options{MaxEntries: maxEntries, LockTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return f
}

func mutateDB(t *testing.T, path string, fn func(*bolt.Tx) error) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(fn); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
