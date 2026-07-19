package crawlfrontier

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestReserveHostPersistsAndUsesMaximumInterval(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	f := openTestFrontier(t, path, 4)
	if err := f.SyncSeeds([]Seed{{Key: "a", JobID: "job", URL: "https://example.com/a"}}, now); err != nil {
		t.Fatal(err)
	}

	wait, err := f.ReserveHost("example.com", now, 10*time.Minute)
	if err != nil || wait != 0 {
		t.Fatalf("first ReserveHost = %v, %v", wait, err)
	}
	wait, err = f.ReserveHost("example.com", now.Add(5*time.Minute), 2*time.Minute)
	if err != nil || wait != 5*time.Minute {
		t.Fatalf("shorter interval wait = %v, %v", wait, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	f = openTestFrontier(t, path, 4)
	defer f.Close()
	wait, err = f.ReserveHost("example.com", now.Add(7*time.Minute), 2*time.Minute)
	if err != nil || wait != 3*time.Minute {
		t.Fatalf("blocked reservation mutated persisted interval = %v, %v", wait, err)
	}
	wait, err = f.ReserveHost("example.com", now.Add(10*time.Minute), 2*time.Minute)
	if err != nil || wait != 0 {
		t.Fatalf("due after reopen = %v, %v", wait, err)
	}
	wait, err = f.ReserveHost("example.com", now.Add(11*time.Minute), 10*time.Minute)
	if err != nil || wait != 9*time.Minute {
		t.Fatalf("longer current interval wait = %v, %v", wait, err)
	}
	wait, err = f.ReserveHost("example.com", now.Add(20*time.Minute), time.Minute)
	if err != nil || wait != 0 {
		t.Fatalf("due after unchanged blocked reservation = %v, %v", wait, err)
	}
}

func TestReserveHostIsAtomicAcrossConcurrentCalls(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 4)
	defer f.Close()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	if err := f.SyncSeeds([]Seed{{Key: "a", JobID: "job", URL: "https://example.com/a"}}, now); err != nil {
		t.Fatal(err)
	}

	const callers = 16
	start := make(chan struct{})
	results := make(chan time.Duration, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			wait, err := f.ReserveHost("example.com", now, time.Minute)
			if err != nil {
				errs <- err
				return
			}
			results <- wait
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatalf("ReserveHost: %v", err)
	}
	reserved := 0
	waiting := 0
	for wait := range results {
		switch wait {
		case 0:
			reserved++
		case time.Minute:
			waiting++
		default:
			t.Fatalf("unexpected wait %v", wait)
		}
	}
	if reserved != 1 || waiting != callers-1 {
		t.Fatalf("reserved = %d, waiting = %d", reserved, waiting)
	}
}

func TestReserveHostRejectsInvalidOrUnknownAuthority(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 4)
	defer f.Close()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	if err := f.SyncSeeds([]Seed{{Key: "a", JobID: "job", URL: "https://example.com/a"}}, now); err != nil {
		t.Fatal(err)
	}

	for _, authority := range []string{
		"", "HTTPS://example.com", "https://example.com", "user@example.com",
		"example.com/path", "Example.com", "example.com:080", "[fe80::1%25en0]",
		strings.Repeat("a", maxAuthorityBytes+1),
	} {
		if _, err := f.ReserveHost(authority, now, time.Minute); !errors.Is(err, ErrInvalidAuthority) {
			t.Errorf("ReserveHost(%q) error = %v, want ErrInvalidAuthority", authority, err)
		}
	}
	for _, interval := range []time.Duration{0, -time.Second, time.Hour + time.Nanosecond} {
		if _, err := f.ReserveHost("example.com", now, interval); !errors.Is(err, ErrInvalidOptions) {
			t.Errorf("ReserveHost interval %v error = %v, want ErrInvalidOptions", interval, err)
		}
	}
	if _, err := f.ReserveHost("unknown.example", now, time.Minute); !errors.Is(err, ErrUnknownAuthority) {
		t.Fatalf("unknown authority error = %v, want ErrUnknownAuthority", err)
	}
}

func TestReserveHostAcceptsExplicitPort80ForHTTPSSeed(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 4)
	defer f.Close()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	if err := f.SyncSeeds([]Seed{{Key: "a", JobID: "job", URL: "https://example.com:80/a"}}, now); err != nil {
		t.Fatal(err)
	}
	wait, err := f.ReserveHost("example.com:80", now, time.Minute)
	if err != nil || wait != 0 {
		t.Fatalf("ReserveHost explicit port 80 = %v, %v", wait, err)
	}
}

func TestReserveHostAcceptsDiscoverySourceAuthority(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 4)
	defer f.Close()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	source := DiscoverySourceSeed{Key: "source", JobID: "job", URL: "https://feeds.example/sitemap.xml"}
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{source}, now); err != nil {
		t.Fatal(err)
	}
	wait, err := f.ReserveHost("feeds.example", now, time.Minute)
	if err != nil || wait != 0 {
		t.Fatalf("ReserveHost discovery source = %v, %v", wait, err)
	}
	if err := f.SyncDiscoverySources(nil, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	wait, err = f.ReserveHost("feeds.example", now.Add(time.Minute), time.Minute)
	if err != nil || wait != 0 {
		t.Fatalf("ReserveHost retained source identity = %v, %v", wait, err)
	}
}

func TestSyncSeedsRejectsAuthorityItCouldNotReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	f := openTestFrontier(t, path, 4)
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	err := f.SyncSeeds([]Seed{{Key: "a", JobID: "job", URL: "https://example.com:080/a"}}, now)
	if !errors.Is(err, ErrInvalidSeed) {
		t.Fatalf("SyncSeeds error = %v, want ErrInvalidSeed", err)
	}
	counts, err := f.Counts()
	if err != nil {
		t.Fatal(err)
	}
	if counts.Total != 0 {
		t.Fatalf("counts after rejected seed = %+v", counts)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f = openTestFrontier(t, path, 4)
	defer f.Close()
}

func TestReserveHostRejectsCorruptPersistedState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	f := openTestFrontier(t, path, 4)
	if err := f.SyncSeeds([]Seed{{Key: "a", JobID: "job", URL: "https://example.com/a"}}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ReserveHost("example.com", now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	mutateDB(t, path, func(tx *bolt.Tx) error {
		return tx.Bucket(hostsBucket).Put([]byte("example.com"), []byte("not-json"))
	})
	_, err := Open(path, Options{MaxEntries: 4, LockTimeout: 10 * time.Millisecond})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open error = %v, want ErrCorrupt", err)
	}
}

func TestPruneDisabledRemovesOrphanHostState(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 4)
	defer f.Close()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	seeds := []Seed{
		{Key: "a", JobID: "job", URL: "https://first.example/a"},
		{Key: "b", JobID: "job", URL: "https://second.example/b"},
	}
	if err := f.SyncSeeds(seeds, now); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ReserveHost("first.example", now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ReserveHost("second.example", now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncSeeds(seeds[1:], now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if pruned, err := f.PruneDisabled(1); err != nil || pruned != 1 {
		t.Fatalf("PruneDisabled = %d, %v", pruned, err)
	}
	if got := testHostCount(t, f); got != 1 {
		t.Fatalf("host count after prune = %d, want 1", got)
	}
	if _, err := f.ReserveHost("first.example", now.Add(time.Hour), time.Minute); !errors.Is(err, ErrUnknownAuthority) {
		t.Fatalf("pruned host reservation error = %v, want ErrUnknownAuthority", err)
	}
}

func testHostCount(t *testing.T, f *Frontier) int {
	t.Helper()
	db, release, err := f.acquire()
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	count := 0
	if err := db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(hostsBucket).ForEach(func(_, _ []byte) error {
			count++
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	return count
}
