package crawlfrontier

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestOpenRejectsCorruptSchedulingIndexes(t *testing.T) {
	t.Run("missing due by job", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "frontier.db")
		now := time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC)
		f := openTestFrontier(t, path, 4)
		if err := f.SyncSeeds([]Seed{{Key: "a", JobID: "job", URL: "https://example.com/a"}}, now); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		mutateDB(t, path, func(tx *bolt.Tx) error {
			bucket := tx.Bucket(dueByJobBucket)
			key, _ := bucket.Cursor().First()
			return bucket.Delete(key)
		})
		_, err := Open(path, Options{MaxEntries: 4, LockTimeout: 10 * time.Millisecond})
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Open error = %v, want ErrCorrupt", err)
		}
	})

	t.Run("missing lease expiry", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "frontier.db")
		now := time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC)
		f := openTestFrontier(t, path, 4)
		if err := f.SyncSeeds([]Seed{{Key: "a", JobID: "job", URL: "https://example.com/a"}}, now); err != nil {
			t.Fatal(err)
		}
		if leased, err := f.LeaseDueJob("job", now, 1, time.Minute); err != nil || len(leased) != 1 {
			t.Fatalf("LeaseDueJob = %#v, %v", leased, err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		mutateDB(t, path, func(tx *bolt.Tx) error {
			bucket := tx.Bucket(leasesByExpiryBucket)
			key, _ := bucket.Cursor().First()
			return bucket.Delete(key)
		})
		_, err := Open(path, Options{MaxEntries: 4, LockTimeout: 10 * time.Millisecond})
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Open error = %v, want ErrCorrupt", err)
		}
	})
}

func TestLeaseDueJobIndexOrderingSurvivesTransitionsAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	now := time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC)
	f := openTestFrontier(t, path, 8)
	if err := f.SyncSeeds([]Seed{
		{Key: "b", JobID: "job", URL: "https://example.com/b"},
		{Key: "a", JobID: "job", URL: "https://example.com/a"},
		{Key: "x", JobID: "other", URL: "https://other.example/x"},
	}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f = openTestFrontier(t, path, 8)
	defer f.Close()

	leased, err := f.LeaseDueJob("job", now, 1, time.Minute)
	if err != nil || len(leased) != 1 || leased[0].Key != "a" {
		t.Fatalf("first job lease = %#v, %v", leased, err)
	}
	if err := f.Complete("a", leased[0].LeaseGeneration, StateRetry, now.Add(2*time.Hour), "retry"); err != nil {
		t.Fatal(err)
	}
	leased, err = f.LeaseDueJob("job", now, 1, time.Minute)
	if err != nil || len(leased) != 1 || leased[0].Key != "b" {
		t.Fatalf("second job lease = %#v, %v", leased, err)
	}
	if err := f.Complete("b", leased[0].LeaseGeneration, StateSucceeded, now.Add(3*time.Hour), "stored"); err != nil {
		t.Fatal(err)
	}
	leased, err = f.LeaseDueJob("other", now, 1, time.Minute)
	if err != nil || len(leased) != 1 || leased[0].Key != "x" {
		t.Fatalf("other job lease = %#v, %v", leased, err)
	}
	leased, err = f.LeaseDueJob("job", now.Add(2*time.Hour), 1, time.Minute)
	if err != nil || len(leased) != 1 || leased[0].Key != "a" {
		t.Fatalf("rescheduled job lease = %#v, %v", leased, err)
	}
}

func TestLeaseDueRecoversAllAdjacentExpiredLeaseIndexes(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 8)
	defer f.Close()
	now := time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC)
	if err := f.SyncSeeds([]Seed{
		{Key: "a", JobID: "job", URL: "https://example.com/a"},
		{Key: "b", JobID: "job", URL: "https://example.com/b"},
		{Key: "c", JobID: "job", URL: "https://example.com/c"},
		{Key: "d", JobID: "job", URL: "https://example.com/d"},
	}, now); err != nil {
		t.Fatal(err)
	}
	leased, err := f.LeaseDueJob("job", now, 4, time.Minute)
	if err != nil || len(leased) != 4 {
		t.Fatalf("initial leases = %#v, %v", leased, err)
	}
	leased, err = f.LeaseDueJob("job", now.Add(2*time.Minute), 4, time.Minute)
	if err != nil || len(leased) != 4 {
		t.Fatalf("recovered leases = %#v, %v", leased, err)
	}
	for _, entry := range leased {
		if entry.Attempts != 2 || entry.LastReason != ReasonLeaseExpired {
			t.Fatalf("recovered entry = %#v", entry)
		}
	}
}

func TestSyncSeedsClearsEveryStaleDueIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	f := openTestFrontier(t, path, 8)
	now := time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC)
	if err := f.SyncSeeds([]Seed{
		{Key: "a", JobID: "job", URL: "https://example.com/a"},
		{Key: "b", JobID: "job", URL: "https://example.com/b"},
		{Key: "c", JobID: "job", URL: "https://example.com/c"},
		{Key: "d", JobID: "job", URL: "https://example.com/d"},
	}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncSeeds(nil, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	leased, err := f.LeaseDueJob("job", now.Add(time.Minute), 8, time.Minute)
	if err != nil || len(leased) != 0 {
		t.Fatalf("disabled LeaseDueJob = %#v, %v", leased, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f = openTestFrontier(t, path, 8)
	defer f.Close()
}
