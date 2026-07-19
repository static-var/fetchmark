package packevidencegate

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCanonicalHostCollapsesEquivalentEffectiveHosts(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"Example.COM.":              "example.com",
		"192.0.2.1":                 "192.0.2.1",
		"192.000.002.001":           "",
		"2001:0DB8:0000:0000::0001": "2001:db8::1",
		"[2001:db8::1]":             "",
		"":                          "",
		"bad host":                  "",
	}
	for raw, want := range tests {
		raw, want := raw, want
		t.Run(raw, func(t *testing.T) {
			got, err := CanonicalHost(raw)
			if want == "" {
				if err == nil {
					t.Fatalf("CanonicalHost(%q) = %q, want error", raw, got)
				}
				return
			}
			if err != nil || got != want {
				t.Fatalf("CanonicalHost(%q) = %q, %v; want %q", raw, got, err, want)
			}
		})
	}
}

func TestStorePersistsRetryAfterBeforeReleaseAndRestart(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	path := filepath.Join(t.TempDir(), "gate.db")
	policy := testPolicy(now)

	store, err := Open(path, policy, clock)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Acquire("attempt-1", "EXAMPLE.org.")
	if err != nil || !first.Granted {
		t.Fatalf("first acquire = %+v, %v", first, err)
	}
	if applied, err := store.RecordResponse(first.Lease, 429, "120"); err != nil || applied != 2*time.Minute {
		t.Fatalf("record response = %v, %v", applied, err)
	}
	if err := store.Release(first.Lease); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(path, policy, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	waiting, err := store.Acquire("attempt-2", "example.org")
	if err != nil || waiting.Granted || !waiting.RetryAt.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("cooldown acquire = %+v, %v", waiting, err)
	}
	now = now.Add(2*time.Minute + time.Nanosecond)
	second, err := store.Acquire("attempt-2", "example.org")
	if err != nil || !second.Granted || second.Lease.Generation <= first.Lease.Generation {
		t.Fatalf("post-cooldown acquire = %+v, %v", second, err)
	}
	if err := store.Release(first.Lease); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("stale release error = %v, want ErrLeaseMismatch", err)
	}
}

func TestStoreCrashLeaseSurvivesRestartConservatively(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	path := filepath.Join(t.TempDir(), "gate.db")
	policy := testPolicy(now)
	store, err := Open(path, policy, clock)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Acquire("attempt-1", "example.org")
	if err != nil || !first.Granted {
		t.Fatalf("first acquire = %+v, %v", first, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	now = now.Add(policy.RequestTimeout + time.Minute)
	store, err = Open(path, policy, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	waiting, err := store.Acquire("attempt-2", "example.org")
	if err != nil || waiting.Granted || !waiting.RetryAt.Equal(first.Lease.ExpiresAt) {
		t.Fatalf("crash recovery acquire = %+v, %v", waiting, err)
	}
	now = first.Lease.ExpiresAt.Add(time.Nanosecond)
	recovered, err := store.Acquire("attempt-2", "example.org")
	if err != nil || !recovered.Granted {
		t.Fatalf("expired crash lease acquire = %+v, %v", recovered, err)
	}
}

func TestStoreBindsPolicyAndAggregateConcurrency(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "gate.db")
	policy := testPolicy(now)
	policy.GlobalConcurrency = 1
	store, err := Open(path, policy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Acquire("attempt-1", "one.example")
	if err != nil || !first.Granted {
		t.Fatalf("first acquire = %+v, %v", first, err)
	}
	blocked, err := store.Acquire("attempt-2", "two.example")
	if err != nil || blocked.Granted || !blocked.RetryAt.Equal(first.Lease.ExpiresAt) {
		t.Fatalf("global concurrency acquire = %+v, %v", blocked, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	mismatch := policy
	mismatch.ConfigSHA256 = strings.Repeat("b", 64)
	if _, err := Open(path, mismatch, func() time.Time { return now }); !errors.Is(err, ErrPolicyMismatch) {
		t.Fatalf("mismatched open error = %v, want ErrPolicyMismatch", err)
	}
}

func testPolicy(now time.Time) Policy {
	return Policy{
		RunID:             "evidence-run-1",
		ConfigSHA256:      strings.Repeat("a", 64),
		MinHostInterval:   time.Second,
		GlobalConcurrency: 2,
		RequestTimeout:    2 * time.Second,
		MaxRetryAfter:     time.Hour,
		ControlGrace:      5 * time.Second,
		ExpiresAt:         now.Add(24 * time.Hour),
	}
}
