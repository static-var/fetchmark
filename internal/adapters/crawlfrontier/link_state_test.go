package crawlfrontier

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestOneHopLinkSnapshotAndLeaseExpansionCapability(t *testing.T) {
	f := openLinkTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 8, 8, 2)
	defer f.Close()
	now := time.Date(2026, 7, 18, 16, 0, 0, 0, time.UTC)
	parent := Seed{Key: "parent", JobID: "job", URL: "https://example.com/parent"}
	if err := f.SyncSeeds([]Seed{parent}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncLinkPolicies([]LinkPolicy{{JobID: "job", MaxLinksPerPage: 2}}, now); err != nil {
		t.Fatal(err)
	}
	leased := leaseEntryByKey(t, f, "job", parent.Key, now)
	if !leased.CanExpand {
		t.Fatalf("directly owned lease CanExpand = false")
	}
	children := []Seed{
		{Key: "b", JobID: "job", URL: "https://example.com/b"},
		{Key: "a", JobID: "job", URL: "https://example.com/a"},
	}
	counts, err := f.CompleteWithLinks(LinkCompletion{
		Key: parent.Key, LeaseGeneration: leased.LeaseGeneration, Outcome: StateSucceeded,
		NextEligible: now.Add(time.Hour), Links: children,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Reported != 2 || counts.AppliedEdges != 2 || counts.AddedEdges != 2 || counts.Created != 2 || counts.RemovedEdges != 0 {
		t.Fatalf("link counts = %+v", counts)
	}
	if got := linkChildren(t, f, parent.Key); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("persisted children = %#v", got)
	}
	for _, key := range []string{"a", "b"} {
		child := leaseEntryByKey(t, f, "job", key, now)
		if child.CanExpand {
			t.Fatalf("link-only child %q CanExpand = true", key)
		}
		if key == "a" {
			if _, err := f.CompleteWithLinks(LinkCompletion{Key: child.Key, LeaseGeneration: child.LeaseGeneration, Outcome: StateSucceeded, NextEligible: now.Add(time.Hour)}, now); !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("link-only child completion error = %v", err)
			}
		}
		if err := f.Complete(child.Key, child.LeaseGeneration, StateSucceeded, now.Add(time.Hour), "admitted"); err != nil {
			t.Fatal(err)
		}
	}

	leased = leaseEntryByKey(t, f, "job", parent.Key, now.Add(time.Hour))
	if err := f.Complete(parent.Key, leased.LeaseGeneration, StateRetry, now.Add(2*time.Hour), "temporary failure"); err != nil {
		t.Fatal(err)
	}
	if got := linkChildren(t, f, parent.Key); len(got) != 2 {
		t.Fatalf("Complete did not preserve snapshot: %#v", got)
	}
	leased = leaseEntryByKey(t, f, "job", parent.Key, now.Add(2*time.Hour))
	counts, err = f.CompleteWithLinks(LinkCompletion{
		Key: parent.Key, LeaseGeneration: leased.LeaseGeneration, Outcome: StateSucceeded,
		NextEligible: now.Add(3 * time.Hour), Links: []Seed{
			{Key: "c", JobID: "job", URL: "https://example.com/c"},
			{Key: "b", JobID: "job", URL: "https://example.com/b"},
		},
	}, now.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if counts.AppliedEdges != 2 || counts.AddedEdges != 1 || counts.RemovedEdges != 1 || counts.Created != 1 {
		t.Fatalf("replacement counts = %+v", counts)
	}
	if entry := readEntry(t, f, "a"); entry.State != StateDisabled {
		t.Fatalf("removed last-link child = %#v", entry)
	}
}

func TestLinkOwnedChildAndIndexesSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	f := openLinkTestFrontier(t, path, 4, 4, 2)
	now := time.Now().UTC()
	parent := Seed{Key: "parent", JobID: "job", URL: "https://example.com/parent"}
	child := Seed{Key: "child", JobID: "job", URL: "https://example.com/child"}
	if err := f.SyncSeeds([]Seed{parent}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncLinkPolicies([]LinkPolicy{{JobID: "job", MaxLinksPerPage: 2}}, now); err != nil {
		t.Fatal(err)
	}
	leased := leaseEntryByKey(t, f, "job", parent.Key, now)
	if _, err := f.CompleteWithLinks(LinkCompletion{Key: parent.Key, LeaseGeneration: leased.LeaseGeneration, Outcome: StateSucceeded, NextEligible: now.Add(time.Hour), Links: []Seed{child}}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f = openLinkTestFrontier(t, path, 4, 4, 2)
	defer f.Close()
	if got := linkChildren(t, f, parent.Key); len(got) != 1 || got[0] != child.Key {
		t.Fatalf("reopened forward links = %#v", got)
	}
	if got := linkParents(t, f, child.Key); len(got) != 1 || got[0] != parent.Key {
		t.Fatalf("reopened reverse links = %#v", got)
	}
	childLease := leaseEntryByKey(t, f, "job", child.Key, now)
	if childLease.CanExpand {
		t.Fatal("reopened link-only child can expand")
	}
}

func TestLinkOwnershipIsSharedInactiveAndReactivated(t *testing.T) {
	f := openLinkTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 8, 8, 2)
	defer f.Close()
	now := time.Date(2026, 7, 18, 16, 0, 0, 0, time.UTC)
	parents := []Seed{
		{Key: "one", JobID: "job", URL: "https://example.com/one"},
		{Key: "two", JobID: "job", URL: "https://example.com/two"},
	}
	shared := Seed{Key: "shared", JobID: "job", URL: "https://example.com/shared"}
	if err := f.SyncSeeds(parents, now); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncLinkPolicies([]LinkPolicy{{JobID: "job", MaxLinksPerPage: 2}}, now); err != nil {
		t.Fatal(err)
	}
	for _, parent := range parents {
		leased := leaseEntryByKey(t, f, "job", parent.Key, now)
		if _, err := f.CompleteWithLinks(LinkCompletion{
			Key: parent.Key, LeaseGeneration: leased.LeaseGeneration, Outcome: StateSucceeded,
			NextEligible: now.Add(time.Hour), Links: []Seed{shared},
		}, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.SyncSeeds(parents[1:], now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if entry := readEntry(t, f, shared.Key); entry.State == StateDisabled {
		t.Fatalf("shared child disabled while second parent is directly owned: %#v", entry)
	}
	if err := f.SyncSeeds(nil, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if entry := readEntry(t, f, shared.Key); entry.State != StateDisabled {
		t.Fatalf("child with only inactive historical edges = %#v", entry)
	}
	if got := linkParents(t, f, shared.Key); len(got) != 2 {
		t.Fatalf("historical reverse edges = %#v", got)
	}
	if err := f.SyncSeeds(parents[:1], now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if entry := readEntry(t, f, shared.Key); entry.State != StateQueued {
		t.Fatalf("reactivated child = %#v", entry)
	}
	if pruned, err := f.PruneDisabled(8); err != nil || pruned != 0 {
		// The disabled second parent still participates in link history.
		t.Fatalf("PruneDisabled relationship participants = %d, %v", pruned, err)
	}
}

func TestLinkPolicyTransitionClearsSnapshotsAndInvalidatesLeases(t *testing.T) {
	f := openLinkTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 8, 8, 2)
	defer f.Close()
	now := time.Date(2026, 7, 18, 16, 0, 0, 0, time.UTC)
	parent := Seed{Key: "parent", JobID: "job", URL: "https://example.com/parent"}
	child := Seed{Key: "child", JobID: "job", URL: "https://example.com/child"}
	if err := f.SyncSeeds([]Seed{parent}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncLinkPolicies([]LinkPolicy{{JobID: "job", MaxLinksPerPage: 2}}, now); err != nil {
		t.Fatal(err)
	}
	leased := leaseEntryByKey(t, f, "job", parent.Key, now)
	if _, err := f.CompleteWithLinks(LinkCompletion{Key: parent.Key, LeaseGeneration: leased.LeaseGeneration, Outcome: StateSucceeded, NextEligible: now.Add(time.Hour), Links: []Seed{child}}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncLinkPolicies([]LinkPolicy{{JobID: "job", MaxLinksPerPage: 2}}, now.Add(30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if entry := readEntry(t, f, parent.Key); entry.State != StateSucceeded || !entry.NextEligible.Equal(now.Add(time.Hour)) {
		t.Fatalf("unchanged policy mutated parent = %#v", entry)
	}
	if got := linkChildren(t, f, parent.Key); len(got) != 1 {
		t.Fatalf("unchanged policy cleared snapshot = %#v", got)
	}
	leased = leaseEntryByKey(t, f, "job", parent.Key, now.Add(time.Hour))
	if err := f.SyncLinkPolicies([]LinkPolicy{{JobID: "job", MaxLinksPerPage: 1}}, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got := linkChildren(t, f, parent.Key); len(got) != 0 {
		t.Fatalf("snapshot after policy change = %#v", got)
	}
	if entry := readEntry(t, f, child.Key); entry.State != StateDisabled {
		t.Fatalf("derived child after policy change = %#v", entry)
	}
	if _, err := f.CompleteWithLinks(LinkCompletion{Key: parent.Key, LeaseGeneration: leased.LeaseGeneration, Outcome: StateSucceeded, NextEligible: now.Add(2 * time.Hour)}, now.Add(time.Hour)); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("stale completion after policy change = %v", err)
	}
	leased = leaseEntryByKey(t, f, "job", parent.Key, now.Add(time.Hour))
	if !leased.CanExpand {
		t.Fatal("changed enabled policy did not permit expansion")
	}
	if err := f.Complete(parent.Key, leased.LeaseGeneration, StateRetry, now.Add(2*time.Hour), "defer"); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncLinkPolicies(nil, now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	leased = leaseEntryByKey(t, f, "job", parent.Key, now.Add(2*time.Hour))
	if leased.CanExpand {
		t.Fatal("omitted policy still permits expansion")
	}
}

func TestLinkPolicyCanBeNarrowedAfterReopenUnderFormatCeiling(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	now := time.Date(2026, 7, 18, 16, 0, 0, 0, time.UTC)
	f := openLinkTestFrontier(t, path, 4, 64, hardMaxLinksPerPage)
	parent := Seed{Key: "parent", JobID: "job", URL: "https://example.com/parent"}
	if err := f.SyncSeeds([]Seed{parent}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncLinkPolicies([]LinkPolicy{{JobID: "job", MaxLinksPerPage: hardMaxLinksPerPage}}, now); err != nil {
		t.Fatal(err)
	}
	leased := leaseEntryByKey(t, f, "job", parent.Key, now)
	if _, err := f.CompleteWithLinks(LinkCompletion{
		Key: parent.Key, LeaseGeneration: leased.LeaseGeneration, Outcome: StateSucceeded,
		NextEligible: now.Add(time.Hour), Links: []Seed{
			{Key: "a", JobID: "job", URL: "https://example.com/a"},
			{Key: "b", JobID: "job", URL: "https://example.com/b"},
		},
	}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path, Options{MaxEntries: 4, MaxLinkEdges: 64, MaxLinksPerPage: 1, LockTimeout: 10 * time.Millisecond}); err == nil {
		t.Fatal("reopen under active narrowed policy unexpectedly accepted old policy state")
	}
	f = openLinkTestFrontier(t, path, 4, 64, hardMaxLinksPerPage)
	defer f.Close()
	if err := f.SyncLinkPolicies([]LinkPolicy{{JobID: "job", MaxLinksPerPage: 1}}, now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got := linkChildren(t, f, parent.Key); len(got) != 0 {
		t.Fatalf("narrowed policy retained old snapshot = %#v", got)
	}
	if entry := readEntry(t, f, parent.Key); entry.State != StateQueued {
		t.Fatalf("narrowed policy parent = %#v", entry)
	}
}

func TestLinkPolicyCanBeRemovedAfterReopenWithMoreEdgesThanEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	now := time.Date(2026, 7, 18, 16, 0, 0, 0, time.UTC)
	f := openLinkTestFrontier(t, path, 5, 64, hardMaxLinksPerPage)
	parents := []Seed{
		{Key: "p1", JobID: "job", URL: "https://example.com/p1"},
		{Key: "p2", JobID: "job", URL: "https://example.com/p2"},
		{Key: "p3", JobID: "job", URL: "https://example.com/p3"},
	}
	children := []Seed{
		{Key: "c1", JobID: "job", URL: "https://example.com/c1"},
		{Key: "c2", JobID: "job", URL: "https://example.com/c2"},
	}
	if err := f.SyncSeeds(parents, now); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncLinkPolicies([]LinkPolicy{{JobID: "job", MaxLinksPerPage: hardMaxLinksPerPage}}, now); err != nil {
		t.Fatal(err)
	}
	for _, parent := range parents {
		leased := leaseEntryByKey(t, f, "job", parent.Key, now)
		if _, err := f.CompleteWithLinks(LinkCompletion{
			Key: parent.Key, LeaseGeneration: leased.LeaseGeneration, Outcome: StateSucceeded,
			NextEligible: now.Add(time.Hour), Links: children,
		}, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path, Options{MaxEntries: 5, MaxLinkEdges: 0, MaxLinksPerPage: hardMaxLinksPerPage, LockTimeout: 10 * time.Millisecond}); !errors.Is(err, ErrMaxLinkEdges) {
		t.Fatalf("legacy zero edge option error = %v, want %v", err, ErrMaxLinkEdges)
	}
	f = openLinkTestFrontier(t, path, 5, 64, hardMaxLinksPerPage)
	if err := f.SyncLinkPolicies(nil, now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := Open(path, Options{MaxEntries: 5, MaxLinkEdges: 0, MaxLinksPerPage: hardMaxLinksPerPage, LockTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatalf("reopen after policy removal: %v", err)
	}
	defer f.Close()
	for _, parent := range parents {
		if got := linkChildren(t, f, parent.Key); len(got) != 0 {
			t.Fatalf("removed policy retained %q snapshot = %#v", parent.Key, got)
		}
	}
}

func TestCompleteWithLinksCapAndIdentityFailuresAreAtomic(t *testing.T) {
	t.Run("per-page policy cap", func(t *testing.T) {
		f := openLinkTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 4, 4, 2)
		defer f.Close()
		now := time.Now().UTC()
		parent := Seed{Key: "parent", JobID: "job", URL: "https://example.com/parent"}
		if err := f.SyncSeeds([]Seed{parent}, now); err != nil {
			t.Fatal(err)
		}
		if err := f.SyncLinkPolicies([]LinkPolicy{{JobID: "job", MaxLinksPerPage: 1}}, now); err != nil {
			t.Fatal(err)
		}
		leased := leaseEntryByKey(t, f, "job", parent.Key, now)
		_, err := f.CompleteWithLinks(LinkCompletion{
			Key: parent.Key, LeaseGeneration: leased.LeaseGeneration, Outcome: StateSucceeded, NextEligible: now.Add(time.Hour),
			Links: []Seed{{Key: "a", JobID: "job", URL: "https://example.com/a"}, {Key: "b", JobID: "job", URL: "https://example.com/b"}},
		}, now)
		if !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("per-page policy cap error = %v", err)
		}
		assertLeasedAndNoLinkMutation(t, f, parent.Key, leased.LeaseGeneration)
	})

	t.Run("entry cap", func(t *testing.T) {
		f := openLinkTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 2, 4, 2)
		defer f.Close()
		now := time.Now().UTC()
		parent := Seed{Key: "parent", JobID: "job", URL: "https://example.com/parent"}
		if err := f.SyncSeeds([]Seed{parent}, now); err != nil {
			t.Fatal(err)
		}
		if err := f.SyncLinkPolicies([]LinkPolicy{{JobID: "job", MaxLinksPerPage: 2}}, now); err != nil {
			t.Fatal(err)
		}
		leased := leaseEntryByKey(t, f, "job", parent.Key, now)
		_, err := f.CompleteWithLinks(LinkCompletion{
			Key: parent.Key, LeaseGeneration: leased.LeaseGeneration, Outcome: StateSucceeded, NextEligible: now.Add(time.Hour),
			Links: []Seed{{Key: "a", JobID: "job", URL: "https://example.com/a"}, {Key: "b", JobID: "job", URL: "https://example.com/b"}},
		}, now)
		if !errors.Is(err, ErrMaxEntries) {
			t.Fatalf("entry cap error = %v", err)
		}
		assertLeasedAndNoLinkMutation(t, f, parent.Key, leased.LeaseGeneration)
	})

	t.Run("edge cap", func(t *testing.T) {
		f := openLinkTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 4, 1, 2)
		defer f.Close()
		now := time.Now().UTC()
		parent := Seed{Key: "parent", JobID: "job", URL: "https://example.com/parent"}
		if err := f.SyncSeeds([]Seed{parent}, now); err != nil {
			t.Fatal(err)
		}
		if err := f.SyncLinkPolicies([]LinkPolicy{{JobID: "job", MaxLinksPerPage: 2}}, now); err != nil {
			t.Fatal(err)
		}
		leased := leaseEntryByKey(t, f, "job", parent.Key, now)
		_, err := f.CompleteWithLinks(LinkCompletion{
			Key: parent.Key, LeaseGeneration: leased.LeaseGeneration, Outcome: StateSucceeded, NextEligible: now.Add(time.Hour),
			Links: []Seed{{Key: "a", JobID: "job", URL: "https://example.com/a"}, {Key: "b", JobID: "job", URL: "https://example.com/b"}},
		}, now)
		if !errors.Is(err, ErrMaxLinkEdges) {
			t.Fatalf("edge cap error = %v", err)
		}
		assertLeasedAndNoLinkMutation(t, f, parent.Key, leased.LeaseGeneration)
	})

	t.Run("identity conflict", func(t *testing.T) {
		f := openLinkTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 4, 4, 2)
		defer f.Close()
		now := time.Now().UTC()
		parent := Seed{Key: "parent", JobID: "job", URL: "https://example.com/parent"}
		existing := Seed{Key: "target", JobID: "job", URL: "https://example.com/original"}
		if err := f.SyncSeeds([]Seed{parent, existing}, now); err != nil {
			t.Fatal(err)
		}
		if err := f.SyncLinkPolicies([]LinkPolicy{{JobID: "job", MaxLinksPerPage: 2}}, now); err != nil {
			t.Fatal(err)
		}
		leased := leaseEntryByKey(t, f, "job", parent.Key, now)
		_, err := f.CompleteWithLinks(LinkCompletion{
			Key: parent.Key, LeaseGeneration: leased.LeaseGeneration, Outcome: StateSucceeded, NextEligible: now.Add(time.Hour),
			Links: []Seed{{Key: "target", JobID: "job", URL: "https://example.com/different"}},
		}, now)
		if !errors.Is(err, ErrKeyConflict) {
			t.Fatalf("identity conflict error = %v", err)
		}
		assertLeasedAndNoLinkMutation(t, f, parent.Key, leased.LeaseGeneration)
	})
}

func TestAuthoritativeRejectionClearsLinkSnapshot(t *testing.T) {
	f := openLinkTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 4, 4, 2)
	defer f.Close()
	now := time.Now().UTC()
	parent := Seed{Key: "parent", JobID: "job", URL: "https://example.com/parent"}
	child := Seed{Key: "child", JobID: "job", URL: "https://example.com/child"}
	if err := f.SyncSeeds([]Seed{parent}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncLinkPolicies([]LinkPolicy{{JobID: "job", MaxLinksPerPage: 2}}, now); err != nil {
		t.Fatal(err)
	}
	leased := leaseEntryByKey(t, f, "job", parent.Key, now)
	if _, err := f.CompleteWithLinks(LinkCompletion{Key: parent.Key, LeaseGeneration: leased.LeaseGeneration, Outcome: StateSucceeded, NextEligible: now.Add(time.Hour), Links: []Seed{child}}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncSeeds([]Seed{parent, child}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	leased = leaseEntryByKey(t, f, "job", parent.Key, now.Add(time.Hour))
	counts, err := f.CompleteWithLinks(LinkCompletion{Key: parent.Key, LeaseGeneration: leased.LeaseGeneration, Outcome: StateRejected, NextEligible: now.Add(2 * time.Hour), Reason: "noindex"}, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if counts.RemovedEdges != 1 || counts.AppliedEdges != 0 {
		t.Fatalf("rejection counts = %+v", counts)
	}
	if entry := readEntry(t, f, child.Key); entry.State == StateDisabled || !entry.Explicit {
		t.Fatalf("explicit child after authoritative rejection = %#v", entry)
	}
	if err := f.SyncSeeds([]Seed{parent}, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if entry := readEntry(t, f, child.Key); entry.State != StateDisabled {
		t.Fatalf("child after final base ownership removal = %#v", entry)
	}
}

func TestDiscoveryOwnedPageCanExpandButLinkOnlyPageCannot(t *testing.T) {
	f := openLinkTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 8, 8, 2)
	defer f.Close()
	now := time.Date(2026, 7, 18, 16, 0, 0, 0, time.UTC)
	root := DiscoverySourceSeed{Key: "root", JobID: "job", URL: "https://example.com/sitemap.xml"}
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now); err != nil {
		t.Fatal(err)
	}
	source := leaseOneSource(t, f, "job", now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{
		SourceKey: root.Key, LeaseGeneration: source.LeaseGeneration,
		Pages: []DiscoveredPage{{Key: "page", URL: "https://example.com/page"}}, NextEligible: now.Add(time.Hour),
	}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncLinkPolicies([]LinkPolicy{{JobID: "job", MaxLinksPerPage: 1}}, now); err != nil {
		t.Fatal(err)
	}
	page := leaseEntryByKey(t, f, "job", "page", now)
	if !page.CanExpand {
		t.Fatal("active source-owned page cannot expand")
	}
	child := Seed{Key: "child", JobID: "job", URL: "https://example.com/child"}
	if _, err := f.CompleteWithLinks(LinkCompletion{Key: page.Key, LeaseGeneration: page.LeaseGeneration, Outcome: StateSucceeded, NextEligible: now.Add(time.Hour), Links: []Seed{child}}, now); err != nil {
		t.Fatal(err)
	}
	childLease := leaseEntryByKey(t, f, "job", child.Key, now)
	if childLease.CanExpand {
		t.Fatal("link-only child can expand")
	}
}

func TestSchemaV5MigratesToV6WithoutLosingLease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	f := openLinkTestFrontier(t, path, 4, 4, 2)
	now := time.Now().UTC()
	seed := Seed{Key: "page", JobID: "job", URL: "https://example.com/page"}
	if err := f.SyncSeeds([]Seed{seed}, now); err != nil {
		t.Fatal(err)
	}
	leased := leaseEntryByKey(t, f, "job", seed.Key, now)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	mutateDB(t, path, func(tx *bolt.Tx) error {
		for _, bucket := range [][]byte{pageLinksBucket, linkParentsBucket, linkPoliciesBucket} {
			if err := tx.DeleteBucket(bucket); err != nil {
				return err
			}
		}
		version := make([]byte, 8)
		binary.BigEndian.PutUint64(version, previousSchemaVersion)
		return tx.Bucket(metaBucket).Put(schemaKey, version)
	})
	f = openLinkTestFrontier(t, path, 4, 4, 2)
	defer f.Close()
	entry := readEntry(t, f, seed.Key)
	if entry.State != StateLeased || entry.LeaseGeneration != leased.LeaseGeneration {
		t.Fatalf("migrated lease = %#v", entry)
	}
	if err := f.db.View(func(tx *bolt.Tx) error {
		if got := binary.BigEndian.Uint64(tx.Bucket(metaBucket).Get(schemaKey)); got != schemaVersion {
			t.Fatalf("schema version = %d", got)
		}
		for _, bucket := range [][]byte{pageLinksBucket, linkParentsBucket, linkPoliciesBucket} {
			if tx.Bucket(bucket) == nil {
				t.Fatalf("missing migrated bucket %q", bucket)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestOpenRejectsCorruptLinkReverseIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	f := openLinkTestFrontier(t, path, 4, 4, 2)
	now := time.Now().UTC()
	parent := Seed{Key: "parent", JobID: "job", URL: "https://example.com/parent"}
	child := Seed{Key: "child", JobID: "job", URL: "https://example.com/child"}
	if err := f.SyncSeeds([]Seed{parent}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncLinkPolicies([]LinkPolicy{{JobID: "job", MaxLinksPerPage: 2}}, now); err != nil {
		t.Fatal(err)
	}
	leased := leaseEntryByKey(t, f, "job", parent.Key, now)
	if _, err := f.CompleteWithLinks(LinkCompletion{Key: parent.Key, LeaseGeneration: leased.LeaseGeneration, Outcome: StateSucceeded, NextEligible: now.Add(time.Hour), Links: []Seed{child}}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	mutateDB(t, path, func(tx *bolt.Tx) error {
		return tx.Bucket(linkParentsBucket).Delete(membershipKey(child.Key, parent.Key))
	})
	if _, err := Open(path, Options{MaxEntries: 4, MaxLinkEdges: 4, MaxLinksPerPage: 2, LockTimeout: 10 * time.Millisecond}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt reverse index open error = %v", err)
	}
}

func TestOpenRejectsExtraLinkReverseIndexRowAsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	f := openLinkTestFrontier(t, path, 4, 4, 2)
	now := time.Now().UTC()
	parent := Seed{Key: "parent", JobID: "job", URL: "https://example.com/parent"}
	child := Seed{Key: "child", JobID: "job", URL: "https://example.com/child"}
	if err := f.SyncSeeds([]Seed{parent}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncLinkPolicies([]LinkPolicy{{JobID: "job", MaxLinksPerPage: 2}}, now); err != nil {
		t.Fatal(err)
	}
	leased := leaseEntryByKey(t, f, "job", parent.Key, now)
	if _, err := f.CompleteWithLinks(LinkCompletion{Key: parent.Key, LeaseGeneration: leased.LeaseGeneration, Outcome: StateSucceeded, NextEligible: now.Add(time.Hour), Links: []Seed{child}}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	mutateDB(t, path, func(tx *bolt.Tx) error {
		return tx.Bucket(linkParentsBucket).Put(membershipKey(parent.Key, child.Key), indexMarker)
	})
	if _, err := Open(path, Options{MaxEntries: 4, MaxLinkEdges: 4, MaxLinksPerPage: 2, LockTimeout: 10 * time.Millisecond}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("extra reverse index open error = %v", err)
	}
}

func TestLinkOptionsEnforceHardPerPageLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	if _, err := Open(path, Options{MaxEntries: 4, MaxLinksPerPage: hardMaxLinksPerPage + 1}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("oversized MaxLinksPerPage error = %v", err)
	}
	if _, err := Open(path, Options{MaxEntries: 4, MaxLinkEdges: -1}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("negative MaxLinkEdges error = %v", err)
	}
}

func openLinkTestFrontier(t *testing.T, path string, maxEntries, maxLinkEdges, maxLinksPerPage int) *Frontier {
	t.Helper()
	f, err := Open(path, Options{
		MaxEntries: maxEntries, MaxSources: maxEntries, MaxPagesPerSource: maxEntries,
		MaxChildrenPerSource: maxEntries, MaxMemberships: maxEntries, MaxSourceEdges: maxEntries,
		MaxSourceDepth: 16, MaxLinkEdges: maxLinkEdges, MaxLinksPerPage: maxLinksPerPage,
		LockTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func leaseEntryByKey(t *testing.T, f *Frontier, jobID, key string, now time.Time) Entry {
	t.Helper()
	for range f.maxEntries {
		entries, err := f.LeaseDueJob(jobID, now, 1, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 0 {
			break
		}
		entry := entries[0]
		if entry.Key == key {
			return entry
		}
		if err := f.Complete(entry.Key, entry.LeaseGeneration, StateRetry, now.Add(time.Minute), "test defer"); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatalf("entry %q not leased", key)
	return Entry{}
}

func readEntry(t *testing.T, f *Frontier, key string) Entry {
	t.Helper()
	var entry Entry
	if err := f.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(entriesBucket).Get([]byte(key))
		var err error
		entry, err = decodeEntry([]byte(key), raw)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return entry
}

func linkChildren(t *testing.T, f *Frontier, parent string) []string {
	t.Helper()
	var values []string
	if err := f.db.View(func(tx *bolt.Tx) error {
		values = membershipSeconds(tx.Bucket(pageLinksBucket), parent)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return values
}

func linkParents(t *testing.T, f *Frontier, child string) []string {
	t.Helper()
	var values []string
	if err := f.db.View(func(tx *bolt.Tx) error {
		values = membershipSeconds(tx.Bucket(linkParentsBucket), child)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return values
}

func assertLeasedAndNoLinkMutation(t *testing.T, f *Frontier, key string, generation uint64) {
	t.Helper()
	entry := readEntry(t, f, key)
	if entry.State != StateLeased || entry.LeaseGeneration != generation {
		t.Fatalf("entry after rolled-back completion = %#v", entry)
	}
	if got := linkChildren(t, f, key); len(got) != 0 {
		t.Fatalf("links after rolled-back completion = %#v", got)
	}
}
