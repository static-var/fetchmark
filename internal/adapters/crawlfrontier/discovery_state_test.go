package crawlfrontier

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestDiscoverySnapshotPersistsAnd304PreservesMemberships(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	f := openTestFrontier(t, path, 16)
	root := DiscoverySourceSeed{Key: "root", JobID: "job", URL: "https://example.com/sitemap.xml"}
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now); err != nil {
		t.Fatal(err)
	}
	leased := leaseOneSource(t, f, "job", now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{
		SourceKey:       "root",
		LeaseGeneration: leased.LeaseGeneration,
		Pages: []DiscoveredPage{
			{Key: "a", URL: "https://example.com/a"},
			{Key: "b", URL: "https://example.com/b"},
		},
		Children:     []DiscoverySourceSeed{{Key: "child", JobID: "job", URL: "https://example.com/feed.xml"}},
		Validators:   SourceValidators{ETag: `"v1"`, LastModified: "Sat, 18 Jul 2026 10:00:00 GMT"},
		NextEligible: now.Add(time.Hour),
	}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	f = openTestFrontier(t, path, 16)
	defer f.Close()
	sources, err := f.LeaseDueDiscoverySourcesJob("job", now.Add(30*time.Minute), 10, time.Minute)
	if err != nil || len(sources) != 1 || sources[0].Key != "child" {
		t.Fatalf("early source leases = %#v, %v", sources, err)
	}
	child := sources[0]
	if child.Kind != DiscoverySourceAuto || child.RootKey != "root" || child.ParentKey != "root" || child.Depth != 1 || child.Configured {
		t.Fatalf("persisted child ancestry = %#v", child)
	}
	if err := f.CompleteDiscoverySource(child.Key, child.LeaseGeneration, StateRetry, now.Add(2*time.Hour), "defer child"); err != nil {
		t.Fatal(err)
	}
	leased = leaseOneSource(t, f, "job", now.Add(time.Hour))
	if leased.Key != "root" {
		t.Fatalf("due source = %#v, want root", leased)
	}
	if err := f.PreserveDiscoverySnapshot("root", leased.LeaseGeneration, SourceValidators{ETag: `"v2"`}, now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncSeeds(nil, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	pages, err := f.LeaseDueJob("job", now.Add(time.Hour), 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) != 2 || pages[0].Key != "a" || pages[1].Key != "b" {
		t.Fatalf("pages after 304 preserve = %#v", pages)
	}
}

func TestDiscoveryPageOwnershipAcrossSourcesAndExplicitSeeds(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 16)
	defer f.Close()
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	sources := []DiscoverySourceSeed{
		{Key: "one", JobID: "job", URL: "https://one.example/sitemap.xml"},
		{Key: "two", JobID: "job", URL: "https://two.example/sitemap.xml"},
	}
	if err := f.SyncDiscoverySources(sources, now); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"one", "two"} {
		leased := leaseSourceByKey(t, f, "job", key, now)
		if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{
			SourceKey: key, LeaseGeneration: leased.LeaseGeneration,
			Pages:        []DiscoveredPage{{Key: "shared", URL: "https://pages.example/shared"}},
			NextEligible: now.Add(time.Hour),
		}, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.SyncDiscoverySources(sources[1:], now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	pages, err := f.LeaseDueJob("job", now.Add(time.Minute), 1, time.Minute)
	if err != nil || len(pages) != 1 || pages[0].Key != "shared" {
		t.Fatalf("shared page after first source removal = %#v, %v", pages, err)
	}
	if err := f.Complete("shared", pages[0].LeaseGeneration, StateSucceeded, now.Add(time.Hour), "stored"); err != nil {
		t.Fatal(err)
	}

	if err := f.SyncSeeds([]Seed{{Key: "shared", JobID: "job", URL: "https://pages.example/shared"}}, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncDiscoverySources(nil, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncSeeds(nil, now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	pages, err = f.LeaseDueJob("job", now.Add(2*time.Hour), 1, time.Minute)
	if err != nil || len(pages) != 0 {
		t.Fatalf("page after all ownership removed = %#v, %v", pages, err)
	}
}

func TestDiscoveryManyParentAncestryUsesMinimumDepthAndStableParent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	f := openTestFrontier(t, path, 12)
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	root := DiscoverySourceSeed{Key: "root", JobID: "job", URL: "https://example.com/root.xml"}
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now); err != nil {
		t.Fatal(err)
	}
	rootLease := leaseOneSource(t, f, "job", now)
	parents := []DiscoverySourceSeed{
		{Key: "left", JobID: "job", URL: "https://example.com/left.xml"},
		{Key: "right", JobID: "job", URL: "https://example.com/right.xml"},
	}
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: root.Key, LeaseGeneration: rootLease.LeaseGeneration, Children: parents, NextEligible: now.Add(time.Hour)}, now); err != nil {
		t.Fatal(err)
	}
	leaf := DiscoverySourceSeed{Key: "leaf", JobID: "job", URL: "https://example.com/leaf.xml"}
	left := leaseSourceByKey(t, f, "job", "left", now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: left.Key, LeaseGeneration: left.LeaseGeneration, Children: []DiscoverySourceSeed{leaf}, NextEligible: now.Add(time.Hour)}, now); err != nil {
		t.Fatal(err)
	}
	right := leaseSourceByKey(t, f, "job", "right", now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: right.Key, LeaseGeneration: right.LeaseGeneration, Children: []DiscoverySourceSeed{leaf}, NextEligible: now.Add(time.Hour)}, now); err != nil {
		t.Fatal(err)
	}
	leafLease := leaseSourceByKey(t, f, "job", "leaf", now.Add(time.Hour))
	if leafLease.RootKey != root.Key || leafLease.ParentKey != "left" || leafLease.Depth != 2 {
		t.Fatalf("diamond ancestry = %#v", leafLease)
	}
	if err := f.CompleteDiscoverySource(leafLease.Key, leafLease.LeaseGeneration, StateRetry, now.Add(2*time.Hour), "inspect later"); err != nil {
		t.Fatal(err)
	}
	left = leaseSourceByKey(t, f, "job", "left", now.Add(time.Hour))
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: left.Key, LeaseGeneration: left.LeaseGeneration, NextEligible: now.Add(2 * time.Hour)}, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f = openTestFrontier(t, path, 12)
	defer f.Close()
	leafLease = leaseSourceByKey(t, f, "job", "leaf", now.Add(2*time.Hour))
	if leafLease.RootKey != root.Key || leafLease.ParentKey != "right" || leafLease.Depth != 2 {
		t.Fatalf("recomputed ancestry after reopen = %#v", leafLease)
	}
}

func TestDiscoveryExactReplacementIgnoresRetainedOverDepthCrossEdge(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	f, err := Open(path, Options{MaxEntries: 8, MaxSources: 4, MaxPagesPerSource: 8, MaxChildrenPerSource: 4, MaxMemberships: 8, MaxSourceEdges: 8, MaxSourceDepth: 2, LockTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	root := DiscoverySourceSeed{Key: "root", JobID: "job", URL: "https://example.com/root.xml"}
	parent := DiscoverySourceSeed{Key: "a-parent", JobID: "job", URL: "https://example.com/parent.xml"}
	middle := DiscoverySourceSeed{Key: "b-middle", JobID: "job", URL: "https://example.com/middle.xml"}
	shallow := DiscoverySourceSeed{Key: "c-shallow", JobID: "job", URL: "https://example.com/shallow.xml"}
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now); err != nil {
		t.Fatal(err)
	}
	rootLease := leaseOneSource(t, f, "job", now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: root.Key, LeaseGeneration: rootLease.LeaseGeneration, Children: []DiscoverySourceSeed{parent, middle, shallow}, NextEligible: now.Add(time.Hour)}, now); err != nil {
		t.Fatal(err)
	}
	parentLease := leaseSourceByKey(t, f, "job", parent.Key, now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: parent.Key, LeaseGeneration: parentLease.LeaseGeneration, Children: []DiscoverySourceSeed{middle}, NextEligible: now.Add(time.Hour)}, now); err != nil {
		t.Fatal(err)
	}
	middleLease := leaseSourceByKey(t, f, "job", middle.Key, now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: middle.Key, LeaseGeneration: middleLease.LeaseGeneration, Children: []DiscoverySourceSeed{shallow}, NextEligible: now.Add(time.Hour)}, now); err != nil {
		t.Fatal(err)
	}
	rootLease = leaseSourceByKey(t, f, "job", root.Key, now.Add(time.Hour))
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: root.Key, LeaseGeneration: rootLease.LeaseGeneration, Children: []DiscoverySourceSeed{parent, shallow}, NextEligible: now.Add(2 * time.Hour)}, now.Add(time.Hour)); err != nil {
		t.Fatalf("exact replacement with retained deep cross-edge: %v", err)
	}
}

func TestDiscoveryReopenWithLowerDepthDisablesAndLaterRestoresSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	now := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	openAtDepth := func(depth int) (*Frontier, error) {
		return Open(path, Options{
			MaxEntries: 8, MaxSources: 8, MaxPagesPerSource: 8,
			MaxChildrenPerSource: 8, MaxMemberships: 8, MaxSourceEdges: 8,
			MaxSourceDepth: depth, LockTimeout: 10 * time.Millisecond,
		})
	}
	f, err := openAtDepth(2)
	if err != nil {
		t.Fatal(err)
	}
	root := DiscoverySourceSeed{Key: "root", JobID: "job", URL: "https://example.com/root.xml"}
	child := DiscoverySourceSeed{Key: "child", JobID: "job", URL: "https://example.com/child.xml"}
	grandchild := DiscoverySourceSeed{Key: "grandchild", JobID: "job", URL: "https://example.com/grandchild.xml"}
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now); err != nil {
		t.Fatal(err)
	}
	rootLease := leaseOneSource(t, f, "job", now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: root.Key, LeaseGeneration: rootLease.LeaseGeneration, Children: []DiscoverySourceSeed{child}, NextEligible: now.Add(24 * time.Hour)}, now); err != nil {
		t.Fatal(err)
	}
	childLease := leaseSourceByKey(t, f, "job", child.Key, now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: child.Key, LeaseGeneration: childLease.LeaseGeneration, Children: []DiscoverySourceSeed{grandchild}, NextEligible: now.Add(24 * time.Hour)}, now); err != nil {
		t.Fatal(err)
	}
	grandchildLease := leaseSourceByKey(t, f, "job", grandchild.Key, now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{
		SourceKey: grandchild.Key, LeaseGeneration: grandchildLease.LeaseGeneration,
		Pages:        []DiscoveredPage{{Key: "page", URL: "https://example.com/page"}},
		Validators:   SourceValidators{ETag: `"retained"`},
		NextEligible: now.Add(24 * time.Hour),
	}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	f, err = openAtDepth(1)
	if err != nil {
		t.Fatalf("reopen at lower depth: %v", err)
	}
	grandchildEntry := readDiscoverySource(t, f, grandchild.Key)
	if grandchildEntry.State != StateDisabled || !grandchildEntry.HasSnapshot || grandchildEntry.Depth != 2 || grandchildEntry.ETag != `"retained"` {
		t.Fatalf("out-of-depth retained source = %#v", grandchildEntry)
	}
	if pages, err := f.LeaseDueJob("job", now.Add(2*time.Hour), 8, time.Minute); err != nil || len(pages) != 0 {
		t.Fatalf("out-of-depth page leases = %#v, %v", pages, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	f, err = openAtDepth(2)
	if err != nil {
		t.Fatalf("reopen after restoring depth: %v", err)
	}
	defer f.Close()
	grandchildEntry = readDiscoverySource(t, f, grandchild.Key)
	if grandchildEntry.State == StateDisabled || !grandchildEntry.HasSnapshot || grandchildEntry.Depth != 2 || grandchildEntry.ETag != `"retained"` {
		t.Fatalf("restored source = %#v", grandchildEntry)
	}
	pages, err := f.LeaseDueJob("job", time.Now().UTC().Add(time.Minute), 8, time.Minute)
	if err != nil || len(pages) != 1 || pages[0].Key != "page" {
		t.Fatalf("restored page leases = %#v, %v", pages, err)
	}
}

func TestDiscoveryConfiguredRootKindChangeInvalidatesSnapshotTrust(t *testing.T) {
	for _, initialKind := range []DiscoverySourceKind{DiscoverySourceAuto, DiscoverySourceFeed} {
		t.Run(string(initialKind), func(t *testing.T) {
			f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 8)
			defer f.Close()
			now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
			root := DiscoverySourceSeed{Key: "root", JobID: "job", URL: "https://example.com/source", Kind: initialKind}
			if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now); err != nil {
				t.Fatal(err)
			}
			leased := leaseOneSource(t, f, "job", now)
			if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{
				SourceKey: root.Key, LeaseGeneration: leased.LeaseGeneration,
				Pages:        []DiscoveredPage{{Key: "cross-origin", URL: "https://outside.example/feed-entry"}},
				Validators:   SourceValidators{ETag: `"old"`, LastModified: "old-date"},
				NextEligible: now.Add(time.Hour),
			}, now); err != nil {
				t.Fatal(err)
			}
			root.Kind = DiscoverySourceSitemap
			if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			changed := readDiscoverySource(t, f, root.Key)
			if changed.Kind != DiscoverySourceSitemap || changed.HasSnapshot || changed.ETag != "" || changed.LastModified != "" || changed.State != StateQueued {
				t.Fatalf("root after trust-policy change = %#v", changed)
			}
			if pages, err := f.LeaseDueJob("job", now.Add(time.Minute), 8, time.Minute); err != nil || len(pages) != 0 {
				t.Fatalf("old-policy page leases = %#v, %v", pages, err)
			}
			leased = leaseOneSource(t, f, "job", now.Add(time.Minute))
			if err := f.PreserveDiscoverySnapshot(root.Key, leased.LeaseGeneration, SourceValidators{}, now.Add(time.Hour)); !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("304 after kind change error = %v, want ErrInvalidTransition", err)
			}
			if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{
				SourceKey: root.Key, LeaseGeneration: leased.LeaseGeneration,
				Pages:      []DiscoveredPage{{Key: "fresh", URL: "https://example.com/fresh"}},
				Validators: SourceValidators{ETag: `"new"`}, NextEligible: now.Add(time.Hour),
			}, now.Add(time.Minute)); err != nil {
				t.Fatalf("fresh snapshot after rejected 304: %v", err)
			}
			pages, err := f.LeaseDueJob("job", now.Add(time.Minute), 8, time.Minute)
			if err != nil || len(pages) != 1 || pages[0].Key != "fresh" {
				t.Fatalf("new-policy page leases = %#v, %v", pages, err)
			}
		})
	}
}

func TestDiscoveryConfiguredRootKindChangeDropsOldChildEdgesAtomically(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 8)
	defer f.Close()
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	root := DiscoverySourceSeed{Key: "root", JobID: "job", URL: "https://example.com/index.xml", Kind: DiscoverySourceSitemap}
	child := DiscoverySourceSeed{Key: "child", JobID: "job", URL: "https://example.com/child.xml", Kind: DiscoverySourceSitemap}
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now); err != nil {
		t.Fatal(err)
	}
	leased := leaseOneSource(t, f, "job", now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{
		SourceKey: root.Key, LeaseGeneration: leased.LeaseGeneration,
		Children: []DiscoverySourceSeed{child}, Validators: SourceValidators{ETag: `"index"`}, NextEligible: now.Add(time.Hour),
	}, now); err != nil {
		t.Fatal(err)
	}
	childLease := leaseSourceByKey(t, f, "job", child.Key, now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{
		SourceKey: child.Key, LeaseGeneration: childLease.LeaseGeneration,
		Pages: []DiscoveredPage{{Key: "child-page", URL: "https://example.com/child-page"}}, NextEligible: now.Add(time.Hour),
	}, now); err != nil {
		t.Fatal(err)
	}
	root.Kind = DiscoverySourceFeed
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	conflict := root
	conflict.URL = "https://example.com/other-feed"
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{conflict}, now.Add(2*time.Minute)); !errors.Is(err, ErrKeyConflict) {
		t.Fatalf("conflicting root update error = %v, want ErrKeyConflict", err)
	}
	changedRoot := readDiscoverySource(t, f, root.Key)
	if changedRoot.Kind != DiscoverySourceFeed || changedRoot.HasSnapshot || changedRoot.ETag != "" {
		t.Fatalf("root after kind change and conflict = %#v", changedRoot)
	}
	retainedChild := readDiscoverySource(t, f, child.Key)
	if retainedChild.Kind != DiscoverySourceSitemap || retainedChild.State != StateDisabled || !retainedChild.HasSnapshot {
		t.Fatalf("old child after root kind change = %#v", retainedChild)
	}
	if pages, err := f.LeaseDueJob("job", now.Add(2*time.Hour), 8, time.Minute); err != nil || len(pages) != 0 {
		t.Fatalf("old child page leases = %#v, %v", pages, err)
	}
}

func TestDiscoveryRootKindChangeRollsBackWhenAnotherRootConflicts(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 8)
	defer f.Close()
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	roots := []DiscoverySourceSeed{
		{Key: "one", JobID: "job", URL: "https://one.example/feed", Kind: DiscoverySourceFeed},
		{Key: "two", JobID: "job", URL: "https://two.example/feed", Kind: DiscoverySourceFeed},
	}
	if err := f.SyncDiscoverySources(roots, now); err != nil {
		t.Fatal(err)
	}
	oneLease := leaseSourceByKey(t, f, "job", roots[0].Key, now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{
		SourceKey: roots[0].Key, LeaseGeneration: oneLease.LeaseGeneration,
		Pages:      []DiscoveredPage{{Key: "page", URL: "https://outside.example/page"}},
		Validators: SourceValidators{ETag: `"kept"`}, NextEligible: now.Add(time.Hour),
	}, now); err != nil {
		t.Fatal(err)
	}
	update := append([]DiscoverySourceSeed(nil), roots...)
	update[0].Kind = DiscoverySourceSitemap
	update[1].URL = "https://two.example/conflict"
	if err := f.SyncDiscoverySources(update, now.Add(time.Minute)); !errors.Is(err, ErrKeyConflict) {
		t.Fatalf("mixed root update error = %v, want ErrKeyConflict", err)
	}
	unchanged := readDiscoverySource(t, f, roots[0].Key)
	if unchanged.Kind != DiscoverySourceFeed || !unchanged.HasSnapshot || unchanged.ETag != `"kept"` {
		t.Fatalf("root after rolled-back mixed update = %#v", unchanged)
	}
	pages, err := f.LeaseDueJob("job", now.Add(time.Minute), 8, time.Minute)
	if err != nil || len(pages) != 1 || pages[0].Key != "page" {
		t.Fatalf("page after rolled-back mixed update = %#v, %v", pages, err)
	}
}

func TestDiscoveryConfiguredRootKindChangeInvalidatesInFlightLease(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 4)
	defer f.Close()
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	root := DiscoverySourceSeed{Key: "root", JobID: "job", URL: "https://example.com/root.xml", Kind: DiscoverySourceAuto}
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now); err != nil {
		t.Fatal(err)
	}
	oldLease := leaseOneSource(t, f, "job", now)
	root.Kind = DiscoverySourceSitemap
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	stale := DiscoverySnapshot{SourceKey: root.Key, LeaseGeneration: oldLease.LeaseGeneration, NextEligible: now.Add(time.Hour)}
	if err := f.ApplyDiscoverySnapshot(stale, now.Add(time.Minute)); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("old-kind lease completion error = %v, want ErrInvalidTransition", err)
	}
	newLease := leaseOneSource(t, f, "job", now.Add(time.Minute))
	if newLease.Kind != DiscoverySourceSitemap || newLease.LeaseGeneration <= oldLease.LeaseGeneration {
		t.Fatalf("replacement kind lease = %#v, old generation %d", newLease, oldLease.LeaseGeneration)
	}
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: root.Key, LeaseGeneration: newLease.LeaseGeneration, NextEligible: now.Add(time.Hour)}, now.Add(time.Minute)); err != nil {
		t.Fatalf("new-kind lease completion: %v", err)
	}
}

func TestDiscoveryRootRemovalDisablesDescendants(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 16)
	defer f.Close()
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	root := DiscoverySourceSeed{Key: "root", JobID: "job", URL: "https://example.com/root.xml"}
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now); err != nil {
		t.Fatal(err)
	}
	rootLease := leaseOneSource(t, f, "job", now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{
		SourceKey: "root", LeaseGeneration: rootLease.LeaseGeneration,
		Children:     []DiscoverySourceSeed{{Key: "child", JobID: "job", URL: "https://example.com/child.xml"}},
		NextEligible: now.Add(time.Hour),
	}, now); err != nil {
		t.Fatal(err)
	}
	childLease := leaseSourceByKey(t, f, "job", "child", now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{
		SourceKey: "child", LeaseGeneration: childLease.LeaseGeneration,
		Pages:        []DiscoveredPage{{Key: "page", URL: "https://example.com/page"}},
		NextEligible: now.Add(time.Hour),
	}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncDiscoverySources(nil, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if sources, err := f.LeaseDueDiscoverySourcesJob("job", now.Add(2*time.Hour), 10, time.Minute); err != nil || len(sources) != 0 {
		t.Fatalf("sources after root removal = %#v, %v", sources, err)
	}
	if pages, err := f.LeaseDueJob("job", now.Add(2*time.Hour), 10, time.Minute); err != nil || len(pages) != 0 {
		t.Fatalf("pages after root removal = %#v, %v", pages, err)
	}
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now.Add(3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	pages, err := f.LeaseDueJob("job", now.Add(3*time.Hour), 10, time.Minute)
	if err != nil || len(pages) != 1 || pages[0].Key != "page" {
		t.Fatalf("page after root restoration = %#v, %v", pages, err)
	}
}

func TestDiscoveryRootRemovalInvalidatesDescendantLease(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 4)
	defer f.Close()
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	root := DiscoverySourceSeed{Key: "root", JobID: "job", URL: "https://example.com/root.xml"}
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now); err != nil {
		t.Fatal(err)
	}
	rootLease := leaseOneSource(t, f, "job", now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{
		SourceKey: root.Key, LeaseGeneration: rootLease.LeaseGeneration,
		Children:     []DiscoverySourceSeed{{Key: "child", JobID: "job", URL: "https://example.com/child.xml"}},
		NextEligible: now.Add(time.Hour),
	}, now); err != nil {
		t.Fatal(err)
	}
	childLease := leaseSourceByKey(t, f, "job", "child", now)
	if err := f.SyncDiscoverySources(nil, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	stale := DiscoverySnapshot{SourceKey: childLease.Key, LeaseGeneration: childLease.LeaseGeneration, NextEligible: now.Add(time.Hour)}
	if err := f.ApplyDiscoverySnapshot(stale, now.Add(time.Minute)); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("removed descendant completion error = %v, want ErrInvalidTransition", err)
	}
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	released := leaseSourceByKey(t, f, "job", "child", now.Add(2*time.Minute))
	if released.LeaseGeneration <= childLease.LeaseGeneration {
		t.Fatalf("re-added lease generation = %d, want > %d", released.LeaseGeneration, childLease.LeaseGeneration)
	}
}

func TestPruneDisabledPreservesInactiveDiscoverySnapshot(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 4)
	defer f.Close()
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	root := DiscoverySourceSeed{Key: "root", JobID: "job", URL: "https://example.com/root.xml"}
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now); err != nil {
		t.Fatal(err)
	}
	leased := leaseOneSource(t, f, "job", now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{
		SourceKey: root.Key, LeaseGeneration: leased.LeaseGeneration,
		Pages:        []DiscoveredPage{{Key: "page", URL: "https://example.com/page"}},
		NextEligible: now.Add(time.Hour),
	}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncDiscoverySources(nil, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if pruned, err := f.PruneDisabled(4); err != nil || pruned != 0 {
		t.Fatalf("PruneDisabled inactive snapshot = %d, %v", pruned, err)
	}
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	pages, err := f.LeaseDueJob("job", now.Add(2*time.Minute), 4, time.Minute)
	if err != nil || len(pages) != 1 || pages[0].Key != "page" {
		t.Fatalf("restored pages after prune = %#v, %v", pages, err)
	}
}

func TestDiscovery304RequiresCommittedSnapshot(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 4)
	defer f.Close()
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	root := DiscoverySourceSeed{Key: "root", JobID: "job", URL: "https://example.com/root.xml"}
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now); err != nil {
		t.Fatal(err)
	}
	leased := leaseOneSource(t, f, "job", now)
	if err := f.PreserveDiscoverySnapshot(root.Key, leased.LeaseGeneration, SourceValidators{ETag: `"unexpected"`}, now.Add(time.Hour)); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("304 before first snapshot error = %v, want ErrInvalidTransition", err)
	}
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: root.Key, LeaseGeneration: leased.LeaseGeneration, NextEligible: now.Add(time.Hour)}, now); err != nil {
		t.Fatalf("lease changed after rejected 304: %v", err)
	}
}

func TestDiscovery304ReplacesValidatorsExactly(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 4)
	defer f.Close()
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	root := DiscoverySourceSeed{Key: "root", JobID: "job", URL: "https://example.com/root.xml"}
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now); err != nil {
		t.Fatal(err)
	}
	leased := leaseOneSource(t, f, "job", now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{
		SourceKey: root.Key, LeaseGeneration: leased.LeaseGeneration,
		Validators: SourceValidators{ETag: `"v1"`, LastModified: "old"}, NextEligible: now.Add(time.Hour),
	}, now); err != nil {
		t.Fatal(err)
	}
	leased = leaseOneSource(t, f, "job", now.Add(time.Hour))
	if err := f.PreserveDiscoverySnapshot(root.Key, leased.LeaseGeneration, SourceValidators{ETag: `"v2"`}, now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	leased = leaseOneSource(t, f, "job", now.Add(2*time.Hour))
	if leased.ETag != `"v2"` || leased.LastModified != "" {
		t.Fatalf("replacement validators = %#v", leased)
	}
	if err := f.PreserveDiscoverySnapshot(root.Key, leased.LeaseGeneration, SourceValidators{}, now.Add(3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	leased = leaseOneSource(t, f, "job", now.Add(3*time.Hour))
	if leased.ETag != "" || leased.LastModified != "" {
		t.Fatalf("cleared validators = %#v", leased)
	}
}

func TestDiscoveryEmptySnapshotClearsMembershipAnd304KeepsItEmpty(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 4)
	defer f.Close()
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	root := DiscoverySourceSeed{Key: "root", JobID: "job", URL: "https://example.com/root.xml"}
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now); err != nil {
		t.Fatal(err)
	}
	leased := leaseOneSource(t, f, "job", now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: root.Key, LeaseGeneration: leased.LeaseGeneration, Pages: []DiscoveredPage{{Key: "page", URL: "https://example.com/page"}}, NextEligible: now.Add(time.Hour)}, now); err != nil {
		t.Fatal(err)
	}
	leased = leaseOneSource(t, f, "job", now.Add(time.Hour))
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: root.Key, LeaseGeneration: leased.LeaseGeneration, NextEligible: now.Add(2 * time.Hour)}, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	leased = leaseOneSource(t, f, "job", now.Add(2*time.Hour))
	if err := f.PreserveDiscoverySnapshot(root.Key, leased.LeaseGeneration, SourceValidators{}, now.Add(3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if pages, err := f.LeaseDueJob("job", now.Add(4*time.Hour), 4, time.Minute); err != nil || len(pages) != 0 {
		t.Fatalf("pages after empty snapshot and 304 = %#v, %v", pages, err)
	}
}

func TestDiscoveryCompletionRejectsClosedFrontierAndNonRetryOutcome(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 4)
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	if err := f.CompleteDiscoverySource("source", 1, StateDisabled, time.Time{}, "disable"); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("disabled completion error = %v, want ErrInvalidTransition", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.CompleteDiscoverySource("source", 1, StateRetry, now.Add(time.Hour), "retry"); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed completion error = %v, want ErrClosed", err)
	}
	validSnapshot := DiscoverySnapshot{SourceKey: "source", LeaseGeneration: 1, NextEligible: now.Add(time.Hour)}
	if err := f.ApplyDiscoverySnapshot(validSnapshot, now); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed snapshot error = %v, want ErrClosed", err)
	}
	var nilFrontier *Frontier
	if err := nilFrontier.ApplyDiscoverySnapshot(validSnapshot, now); !errors.Is(err, ErrClosed) {
		t.Fatalf("nil snapshot error = %v, want ErrClosed", err)
	}
}

func TestDiscoverySnapshotCapFailureIsAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	f, err := Open(path, Options{MaxEntries: 1, MaxSources: 2, MaxPagesPerSource: 2, MaxChildrenPerSource: 2, MaxMemberships: 2, LockTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{{Key: "root", JobID: "job", URL: "https://example.com/root.xml"}}, now); err != nil {
		t.Fatal(err)
	}
	leased := leaseOneSource(t, f, "job", now)
	err = f.ApplyDiscoverySnapshot(DiscoverySnapshot{
		SourceKey: "root", LeaseGeneration: leased.LeaseGeneration,
		Pages:      []DiscoveredPage{{Key: "a", URL: "https://example.com/a"}, {Key: "b", URL: "https://example.com/b"}},
		Validators: SourceValidators{ETag: `"must-not-commit"`}, NextEligible: now.Add(time.Hour),
	}, now)
	if !errors.Is(err, ErrMaxEntries) {
		t.Fatalf("oversized snapshot error = %v, want ErrMaxEntries", err)
	}
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{
		SourceKey: "root", LeaseGeneration: leased.LeaseGeneration,
		Pages:      []DiscoveredPage{{Key: "a", URL: "https://example.com/a"}},
		Validators: SourceValidators{ETag: `"committed"`}, NextEligible: now.Add(time.Hour),
	}, now); err != nil {
		t.Fatalf("second snapshot with same lease: %v", err)
	}
}

func TestDiscoverySourceEdgeCapFailureIsAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	f, err := Open(path, Options{MaxEntries: 4, MaxSources: 4, MaxPagesPerSource: 4, MaxChildrenPerSource: 4, MaxMemberships: 4, MaxSourceEdges: 1, LockTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{{Key: "root", JobID: "job", URL: "https://example.com/root.xml"}}, now); err != nil {
		t.Fatal(err)
	}
	leased := leaseOneSource(t, f, "job", now)
	children := []DiscoverySourceSeed{
		{Key: "a", JobID: "job", URL: "https://example.com/a.xml"},
		{Key: "b", JobID: "job", URL: "https://example.com/b.xml"},
	}
	err = f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: "root", LeaseGeneration: leased.LeaseGeneration, Children: children, NextEligible: now.Add(time.Hour)}, now)
	if !errors.Is(err, ErrMaxEntries) {
		t.Fatalf("edge cap error = %v, want ErrMaxEntries", err)
	}
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: "root", LeaseGeneration: leased.LeaseGeneration, Children: children[:1], NextEligible: now.Add(time.Hour)}, now); err != nil {
		t.Fatalf("lease changed after edge-cap rollback: %v", err)
	}
}

func TestDiscoverySourceIdentityCapFailureIsAtomicAndDoesNotEvict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	f, err := Open(path, Options{MaxEntries: 4, MaxSources: 2, MaxPagesPerSource: 4, MaxChildrenPerSource: 4, MaxMemberships: 4, MaxSourceEdges: 4, LockTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	root := DiscoverySourceSeed{Key: "root", JobID: "job", URL: "https://example.com/root.xml"}
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{root}, now); err != nil {
		t.Fatal(err)
	}
	leased := leaseOneSource(t, f, "job", now)
	children := []DiscoverySourceSeed{
		{Key: "a", JobID: "job", URL: "https://example.com/a.xml"},
		{Key: "b", JobID: "job", URL: "https://example.com/b.xml"},
	}
	err = f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: root.Key, LeaseGeneration: leased.LeaseGeneration, Children: children, NextEligible: now.Add(time.Hour)}, now)
	if !errors.Is(err, ErrMaxEntries) {
		t.Fatalf("source identity cap error = %v, want ErrMaxEntries", err)
	}
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: root.Key, LeaseGeneration: leased.LeaseGeneration, Children: children[:1], NextEligible: now.Add(time.Hour)}, now); err != nil {
		t.Fatalf("lease changed after source-cap rollback: %v", err)
	}
	if err := f.SyncDiscoverySources(nil, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	newRoot := DiscoverySourceSeed{Key: "new", JobID: "job", URL: "https://example.com/new.xml"}
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{newRoot}, now.Add(2*time.Minute)); !errors.Is(err, ErrMaxEntries) {
		t.Fatalf("source identity eviction error = %v, want ErrMaxEntries", err)
	}
}

func TestDiscoveryGlobalPageMembershipCapIsAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	f, err := Open(path, Options{MaxEntries: 4, MaxSources: 2, MaxPagesPerSource: 4, MaxChildrenPerSource: 4, MaxMemberships: 1, MaxSourceEdges: 4, LockTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	sources := []DiscoverySourceSeed{
		{Key: "one", JobID: "job", URL: "https://one.example/sitemap.xml"},
		{Key: "two", JobID: "job", URL: "https://two.example/sitemap.xml"},
	}
	if err := f.SyncDiscoverySources(sources, now); err != nil {
		t.Fatal(err)
	}
	one := leaseSourceByKey(t, f, "job", "one", now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: one.Key, LeaseGeneration: one.LeaseGeneration, Pages: []DiscoveredPage{{Key: "a", URL: "https://pages.example/a"}}, NextEligible: now.Add(time.Hour)}, now); err != nil {
		t.Fatal(err)
	}
	two := leaseSourceByKey(t, f, "job", "two", now)
	err = f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: two.Key, LeaseGeneration: two.LeaseGeneration, Pages: []DiscoveredPage{{Key: "b", URL: "https://pages.example/b"}}, NextEligible: now.Add(time.Hour)}, now)
	if !errors.Is(err, ErrMaxEntries) {
		t.Fatalf("page membership cap error = %v, want ErrMaxEntries", err)
	}
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: two.Key, LeaseGeneration: two.LeaseGeneration, NextEligible: now.Add(time.Hour)}, now); err != nil {
		t.Fatalf("lease changed after membership-cap rollback: %v", err)
	}
}

func TestDiscoverySnapshotRejectsStaleWorkerAndConcurrentDuplicate(t *testing.T) {
	f := openTestFrontier(t, filepath.Join(t.TempDir(), "frontier.db"), 16)
	defer f.Close()
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{{Key: "root", JobID: "job", URL: "https://example.com/root.xml"}}, now); err != nil {
		t.Fatal(err)
	}
	first := leaseOneSource(t, f, "job", now)
	second := leaseOneSource(t, f, "job", now.Add(2*time.Minute))
	stale := DiscoverySnapshot{SourceKey: "root", LeaseGeneration: first.LeaseGeneration, NextEligible: now.Add(time.Hour)}
	if err := f.ApplyDiscoverySnapshot(stale, now); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("stale snapshot error = %v, want ErrLeaseMismatch", err)
	}
	snapshot := DiscoverySnapshot{SourceKey: "root", LeaseGeneration: second.LeaseGeneration, NextEligible: now.Add(time.Hour)}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- f.ApplyDiscoverySnapshot(snapshot, now)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	succeeded := 0
	rejected := 0
	for err := range errs {
		if err == nil {
			succeeded++
		} else if errors.Is(err, ErrInvalidTransition) {
			rejected++
		} else {
			t.Fatalf("duplicate snapshot error = %v", err)
		}
	}
	if succeeded != 1 || rejected != 1 {
		t.Fatalf("snapshot outcomes succeeded=%d rejected=%d", succeeded, rejected)
	}
}

func TestOpenRejectsCorruptDiscoverySourceIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	f := openTestFrontier(t, path, 16)
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{{Key: "root", JobID: "job", URL: "https://example.com/root.xml"}}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	mutateDB(t, path, func(tx *bolt.Tx) error {
		bucket := tx.Bucket(sourceDueByJobBucket)
		key, _ := bucket.Cursor().First()
		return bucket.Delete(key)
	})
	_, err := Open(path, Options{MaxEntries: 16, LockTimeout: 10 * time.Millisecond})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open error = %v, want ErrCorrupt", err)
	}
}

func TestOpenRejectsCorruptDiscoveryMembership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	f := openTestFrontier(t, path, 4)
	if err := f.SyncDiscoverySources([]DiscoverySourceSeed{{Key: "root", JobID: "job", URL: "https://example.com/root.xml"}}, now); err != nil {
		t.Fatal(err)
	}
	leased := leaseOneSource(t, f, "job", now)
	if err := f.ApplyDiscoverySnapshot(DiscoverySnapshot{SourceKey: "root", LeaseGeneration: leased.LeaseGeneration, Pages: []DiscoveredPage{{Key: "page", URL: "https://example.com/page"}}, NextEligible: now.Add(time.Hour)}, now); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	mutateDB(t, path, func(tx *bolt.Tx) error {
		return tx.Bucket(pageSourcesBucket).Delete(membershipKey("page", "root"))
	})
	_, err := Open(path, Options{MaxEntries: 4, LockTimeout: 10 * time.Millisecond})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open error = %v, want ErrCorrupt", err)
	}
}

func leaseOneSource(t *testing.T, f *Frontier, jobID string, now time.Time) DiscoverySourceEntry {
	t.Helper()
	leased, err := f.LeaseDueDiscoverySourcesJob(jobID, now, 1, time.Minute)
	if err != nil || len(leased) != 1 {
		t.Fatalf("LeaseDueDiscoverySourcesJob = %#v, %v", leased, err)
	}
	return leased[0]
}

func leaseSourceByKey(t *testing.T, f *Frontier, jobID, key string, now time.Time) DiscoverySourceEntry {
	t.Helper()
	for attempts := 0; attempts < 8; attempts++ {
		leased := leaseOneSource(t, f, jobID, now)
		if leased.Key == key {
			return leased
		}
		if err := f.CompleteDiscoverySource(leased.Key, leased.LeaseGeneration, StateRetry, now.Add(time.Hour), "defer"); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatalf("source %q was not leased", key)
	return DiscoverySourceEntry{}
}

func readDiscoverySource(t *testing.T, f *Frontier, key string) DiscoverySourceEntry {
	t.Helper()
	db, release, err := f.acquire()
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var source DiscoverySourceEntry
	if err := db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(sourcesBucket).Get([]byte(key))
		if raw == nil {
			return ErrNotFound
		}
		var err error
		source, err = decodeDiscoverySource([]byte(key), raw)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return source
}
