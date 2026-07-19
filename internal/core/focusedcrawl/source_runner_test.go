package focusedcrawl

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestSourceRunnerAppliesScopedSnapshotWithInheritedBudgets(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	config := mustSourceConfig(t, "https://example.org/feeds/root.xml")
	frontier := &memorySourceFrontier{}
	fetcher := &recordingSourceFetcher{results: map[string]SourceFetchResult{
		"https://example.org/feeds/root.xml": {
			Status: 200, Body: []byte("xml"), FinalURL: "https://example.org/feeds/current.xml",
			ETag: `"v2"`, LastModified: "Fri, 18 Jul 2026 11:00:00 GMT", FreshUntil: now.Add(8 * time.Hour),
		},
	}}
	parser := func(raw []byte, sourceURL string, requested SourceKind, limits DiscoveryLimits) (ParsedDiscoveryDocument, error) {
		if string(raw) != "xml" || sourceURL != "https://example.org/feeds/current.xml" || requested != SourceKindSitemap {
			t.Fatalf("parse request raw=%q source=%q kind=%q", raw, sourceURL, requested)
		}
		if limits.MaxDocumentBytes != 8192 || limits.MaxEntries != 10 || limits.MaxURLBytes != 2048 {
			t.Fatalf("limits=%+v", limits)
		}
		return ParsedDiscoveryDocument{
			Kind: SourceKindSitemap,
			Pages: []ParsedDiscoveryPage{
				{URL: "https://example.org/docs/one"},
				{URL: "https://example.org/private/two"},
				{URL: "https://other.example/docs/three"},
			},
			Sources: []ParsedDiscoverySource{
				{URL: "https://example.org/feeds/child.xml", Kind: SourceKindSitemap},
				{URL: "https://example.org/feeds/root.xml", Kind: SourceKindSitemap},
				{URL: "https://example.org/outside/child.xml", Kind: SourceKindSitemap},
			},
		}, nil
	}

	summary, err := (SourceRunner{
		Config: config, Frontier: frontier, Fetcher: fetcher, Parse: parser,
		Clock: &fakeClock{now: now}, LeaseTTL: time.Minute,
	}).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if summary.Planned != 1 || summary.Attempted != 1 || summary.SnapshotsApplied != 1 ||
		summary.PagesDiscovered != 3 || summary.PagesEligible != 1 || summary.ChildrenAdded != 1 || summary.Failed != 0 {
		t.Fatalf("summary=%+v", summary)
	}
	if len(fetcher.requests) != 1 {
		t.Fatalf("fetch requests=%+v", fetcher.requests)
	}
	request := fetcher.requests[0]
	if request.MaxWireBytes != 4096 || request.MaxDecompressedBytes != 8192 || !reflect.DeepEqual(request.AllowedPathPrefixes, []string{"/feeds/"}) {
		t.Fatalf("fetch request=%+v", request)
	}
	if len(frontier.applied) != 1 {
		t.Fatalf("applied=%+v", frontier.applied)
	}
	snapshot := frontier.applied[0]
	if len(snapshot.Pages) != 1 || snapshot.Pages[0].URL != "https://example.org/docs/one" {
		t.Fatalf("pages=%+v", snapshot.Pages)
	}
	if len(snapshot.Children) != 1 || snapshot.Children[0].URL != "https://example.org/feeds/child.xml" ||
		snapshot.Children[0].RootKey != config.SourceSeeds()[0].Key || snapshot.Children[0].ParentKey != config.SourceSeeds()[0].Key {
		t.Fatalf("children=%+v", snapshot.Children)
	}
	if snapshot.NextEligible != now.Add(8*time.Hour) || snapshot.Validators.ETag != `"v2"` {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}

func TestBuildSourceSnapshotRestrictsSitemapsButAllowsFeedJobScope(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	config := mustSourceConfig(t, "https://example.org/feeds/root.xml")
	config.Jobs[0].AllowedDomains = append(config.Jobs[0].AllowedDomains, "other.example")
	if err := config.validate(); err != nil {
		t.Fatal(err)
	}
	job := config.Jobs[0]
	root, ok := config.RootSource(config.SourceSeeds()[0].Key)
	if !ok {
		t.Fatal("missing root")
	}
	entry := SourceFrontierEntry{Key: root.Seed.Key, JobID: job.ID, URL: root.Seed.URL, RootKey: root.Seed.Key, LeaseGeneration: 1}
	pages := []ParsedDiscoveryPage{
		{URL: "https://example.org/docs/local"},
		{URL: "https://other.example/docs/allowed-by-job"},
	}
	sitemap, err := buildSourceSnapshot(job, root, entry, ParsedDiscoveryDocument{Kind: SourceKindSitemap, Pages: pages}, SourceFetchResult{}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(sitemap.Pages) != 1 || sitemap.Pages[0].URL != "https://example.org/docs/local" {
		t.Fatalf("sitemap pages=%+v", sitemap.Pages)
	}
	feed, err := buildSourceSnapshot(job, root, entry, ParsedDiscoveryDocument{Kind: SourceKindFeed, Pages: pages}, SourceFetchResult{}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(feed.Pages) != 2 {
		t.Fatalf("feed pages=%+v", feed.Pages)
	}
}

func TestSourceRunnerNotModifiedMergesOrClearsValidators(t *testing.T) {
	for _, test := range []struct {
		name      string
		result    SourceFetchResult
		want      SourceValidators
		wantAfter time.Duration
	}{
		{
			name: "merge", result: SourceFetchResult{Status: 304, NotModified: true, ETag: `"new"`},
			want: SourceValidators{ETag: `"new"`, LastModified: "old-date"}, wantAfter: 6 * time.Hour,
		},
		{
			name: "no store clears", result: SourceFetchResult{Status: 304, NotModified: true, NoStore: true},
			want: SourceValidators{}, wantAfter: 6 * time.Hour,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
			config := mustSourceConfig(t, "https://example.org/feeds/root.xml")
			frontier := &memorySourceFrontier{initialValidators: SourceValidators{ETag: `"old"`, LastModified: "old-date"}}
			fetcher := &recordingSourceFetcher{results: map[string]SourceFetchResult{
				"https://example.org/feeds/root.xml": test.result,
			}}
			summary, err := (SourceRunner{
				Config: config, Frontier: frontier, Fetcher: fetcher,
				Parse: func([]byte, string, SourceKind, DiscoveryLimits) (ParsedDiscoveryDocument, error) {
					t.Fatal("304 must not be parsed")
					return ParsedDiscoveryDocument{Kind: SourceKindSitemap}, nil
				},
				Clock: &fakeClock{now: now},
			}).Run(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if summary.NotModified != 1 || len(frontier.preserved) != 1 {
				t.Fatalf("summary=%+v preserved=%+v", summary, frontier.preserved)
			}
			preserved := frontier.preserved[0]
			if preserved.validators != test.want || preserved.next != now.Add(test.wantAfter) {
				t.Fatalf("preserved=%+v", preserved)
			}
			if fetcher.requests[0].IfNoneMatch != `"old"` || fetcher.requests[0].IfModifiedSince != "old-date" {
				t.Fatalf("conditionals=%+v", fetcher.requests[0])
			}
		})
	}
}

func TestSourceRunnerFailurePreservesSnapshotAndContinues(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	config := mustSourceConfig(t,
		"https://example.org/feeds/a.xml",
		"https://example.org/feeds/b.xml",
		"https://example.org/feeds/c.xml",
	)
	frontier := &memorySourceFrontier{}
	fetcher := &recordingSourceFetcher{
		results: map[string]SourceFetchResult{
			"https://example.org/feeds/b.xml": {Status: 429, RetryAfter: 3 * time.Hour},
			"https://example.org/feeds/c.xml": {Status: 200, Body: []byte("ok")},
		},
		errors: map[string]error{"https://example.org/feeds/a.xml": errors.New("source transport failed")},
	}
	summary, err := (SourceRunner{
		Config: config, Frontier: frontier, Fetcher: fetcher,
		Parse: func([]byte, string, SourceKind, DiscoveryLimits) (ParsedDiscoveryDocument, error) {
			return ParsedDiscoveryDocument{Kind: SourceKindSitemap}, nil
		},
		Clock: &fakeClock{now: now},
	}).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if summary.Attempted != 3 || summary.Failed != 2 || summary.SnapshotsApplied != 1 || len(frontier.retried) != 2 || len(frontier.applied) != 1 {
		t.Fatalf("summary=%+v retries=%+v applied=%+v", summary, frontier.retried, frontier.applied)
	}
	if frontier.retried[0].next.Before(now.Add(time.Hour)) {
		t.Fatalf("retry=%+v", frontier.retried[0])
	}
	if frontier.retried[1].next != now.Add(3*time.Hour) || frontier.retried[1].reason != "http_429" {
		t.Fatalf("retry-after=%+v", frontier.retried[1])
	}
	if len(frontier.applied[0].Pages) != 0 || len(frontier.applied[0].Children) != 0 {
		t.Fatalf("authoritative empty snapshot=%+v", frontier.applied[0])
	}
}

func TestSourceRunnerRejectsOverDepthSnapshotWithoutReplacingMemberships(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	config := mustSourceConfig(t, "https://example.org/feeds/root.xml")
	frontier := &memorySourceFrontier{entryDepth: 2}
	fetcher := &recordingSourceFetcher{results: map[string]SourceFetchResult{
		"https://example.org/feeds/root.xml": {Status: 200, Body: []byte("xml")},
	}}
	summary, err := (SourceRunner{
		Config: config, Frontier: frontier, Fetcher: fetcher,
		Parse: func([]byte, string, SourceKind, DiscoveryLimits) (ParsedDiscoveryDocument, error) {
			return ParsedDiscoveryDocument{Kind: SourceKindSitemap, Sources: []ParsedDiscoverySource{{URL: "https://example.org/feeds/child.xml", Kind: SourceKindSitemap}}}, nil
		},
		Clock: &fakeClock{now: now},
	}).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if summary.Failed != 1 || len(frontier.retried) != 1 || len(frontier.applied) != 0 {
		t.Fatalf("summary=%+v retries=%+v applied=%+v", summary, frontier.retried, frontier.applied)
	}
}

func TestSourceRunnerPolicyNarrowingDoesNotSuppressOtherDueSources(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	config := mustSourceConfig(t, "https://example.org/feeds/root.xml")
	config.Jobs[0].MaxSourcePollsPerRun = 2
	root := config.SourceSeeds()[0]
	frontier := &memorySourceFrontier{entries: []SourceFrontierEntry{
		{Key: "retained-child", JobID: "docs", URL: "https://example.org/old-policy/child.xml", Kind: SourceKindSitemap, RootKey: root.Key, Depth: 1},
		{Key: root.Key, JobID: "docs", URL: root.URL, Kind: root.Kind, RootKey: root.Key},
	}}
	fetcher := &recordingSourceFetcher{results: map[string]SourceFetchResult{
		root.URL: {Status: 200, Body: []byte("xml")},
	}}
	summary, err := (SourceRunner{
		Config: config, Frontier: frontier, Fetcher: fetcher,
		Parse: func([]byte, string, SourceKind, DiscoveryLimits) (ParsedDiscoveryDocument, error) {
			return ParsedDiscoveryDocument{Kind: SourceKindSitemap}, nil
		},
		Clock: &fakeClock{now: now},
	}).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if summary.Failed != 1 || summary.SnapshotsApplied != 1 || len(frontier.retried) != 1 || frontier.retried[0].reason != "outside_source_scope" || len(fetcher.requests) != 1 {
		t.Fatalf("summary=%+v retries=%+v fetches=%+v", summary, frontier.retried, fetcher.requests)
	}
}

func TestSourceRunnerSynchronizesRemovedSourcesWithoutNetworkDependencies(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	config := mustTestConfig(t, []string{"https://example.org/docs/a"}, []string{"example.org"}, 1)
	frontier := &memorySourceFrontier{}
	summary, err := (SourceRunner{Config: config, Frontier: frontier, Clock: &fakeClock{now: now}}).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if summary.Planned != 0 || frontier.syncCalls != 1 || len(frontier.entries) != 0 {
		t.Fatalf("summary=%+v frontier=%+v", summary, frontier)
	}
}

func mustSourceConfig(t *testing.T, sourceURLs ...string) Config {
	t.Helper()
	sources := make([]DiscoverySource, 0, len(sourceURLs))
	for _, sourceURL := range sourceURLs {
		sources = append(sources, DiscoverySource{
			URL: sourceURL, Kind: SourceKindSitemap, AllowedSourcePathPrefixes: []string{"/feeds/"},
			PollInterval: Duration{6 * time.Hour}, ErrorRecheckInterval: Duration{time.Hour},
			MaxCompressedBytes: 4096, MaxDecompressedBytes: 8192, MaxEntries: 10,
			MaxChildSources: 2, MaxDepth: 2,
		})
	}
	job := Job{
		ID: "docs", AllowedDomains: []string{"example.org"}, AllowedPathPrefixes: []string{"/docs/"},
		MaxURLsPerRun: 1, MaxSourcePollsPerRun: len(sources), RefreshInterval: Duration{time.Hour},
		RejectionRecheckInterval: Duration{24 * time.Hour}, MinHostInterval: Duration{time.Second},
		MaxAttempts: 4, DiscoverySources: sources,
	}
	config := Config{
		Version: 3, ContactURI: "mailto:operator@example.org", MaxFrontierURLs: 100,
		MaxDiscoverySources: 100, MaxDiscoveryPageMemberships: 1000,
		MaxDiscoverySourceEdges: 1000, Jobs: []Job{job},
	}
	if err := config.validate(); err != nil {
		t.Fatal(err)
	}
	return config
}

type memorySourceFrontier struct {
	entries           []SourceFrontierEntry
	leased            map[string]bool
	completed         map[string]bool
	syncCalls         int
	entryDepth        int
	initialValidators SourceValidators
	applied           []SourceSnapshot
	preserved         []preservedSource
	retried           []retriedSource
}

type preservedSource struct {
	key        string
	validators SourceValidators
	next       time.Time
}

type retriedSource struct {
	key    string
	next   time.Time
	reason string
}

func (f *memorySourceFrontier) SyncDiscoverySources(seeds []SourceFrontierSeed, _ time.Time) error {
	f.syncCalls++
	if f.leased == nil {
		f.leased = make(map[string]bool)
	}
	if f.completed == nil {
		f.completed = make(map[string]bool)
	}
	if f.entries == nil {
		for _, seed := range seeds {
			f.entries = append(f.entries, SourceFrontierEntry{
				Key: seed.Key, JobID: seed.JobID, URL: seed.URL, Kind: seed.Kind, RootKey: seed.RootKey,
				Depth: f.entryDepth, ETag: f.initialValidators.ETag, LastModified: f.initialValidators.LastModified,
			})
		}
	}
	return nil
}

func (f *memorySourceFrontier) LeaseDueDiscoverySourcesJob(jobID string, _ time.Time, limit int, _ time.Duration) ([]SourceFrontierEntry, error) {
	for index := range f.entries {
		entry := &f.entries[index]
		if entry.JobID != jobID || f.leased[entry.Key] || f.completed[entry.Key] {
			continue
		}
		entry.Attempts++
		entry.LeaseGeneration = uint64(index + 1)
		f.leased[entry.Key] = true
		return []SourceFrontierEntry{*entry}, nil
	}
	return nil, nil
}

func (f *memorySourceFrontier) ReserveHost(string, time.Time, time.Duration) (time.Duration, error) {
	return 0, nil
}

func (f *memorySourceFrontier) ApplyDiscoverySnapshot(snapshot SourceSnapshot, _ time.Time) error {
	f.applied = append(f.applied, snapshot)
	f.completed[snapshot.SourceKey] = true
	return nil
}

func (f *memorySourceFrontier) PreserveDiscoverySnapshot(key string, _ uint64, validators SourceValidators, next time.Time) error {
	f.preserved = append(f.preserved, preservedSource{key: key, validators: validators, next: next})
	f.completed[key] = true
	return nil
}

func (f *memorySourceFrontier) CompleteDiscoverySource(key string, _ uint64, next time.Time, reason string) error {
	f.retried = append(f.retried, retriedSource{key: key, next: next, reason: reason})
	f.completed[key] = true
	return nil
}

type recordingSourceFetcher struct {
	results  map[string]SourceFetchResult
	errors   map[string]error
	requests []SourceFetchRequest
}

func (f *recordingSourceFetcher) Fetch(_ context.Context, request SourceFetchRequest) (SourceFetchResult, error) {
	f.requests = append(f.requests, request)
	return f.results[request.URL], f.errors[request.URL]
}
