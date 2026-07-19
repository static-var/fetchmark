package focusedcrawl

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

type memoryFrontier struct {
	entries       map[string]FrontierEntry
	hosts         map[string]memoryHostState
	linkPolicies  map[string]LinkPolicy
	linkSnapshots map[string][]Seed
	mutations     int
}

func (f *memoryFrontier) SyncLinkPolicies(policies []LinkPolicy, _ time.Time) error {
	f.mutations++
	f.linkPolicies = make(map[string]LinkPolicy, len(policies))
	for _, policy := range policies {
		f.linkPolicies[policy.JobID] = policy
	}
	for key, entry := range f.entries {
		_, entry.CanExpand = f.linkPolicies[entry.JobID]
		f.entries[key] = entry
	}
	return nil
}

type memoryHostState struct {
	last     time.Time
	interval time.Duration
}

func (f *memoryFrontier) SyncSeeds(seeds []Seed, now time.Time) error {
	f.mutations++
	if f.entries == nil {
		f.entries = make(map[string]FrontierEntry)
	}
	wanted := make(map[string]struct{}, len(seeds))
	for _, seed := range seeds {
		wanted[seed.Key] = struct{}{}
		entry, exists := f.entries[seed.Key]
		if !exists {
			entry = FrontierEntry{Key: seed.Key, JobID: seed.JobID, URL: seed.URL, State: StateQueued, NextEligible: now}
		}
		entry.JobID, entry.URL = seed.JobID, seed.URL
		if entry.State == StateDisabled {
			entry.State = StateQueued
			entry.NextEligible = now
		}
		f.entries[seed.Key] = entry
	}
	for key, entry := range f.entries {
		if _, exists := wanted[key]; !exists {
			entry.State = StateDisabled
			f.entries[key] = entry
		}
	}
	return nil
}

func (f *memoryFrontier) LeaseDueJob(jobID string, now time.Time, limit int, leaseTTL time.Duration) ([]FrontierEntry, error) {
	f.mutations++
	var candidates []FrontierEntry
	for key, entry := range f.entries {
		if entry.JobID != jobID || entry.NextEligible.After(now) || entry.State == StateDisabled {
			continue
		}
		if entry.State == StateLeased && entry.LeaseUntil.After(now) {
			continue
		}
		entry.Key = key
		candidates = append(candidates, entry)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].URL < candidates[j].URL })
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	out := make([]FrontierEntry, 0, len(candidates))
	for _, entry := range candidates {
		entry.State = StateLeased
		entry.Attempts++
		entry.LeaseUntil = now.Add(leaseTTL)
		entry.LeaseGeneration++
		f.entries[entry.Key] = entry
		out = append(out, entry)
	}
	return out, nil
}

func (f *memoryFrontier) Complete(key string, leaseGeneration uint64, state EntryState, next time.Time, reason string) error {
	f.mutations++
	entry := f.entries[key]
	if entry.LeaseGeneration != leaseGeneration {
		return errors.New("stale lease")
	}
	entry.State = state
	entry.NextEligible = next
	entry.LeaseUntil = time.Time{}
	entry.LastReason = reason
	if state == StateSucceeded || state == StateRejected {
		entry.Attempts = 0
	}
	f.entries[key] = entry
	return nil
}

func (f *memoryFrontier) CompleteWithLinks(completion LinkCompletion, now time.Time) (LinkMutationCounts, error) {
	parent, ok := f.entries[completion.Key]
	if !ok || parent.LeaseGeneration != completion.LeaseGeneration || !parent.CanExpand {
		return LinkMutationCounts{}, errors.New("invalid link completion")
	}
	counts := LinkMutationCounts{Reported: len(completion.Links), AppliedEdges: len(completion.Links)}
	if f.linkSnapshots == nil {
		f.linkSnapshots = make(map[string][]Seed)
	}
	previous := f.linkSnapshots[completion.Key]
	old := make(map[string]struct{}, len(previous))
	for _, seed := range previous {
		old[seed.Key] = struct{}{}
	}
	for _, seed := range completion.Links {
		if _, exists := old[seed.Key]; !exists {
			counts.AddedEdges++
		}
		if existing, exists := f.entries[seed.Key]; !exists {
			f.entries[seed.Key] = FrontierEntry{
				Key: seed.Key, JobID: seed.JobID, URL: seed.URL, State: StateQueued, NextEligible: now,
			}
			counts.Created++
		} else if existing.State == StateDisabled {
			existing.State = StateQueued
			existing.NextEligible = now
			f.entries[seed.Key] = existing
			counts.Reactivated++
		}
	}
	counts.RemovedEdges = len(previous) - (counts.AppliedEdges - counts.AddedEdges)
	f.linkSnapshots[completion.Key] = append([]Seed(nil), completion.Links...)
	if err := f.Complete(completion.Key, completion.LeaseGeneration, completion.Outcome, completion.NextEligible, completion.Reason); err != nil {
		return LinkMutationCounts{}, err
	}
	return counts, nil
}

func (f *memoryFrontier) ReserveHost(authority string, now time.Time, interval time.Duration) (time.Duration, error) {
	f.mutations++
	if f.hosts == nil {
		f.hosts = make(map[string]memoryHostState)
	}
	state, exists := f.hosts[authority]
	if exists {
		required := max(interval, state.interval)
		if next := state.last.Add(required); now.Before(next) {
			return next.Sub(now), nil
		}
	}
	f.hosts[authority] = memoryHostState{last: now, interval: interval}
	return 0, nil
}

func (f *memoryFrontier) Counts() (map[EntryState]int, error) {
	counts := make(map[EntryState]int)
	for _, entry := range f.entries {
		counts[entry.State]++
	}
	return counts, nil
}

type recordingAdmission struct {
	results  map[string]AdmissionResult
	errors   map[string]error
	urls     []string
	requests []AdmissionRequest
}

func (a *recordingAdmission) Admit(_ context.Context, request AdmissionRequest) (AdmissionResult, error) {
	a.urls = append(a.urls, request.URL)
	a.requests = append(a.requests, request)
	return a.results[request.URL], a.errors[request.URL]
}

type retryError struct {
	fatal bool
	after time.Duration
}

func (e retryError) Error() string             { return "retry" }
func (e retryError) Fatal() bool               { return e.fatal }
func (e retryError) Temporary() bool           { return !e.fatal }
func (e retryError) RetryAfter() time.Duration { return e.after }

func TestRunnerTransitionsAdmissionsAndHonorsHostPacing(t *testing.T) {
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	config := mustTestConfig(t, []string{
		"https://example.org/docs/a",
		"https://example.org/docs/b",
		"https://other.org/docs/c",
	}, []string{"example.org", "other.org"}, 3)
	frontier := &memoryFrontier{}
	admission := &recordingAdmission{results: map[string]AdmissionResult{
		"https://example.org/docs/a": {URL: "https://example.org/docs/a", Status: "admitted"},
		"https://example.org/docs/b": {URL: "https://example.org/docs/b", Status: "rejected", Reason: "robots_blocked"},
		"https://other.org/docs/c":   {URL: "https://other.org/docs/c", Status: "failed", Reason: "fetch_failed"},
	}, errors: map[string]error{}}
	clock := &fakeClock{now: now}
	summary, err := (Runner{Config: config, Frontier: frontier, Admissions: admission, Clock: clock, LeaseTTL: time.Minute}).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if summary.Attempted != 3 || summary.Admitted != 1 || summary.Rejected != 1 || summary.Retried != 1 {
		t.Fatalf("summary=%+v", summary)
	}
	if got := clock.sleeps; !reflect.DeepEqual(got, []time.Duration{time.Second}) {
		t.Fatalf("sleeps=%v", got)
	}
	for _, entry := range frontier.entries {
		switch entry.URL {
		case "https://example.org/docs/a":
			if entry.State != StateSucceeded || entry.NextEligible.Before(now.Add(time.Hour)) || entry.NextEligible.After(now.Add(time.Hour+time.Second)) {
				t.Fatalf("admitted entry=%+v", entry)
			}
		case "https://example.org/docs/b":
			if entry.State != StateRejected || entry.NextEligible.Before(now.Add(24*time.Hour)) || entry.NextEligible.After(now.Add(24*time.Hour+time.Second)) {
				t.Fatalf("rejected entry=%+v", entry)
			}
		case "https://other.org/docs/c":
			if entry.State != StateRetry || !entry.NextEligible.After(now) {
				t.Fatalf("retry entry=%+v", entry)
			}
		}
	}
}

func TestRunnerFatalAdmissionStopsWithoutHotLooping(t *testing.T) {
	config := mustTestConfig(t, []string{"https://example.org/docs/a", "https://example.org/docs/b"}, []string{"example.org"}, 2)
	frontier := &memoryFrontier{}
	admission := &recordingAdmission{results: map[string]AdmissionResult{}, errors: map[string]error{
		"https://example.org/docs/a": retryError{fatal: true},
	}}
	_, err := (Runner{Config: config, Frontier: frontier, Admissions: admission, Clock: &fakeClock{now: time.Now().UTC()}, LeaseTTL: time.Minute}).Run(context.Background())
	if err == nil || len(admission.urls) != 1 {
		t.Fatalf("error=%v calls=%v", err, admission.urls)
	}
	entry := frontier.entries[config.Seeds()[0].Key]
	if entry.State != StateRetry || entry.NextEligible.Before(time.Now().Add(4*time.Minute)) {
		t.Fatalf("fatal entry=%+v", entry)
	}
	if untouched := frontier.entries[config.Seeds()[1].Key]; untouched.State != StateQueued || !untouched.LeaseUntil.IsZero() {
		t.Fatalf("unattempted entry was leased: %+v", untouched)
	}
}

func TestRunnerPersistsHostPacingAcrossRuns(t *testing.T) {
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	config := mustTestConfig(t, []string{"https://example.org/docs/a", "https://example.org/docs/b"}, []string{"example.org"}, 1)
	frontier := &memoryFrontier{}
	admission := &recordingAdmission{results: map[string]AdmissionResult{
		"https://example.org/docs/a": {URL: "https://example.org/docs/a", Status: "admitted"},
		"https://example.org/docs/b": {URL: "https://example.org/docs/b", Status: "admitted"},
	}, errors: map[string]error{}}
	clock := &fakeClock{now: now}
	runner := Runner{Config: config, Frontier: frontier, Admissions: admission, Clock: clock, LeaseTTL: time.Minute}
	if _, err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(clock.sleeps) != 0 {
		t.Fatalf("first run sleeps = %v", clock.sleeps)
	}
	if _, err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(clock.sleeps, []time.Duration{time.Second}) {
		t.Fatalf("cross-run sleeps = %v", clock.sleeps)
	}
}

func TestRunnerUsesMaximumHostIntervalAcrossJobs(t *testing.T) {
	config, err := DecodeConfig(strings.NewReader(`{
  "version":3,"contact_uri":"mailto:operator@example.org","max_frontier_urls":10,"max_discovery_sources":0,"jobs":[
    {"id":"slow","seeds":["https://example.org/slow/a"],"allowed_domains":["example.org"],"allowed_path_prefixes":["/slow/"],"max_urls_per_run":1,"refresh_interval":"1h","rejection_recheck_interval":"24h","min_host_interval":"10s","max_attempts":2},
    {"id":"fast","seeds":["https://example.org/fast/b"],"allowed_domains":["example.org"],"allowed_path_prefixes":["/fast/"],"max_urls_per_run":1,"refresh_interval":"1h","rejection_recheck_interval":"24h","min_host_interval":"1s","max_attempts":2}
  ]}`))
	if err != nil {
		t.Fatal(err)
	}
	frontier := &memoryFrontier{}
	admission := &recordingAdmission{results: map[string]AdmissionResult{
		"https://example.org/slow/a": {URL: "https://example.org/slow/a", Status: "admitted"},
		"https://example.org/fast/b": {URL: "https://example.org/fast/b", Status: "admitted"},
	}, errors: map[string]error{}}
	clock := &fakeClock{now: time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)}
	if _, err := (Runner{Config: config, Frontier: frontier, Admissions: admission, Clock: clock}).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(clock.sleeps, []time.Duration{10 * time.Second}) {
		t.Fatalf("sleeps = %v", clock.sleeps)
	}
	if len(admission.requests) != 2 || !reflect.DeepEqual(admission.requests[0].AllowedPathPrefixes, []string{"/slow/"}) ||
		!reflect.DeepEqual(admission.requests[1].AllowedPathPrefixes, []string{"/fast/"}) {
		t.Fatalf("requests = %+v", admission.requests)
	}
}

func TestRunnerAppliesOneHopLinkSnapshotAndDoesNotExpandLinkOnlyChild(t *testing.T) {
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	config := mustTestConfig(t, []string{"https://example.org/docs/a"}, []string{"example.org"}, 2)
	config.MaxLinkEdges = 100
	config.Jobs[0].LinkExpansion = &LinkExpansion{MaxLinksPerPage: 4, MaxPagesPerRun: 1}
	if err := config.validate(); err != nil {
		t.Fatal(err)
	}
	links := []string{
		"https://example.org/docs/b",
		"https://example.org/outside",
		"https://other.example/docs/c",
		"https://example.org/docs/a",
	}
	if eligible, _, _ := eligibleLinkSeeds(config.Jobs[0], FrontierEntry{URL: "https://example.org/docs/a"}, links); len(eligible) != 1 {
		t.Fatalf("eligible links=%+v", eligible)
	}
	frontier := &memoryFrontier{}
	admission := &recordingAdmission{results: map[string]AdmissionResult{
		"https://example.org/docs/a": {URL: "https://example.org/docs/a", Status: "admitted", OutboundLinks: &links},
		"https://example.org/docs/b": {URL: "https://example.org/docs/b", Status: "admitted"},
	}, errors: map[string]error{}}
	summary, err := (Runner{Config: config, Frontier: frontier, Admissions: admission, Clock: &fakeClock{now: now}}).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(admission.requests) != 2 || admission.requests[0].MaxOutboundLinks != 4 || admission.requests[1].MaxOutboundLinks != 0 {
		t.Fatalf("summary=%+v requests=%+v", summary, admission.requests)
	}
	if summary.LinkSnapshotsApplied != 1 || summary.LinksReported != 4 || summary.LinksEligible != 1 ||
		summary.LinkCandidatesDroppedScope != 3 || summary.LinkEdgesApplied != 1 || summary.LinkURLsCreated != 1 {
		t.Fatalf("summary=%+v", summary)
	}
	childKey := configKey("docs", "https://example.org/docs/b")
	if child := frontier.entries[childKey]; child.CanExpand || child.State != StateSucceeded {
		t.Fatalf("link-only child=%+v", child)
	}
}

func TestRunnerPreservesSnapshotWhenLinkPageBudgetIsExhausted(t *testing.T) {
	config := mustTestConfig(t, []string{"https://example.org/docs/a", "https://example.org/docs/b"}, []string{"example.org"}, 2)
	config.MaxLinkEdges = 100
	config.Jobs[0].LinkExpansion = &LinkExpansion{MaxLinksPerPage: 4, MaxPagesPerRun: 1}
	if err := config.validate(); err != nil {
		t.Fatal(err)
	}
	empty := []string{}
	admission := &recordingAdmission{results: map[string]AdmissionResult{
		"https://example.org/docs/a": {URL: "https://example.org/docs/a", Status: "admitted", OutboundLinks: &empty},
		"https://example.org/docs/b": {URL: "https://example.org/docs/b", Status: "admitted"},
	}, errors: map[string]error{}}
	summary, err := (Runner{
		Config: config, Frontier: &memoryFrontier{}, Admissions: admission,
		Clock: &fakeClock{now: time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)},
	}).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if summary.LinkSnapshotsApplied != 1 || summary.LinkSnapshotsSkippedBudget != 1 || len(admission.requests) != 2 ||
		admission.requests[0].MaxOutboundLinks != 4 || admission.requests[1].MaxOutboundLinks != 0 {
		t.Fatalf("summary=%+v requests=%+v", summary, admission.requests)
	}
}

func TestRunnerAuthoritativeRejectionClearsPriorLinkSnapshot(t *testing.T) {
	config := mustTestConfig(t, []string{"https://example.org/docs/a"}, []string{"example.org"}, 1)
	config.MaxLinkEdges = 100
	config.Jobs[0].LinkExpansion = &LinkExpansion{MaxLinksPerPage: 4, MaxPagesPerRun: 1}
	if err := config.validate(); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: now}
	frontier := &memoryFrontier{}
	links := []string{"https://example.org/docs/b"}
	admission := &recordingAdmission{results: map[string]AdmissionResult{
		"https://example.org/docs/a": {URL: "https://example.org/docs/a", Status: "admitted", OutboundLinks: &links},
	}, errors: map[string]error{}}
	if _, err := (Runner{Config: config, Frontier: frontier, Admissions: admission, Clock: clock}).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	parentKey := config.Seeds()[0].Key
	if len(frontier.linkSnapshots[parentKey]) != 1 {
		t.Fatalf("initial snapshot=%+v", frontier.linkSnapshots[parentKey])
	}
	clock.now = now.Add(2 * time.Hour)
	admission.results["https://example.org/docs/a"] = AdmissionResult{
		URL: "https://example.org/docs/a", Status: "rejected", Reason: "noindex",
	}
	summary, err := (Runner{Config: config, Frontier: frontier, Admissions: admission, Clock: clock}).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if summary.LinkSnapshotsApplied != 1 || len(frontier.linkSnapshots[parentKey]) != 0 {
		t.Fatalf("summary=%+v snapshot=%+v", summary, frontier.linkSnapshots[parentKey])
	}
}

func TestRunnerCancellationReleasesCurrentLease(t *testing.T) {
	config := mustTestConfig(t, []string{"https://example.org/docs/a"}, []string{"example.org"}, 1)
	frontier := &memoryFrontier{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (Runner{
		Config: config, Frontier: frontier, Admissions: &recordingAdmission{},
		Clock: &fakeClock{now: time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)}, LeaseTTL: time.Minute,
	}).Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	entry := frontier.entries[config.Seeds()[0].Key]
	if entry.State != StateRetry || !entry.LeaseUntil.IsZero() || entry.LastReason != "interrupted" {
		t.Fatalf("entry = %+v", entry)
	}
}

func TestDryRunDoesNotTouchFrontierOrAdmissions(t *testing.T) {
	config := mustTestConfig(t, []string{"https://example.org/docs/a"}, []string{"example.org"}, 1)
	frontier := &memoryFrontier{}
	admission := &recordingAdmission{}
	summary := DryRun(config)
	if summary.Planned != 1 || frontier.mutations != 0 || len(admission.urls) != 0 {
		t.Fatalf("summary=%+v mutations=%d calls=%v", summary, frontier.mutations, admission.urls)
	}
}

type fakeClock struct {
	now    time.Time
	sleeps []time.Duration
}

func (c *fakeClock) Now() time.Time { return c.now }
func (c *fakeClock) Sleep(ctx context.Context, duration time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	c.sleeps = append(c.sleeps, duration)
	c.now = c.now.Add(duration)
	return nil
}

func mustTestConfig(t *testing.T, seeds, domains []string, maxRun int) Config {
	t.Helper()
	job := Job{
		ID: "docs", SeedURLs: seeds, AllowedDomains: domains, AllowedPathPrefixes: []string{"/docs/"},
		MaxURLsPerRun: maxRun, RefreshInterval: Duration{time.Hour},
		RejectionRecheckInterval: Duration{24 * time.Hour}, MinHostInterval: Duration{time.Second}, MaxAttempts: 4,
	}
	config := Config{Version: 3, ContactURI: "mailto:operator@example.org", MaxFrontierURLs: 100, Jobs: []Job{job}}
	if err := config.validate(); err != nil {
		t.Fatal(err)
	}
	return config
}

var _ = errors.New
