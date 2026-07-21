package focusedcrawl

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// SourceFrontierSeed is one configured discovery root synchronized into the
// persistent crawler frontier.
type SourceFrontierSeed struct {
	Key     string
	JobID   string
	URL     string
	Kind    SourceKind
	RootKey string
}

// SourceFrontierEntry is one leased sitemap or feed source. Child identities
// remain scoped to their configured root so inherited policy is unambiguous.
type SourceFrontierEntry struct {
	Key             string
	JobID           string
	URL             string
	Kind            SourceKind
	RootKey         string
	Depth           int
	Attempts        int
	LeaseGeneration uint64
	ETag            string
	LastModified    string
}

type SourceValidators struct {
	ETag         string
	LastModified string
}

type DiscoveredPage struct {
	Key string
	URL string
}

type DiscoveredChildSource struct {
	Key       string
	JobID     string
	URL       string
	Kind      SourceKind
	RootKey   string
	ParentKey string
	Depth     int
}

type SourceSnapshot struct {
	SourceKey       string
	LeaseGeneration uint64
	Pages           []DiscoveredPage
	Children        []DiscoveredChildSource
	Validators      SourceValidators
	NextEligible    time.Time
}

// SourceFrontier owns discovery-source leases and their atomic page/source
// membership snapshots. Retry completion deliberately preserves the last
// successful snapshot and validators.
type SourceFrontier interface {
	SyncDiscoverySources(seeds []SourceFrontierSeed, now time.Time) error
	LeaseDueDiscoverySourcesJob(jobID string, now time.Time, limit int, leaseTTL time.Duration) ([]SourceFrontierEntry, error)
	ReserveHost(authority string, now time.Time, interval time.Duration) (time.Duration, error)
	ApplyDiscoverySnapshot(snapshot SourceSnapshot, observedAt time.Time) error
	PreserveDiscoverySnapshot(key string, leaseGeneration uint64, validators SourceValidators, nextEligible time.Time) error
	CompleteDiscoverySource(key string, leaseGeneration uint64, nextEligible time.Time, reason string) error
}

type SourceFetchRequest struct {
	URL                  string
	AllowedPathPrefixes  []string
	IfNoneMatch          string
	IfModifiedSince      string
	MaxWireBytes         int64
	MaxDecompressedBytes int64
}

type SourceFetchResult struct {
	Status       int
	Body         []byte
	FinalURL     string
	ETag         string
	LastModified string
	NotModified  bool
	FreshUntil   time.Time
	NoStore      bool
	RetryAfter   time.Duration
}

type SourceFetcher interface {
	Fetch(context.Context, SourceFetchRequest) (SourceFetchResult, error)
}

type DiscoveryLimits struct {
	MaxDocumentBytes int
	MaxEntries       int
	MaxURLBytes      int
	MaxXMLDepth      int
}

type ParsedDiscoveryPage struct {
	URL string
}

type ParsedDiscoverySource struct {
	URL  string
	Kind SourceKind
}

type ParsedDiscoveryDocument struct {
	Kind    SourceKind
	Pages   []ParsedDiscoveryPage
	Sources []ParsedDiscoverySource
}

type ParseDiscoveryFunc func(raw []byte, sourceURL string, requested SourceKind, limits DiscoveryLimits) (ParsedDiscoveryDocument, error)

type SourceRunner struct {
	Config   Config
	Frontier SourceFrontier
	Fetcher  SourceFetcher
	Parse    ParseDiscoveryFunc
	Clock    Clock
	LeaseTTL time.Duration
}

type SourceSummary struct {
	Planned          int           `json:"planned"`
	Due              int           `json:"due"`
	Attempted        int           `json:"attempted"`
	SnapshotsApplied int           `json:"snapshots_applied"`
	NotModified      int           `json:"not_modified"`
	Failed           int           `json:"failed"`
	PagesDiscovered  int           `json:"pages_discovered"`
	PagesEligible    int           `json:"pages_eligible"`
	ChildrenAdded    int           `json:"children_added"`
	Duration         time.Duration `json:"-"`
	DurationMS       int64         `json:"duration_ms"`
}

func (r SourceRunner) Run(ctx context.Context) (SourceSummary, error) {
	if r.Frontier == nil {
		return SourceSummary{}, errors.New("focused crawl: discovery frontier is required")
	}
	clock := r.Clock
	if clock == nil {
		clock = realClock{}
	}
	started := clock.Now()
	summary := SourceSummary{Planned: len(r.Config.SourceSeeds())}
	seeds := make([]SourceFrontierSeed, 0, summary.Planned)
	for _, seed := range r.Config.SourceSeeds() {
		seeds = append(seeds, SourceFrontierSeed{
			Key: seed.Key, JobID: seed.JobID, URL: seed.URL, Kind: seed.Kind, RootKey: seed.Key,
		})
	}
	if err := r.Frontier.SyncDiscoverySources(seeds, started); err != nil {
		return r.finish(summary, started, clock), fmt.Errorf("focused crawl: sync discovery sources: %w", err)
	}
	if summary.Planned == 0 {
		return r.finish(summary, started, clock), nil
	}
	if r.Fetcher == nil || r.Parse == nil {
		return r.finish(summary, started, clock), errors.New("focused crawl: source fetcher and parser are required")
	}
	leaseTTL := r.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = 5 * time.Minute
	}

	for _, job := range r.Config.Jobs {
		if job.MaxSourcePollsPerRun == 0 {
			continue
		}
		jobLeaseTTL := max(leaseTTL, job.MinHostInterval.Duration+15*time.Minute)
		for attempt := 0; attempt < job.MaxSourcePollsPerRun; attempt++ {
			entries, err := r.Frontier.LeaseDueDiscoverySourcesJob(job.ID, clock.Now(), 1, jobLeaseTTL)
			if err != nil {
				return r.finish(summary, started, clock), fmt.Errorf("focused crawl: lease discovery source for job %q: %w", job.ID, err)
			}
			if len(entries) == 0 {
				break
			}
			summary.Due++
			entry := entries[0]
			if err := ctx.Err(); err != nil {
				return r.finish(summary, started, clock), r.releaseInterrupted(entry, clock.Now(), err)
			}
			root, ok := r.Config.RootSource(entry.RootKey)
			if !ok || entry.JobID != job.ID || entry.Depth < 0 || entry.Depth > root.Config.MaxDepth {
				err := errors.New("focused crawl: leased discovery source has no valid configured root policy")
				return r.finish(summary, started, clock), r.releaseInterrupted(entry, clock.Now(), err)
			}
			canonicalSource, ok := sourceAllowedByRoot(entry.URL, root)
			if !ok || canonicalSource != entry.URL {
				// Policy narrowing can make a retained child snapshot ineligible
				// before its root is next refreshed. Do not let that expected
				// configuration change suppress unrelated page admissions.
				summary.Failed++
				if err := r.retry(entry, root, clock.Now(), 0, "outside_source_scope"); err != nil {
					return r.finish(summary, started, clock), err
				}
				continue
			}
			for {
				wait, err := r.Frontier.ReserveHost(hostForURL(entry.URL), clock.Now(), job.MinHostInterval.Duration)
				if err != nil {
					return r.finish(summary, started, clock), r.releaseInterrupted(entry, clock.Now(), fmt.Errorf("focused crawl: reserve discovery host: %w", err))
				}
				if wait <= 0 {
					break
				}
				if err := clock.Sleep(ctx, wait); err != nil {
					return r.finish(summary, started, clock), r.releaseInterrupted(entry, clock.Now(), err)
				}
			}

			summary.Attempted++
			result, fetchErr := r.Fetcher.Fetch(ctx, SourceFetchRequest{
				URL: entry.URL, AllowedPathPrefixes: append([]string(nil), root.Config.AllowedSourcePathPrefixes...),
				IfNoneMatch: entry.ETag, IfModifiedSince: entry.LastModified,
				MaxWireBytes: root.Config.MaxCompressedBytes, MaxDecompressedBytes: root.Config.MaxDecompressedBytes,
			})
			if err := ctx.Err(); err != nil {
				return r.finish(summary, started, clock), r.releaseInterrupted(entry, clock.Now(), err)
			}
			now := clock.Now()
			if fetchErr != nil {
				summary.Failed++
				if err := r.retry(entry, root, now, 0, boundedReason(fetchErr.Error())); err != nil {
					return r.finish(summary, started, clock), err
				}
				continue
			}
			if result.NotModified || result.Status == 304 {
				validators := mergeSourceValidators(entry, result)
				next := nextSourcePoll(now, root.Config.PollInterval.Duration, result.FreshUntil)
				if err := r.Frontier.PreserveDiscoverySnapshot(entry.Key, entry.LeaseGeneration, validators, next); err != nil {
					return r.finish(summary, started, clock), fmt.Errorf("focused crawl: preserve discovery snapshot %q: %w", entry.Key, err)
				}
				summary.NotModified++
				continue
			}
			if result.Status != 200 {
				summary.Failed++
				if err := r.retry(entry, root, now, result.RetryAfter, "http_"+strconv.Itoa(result.Status)); err != nil {
					return r.finish(summary, started, clock), err
				}
				continue
			}

			baseURL := result.FinalURL
			if baseURL == "" {
				baseURL = entry.URL
			}
			document, err := r.Parse(result.Body, baseURL, entry.Kind, DiscoveryLimits{
				MaxDocumentBytes: int(root.Config.MaxDecompressedBytes), MaxEntries: root.Config.MaxEntries,
				MaxURLBytes: 2048, MaxXMLDepth: 128,
			})
			if err != nil {
				summary.Failed++
				if retryErr := r.retry(entry, root, now, 0, boundedReason(err.Error())); retryErr != nil {
					return r.finish(summary, started, clock), retryErr
				}
				continue
			}
			summary.PagesDiscovered += len(document.Pages)
			snapshot, err := buildSourceSnapshot(job, root, entry, document, result, now)
			if err != nil {
				summary.Failed++
				if retryErr := r.retry(entry, root, now, 0, boundedReason(err.Error())); retryErr != nil {
					return r.finish(summary, started, clock), retryErr
				}
				continue
			}
			if err := r.Frontier.ApplyDiscoverySnapshot(snapshot, now); err != nil {
				releaseErr := r.retry(entry, root, now, 0, "snapshot_storage_failed")
				return r.finish(summary, started, clock), errors.Join(
					fmt.Errorf("focused crawl: apply discovery snapshot %q: %w", entry.Key, err), releaseErr,
				)
			}
			summary.SnapshotsApplied++
			summary.PagesEligible += len(snapshot.Pages)
			summary.ChildrenAdded += len(snapshot.Children)
		}
	}
	return r.finish(summary, started, clock), nil
}

func (r SourceRunner) retry(entry SourceFrontierEntry, root RootSource, now time.Time, retryAfter time.Duration, reason string) error {
	delay := root.Config.ErrorRecheckInterval.Duration
	if backoff := retryDelay(entry.Key, entry.Attempts); backoff > delay {
		delay = backoff
	}
	if retryAfter > delay {
		delay = retryAfter
	}
	return r.Frontier.CompleteDiscoverySource(entry.Key, entry.LeaseGeneration, now.Add(delay), boundedReason(reason))
}

func (r SourceRunner) releaseInterrupted(entry SourceFrontierEntry, now time.Time, cause error) error {
	err := r.Frontier.CompleteDiscoverySource(entry.Key, entry.LeaseGeneration, now.UTC(), "interrupted")
	if err != nil {
		return errors.Join(cause, fmt.Errorf("focused crawl: release interrupted discovery lease %q: %w", entry.Key, err))
	}
	return cause
}

func (r SourceRunner) finish(summary SourceSummary, started time.Time, clock Clock) SourceSummary {
	summary.Duration = clock.Now().Sub(started)
	summary.DurationMS = summary.Duration.Milliseconds()
	return summary
}

func buildSourceSnapshot(job Job, root RootSource, entry SourceFrontierEntry, document ParsedDiscoveryDocument, result SourceFetchResult, now time.Time) (SourceSnapshot, error) {
	if document.Kind != SourceKindSitemap && document.Kind != SourceKindFeed {
		return SourceSnapshot{}, errors.New("focused crawl: parser returned an unsupported discovery kind")
	}
	pages := make([]DiscoveredPage, 0, len(document.Pages))
	seenPages := make(map[string]struct{}, len(document.Pages))
	for _, page := range document.Pages {
		canonical, err := canonicalWebURL(page.URL)
		if err != nil || !job.Allows(canonical) {
			continue
		}
		if document.Kind == SourceKindSitemap && !sameSourceOrigin(canonical, root.Seed.URL) {
			continue
		}
		key := configKey(job.ID, canonical)
		if _, duplicate := seenPages[key]; duplicate {
			continue
		}
		seenPages[key] = struct{}{}
		pages = append(pages, DiscoveredPage{Key: key, URL: canonical})
	}

	children := make([]DiscoveredChildSource, 0, len(document.Sources))
	seenChildren := make(map[string]struct{}, len(document.Sources))
	for _, child := range document.Sources {
		canonical, allowed := sourceAllowedByRoot(child.URL, root)
		if !allowed {
			continue
		}
		// A sitemap index that names itself or its configured root does not
		// create a second identity or a redundant self-cycle.
		if canonical == entry.URL || canonical == root.Seed.URL {
			continue
		}
		if child.Kind != SourceKindSitemap {
			continue
		}
		key := childSourceKey(job.ID, root.Seed.Key, canonical)
		if _, duplicate := seenChildren[key]; duplicate {
			continue
		}
		seenChildren[key] = struct{}{}
		children = append(children, DiscoveredChildSource{
			Key: key, JobID: job.ID, URL: canonical, Kind: SourceKindSitemap,
			RootKey: root.Seed.Key, ParentKey: entry.Key, Depth: entry.Depth + 1,
		})
	}
	if len(children) > root.Config.MaxChildSources {
		return SourceSnapshot{}, fmt.Errorf("focused crawl: discovery source exceeds configured child-source limit")
	}
	if len(children) > 0 && entry.Depth >= root.Config.MaxDepth {
		return SourceSnapshot{}, fmt.Errorf("focused crawl: discovery source exceeds configured depth limit")
	}
	return SourceSnapshot{
		SourceKey: entry.Key, LeaseGeneration: entry.LeaseGeneration, Pages: pages, Children: children,
		Validators:   SourceValidators{ETag: result.ETag, LastModified: result.LastModified},
		NextEligible: nextSourcePoll(now, root.Config.PollInterval.Duration, result.FreshUntil),
	}, nil
}

func mergeSourceValidators(entry SourceFrontierEntry, result SourceFetchResult) SourceValidators {
	if result.NoStore {
		return SourceValidators{}
	}
	validators := SourceValidators{ETag: entry.ETag, LastModified: entry.LastModified}
	if result.ETag != "" {
		validators.ETag = result.ETag
	}
	if result.LastModified != "" {
		validators.LastModified = result.LastModified
	}
	return validators
}

func nextSourcePoll(now time.Time, interval time.Duration, freshUntil time.Time) time.Time {
	next := now.Add(interval)
	if freshUntil.After(next) {
		next = freshUntil
	}
	return next.UTC()
}

func sourceAllowedByRoot(raw string, root RootSource) (string, bool) {
	canonical, err := canonicalWebURL(raw)
	if err != nil {
		return "", false
	}
	candidate, err := url.Parse(canonical)
	if err != nil {
		return "", false
	}
	configured, err := url.Parse(root.Seed.URL)
	if err != nil || !strings.EqualFold(candidate.Scheme, configured.Scheme) || !strings.EqualFold(candidate.Hostname(), configured.Hostname()) || canonicalPort(candidate) != canonicalPort(configured) {
		return "", false
	}
	path := candidate.Path
	if path == "" {
		path = "/"
	}
	if !pathMatchesAnyPrefix(path, root.Config.AllowedSourcePathPrefixes) {
		return "", false
	}
	return canonical, true
}

func canonicalPort(parsed *url.URL) string {
	if parsed.Port() != "" {
		return parsed.Port()
	}
	if strings.EqualFold(parsed.Scheme, "https") {
		return "443"
	}
	return "80"
}

func sameSourceOrigin(left, right string) bool {
	leftURL, leftErr := url.Parse(left)
	rightURL, rightErr := url.Parse(right)
	return leftErr == nil && rightErr == nil &&
		strings.EqualFold(leftURL.Scheme, rightURL.Scheme) &&
		strings.EqualFold(leftURL.Hostname(), rightURL.Hostname()) &&
		canonicalPort(leftURL) == canonicalPort(rightURL)
}

func childSourceKey(jobID, rootKey, canonicalURL string) string {
	hash := sha256.Sum256([]byte(jobID + "\x00source-child\x00" + rootKey + "\x00" + canonicalURL))
	return hex.EncodeToString(hash[:])
}
