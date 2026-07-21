package focusedcrawl

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"net/url"
	"time"
)

type EntryState string

const (
	StateQueued    EntryState = "queued"
	StateLeased    EntryState = "leased"
	StateSucceeded EntryState = "succeeded"
	StateRejected  EntryState = "rejected"
	StateRetry     EntryState = "retry"
	StateDisabled  EntryState = "disabled"
)

type FrontierEntry struct {
	Key             string
	JobID           string
	URL             string
	State           EntryState
	Attempts        int
	NextEligible    time.Time
	LeaseUntil      time.Time
	LeaseGeneration uint64
	LastReason      string
	CanExpand       bool
}

type LinkPolicy struct {
	JobID           string
	MaxLinksPerPage int
}

type LinkCompletion struct {
	Key             string
	LeaseGeneration uint64
	Outcome         EntryState
	NextEligible    time.Time
	Reason          string
	Links           []Seed
}

type LinkMutationCounts struct {
	Reported     int
	AppliedEdges int
	AddedEdges   int
	RemovedEdges int
	Created      int
	Reactivated  int
}

type Frontier interface {
	SyncSeeds(seeds []Seed, now time.Time) error
	SyncLinkPolicies(policies []LinkPolicy, now time.Time) error
	LeaseDueJob(jobID string, now time.Time, limit int, leaseTTL time.Duration) ([]FrontierEntry, error)
	ReserveHost(authority string, now time.Time, interval time.Duration) (time.Duration, error)
	Complete(key string, leaseGeneration uint64, state EntryState, nextEligible time.Time, reason string) error
	CompleteWithLinks(completion LinkCompletion, now time.Time) (LinkMutationCounts, error)
	Counts() (map[EntryState]int, error)
}

type AdmissionResult struct {
	URL           string
	Status        string
	Reason        string
	OutboundLinks *[]string
}

type AdmissionRequest struct {
	URL                 string
	AllowedPathPrefixes []string
	DeniedPathPrefixes  []string
	DeniedURLs          []string
	MaxOutboundLinks    int
}

type AdmissionClient interface {
	Admit(ctx context.Context, request AdmissionRequest) (AdmissionResult, error)
}

type Clock interface {
	Now() time.Time
	Sleep(ctx context.Context, duration time.Duration) error
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }
func (realClock) Sleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type Runner struct {
	Config     Config
	Frontier   Frontier
	Admissions AdmissionClient
	Clock      Clock
	LeaseTTL   time.Duration
}

type Summary struct {
	Planned    int                `json:"planned"`
	Due        int                `json:"due"`
	Attempted  int                `json:"attempted"`
	Admitted   int                `json:"admitted"`
	Rejected   int                `json:"rejected"`
	Retried    int                `json:"retried"`
	Disabled   int                `json:"disabled"`
	Remaining  int                `json:"remaining"`
	States     map[EntryState]int `json:"states,omitempty"`
	Duration   time.Duration      `json:"-"`
	DurationMS int64              `json:"duration_ms"`

	LinkSnapshotsApplied       int `json:"link_snapshots_applied"`
	LinksReported              int `json:"links_reported"`
	LinksEligible              int `json:"links_eligible"`
	LinkEdgesApplied           int `json:"link_edges_applied"`
	LinkURLsCreated            int `json:"link_urls_created"`
	LinkURLsReactivated        int `json:"link_urls_reactivated"`
	LinkCandidatesDroppedScope int `json:"link_candidates_dropped_scope"`
	LinkSnapshotsSkippedBudget int `json:"link_snapshots_skipped_budget"`
}

var ErrStoragePaused = errors.New("focused crawl: Fetchmark storage admission paused")

type classifiedAdmissionError interface {
	error
	Fatal() bool
	Temporary() bool
	RetryAfter() time.Duration
}

func DryRun(config Config) Summary {
	return Summary{Planned: len(config.Seeds())}
}

// SyncPlan applies page-seed and link-expansion trust policy before any live
// source or page request. Runner calls it again immediately before leasing so
// direct use remains safe and command orchestration can fail closed early.
func SyncPlan(config Config, frontier Frontier, now time.Time) error {
	if frontier == nil || now.IsZero() {
		return errors.New("focused crawl: frontier and synchronization time are required")
	}
	if err := frontier.SyncSeeds(config.Seeds(), now); err != nil {
		return fmt.Errorf("focused crawl: sync frontier: %w", err)
	}
	policies := make([]LinkPolicy, 0, len(config.Jobs))
	for _, job := range config.Jobs {
		if job.LinkExpansion != nil {
			policies = append(policies, LinkPolicy{JobID: job.ID, MaxLinksPerPage: job.LinkExpansion.MaxLinksPerPage})
		}
	}
	if err := frontier.SyncLinkPolicies(policies, now); err != nil {
		return fmt.Errorf("focused crawl: sync link expansion policy: %w", err)
	}
	return nil
}

func (r Runner) Run(ctx context.Context) (Summary, error) {
	if r.Frontier == nil || r.Admissions == nil {
		return Summary{}, errors.New("focused crawl: frontier and admission client are required")
	}
	clock := r.Clock
	if clock == nil {
		clock = realClock{}
	}
	leaseTTL := r.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = 5 * time.Minute
	}
	started := clock.Now()
	summary := Summary{Planned: len(r.Config.Seeds())}
	if err := SyncPlan(r.Config, r.Frontier, started); err != nil {
		return summary, err
	}

	for _, job := range r.Config.Jobs {
		linkPagesRemaining := 0
		if job.LinkExpansion != nil {
			linkPagesRemaining = job.LinkExpansion.MaxPagesPerRun
		}
		jobLeaseTTL := max(leaseTTL, job.MinHostInterval.Duration+15*time.Minute)
		for jobAttempt := 0; jobAttempt < job.MaxURLsPerRun; jobAttempt++ {
			entries, err := r.Frontier.LeaseDueJob(job.ID, clock.Now(), 1, jobLeaseTTL)
			if err != nil {
				return r.finishSummary(summary, started, clock), fmt.Errorf("focused crawl: lease job %q: %w", job.ID, err)
			}
			if len(entries) == 0 {
				break
			}
			summary.Due++
			entry := entries[0]
			if err := ctx.Err(); err != nil {
				return r.finishSummary(summary, started, clock), r.releaseInterrupted(entry, clock.Now(), err)
			}
			if !job.Allows(entry.URL) {
				if err := r.Frontier.Complete(entry.Key, entry.LeaseGeneration, StateDisabled, time.Time{}, "outside_scope"); err != nil {
					return r.finishSummary(summary, started, clock), err
				}
				summary.Disabled++
				continue
			}
			host := hostForURL(entry.URL)
			for {
				wait, err := r.Frontier.ReserveHost(host, clock.Now(), job.MinHostInterval.Duration)
				if err != nil {
					return r.finishSummary(summary, started, clock), r.releaseInterrupted(entry, clock.Now(), fmt.Errorf("focused crawl: reserve host %q: %w", host, err))
				}
				if wait <= 0 {
					break
				}
				if err := clock.Sleep(ctx, wait); err != nil {
					return r.finishSummary(summary, started, clock), r.releaseInterrupted(entry, clock.Now(), err)
				}
			}
			summary.Attempted++
			maxOutboundLinks := 0
			if job.LinkExpansion != nil && entry.CanExpand {
				if linkPagesRemaining > 0 {
					maxOutboundLinks = job.LinkExpansion.MaxLinksPerPage
					linkPagesRemaining--
				} else {
					summary.LinkSnapshotsSkippedBudget++
				}
			}
			result, admissionErr := r.Admissions.Admit(ctx, AdmissionRequest{
				URL: entry.URL, AllowedPathPrefixes: append([]string(nil), job.AllowedPathPrefixes...),
				DeniedPathPrefixes: append([]string(nil), job.DeniedPathPrefixes...),
				DeniedURLs:         append([]string(nil), job.DeniedURLs...),
				MaxOutboundLinks:   maxOutboundLinks,
			})
			if err := ctx.Err(); err != nil {
				return r.finishSummary(summary, started, clock), r.releaseInterrupted(entry, clock.Now(), err)
			}
			if admissionErr == nil && result.Status == "admitted" && maxOutboundLinks > 0 && result.OutboundLinks == nil {
				admissionErr = errors.New("focused crawl: admission omitted requested outbound-link snapshot")
			}
			completedAt := clock.Now()
			state, next, reason, stopErr := classifyAdmission(job, entry, result, admissionErr, completedAt)
			linkSnapshot := admissionErr == nil && entry.CanExpand && job.LinkExpansion != nil &&
				(result.Status == "rejected" || result.Status == "admitted" && maxOutboundLinks > 0 && result.OutboundLinks != nil)
			if linkSnapshot {
				links := []Seed(nil)
				if result.Status == "admitted" {
					var reported, dropped int
					links, reported, dropped = eligibleLinkSeeds(job, entry, *result.OutboundLinks)
					summary.LinksReported += reported
					summary.LinksEligible += len(links)
					summary.LinkCandidatesDroppedScope += dropped
				}
				counts, err := r.Frontier.CompleteWithLinks(LinkCompletion{
					Key: entry.Key, LeaseGeneration: entry.LeaseGeneration, Outcome: state,
					NextEligible: next, Reason: reason, Links: links,
				}, completedAt)
				if err != nil {
					return r.finishSummary(summary, started, clock), fmt.Errorf("focused crawl: complete link snapshot %q: %w", entry.Key, err)
				}
				summary.LinkSnapshotsApplied++
				summary.LinkEdgesApplied += counts.AppliedEdges
				summary.LinkURLsCreated += counts.Created
				summary.LinkURLsReactivated += counts.Reactivated
			} else if err := r.Frontier.Complete(entry.Key, entry.LeaseGeneration, state, next, reason); err != nil {
				return r.finishSummary(summary, started, clock), fmt.Errorf("focused crawl: complete %q: %w", entry.Key, err)
			}
			switch state {
			case StateSucceeded:
				summary.Admitted++
			case StateRejected:
				summary.Rejected++
			case StateRetry:
				summary.Retried++
			}
			if stopErr != nil {
				return r.finishSummary(summary, started, clock), stopErr
			}
		}
	}
	return r.finishSummary(summary, started, clock), nil
}

func (r Runner) releaseInterrupted(entry FrontierEntry, now time.Time, cause error) error {
	completeErr := r.Frontier.Complete(entry.Key, entry.LeaseGeneration, StateRetry, now.UTC(), "interrupted")
	if completeErr != nil {
		return errors.Join(cause, fmt.Errorf("focused crawl: release interrupted lease %q: %w", entry.Key, completeErr))
	}
	return cause
}

func eligibleLinkSeeds(job Job, parent FrontierEntry, reported []string) ([]Seed, int, int) {
	links := make([]Seed, 0, len(reported))
	seen := make(map[string]struct{}, len(reported))
	for _, raw := range reported {
		canonical, err := canonicalWebURL(raw)
		if err != nil || canonical != raw || canonical == parent.URL || !job.Allows(canonical) {
			continue
		}
		key := configKey(job.ID, canonical)
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		links = append(links, Seed{Key: key, JobID: job.ID, URL: canonical})
	}
	return links, len(reported), len(reported) - len(links)
}

func (r Runner) finishSummary(summary Summary, started time.Time, clock Clock) Summary {
	counts, err := r.Frontier.Counts()
	if err == nil {
		summary.States = counts
		summary.Disabled = counts[StateDisabled]
		for state, count := range counts {
			if state != StateDisabled {
				summary.Remaining += count
			}
		}
	}
	summary.Duration = clock.Now().Sub(started)
	summary.DurationMS = summary.Duration.Milliseconds()
	return summary
}

func classifyAdmission(job Job, entry FrontierEntry, result AdmissionResult, admissionErr error, now time.Time) (EntryState, time.Time, string, error) {
	if admissionErr != nil {
		delay := retryDelay(entry.Key, entry.Attempts)
		var classified classifiedAdmissionError
		if errors.As(admissionErr, &classified) {
			if after := classified.RetryAfter(); after > delay {
				delay = after
			}
			if classified.Fatal() {
				if delay < 5*time.Minute {
					delay = 5 * time.Minute
				}
				return StateRetry, now.Add(delay), boundedReason(admissionErr.Error()), admissionErr
			}
			if !classified.Temporary() {
				return StateRejected, now.Add(job.RejectionRecheckInterval.Duration), boundedReason(admissionErr.Error()), nil
			}
		}
		if entry.Attempts >= job.MaxAttempts {
			return StateRejected, now.Add(job.RejectionRecheckInterval.Duration), "attempts_exhausted", nil
		}
		return StateRetry, now.Add(delay), boundedReason(admissionErr.Error()), nil
	}

	switch result.Status {
	case "admitted":
		return StateSucceeded, now.Add(job.RefreshInterval.Duration), "", nil
	case "rejected":
		return StateRejected, now.Add(job.RejectionRecheckInterval.Duration), boundedReason(result.Reason), nil
	case "failed":
		if result.Reason == "storage_failed" {
			return StateRetry, now.Add(time.Hour), result.Reason, ErrStoragePaused
		}
		if entry.Attempts >= job.MaxAttempts {
			return StateRejected, now.Add(job.RejectionRecheckInterval.Duration), "attempts_exhausted", nil
		}
		return StateRetry, now.Add(retryDelay(entry.Key, entry.Attempts)), boundedReason(result.Reason), nil
	default:
		if entry.Attempts >= job.MaxAttempts {
			return StateRejected, now.Add(job.RejectionRecheckInterval.Duration), "attempts_exhausted", nil
		}
		return StateRetry, now.Add(retryDelay(entry.Key, entry.Attempts)), "invalid_admission_status", nil
	}
}

func retryDelay(key string, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 9 {
		attempt = 9
	}
	base := time.Minute * time.Duration(1<<(attempt-1))
	if base > 6*time.Hour {
		base = 6 * time.Hour
	}
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", key, attempt)))
	jitterWindow := max(time.Second, base/10)
	jitter := time.Duration(binary.BigEndian.Uint64(hash[:8]) % uint64(jitterWindow))
	return base + jitter
}

func boundedReason(reason string) string {
	const maxReasonBytes = 256
	if len(reason) > maxReasonBytes {
		return reason[:maxReasonBytes]
	}
	return reason
}

func hostForURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return parsed.Host
}
