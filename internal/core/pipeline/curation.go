package pipeline

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/adapters/extractor"
	"github.com/staticvar/fetchmark/internal/adapters/fetcher"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
	"github.com/staticvar/fetchmark/internal/obs"
)

type urlMutationLockEntry struct {
	token chan struct{}
	refs  int
}

// urlMutationLocks is bounded by active and waiting URL lifecycles. Entries
// disappear after the final release, so arbitrary URLs cannot create durable
// or process-lifetime lock state.
type urlMutationLocks struct {
	mu      sync.Mutex
	entries map[string]*urlMutationLockEntry
}

func (locks *urlMutationLocks) acquire(ctx context.Context, key string) (func(), error) {
	locks.mu.Lock()
	if locks.entries == nil {
		locks.entries = make(map[string]*urlMutationLockEntry)
	}
	entry := locks.entries[key]
	if entry == nil {
		entry = &urlMutationLockEntry{token: make(chan struct{}, 1)}
		locks.entries[key] = entry
	}
	entry.refs++
	locks.mu.Unlock()

	select {
	case entry.token <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() {
				<-entry.token
				locks.mu.Lock()
				entry.refs--
				if entry.refs == 0 {
					delete(locks.entries, key)
				}
				locks.mu.Unlock()
			})
		}, nil
	case <-ctx.Done():
		locks.mu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(locks.entries, key)
		}
		locks.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (p *Pipeline) acquireCuratedMutation(ctx context.Context, canonicalURL string) (func(), error) {
	mode := p.LocalRetentionPolicy.Mode
	if (mode != localcorpus.ModeCurated && mode != localcorpus.ModeArchive) || (p.LocalCorpus == nil && p.LocalArtifacts == nil) {
		return func() {}, nil
	}
	return p.curatedMutationLocks.acquire(ctx, canonicalURL)
}

type CuratedMutationStatus string

const (
	CuratedStatusAdmitted  CuratedMutationStatus = "admitted"
	CuratedStatusRejected  CuratedMutationStatus = "rejected"
	CuratedStatusFailed    CuratedMutationStatus = "failed"
	CuratedStatusTakenDown CuratedMutationStatus = "taken_down"
)

// MaxFocusedOutboundLinks bounds the crawler-only metadata returned from one
// focused admission. Ordinary curated mutations never return link metadata.
const MaxFocusedOutboundLinks = 64

const (
	CuratedReasonInvalidURL       = "invalid_url"
	CuratedReasonNotConfigured    = "not_configured"
	CuratedReasonRobotsBlocked    = "robots_blocked"
	CuratedReasonRobotsUnknown    = "robots_unknown"
	CuratedReasonNoIndex          = "noindex"
	CuratedReasonNoArchive        = "noarchive"
	CuratedReasonUnsupported      = "unsupported"
	CuratedReasonFetchFailed      = "fetch_failed"
	CuratedReasonExtractionFailed = "extraction_failed"
	CuratedReasonStorageFailed    = "storage_failed"
	CuratedReasonTakedownFailed   = "takedown_failed"
	CuratedReasonInvalidSafety    = "invalid_safety_classification"
	CuratedReasonRedirectScope    = "redirect_scope"
)

// CuratedMutationResult never exposes a page body or snippet. The crawler-only
// focused path may additionally return a bounded outbound-link snapshot.
type CuratedMutationResult struct {
	URL           string                `json:"url"`
	Status        CuratedMutationStatus `json:"status"`
	Reason        string                `json:"reason,omitempty"`
	OutboundLinks *[]string             `json:"outbound_links,omitempty"`
}

// FocusedAdmission carries only a canonical URL and the operator-configured
// path boundary needed to validate redirects. It never carries page content.
type FocusedAdmission struct {
	URL                 string
	AllowedPathPrefixes []string
	DeniedPathPrefixes  []string
	DeniedURLs          []string
	MaxOutboundLinks    int
}

// AdmitCurated performs a fresh, policy-checked retrieval for each URL. The
// package-private retention intent prevents ordinary API translators from
// turning a parse request into a persistent curated mutation.
func (p *Pipeline) AdmitCurated(ctx context.Context, urls []string) []CuratedMutationResult {
	return p.AdmitCuratedClassified(ctx, urls, localcorpus.SafetyUnclassified)
}

// AdmitCuratedFocused is the crawler-only admission boundary. In addition to
// the ordinary curated policy checks, redirects are confined to the submitted
// URL's original scheme, authority, and directory before any body is fetched
// or retained.
func (p *Pipeline) AdmitCuratedFocused(ctx context.Context, admission FocusedAdmission) []CuratedMutationResult {
	scope := &fetcher.RedirectScope{
		AllowedPathPrefixes: append([]string(nil), admission.AllowedPathPrefixes...),
		DeniedPathPrefixes:  append([]string(nil), admission.DeniedPathPrefixes...),
		DeniedURLs:          append([]string(nil), admission.DeniedURLs...),
	}
	return p.admitCurated(ctx, []string{admission.URL}, localcorpus.SafetyUnclassified, scope, admission.MaxOutboundLinks)
}

// AdmitCuratedClassified records only an explicit operator assertion. It does
// not attempt to infer safety from untrusted page text.
func (p *Pipeline) AdmitCuratedClassified(ctx context.Context, urls []string, classification localcorpus.SafetyClassification) []CuratedMutationResult {
	return p.admitCurated(ctx, urls, classification, nil, 0)
}

func (p *Pipeline) admitCurated(ctx context.Context, urls []string, classification localcorpus.SafetyClassification, focusedRedirectScope *fetcher.RedirectScope, maxOutboundLinks int) []CuratedMutationResult {
	if p == nil || p.LocalRetentionPolicy.Mode != localcorpus.ModeCurated || p.LocalCorpus == nil || p.LocalArtifacts == nil {
		return failedCuratedResults(urls, CuratedReasonNotConfigured)
	}
	parsedClassification, err := localcorpus.ParseSafetyClassification(string(classification))
	if err != nil {
		return failedCuratedResults(urls, CuratedReasonInvalidSafety)
	}
	results := p.Parse(ctx, Options{
		URLs: urls, MaxResults: len(urls), RespectRobots: true, UserAgent: p.RobotsUserAgent,
		PreserveURLResults: true, forceFresh: true, skipOutputReservation: true, retentionIntent: retentionCurated,
		safetyClassification: parsedClassification, focusedRedirectScope: focusedRedirectScope,
	})
	out := make([]CuratedMutationResult, 0, len(results))
	for _, result := range results {
		mutation := CuratedMutationResult{URL: result.URL}
		if result.Content != nil && result.Unsupported == "" {
			mutation.Status = CuratedStatusAdmitted
			if maxOutboundLinks > 0 {
				outboundLinks := focusedOutboundLinks(result.Content.OutboundLinks, maxOutboundLinks)
				mutation.OutboundLinks = &outboundLinks
			}
			obs.LocalCuratedMutationTotal.WithLabelValues("admit", "ok").Inc()
		} else {
			mutation.Reason = normalizeCuratedReason(result.Unsupported)
			if curatedReasonIsRejection(mutation.Reason) {
				mutation.Status = CuratedStatusRejected
				obs.LocalCuratedMutationTotal.WithLabelValues("admit", "rejected").Inc()
			} else {
				mutation.Status = CuratedStatusFailed
				obs.LocalCuratedMutationTotal.WithLabelValues("admit", "error").Inc()
			}
		}
		out = append(out, mutation)
	}
	return out
}

func focusedOutboundLinks(extracted []string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	if limit > MaxFocusedOutboundLinks {
		limit = MaxFocusedOutboundLinks
	}
	out := make([]string, 0, min(limit, len(extracted)))
	seen := make(map[string]struct{}, min(limit, len(extracted)))
	for _, raw := range extracted {
		if len(raw) > 2048 {
			continue
		}
		parsed, err := url.Parse(strings.TrimSpace(raw))
		if err != nil || parsed.User != nil || parsed.Hostname() == "" ||
			(!strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https")) {
			continue
		}
		canonical, err := cache.CanonicalURL(parsed.String())
		if err != nil || canonical == "" || len(canonical) > 2048 {
			continue
		}
		if _, duplicate := seen[canonical]; duplicate {
			continue
		}
		seen[canonical] = struct{}{}
		out = append(out, canonical)
		if len(out) == limit {
			break
		}
	}
	return out
}

// TakedownCurated writes the non-searchable tombstone before revoking source
// bytes. A failed index mutation never reports success or proceeds as though
// the privacy boundary had been established.
func (p *Pipeline) TakedownCurated(ctx context.Context, urls []string) []CuratedMutationResult {
	if p == nil || (p.LocalRetentionPolicy.Mode != localcorpus.ModeCurated && p.LocalRetentionPolicy.Mode != localcorpus.ModeArchive) || p.LocalCorpus == nil || p.LocalArtifacts == nil {
		return failedCuratedResults(urls, CuratedReasonNotConfigured)
	}
	out := make([]CuratedMutationResult, 0, len(urls))
	for _, rawURL := range urls {
		canonical, err := cache.CanonicalURL(rawURL)
		if err != nil {
			out = append(out, CuratedMutationResult{URL: rawURL, Status: CuratedStatusFailed, Reason: CuratedReasonInvalidURL})
			obs.LocalCuratedMutationTotal.WithLabelValues("takedown", "error").Inc()
			continue
		}
		releaseMutation, err := p.acquireCuratedMutation(ctx, canonical)
		if err != nil {
			out = append(out, CuratedMutationResult{URL: canonical, Status: CuratedStatusFailed, Reason: CuratedReasonTakedownFailed})
			obs.LocalCuratedMutationTotal.WithLabelValues("takedown", "error").Inc()
			continue
		}
		observedAt := time.Now().UTC()
		document, retain := p.LocalRetentionPolicy.ApplyExplicit(localcorpus.Document{
			URL: canonical, FetchedAt: observedAt, IndexingDisposition: localcorpus.DispositionTakedown,
		})
		if !retain {
			releaseMutation()
			out = append(out, CuratedMutationResult{URL: canonical, Status: CuratedStatusFailed, Reason: CuratedReasonNotConfigured})
			obs.LocalCuratedMutationTotal.WithLabelValues("takedown", "error").Inc()
			continue
		}
		indexCtx, indexCancel := localCorpusMutationContext(ctx)
		indexErr := p.LocalCorpus.Reconcile(indexCtx, document)
		indexCancel()
		artifactCtx, artifactCancel := localArtifactMutationContext(ctx)
		artifactErr := p.LocalArtifacts.Revoke(artifactCtx, canonical, localcorpus.DispositionTakedown, observedAt)
		artifactCancel()
		err = errors.Join(indexErr, artifactErr)
		releaseMutation()
		if err != nil {
			out = append(out, CuratedMutationResult{URL: canonical, Status: CuratedStatusFailed, Reason: CuratedReasonTakedownFailed})
			obs.LocalCuratedMutationTotal.WithLabelValues("takedown", "error").Inc()
			continue
		}
		out = append(out, CuratedMutationResult{URL: canonical, Status: CuratedStatusTakenDown})
		obs.LocalCuratedMutationTotal.WithLabelValues("takedown", "ok").Inc()
	}
	return out
}

func (p *Pipeline) reconcileCuratedRevocation(ctx context.Context, rawURL string, disposition localcorpus.IndexingDisposition, observedAt time.Time) error {
	if p.LocalRetentionPolicy.Mode != localcorpus.ModeCurated || p.LocalCorpus == nil || p.LocalArtifacts == nil {
		return errors.New("curated corpus is not configured")
	}
	document, retain := p.LocalRetentionPolicy.ApplyExplicit(localcorpus.Document{
		URL: rawURL, FetchedAt: observedAt, IndexingDisposition: disposition,
	})
	if !retain {
		return errors.New("curated revocation was not admitted")
	}
	indexCtx, indexCancel := localCorpusMutationContext(ctx)
	indexErr := p.LocalCorpus.Reconcile(indexCtx, document)
	indexCancel()
	if indexErr != nil {
		obs.LocalIndexMutationTotal.WithLabelValues("error").Inc()
	} else {
		obs.LocalIndexMutationTotal.WithLabelValues("removed").Inc()
	}
	artifactCtx, artifactCancel := localArtifactMutationContext(ctx)
	artifactErr := p.LocalArtifacts.Revoke(artifactCtx, rawURL, disposition, observedAt)
	artifactCancel()
	if artifactErr != nil {
		obs.LocalArtifactMutationTotal.WithLabelValues("error").Inc()
	} else {
		obs.LocalArtifactMutationTotal.WithLabelValues("revoked").Inc()
	}
	return errors.Join(indexErr, artifactErr)
}

func (p *Pipeline) failClosedCuratedRetention(ctx context.Context, rawURL string, observedAt time.Time) error {
	err := p.reconcileCuratedRevocation(ctx, rawURL, localcorpus.DispositionUnknown, observedAt)
	if err != nil {
		// Stop the local lane immediately. Read-side artifact verification
		// provides the same fail-closed guarantee after restart.
		p.localCorpusQuarantined.Store(true)
	}
	return err
}

// reconcileExistingCuratedRevocation removes only state that already exists.
// Each store is attempted independently: a crash or index rebuild can leave a
// retained artifact without a searchable projection, and a later policy denial
// must still purge that source without letting ordinary requests allocate new
// durable tombstones.
func (p *Pipeline) reconcileExistingCuratedRevocation(ctx context.Context, document localcorpus.Document) error {
	var indexErr, artifactErr error
	if writer, ok := p.LocalCorpus.(existingCorpusWriter); ok {
		mutationCtx, cancel := localCorpusMutationContext(ctx)
		var mutated bool
		mutated, indexErr = writer.ReconcileExisting(mutationCtx, document)
		cancel()
		switch {
		case indexErr != nil:
			obs.LocalIndexMutationTotal.WithLabelValues("error").Inc()
		case mutated:
			obs.LocalIndexMutationTotal.WithLabelValues("removed").Inc()
		}
	}
	if revoker, ok := p.LocalArtifacts.(existingArtifactRevoker); ok {
		mutationCtx, cancel := localArtifactMutationContext(ctx)
		var mutated bool
		mutated, artifactErr = revoker.RevokeExisting(
			mutationCtx, document.URL, document.IndexingDisposition, document.FetchedAt,
		)
		cancel()
		switch {
		case artifactErr != nil:
			obs.LocalArtifactMutationTotal.WithLabelValues("error").Inc()
		case mutated:
			obs.LocalArtifactMutationTotal.WithLabelValues("revoked").Inc()
		}
	}
	return errors.Join(indexErr, artifactErr)
}

func curatedUnsupportedReason(disposition localcorpus.IndexingDisposition, extractedReason string, err error) string {
	if err != nil {
		return CuratedReasonStorageFailed
	}
	if strings.TrimSpace(extractedReason) != "" {
		return normalizeCuratedReason(extractedReason)
	}
	switch disposition {
	case localcorpus.DispositionRobotsBlocked:
		return CuratedReasonRobotsBlocked
	case localcorpus.DispositionNoIndexHeader, localcorpus.DispositionNoIndexMetadata:
		return CuratedReasonNoIndex
	case localcorpus.DispositionNoArchiveHeader, localcorpus.DispositionNoArchiveMetadata:
		return CuratedReasonNoArchive
	case localcorpus.DispositionUnknown:
		return CuratedReasonRobotsUnknown
	default:
		return CuratedReasonUnsupported
	}
}

func normalizeCuratedReason(reason string) string {
	switch strings.TrimSpace(reason) {
	case fetcher.ReasonRobots:
		return CuratedReasonRobotsBlocked
	case "noindex":
		return CuratedReasonNoIndex
	case "noarchive":
		return CuratedReasonNoArchive
	case "extract_failed":
		return CuratedReasonExtractionFailed
	case extractor.ReasonJSRequired, fetcher.ReasonNonHTML, fetcher.ReasonTooLarge:
		return CuratedReasonUnsupported
	case fetcher.ReasonRedirectScope:
		return CuratedReasonRedirectScope
	case CuratedReasonStorageFailed:
		return CuratedReasonStorageFailed
	case CuratedReasonRobotsBlocked, CuratedReasonRobotsUnknown, CuratedReasonUnsupported,
		CuratedReasonExtractionFailed, CuratedReasonInvalidURL, CuratedReasonNotConfigured,
		CuratedReasonTakedownFailed, CuratedReasonInvalidSafety:
		return strings.TrimSpace(reason)
	case "", "fetch_failed":
		return CuratedReasonFetchFailed
	default:
		return CuratedReasonUnsupported
	}
}

func curatedReasonIsRejection(reason string) bool {
	switch reason {
	case CuratedReasonRobotsBlocked, CuratedReasonRobotsUnknown, CuratedReasonNoIndex,
		CuratedReasonNoArchive, CuratedReasonUnsupported, CuratedReasonExtractionFailed, CuratedReasonRedirectScope:
		return true
	default:
		return false
	}
}

func failedCuratedResults(urls []string, reason string) []CuratedMutationResult {
	out := make([]CuratedMutationResult, 0, len(urls))
	for _, rawURL := range urls {
		canonical, err := cache.CanonicalURL(rawURL)
		resultReason := reason
		if err != nil {
			canonical = rawURL
			resultReason = CuratedReasonInvalidURL
		}
		out = append(out, CuratedMutationResult{URL: canonical, Status: CuratedStatusFailed, Reason: resultReason})
	}
	return out
}
