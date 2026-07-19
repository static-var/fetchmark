// Package search declares the domain-facing search contract. Adapters
// like SearXNG live outside this package and implement Searcher.
package search

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/staticvar/fetchmark/internal/core/model"
)

// UnsupportedControlError reports that a request control cannot be honored by
// the configured discovery providers. Callers can map it to a client error
// without parsing an adapter-specific message.
type UnsupportedControlError struct {
	Control string
	Reason  string
}

func (e *UnsupportedControlError) Error() string {
	if e == nil {
		return "unsupported search control"
	}
	if e.Reason == "" {
		return fmt.Sprintf("search control %q is unsupported", e.Control)
	}
	return fmt.Sprintf("search control %q is unsupported: %s", e.Control, e.Reason)
}

// Query describes a user's search intent.
type Query struct {
	Q              string
	Engines        []string
	Categories     []string
	Language       string
	TimeRange      string
	SafeSearch     *int
	IncludeDomains []string
	ExcludeDomains []string
	ExactMatch     bool
	SearchDepth    string
	MaxResults     int
}

// Hit is the minimal per-result data returned by a Searcher, prior to
// any URL fetching or content extraction.
type Hit struct {
	URL         string
	Title       string
	Snippet     string
	Engines     []string
	PublishedAt *time.Time
	Metadata    map[string]string
	Provenance  []model.DiscoveryProvenance
	// ProviderDocument is optional content supplied by a fixed provider API.
	// The pipeline extracts it without crawling the result URL or retaining it.
	ProviderDocument *model.ProviderDocument `json:"-"`
}

// BatchStatus describes whether a provider's response can be trusted as a
// complete answer. It is intentionally separate from transport errors: some
// providers return HTTP success while reporting that their underlying sources
// failed.
type BatchStatus string

const (
	BatchHealthy            BatchStatus = "healthy"
	BatchPartial            BatchStatus = "partial"
	BatchDegradedEmpty      BatchStatus = "degraded_empty"
	BatchAuthoritativeEmpty BatchStatus = "authoritative_empty"
	BatchFailed             BatchStatus = "failed"
)

const (
	MaxDiscoveryLanes              = 128
	MaxDiscoveryDiagnosticsPerLane = 16
	MaxDiscoveryCandidateCount     = 100
	MaxDiscoveryDuration           = 24 * time.Hour
	MaxDiscoveryRetryAfter         = 24 * time.Hour
)

// ValidBatchStatus reports whether status is one of the closed discovery
// outcome values accepted by native evidence and evaluation artifacts.
func ValidBatchStatus(status BatchStatus) bool {
	switch status {
	case BatchHealthy, BatchPartial, BatchDegradedEmpty, BatchAuthoritativeEmpty, BatchFailed:
		return true
	default:
		return false
	}
}

// ProviderDiagnostic preserves a provider's source-level failure without
// forcing callers to parse adapter-specific response payloads.
type ProviderDiagnostic struct {
	Provider   string
	Instance   string
	Source     string
	Reason     string
	Retryable  bool
	RetryAfter time.Duration
}

// DiscoveryDiagnostic is the bounded, transport-neutral diagnostic retained
// for one planned lane. Provider instances and raw error text are deliberately
// excluded so native responses cannot disclose internal endpoints or create
// unbounded public data.
type DiscoveryDiagnostic struct {
	Source       string `json:"source,omitempty"`
	Reason       string `json:"reason"`
	Retryable    bool   `json:"retryable,omitempty"`
	RetryAfterMS int64  `json:"retry_after_ms,omitempty"`
}

// DiscoveryLaneReport records one trusted planner lane in deterministic plan
// order. Identity comes from the validated plan rather than adapter-returned
// provider strings.
type DiscoveryLaneReport struct {
	Provider             string                `json:"provider"`
	Lane                 string                `json:"lane"`
	Variant              string                `json:"variant"`
	Status               BatchStatus           `json:"status"`
	CandidateCount       int                   `json:"candidate_count"`
	DurationMS           int64                 `json:"duration_ms"`
	Diagnostics          []DiscoveryDiagnostic `json:"diagnostics,omitempty"`
	DiagnosticsTruncated bool                  `json:"diagnostics_truncated,omitempty"`
}

// DiscoveryReport is the aggregate discovery outcome for one canonical
// search. It remains separate from extraction results: a healthy discovery
// can still yield fetch or robots failures later in the pipeline.
type DiscoveryReport struct {
	Status BatchStatus           `json:"status"`
	Lanes  []DiscoveryLaneReport `json:"lanes,omitempty"`
}

// ValidateDiscoveryReport enforces the public evidence bounds and proves that
// the aggregate status is derivable from the ordered lane outcomes. It is
// shared by the HTTP boundary and offline artifact loader so accepted evidence
// cannot have different meanings in each subsystem.
func ValidateDiscoveryReport(report DiscoveryReport) error {
	if !ValidBatchStatus(report.Status) {
		return errors.New("invalid aggregate status")
	}
	if len(report.Lanes) == 0 {
		if report.Status != BatchFailed {
			return errors.New("non-failed discovery report has no lanes")
		}
		return nil
	}
	if len(report.Lanes) > MaxDiscoveryLanes {
		return errors.New("too many lane reports")
	}
	seen := make(map[string]struct{}, len(report.Lanes))
	for _, lane := range report.Lanes {
		if !validEvidenceToken(lane.Provider) || !validEvidenceToken(lane.Lane) || !validDiscoveryVariant(lane.Variant) || !ValidBatchStatus(lane.Status) {
			return errors.New("invalid lane identity or status")
		}
		key := lane.Provider + "/" + lane.Lane + ":" + lane.Variant
		if _, duplicate := seen[key]; duplicate {
			return errors.New("duplicate lane report")
		}
		seen[key] = struct{}{}
		if lane.CandidateCount < 0 || lane.CandidateCount > MaxDiscoveryCandidateCount || lane.DurationMS < 0 || lane.DurationMS > MaxDiscoveryDuration.Milliseconds() {
			return errors.New("invalid lane count or duration")
		}
		usable := lane.Status == BatchHealthy || lane.Status == BatchPartial
		if usable != (lane.CandidateCount > 0) {
			return errors.New("lane status and candidate count disagree")
		}
		if len(lane.Diagnostics) > MaxDiscoveryDiagnosticsPerLane {
			return errors.New("too many lane diagnostics")
		}
		for _, diagnostic := range lane.Diagnostics {
			if diagnostic.Source != "" && !validEvidenceToken(diagnostic.Source) {
				return errors.New("invalid diagnostic source")
			}
			if !validEvidenceToken(diagnostic.Reason) || diagnostic.RetryAfterMS < 0 || diagnostic.RetryAfterMS > MaxDiscoveryRetryAfter.Milliseconds() {
				return errors.New("invalid diagnostic reason or retry delay")
			}
		}
	}
	derived := AggregateDiscoveryStatus(report.Lanes)
	if report.Status != derived {
		return errors.New("aggregate status disagrees with lane outcomes")
	}
	return nil
}

// ValidatedDiscoveryReport returns a deep copy only when the report satisfies
// the complete public contract. Callers can fail closed without retaining
// slices owned by an optional implementation.
func ValidatedDiscoveryReport(report DiscoveryReport) (DiscoveryReport, error) {
	if err := ValidateDiscoveryReport(report); err != nil {
		return DiscoveryReport{}, err
	}
	cloned := DiscoveryReport{Status: report.Status, Lanes: make([]DiscoveryLaneReport, len(report.Lanes))}
	copy(cloned.Lanes, report.Lanes)
	for index := range cloned.Lanes {
		cloned.Lanes[index].Diagnostics = append([]DiscoveryDiagnostic(nil), report.Lanes[index].Diagnostics...)
	}
	return cloned, nil
}

// AggregateDiscoveryStatus derives the broker aggregate using the same rules
// as lane collection. Callers must validate individual lanes first.
func AggregateDiscoveryStatus(lanes []DiscoveryLaneReport) BatchStatus {
	usable := false
	degraded := false
	authoritative := false
	failed := false
	for _, lane := range lanes {
		switch lane.Status {
		case BatchHealthy:
			usable = true
		case BatchPartial:
			usable = true
			degraded = true
		case BatchDegradedEmpty:
			degraded = true
		case BatchAuthoritativeEmpty:
			authoritative = true
		case BatchFailed:
			failed = true
		}
	}
	if usable {
		if degraded || failed {
			return BatchPartial
		}
		return BatchHealthy
	}
	if degraded || (authoritative && failed) {
		return BatchDegradedEmpty
	}
	if authoritative {
		return BatchAuthoritativeEmpty
	}
	return BatchFailed
}

func validEvidenceToken(value string) bool {
	if value == "" || len(value) > 48 || strings.TrimSpace(value) != value || strings.ToLower(value) != value {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '_' && character != '-' {
			return false
		}
	}
	return true
}

func validDiscoveryVariant(variant string) bool {
	switch variant {
	case "original", "exact", "freshness", "docs", "other":
		return true
	default:
		return false
	}
}

// SearchBatch is the richer discovery result used by provider-aware callers.
// Searcher remains the compatibility contract while adapters migrate to
// BatchSearcher incrementally.
type SearchBatch struct {
	Hits        []Hit
	Provider    string
	Instance    string
	Status      BatchStatus
	Diagnostics []ProviderDiagnostic
	Duration    time.Duration
}

// Partial reports whether the batch contains usable hits alongside provider
// degradation. Keeping this derived avoids contradictory Status/Partial state.
func (b SearchBatch) Partial() bool {
	return b.Status == BatchPartial
}

// Searcher returns a ranked list of URLs matching a query. Implementations
// must be safe for concurrent use.
type Searcher interface {
	Search(ctx context.Context, q Query) ([]Hit, error)
}

// BatchSearcher is an optional richer contract. Implementations should keep
// Search for compatibility and delegate it to SearchBatch where practical.
type BatchSearcher interface {
	SearchBatch(ctx context.Context, q Query) (SearchBatch, error)
}

var _ error = (*UnsupportedControlError)(nil)
