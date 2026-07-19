// Package feedindex exposes an operator-built RSS/Atom metadata snapshot as a
// fresh-only, CPU-local discovery source. It never performs network I/O: a
// separate collector must establish the feed/page policy evidence in the
// snapshot before Fetchmark will admit a document.
package feedindex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/staticvar/fetchmark/internal/adapters/bleveindex"
	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/search"
)

const (
	providerID                 = "feedindex"
	defaultMaxSnapshotBytes    = int64(2 << 20)
	defaultMaxSnapshotAge      = 7 * 24 * time.Hour
	defaultMaxDocumentAge      = 400 * 24 * time.Hour
	defaultMaxSources          = 32
	defaultMaxDocuments        = 3200
	minimumPollIntervalSeconds = 300
	maximumItemsPerFetch       = 100
	maximumTopicsPerSource     = 16
	maximumTopicBytes          = 80
	maximumFutureSkew          = 5 * time.Minute
	maximumPublishFutureSkew   = 24 * time.Hour
	defaultSearchResults       = 10
	maximumSearchResults       = 100
)

var (
	validSourceID      = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
	prereleaseTitle    = regexp.MustCompile(`(?i)(alpha|beta|release candidate|preview|\d+(?:\.\d+)*[ab]\d+|\d+(?:\.\d+)*rc\d+)`)
	stableReleaseTitle = regexp.MustCompile(`(?i)(available|released|release|\bis out\b|\bfinal\b)`)
)

// Options bounds and time-binds one immutable snapshot load.
type Options struct {
	Path             string
	Now              time.Time
	MaxSnapshotBytes int64
	MaxSnapshotAge   time.Duration
	MaxDocumentAge   time.Duration
	MaxSources       int
	MaxDocuments     int
}

type snapshot struct {
	Version     int              `json:"version"`
	GeneratedAt string           `json:"generated_at"`
	Sources     []snapshotSource `json:"sources"`
}

type snapshotSource struct {
	ID                     string             `json:"id"`
	FeedURL                string             `json:"feed_url"`
	Topics                 []string           `json:"topics"`
	License                string             `json:"license"`
	LicenseURL             string             `json:"license_url"`
	FetchedAt              string             `json:"fetched_at"`
	FeedRobotsObservedAt   string             `json:"feed_robots_observed_at"`
	FeedRobotsAllowed      *bool              `json:"feed_robots_allowed"`
	FeedNoIndex            *bool              `json:"feed_noindex"`
	FeedXRobotsNoIndex     *bool              `json:"feed_x_robots_noindex"`
	MinPollIntervalSeconds int                `json:"min_poll_interval_seconds"`
	MaxItemsPerFetch       int                `json:"max_items_per_fetch"`
	Documents              []snapshotDocument `json:"documents"`
}

type snapshotDocument struct {
	URL              string `json:"url"`
	Title            string `json:"title"`
	Summary          string `json:"summary"`
	PublishedAt      string `json:"published_at"`
	ObservedAt       string `json:"observed_at"`
	RobotsObservedAt string `json:"robots_observed_at"`
	RobotsAllowed    *bool  `json:"robots_allowed"`
	NoIndex          *bool  `json:"noindex"`
	XRobotsNoIndex   *bool  `json:"x_robots_noindex"`
}

type sourceIndex struct {
	id         string
	feedURL    string
	topics     []string
	license    string
	licenseURL string
	index      *bleveindex.Index
}

// Index is an immutable set of source-specific lexical indexes. Keeping each
// feed separate makes topic routing authoritative and prevents an unrelated
// feed from matching a broad freshness query.
type Index struct {
	mu        sync.RWMutex
	sources   []*sourceIndex
	generated time.Time
	now       func() time.Time
	closed    bool
}

var _ search.Searcher = (*Index)(nil)
var _ search.BatchSearcher = (*Index)(nil)
var _ io.Closer = (*Index)(nil)

// Open strictly validates and indexes an operator-provided snapshot. Missing
// robots/noindex/license evidence is rejected rather than inferred.
func Open(options Options) (*Index, error) {
	clock := time.Now
	now := options.Now.UTC()
	if now.IsZero() {
		now = clock().UTC()
	} else {
		clock = func() time.Time { return now }
	}
	maxBytes := options.MaxSnapshotBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxSnapshotBytes
	}
	maxSnapshotAge := options.MaxSnapshotAge
	if maxSnapshotAge <= 0 {
		maxSnapshotAge = defaultMaxSnapshotAge
	}
	maxDocumentAge := options.MaxDocumentAge
	if maxDocumentAge <= 0 {
		maxDocumentAge = defaultMaxDocumentAge
	}
	maxSources := options.MaxSources
	if maxSources <= 0 {
		maxSources = defaultMaxSources
	}
	maxDocuments := options.MaxDocuments
	if maxDocuments <= 0 {
		maxDocuments = defaultMaxDocuments
	}
	if maxBytes > 64<<20 || maxSnapshotAge > 30*24*time.Hour || maxDocumentAge > 5*365*24*time.Hour || maxSources > 128 || maxDocuments > 10000 {
		return nil, errors.New("feed index: configured bounds exceed hard limits")
	}

	raw, err := secureconfigfile.Read(options.Path, secureconfigfile.Options{MaxBytes: maxBytes, Mode: secureconfigfile.PublicConfig})
	if err != nil {
		return nil, fmt.Errorf("feed index: read snapshot: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var document snapshot
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("feed index: decode snapshot: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("feed index: snapshot contains trailing data")
	}
	generatedAt, err := validateSnapshotHeader(document, now, maxSnapshotAge, maxSources)
	if err != nil {
		return nil, err
	}

	local := &Index{generated: generatedAt, now: clock}
	seenSources := make(map[string]struct{}, len(document.Sources))
	totalDocuments := 0
	for position := range document.Sources {
		source := document.Sources[position]
		validated, sourceDocuments, err := validateSource(source, generatedAt, now, maxSnapshotAge, maxDocumentAge)
		if err != nil {
			_ = local.Close()
			return nil, fmt.Errorf("feed index: source %d: %w", position+1, err)
		}
		if _, duplicate := seenSources[validated.id]; duplicate {
			_ = local.Close()
			return nil, fmt.Errorf("feed index: duplicate source id %q", validated.id)
		}
		seenSources[validated.id] = struct{}{}
		totalDocuments += len(sourceDocuments)
		if totalDocuments > maxDocuments {
			_ = local.Close()
			return nil, errors.New("feed index: document count exceeds budget")
		}
		lexical, err := bleveindex.Open(bleveindex.Options{
			InMemory: true, MaxDocumentBytes: 64 << 10, MaxDocuments: max(1, len(sourceDocuments)), MaxBytes: max(1<<20, int64(len(raw))*4),
		})
		if err != nil {
			_ = local.Close()
			return nil, fmt.Errorf("feed index: open source %q: %w", validated.id, err)
		}
		validated.index = lexical
		local.sources = append(local.sources, validated)
		for _, item := range sourceDocuments {
			if err := lexical.Reconcile(context.Background(), item); err != nil {
				_ = local.Close()
				return nil, fmt.Errorf("feed index: index source %q: %w", validated.id, err)
			}
		}
	}
	return local, nil
}

func validateSnapshotHeader(document snapshot, now time.Time, maxAge time.Duration, maxSources int) (time.Time, error) {
	if document.Version != 1 {
		return time.Time{}, fmt.Errorf("feed index: unsupported snapshot version %d", document.Version)
	}
	if len(document.Sources) == 0 || len(document.Sources) > maxSources {
		return time.Time{}, errors.New("feed index: source count is outside configured bounds")
	}
	generated, err := time.Parse(time.RFC3339, document.GeneratedAt)
	if err != nil {
		return time.Time{}, errors.New("feed index: generated_at must be RFC3339")
	}
	generated = generated.UTC()
	if generated.After(now.Add(maximumFutureSkew)) || generated.Before(now.Add(-maxAge)) {
		return time.Time{}, errors.New("feed index: snapshot is outside its freshness window")
	}
	return generated, nil
}

func validateSource(source snapshotSource, generated, now time.Time, maxSnapshotAge, maxDocumentAge time.Duration) (*sourceIndex, []localcorpus.Document, error) {
	id := strings.ToLower(strings.TrimSpace(source.ID))
	if !validSourceID.MatchString(id) {
		return nil, nil, errors.New("invalid source id")
	}
	feedURL, err := validatePublicHTTPSURL(source.FeedURL)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid feed_url: %w", err)
	}
	license := strings.TrimSpace(source.License)
	if license == "" || len(license) > 80 {
		return nil, nil, errors.New("license identifier is required")
	}
	licenseURL, err := validatePublicHTTPSURL(source.LicenseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid license_url: %w", err)
	}
	if source.FeedRobotsAllowed == nil || source.FeedNoIndex == nil || source.FeedXRobotsNoIndex == nil ||
		!*source.FeedRobotsAllowed || *source.FeedNoIndex || *source.FeedXRobotsNoIndex {
		return nil, nil, errors.New("feed lacks affirmative robots/noindex permission")
	}
	if source.MinPollIntervalSeconds < minimumPollIntervalSeconds || source.MinPollIntervalSeconds > 30*24*60*60 || source.MaxItemsPerFetch < 1 || source.MaxItemsPerFetch > maximumItemsPerFetch {
		return nil, nil, errors.New("feed polling budget is outside allowed bounds")
	}
	if len(source.Documents) > source.MaxItemsPerFetch {
		return nil, nil, errors.New("feed document count exceeds per-fetch budget")
	}
	fetchedAt, err := boundedEvidenceTime(source.FetchedAt, generated, now, maxSnapshotAge)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid fetched_at: %w", err)
	}
	if _, err := boundedEvidenceTime(source.FeedRobotsObservedAt, generated, now, maxSnapshotAge); err != nil {
		return nil, nil, fmt.Errorf("invalid feed_robots_observed_at: %w", err)
	}
	topics, err := validateTopics(source.Topics)
	if err != nil {
		return nil, nil, err
	}
	validated := &sourceIndex{id: id, feedURL: feedURL, topics: topics, license: license, licenseURL: licenseURL}
	documents := make([]localcorpus.Document, 0, len(source.Documents))
	seenURLs := make(map[string]struct{}, len(source.Documents))
	for position, item := range source.Documents {
		urlValue, err := validatePublicHTTPSURL(item.URL)
		if err != nil {
			return nil, nil, fmt.Errorf("document %d URL: %w", position+1, err)
		}
		if _, duplicate := seenURLs[urlValue]; duplicate {
			return nil, nil, fmt.Errorf("document %d repeats URL", position+1)
		}
		seenURLs[urlValue] = struct{}{}
		if strings.TrimSpace(item.Title) == "" || len(item.Title) > 1000 || len(item.Summary) > 16000 {
			return nil, nil, fmt.Errorf("document %d has invalid title or summary bounds", position+1)
		}
		if item.RobotsAllowed == nil || item.NoIndex == nil || item.XRobotsNoIndex == nil ||
			!*item.RobotsAllowed || *item.NoIndex || *item.XRobotsNoIndex {
			return nil, nil, fmt.Errorf("document %d lacks affirmative robots/noindex permission", position+1)
		}
		observedAt, err := boundedEvidenceTime(item.ObservedAt, generated, now, maxSnapshotAge)
		if err != nil {
			return nil, nil, fmt.Errorf("document %d observed_at: %w", position+1, err)
		}
		if _, err := boundedEvidenceTime(item.RobotsObservedAt, generated, now, maxSnapshotAge); err != nil {
			return nil, nil, fmt.Errorf("document %d robots_observed_at: %w", position+1, err)
		}
		publishedAt, err := time.Parse(time.RFC3339, item.PublishedAt)
		if err != nil {
			return nil, nil, fmt.Errorf("document %d published_at must be RFC3339", position+1)
		}
		publishedAt = publishedAt.UTC()
		if publishedAt.After(now.Add(maximumPublishFutureSkew)) || publishedAt.Before(now.Add(-maxDocumentAge)) {
			return nil, nil, fmt.Errorf("document %d is outside the publication freshness window", position+1)
		}
		documents = append(documents, localcorpus.Document{
			URL: urlValue, Title: strings.TrimSpace(item.Title), Body: strings.TrimSpace(item.Summary),
			PublishedAt: &publishedAt, FetchedAt: maxTime(fetchedAt, observedAt),
			Provenance: []string{"rss:" + id}, MIME: "application/feed+json",
			ExtractionStatus: "feed_metadata", SafetyClassification: localcorpus.SafetyUnclassified,
			IndexingDisposition: localcorpus.DispositionPermitted,
		})
	}
	return validated, documents, nil
}

func boundedEvidenceTime(raw string, generated, now time.Time, maxAge time.Duration) (time.Time, error) {
	value, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, errors.New("must be RFC3339")
	}
	value = value.UTC()
	if value.After(generated.Add(maximumFutureSkew)) || value.After(now.Add(maximumFutureSkew)) || value.Before(now.Add(-maxAge)) {
		return time.Time{}, errors.New("outside snapshot evidence window")
	}
	return value, nil
}

func validateTopics(raw []string) ([]string, error) {
	if len(raw) == 0 || len(raw) > maximumTopicsPerSource {
		return nil, errors.New("topics must contain a bounded non-empty list")
	}
	seen := make(map[string]struct{}, len(raw))
	out := make([]string, 0, len(raw))
	for _, value := range raw {
		value = strings.Join(strings.Fields(strings.ToLower(value)), " ")
		if len(value) < 2 || len(value) > maximumTopicBytes {
			return nil, errors.New("topic is outside length bounds")
		}
		if _, duplicate := seen[value]; duplicate {
			return nil, errors.New("duplicate topic")
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out, nil
}

func validatePublicHTTPSURL(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return "", errors.New("absolute HTTPS URL without credentials or fragment is required")
	}
	host := strings.ToLower(strings.TrimSuffix(parsed.Hostname(), "."))
	if host == "" || strings.Contains(host, "%") || host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".internal") {
		return "", errors.New("local hostnames are not allowed")
	}
	if address := net.ParseIP(host); address != nil && (address.IsLoopback() || address.IsPrivate() || address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() || address.IsUnspecified() || address.IsMulticast()) {
		return "", errors.New("non-public IP literals are not allowed")
	}
	parsed.Scheme = "https"
	parsed.Host = strings.ToLower(parsed.Host)
	if parsed.Path == "" {
		parsed.Path = "/"
	}
	if parsed.Path != "/" {
		parsed.Path = strings.TrimSuffix(parsed.Path, "/")
	}
	return parsed.String(), nil
}

// Search implements the compatibility discovery contract.
func (local *Index) Search(ctx context.Context, query search.Query) ([]search.Hit, error) {
	batch, err := local.SearchBatch(ctx, query)
	return batch.Hits, err
}

// SearchBatch routes only to feeds whose declared topic is present in the
// query, then performs local lexical retrieval and a bounded recency tie-break.
func (local *Index) SearchBatch(ctx context.Context, query search.Query) (search.SearchBatch, error) {
	started := time.Now()
	if ctx == nil {
		return failedBatch(started, errors.New("feed index: context is required"))
	}
	if err := ctx.Err(); err != nil {
		return failedBatch(started, err)
	}
	text := strings.TrimSpace(query.Q)
	if text == "" || !matchesEngineSelection(query.Engines) {
		return emptyBatch(started), nil
	}
	maximum := query.MaxResults
	if maximum <= 0 {
		maximum = defaultSearchResults
	}
	if maximum > maximumSearchResults {
		maximum = maximumSearchResults
	}

	local.mu.RLock()
	if local.closed {
		local.mu.RUnlock()
		return failedBatch(started, errors.New("feed index: closed"))
	}
	sources := append([]*sourceIndex(nil), local.sources...)
	now := local.now().UTC()
	local.mu.RUnlock()

	selected := selectedSources(text, sources)
	if len(selected) == 0 {
		return emptyBatch(started), nil
	}
	var hits []search.Hit
	for _, source := range selected {
		innerQuery := query
		innerQuery.MaxResults = maximumSearchResults
		batch, err := source.index.SearchBatch(ctx, innerQuery)
		if err != nil {
			return failedBatch(started, err)
		}
		for _, hit := range batch.Hits {
			hit.Engines = []string{providerID}
			hit.Metadata = cloneMetadata(hit.Metadata)
			hit.Metadata["provider"] = providerID
			hit.Metadata["feed_source"] = source.id
			hit.Metadata["feed_url"] = source.feedURL
			hit.Metadata["license"] = source.license
			hit.Metadata["license_url"] = source.licenseURL
			hit.Metadata["snapshot_generated_at"] = local.generated.Format(time.RFC3339)
			hit.Provenance = []model.DiscoveryProvenance{{Provider: providerID, Lane: source.id, Variant: "original"}}
			hits = append(hits, hit)
		}
	}
	sort.SliceStable(hits, func(left, right int) bool {
		leftScore := effectiveScore(text, hits[left], now)
		rightScore := effectiveScore(text, hits[right], now)
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		return hits[left].URL < hits[right].URL
	})
	hits = dedupeHits(hits, maximum)
	status := search.BatchHealthy
	if len(hits) == 0 {
		status = search.BatchAuthoritativeEmpty
	}
	return search.SearchBatch{Hits: hits, Provider: providerID, Instance: "snapshot@" + local.generated.Format(time.RFC3339), Status: status, Duration: time.Since(started)}, nil
}

func selectedSources(query string, sources []*sourceIndex) []*sourceIndex {
	normalized := normalizeText(query)
	tokens := tokenSet(normalized)
	out := make([]*sourceIndex, 0, len(sources))
	for _, source := range sources {
		for _, topic := range source.topics {
			matched := false
			if strings.Contains(topic, " ") {
				matched = strings.Contains(normalized, topic)
			} else {
				_, matched = tokens[topic]
			}
			if matched {
				out = append(out, source)
				break
			}
		}
	}
	return out
}

func normalizeText(value string) string {
	return strings.Join(strings.FieldsFunc(strings.ToLower(value), func(character rune) bool {
		return !unicode.IsLetter(character) && !unicode.IsDigit(character)
	}), " ")
}

func tokenSet(value string) map[string]struct{} {
	out := make(map[string]struct{})
	for _, token := range strings.Fields(value) {
		out[token] = struct{}{}
	}
	return out
}

func effectiveScore(query string, hit search.Hit, now time.Time) float64 {
	score, _ := strconv.ParseFloat(hit.Metadata["local_score"], 64)
	stableRelease := strings.Contains(strings.ToLower(query), "latest stable")
	if stableRelease && (prereleaseTitle.MatchString(hit.Title) || !stableReleaseTitle.MatchString(hit.Title)) {
		return score - 1
	}
	if hit.PublishedAt == nil {
		return score
	}
	age := now.Sub(hit.PublishedAt.UTC())
	if age < 0 {
		age = 0
	}
	year := 365 * 24 * time.Hour
	if age >= year {
		return score
	}
	weight := 0.1
	if stableRelease {
		weight = 0.6
	}
	return score + weight*(1-float64(age)/float64(year))
}

func dedupeHits(input []search.Hit, maximum int) []search.Hit {
	seen := make(map[string]struct{}, len(input))
	out := make([]search.Hit, 0, min(maximum, len(input)))
	for _, hit := range input {
		if _, duplicate := seen[hit.URL]; duplicate {
			continue
		}
		seen[hit.URL] = struct{}{}
		out = append(out, hit)
		if len(out) == maximum {
			break
		}
	}
	return out
}

func matchesEngineSelection(engines []string) bool {
	if len(engines) == 0 {
		return true
	}
	for _, engine := range engines {
		if strings.EqualFold(strings.TrimSpace(engine), providerID) {
			return true
		}
	}
	return false
}

func cloneMetadata(input map[string]string) map[string]string {
	out := make(map[string]string, len(input)+7)
	for key, value := range input {
		out[key] = value
	}
	return out
}

func emptyBatch(started time.Time) search.SearchBatch {
	return search.SearchBatch{Provider: providerID, Instance: providerID, Status: search.BatchAuthoritativeEmpty, Duration: time.Since(started)}
}

func failedBatch(started time.Time, err error) (search.SearchBatch, error) {
	return search.SearchBatch{
		Provider: providerID, Instance: providerID, Status: search.BatchFailed, Duration: time.Since(started),
		Diagnostics: []search.ProviderDiagnostic{{Provider: providerID, Instance: providerID, Reason: "snapshot_error"}},
	}, err
}

// Close releases all in-memory Bleve indexes. It is idempotent.
func (local *Index) Close() error {
	if local == nil {
		return nil
	}
	local.mu.Lock()
	defer local.mu.Unlock()
	if local.closed {
		return nil
	}
	local.closed = true
	var joined error
	for _, source := range local.sources {
		if source.index != nil {
			joined = errors.Join(joined, source.index.Close())
		}
	}
	return joined
}

func maxTime(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}
