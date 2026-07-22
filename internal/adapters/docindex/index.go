// Package docindex exposes an operator-built official-document metadata
// snapshot as an immutable, CPU-local discovery source. Loading and querying
// the snapshot never performs network I/O; collection and policy verification
// belong to a separate operator-controlled process.
package docindex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/bleveindex"
	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/canonicalurl"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
	"github.com/staticvar/fetchmark/internal/core/search"
)

const (
	providerID                     = "docindex"
	defaultMaxSnapshotBytes        = int64(64 << 20)
	defaultMaxSources              = 64
	defaultMaxDocuments            = 200_000
	hardMaxSnapshotBytes           = int64(128 << 20)
	hardMaxSources                 = 128
	hardMaxDocuments               = 250_000
	maximumAllowedHostsPerSource   = 16
	maximumHeadingsPerDocument     = 16
	maximumHeadingBytes            = 256
	maximumTitleBytes              = 1_000
	maximumMetadataValueBytes      = 1_024
	maximumFutureSkew              = 5 * time.Minute
	defaultMaxEvidenceAge          = 7 * 24 * time.Hour
	defaultSearchResults           = 10
	maximumSearchResults           = 100
	maximumIndexedDocumentBytes    = 32 << 10
	minimumAggregateIndexByteLimit = 1 << 20
)

var validSourceID = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
var validLanguage = regexp.MustCompile(`^[a-z]{2,3}(?:-[a-z0-9]{2,8})*$`)

// Options bounds one immutable snapshot load. Path must be a clean absolute
// path to an operator-owned regular file accepted by secureconfigfile.
type Options struct {
	Path             string
	Now              time.Time
	MaxSnapshotBytes int64
	MaxSources       int
	MaxDocuments     int
}

type snapshot struct {
	Version     int              `json:"version"`
	GeneratedAt string           `json:"generated_at"`
	Operator    snapshotOperator `json:"operator"`
	Sources     []snapshotSource `json:"sources"`
}

type snapshotOperator struct {
	Name       string `json:"name"`
	ContactURL string `json:"contact_url"`
	PolicyURL  string `json:"policy_url"`
}

type snapshotSource struct {
	ID               string             `json:"id"`
	Owner            string             `json:"owner"`
	SourceURL        string             `json:"source_url"`
	AllowedHosts     []string           `json:"allowed_hosts"`
	License          string             `json:"license"`
	LicenseURL       string             `json:"license_url"`
	PolicyURL        string             `json:"policy_url"`
	ObservedAt       string             `json:"observed_at"`
	RobotsObservedAt string             `json:"robots_observed_at"`
	RobotsAllowed    *bool              `json:"robots_allowed"`
	NoIndex          *bool              `json:"noindex"`
	XRobotsNoIndex   *bool              `json:"x_robots_noindex"`
	Documents        []snapshotDocument `json:"documents"`
}

type snapshotDocument struct {
	URL                  string   `json:"url"`
	Title                string   `json:"title"`
	Headings             []string `json:"headings"`
	Language             string   `json:"language"`
	SafetyClassification string   `json:"safety_classification"`
	ObservedAt           string   `json:"observed_at"`
	RobotsObservedAt     string   `json:"robots_observed_at"`
	RobotsAllowed        *bool    `json:"robots_allowed"`
	NoIndex              *bool    `json:"noindex"`
	XRobotsNoIndex       *bool    `json:"x_robots_noindex"`
}

type sourceMetadata struct {
	id         string
	owner      string
	sourceURL  string
	license    string
	licenseURL string
	policyURL  string
}

type documentMetadata struct {
	source *sourceMetadata
}

// Index owns one in-memory lexical projection plus immutable provenance
// metadata loaded from the same strictly validated snapshot.
type Index struct {
	mu                 sync.RWMutex
	index              *bleveindex.Index
	documents          map[string]documentMetadata
	generatedAt        time.Time
	operatorName       string
	operatorContact    string
	operatorPolicy     string
	snapshotSHA256     string
	evidenceValidUntil time.Time
	now                func() time.Time
	closed             bool
}

var _ search.Searcher = (*Index)(nil)
var _ search.BatchSearcher = (*Index)(nil)
var _ io.Closer = (*Index)(nil)

// SnapshotSHA256 returns the identity of the exact immutable bytes admitted by
// Open. Callers can bind evaluation identity to the loaded corpus without
// rereading a replaceable path.
func (local *Index) SnapshotSHA256() string {
	local.mu.RLock()
	defer local.mu.RUnlock()
	return local.snapshotSHA256
}

// Open validates every source, field, URL, and indexing-policy assertion
// before admitting any document. Unknown JSON fields and trailing data fail
// closed, as do missing robots/noindex or ownership/license evidence.
func Open(options Options) (*Index, error) {
	clock := time.Now
	now := options.Now.UTC()
	if now.IsZero() {
		now = clock().UTC()
	} else {
		clock = func() time.Time { return now }
	}
	maxSnapshotBytes := options.MaxSnapshotBytes
	if maxSnapshotBytes <= 0 {
		maxSnapshotBytes = defaultMaxSnapshotBytes
	}
	maxSources := options.MaxSources
	if maxSources <= 0 {
		maxSources = defaultMaxSources
	}
	maxDocuments := options.MaxDocuments
	if maxDocuments <= 0 {
		maxDocuments = defaultMaxDocuments
	}
	if maxSnapshotBytes > hardMaxSnapshotBytes || maxSources > hardMaxSources || maxDocuments > hardMaxDocuments {
		return nil, errors.New("official document index: configured bounds exceed hard limits")
	}

	raw, err := secureconfigfile.Read(options.Path, secureconfigfile.Options{
		MaxBytes: maxSnapshotBytes,
		Mode:     secureconfigfile.PublicConfig,
	})
	if err != nil {
		return nil, fmt.Errorf("official document index: read snapshot: %w", err)
	}
	document, err := decodeSnapshot(raw)
	if err != nil {
		return nil, err
	}
	generatedAt, operatorName, operatorContact, operatorPolicy, err := validateSnapshot(document, now, maxSources)
	if err != nil {
		return nil, err
	}

	lexical, err := bleveindex.Open(bleveindex.Options{
		InMemory:         true,
		MaxDocumentBytes: maximumIndexedDocumentBytes,
		MaxBytes:         max(minimumAggregateIndexByteLimit, int64(len(raw))*4),
		MaxDocuments:     maxDocuments,
	})
	if err != nil {
		return nil, fmt.Errorf("official document index: open lexical projection: %w", err)
	}
	local := &Index{
		index: lexical, documents: make(map[string]documentMetadata), generatedAt: generatedAt,
		operatorName: operatorName, operatorContact: operatorContact, operatorPolicy: operatorPolicy,
		snapshotSHA256: digest(raw), evidenceValidUntil: generatedAt.Add(defaultMaxEvidenceAge), now: clock,
	}
	seenSources := make(map[string]struct{}, len(document.Sources))
	totalDocuments := 0
	for position, source := range document.Sources {
		if len(source.Documents) > maxDocuments-totalDocuments {
			_ = local.Close()
			return nil, errors.New("official document index: document count exceeds budget")
		}
		metadata, documents, oldestEvidence, err := validateSource(source, generatedAt, now)
		if err != nil {
			_ = local.Close()
			return nil, fmt.Errorf("official document index: source %d: %w", position+1, err)
		}
		if _, duplicate := seenSources[metadata.id]; duplicate {
			_ = local.Close()
			return nil, fmt.Errorf("official document index: duplicate source id %q", metadata.id)
		}
		seenSources[metadata.id] = struct{}{}
		if until := oldestEvidence.Add(defaultMaxEvidenceAge); until.Before(local.evidenceValidUntil) {
			local.evidenceValidUntil = until
		}
		totalDocuments += len(documents)
		if totalDocuments > maxDocuments {
			_ = local.Close()
			return nil, errors.New("official document index: document count exceeds budget")
		}
		for _, item := range documents {
			if _, duplicate := local.documents[item.URL]; duplicate {
				_ = local.Close()
				return nil, fmt.Errorf("official document index: duplicate document URL %q", item.URL)
			}
			if err := lexical.Reconcile(context.Background(), item); err != nil {
				_ = local.Close()
				return nil, fmt.Errorf("official document index: index source %q: %w", metadata.id, err)
			}
			local.documents[item.URL] = documentMetadata{source: metadata}
		}
	}
	return local, nil
}

func decodeSnapshot(raw []byte) (snapshot, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var document snapshot
	if err := decoder.Decode(&document); err != nil {
		return snapshot{}, fmt.Errorf("official document index: decode snapshot: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return snapshot{}, errors.New("official document index: snapshot contains trailing data")
	}
	return document, nil
}

func validateSnapshot(document snapshot, now time.Time, maxSources int) (time.Time, string, string, string, error) {
	if document.Version != 1 {
		return time.Time{}, "", "", "", fmt.Errorf("official document index: unsupported snapshot version %d", document.Version)
	}
	if len(document.Sources) == 0 || len(document.Sources) > maxSources {
		return time.Time{}, "", "", "", errors.New("official document index: source count is outside configured bounds")
	}
	generatedAt, err := parseEvidenceTime(document.GeneratedAt, now)
	if err != nil {
		return time.Time{}, "", "", "", fmt.Errorf("official document index: generated_at: %w", err)
	}
	name, err := boundedRequired(document.Operator.Name, "operator name")
	if err != nil {
		return time.Time{}, "", "", "", fmt.Errorf("official document index: %w", err)
	}
	contact, err := validateCanonicalPublicURL(document.Operator.ContactURL)
	if err != nil {
		return time.Time{}, "", "", "", fmt.Errorf("official document index: operator contact_url: %w", err)
	}
	policy, err := validateCanonicalPublicURL(document.Operator.PolicyURL)
	if err != nil {
		return time.Time{}, "", "", "", fmt.Errorf("official document index: operator policy_url: %w", err)
	}
	return generatedAt, name, contact, policy, nil
}

func validateSource(source snapshotSource, generatedAt, now time.Time) (*sourceMetadata, []localcorpus.Document, time.Time, error) {
	id := strings.TrimSpace(strings.ToLower(source.ID))
	if source.ID != id || !validSourceID.MatchString(id) {
		return nil, nil, time.Time{}, errors.New("invalid source id")
	}
	owner, err := boundedRequired(source.Owner, "source owner")
	if err != nil {
		return nil, nil, time.Time{}, err
	}
	license, err := boundedRequired(source.License, "license identifier")
	if err != nil {
		return nil, nil, time.Time{}, err
	}
	sourceURL, err := validateCanonicalPublicURL(source.SourceURL)
	if err != nil {
		return nil, nil, time.Time{}, fmt.Errorf("source_url: %w", err)
	}
	licenseURL, err := validateCanonicalPublicURL(source.LicenseURL)
	if err != nil {
		return nil, nil, time.Time{}, fmt.Errorf("license_url: %w", err)
	}
	policyURL, err := validateCanonicalPublicURL(source.PolicyURL)
	if err != nil {
		return nil, nil, time.Time{}, fmt.Errorf("policy_url: %w", err)
	}
	hosts, err := validateAllowedHosts(source.AllowedHosts)
	if err != nil {
		return nil, nil, time.Time{}, err
	}
	if !hosts[urlHostname(sourceURL)] {
		return nil, nil, time.Time{}, errors.New("source_url host is not explicitly allowed")
	}
	observedAt, err := parseEvidenceTime(source.ObservedAt, now)
	if err != nil || observedAt.After(generatedAt.Add(maximumFutureSkew)) {
		return nil, nil, time.Time{}, errors.New("observed_at is invalid, stale, or after snapshot generation")
	}
	robotsObservedAt, err := parseEvidenceTime(source.RobotsObservedAt, now)
	if err != nil || robotsObservedAt.After(observedAt.Add(maximumFutureSkew)) {
		return nil, nil, time.Time{}, errors.New("robots_observed_at is invalid, stale, or after source observation")
	}
	if source.RobotsAllowed == nil || source.NoIndex == nil || source.XRobotsNoIndex == nil ||
		!*source.RobotsAllowed || *source.NoIndex || *source.XRobotsNoIndex {
		return nil, nil, time.Time{}, errors.New("source requires affirmative robots/noindex permission")
	}
	if len(source.Documents) == 0 {
		return nil, nil, time.Time{}, errors.New("documents must be non-empty")
	}
	metadata := &sourceMetadata{
		id: id, owner: owner, sourceURL: sourceURL, license: license, licenseURL: licenseURL, policyURL: policyURL,
	}
	documents := make([]localcorpus.Document, 0, len(source.Documents))
	oldestEvidence := minTime(observedAt, robotsObservedAt)
	for position, item := range source.Documents {
		validated, documentEvidence, err := validateDocument(item, hosts, observedAt, now)
		if err != nil {
			return nil, nil, time.Time{}, fmt.Errorf("document %d: %w", position+1, err)
		}
		documents = append(documents, validated)
		oldestEvidence = minTime(oldestEvidence, documentEvidence)
	}
	return metadata, documents, oldestEvidence, nil
}

func validateDocument(item snapshotDocument, allowedHosts map[string]bool, sourceObservedAt, now time.Time) (localcorpus.Document, time.Time, error) {
	canonical, err := validateCanonicalPublicURL(item.URL)
	if err != nil {
		return localcorpus.Document{}, time.Time{}, fmt.Errorf("URL: %w", err)
	}
	if !allowedHosts[urlHostname(canonical)] {
		return localcorpus.Document{}, time.Time{}, errors.New("URL host is not explicitly allowed")
	}
	title, err := boundedRequired(item.Title, "title")
	if err != nil || len(title) > maximumTitleBytes {
		return localcorpus.Document{}, time.Time{}, errors.New("title is outside allowed bounds")
	}
	headings, err := validateHeadings(item.Headings)
	if err != nil {
		return localcorpus.Document{}, time.Time{}, err
	}
	language := strings.ToLower(strings.TrimSpace(item.Language))
	if item.Language != language || !validLanguage.MatchString(language) {
		return localcorpus.Document{}, time.Time{}, errors.New("canonical language is required")
	}
	if item.SafetyClassification != string(localcorpus.SafetySafe) {
		return localcorpus.Document{}, time.Time{}, errors.New("explicit safe classification is required")
	}
	if item.RobotsAllowed == nil || item.NoIndex == nil || item.XRobotsNoIndex == nil ||
		!*item.RobotsAllowed || *item.NoIndex || *item.XRobotsNoIndex {
		return localcorpus.Document{}, time.Time{}, errors.New("affirmative robots/noindex permission is required")
	}
	observedAt, err := parseEvidenceTime(item.ObservedAt, now)
	if err != nil || observedAt.After(sourceObservedAt.Add(maximumFutureSkew)) {
		return localcorpus.Document{}, time.Time{}, errors.New("observed_at is invalid, stale, or after source observation")
	}
	robotsObservedAt, err := parseEvidenceTime(item.RobotsObservedAt, now)
	if err != nil || robotsObservedAt.After(observedAt.Add(maximumFutureSkew)) {
		return localcorpus.Document{}, time.Time{}, errors.New("robots_observed_at is invalid, stale, or after document observation")
	}
	return localcorpus.Document{
		URL: canonical, Title: title, Headings: headings, Body: strings.Join(append([]string{title}, headings...), "\n"), Language: language, FetchedAt: observedAt,
		MIME: "text/html", ExtractionStatus: "official_metadata",
		SafetyClassification: localcorpus.SafetySafe, IndexingDisposition: localcorpus.DispositionPermitted,
	}, minTime(observedAt, robotsObservedAt), nil
}

func validateHeadings(raw []string) ([]string, error) {
	if len(raw) > maximumHeadingsPerDocument {
		return nil, errors.New("headings exceed per-document budget")
	}
	headings := make([]string, 0, len(raw))
	for _, value := range raw {
		value = strings.TrimSpace(value)
		if value == "" || len(value) > maximumHeadingBytes {
			return nil, errors.New("heading is outside allowed bounds")
		}
		headings = append(headings, value)
	}
	return headings, nil
}

func validateAllowedHosts(raw []string) (map[string]bool, error) {
	if len(raw) == 0 || len(raw) > maximumAllowedHostsPerSource {
		return nil, errors.New("allowed_hosts must be a bounded non-empty list")
	}
	hosts := make(map[string]bool, len(raw))
	for _, value := range raw {
		host := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(value), "."))
		if host == "" || value != host || !isPublicHost(host) {
			return nil, errors.New("allowed_hosts contains an invalid public hostname")
		}
		if hosts[host] {
			return nil, errors.New("allowed_hosts contains a duplicate")
		}
		hosts[host] = true
	}
	return hosts, nil
}

func validateCanonicalPublicURL(raw string) (string, error) {
	if strings.TrimSpace(raw) != raw {
		return "", errors.New("clean canonical URL is required")
	}
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil {
		return "", errors.New("absolute HTTP(S) URL without userinfo is required")
	}
	if !isPublicHost(parsed.Hostname()) {
		return "", errors.New("public hostname is required")
	}
	canonical, err := canonicalurl.V1(raw)
	if err != nil || canonical != raw {
		return "", errors.New("URL must already use canonical URL v1 form")
	}
	return canonical, nil
}

func isPublicHost(raw string) bool {
	host := strings.ToLower(strings.TrimSuffix(raw, "."))
	if host == "" || strings.Contains(host, "%") || host == "localhost" || strings.HasSuffix(host, ".localhost") ||
		strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".internal") || host == "example" ||
		strings.HasSuffix(host, ".example") || host == "invalid" || strings.HasSuffix(host, ".invalid") ||
		host == "test" || strings.HasSuffix(host, ".test") || host == "onion" || strings.HasSuffix(host, ".onion") ||
		host == "home.arpa" || strings.HasSuffix(host, ".home.arpa") {
		return false
	}
	if address := net.ParseIP(host); address != nil {
		return !(address.IsLoopback() || address.IsPrivate() || address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() ||
			address.IsUnspecified() || address.IsMulticast())
	}
	if len(host) > 253 {
		return false
	}
	if !strings.Contains(host, ".") || looksLikeAlternateNumericHost(host) {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
				return false
			}
		}
	}
	return true
}

func looksLikeAlternateNumericHost(host string) bool {
	for _, label := range strings.Split(host, ".") {
		if label == "" {
			return false
		}
		candidate := label
		if strings.HasPrefix(candidate, "0x") {
			candidate = strings.TrimPrefix(candidate, "0x")
			if candidate == "" {
				return false
			}
			for _, character := range candidate {
				if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
					return false
				}
			}
			continue
		}
		for _, character := range candidate {
			if character < '0' || character > '9' {
				return false
			}
		}
	}
	return true
}

func parseEvidenceTime(raw string, now time.Time) (time.Time, error) {
	value, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, errors.New("must be RFC3339")
	}
	value = value.UTC()
	if value.After(now.Add(maximumFutureSkew)) || value.Before(now.Add(-defaultMaxEvidenceAge)) {
		return time.Time{}, errors.New("must be within the evidence freshness window")
	}
	return value, nil
}

func minTime(left, right time.Time) time.Time {
	if right.Before(left) {
		return right
	}
	return left
}

func boundedRequired(raw, name string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" || value != raw || len(value) > maximumMetadataValueBytes {
		return "", fmt.Errorf("%s is required and bounded", name)
	}
	return value, nil
}

func urlHostname(raw string) string {
	parsed, _ := url.Parse(raw)
	return strings.ToLower(strings.TrimSuffix(parsed.Hostname(), "."))
}

// Search implements the compatibility discovery contract.
func (local *Index) Search(ctx context.Context, query search.Query) ([]search.Hit, error) {
	batch, err := local.SearchBatch(ctx, query)
	return batch.Hits, err
}

// SearchBatch searches the immutable in-memory lexical projection. Empty
// results are degraded-empty because this bounded corpus cannot authoritatively
// assert that no relevant official document exists outside the snapshot.
func (local *Index) SearchBatch(ctx context.Context, query search.Query) (search.SearchBatch, error) {
	started := time.Now()
	if ctx == nil {
		return failedBatch(started, errors.New("official document index: context is required"))
	}
	if err := ctx.Err(); err != nil {
		return failedBatch(started, err)
	}
	if !matchesEngineSelection(query.Engines) || strings.TrimSpace(query.Q) == "" {
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
		return failedBatch(started, errors.New("official document index: closed"))
	}
	lexical := local.index
	documents := local.documents
	generatedAt := local.generatedAt
	operatorName := local.operatorName
	operatorContact := local.operatorContact
	operatorPolicy := local.operatorPolicy
	snapshotSHA256 := local.snapshotSHA256
	evidenceValidUntil := local.evidenceValidUntil
	clock := local.now
	local.mu.RUnlock()
	if clock().UTC().After(evidenceValidUntil) {
		return staleBatch(started), nil
	}

	inner := query
	inner.MaxResults = maximum
	batch, err := lexical.SearchBatch(ctx, inner)
	if err != nil {
		return failedBatch(started, err)
	}
	hits := make([]search.Hit, 0, len(batch.Hits))
	for _, hit := range batch.Hits {
		metadata, found := documents[hit.URL]
		if !found {
			continue
		}
		hit.Engines = []string{providerID}
		hit.Metadata = cloneMetadata(hit.Metadata)
		hit.Metadata["provider"] = providerID
		hit.Metadata["document_source"] = metadata.source.id
		hit.Metadata["source_owner"] = metadata.source.owner
		hit.Metadata["source_url"] = metadata.source.sourceURL
		hit.Metadata["license"] = metadata.source.license
		hit.Metadata["license_url"] = metadata.source.licenseURL
		hit.Metadata["policy_url"] = metadata.source.policyURL
		hit.Metadata["operator"] = operatorName
		hit.Metadata["operator_contact_url"] = operatorContact
		hit.Metadata["operator_policy_url"] = operatorPolicy
		hit.Metadata["snapshot_generated_at"] = generatedAt.Format(time.RFC3339)
		hit.Metadata["snapshot_sha256"] = snapshotSHA256
		delete(hit.Metadata, "provenance")
		hit.Provenance = nil
		hits = append(hits, hit)
	}
	if len(hits) == 0 {
		return emptyBatch(started), nil
	}
	return search.SearchBatch{
		Hits: hits, Provider: providerID, Instance: "snapshot@" + snapshotSHA256[:16],
		Status: search.BatchPartial, Duration: time.Since(started),
	}, nil
}

func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
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
	out := make(map[string]string, len(input)+11)
	for key, value := range input {
		out[key] = value
	}
	return out
}

func emptyBatch(started time.Time) search.SearchBatch {
	return search.SearchBatch{
		Provider: providerID, Instance: providerID, Status: search.BatchDegradedEmpty, Duration: time.Since(started),
		Diagnostics: []search.ProviderDiagnostic{{Provider: providerID, Instance: providerID, Reason: "bounded_snapshot_no_match"}},
	}
}

func staleBatch(started time.Time) search.SearchBatch {
	return search.SearchBatch{
		Provider: providerID, Instance: providerID, Status: search.BatchDegradedEmpty, Duration: time.Since(started),
		Diagnostics: []search.ProviderDiagnostic{{Provider: providerID, Instance: providerID, Reason: "stale_policy_evidence"}},
	}
}

func failedBatch(started time.Time, err error) (search.SearchBatch, error) {
	return search.SearchBatch{
		Provider: providerID, Instance: providerID, Status: search.BatchFailed, Duration: time.Since(started),
		Diagnostics: []search.ProviderDiagnostic{{Provider: providerID, Instance: providerID, Reason: "snapshot_error"}},
	}, err
}

// Close releases the in-memory lexical projection. It is idempotent.
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
	if local.index == nil {
		return nil
	}
	return local.index.Close()
}
