package openpackindex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	blevequery "github.com/blevesearch/bleve/v2/search/query"

	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/search"
)

const (
	defaultSearchResults = 10
	maxSearchResults     = 100
	maxFilterTokens      = 1024
)

type projectionMarker struct {
	MarkerKind     string `json:"marker_kind"`
	SchemaVersion  string `json:"schema_version"`
	ManifestSHA256 string `json:"manifest_sha256"`
	PackID         string `json:"pack_id"`
	Revision       string `json:"revision"`
	RecordCount    string `json:"record_count"`
	SigningKeyID   string `json:"signing_key_id"`
	CreatedAt      string `json:"created_at"`
	ExpiresAt      string `json:"expires_at"`
}

type storedDocument struct {
	URL            string     `json:"url"`
	HostTokens     []string   `json:"host_tokens"`
	PathTokens     []string   `json:"path_tokens"`
	URLTerms       []string   `json:"url_terms"`
	Title          string     `json:"title"`
	Headings       []string   `json:"headings"`
	AnchorTerms    []string   `json:"anchor_terms"`
	SalientSketch  string     `json:"salient_sketch"`
	Language       string     `json:"language"`
	PublishedAt    *time.Time `json:"published_at,omitempty"`
	FetchedAt      time.Time  `json:"fetched_at"`
	ContentSHA256  string     `json:"content_sha256"`
	AuthorityScore string     `json:"authority_score"`
	FreshnessScore string     `json:"freshness_score"`
	Provenance     string     `json:"provenance"`
}

type OpenOptions struct {
	Path                   string
	ExpectedManifestSHA256 string
	ExpectedPackID         string
	ExpectedRevision       uint64
	ExpectedRecordCount    uint64
	ExpectedKeyID          string
	ExpectedCreatedAt      string
	ExpectedExpiresAt      string
	Now                    time.Time
	ProviderID             string
}

type Index struct {
	mu             sync.RWMutex
	index          bleve.Index
	providerID     string
	instanceID     string
	manifestDigest string
	packID         string
	revision       uint64
	createdAt      time.Time
	expiresAt      time.Time
	now            func() time.Time
	closed         bool
}

var _ search.Searcher = (*Index)(nil)
var _ search.BatchSearcher = (*Index)(nil)

func Open(options OpenOptions) (*Index, error) {
	return open(options, MaxInstallRecords)
}

func open(options OpenOptions, maxRecords uint64) (*Index, error) {
	if maxRecords == 0 || maxRecords > indexpack.MaxPackRecords {
		return nil, fmt.Errorf("%w: invalid record policy maximum", ErrSchemaMismatch)
	}
	path, err := requireSecureExistingDirectory(options.Path)
	if err != nil {
		return nil, fmt.Errorf("open pack index: projection path: %w", err)
	}
	if !validDigest(options.ExpectedManifestSHA256) || filepath.Base(path) != options.ExpectedManifestSHA256 {
		return nil, fmt.Errorf("%w: projection path is not the expected manifest digest", ErrSchemaMismatch)
	}
	if strings.TrimSpace(options.ExpectedPackID) == "" || options.ExpectedRevision == 0 || options.ExpectedRecordCount == 0 ||
		!validDigest(options.ExpectedKeyID) || strings.TrimSpace(options.ExpectedCreatedAt) == "" ||
		strings.TrimSpace(options.ExpectedExpiresAt) == "" || options.Now.IsZero() {
		return nil, fmt.Errorf("%w: expected pack ID, revision, record count, key ID, validity window, and current time are required", ErrSchemaMismatch)
	}
	indexPath := filepath.Join(path, projectionDirectoryName)
	if _, err := requireSecureExistingDirectory(indexPath); err != nil {
		return nil, fmt.Errorf("open pack index: index path: %w", err)
	}
	projection, err := bleve.Open(indexPath)
	if err != nil {
		return nil, fmt.Errorf("open pack index: open projection: %w", err)
	}
	marker, err := readAndVerifyMarkerBound(projection, options, maxRecords)
	if err != nil {
		_ = projection.Close()
		return nil, err
	}
	providerID, err := cleanProviderID(options.ProviderID, marker.PackID)
	if err != nil {
		_ = projection.Close()
		return nil, err
	}
	return &Index{
		index: projection, providerID: providerID,
		instanceID:     marker.PackID + "@" + marker.Revision,
		manifestDigest: marker.ManifestSHA256, packID: marker.PackID, revision: options.ExpectedRevision,
		createdAt: mustParseMarkerTime(marker.CreatedAt), expiresAt: mustParseMarkerTime(marker.ExpiresAt),
		now: time.Now,
	}, nil
}

func mustParseMarkerTime(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		panic("validated open-pack marker timestamp did not parse: " + err.Error())
	}
	return parsed
}

func readAndVerifyMarker(projection bleve.Index, options OpenOptions) (projectionMarker, error) {
	return readAndVerifyMarkerBound(projection, options, MaxInstallRecords)
}

func readAndVerifyMarkerBound(projection bleve.Index, options OpenOptions, maxRecords uint64) (projectionMarker, error) {
	if maxRecords == 0 || maxRecords > indexpack.MaxPackRecords {
		return projectionMarker{}, fmt.Errorf("%w: invalid record policy maximum", ErrSchemaMismatch)
	}
	request := bleve.NewSearchRequestOptions(bleve.NewDocIDQuery([]string{markerDocumentID}), 1, 0, false)
	request.Fields = []string{
		"marker_kind", "schema_version", "manifest_sha256", "pack_id", "revision", "record_count",
		"signing_key_id", "created_at", "expires_at",
	}
	result, err := projection.Search(request)
	if err != nil || len(result.Hits) != 1 {
		return projectionMarker{}, fmt.Errorf("%w: marker missing", ErrSchemaMismatch)
	}
	fields := result.Hits[0].Fields
	marker := projectionMarker{
		MarkerKind: stringField(fields["marker_kind"]), SchemaVersion: stringField(fields["schema_version"]),
		ManifestSHA256: stringField(fields["manifest_sha256"]), PackID: stringField(fields["pack_id"]),
		Revision: stringField(fields["revision"]), RecordCount: stringField(fields["record_count"]),
		SigningKeyID: stringField(fields["signing_key_id"]), CreatedAt: stringField(fields["created_at"]),
		ExpiresAt: stringField(fields["expires_at"]),
	}
	expectedRevision := strconv.FormatUint(options.ExpectedRevision, 10)
	if marker.MarkerKind != "open-pack-projection" || !supportedProjectionSchema(marker.SchemaVersion) ||
		marker.ManifestSHA256 != options.ExpectedManifestSHA256 || marker.PackID != options.ExpectedPackID ||
		marker.Revision != expectedRevision || marker.SigningKeyID != options.ExpectedKeyID ||
		marker.CreatedAt != options.ExpectedCreatedAt || marker.ExpiresAt != options.ExpectedExpiresAt {
		return projectionMarker{}, fmt.Errorf("%w: marker identity differs from expectations", ErrSchemaMismatch)
	}
	createdAt, createdErr := time.Parse(time.RFC3339, marker.CreatedAt)
	expiresAt, expiresErr := time.Parse(time.RFC3339, marker.ExpiresAt)
	now := options.Now.UTC()
	if createdErr != nil || expiresErr != nil || now.Before(createdAt) || !now.Before(expiresAt) {
		return projectionMarker{}, fmt.Errorf("%w: projection manifest is outside its validity window", ErrSchemaMismatch)
	}
	recordCount, err := strconv.ParseUint(marker.RecordCount, 10, 64)
	if err != nil || recordCount == 0 || recordCount > maxRecords || recordCount != options.ExpectedRecordCount {
		return projectionMarker{}, fmt.Errorf("%w: invalid marker record count", ErrSchemaMismatch)
	}
	count, err := projection.DocCount()
	if err != nil || count != recordCount+1 {
		return projectionMarker{}, fmt.Errorf("%w: marker count does not match projection", ErrSchemaMismatch)
	}
	return marker, nil
}

func projectionMapping() mapping.IndexMapping {
	indexMapping := bleve.NewIndexMapping()
	indexMapping.IndexDynamic = false
	indexMapping.StoreDynamic = false
	indexMapping.DocValuesDynamic = false
	indexMapping.ScoringModel = "bm25"
	document := mapping.NewDocumentMapping()
	document.Dynamic = false
	addKeyword := func(name string, store bool) {
		field := mapping.NewKeywordFieldMapping()
		field.Store = store
		field.IncludeInAll = false
		field.DocValues = false
		document.AddFieldMappingsAt(name, field)
	}
	addText := func(name string, store, includeInAll bool) {
		field := mapping.NewTextFieldMapping()
		field.Store = store
		field.IncludeInAll = includeInAll
		field.DocValues = false
		document.AddFieldMappingsAt(name, field)
	}
	addDate := func(name string) {
		field := mapping.NewDateTimeFieldMapping()
		field.Store = true
		field.IncludeInAll = false
		field.DocValues = false
		document.AddFieldMappingsAt(name, field)
	}
	addKeyword("url", true)
	addKeyword("host_tokens", false)
	addKeyword("path_tokens", false)
	addText("url_terms", false, true)
	addText("title", true, true)
	addText("headings", false, true)
	addText("anchor_terms", false, true)
	addText("salient_sketch", true, true)
	addKeyword("language", true)
	addDate("published_at")
	addDate("fetched_at")
	addKeyword("content_sha256", true)
	addKeyword("authority_score", true)
	addKeyword("freshness_score", true)
	addKeyword("provenance", true)
	addKeyword("marker_kind", true)
	addKeyword("schema_version", true)
	addKeyword("manifest_sha256", true)
	addKeyword("pack_id", true)
	addKeyword("revision", true)
	addKeyword("record_count", true)
	addKeyword("signing_key_id", true)
	addKeyword("created_at", true)
	addKeyword("expires_at", true)
	indexMapping.DefaultMapping = document
	return indexMapping
}

func makeStoredDocument(record indexpack.Record) (storedDocument, error) {
	parsed, err := url.Parse(record.URL)
	if err != nil {
		return storedDocument{}, fmt.Errorf("parse verified URL: %w", err)
	}
	fetched, err := time.Parse(time.RFC3339, record.FetchedAt)
	if err != nil {
		return storedDocument{}, err
	}
	provenance, err := json.Marshal(record.Provenance)
	if err != nil {
		return storedDocument{}, err
	}
	document := storedDocument{
		URL: record.URL, HostTokens: hostTokens(parsed.Hostname()), PathTokens: pathTokens(parsed.Hostname(), parsed.EscapedPath()),
		URLTerms: lexicalURLTerms(parsed.Hostname(), parsed.EscapedPath()),
		Title:    record.Title, Headings: record.Headings, AnchorTerms: record.AnchorTerms, SalientSketch: record.SalientSketch,
		Language: record.Language, FetchedAt: fetched, ContentSHA256: record.ContentSHA256,
		AuthorityScore: strconv.FormatUint(uint64(record.AuthorityScore), 10),
		FreshnessScore: strconv.FormatUint(uint64(record.FreshnessScore), 10), Provenance: string(provenance),
	}
	if record.PublishedAt != "" {
		published, err := time.Parse(time.RFC3339, record.PublishedAt)
		if err != nil {
			return storedDocument{}, err
		}
		document.PublishedAt = &published
	}
	return document, nil
}

func supportedProjectionSchema(version string) bool {
	return version == "1" || version == projectionSchemaVersion
}

func lexicalURLTerms(host, escapedPath string) []string {
	const maximumTerms = 128
	seen := make(map[string]struct{})
	terms := make([]string, 0, 16)
	add := func(value string) {
		for _, term := range strings.FieldsFunc(strings.ToLower(value), func(character rune) bool {
			return !((character >= 'a' && character <= 'z') || (character >= '0' && character <= '9'))
		}) {
			if len(term) < 2 || len(term) > 64 || len(terms) >= maximumTerms {
				continue
			}
			if _, duplicate := seen[term]; duplicate {
				continue
			}
			seen[term] = struct{}{}
			terms = append(terms, term)
		}
	}
	add(host)
	if decoded, err := url.PathUnescape(escapedPath); err == nil {
		add(decoded)
	}
	return terms
}

func (local *Index) Search(ctx context.Context, query search.Query) ([]search.Hit, error) {
	batch, err := local.SearchBatch(ctx, query)
	return batch.Hits, err
}

func (local *Index) SearchBatch(ctx context.Context, query search.Query) (search.SearchBatch, error) {
	started := time.Now()
	if ctx == nil {
		return local.failedBatch(started, errors.New("open pack index: context is required"))
	}
	if err := ctx.Err(); err != nil {
		return local.failedBatch(started, err)
	}
	local.mu.RLock()
	closed := local.closed
	local.mu.RUnlock()
	if closed {
		return local.failedBatch(started, ErrClosed)
	}
	now := local.now().UTC()
	if now.Before(local.createdAt) || !now.Before(local.expiresAt) {
		return local.failedBatch(started, ErrOutsideValidity)
	}
	if query.SafeSearch != nil && *query.SafeSearch > 0 {
		return local.emptyBatch(started), nil
	}
	if !local.matchesEngineSelection(query.Engines) {
		return local.emptyBatch(started), nil
	}
	text := strings.TrimSpace(query.Q)
	if text == "" {
		return local.emptyBatch(started), nil
	}
	maximum := query.MaxResults
	if maximum <= 0 {
		maximum = defaultSearchResults
	}
	if maximum > maxSearchResults {
		maximum = maxSearchResults
	}
	var lexical blevequery.Query
	if query.ExactMatch {
		lexical = bleve.NewMatchPhraseQuery(text)
	} else {
		lexical = bleve.NewMatchQuery(text)
	}
	lexical = applyFilters(lexical, query)
	request := bleve.NewSearchRequestOptions(lexical, maximum, 0, false)
	request.Fields = []string{
		"url", "title", "salient_sketch", "language", "published_at", "fetched_at", "content_sha256",
		"authority_score", "freshness_score", "provenance",
	}

	local.mu.RLock()
	defer local.mu.RUnlock()
	if local.closed {
		return local.failedBatch(started, ErrClosed)
	}
	result, err := local.index.SearchInContext(ctx, request)
	if err != nil {
		return local.failedBatch(started, err)
	}
	hits := make([]search.Hit, 0, min(maximum, len(result.Hits)))
	for _, resultHit := range result.Hits {
		hit, ok := local.mapHit(resultHit.Fields, resultHit.Score)
		if !ok || !matchesFilters(hit, query) {
			continue
		}
		hits = append(hits, hit)
	}
	status := search.BatchHealthy
	if len(hits) == 0 {
		status = search.BatchAuthoritativeEmpty
	}
	return search.SearchBatch{
		Hits: hits, Provider: local.providerID, Instance: local.instanceID,
		Status: status, Duration: time.Since(started),
	}, nil
}

func (local *Index) emptyBatch(started time.Time) search.SearchBatch {
	return search.SearchBatch{Provider: local.providerID, Instance: local.instanceID, Status: search.BatchAuthoritativeEmpty, Duration: time.Since(started)}
}

func (local *Index) failedBatch(started time.Time, err error) (search.SearchBatch, error) {
	reason := "index_error"
	if errors.Is(err, ErrOutsideValidity) {
		reason = "outside_validity"
	}
	return search.SearchBatch{
		Provider: local.providerID, Instance: local.instanceID, Status: search.BatchFailed, Duration: time.Since(started),
		Diagnostics: []search.ProviderDiagnostic{{Provider: local.providerID, Instance: local.instanceID, Source: local.packID, Reason: reason}},
	}, err
}

func (local *Index) matchesEngineSelection(engines []string) bool {
	if len(engines) == 0 {
		return true
	}
	for _, engine := range engines {
		if strings.EqualFold(strings.TrimSpace(engine), local.providerID) || strings.EqualFold(strings.TrimSpace(engine), "open-pack") {
			return true
		}
	}
	return false
}

func applyFilters(textQuery blevequery.Query, query search.Query) blevequery.Query {
	filters := make([]blevequery.Query, 0, 3)
	mustNot := make([]blevequery.Query, 0, 1)
	if include := domainQueries(query.IncludeDomains); len(include) == 1 {
		filters = append(filters, include[0])
	} else if len(include) > 1 {
		filters = append(filters, blevequery.NewDisjunctionQuery(include))
	}
	if exclude := domainQueries(query.ExcludeDomains); len(exclude) == 1 {
		mustNot = append(mustNot, exclude[0])
	} else if len(exclude) > 1 {
		mustNot = append(mustNot, blevequery.NewDisjunctionQuery(exclude))
	}
	if language := strings.ToLower(strings.TrimSpace(query.Language)); language != "" && language != "auto" {
		languageQuery := blevequery.NewTermQuery(language)
		languageQuery.SetField("language")
		filters = append(filters, languageQuery)
	}
	if cutoff := timeRangeCutoff(query.TimeRange); !cutoff.IsZero() {
		dateQuery := blevequery.NewDateRangeQuery(cutoff, time.Now().UTC().Add(time.Minute))
		dateQuery.SetField("published_at")
		filters = append(filters, dateQuery)
	}
	boolean := blevequery.NewBooleanQuery([]blevequery.Query{textQuery}, nil, mustNot)
	if len(filters) > 0 {
		boolean.Filter = blevequery.NewConjunctionQuery(filters)
	}
	return boolean
}

func domainQueries(filters []string) []blevequery.Query {
	queries := make([]blevequery.Query, 0, len(filters))
	for _, filter := range filters {
		parsed, ok := parseDomainFilter(filter)
		if !ok {
			continue
		}
		host := strings.TrimPrefix(parsed.Hostname(), "*.")
		host = strings.TrimPrefix(host, "www.")
		path := strings.TrimRight(parsed.EscapedPath(), "/")
		term, field := host, "host_tokens"
		if path != "" {
			term, field = host+path, "path_tokens"
		}
		termQuery := blevequery.NewTermQuery(term)
		termQuery.SetField(field)
		queries = append(queries, termQuery)
	}
	return queries
}

func parseDomainFilter(value string) (*url.URL, bool) {
	value = strings.ToLower(strings.TrimSpace(value))
	if !strings.Contains(value, "://") {
		value = "https://" + value
	}
	parsed, err := url.Parse(value)
	return parsed, err == nil && parsed.Hostname() != ""
}

func hostTokens(host string) []string {
	host = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(host)), "www.")
	if host == "" {
		return nil
	}
	parts := strings.Split(host, ".")
	if len(parts) < 2 || net.ParseIP(host) != nil {
		return []string{host}
	}
	tokens := make([]string, 0, len(parts)-1)
	for index := 0; index < min(len(parts)-1, 32); index++ {
		tokens = append(tokens, strings.Join(parts[index:], "."))
	}
	return tokens
}

func pathTokens(host, escapedPath string) []string {
	hosts := hostTokens(host)
	segments := strings.Split(strings.Trim(escapedPath, "/"), "/")
	if len(segments) == 0 || segments[0] == "" {
		return nil
	}
	tokens := make([]string, 0, min(len(hosts)*len(segments), maxFilterTokens))
	path := ""
	for _, segment := range segments {
		if segment == "" {
			continue
		}
		path += "/" + segment
		for _, host := range hosts {
			tokens = append(tokens, host+path)
			if len(tokens) == maxFilterTokens {
				return tokens
			}
		}
	}
	return tokens
}

func (local *Index) mapHit(fields map[string]any, score float64) (search.Hit, bool) {
	rawURL := stringField(fields["url"])
	if rawURL == "" {
		return search.Hit{}, false
	}
	hit := search.Hit{
		URL: rawURL, Title: stringField(fields["title"]), Snippet: stringField(fields["salient_sketch"]),
		Engines: []string{local.providerID}, Metadata: map[string]string{
			"provider": local.providerID, "source_id": local.providerID, "instance": local.instanceID, "pack_id": local.packID,
			"pack_revision": strconv.FormatUint(local.revision, 10), "manifest_sha256": local.manifestDigest,
			"lexical_score": strconv.FormatFloat(score, 'f', 6, 64), "language": stringField(fields["language"]),
			"fetched_at": stringField(fields["fetched_at"]), "content_sha256": stringField(fields["content_sha256"]),
			"authority_score": stringField(fields["authority_score"]), "freshness_score": stringField(fields["freshness_score"]),
			"provenance": stringField(fields["provenance"]),
		},
	}
	if published, err := time.Parse(time.RFC3339, stringField(fields["published_at"])); err == nil {
		hit.PublishedAt = &published
	}
	return hit, true
}

func matchesFilters(hit search.Hit, query search.Query) bool {
	parsed, err := url.Parse(hit.URL)
	if err != nil {
		return false
	}
	if len(query.IncludeDomains) > 0 && !matchesAnyDomain(parsed, query.IncludeDomains) {
		return false
	}
	if matchesAnyDomain(parsed, query.ExcludeDomains) {
		return false
	}
	if language := strings.ToLower(strings.TrimSpace(query.Language)); language != "" && language != "auto" && language != hit.Metadata["language"] {
		return false
	}
	if cutoff := timeRangeCutoff(query.TimeRange); !cutoff.IsZero() && (hit.PublishedAt == nil || hit.PublishedAt.Before(cutoff)) {
		return false
	}
	return true
}

func matchesAnyDomain(resultURL *url.URL, filters []string) bool {
	host := strings.TrimPrefix(strings.ToLower(resultURL.Hostname()), "www.")
	path := strings.TrimRight(resultURL.EscapedPath(), "/")
	for _, filter := range filters {
		parsed, ok := parseDomainFilter(filter)
		if !ok {
			continue
		}
		filterHost := strings.TrimPrefix(parsed.Hostname(), "*.")
		filterHost = strings.TrimPrefix(filterHost, "www.")
		if host != filterHost && !strings.HasSuffix(host, "."+filterHost) {
			continue
		}
		filterPath := strings.TrimRight(parsed.EscapedPath(), "/")
		if filterPath == "" || path == filterPath || strings.HasPrefix(path, filterPath+"/") {
			return true
		}
	}
	return false
}

func timeRangeCutoff(value string) time.Time {
	now := time.Now().UTC()
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "day":
		return now.Add(-24 * time.Hour)
	case "month":
		return now.AddDate(0, -1, 0)
	case "year":
		return now.AddDate(-1, 0, 0)
	default:
		return time.Time{}
	}
}

func documentID(canonicalURL string) string {
	digest := sha256.Sum256([]byte(canonicalURL))
	return hex.EncodeToString(digest[:])
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}

func stringField(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []string:
		if len(typed) > 0 {
			return typed[0]
		}
	case []any:
		if len(typed) > 0 {
			return stringField(typed[0])
		}
	}
	return ""
}

func (local *Index) Close() error {
	local.mu.Lock()
	defer local.mu.Unlock()
	if local.closed {
		return nil
	}
	local.closed = true
	return local.index.Close()
}
