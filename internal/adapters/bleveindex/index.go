// Package bleveindex implements an embedded CPU-only lexical discovery index.
// It stores only documents with an explicit permitted disposition and also
// implements the existing search ports so broker wiring can remain adapter
// agnostic.
package bleveindex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	blevequery "github.com/blevesearch/bleve/v2/search/query"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
	"github.com/staticvar/fetchmark/internal/core/search"
)

const (
	providerID             = "local-index"
	schemaDocumentID       = "__fetchmark_schema__"
	currentSchemaVersion   = "3"
	defaultMaxDocumentSize = 2 << 20
	defaultMaxIndexBytes   = 1 << 30
	defaultMaxDocuments    = 100_000
	defaultSearchResults   = 10
	maxSearchResults       = 100
	maxDocumentListItems   = 4096
	maxDerivedFilterTokens = 1024
)

var (
	ErrNotPermitted        = errors.New("local index: document is not explicitly permitted")
	ErrStaleObservation    = errors.New("local index: observation is older than retained state")
	ErrSchemaMismatch      = errors.New("local index: incompatible schema")
	ErrIndexBudgetExceeded = errors.New("local index: aggregate budget exceeded")
	ErrClosed              = errors.New("local index: closed")
)

// Options configures an embedded index. InMemory is intended for ephemeral
// mode and tests; otherwise Path must name a missing or valid Bleve directory.
type Options struct {
	Path             string
	InMemory         bool
	MaxDocumentBytes int
	MaxBytes         int64
	MaxDocuments     int
}

// Index is safe for concurrent searches and writes. The mutex only protects
// Close from racing an operation; Bleve owns its internal concurrency.
type Index struct {
	mu               sync.RWMutex
	index            bleve.Index
	maxDocumentBytes int
	maxBytes         int64
	maxDocuments     int
	usedBytes        int64
	documents        int
	closed           bool
}

type storedDocument struct {
	URL                  string     `json:"url"`
	HostTokens           []string   `json:"host_tokens"`
	PathTokens           []string   `json:"path_tokens"`
	Title                string     `json:"title"`
	Headings             []string   `json:"headings"`
	Body                 string     `json:"body"`
	Snippet              string     `json:"snippet"`
	Language             string     `json:"language"`
	Author               string     `json:"author"`
	PublishedAt          *time.Time `json:"published_at,omitempty"`
	FetchedAt            time.Time  `json:"fetched_at"`
	ExpiresAt            *time.Time `json:"expires_at,omitempty"`
	ExpiryClass          string     `json:"expiry_class,omitempty"`
	ContentHash          string     `json:"content_hash"`
	OutboundLinks        []string   `json:"outbound_links"`
	Provenance           []string   `json:"provenance"`
	MIME                 string     `json:"mime"`
	ExtractionStatus     string     `json:"extraction_status"`
	SafetyClassification string     `json:"safety_classification"`
	IndexingDisposition  string     `json:"indexing_disposition"`
	SchemaVersion        string     `json:"schema_version,omitempty"`
	LogicalBytes         int64      `json:"logical_bytes,omitempty"`
}

// Open creates or opens an index without deleting or repairing existing data.
func Open(options Options) (*Index, error) {
	maxDocumentBytes := options.MaxDocumentBytes
	if maxDocumentBytes <= 0 {
		maxDocumentBytes = defaultMaxDocumentSize
	}
	maxBytes := options.MaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxIndexBytes
	}
	maxDocuments := options.MaxDocuments
	if maxDocuments <= 0 {
		maxDocuments = defaultMaxDocuments
	}
	if int64(maxDocumentBytes) > maxBytes {
		return nil, errors.New("local index: per-document budget exceeds aggregate budget")
	}
	var (
		index   bleve.Index
		err     error
		created bool
	)
	if options.InMemory {
		if strings.TrimSpace(options.Path) != "" {
			return nil, errors.New("local index: path and in-memory mode are mutually exclusive")
		}
		index, err = bleve.NewMemOnly(indexMapping())
		created = err == nil
	} else {
		path := filepath.Clean(strings.TrimSpace(options.Path))
		if path == "." || !filepath.IsAbs(path) {
			return nil, errors.New("local index: absolute path is required")
		}
		if err := rejectSymlinkComponents(path); err != nil {
			return nil, err
		}
		path, err = resolvePersistentPath(path)
		if err != nil {
			return nil, err
		}
		info, statErr := os.Lstat(path)
		switch {
		case statErr == nil:
			if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
				return nil, errors.New("local index: path exists and is not a directory")
			}
			index, err = bleve.Open(path)
		case errors.Is(statErr, os.ErrNotExist):
			if mkdirErr := os.MkdirAll(filepath.Dir(path), 0o700); mkdirErr != nil {
				return nil, fmt.Errorf("local index: create parent: %w", mkdirErr)
			}
			index, err = bleve.New(path, indexMapping())
			created = err == nil
		default:
			return nil, fmt.Errorf("local index: inspect path: %w", statErr)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("local index: open: %w", err)
	}
	if created {
		if err := index.Index(schemaDocumentID, storedDocument{SchemaVersion: currentSchemaVersion}); err != nil {
			_ = index.Close()
			return nil, fmt.Errorf("local index: initialize schema: %w", err)
		}
	} else if err := verifySchema(index); err != nil {
		_ = index.Close()
		return nil, err
	}
	local := &Index{index: index, maxDocumentBytes: maxDocumentBytes, maxBytes: maxBytes, maxDocuments: maxDocuments}
	if err := local.loadUsage(); err != nil {
		_ = index.Close()
		return nil, err
	}
	if err := local.pruneExpiredLocked(time.Now().UTC()); err != nil {
		_ = index.Close()
		return nil, err
	}
	if local.documents > local.maxDocuments || local.usedBytes > local.maxBytes {
		_ = index.Close()
		return nil, ErrIndexBudgetExceeded
	}
	return local, nil
}

func verifySchema(index bleve.Index) error {
	request := bleve.NewSearchRequestOptions(bleve.NewDocIDQuery([]string{schemaDocumentID}), 1, 0, false)
	request.Fields = []string{"schema_version"}
	result, err := index.Search(request)
	if err != nil {
		return fmt.Errorf("%w: read marker: %v", ErrSchemaMismatch, err)
	}
	if len(result.Hits) != 1 || stringField(result.Hits[0].Fields["schema_version"]) != currentSchemaVersion {
		return fmt.Errorf("%w: expected version %s; rebuild the opt-in index", ErrSchemaMismatch, currentSchemaVersion)
	}
	return nil
}

func indexMapping() mapping.IndexMapping {
	indexMapping := bleve.NewIndexMapping()
	indexMapping.IndexDynamic = false
	indexMapping.StoreDynamic = false
	indexMapping.DocValuesDynamic = false
	indexMapping.ScoringModel = "bm25"
	document := mapping.NewDocumentMapping()
	document.Dynamic = false

	addKeyword := func(name string, includeInAll bool) {
		field := mapping.NewKeywordFieldMapping()
		field.IncludeInAll = includeInAll
		document.AddFieldMappingsAt(name, field)
	}
	addText := func(name string, store, includeInAll bool) {
		field := mapping.NewTextFieldMapping()
		field.Store = store
		field.IncludeInAll = includeInAll
		document.AddFieldMappingsAt(name, field)
	}
	addDate := func(name string) {
		field := mapping.NewDateTimeFieldMapping()
		field.IncludeInAll = false
		document.AddFieldMappingsAt(name, field)
	}

	addKeyword("url", false)
	addKeyword("host_tokens", false)
	addKeyword("path_tokens", false)
	addText("title", true, true)
	addText("headings", false, true)
	addText("body", false, true)
	addText("snippet", true, false)
	addKeyword("language", false)
	addText("author", true, true)
	addDate("published_at")
	addDate("fetched_at")
	addDate("expires_at")
	addKeyword("expiry_class", false)
	addKeyword("content_hash", false)
	addKeyword("outbound_links", false)
	addKeyword("provenance", false)
	addKeyword("mime", false)
	addKeyword("extraction_status", false)
	addKeyword("safety_classification", false)
	addKeyword("indexing_disposition", false)
	addKeyword("schema_version", false)
	logicalBytes := mapping.NewNumericFieldMapping()
	logicalBytes.Store = true
	logicalBytes.IncludeInAll = false
	document.AddFieldMappingsAt("logical_bytes", logicalBytes)
	indexMapping.DefaultMapping = document
	return indexMapping
}

// Reconcile inserts permitted content or a non-searchable tombstone according
// to observation time. Permission is fail-closed.
func (local *Index) Reconcile(ctx context.Context, document localcorpus.Document) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	canonical, err := cache.CanonicalURL(document.URL)
	if err != nil {
		return fmt.Errorf("local index: canonical URL: %w", err)
	}
	parsed, err := url.Parse(canonical)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return errors.New("local index: HTTP(S) URL is required")
	}
	fetchedAt := document.FetchedAt.UTC()
	if fetchedAt.IsZero() {
		fetchedAt = time.Now().UTC()
	}
	if document.IndexingDisposition != localcorpus.DispositionPermitted {
		local.mu.Lock()
		defer local.mu.Unlock()
		if local.closed {
			return ErrClosed
		}
		if err := local.pruneExpiredLocked(time.Now().UTC()); err != nil {
			return err
		}
		_, err = local.reconcileTombstoneLocked(canonical, fetchedAt, document.IndexingDisposition, false)
		return err
	}
	body := strings.TrimSpace(document.Body)
	if body == "" {
		return errors.New("local index: non-empty extracted body is required")
	}
	classification, err := localcorpus.ParseSafetyClassification(string(document.SafetyClassification))
	if err != nil {
		return fmt.Errorf("local index: %w", err)
	}
	if len(document.Headings)+len(document.OutboundLinks)+len(document.Provenance) > maxDocumentListItems {
		return fmt.Errorf("local index: document exceeds %d list items", maxDocumentListItems)
	}
	logicalBytes := documentSize(document)
	if logicalBytes > local.maxDocumentBytes {
		return fmt.Errorf("local index: document exceeds %d bytes", local.maxDocumentBytes)
	}
	contentHash := strings.TrimSpace(document.ContentHash)
	if contentHash == "" {
		sum := sha256.Sum256([]byte(body))
		contentHash = hex.EncodeToString(sum[:])
	}
	stored := storedDocument{
		URL: canonical, HostTokens: hostTokens(parsed.Hostname()), PathTokens: pathTokens(parsed.Hostname(), parsed.EscapedPath()),
		Title: strings.TrimSpace(document.Title), Headings: cleanStrings(document.Headings),
		Body: body, Snippet: snippet(body, 500), Language: strings.ToLower(strings.TrimSpace(document.Language)),
		Author: strings.TrimSpace(document.Author), FetchedAt: fetchedAt, ContentHash: contentHash,
		OutboundLinks: cleanStrings(document.OutboundLinks), Provenance: cleanStrings(document.Provenance),
		MIME: strings.ToLower(strings.TrimSpace(document.MIME)), ExtractionStatus: strings.TrimSpace(document.ExtractionStatus),
		SafetyClassification: string(classification),
		IndexingDisposition:  string(document.IndexingDisposition),
		LogicalBytes:         int64(logicalBytes),
	}
	if document.PublishedAt != nil {
		publishedAt := document.PublishedAt.UTC()
		stored.PublishedAt = &publishedAt
	}
	if document.ExpiresAt != nil {
		expiresAt := document.ExpiresAt.UTC()
		stored.ExpiresAt = &expiresAt
		stored.ExpiryClass = "bounded"
	} else {
		stored.ExpiryClass = "unbounded"
	}

	local.mu.Lock()
	defer local.mu.Unlock()
	if local.closed {
		return ErrClosed
	}
	if err := local.pruneExpiredLocked(time.Now().UTC()); err != nil {
		return err
	}
	record, err := local.readRetained(documentID(canonical))
	if err != nil {
		return err
	}
	if retainedObservationIsStale(record, fetchedAt, document.IndexingDisposition) {
		return ErrStaleObservation
	}
	if err := local.admit(record, int64(logicalBytes)); err != nil {
		return err
	}
	if err := local.index.Index(documentID(canonical), stored); err != nil {
		return err
	}
	local.commitUsage(record, int64(logicalBytes))
	return nil
}

// ReconcileExisting records a non-permitted observation only when the URL
// already has retained index state. Curated ordinary request paths use this
// atomic boundary so arbitrary denied URLs cannot consume durable tombstone
// capacity, while an existing searchable document can still be revoked.
func (local *Index) ReconcileExisting(ctx context.Context, document localcorpus.Document) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if document.IndexingDisposition == localcorpus.DispositionPermitted {
		return false, errors.New("local index: ReconcileExisting requires a non-permitted disposition")
	}
	canonical, err := cache.CanonicalURL(document.URL)
	if err != nil {
		return false, fmt.Errorf("local index: canonical URL: %w", err)
	}
	parsed, err := url.Parse(canonical)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return false, errors.New("local index: HTTP(S) URL is required")
	}
	fetchedAt := document.FetchedAt.UTC()
	if fetchedAt.IsZero() {
		fetchedAt = time.Now().UTC()
	}
	local.mu.Lock()
	defer local.mu.Unlock()
	if local.closed {
		return false, ErrClosed
	}
	if err := local.pruneExpiredLocked(time.Now().UTC()); err != nil {
		return false, err
	}
	return local.reconcileTombstoneLocked(canonical, fetchedAt, document.IndexingDisposition, true)
}

func (local *Index) reconcileTombstoneLocked(canonical string, fetchedAt time.Time, disposition localcorpus.IndexingDisposition, requireExisting bool) (bool, error) {
	record, err := local.readRetained(documentID(canonical))
	if err != nil {
		return false, err
	}
	if requireExisting && !record.exists {
		return false, nil
	}
	if retainedObservationIsStale(record, fetchedAt, disposition) {
		return false, ErrStaleObservation
	}
	logicalBytes := int64(len(canonical) + len(disposition))
	if err := local.admit(record, logicalBytes); err != nil {
		return false, err
	}
	tombstone := storedDocument{
		URL: canonical, FetchedAt: fetchedAt, IndexingDisposition: string(disposition), LogicalBytes: logicalBytes,
	}
	if err := local.index.Index(documentID(canonical), tombstone); err != nil {
		return false, fmt.Errorf("local index: remove no-longer-permitted document: %w", err)
	}
	local.commitUsage(record, logicalBytes)
	return true, nil
}

// Index is a convenience surface for direct callers. Denied observations are
// still reconciled as tombstones, then reported as ErrNotPermitted.
func (local *Index) Index(ctx context.Context, document localcorpus.Document) error {
	err := local.Reconcile(ctx, document)
	if err == nil && document.IndexingDisposition != localcorpus.DispositionPermitted {
		return ErrNotPermitted
	}
	return err
}

// Delete removes the canonical URL if present.
func (local *Index) Delete(ctx context.Context, rawURL string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	canonical, err := cache.CanonicalURL(rawURL)
	if err != nil {
		return fmt.Errorf("local index: canonical URL: %w", err)
	}
	local.mu.Lock()
	defer local.mu.Unlock()
	if local.closed {
		return ErrClosed
	}
	fetchedAt := time.Now().UTC()
	if err := local.pruneExpiredLocked(fetchedAt); err != nil {
		return err
	}
	record, err := local.readRetained(documentID(canonical))
	if err != nil {
		return err
	}
	if retainedObservationIsStale(record, fetchedAt, localcorpus.DispositionTakedown) {
		return ErrStaleObservation
	}
	logicalBytes := int64(len(canonical) + len(localcorpus.DispositionTakedown))
	if err := local.admit(record, logicalBytes); err != nil {
		return err
	}
	if err := local.index.Index(documentID(canonical), storedDocument{
		URL: canonical, FetchedAt: fetchedAt, IndexingDisposition: string(localcorpus.DispositionTakedown), LogicalBytes: logicalBytes,
	}); err != nil {
		return err
	}
	local.commitUsage(record, logicalBytes)
	return nil
}

// Sweep removes all bounded permitted documents whose retention expires at or
// before now. Deletions are committed as one batch so aggregate usage changes
// only after the underlying index accepts the complete sweep.
func (local *Index) Sweep(ctx context.Context, now time.Time) (int, error) {
	if now.IsZero() {
		return 0, errors.New("local index: sweep time is required")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	local.mu.Lock()
	defer local.mu.Unlock()
	if local.closed {
		return 0, ErrClosed
	}
	return local.sweepExpiredLocked(ctx, now.UTC())
}

// Search implements search.Searcher.
func (local *Index) Search(ctx context.Context, searchQuery search.Query) ([]search.Hit, error) {
	batch, err := local.SearchBatch(ctx, searchQuery)
	return batch.Hits, err
}

// SearchBatch performs lexical search and maps only stored metadata back into
// the discovery contract; full bodies never leave the index adapter.
func (local *Index) SearchBatch(ctx context.Context, searchQuery search.Query) (search.SearchBatch, error) {
	started := time.Now()
	text := strings.TrimSpace(searchQuery.Q)
	if text == "" {
		return search.SearchBatch{Provider: providerID, Instance: providerID, Status: search.BatchAuthoritativeEmpty, Duration: time.Since(started)}, nil
	}
	maxResults := searchQuery.MaxResults
	if maxResults <= 0 {
		maxResults = defaultSearchResults
	}
	if maxResults > maxSearchResults {
		maxResults = maxSearchResults
	}
	var bleveQuery blevequery.Query
	if searchQuery.ExactMatch {
		bleveQuery = bleve.NewMatchPhraseQuery(text)
	} else {
		bleveQuery = bleve.NewMatchQuery(text)
	}
	bleveQuery = applyQueryFilters(bleveQuery, searchQuery)
	request := bleve.NewSearchRequestOptions(bleveQuery, maxResults, 0, false)
	request.Fields = []string{"url", "title", "snippet", "language", "author", "published_at", "fetched_at", "content_hash", "provenance", "mime", "safety_classification"}

	local.mu.RLock()
	defer local.mu.RUnlock()
	if local.closed {
		return failedBatch(started, ErrClosed)
	}
	result, err := local.index.SearchInContext(ctx, request)
	if err != nil {
		return failedBatch(started, err)
	}
	hits := make([]search.Hit, 0, min(maxResults, len(result.Hits)))
	for _, match := range result.Hits {
		hit, ok := mapHit(match.Fields, match.Score)
		if !ok || !matchesFilters(hit, searchQuery) {
			continue
		}
		hits = append(hits, hit)
		if len(hits) == maxResults {
			break
		}
	}
	status := search.BatchHealthy
	if len(hits) == 0 {
		status = search.BatchAuthoritativeEmpty
	}
	return search.SearchBatch{Hits: hits, Provider: providerID, Instance: providerID, Status: status, Duration: time.Since(started)}, nil
}

func failedBatch(started time.Time, err error) (search.SearchBatch, error) {
	return search.SearchBatch{
		Provider: providerID, Instance: providerID, Status: search.BatchFailed, Duration: time.Since(started),
		Diagnostics: []search.ProviderDiagnostic{{Provider: providerID, Instance: providerID, Reason: "index_error"}},
	}, err
}

func applyQueryFilters(textQuery blevequery.Query, searchQuery search.Query) blevequery.Query {
	filters := []blevequery.Query{activeDocumentQuery(time.Now().UTC())}
	mustNot := make([]blevequery.Query, 0, 1)
	if include := domainQueries(searchQuery.IncludeDomains); len(include) == 1 {
		filters = append(filters, include[0])
	} else if len(include) > 1 {
		filters = append(filters, blevequery.NewDisjunctionQuery(include))
	}
	if exclude := domainQueries(searchQuery.ExcludeDomains); len(exclude) == 1 {
		mustNot = append(mustNot, exclude[0])
	} else if len(exclude) > 1 {
		mustNot = append(mustNot, blevequery.NewDisjunctionQuery(exclude))
	}
	if language := strings.ToLower(strings.TrimSpace(searchQuery.Language)); language != "" && language != "auto" {
		languageQuery := blevequery.NewTermQuery(language)
		languageQuery.SetField("language")
		filters = append(filters, languageQuery)
	}
	if cutoff := timeRangeCutoff(searchQuery.TimeRange); !cutoff.IsZero() {
		dateQuery := blevequery.NewDateRangeQuery(cutoff, time.Now().UTC().Add(time.Minute))
		dateQuery.SetField("published_at")
		filters = append(filters, dateQuery)
	}
	if searchQuery.SafeSearch != nil && *searchQuery.SafeSearch > 0 {
		safetyQuery := blevequery.NewTermQuery("safe")
		safetyQuery.SetField("safety_classification")
		filters = append(filters, safetyQuery)
	}
	if len(filters) == 0 && len(mustNot) == 0 {
		return textQuery
	}
	query := blevequery.NewBooleanQuery([]blevequery.Query{textQuery}, nil, mustNot)
	if len(filters) > 0 {
		query.Filter = blevequery.NewConjunctionQuery(filters)
	}
	return query
}

func activeDocumentQuery(now time.Time) blevequery.Query {
	permitted := blevequery.NewTermQuery(string(localcorpus.DispositionPermitted))
	permitted.SetField("indexing_disposition")
	exclusiveStart := false
	expires := blevequery.NewDateRangeInclusiveQuery(now, time.Time{}, &exclusiveStart, nil)
	expires.SetField("expires_at")
	unbounded := blevequery.NewTermQuery("unbounded")
	unbounded.SetField("expiry_class")
	activeExpiry := blevequery.NewDisjunctionQuery([]blevequery.Query{unbounded, expires})
	return blevequery.NewConjunctionQuery([]blevequery.Query{permitted, activeExpiry})
}

func domainQueries(filters []string) []blevequery.Query {
	queries := make([]blevequery.Query, 0, len(filters))
	for _, filter := range filters {
		filter = strings.ToLower(strings.TrimSpace(filter))
		if !strings.Contains(filter, "://") {
			filter = "https://" + filter
		}
		parsed, err := url.Parse(filter)
		if err != nil {
			continue
		}
		host := strings.TrimPrefix(parsed.Hostname(), "*.")
		host = strings.TrimPrefix(host, "www.")
		if host == "" {
			continue
		}
		path := strings.TrimRight(parsed.EscapedPath(), "/")
		term := host
		field := "host_tokens"
		if path != "" {
			term += path
			field = "path_tokens"
		}
		query := blevequery.NewTermQuery(term)
		query.SetField(field)
		queries = append(queries, query)
	}
	return queries
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
	startLimit := min(len(parts)-1, 32)
	for index := 0; index < startLimit; index++ {
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
	capacity := min(len(hosts)*len(segments), maxDerivedFilterTokens)
	tokens := make([]string, 0, capacity)
	path := ""
	for _, segment := range segments {
		if segment == "" {
			continue
		}
		path += "/" + segment
		for _, host := range hosts {
			tokens = append(tokens, host+path)
			if len(tokens) == maxDerivedFilterTokens {
				return tokens
			}
		}
	}
	return tokens
}

func mapHit(fields map[string]any, score float64) (search.Hit, bool) {
	rawURL := stringField(fields["url"])
	if rawURL == "" {
		return search.Hit{}, false
	}
	hit := search.Hit{
		URL: rawURL, Title: stringField(fields["title"]), Snippet: stringField(fields["snippet"]),
		Engines: []string{providerID}, Metadata: map[string]string{
			"provider": providerID, "local_score": strconv.FormatFloat(score, 'f', 6, 64),
			"content_hash": stringField(fields["content_hash"]), "language": stringField(fields["language"]),
			"author": stringField(fields["author"]), "fetched_at": stringField(fields["fetched_at"]),
			"mime": stringField(fields["mime"]), "provenance": strings.Join(stringSliceField(fields["provenance"]), ","),
			"safety_classification": stringField(fields["safety_classification"]),
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
	if cutoff := timeRangeCutoff(query.TimeRange); !cutoff.IsZero() {
		if hit.PublishedAt == nil || hit.PublishedAt.Before(cutoff) {
			return false
		}
	}
	return true
}

func matchesAnyDomain(resultURL *url.URL, filters []string) bool {
	host := strings.TrimPrefix(strings.ToLower(resultURL.Hostname()), "www.")
	path := strings.TrimRight(resultURL.EscapedPath(), "/")
	for _, filter := range filters {
		filter = strings.ToLower(strings.TrimSpace(filter))
		if !strings.Contains(filter, "://") {
			filter = "https://" + filter
		}
		parsed, err := url.Parse(filter)
		if err != nil {
			continue
		}
		filterHost := strings.TrimPrefix(parsed.Hostname(), "*.")
		filterHost = strings.TrimPrefix(filterHost, "www.")
		if filterHost == "" || (host != filterHost && !strings.HasSuffix(host, "."+filterHost)) {
			continue
		}
		filterPath := strings.TrimRight(parsed.EscapedPath(), "/")
		if filterPath != "" && !strings.HasPrefix(path, filterPath) {
			continue
		}
		return true
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
	sum := sha256.Sum256([]byte(canonicalURL))
	return hex.EncodeToString(sum[:])
}

type retainedRecord struct {
	exists       bool
	fetchedAt    time.Time
	disposition  localcorpus.IndexingDisposition
	logicalBytes int64
	expiresAt    *time.Time
}

func (local *Index) readRetained(id string) (retainedRecord, error) {
	query := bleve.NewDocIDQuery([]string{id})
	request := bleve.NewSearchRequestOptions(query, 1, 0, false)
	request.Fields = []string{"fetched_at", "indexing_disposition", "logical_bytes", "expires_at"}
	result, err := local.index.Search(request)
	if err != nil {
		return retainedRecord{}, fmt.Errorf("local index: inspect retained observation: %w", err)
	}
	if len(result.Hits) == 0 {
		return retainedRecord{}, nil
	}
	fields := result.Hits[0].Fields
	record := retainedRecord{
		exists: true, disposition: localcorpus.IndexingDisposition(stringField(fields["indexing_disposition"])),
		logicalBytes: int64(numberField(fields["logical_bytes"])),
	}
	record.fetchedAt, _ = time.Parse(time.RFC3339, stringField(fields["fetched_at"]))
	if expiresAt, err := time.Parse(time.RFC3339, stringField(fields["expires_at"])); err == nil {
		record.expiresAt = &expiresAt
	}
	return record, nil
}

func retainedObservationIsStale(record retainedRecord, observedAt time.Time, incoming localcorpus.IndexingDisposition) bool {
	if !record.exists {
		return false
	}
	if record.disposition == localcorpus.DispositionTakedown && incoming != localcorpus.DispositionTakedown {
		return true
	}
	if record.fetchedAt.After(observedAt) {
		return true
	}
	return record.fetchedAt.Equal(observedAt) && record.disposition != localcorpus.DispositionPermitted && incoming == localcorpus.DispositionPermitted
}

func (local *Index) admit(record retainedRecord, logicalBytes int64) error {
	documents := local.documents
	if !record.exists {
		documents++
	}
	if documents > local.maxDocuments {
		return ErrIndexBudgetExceeded
	}
	used := local.usedBytes - record.logicalBytes + logicalBytes
	if used < 0 || used > local.maxBytes {
		return ErrIndexBudgetExceeded
	}
	return nil
}

func (local *Index) commitUsage(record retainedRecord, logicalBytes int64) {
	local.usedBytes = local.usedBytes - record.logicalBytes + logicalBytes
	if !record.exists {
		local.documents++
	}
}

func (local *Index) loadUsage() error {
	docCount, err := local.index.DocCount()
	if err != nil {
		return fmt.Errorf("local index: count aggregate usage: %w", err)
	}
	if docCount > uint64(^uint(0)>>1) {
		return ErrIndexBudgetExceeded
	}
	// The schema marker is the only non-corpus document. Refuse a lowered
	// operator cap before allocating a hit set proportional to an oversized
	// existing index.
	if docCount > uint64(local.maxDocuments)+1 {
		return ErrIndexBudgetExceeded
	}
	request := bleve.NewSearchRequestOptions(bleve.NewMatchAllQuery(), int(docCount), 0, false)
	request.Fields = []string{"logical_bytes", "schema_version"}
	result, err := local.index.Search(request)
	if err != nil {
		return fmt.Errorf("local index: inspect aggregate usage: %w", err)
	}
	for _, hit := range result.Hits {
		if hit.ID == schemaDocumentID {
			continue
		}
		local.documents++
		local.usedBytes += int64(numberField(hit.Fields["logical_bytes"]))
	}
	return nil
}

func (local *Index) pruneExpiredLocked(now time.Time) error {
	_, err := local.sweepExpiredLocked(context.Background(), now.UTC())
	return err
}

func (local *Index) sweepExpiredLocked(ctx context.Context, now time.Time) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	permitted := bleve.NewTermQuery(string(localcorpus.DispositionPermitted))
	permitted.SetField("indexing_disposition")
	bounded := bleve.NewTermQuery("bounded")
	bounded.SetField("expiry_class")
	inclusiveEnd := true
	expired := bleve.NewDateRangeInclusiveQuery(time.Time{}, now, nil, &inclusiveEnd)
	expired.SetField("expires_at")
	query := blevequery.NewConjunctionQuery([]blevequery.Query{permitted, bounded, expired})
	limit := local.documents
	if limit < 1 {
		limit = 1
	}
	request := bleve.NewSearchRequestOptions(query, limit, 0, false)
	request.Fields = []string{"logical_bytes"}
	result, err := local.index.SearchInContext(ctx, request)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		return 0, fmt.Errorf("local index: find expired documents: %w", err)
	}
	batch := local.index.NewBatch()
	removedBytes := int64(0)
	for _, hit := range result.Hits {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		batch.Delete(hit.ID)
		removedBytes += int64(numberField(hit.Fields["logical_bytes"]))
	}
	if len(result.Hits) == 0 {
		return 0, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	remainingBytes := local.usedBytes - removedBytes
	remainingDocuments := local.documents - len(result.Hits)
	if remainingBytes < 0 || remainingDocuments < 0 {
		return 0, ErrSchemaMismatch
	}
	if err := local.index.Batch(batch); err != nil {
		return 0, fmt.Errorf("local index: prune expired documents: %w", err)
	}
	local.usedBytes = remainingBytes
	local.documents = remainingDocuments
	return len(result.Hits), nil
}

func documentSize(document localcorpus.Document) int {
	size := len(document.URL) + len(document.Title) + len(document.Body) + len(document.Language) +
		len(document.Author) + len(document.ContentHash) + len(document.MIME) + len(document.ExtractionStatus) +
		len(document.SafetyClassification) + len(document.IndexingDisposition)
	for _, values := range [][]string{document.Headings, document.OutboundLinks, document.Provenance} {
		for _, value := range values {
			if len(value) > int(^uint(0)>>1)-size {
				return int(^uint(0) >> 1)
			}
			size += len(value)
		}
	}
	return size
}

func snippet(body string, maxRunes int) string {
	value := strings.Join(strings.Fields(body), " ")
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return value
	}
	return strings.TrimSpace(string(runes[:maxRunes])) + "…"
}

func cleanStrings(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
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
			return fmt.Sprint(typed[0])
		}
	}
	return ""
}

func numberField(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	case string:
		parsed, _ := strconv.ParseFloat(typed, 64)
		return parsed
	case []any:
		if len(typed) > 0 {
			return numberField(typed[0])
		}
	}
	return 0
}

func rejectSymlinkComponents(path string) error {
	info, err := os.Lstat(path)
	if err == nil && info.Mode()&os.ModeSymlink != 0 {
		return errors.New("local index: path must not be a symlink")
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("local index: inspect path: %w", err)
	}
	return nil
}

func resolvePersistentPath(path string) (string, error) {
	tail := make([]string, 0, 4)
	current := filepath.Clean(path)
	for {
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			for index := len(tail) - 1; index >= 0; index-- {
				resolved = filepath.Join(resolved, tail[index])
			}
			return filepath.Clean(resolved), nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("local index: resolve path: %w", err)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", fmt.Errorf("local index: resolve path: %w", err)
		}
		tail = append(tail, filepath.Base(current))
		current = parent
	}
}

func stringSliceField(value any) []string {
	switch typed := value.(type) {
	case string:
		return []string{typed}
	case []string:
		return typed
	case []any:
		result := make([]string, 0, len(typed))
		for _, item := range typed {
			result = append(result, fmt.Sprint(item))
		}
		return result
	default:
		return nil
	}
}

// Count returns the current document count.
func (local *Index) Count() (uint64, error) {
	local.mu.RLock()
	defer local.mu.RUnlock()
	if local.closed {
		return 0, ErrClosed
	}
	request := bleve.NewSearchRequestOptions(activeDocumentQuery(time.Now().UTC()), 0, 0, false)
	result, err := local.index.Search(request)
	if err != nil {
		return 0, err
	}
	return result.Total, nil
}

// Close flushes and closes the underlying index exactly once.
func (local *Index) Close() error {
	local.mu.Lock()
	defer local.mu.Unlock()
	if local.closed {
		return nil
	}
	local.closed = true
	return local.index.Close()
}

var (
	_ localcorpus.Writer   = (*Index)(nil)
	_ search.Searcher      = (*Index)(nil)
	_ search.BatchSearcher = (*Index)(nil)
)
