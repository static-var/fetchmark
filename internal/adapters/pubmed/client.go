// Package pubmed implements bounded, metadata-only discovery through NCBI's
// PubMed E-utilities. It never requests abstracts or full text.
package pubmed

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/mail"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/staticvar/fetchmark/internal/adapters/providerbudget"
	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/retryafter"
	"github.com/staticvar/fetchmark/internal/core/search"
)

const (
	providerInstance       = "eutils.ncbi.nlm.nih.gov"
	providerOrigin         = "https://eutils.ncbi.nlm.nih.gov"
	resultHost             = "pubmed.ncbi.nlm.nih.gov"
	searchPath             = "/entrez/eutils/esearch.fcgi"
	summaryPath            = "/entrez/eutils/esummary.fcgi"
	defaultBlockBackoff    = time.Minute
	maxRetryAfter          = 24 * time.Hour
	maxProviderResults     = 20
	maxProviderRate        = 2.0
	maximumQueryInputBytes = 2048
	maximumQueryBytes      = 300
	maximumQueryTerms      = 24
	maximumFieldBytes      = 1024
	maximumEmailBytes      = 254
	rightsNoticeURL        = "https://www.nlm.nih.gov/databases/download.html"
	disclaimerURL          = "https://www.ncbi.nlm.nih.gov/home/about/policies/"
)

// Options defines the fixed endpoint identity and process-local shared-IP
// budgets. Email must identify a real operator/developer contact as required
// by NCBI for distributed E-utilities clients.
type Options struct {
	HTTPClient     *http.Client
	UserAgent      string
	Email          string
	MaxResults     int
	MaxBodyBytes   int64
	RatePerSecond  float64
	Burst          int
	MaxConcurrency int
}

// StatusError retains bounded provider scheduling evidence.
type StatusError struct {
	StatusCode int
	RetryAfter time.Duration
}

func (statusError *StatusError) Error() string {
	return fmt.Sprintf("pubmed: upstream status %d", statusError.StatusCode)
}

// Client implements the compatibility and provider-aware discovery contracts.
type Client struct {
	httpClient   *http.Client
	userAgent    string
	email        string
	maxResults   int
	maxBodyBytes int64
	budget       *providerbudget.Gate

	mu            sync.Mutex
	cooldownUntil time.Time
	now           func() time.Time
}

var _ search.Searcher = (*Client)(nil)
var _ search.BatchSearcher = (*Client)(nil)

// New pins all traffic to the official PubMed E-utilities origin. Operator
// values may only tighten the conservative anonymous-service ceilings.
func New(options Options) (*Client, error) {
	if options.HTTPClient == nil {
		return nil, errors.New("pubmed: HTTP client is required")
	}
	if !validUserAgent(options.UserAgent) {
		return nil, errors.New("pubmed: descriptive contactable User-Agent is required")
	}
	options.Email = strings.TrimSpace(options.Email)
	address, err := mail.ParseAddress(options.Email)
	if err != nil || address.Address != options.Email || len(options.Email) > maximumEmailBytes || strings.ContainsAny(options.Email, "\r\n") {
		return nil, errors.New("pubmed: a plain valid operator email is required")
	}
	if options.MaxResults < 1 || options.MaxBodyBytes < 1 || options.RatePerSecond <= 0 || options.Burst < 1 || options.MaxConcurrency < 1 {
		return nil, errors.New("pubmed: result, body, rate, burst, and concurrency limits must be positive")
	}
	options.MaxResults = min(options.MaxResults, maxProviderResults)
	options.RatePerSecond = min(options.RatePerSecond, maxProviderRate)
	options.Burst = min(options.Burst, 1)
	options.MaxConcurrency = min(options.MaxConcurrency, 1)
	budget, err := providerbudget.New(options.RatePerSecond, options.Burst, options.MaxConcurrency)
	if err != nil {
		return nil, fmt.Errorf("pubmed: configure provider budget: %w", err)
	}
	return &Client{
		httpClient: singleAttemptHTTPClient(options.HTTPClient), userAgent: options.UserAgent,
		email: options.Email, maxResults: options.MaxResults, maxBodyBytes: options.MaxBodyBytes,
		budget: budget, now: time.Now,
	}, nil
}

// singleAttemptHTTPClient makes each rate token correspond to one outbound
// request. Redirects and transparent replay after stale connection reuse would
// otherwise escape the shared E-utilities budget.
func singleAttemptHTTPClient(original *http.Client) *http.Client {
	client := *original
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	transport, ok := original.Transport.(*http.Transport)
	if original.Transport == nil {
		transport = http.DefaultTransport.(*http.Transport)
		ok = true
	}
	if ok {
		cloned := transport.Clone()
		cloned.DisableKeepAlives = true
		cloned.ForceAttemptHTTP2 = false
		cloned.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		client.Transport = cloned
	}
	return &client
}

func (client *Client) Search(ctx context.Context, query search.Query) ([]search.Hit, error) {
	batch, err := client.SearchBatch(ctx, query)
	if err != nil {
		return nil, err
	}
	return batch.Hits, nil
}

// SearchBatch performs one bounded ESearch request and, only when IDs exist,
// one batched ESummary request. Both calls share one concurrency slot and spend
// separate rate tokens. No abstract or full-text endpoint is used.
func (client *Client) SearchBatch(ctx context.Context, query search.Query) (search.SearchBatch, error) {
	started := time.Now()
	fail := func(source, reason string, retryable bool, retryAfter time.Duration, err error) (search.SearchBatch, error) {
		return search.SearchBatch{
			Provider: "pubmed", Instance: providerInstance, Status: search.BatchFailed,
			Diagnostics: []search.ProviderDiagnostic{{
				Provider: "pubmed", Instance: providerInstance, Source: source,
				Reason: reason, Retryable: retryable, RetryAfter: retryAfter,
			}},
			Duration: time.Since(started),
		}, err
	}
	if err := ctx.Err(); err != nil {
		return fail("esearch", "canceled", false, 0, err)
	}
	if control := unsupportedControl(query); control != "" {
		err := &search.UnsupportedControlError{Control: control, Reason: "PubMed cannot preserve this search control"}
		return fail(control, "unsupported_control", false, 0, err)
	}
	projected, err := projectQuery(query.Q)
	if err != nil {
		return fail("query", "invalid_query", false, 0, err)
	}
	days, err := freshnessDays(query.TimeRange)
	if err != nil {
		return fail("time_range", "unsupported_control", false, 0, err)
	}
	if !domainAllowed(resultHost, query.IncludeDomains, query.ExcludeDomains) {
		return search.SearchBatch{Provider: "pubmed", Instance: providerInstance, Status: search.BatchAuthoritativeEmpty, Duration: time.Since(started)}, nil
	}
	if wait := client.cooldownRemaining(); wait > 0 {
		return fail("esearch", "cooldown", true, wait, &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: wait})
	}
	release, err := client.budget.AcquireSlot(ctx)
	if err != nil {
		return fail("esearch", "canceled", false, 0, err)
	}
	defer release()

	remaining := client.maxBodyBytes
	resultLimit := client.resultLimit(query.MaxResults)
	searchValues := client.commonValues()
	searchValues.Set("term", projected)
	searchValues.Set("retmax", strconv.Itoa(resultLimit))
	searchValues.Set("retstart", "0")
	searchValues.Set("sort", "relevance")
	if days > 0 {
		searchValues.Set("datetype", "pdat")
		searchValues.Set("reldate", strconv.Itoa(days))
	}
	searchRaw, requestErr := client.get(ctx, "esearch", searchPath, searchValues, remaining)
	if requestErr != nil {
		return fail(requestErr.source, requestErr.reason, requestErr.retryable, requestErr.retryAfter, requestErr.err)
	}
	remaining -= int64(len(searchRaw))
	var searchPayload searchResponse
	if err := json.Unmarshal(searchRaw, &searchPayload); err != nil {
		return fail("esearch", "malformed", false, 0, fmt.Errorf("pubmed: decode ESearch response: %w", err))
	}
	if searchPayload.Error != "" {
		return client.apiError(started, "esearch", searchPayload.Error)
	}
	if searchPayload.Result == nil {
		return fail("esearch", "malformed", false, 0, errors.New("pubmed: ESearch result is required"))
	}
	ids, invalidIDs := validPMIDs(searchPayload.Result.IDList, resultLimit)
	if len(ids) == 0 {
		count, countErr := strconv.ParseUint(searchPayload.Result.Count, 10, 64)
		if countErr != nil || count != 0 || invalidIDs > 0 {
			return fail("esearch", "malformed_results", false, 0, errors.New("pubmed: ESearch returned no valid PMIDs"))
		}
		return search.SearchBatch{Provider: "pubmed", Instance: providerInstance, Status: search.BatchAuthoritativeEmpty, Duration: time.Since(started)}, nil
	}

	summaryValues := client.commonValues()
	summaryValues.Set("id", strings.Join(ids, ","))
	summaryValues.Set("version", "2.0")
	summaryRaw, requestErr := client.get(ctx, "esummary", summaryPath, summaryValues, remaining)
	if requestErr != nil {
		return fail(requestErr.source, requestErr.reason, requestErr.retryable, requestErr.retryAfter, requestErr.err)
	}
	var summaryPayload summaryResponse
	if err := json.Unmarshal(summaryRaw, &summaryPayload); err != nil {
		return fail("esummary", "malformed", false, 0, fmt.Errorf("pubmed: decode ESummary response: %w", err))
	}
	if summaryPayload.Error != "" {
		return client.apiError(started, "esummary", summaryPayload.Error)
	}
	if summaryPayload.Result == nil {
		return fail("esummary", "malformed", false, 0, errors.New("pubmed: ESummary result is required"))
	}

	hits := make([]search.Hit, 0, len(ids))
	invalidRows := invalidIDs
	observedAt := client.now().UTC()
	for _, id := range ids {
		raw, exists := summaryPayload.Result[id]
		if !exists {
			invalidRows++
			continue
		}
		var record summaryRecord
		if err := json.Unmarshal(raw, &record); err != nil || record.UID != id {
			invalidRows++
			continue
		}
		hit, ok := summaryHit(record, observedAt)
		if !ok {
			invalidRows++
			continue
		}
		hits = append(hits, hit)
	}
	if len(hits) == 0 {
		return fail("esummary", "malformed_results", false, 0, errors.New("pubmed: ESummary contained no valid records"))
	}
	status := search.BatchHealthy
	diagnostics := []search.ProviderDiagnostic(nil)
	if invalidRows > 0 {
		status = search.BatchPartial
		diagnostics = []search.ProviderDiagnostic{{Provider: "pubmed", Instance: providerInstance, Source: "esummary", Reason: "malformed_results"}}
	}
	return search.SearchBatch{
		Hits: hits, Provider: "pubmed", Instance: providerInstance, Status: status,
		Diagnostics: diagnostics, Duration: time.Since(started),
	}, nil
}

type searchResponse struct {
	Error  string        `json:"error"`
	Result *searchResult `json:"esearchresult"`
}

type searchResult struct {
	Count  string   `json:"count"`
	IDList []string `json:"idlist"`
}

type summaryResponse struct {
	Error  string                     `json:"error"`
	Result map[string]json.RawMessage `json:"result"`
}

type summaryRecord struct {
	UID             string      `json:"uid"`
	PubDate         string      `json:"pubdate"`
	Source          string      `json:"source"`
	Authors         []author    `json:"authors"`
	Title           string      `json:"title"`
	Language        []string    `json:"lang"`
	PublicationType []string    `json:"pubtype"`
	ArticleIDs      []articleID `json:"articleids"`
	RecordStatus    string      `json:"recordstatus"`
	FullJournalName string      `json:"fulljournalname"`
	SortPubDate     string      `json:"sortpubdate"`
}

type author struct {
	Name string `json:"name"`
}

type articleID struct {
	Type  string `json:"idtype"`
	Value string `json:"value"`
}

type requestError struct {
	source     string
	reason     string
	retryable  bool
	retryAfter time.Duration
	err        error
}

func (client *Client) get(ctx context.Context, source, path string, values url.Values, limit int64) ([]byte, *requestError) {
	if limit < 1 {
		return nil, &requestError{source: source, reason: "oversized", err: errors.New("pubmed: combined response body limit exhausted")}
	}
	if wait := client.cooldownRemaining(); wait > 0 {
		return nil, &requestError{source: source, reason: "cooldown", retryable: true, retryAfter: wait, err: &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: wait}}
	}
	if err := client.budget.WaitRate(ctx); err != nil {
		return nil, &requestError{source: source, reason: "canceled", err: err}
	}
	if wait := client.cooldownRemaining(); wait > 0 {
		return nil, &requestError{source: source, reason: "cooldown", retryable: true, retryAfter: wait, err: &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: wait}}
	}
	endpoint := url.URL{Scheme: "https", Host: providerInstance, Path: path, RawQuery: values.Encode()}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, &requestError{source: source, reason: "request", err: err}
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", client.userAgent)
	response, err := client.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, &requestError{source: source, reason: "canceled", err: ctx.Err()}
		}
		return nil, &requestError{source: source, reason: "network", retryable: true, err: err}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		wait := retryafter.Parse(response.Header.Get("Retry-After"), client.now(), maxRetryAfter)
		retryable := response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500
		if (response.StatusCode == http.StatusForbidden || response.StatusCode == http.StatusTooManyRequests) && wait == 0 {
			wait = defaultBlockBackoff
		}
		if wait > 0 || response.StatusCode == http.StatusForbidden {
			client.openCooldown(wait)
		}
		return nil, &requestError{
			source: source, reason: "http_" + strconv.Itoa(response.StatusCode), retryable: retryable,
			retryAfter: wait, err: &StatusError{StatusCode: response.StatusCode, RetryAfter: wait},
		}
	}
	raw, err := readBounded(response.Body, limit)
	if err != nil {
		return nil, &requestError{source: source, reason: "oversized", err: err}
	}
	return raw, nil
}

func (client *Client) apiError(started time.Time, source, message string) (search.SearchBatch, error) {
	retryable := strings.Contains(strings.ToLower(message), "rate limit")
	wait := time.Duration(0)
	if retryable {
		wait = defaultBlockBackoff
		client.openCooldown(wait)
	}
	return search.SearchBatch{
		Provider: "pubmed", Instance: providerInstance, Status: search.BatchFailed,
		Diagnostics: []search.ProviderDiagnostic{{
			Provider: "pubmed", Instance: providerInstance, Source: source,
			Reason: "api_error", Retryable: retryable, RetryAfter: wait,
		}},
		Duration: time.Since(started),
	}, errors.New("pubmed: E-utilities returned an API error")
}

func (client *Client) commonValues() url.Values {
	return url.Values{"db": {"pubmed"}, "retmode": {"json"}, "tool": {"fetchmark"}, "email": {client.email}}
}

func summaryHit(record summaryRecord, observedAt time.Time) (search.Hit, bool) {
	if !validPMID(record.UID) {
		return search.Hit{}, false
	}
	title := cleanField(record.Title, maximumFieldBytes)
	if title == "" {
		return search.Hit{}, false
	}
	journal := cleanField(record.FullJournalName, 512)
	if journal == "" {
		journal = cleanField(record.Source, 256)
	}
	authors := cleanAuthors(record.Authors)
	published := parseSummaryDate(record.SortPubDate, observedAt)
	year := publicationYear(published, record.PubDate, observedAt)
	metadata := map[string]string{
		"source": "PubMed / NLM", "pmid": record.UID,
		"attribution_url": "https://pubmed.ncbi.nlm.nih.gov/", "rights_notice_url": rightsNoticeURL,
		"disclaimer_url": disclaimerURL, "observed_at": observedAt.Format(time.RFC3339),
		"staleness": "observed_at_query_time",
	}
	if journal != "" {
		metadata["journal"] = journal
	}
	if len(authors) > 0 {
		metadata["authors"] = strings.Join(authors, ", ")
	}
	if value := boundedList(record.Language, 8, 32); value != "" {
		metadata["languages"] = value
	}
	if value := boundedList(record.PublicationType, 12, 96); value != "" {
		metadata["publication_types"] = value
	}
	if value := cleanField(record.RecordStatus, 256); value != "" {
		metadata["record_status"] = value
	}
	for _, identifier := range record.ArticleIDs {
		kind := strings.ToLower(strings.TrimSpace(identifier.Type))
		value := cleanField(identifier.Value, 256)
		if value == "" {
			continue
		}
		switch kind {
		case "doi", "pmc", "pmcid":
			if _, exists := metadata[kind]; !exists {
				metadata[kind] = value
			}
		}
	}
	snippetParts := make([]string, 0, 3)
	if len(authors) > 0 {
		snippetParts = append(snippetParts, strings.Join(authors, ", "))
	}
	if journal != "" {
		snippetParts = append(snippetParts, journal)
	}
	if year != "" {
		snippetParts = append(snippetParts, year)
	}
	providerDocument := &model.ProviderDocument{Author: strings.Join(authors, ", "), SiteName: "PubMed"}
	providerDocument.HTML = bibliographicHTML(authors, journal, cleanField(record.PubDate, 64))
	return search.Hit{
		URL: "https://" + resultHost + "/" + record.UID + "/", Title: title,
		Snippet: strings.Join(snippetParts, " · "), Engines: []string{"pubmed"}, PublishedAt: published,
		Metadata: metadata, ProviderDocument: providerDocument,
	}, true
}

func bibliographicHTML(authors []string, journal, publicationDate string) []byte {
	var builder strings.Builder
	if len(authors) > 0 {
		builder.WriteString("<p>Authors: ")
		builder.WriteString(html.EscapeString(strings.Join(authors, ", ")))
		builder.WriteString("</p>")
	}
	if journal != "" {
		builder.WriteString("<p>Journal: ")
		builder.WriteString(html.EscapeString(journal))
		builder.WriteString("</p>")
	}
	if publicationDate != "" {
		builder.WriteString("<p>Publication date: ")
		builder.WriteString(html.EscapeString(publicationDate))
		builder.WriteString("</p>")
	}
	if builder.Len() == 0 {
		builder.WriteString("<p>PubMed citation metadata.</p>")
	}
	return []byte(builder.String())
}

func cleanAuthors(values []author) []string {
	result := make([]string, 0, min(len(values), 16))
	seen := make(map[string]struct{}, min(len(values), 16))
	for _, value := range values {
		name := cleanField(value.Name, 128)
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, name)
		if len(result) == 16 {
			break
		}
	}
	return result
}

func boundedList(values []string, maximumItems, maximumItemBytes int) string {
	result := make([]string, 0, min(len(values), maximumItems))
	for _, value := range values {
		if cleaned := cleanField(value, maximumItemBytes); cleaned != "" {
			result = append(result, cleaned)
			if len(result) == maximumItems {
				break
			}
		}
	}
	return strings.Join(result, ",")
}

func cleanField(value string, maximumBytes int) string {
	value = strings.Join(strings.Fields(html.UnescapeString(value)), " ")
	if value == "" || len(value) > maximumBytes || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
		return ""
	}
	return value
}

func parseSummaryDate(value string, observedAt time.Time) *time.Time {
	parsed, err := time.ParseInLocation("2006/01/02 15:04", strings.TrimSpace(value), time.UTC)
	if err != nil || parsed.Year() < 1000 || parsed.Year() > observedAt.UTC().Year()+5 {
		return nil
	}
	return &parsed
}

func publicationYear(published *time.Time, fallback string, observedAt time.Time) string {
	if published != nil {
		return strconv.Itoa(published.Year())
	}
	fields := strings.Fields(strings.TrimSpace(fallback))
	if len(fields) > 0 && len(fields[0]) == 4 {
		if year, err := strconv.Atoi(fields[0]); err == nil && year >= 1000 && year <= observedAt.UTC().Year()+5 {
			return fields[0]
		}
	}
	return ""
}

func validPMIDs(values []string, maximum int) ([]string, int) {
	result := make([]string, 0, min(len(values), maximum))
	seen := make(map[string]struct{}, min(len(values), maximum))
	invalid := 0
	for _, value := range values {
		if len(result) == maximum {
			invalid++
			continue
		}
		if !validPMID(value) {
			invalid++
			continue
		}
		if _, duplicate := seen[value]; duplicate {
			invalid++
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result, invalid
}

func validPMID(value string) bool {
	if value == "" || len(value) > 12 || value[0] == '0' {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func projectQuery(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maximumQueryInputBytes || !utf8.ValidString(value) {
		return "", errors.New("pubmed: query is empty, invalid, or oversized")
	}
	terms := make([]string, 0, maximumQueryTerms)
	var current strings.Builder
	bracketDepth := 0
	flush := func() {
		if current.Len() == 0 || len(terms) == maximumQueryTerms {
			current.Reset()
			return
		}
		term := current.String()
		current.Reset()
		if term == "AND" || term == "OR" || term == "NOT" || len(term) > 64 {
			return
		}
		terms = append(terms, strings.ToLower(term))
	}
	for _, character := range value {
		if character == '[' {
			flush()
			bracketDepth++
			continue
		}
		if bracketDepth > 0 {
			if character == ']' {
				bracketDepth--
			}
			continue
		}
		if unicode.IsLetter(character) || unicode.IsDigit(character) {
			current.WriteRune(character)
			continue
		}
		flush()
		if len(terms) == maximumQueryTerms {
			break
		}
	}
	flush()
	projected := strings.Join(terms, " ")
	if projected == "" || len(projected) > maximumQueryBytes {
		return "", errors.New("pubmed: query has no bounded lexical terms")
	}
	return projected, nil
}

func unsupportedControl(query search.Query) string {
	switch {
	case len(query.Engines) > 0:
		return "engines"
	case len(query.Categories) > 0:
		return "categories"
	case strings.TrimSpace(query.Language) != "":
		return "language"
	case query.SafeSearch != nil:
		return "safe_search"
	case query.ExactMatch:
		return "exact_match"
	default:
		return ""
	}
}

func freshnessDays(value string) (int, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return 0, nil
	case "day":
		return 1, nil
	case "week":
		return 7, nil
	case "month":
		return 30, nil
	case "year":
		return 365, nil
	default:
		return 0, &search.UnsupportedControlError{Control: "time_range", Reason: "PubMed supports day, week, month, or year publication windows"}
	}
}

func domainAllowed(resultHostname string, includeDomains, excludeDomains []string) bool {
	for _, domain := range excludeDomains {
		if domainMatches(resultHostname, domain) {
			return false
		}
	}
	if len(includeDomains) == 0 {
		return true
	}
	for _, domain := range includeDomains {
		if domainMatches(resultHostname, domain) {
			return true
		}
	}
	return false
}

func domainMatches(hostname, filter string) bool {
	filter = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(filter)), ".")
	hostname = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(hostname)), ".")
	return filter != "" && (hostname == filter || strings.HasSuffix(hostname, "."+filter))
}

func (client *Client) resultLimit(requested int) int {
	if requested > 0 && requested < client.maxResults {
		return requested
	}
	return client.maxResults
}

func (client *Client) cooldownRemaining() time.Duration {
	now := client.now()
	client.mu.Lock()
	defer client.mu.Unlock()
	if !now.Before(client.cooldownUntil) {
		return 0
	}
	return client.cooldownUntil.Sub(now)
}

func (client *Client) openCooldown(duration time.Duration) {
	if duration <= 0 {
		duration = defaultBlockBackoff
	}
	if duration > maxRetryAfter {
		duration = maxRetryAfter
	}
	until := client.now().Add(duration)
	client.mu.Lock()
	if until.After(client.cooldownUntil) {
		client.cooldownUntil = until
	}
	client.mu.Unlock()
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, fmt.Errorf("pubmed: combined response exceeds %d remaining bytes", limit)
	}
	return raw, nil
}

func validUserAgent(userAgent string) bool {
	lower := strings.ToLower(strings.TrimSpace(userAgent))
	hasContact := strings.Contains(lower, "https://") || strings.Contains(lower, "http://") || strings.Contains(lower, "mailto:")
	return len(lower) >= 12 && len(lower) <= 512 && !strings.ContainsAny(lower, "\r\n") && hasContact &&
		strings.Contains(lower, "/") && strings.Contains(lower, "(") && strings.Contains(lower, ")") &&
		!strings.HasPrefix(lower, "go-http-client") && !strings.HasPrefix(lower, "curl/")
}
