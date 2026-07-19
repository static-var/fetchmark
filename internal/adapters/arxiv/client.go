// Package arxiv implements bounded, no-key research discovery through arXiv's
// public Atom API. It returns descriptive metadata and abstract-page URLs;
// article bodies remain subject to Fetchmark's ordinary robots-aware fetch path.
package arxiv

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/staticvar/fetchmark/internal/adapters/providerbudget"
	"github.com/staticvar/fetchmark/internal/core/retryafter"
	"github.com/staticvar/fetchmark/internal/core/search"
)

const (
	defaultEndpoint     = "https://export.arxiv.org/api/query"
	defaultBlockBackoff = 5 * time.Minute
	maxRetryAfter       = 24 * time.Hour
	maxProviderResults  = 100
	maxProviderRate     = 1.0 / 3.0
	metadataLicense     = "CC0-1.0"
	metadataLicenseURL  = "https://creativecommons.org/publicdomain/zero/1.0/"
	attributionText     = "Thank you to arXiv for use of its open access interoperability."
	maxQueryTerms       = 12
	maxQueryTermBytes   = 48
	maxTitleBytes       = 512
	maxSummaryBytes     = 4096
	maxMetadataBytes    = 1024
	maxMetadataItems    = 32
)

var arxivIDPattern = regexp.MustCompile(`^(?:[0-9]{4}\.[0-9]{4,5}|[A-Za-z][A-Za-z0-9.-]*/[0-9]{7})(?:v[0-9]+)?$`)

// Options defines mandatory process-wide public-service budgets. New applies
// arXiv's published request-frequency and single-connection ceilings even when
// operator configuration asks for more.
type Options struct {
	Endpoint       string
	HTTPClient     *http.Client
	UserAgent      string
	MaxResults     int
	MaxBodyBytes   int64
	RatePerSecond  float64
	Burst          int
	MaxConcurrency int
}

// StatusError retains bounded provider backoff evidence.
type StatusError struct {
	StatusCode int
	RetryAfter time.Duration
}

func (errorValue *StatusError) Error() string {
	return fmt.Sprintf("arxiv: upstream status %d", errorValue.StatusCode)
}

// Client implements both compatibility and rich discovery contracts.
type Client struct {
	endpoint     *url.URL
	httpClient   *http.Client
	userAgent    string
	maxResults   int
	maxBodyBytes int64
	budget       *providerbudget.Gate

	mu            sync.Mutex
	cooldownUntil time.Time
	now           func() time.Time
}

var _ search.Searcher = (*Client)(nil)
var _ search.BatchSearcher = (*Client)(nil)

// New pins requests to the official HTTPS query endpoint and hard-caps the
// process to one connection and no more than one request every three seconds.
func New(options Options) (*Client, error) {
	if options.Endpoint == "" {
		options.Endpoint = defaultEndpoint
	}
	endpoint, err := url.Parse(options.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host != "export.arxiv.org" ||
		endpoint.Path != "/api/query" || endpoint.RawPath != "" || endpoint.RawQuery != "" ||
		endpoint.Fragment != "" || endpoint.User != nil {
		return nil, errors.New("arxiv: endpoint must be the official HTTPS query API")
	}
	if options.HTTPClient == nil {
		return nil, errors.New("arxiv: HTTP client is required")
	}
	if !validUserAgent(options.UserAgent) {
		return nil, errors.New("arxiv: descriptive contactable User-Agent is required")
	}
	if options.MaxResults < 1 || options.MaxBodyBytes < 1 || options.RatePerSecond <= 0 ||
		options.Burst < 1 || options.MaxConcurrency < 1 {
		return nil, errors.New("arxiv: result, body, rate, burst, and concurrency limits must be positive")
	}
	if options.MaxResults > maxProviderResults {
		options.MaxResults = maxProviderResults
	}
	if options.RatePerSecond > maxProviderRate {
		options.RatePerSecond = maxProviderRate
	}
	if options.Burst > 1 {
		options.Burst = 1
	}
	if options.MaxConcurrency > 1 {
		options.MaxConcurrency = 1
	}
	budget, err := providerbudget.New(options.RatePerSecond, options.Burst, options.MaxConcurrency)
	if err != nil {
		return nil, fmt.Errorf("arxiv: configure provider budget: %w", err)
	}
	return &Client{
		endpoint: endpoint, httpClient: singleAttemptHTTPClient(options.HTTPClient), userAgent: options.UserAgent,
		maxResults: options.MaxResults, maxBodyBytes: options.MaxBodyBytes,
		budget: budget, now: time.Now,
	}, nil
}

// singleAttemptHTTPClient makes the adapter's rate token match exactly one
// request attempt. Redirects and net/http's transparent reused-connection
// replay would otherwise escape the provider budget.
func singleAttemptHTTPClient(original *http.Client) *http.Client {
	client := *original
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
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

// SearchBatch performs one first-page metadata request without retrying the
// shared public service inside the caller's latency budget.
func (client *Client) SearchBatch(ctx context.Context, query search.Query) (search.SearchBatch, error) {
	started := time.Now()
	fail := func(reason, control string, retryable bool, wait time.Duration, err error) (search.SearchBatch, error) {
		diagnostic := search.ProviderDiagnostic{
			Provider: "arxiv", Instance: "export.arxiv.org", Source: "atom_api",
			Reason: reason, Retryable: retryable, RetryAfter: wait,
		}
		if control != "" {
			diagnostic.Source = control
		}
		return search.SearchBatch{
			Provider: "arxiv", Instance: "export.arxiv.org", Status: search.BatchFailed,
			Diagnostics: []search.ProviderDiagnostic{diagnostic}, Duration: time.Since(started),
		}, err
	}
	if err := ctx.Err(); err != nil {
		return fail("canceled", "", false, 0, err)
	}
	if control := unsupportedControl(query); control != "" {
		err := &search.UnsupportedControlError{Control: control, Reason: "arXiv does not support this search control"}
		return fail("unsupported_control", control, false, 0, err)
	}
	searchExpression, err := queryExpression(query, client.now())
	if err != nil {
		return fail("invalid_query", "query", false, 0, err)
	}
	if wait := client.cooldownRemaining(); wait > 0 {
		err := &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: wait}
		return fail("cooldown", "", true, wait, err)
	}
	release, err := client.budget.AcquireSlot(ctx)
	if err != nil {
		return fail("canceled", "", false, 0, err)
	}
	defer release()
	if wait := client.cooldownRemaining(); wait > 0 {
		err := &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: wait}
		return fail("cooldown", "", true, wait, err)
	}
	if err := client.budget.WaitRate(ctx); err != nil {
		return fail("canceled", "", false, 0, err)
	}
	if wait := client.cooldownRemaining(); wait > 0 {
		err := &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: wait}
		return fail("cooldown", "", true, wait, err)
	}

	endpoint := *client.endpoint
	values := endpoint.Query()
	values.Set("search_query", searchExpression)
	values.Set("start", "0")
	values.Set("max_results", strconv.Itoa(client.resultLimit(query.MaxResults)))
	if strings.TrimSpace(query.TimeRange) == "" {
		values.Set("sortBy", "relevance")
	} else {
		values.Set("sortBy", "submittedDate")
	}
	values.Set("sortOrder", "descending")
	endpoint.RawQuery = values.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return fail("request", "", false, 0, err)
	}
	request.Header.Set("Accept", "application/atom+xml")
	request.Header.Set("User-Agent", client.userAgent)
	response, err := client.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return fail("canceled", "", false, 0, ctx.Err())
		}
		return fail("network", "", true, 0, err)
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
		statusErr := &StatusError{StatusCode: response.StatusCode, RetryAfter: wait}
		return fail("http_"+strconv.Itoa(response.StatusCode), "", retryable, wait, statusErr)
	}
	raw, err := readBounded(response.Body, client.maxBodyBytes)
	if err != nil {
		return fail("oversized", "", false, 0, err)
	}
	var payload atomFeed
	decoder := xml.NewDecoder(bytes.NewReader(raw))
	decoder.Strict = true
	if err := decoder.Decode(&payload); err != nil {
		return fail("malformed", "", false, 0, fmt.Errorf("arxiv: decode Atom response: %w", err))
	}

	hits := make([]search.Hit, 0, min(client.resultLimit(query.MaxResults), len(payload.Entries)))
	invalid := 0
	for _, entry := range payload.Entries {
		if len(hits) >= client.resultLimit(query.MaxResults) {
			break
		}
		hit, ok := entry.hit()
		if !ok {
			invalid++
			continue
		}
		hits = append(hits, hit)
	}
	status := search.BatchAuthoritativeEmpty
	diagnostics := []search.ProviderDiagnostic(nil)
	if len(hits) > 0 {
		status = search.BatchHealthy
	}
	if !payload.TotalResults.Present {
		diagnostics = []search.ProviderDiagnostic{{
			Provider: "arxiv", Instance: "export.arxiv.org", Source: "atom_api", Reason: "missing_total_results",
		}}
		if len(hits) > 0 {
			status = search.BatchPartial
		} else {
			status = search.BatchDegradedEmpty
		}
	} else if invalid > 0 ||
		(payload.TotalResults.Value > 0 && len(payload.Entries) == 0) ||
		payload.TotalResults.Value < len(payload.Entries) {
		diagnostics = []search.ProviderDiagnostic{{
			Provider: "arxiv", Instance: "export.arxiv.org", Source: "atom_api", Reason: "malformed_results",
		}}
		if len(hits) > 0 {
			status = search.BatchPartial
		} else {
			status = search.BatchDegradedEmpty
		}
	}
	return search.SearchBatch{
		Hits: hits, Provider: "arxiv", Instance: "export.arxiv.org", Status: status,
		Diagnostics: diagnostics, Duration: time.Since(started),
	}, nil
}

type atomFeed struct {
	XMLName      xml.Name     `xml:"feed"`
	TotalResults totalResults `xml:"totalResults"`
	Entries      []atomEntry  `xml:"entry"`
}

type totalResults struct {
	Present bool
	Value   int
}

func (result *totalResults) UnmarshalXML(decoder *xml.Decoder, start xml.StartElement) error {
	var raw string
	if err := decoder.DecodeElement(&raw, &start); err != nil {
		return err
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value < 0 {
		return errors.New("arxiv: invalid totalResults")
	}
	result.Present = true
	result.Value = value
	return nil
}

type atomEntry struct {
	ID              string         `xml:"id"`
	Updated         string         `xml:"updated"`
	Published       string         `xml:"published"`
	Title           string         `xml:"title"`
	Summary         string         `xml:"summary"`
	Authors         []atomAuthor   `xml:"author"`
	Categories      []atomCategory `xml:"category"`
	PrimaryCategory atomCategory   `xml:"primary_category"`
	DOI             string         `xml:"doi"`
	JournalRef      string         `xml:"journal_ref"`
	License         atomLicense    `xml:"license"`
}

type atomAuthor struct {
	Name string `xml:"name"`
}

type atomCategory struct {
	Term string `xml:"term,attr"`
}

type atomLicense struct {
	Href string `xml:"href,attr"`
	Text string `xml:",chardata"`
}

func (entry atomEntry) hit() (search.Hit, bool) {
	id, resultURL, ok := canonicalAbstractURL(entry.ID)
	if !ok {
		return search.Hit{}, false
	}
	title, ok := boundedText(entry.Title, maxTitleBytes, false)
	if !ok || title == "" {
		return search.Hit{}, false
	}
	published, err := time.Parse(time.RFC3339, strings.TrimSpace(entry.Published))
	if err != nil {
		return search.Hit{}, false
	}
	metadata := map[string]string{
		"source": "arXiv", "arxiv_id": id, "metadata_license": metadataLicense,
		"metadata_license_url": metadataLicenseURL,
		"attribution":          attributionText,
	}
	authors := boundedAuthors(entry.Authors)
	if len(authors) > 0 {
		metadata["authors"] = strings.Join(authors, ", ")
	}
	categories := boundedCategories(entry.Categories)
	if len(categories) > 0 {
		metadata["categories"] = strings.Join(categories, ", ")
	}
	if value, ok := boundedText(entry.PrimaryCategory.Term, maxMetadataBytes, false); ok && value != "" {
		metadata["primary_category"] = value
	}
	if value, ok := boundedText(entry.DOI, maxMetadataBytes, false); ok && value != "" {
		metadata["doi"] = value
	}
	if value, ok := boundedText(entry.JournalRef, maxMetadataBytes, false); ok && value != "" {
		metadata["journal_ref"] = value
	}
	if updated, parseErr := time.Parse(time.RFC3339, strings.TrimSpace(entry.Updated)); parseErr == nil {
		metadata["updated_at"] = updated.UTC().Format(time.RFC3339)
	} else if strings.TrimSpace(entry.Updated) != "" {
		return search.Hit{}, false
	}
	if licenseURL := entry.License.value(); licenseURL != "" {
		metadata["article_license_url"] = licenseURL
	}
	summary, _ := boundedText(entry.Summary, maxSummaryBytes, true)
	published = published.UTC()
	return search.Hit{
		URL: resultURL, Title: title, Snippet: summary, Engines: []string{"arxiv"},
		PublishedAt: &published, Metadata: metadata,
	}, true
}

func canonicalAbstractURL(raw string) (string, string, bool) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host != "arxiv.org" ||
		parsed.User != nil || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.Fragment != "" ||
		!strings.HasPrefix(parsed.Path, "/abs/") {
		return "", "", false
	}
	id := strings.TrimPrefix(parsed.Path, "/abs/")
	if !arxivIDPattern.MatchString(id) {
		return "", "", false
	}
	return id, "https://arxiv.org/abs/" + id, true
}

func queryExpression(query search.Query, now time.Time) (string, error) {
	tokens := literalTerms(query.Q)
	if query.ExactMatch {
		tokens = exactTerms(query.Q)
	}
	if len(tokens) == 0 {
		return "", errors.New("arxiv: query requires at least one literal search term")
	}
	var expression string
	if query.ExactMatch {
		expression = `all:"` + strings.Join(tokens, " ") + `"`
	} else {
		parts := make([]string, 0, len(tokens))
		for _, token := range tokens {
			parts = append(parts, "all:"+token)
		}
		expression = strings.Join(parts, " AND ")
	}
	if timeRange := strings.TrimSpace(query.TimeRange); timeRange != "" {
		start, ok := timeRangeStart(timeRange, now)
		if !ok {
			return "", errors.New("arxiv: unsupported time range")
		}
		expression += " AND submittedDate:[" + start.UTC().Format("200601021504") + " TO " + now.UTC().Format("200601021504") + "]"
	}
	return expression, nil
}

func literalTerms(raw string) []string {
	return boundedQueryTerms(raw, true)
}

func exactTerms(raw string) []string {
	return boundedQueryTerms(raw, false)
}

func boundedQueryTerms(raw string, removeStopwords bool) []string {
	fields := strings.FieldsFunc(strings.ToLower(strings.TrimSpace(raw)), func(character rune) bool {
		return !unicode.IsLetter(character) && !unicode.IsDigit(character)
	})
	terms := make([]string, 0, min(len(fields), maxQueryTerms))
	for _, field := range fields {
		_, stop := queryStopwords[field]
		if (removeStopwords && stop) || len(field) == 0 || len(field) > maxQueryTermBytes {
			continue
		}
		terms = append(terms, field)
		if len(terms) == maxQueryTerms {
			break
		}
	}
	return terms
}

func (license atomLicense) value() string {
	rawText := strings.TrimSpace(license.Text)
	rawHref := strings.TrimSpace(license.Href)
	text := validatedHTTPSURL(rawText)
	href := validatedHTTPSURL(rawHref)
	if (rawText != "" && text == "") || (rawHref != "" && href == "") ||
		(text != "" && href != "" && text != href) {
		return ""
	}
	if text != "" {
		return text
	}
	return href
}

var queryStopwords = map[string]struct{}{
	"a": {}, "about": {}, "an": {}, "and": {}, "are": {}, "for": {}, "from": {}, "how": {},
	"in": {}, "is": {}, "latest": {}, "of": {}, "on": {}, "or": {}, "paper": {}, "papers": {},
	"recent": {}, "the": {}, "to": {}, "what": {}, "when": {}, "where": {}, "which": {}, "who": {}, "why": {}, "with": {},
}

func unsupportedControl(query search.Query) string {
	if len(query.Engines) > 0 {
		return "engines"
	}
	if len(query.Categories) > 0 {
		return "categories"
	}
	if language := strings.TrimSpace(query.Language); language != "" && !strings.EqualFold(language, "auto") {
		return "language"
	}
	if query.SafeSearch != nil {
		return "safesearch"
	}
	if len(query.IncludeDomains) > 0 {
		return "include_domains"
	}
	if len(query.ExcludeDomains) > 0 {
		return "exclude_domains"
	}
	if timeRange := strings.ToLower(strings.TrimSpace(query.TimeRange)); timeRange != "" {
		if _, ok := timeRangeStart(timeRange, time.Now()); !ok {
			return "time_range"
		}
	}
	return ""
}

func timeRangeStart(value string, now time.Time) (time.Time, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "day":
		return now.AddDate(0, 0, -1), true
	case "week":
		return now.AddDate(0, 0, -7), true
	case "month":
		return now.AddDate(0, -1, 0), true
	case "year":
		return now.AddDate(-1, 0, 0), true
	default:
		return time.Time{}, false
	}
}

func boundedAuthors(raw []atomAuthor) []string {
	values := make([]string, 0, min(len(raw), maxMetadataItems))
	for _, author := range raw {
		value, ok := boundedText(author.Name, maxMetadataBytes, false)
		if ok && value != "" {
			values = append(values, value)
		}
		if len(values) == maxMetadataItems {
			break
		}
	}
	return values
}

func boundedCategories(raw []atomCategory) []string {
	values := make([]string, 0, min(len(raw), maxMetadataItems))
	for _, category := range raw {
		value := strings.TrimSpace(category.Term)
		if value != "" && len(value) <= maxMetadataBytes && strings.IndexFunc(value, func(character rune) bool {
			return !unicode.IsLetter(character) && !unicode.IsDigit(character) && character != '.' && character != '-'
		}) == -1 {
			values = append(values, value)
		}
		if len(values) == maxMetadataItems {
			break
		}
	}
	return values
}

func boundedText(raw string, maximum int, truncate bool) (string, bool) {
	value := strings.Join(strings.Fields(raw), " ")
	if len(value) <= maximum {
		return value, true
	}
	if !truncate {
		return "", false
	}
	value = value[:maximum]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return strings.TrimSpace(value), true
}

func validatedHTTPSURL(raw string) string {
	if len(raw) > maxMetadataBytes {
		return ""
	}
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil {
		return ""
	}
	return parsed.String()
}

func (client *Client) resultLimit(requested int) int {
	if requested > 0 && requested < client.maxResults {
		return requested
	}
	return client.maxResults
}

func validUserAgent(value string) bool {
	lower := strings.ToLower(strings.TrimSpace(value))
	hasContact := strings.Contains(lower, "https://") || strings.Contains(lower, "http://") || strings.Contains(lower, "mailto:")
	return len(lower) >= 12 && len(lower) <= 512 && !strings.ContainsAny(lower, "\r\n") && hasContact &&
		strings.Contains(lower, "/") && strings.Contains(lower, "(") && strings.Contains(lower, ")") &&
		!strings.HasPrefix(lower, "go-http-client") && !strings.HasPrefix(lower, "curl/")
}

func readBounded(reader io.Reader, maximum int64) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(reader, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maximum {
		return nil, fmt.Errorf("arxiv: response exceeds %d bytes", maximum)
	}
	return raw, nil
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
