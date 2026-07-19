// Package yacy implements bounded discovery against one explicitly configured,
// operator-controlled YaCy node. It never selects public peers as HTTP proxies
// and never fetches result pages on YaCy's behalf.
package yacy

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	stdhtml "html"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	xhtml "golang.org/x/net/html"

	"github.com/staticvar/fetchmark/internal/adapters/providerbudget"
	"github.com/staticvar/fetchmark/internal/core/retryafter"
	"github.com/staticvar/fetchmark/internal/core/search"
)

const (
	searchPath             = "/yacysearch.json"
	defaultBlockBackoff    = time.Minute
	maxRetryAfter          = 24 * time.Hour
	maxProviderResults     = 100
	maxGlobalRate          = 0.2
	maxLocalRate           = 10.0
	maximumQueryInputBytes = 2048
	maximumQueryBytes      = 300
	maximumQueryTerms      = 24
	maximumTitleBytes      = 1024
	maximumSnippetBytes    = 4096
	maximumMarkupBytes     = 8192
)

// Options binds the adapter to one trusted YaCy origin and a process-local
// provider budget. Plain HTTP requires an explicit operator opt-in and should
// be confined to a private service network.
type Options struct {
	Endpoint          string
	HTTPClient        *http.Client
	UserAgent         string
	Resource          string
	AllowInsecureHTTP bool
	MaxResults        int
	MaxBodyBytes      int64
	RatePerSecond     float64
	Burst             int
	MaxConcurrency    int
}

// StatusError retains bounded provider scheduling evidence.
type StatusError struct {
	StatusCode int
	RetryAfter time.Duration
}

func (statusError *StatusError) Error() string {
	return fmt.Sprintf("yacy: upstream status %d", statusError.StatusCode)
}

// Client implements the legacy and provider-aware discovery contracts.
type Client struct {
	endpoint     *url.URL
	httpClient   *http.Client
	userAgent    string
	resource     string
	maxResults   int
	maxBodyBytes int64
	budget       *providerbudget.Gate

	mu            sync.Mutex
	cooldownUntil time.Time
	now           func() time.Time
}

var _ search.Searcher = (*Client)(nil)
var _ search.BatchSearcher = (*Client)(nil)

// New validates an origin-only infrastructure binding and applies conservative
// caps even when an operator-supplied source pack requests more capacity.
func New(options Options) (*Client, error) {
	if options.Endpoint == "" || strings.TrimSpace(options.Endpoint) != options.Endpoint {
		return nil, errors.New("yacy: endpoint origin is required without surrounding whitespace")
	}
	endpoint, err := url.Parse(options.Endpoint)
	if err != nil || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" || endpoint.RawPath != "" || (endpoint.Path != "" && endpoint.Path != "/") {
		return nil, errors.New("yacy: endpoint must be an origin without credentials, path, query, or fragment")
	}
	if endpoint.Scheme != "https" && !(endpoint.Scheme == "http" && options.AllowInsecureHTTP) {
		return nil, errors.New("yacy: endpoint must use HTTPS unless insecure HTTP is explicitly enabled")
	}
	if options.HTTPClient == nil {
		return nil, errors.New("yacy: HTTP client is required")
	}
	if !validUserAgent(options.UserAgent) {
		return nil, errors.New("yacy: descriptive contactable User-Agent is required")
	}
	options.Resource = strings.ToLower(strings.TrimSpace(options.Resource))
	if options.Resource != "local" && options.Resource != "global" {
		return nil, errors.New("yacy: resource must be local or global")
	}
	if options.MaxResults < 1 || options.MaxBodyBytes < 1 || options.RatePerSecond <= 0 || options.Burst < 1 || options.MaxConcurrency < 1 {
		return nil, errors.New("yacy: result, body, rate, burst, and concurrency limits must be positive")
	}
	options.MaxResults = min(options.MaxResults, maxProviderResults)
	maximumRate := maxGlobalRate
	if options.Resource == "local" {
		maximumRate = maxLocalRate
	}
	options.RatePerSecond = min(options.RatePerSecond, maximumRate)
	options.Burst = min(options.Burst, 1)
	options.MaxConcurrency = min(options.MaxConcurrency, 1)
	budget, err := providerbudget.New(options.RatePerSecond, options.Burst, options.MaxConcurrency)
	if err != nil {
		return nil, fmt.Errorf("yacy: configure provider budget: %w", err)
	}
	endpoint.Path = searchPath
	return &Client{
		endpoint: endpoint, httpClient: singleAttemptHTTPClient(options.HTTPClient, endpoint),
		userAgent: options.UserAgent, resource: options.Resource,
		maxResults: options.MaxResults, maxBodyBytes: options.MaxBodyBytes,
		budget: budget, now: time.Now,
	}, nil
}

// singleAttemptHTTPClient makes one provider-budget token correspond to one
// outbound HTTP attempt for the standard transport. Redirects are always
// refused, including for injected test or operator transports.
func singleAttemptHTTPClient(original *http.Client, endpoint *url.URL) *http.Client {
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
	client.Transport = &exactOriginTransport{base: client.Transport, scheme: endpoint.Scheme, host: endpoint.Host, path: endpoint.Path}
	return &client
}

type exactOriginTransport struct {
	base               http.RoundTripper
	scheme, host, path string
}

func (transport *exactOriginTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil || request.URL == nil || request.URL.User != nil || request.URL.Scheme != transport.scheme || !strings.EqualFold(request.URL.Host, transport.host) || request.URL.Path != transport.path || request.URL.Fragment != "" {
		return nil, errors.New("yacy: request is outside configured origin and search path")
	}
	return transport.base.RoundTrip(request)
}

func (client *Client) Search(ctx context.Context, query search.Query) ([]search.Hit, error) {
	batch, err := client.SearchBatch(ctx, query)
	if err != nil {
		return nil, err
	}
	return batch.Hits, nil
}

// SearchBatch performs one first-page metadata request. verify=false prevents
// YaCy from fetching result pages; Fetchmark's ordinary SSRF/robots/noindex
// pipeline remains the only result-content retrieval path.
func (client *Client) SearchBatch(ctx context.Context, query search.Query) (search.SearchBatch, error) {
	started := time.Now()
	fail := func(source, reason string, retryable bool, retryAfter time.Duration, err error) (search.SearchBatch, error) {
		return search.SearchBatch{
			Provider: "yacy", Instance: client.endpoint.Host, Status: search.BatchFailed,
			Diagnostics: []search.ProviderDiagnostic{{
				Provider: "yacy", Instance: client.endpoint.Host, Source: source,
				Reason: reason, Retryable: retryable, RetryAfter: retryAfter,
			}},
			Duration: time.Since(started),
		}, err
	}
	if err := ctx.Err(); err != nil {
		return fail("search", "canceled", false, 0, err)
	}
	if control := unsupportedControl(query); control != "" {
		err := &search.UnsupportedControlError{Control: control, Reason: "YaCy cannot preserve this search control"}
		return fail(control, "unsupported_control", false, 0, err)
	}
	projected, err := projectQuery(query.Q)
	if err != nil {
		return fail("query", "invalid_query", false, 0, err)
	}
	if wait := client.cooldownRemaining(); wait > 0 {
		return fail("search", "cooldown", true, wait, &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: wait})
	}
	release, err := client.budget.AcquireSlot(ctx)
	if err != nil {
		return fail("search", "canceled", false, 0, err)
	}
	defer release()
	if wait := client.cooldownRemaining(); wait > 0 {
		return fail("search", "cooldown", true, wait, &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: wait})
	}
	if err := client.budget.WaitRate(ctx); err != nil {
		return fail("search", "canceled", false, 0, err)
	}
	if wait := client.cooldownRemaining(); wait > 0 {
		return fail("search", "cooldown", true, wait, &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: wait})
	}

	resultLimit := client.resultLimit(query.MaxResults)
	endpoint := *client.endpoint
	values := make(url.Values)
	values.Set("query", projected)
	values.Set("resource", client.resource)
	values.Set("contentdom", "text")
	values.Set("maximumRecords", strconv.Itoa(resultLimit))
	values.Set("startRecord", "0")
	values.Set("verify", "false")
	values.Set("nav", "none")
	values.Set("urlmaskfilter", ".*")
	values.Set("prefermaskfilter", "")
	endpoint.RawQuery = values.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return fail("search", "request", false, 0, err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", client.userAgent)
	response, err := client.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return fail("search", "canceled", false, 0, ctx.Err())
		}
		return fail("search", "network", true, 0, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		wait := retryafter.Parse(response.Header.Get("Retry-After"), client.now(), maxRetryAfter)
		retryable := response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500
		if response.StatusCode == http.StatusTooManyRequests && wait == 0 {
			wait = defaultBlockBackoff
		}
		if wait > 0 {
			client.openCooldown(wait)
		}
		return fail("search", "http_"+strconv.Itoa(response.StatusCode), retryable, wait, &StatusError{StatusCode: response.StatusCode, RetryAfter: wait})
	}
	raw, err := readBounded(response.Body, client.maxBodyBytes)
	if err != nil {
		return fail("search", "oversized", false, 0, err)
	}
	var payload responseEnvelope
	if err := json.Unmarshal(raw, &payload); err != nil {
		return fail("search", "malformed", false, 0, fmt.Errorf("yacy: decode response: %w", err))
	}
	if len(payload.Channels) != 1 {
		return fail("search", "malformed", false, 0, errors.New("yacy: response must contain exactly one search channel"))
	}
	channel := payload.Channels[0]
	effectiveResource := parseResource(channel.Link)
	resourceReason := resourceDiagnostic(client.resource, effectiveResource)

	includeFilters := parseDomainFilters(query.IncludeDomains)
	excludeFilters := parseDomainFilters(query.ExcludeDomains)
	hits := make([]search.Hit, 0, min(resultLimit, len(channel.Items)))
	invalidRows := 0
	validRows := 0
	observedAt := client.now().UTC()
	for _, item := range channel.Items {
		if len(hits) == resultLimit {
			break
		}
		parsed, ok := validResultURL(item.Link)
		title := cleanText(item.Title, maximumTitleBytes)
		if !ok || title == "" {
			invalidRows++
			continue
		}
		validRows++
		if !domainAllowed(parsed, includeFilters, excludeFilters) {
			continue
		}
		resource := client.resource
		metadata := map[string]string{"source": "YaCy", "yacy_requested_resource": client.resource}
		if effectiveResource != "" {
			resource = effectiveResource
			metadata["yacy_effective_resource"] = effectiveResource
		}
		metadata["yacy_resource"] = resource
		if ranking := cleanText(item.Ranking, 64); ranking != "" {
			metadata["yacy_ranking"] = ranking
		}
		if host := cleanText(item.Host, 255); host != "" {
			metadata["yacy_reported_host"] = host
		}
		hits = append(hits, search.Hit{
			URL: parsed.String(), Title: title, Snippet: cleanMarkup(item.Description),
			Engines: []string{"yacy"}, PublishedAt: parsePublicationDate(item.PubDate, observedAt), Metadata: metadata,
		})
	}
	if invalidRows > 0 && validRows == 0 {
		return fail("search", "malformed_results", false, 0, errors.New("yacy: response contained no valid results"))
	}

	diagnostics := make([]search.ProviderDiagnostic, 0, 2)
	appendDiagnostic := func(reason string) {
		diagnostics = append(diagnostics, search.ProviderDiagnostic{
			Provider: "yacy", Instance: client.endpoint.Host, Source: "search", Reason: reason,
		})
	}
	status := search.BatchHealthy
	if len(hits) == 0 {
		if client.resource == "local" && effectiveResource == "local" {
			status = search.BatchAuthoritativeEmpty
		} else {
			status = search.BatchDegradedEmpty
		}
	}
	if resourceReason != "" {
		appendDiagnostic(resourceReason)
		if len(hits) > 0 {
			status = search.BatchPartial
		} else {
			status = search.BatchDegradedEmpty
		}
	} else if client.resource == "global" && len(hits) == 0 {
		appendDiagnostic("global_empty")
	}
	if invalidRows > 0 {
		appendDiagnostic("malformed_results")
		if len(hits) > 0 {
			status = search.BatchPartial
		} else {
			status = search.BatchDegradedEmpty
		}
	}
	return search.SearchBatch{
		Hits: hits, Provider: "yacy", Instance: client.endpoint.Host, Status: status,
		Diagnostics: diagnostics, Duration: time.Since(started),
	}, nil
}

type responseEnvelope struct {
	Channels []channel `json:"channels"`
}

type channel struct {
	Link  string `json:"link"`
	Items []item `json:"items"`
}

type item struct {
	Title       string `json:"title"`
	Link        string `json:"link"`
	Description string `json:"description"`
	PubDate     string `json:"pubDate"`
	Host        string `json:"host"`
	Ranking     string `json:"ranking"`
}

func parseResource(raw string) string {
	parsed, err := url.Parse(stdhtml.UnescapeString(strings.TrimSpace(raw)))
	if err != nil {
		return ""
	}
	resource := strings.ToLower(strings.TrimSpace(parsed.Query().Get("resource")))
	if resource != "local" && resource != "global" {
		return ""
	}
	return resource
}

func resourceDiagnostic(requested, effective string) string {
	if effective == "" {
		return "resource_unverified"
	}
	if effective == requested {
		return ""
	}
	if requested == "global" && effective == "local" {
		return "global_downgraded"
	}
	return "resource_mismatch"
}

func validResultURL(raw string) (*url.URL, bool) {
	if len(raw) == 0 || len(raw) > 8192 || !utf8.ValidString(raw) {
		return nil, false
	}
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.User != nil || parsed.Hostname() == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, false
	}
	return parsed, true
}

func cleanMarkup(fragment string) string {
	if fragment == "" || len(fragment) > maximumMarkupBytes || !utf8.ValidString(fragment) {
		return ""
	}
	tokenizer := xhtml.NewTokenizer(strings.NewReader(fragment))
	var text strings.Builder
	skipDepth := 0
	for {
		tokenType := tokenizer.Next()
		switch tokenType {
		case xhtml.ErrorToken:
			if tokenizer.Err() != io.EOF {
				return ""
			}
			cleaned := strings.Join(strings.Fields(text.String()), " ")
			if len(cleaned) > maximumSnippetBytes {
				return ""
			}
			return cleaned
		case xhtml.StartTagToken:
			name, _ := tokenizer.TagName()
			if skipDepth > 0 || strings.EqualFold(string(name), "script") || strings.EqualFold(string(name), "style") {
				skipDepth++
			}
		case xhtml.EndTagToken:
			if skipDepth > 0 {
				skipDepth--
			}
		case xhtml.TextToken:
			if skipDepth == 0 {
				text.WriteString(stdhtml.UnescapeString(string(tokenizer.Text())))
				text.WriteByte(' ')
			}
		}
	}
}

func cleanText(value string, maximumBytes int) string {
	if !utf8.ValidString(value) {
		return ""
	}
	value = strings.Join(strings.Fields(stdhtml.UnescapeString(value)), " ")
	if value == "" || len(value) > maximumBytes || strings.ContainsRune(value, '\x00') {
		return ""
	}
	return value
}

func parsePublicationDate(value string, observedAt time.Time) *time.Time {
	parsed, err := http.ParseTime(strings.TrimSpace(value))
	if err != nil || parsed.Year() < 1000 || parsed.After(observedAt.AddDate(5, 0, 0)) {
		return nil
	}
	parsed = parsed.UTC()
	return &parsed
}

func projectQuery(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maximumQueryInputBytes || !utf8.ValidString(value) {
		return "", errors.New("yacy: query is empty, invalid, or oversized")
	}
	terms := make([]string, 0, maximumQueryTerms)
	var current strings.Builder
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
		if unicode.IsLetter(character) || unicode.IsDigit(character) {
			current.WriteRune(character)
		} else {
			flush()
		}
		if len(terms) == maximumQueryTerms {
			break
		}
	}
	flush()
	projected := strings.Join(terms, " ")
	if projected == "" || len(projected) > maximumQueryBytes {
		return "", errors.New("yacy: query has no bounded lexical terms")
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
	case strings.TrimSpace(query.TimeRange) != "":
		return "time_range"
	case query.SafeSearch != nil:
		return "safe_search"
	case query.ExactMatch:
		return "exact_match"
	default:
		return ""
	}
}

type domainFilter struct {
	host string
	path string
}

func parseDomainFilters(raw []string) []domainFilter {
	filters := make([]domainFilter, 0, len(raw))
	for _, item := range raw {
		item = strings.TrimSpace(strings.ToLower(item))
		if item == "" {
			continue
		}
		if !strings.Contains(item, "://") {
			item = "https://" + item
		}
		parsed, err := url.Parse(item)
		if err != nil {
			continue
		}
		host := strings.TrimPrefix(parsed.Hostname(), "*.")
		host = strings.TrimPrefix(host, "www.")
		if host == "" {
			continue
		}
		filters = append(filters, domainFilter{host: host, path: strings.TrimRight(parsed.EscapedPath(), "/")})
	}
	return filters
}

func domainAllowed(target *url.URL, includes, excludes []domainFilter) bool {
	if len(includes) > 0 && !matchesAnyDomainFilter(target, includes) {
		return false
	}
	return !matchesAnyDomainFilter(target, excludes)
}

func matchesAnyDomainFilter(target *url.URL, filters []domainFilter) bool {
	host := strings.TrimPrefix(strings.ToLower(target.Hostname()), "www.")
	path := strings.TrimRight(target.EscapedPath(), "/")
	for _, filter := range filters {
		if host != filter.host && !strings.HasSuffix(host, "."+filter.host) {
			continue
		}
		if filter.path != "" && !strings.HasPrefix(path, filter.path) {
			continue
		}
		return true
	}
	return false
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

func readBounded(reader io.Reader, maximum int64) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(reader, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maximum {
		return nil, errors.New("yacy: response exceeds body limit")
	}
	return raw, nil
}

func validUserAgent(value string) bool {
	value = strings.TrimSpace(value)
	return len(value) >= 12 && len(value) <= 512 && !strings.ContainsAny(value, "\r\n") && strings.Contains(value, "/") && strings.Contains(value, "(") && strings.Contains(value, ")") &&
		(strings.Contains(value, "http://") || strings.Contains(value, "https://") || strings.Contains(value, "mailto:"))
}
