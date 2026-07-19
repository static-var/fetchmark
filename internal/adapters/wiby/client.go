// Package wiby implements a bounded no-key adapter for Wiby's official JSON
// search API. Wiby is an opportunistic general-web lane and requires clients
// to link back to wiby.me alongside returned results.
package wiby

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/providerbudget"
	"github.com/staticvar/fetchmark/internal/core/retryafter"
	"github.com/staticvar/fetchmark/internal/core/search"
)

const (
	defaultEndpoint     = "https://wiby.me/json/"
	attributionURL      = "https://wiby.me/"
	defaultBlockBackoff = 5 * time.Minute
	maxRetryAfter       = 24 * time.Hour
	maxProviderResults  = 12
	maxProviderRate     = 0.2
)

// Options defines mandatory process-wide shared-service budgets.
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

func (e *StatusError) Error() string {
	return fmt.Sprintf("wiby: upstream status %d", e.StatusCode)
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

// New pins requests to Wiby's official HTTPS JSON endpoint and applies hard
// caps that operator configuration cannot relax.
func New(options Options) (*Client, error) {
	if options.Endpoint == "" {
		options.Endpoint = defaultEndpoint
	}
	endpoint, err := url.Parse(options.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host != "wiby.me" || endpoint.Path != "/json/" || endpoint.RawPath != "" || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.User != nil {
		return nil, errors.New("wiby: endpoint must be the official HTTPS JSON API")
	}
	if options.HTTPClient == nil {
		return nil, errors.New("wiby: HTTP client is required")
	}
	if !validUserAgent(options.UserAgent) {
		return nil, errors.New("wiby: descriptive contactable User-Agent is required")
	}
	if options.MaxResults < 1 || options.MaxBodyBytes < 1 || options.RatePerSecond <= 0 || options.Burst < 1 || options.MaxConcurrency < 1 {
		return nil, errors.New("wiby: result, body, rate, burst, and concurrency limits must be positive")
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
		return nil, fmt.Errorf("wiby: configure provider budget: %w", err)
	}
	return &Client{
		endpoint: endpoint, httpClient: options.HTTPClient, userAgent: options.UserAgent,
		maxResults: options.MaxResults, maxBodyBytes: options.MaxBodyBytes,
		budget: budget, now: time.Now,
	}, nil
}

func (c *Client) Search(ctx context.Context, query search.Query) ([]search.Hit, error) {
	batch, err := c.SearchBatch(ctx, query)
	if err != nil {
		return nil, err
	}
	return batch.Hits, nil
}

// SearchBatch performs one bounded first-page metadata request. Fetchmark does
// not page implicitly because every public-service request spends shared Wiby
// capacity and may produce substantially less relevant results.
func (c *Client) SearchBatch(ctx context.Context, query search.Query) (search.SearchBatch, error) {
	started := time.Now()
	fail := func(reason, control string, retryable bool, retryAfter time.Duration, err error) (search.SearchBatch, error) {
		diagnostic := search.ProviderDiagnostic{
			Provider: "wiby", Instance: "wiby.me", Source: "official_api",
			Reason: reason, Retryable: retryable, RetryAfter: retryAfter,
		}
		if control != "" {
			diagnostic.Source = control
		}
		return search.SearchBatch{
			Provider: "wiby", Instance: "wiby.me", Status: search.BatchFailed,
			Diagnostics: []search.ProviderDiagnostic{diagnostic}, Duration: time.Since(started),
		}, err
	}
	if err := ctx.Err(); err != nil {
		return fail("canceled", "", false, 0, err)
	}
	if control := unsupportedControl(query); control != "" {
		err := &search.UnsupportedControlError{Control: control, Reason: "Wiby does not support this search control"}
		return fail("unsupported_control", control, false, 0, err)
	}
	queryText := strings.Join(strings.Fields(query.Q), " ")
	if queryText == "" {
		return fail("invalid_query", "query", false, 0, errors.New("wiby: query is required"))
	}
	if retryAfter := c.cooldownRemaining(); retryAfter > 0 {
		err := &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: retryAfter}
		return fail("cooldown", "", true, retryAfter, err)
	}
	release, err := c.budget.AcquireSlot(ctx)
	if err != nil {
		return fail("canceled", "", false, 0, err)
	}
	defer release()
	if retryAfter := c.cooldownRemaining(); retryAfter > 0 {
		err := &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: retryAfter}
		return fail("cooldown", "", true, retryAfter, err)
	}
	if err := c.budget.WaitRate(ctx); err != nil {
		return fail("canceled", "", false, 0, err)
	}
	if retryAfter := c.cooldownRemaining(); retryAfter > 0 {
		err := &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: retryAfter}
		return fail("cooldown", "", true, retryAfter, err)
	}

	endpoint := *c.endpoint
	values := endpoint.Query()
	values.Set("q", queryText)
	values.Set("p", "0")
	endpoint.RawQuery = values.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return fail("request", "", false, 0, err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", c.userAgent)
	response, err := c.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return fail("canceled", "", false, 0, ctx.Err())
		}
		return fail("network", "", true, 0, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		retryAfter := retryafter.Parse(response.Header.Get("Retry-After"), c.now(), maxRetryAfter)
		retryable := response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500
		if (response.StatusCode == http.StatusForbidden || response.StatusCode == http.StatusTooManyRequests) && retryAfter == 0 {
			retryAfter = defaultBlockBackoff
		}
		if retryAfter > 0 || response.StatusCode == http.StatusForbidden {
			c.openCooldown(retryAfter)
		}
		statusErr := &StatusError{StatusCode: response.StatusCode, RetryAfter: retryAfter}
		return fail("http_"+strconv.Itoa(response.StatusCode), "", retryable, retryAfter, statusErr)
	}
	raw, err := readBounded(response.Body, c.maxBodyBytes)
	if err != nil {
		return fail("oversized", "", false, 0, err)
	}
	var payload []apiResult
	if err := json.Unmarshal(raw, &payload); err != nil {
		return fail("malformed", "", false, 0, fmt.Errorf("wiby: decode response: %w", err))
	}
	if payload == nil {
		return fail("malformed", "", false, 0, errors.New("wiby: response must be a JSON result array"))
	}

	limit := c.resultLimit(query.MaxResults)
	includeFilters := parseDomainFilters(query.IncludeDomains)
	excludeFilters := parseDomainFilters(query.ExcludeDomains)
	hits := make([]search.Hit, 0, min(limit, len(payload)))
	invalid := 0
	for _, result := range payload {
		if len(hits) >= limit {
			break
		}
		parsed, parseErr := url.Parse(strings.TrimSpace(result.URL))
		if parseErr != nil || parsed.User != nil || parsed.Hostname() == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			invalid++
			continue
		}
		if !domainAllowed(parsed, includeFilters, excludeFilters) {
			continue
		}
		title := cleanText(result.Title)
		if title == "" {
			invalid++
			continue
		}
		snippet := cleanText(result.Snippet)
		description := cleanText(result.Description)
		if snippet == "" {
			snippet = description
		}
		metadata := map[string]string{
			"source":          "Wiby",
			"attribution_url": attributionURL,
		}
		if description != "" {
			metadata["wiby_description"] = description
		}
		hits = append(hits, search.Hit{
			URL: parsed.String(), Title: title, Snippet: snippet,
			Engines: []string{"wiby"}, Metadata: metadata,
		})
	}
	status := search.BatchAuthoritativeEmpty
	diagnostics := []search.ProviderDiagnostic(nil)
	if len(hits) > 0 {
		status = search.BatchHealthy
	}
	if invalid > 0 {
		diagnostics = []search.ProviderDiagnostic{{
			Provider: "wiby", Instance: "wiby.me", Source: "official_api", Reason: "malformed_results",
		}}
		if len(hits) > 0 {
			status = search.BatchPartial
		} else {
			return fail("malformed_results", "", false, 0, errors.New("wiby: response contained no valid results"))
		}
	}
	return search.SearchBatch{
		Hits: hits, Provider: "wiby", Instance: "wiby.me", Status: status,
		Diagnostics: diagnostics, Duration: time.Since(started),
	}, nil
}

type apiResult struct {
	URL         string `json:"URL"`
	Title       string `json:"Title"`
	Snippet     string `json:"Snippet"`
	Description string `json:"Description"`
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
	if strings.TrimSpace(query.TimeRange) != "" {
		return "time_range"
	}
	if query.SafeSearch != nil {
		// Wiby's default response omits NSFW-tagged results, which maps to
		// Fetchmark's moderate mode. Disabled mode would require sending the
		// provider's nsfw flag, while strict mode has no equivalent control.
		if *query.SafeSearch != 1 {
			return "safesearch"
		}
	}
	if query.ExactMatch {
		return "exact_match"
	}
	return ""
}

func (c *Client) resultLimit(requested int) int {
	if requested > 0 && requested < c.maxResults {
		return requested
	}
	return c.maxResults
}

func cleanText(value string) string {
	return strings.Join(strings.Fields(value), " ")
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
		filters = append(filters, domainFilter{
			host: host,
			path: strings.TrimRight(parsed.EscapedPath(), "/"),
		})
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

func validUserAgent(value string) bool {
	value = strings.TrimSpace(value)
	return len(value) >= 10 && len(value) <= 512 && (strings.Contains(value, "http://") || strings.Contains(value, "https://") || strings.Contains(value, "mailto:"))
}

func readBounded(reader io.Reader, maximum int64) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(reader, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maximum {
		return nil, errors.New("wiby: response exceeds body limit")
	}
	return raw, nil
}

func (c *Client) cooldownRemaining() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	remaining := c.cooldownUntil.Sub(c.now())
	if remaining <= 0 {
		return 0
	}
	return remaining
}

func (c *Client) openCooldown(duration time.Duration) {
	if duration <= 0 {
		duration = defaultBlockBackoff
	}
	if duration > maxRetryAfter {
		duration = maxRetryAfter
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	until := c.now().Add(duration)
	if until.After(c.cooldownUntil) {
		c.cooldownUntil = until
	}
}
