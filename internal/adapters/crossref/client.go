// Package crossref implements bounded, no-key discovery through the Crossref
// REST API. It returns bibliographic metadata and leaves content retrieval to
// Fetchmark's normal robots-aware fetch path.
package crossref

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/mail"
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
	defaultEndpoint     = "https://api.crossref.org/v1/works"
	defaultBlockBackoff = 5 * time.Minute
	maxRetryAfter       = 24 * time.Hour
)

// Options defines process-wide provider budgets. Budgets are additionally
// capped to Crossref's public or polite-pool limits in New.
type Options struct {
	Endpoint       string
	HTTPClient     *http.Client
	UserAgent      string
	Mailto         string
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
	return fmt.Sprintf("crossref: upstream status %d", e.StatusCode)
}

// Client implements both the compatibility and rich discovery contracts.
type Client struct {
	endpoint     *url.URL
	httpClient   *http.Client
	userAgent    string
	mailto       string
	maxResults   int
	maxBodyBytes int64
	budget       *providerbudget.Gate

	mu            sync.Mutex
	cooldownUntil time.Time
	now           func() time.Time
}

var _ search.Searcher = (*Client)(nil)
var _ search.BatchSearcher = (*Client)(nil)

// New validates the fixed shared-service boundary and enforces Crossref's
// published public/polite pool ceilings even when an operator asks for more.
func New(options Options) (*Client, error) {
	if options.Endpoint == "" {
		options.Endpoint = defaultEndpoint
	}
	endpoint, err := url.Parse(options.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Hostname() != "api.crossref.org" || endpoint.Path != "/v1/works" || endpoint.RawQuery != "" {
		return nil, errors.New("crossref: endpoint must be the HTTPS Crossref works API")
	}
	if options.HTTPClient == nil {
		return nil, errors.New("crossref: HTTP client is required")
	}
	if !validUserAgent(options.UserAgent) {
		return nil, errors.New("crossref: descriptive contactable User-Agent is required")
	}
	options.Mailto = strings.TrimSpace(options.Mailto)
	if options.Mailto != "" {
		address, err := mail.ParseAddress(options.Mailto)
		if err != nil || !strings.EqualFold(address.Address, options.Mailto) {
			return nil, errors.New("crossref: mailto must be a plain valid email address")
		}
	}
	if options.MaxResults < 1 || options.MaxResults > 100 || options.MaxBodyBytes < 1 || options.RatePerSecond <= 0 || options.Burst < 1 || options.MaxConcurrency < 1 {
		return nil, errors.New("crossref: result, body, rate, burst, and concurrency limits must be positive and bounded")
	}
	maxRate, maxConcurrency := 5.0, 1
	if options.Mailto != "" {
		maxRate, maxConcurrency = 10, 3
	}
	if options.RatePerSecond > maxRate {
		options.RatePerSecond = maxRate
	}
	if options.MaxConcurrency > maxConcurrency {
		options.MaxConcurrency = maxConcurrency
	}
	if options.Burst > maxConcurrency {
		options.Burst = maxConcurrency
	}
	budget, err := providerbudget.New(options.RatePerSecond, options.Burst, options.MaxConcurrency)
	if err != nil {
		return nil, fmt.Errorf("crossref: configure provider budget: %w", err)
	}
	return &Client{
		endpoint:     endpoint,
		httpClient:   options.HTTPClient,
		userAgent:    options.UserAgent,
		mailto:       options.Mailto,
		maxResults:   options.MaxResults,
		maxBodyBytes: options.MaxBodyBytes,
		budget:       budget,
		now:          time.Now,
	}, nil
}

func (c *Client) Search(ctx context.Context, q search.Query) ([]search.Hit, error) {
	batch, err := c.SearchBatch(ctx, q)
	if err != nil {
		return nil, err
	}
	return batch.Hits, nil
}

// SearchBatch performs one bounded bibliographic search without retrying a
// shared public service inside the request latency budget.
func (c *Client) SearchBatch(ctx context.Context, q search.Query) (search.SearchBatch, error) {
	started := time.Now()
	fail := func(reason string, retryable bool, retryAfter time.Duration, err error) (search.SearchBatch, error) {
		return search.SearchBatch{
			Provider: "crossref",
			Status:   search.BatchFailed,
			Diagnostics: []search.ProviderDiagnostic{{
				Provider: "crossref", Instance: c.endpoint.Hostname(), Source: "works_api",
				Reason: reason, Retryable: retryable, RetryAfter: retryAfter,
			}},
			Duration: time.Since(started),
		}, err
	}
	if err := ctx.Err(); err != nil {
		return fail("canceled", false, 0, err)
	}
	if strings.TrimSpace(q.Q) == "" {
		return fail("invalid_query", false, 0, errors.New("crossref: query is required"))
	}
	if retryAfter := c.cooldownRemaining(); retryAfter > 0 {
		err := &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: retryAfter}
		return fail("cooldown", true, retryAfter, err)
	}
	release, err := c.budget.AcquireSlot(ctx)
	if err != nil {
		return fail("canceled", false, 0, err)
	}
	defer release()
	if retryAfter := c.cooldownRemaining(); retryAfter > 0 {
		err := &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: retryAfter}
		return fail("cooldown", true, retryAfter, err)
	}
	if err := c.budget.WaitRate(ctx); err != nil {
		return fail("canceled", false, 0, err)
	}
	if retryAfter := c.cooldownRemaining(); retryAfter > 0 {
		err := &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: retryAfter}
		return fail("cooldown", true, retryAfter, err)
	}

	endpoint := *c.endpoint
	values := endpoint.Query()
	queryField := "query.bibliographic"
	if q.ExactMatch {
		queryField = "query.title"
	}
	values.Set(queryField, strings.TrimSpace(q.Q))
	values.Set("rows", strconv.Itoa(c.resultLimit(q.MaxResults)))
	values.Set("select", "DOI,title,author,published,URL,type,publisher,container-title,license")
	if c.mailto != "" {
		values.Set("mailto", c.mailto)
	}
	if filter := publicationFilter(q.TimeRange, c.now()); filter != "" {
		values.Set("filter", filter)
	}
	endpoint.RawQuery = values.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return fail("request", false, 0, err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", c.userAgent)
	response, err := c.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return fail("canceled", false, 0, ctx.Err())
		}
		return fail("network", true, 0, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		retryAfter := parseRetryAfter(response.Header.Get("Retry-After"), c.now())
		retryable := response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500
		if (response.StatusCode == http.StatusForbidden || response.StatusCode == http.StatusTooManyRequests) && retryAfter == 0 {
			retryAfter = defaultBlockBackoff
		}
		if retryAfter > 0 || response.StatusCode == http.StatusForbidden {
			c.openCooldown(retryAfter)
		}
		statusErr := &StatusError{StatusCode: response.StatusCode, RetryAfter: retryAfter}
		return fail("http_"+strconv.Itoa(response.StatusCode), retryable, retryAfter, statusErr)
	}
	raw, err := readBounded(response.Body, c.maxBodyBytes)
	if err != nil {
		return fail("oversized", false, 0, err)
	}
	var payload apiResponse
	if err := json.Unmarshal(raw, &payload); err != nil {
		return fail("malformed", false, 0, fmt.Errorf("crossref: decode response: %w", err))
	}
	if payload.Status != "ok" {
		return fail("api_error", false, 0, fmt.Errorf("crossref: API status %q", payload.Status))
	}
	hits := make([]search.Hit, 0, len(payload.Message.Items))
	for _, item := range payload.Message.Items {
		doi := strings.TrimSpace(item.DOI)
		title := first(item.Title)
		if doi == "" || title == "" {
			continue
		}
		doiURL := (&url.URL{Scheme: "https", Host: "doi.org", Path: "/" + doi}).String()
		authors := authorNames(item.Author)
		container := first(item.ContainerTitle)
		licenseURL := ""
		if len(item.License) > 0 {
			licenseURL = strings.TrimSpace(item.License[0].URL)
		}
		metadata := map[string]string{
			"doi": doi, "authors": strings.Join(authors, ", "), "publisher": strings.TrimSpace(item.Publisher),
			"container_title": container, "type": strings.TrimSpace(item.Type), "license_url": licenseURL,
			"source": "Crossref",
		}
		hits = append(hits, search.Hit{
			URL: doiURL, Title: title, Snippet: bibliographicSnippet(authors, container, item.Publisher),
			Engines: []string{"crossref"}, PublishedAt: item.Published.time(), Metadata: metadata,
		})
	}
	status := search.BatchAuthoritativeEmpty
	if len(hits) > 0 {
		status = search.BatchHealthy
	}
	return search.SearchBatch{Hits: hits, Provider: "crossref", Instance: endpoint.Hostname(), Status: status, Duration: time.Since(started)}, nil
}

type apiResponse struct {
	Status  string `json:"status"`
	Message struct {
		Items []work `json:"items"`
	} `json:"message"`
}

type work struct {
	DOI            string    `json:"DOI"`
	Title          []string  `json:"title"`
	Author         []author  `json:"author"`
	Published      dateParts `json:"published"`
	Type           string    `json:"type"`
	Publisher      string    `json:"publisher"`
	ContainerTitle []string  `json:"container-title"`
	License        []license `json:"license"`
}

type author struct {
	Given  string `json:"given"`
	Family string `json:"family"`
}

type license struct {
	URL string `json:"URL"`
}

type dateParts struct {
	Parts [][]int `json:"date-parts"`
}

func (d dateParts) time() *time.Time {
	if len(d.Parts) == 0 || len(d.Parts[0]) == 0 || d.Parts[0][0] < 1 {
		return nil
	}
	parts := d.Parts[0]
	month, day := 1, 1
	if len(parts) > 1 && parts[1] >= 1 && parts[1] <= 12 {
		month = parts[1]
	}
	if len(parts) > 2 && parts[2] >= 1 && parts[2] <= 31 {
		day = parts[2]
	}
	value := time.Date(parts[0], time.Month(month), day, 0, 0, 0, 0, time.UTC)
	if value.Year() != parts[0] || int(value.Month()) != month || value.Day() != day {
		return nil
	}
	return &value
}

func (c *Client) resultLimit(requested int) int {
	if requested > 0 && requested < c.maxResults {
		return requested
	}
	return c.maxResults
}

func publicationFilter(timeRange string, now time.Time) string {
	var start time.Time
	switch strings.ToLower(strings.TrimSpace(timeRange)) {
	case "day":
		start = now.AddDate(0, 0, -1)
	case "week":
		start = now.AddDate(0, 0, -7)
	case "month":
		start = now.AddDate(0, -1, 0)
	case "year":
		start = now.AddDate(-1, 0, 0)
	default:
		return ""
	}
	return "from-pub-date:" + start.UTC().Format("2006-01-02") + ",until-pub-date:" + now.UTC().Format("2006-01-02")
}

func authorNames(authors []author) []string {
	result := make([]string, 0, len(authors))
	for _, person := range authors {
		name := strings.TrimSpace(strings.TrimSpace(person.Given) + " " + strings.TrimSpace(person.Family))
		if name != "" {
			result = append(result, name)
		}
	}
	return result
}

func bibliographicSnippet(authors []string, container, publisher string) string {
	parts := make([]string, 0, 3)
	if len(authors) > 0 {
		parts = append(parts, strings.Join(authors, ", "))
	}
	if container = strings.TrimSpace(container); container != "" {
		parts = append(parts, container)
	}
	if publisher = strings.TrimSpace(publisher); publisher != "" && !strings.EqualFold(publisher, container) {
		parts = append(parts, publisher)
	}
	return strings.Join(parts, " · ")
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}

func (c *Client) cooldownRemaining() time.Duration {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if !now.Before(c.cooldownUntil) {
		return 0
	}
	return c.cooldownUntil.Sub(now)
}

func (c *Client) openCooldown(duration time.Duration) {
	if duration <= 0 {
		duration = defaultBlockBackoff
	}
	if duration > maxRetryAfter {
		duration = maxRetryAfter
	}
	until := c.now().Add(duration)
	c.mu.Lock()
	if until.After(c.cooldownUntil) {
		c.cooldownUntil = until
	}
	c.mu.Unlock()
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	return retryafter.Parse(value, now, maxRetryAfter)
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, fmt.Errorf("crossref: response exceeds %d bytes", limit)
	}
	return raw, nil
}

func validUserAgent(userAgent string) bool {
	lower := strings.ToLower(strings.TrimSpace(userAgent))
	hasContact := strings.Contains(lower, "https://") || strings.Contains(lower, "http://") || strings.Contains(lower, "mailto:")
	return len(lower) >= 12 && len(lower) <= 512 && !strings.ContainsAny(lower, "\r\n") && hasContact && strings.Contains(lower, "/") && strings.Contains(lower, "(") && strings.Contains(lower, ")") &&
		!strings.HasPrefix(lower, "go-http-client") && !strings.HasPrefix(lower, "curl/")
}
