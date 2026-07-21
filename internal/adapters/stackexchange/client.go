// Package stackexchange implements bounded, no-key developer discovery through
// Stack Overflow's official Stack Exchange API. It uses only the fixed
// /similar endpoint. Its bounded response also supplies question HTML, which
// avoids scraping Stack Overflow pages that may require a browser challenge.
package stackexchange

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/providerbudget"
	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/retryafter"
	"github.com/staticvar/fetchmark/internal/core/search"
)

const (
	defaultEndpoint       = "https://api.stackexchange.com/2.3/similar"
	providerInstance      = "api.stackexchange.com"
	stackOverflowOrigin   = "https://stackoverflow.com/"
	licenseURL            = "https://stackoverflow.com/help/licensing"
	defaultBlockBackoff   = 5 * time.Minute
	duplicateRequestDelay = time.Minute
	quotaExhaustionDelay  = 24 * time.Hour
	maxRetryAfter         = 24 * time.Hour
	maxProviderResults    = 10
	maxProviderRate       = 1.0
	maxRecentQueries      = 256
)

var errResponseTooLarge = errors.New("stackexchange: response exceeds body limit")

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
	return fmt.Sprintf("stackexchange: upstream status %d", e.StatusCode)
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
	recentQueries map[[sha256.Size]byte]queryReservation
	now           func() time.Time
}

type queryReservation struct {
	pendingAt    time.Time
	dispatchedAt time.Time
}

var _ search.Searcher = (*Client)(nil)
var _ search.BatchSearcher = (*Client)(nil)

// New pins requests to Stack Overflow's official API and applies hard caps
// that operator configuration cannot relax.
func New(options Options) (*Client, error) {
	if options.Endpoint == "" {
		options.Endpoint = defaultEndpoint
	}
	endpoint, err := url.Parse(options.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host != providerInstance || endpoint.Path != "/2.3/similar" || endpoint.RawPath != "" || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.User != nil {
		return nil, errors.New("stackexchange: endpoint must be the official HTTPS /2.3/similar API")
	}
	if options.HTTPClient == nil {
		return nil, errors.New("stackexchange: HTTP client is required")
	}
	if !validUserAgent(options.UserAgent) {
		return nil, errors.New("stackexchange: descriptive contactable User-Agent is required")
	}
	if options.MaxResults < 1 || options.MaxBodyBytes < 1 || options.RatePerSecond <= 0 || options.Burst < 1 || options.MaxConcurrency < 1 {
		return nil, errors.New("stackexchange: result, body, rate, burst, and concurrency limits must be positive")
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
		return nil, fmt.Errorf("stackexchange: configure provider budget: %w", err)
	}
	httpClient := *options.HTTPClient
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("stackexchange: provider redirects are not allowed")
	}
	return &Client{
		endpoint: endpoint, httpClient: &httpClient, userAgent: options.UserAgent,
		maxResults: options.MaxResults, maxBodyBytes: options.MaxBodyBytes, budget: budget,
		recentQueries: make(map[[sha256.Size]byte]queryReservation), now: time.Now,
	}, nil
}

func (c *Client) Search(ctx context.Context, query search.Query) ([]search.Hit, error) {
	batch, err := c.SearchBatch(ctx, query)
	if err != nil {
		return nil, err
	}
	return batch.Hits, nil
}

// SearchBatch performs one first-page metadata request. It never selects
// another Stack Exchange site or follows provider-supplied result URLs.
func (c *Client) SearchBatch(ctx context.Context, query search.Query) (search.SearchBatch, error) {
	started := time.Now()
	var schedulingDiagnostics []search.ProviderDiagnostic
	fail := func(reason, control string, retryable bool, retryAfter time.Duration, err error) (search.SearchBatch, error) {
		diagnostic := search.ProviderDiagnostic{
			Provider: "stackexchange", Instance: providerInstance, Source: "similar_api",
			Reason: reason, Retryable: retryable, RetryAfter: retryAfter,
		}
		if control != "" {
			diagnostic.Source = control
		}
		diagnostics := []search.ProviderDiagnostic{diagnostic}
		diagnostics = append(diagnostics, schedulingDiagnostics...)
		return search.SearchBatch{
			Provider: "stackexchange", Instance: providerInstance, Status: search.BatchFailed,
			Diagnostics: diagnostics, Duration: time.Since(started),
		}, err
	}
	if err := ctx.Err(); err != nil {
		return fail("canceled", "", false, 0, err)
	}
	if control := unsupportedControl(query); control != "" {
		err := &search.UnsupportedControlError{Control: control, Reason: "Stack Overflow similar-question search does not support this control"}
		return fail("unsupported_control", control, false, 0, err)
	}
	queryText := strings.Join(strings.Fields(query.Q), " ")
	if queryText == "" {
		return fail("invalid_query", "query", false, 0, errors.New("stackexchange: query is required"))
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
	endpoint := *c.endpoint
	values := endpoint.Query()
	values.Set("site", "stackoverflow")
	values.Set("title", queryText)
	values.Set("sort", "relevance")
	values.Set("order", "desc")
	values.Set("filter", "withbody")
	resultLimit := c.resultLimit(query.MaxResults)
	values.Set("pagesize", strconv.Itoa(resultLimit))
	if from, to := timeRangeBounds(query.TimeRange, c.now().UTC()); !from.IsZero() {
		values.Set("fromdate", strconv.FormatInt(from.Unix(), 10))
		values.Set("todate", strconv.FormatInt(to.Unix(), 10))
	}
	endpoint.RawQuery = values.Encode()
	// Time-window endpoints move every second, but the provider still considers
	// the caller's same title/range/page-size request semantically identical.
	// Keep the duplicate guard stable across that moving absolute boundary.
	queryKey := sha256.Sum256([]byte(strings.Join([]string{
		queryText, strings.TrimSpace(query.TimeRange), strconv.Itoa(resultLimit),
	}, "\x00")))
	if retryAfter := c.reserveQuery(queryKey); retryAfter > 0 {
		err := &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: retryAfter}
		return fail("duplicate_cooldown", "", true, retryAfter, err)
	}
	if err := c.budget.WaitRate(ctx); err != nil {
		c.releaseQuery(queryKey)
		return fail("canceled", "", false, 0, err)
	}
	if retryAfter := c.cooldownRemaining(); retryAfter > 0 {
		c.releaseQuery(queryKey)
		err := &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: retryAfter}
		return fail("cooldown", "", true, retryAfter, err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		c.releaseQuery(queryKey)
		return fail("request", "", false, 0, err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", c.userAgent)
	if err := c.commitQueryDispatch(ctx, queryKey); err != nil {
		reason := "request"
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			reason = "canceled"
		}
		return fail(reason, "", false, 0, err)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return fail("canceled", "", false, 0, ctx.Err())
		}
		return fail("network", "", true, 0, err)
	}
	defer response.Body.Close()
	retryableStatus := response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500
	statusRetryAfter := time.Duration(0)
	if retryableStatus {
		statusRetryAfter = retryafter.Parse(response.Header.Get("Retry-After"), c.now(), maxRetryAfter)
		if statusRetryAfter > 0 {
			c.openCooldown(statusRetryAfter)
		}
	}
	raw, err := readBounded(response.Body, c.maxBodyBytes)
	if err != nil {
		if retryableStatus && statusRetryAfter == 0 {
			statusRetryAfter = defaultBlockBackoff
			c.openCooldown(statusRetryAfter)
		}
		reason := "body_read"
		if errors.Is(err, errResponseTooLarge) {
			reason = "oversized"
		}
		return fail(reason, "", retryableStatus || reason == "body_read", statusRetryAfter, err)
	}
	var payload apiResponse
	decodeErr := json.Unmarshal(raw, &payload)
	backoff, backoffOK := validBackoff(payload.Backoff)
	effectiveRetryAfter := statusRetryAfter
	if decodeErr == nil && backoffOK && backoff > 0 {
		c.openCooldown(backoff)
		if backoff > effectiveRetryAfter {
			effectiveRetryAfter = backoff
		}
		schedulingDiagnostics = append(schedulingDiagnostics, search.ProviderDiagnostic{
			Provider: "stackexchange", Instance: providerInstance, Source: "similar_api",
			Reason: "backoff", Retryable: true, RetryAfter: backoff,
		})
	}
	if decodeErr == nil && payload.QuotaRemaining != nil && *payload.QuotaRemaining == 0 {
		c.openCooldown(quotaExhaustionDelay)
		if quotaExhaustionDelay > effectiveRetryAfter {
			effectiveRetryAfter = quotaExhaustionDelay
		}
		schedulingDiagnostics = append(schedulingDiagnostics, search.ProviderDiagnostic{
			Provider: "stackexchange", Instance: providerInstance, Source: "quota",
			Reason: "quota_exhausted", Retryable: true, RetryAfter: quotaExhaustionDelay,
		})
	}
	if response.StatusCode != http.StatusOK {
		if retryableStatus && effectiveRetryAfter == 0 {
			effectiveRetryAfter = defaultBlockBackoff
			c.openCooldown(effectiveRetryAfter)
		}
		statusErr := &StatusError{StatusCode: response.StatusCode, RetryAfter: effectiveRetryAfter}
		return fail("http_"+strconv.Itoa(response.StatusCode), "", retryableStatus, effectiveRetryAfter, statusErr)
	}
	if decodeErr != nil {
		return fail("malformed", "", effectiveRetryAfter > 0, effectiveRetryAfter, fmt.Errorf("stackexchange: decode response: %w", decodeErr))
	}
	if payload.Items == nil {
		return fail("malformed", "", effectiveRetryAfter > 0, effectiveRetryAfter, errors.New("stackexchange: response items are required"))
	}
	if !backoffOK {
		return fail("malformed", "", effectiveRetryAfter > 0, effectiveRetryAfter, errors.New("stackexchange: invalid response backoff"))
	}
	if payload.QuotaRemaining != nil && *payload.QuotaRemaining < 0 {
		return fail("malformed", "", effectiveRetryAfter > 0, effectiveRetryAfter, errors.New("stackexchange: invalid quota remaining"))
	}

	diagnostics := schedulingDiagnostics

	limit := resultLimit
	includeFilters := parseDomainFilters(query.IncludeDomains)
	excludeFilters := parseDomainFilters(query.ExcludeDomains)
	hits := make([]search.Hit, 0, min(limit, len(payload.Items)))
	seen := make(map[int64]struct{}, len(payload.Items))
	invalid := 0
	for _, item := range payload.Items {
		if len(hits) >= limit {
			break
		}
		if item.QuestionID <= 0 || strings.TrimSpace(item.Title) == "" {
			invalid++
			continue
		}
		if _, duplicate := seen[item.QuestionID]; duplicate {
			invalid++
			continue
		}
		seen[item.QuestionID] = struct{}{}
		resultURL := &url.URL{Scheme: "https", Host: "stackoverflow.com", Path: "/questions/" + strconv.FormatInt(item.QuestionID, 10)}
		if !domainAllowed(resultURL, includeFilters, excludeFilters) {
			continue
		}
		title := cleanText(html.UnescapeString(item.Title))
		if title == "" {
			invalid++
			continue
		}
		tags := cleanTags(item.Tags)
		metadata := map[string]string{
			"source": "Stack Overflow", "attribution_url": stackOverflowOrigin,
			"license_url": licenseURL, "question_id": strconv.FormatInt(item.QuestionID, 10),
			"score": strconv.Itoa(item.Score), "answer_count": strconv.Itoa(item.AnswerCount),
			"is_answered": strconv.FormatBool(item.IsAnswered),
		}
		if item.AcceptedAnswerID > 0 {
			metadata["accepted_answer_id"] = strconv.FormatInt(item.AcceptedAnswerID, 10)
		}
		if license := cleanText(item.ContentLicense); license != "" {
			metadata["license"] = license
		}
		ownerName := cleanText(html.UnescapeString(item.Owner.DisplayName))
		if ownerName != "" {
			metadata["owner"] = ownerName
		}
		if item.Owner.UserID > 0 {
			metadata["owner_id"] = strconv.FormatInt(item.Owner.UserID, 10)
		}
		if len(tags) > 0 {
			metadata["tags"] = strings.Join(tags, ",")
		}
		rowMalformed := false
		observationTime := c.now().UTC()
		if item.LastActivityDate != 0 {
			if _, ok := validProviderTimestamp(item.LastActivityDate, observationTime); ok {
				metadata["last_activity_unix"] = strconv.FormatInt(item.LastActivityDate, 10)
			} else {
				rowMalformed = true
			}
		}
		var published *time.Time
		if item.CreationDate != 0 {
			if value, ok := validProviderTimestamp(item.CreationDate, observationTime); ok {
				published = &value
			} else {
				rowMalformed = true
			}
		}
		if rowMalformed {
			invalid++
		}
		providerDocument := &model.ProviderDocument{Author: ownerName, SiteName: "Stack Overflow"}
		if item.Body != nil && strings.TrimSpace(*item.Body) != "" && metadata["license"] != "" {
			providerDocument.HTML = []byte(*item.Body)
		}
		hits = append(hits, search.Hit{
			URL: resultURL.String(), Title: title, Snippet: questionSnippet(tags, item.Score, item.AnswerCount),
			Engines: []string{"stackexchange"}, PublishedAt: published, Metadata: metadata,
			ProviderDocument: providerDocument,
		})
	}
	if invalid > 0 {
		diagnostics = append(diagnostics, search.ProviderDiagnostic{
			Provider: "stackexchange", Instance: providerInstance, Source: "similar_api", Reason: "malformed_results",
		})
		if len(hits) == 0 {
			return fail("malformed_results", "", false, 0, errors.New("stackexchange: response contained no valid results"))
		}
	}
	status := search.BatchAuthoritativeEmpty
	if len(hits) > 0 {
		status = search.BatchHealthy
	}
	if invalid > 0 && len(hits) > 0 {
		status = search.BatchPartial
	}
	return search.SearchBatch{
		Hits: hits, Provider: "stackexchange", Instance: providerInstance, Status: status,
		Diagnostics: diagnostics, Duration: time.Since(started),
	}, nil
}

type apiResponse struct {
	Items          []question `json:"items"`
	Backoff        *int64     `json:"backoff"`
	QuotaRemaining *int       `json:"quota_remaining"`
}

type question struct {
	Tags             []string `json:"tags"`
	Owner            owner    `json:"owner"`
	IsAnswered       bool     `json:"is_answered"`
	AnswerCount      int      `json:"answer_count"`
	Score            int      `json:"score"`
	LastActivityDate int64    `json:"last_activity_date"`
	CreationDate     int64    `json:"creation_date"`
	QuestionID       int64    `json:"question_id"`
	AcceptedAnswerID int64    `json:"accepted_answer_id"`
	ContentLicense   string   `json:"content_license"`
	Body             *string  `json:"body"`
	Title            string   `json:"title"`
}

type owner struct {
	UserID      int64  `json:"user_id"`
	DisplayName string `json:"display_name"`
}

func unsupportedControl(query search.Query) string {
	if len(query.Engines) > 0 {
		return "engines"
	}
	if len(query.Categories) > 0 {
		return "categories"
	}
	if language := strings.TrimSpace(query.Language); language != "" && !strings.EqualFold(language, "auto") && !strings.EqualFold(language, "en") {
		return "language"
	}
	if timeRange := strings.TrimSpace(query.TimeRange); timeRange != "" {
		switch timeRange {
		case "day", "week", "month", "year":
		default:
			return "time_range"
		}
	}
	if query.SafeSearch != nil && *query.SafeSearch != 1 {
		return "safesearch"
	}
	if query.ExactMatch {
		return "exact_match"
	}
	return ""
}

func timeRangeBounds(value string, now time.Time) (time.Time, time.Time) {
	var duration time.Duration
	switch strings.TrimSpace(value) {
	case "day":
		duration = 24 * time.Hour
	case "week":
		duration = 7 * 24 * time.Hour
	case "month":
		duration = 30 * 24 * time.Hour
	case "year":
		duration = 365 * 24 * time.Hour
	default:
		return time.Time{}, time.Time{}
	}
	return now.Add(-duration), now
}

func validProviderTimestamp(value int64, now time.Time) (time.Time, bool) {
	if value <= 0 {
		return time.Time{}, false
	}
	parsed := time.Unix(value, 0).UTC()
	if parsed.Year() < 1970 || parsed.Year() > 9999 || parsed.After(now.Add(24*time.Hour)) {
		return time.Time{}, false
	}
	return parsed, true
}

func validBackoff(value *int64) (time.Duration, bool) {
	if value == nil {
		return 0, true
	}
	if *value < 0 {
		return 0, false
	}
	if *value > int64(maxRetryAfter/time.Second) {
		return maxRetryAfter, true
	}
	return time.Duration(*value) * time.Second, true
}

func (c *Client) resultLimit(requested int) int {
	if requested > 0 && requested < c.maxResults {
		return requested
	}
	return c.maxResults
}

func questionSnippet(tags []string, score, answers int) string {
	parts := make([]string, 0, 3)
	if len(tags) > 0 {
		parts = append(parts, strings.Join(tags, ", "))
	}
	parts = append(parts, "score "+strconv.Itoa(score))
	label := "answers"
	if answers == 1 {
		label = "answer"
	}
	parts = append(parts, strconv.Itoa(answers)+" "+label)
	return strings.Join(parts, " · ")
}

func cleanTags(values []string) []string {
	result := make([]string, 0, min(len(values), 12))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = cleanText(html.UnescapeString(value))
		if value == "" || len(value) > 64 {
			continue
		}
		if _, duplicate := seen[value]; duplicate {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
		if len(result) == 12 {
			break
		}
	}
	return result
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
		return nil, errResponseTooLarge
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

func (c *Client) reserveQuery(key [sha256.Size]byte) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	var oldestKey [sha256.Size]byte
	var oldest time.Time
	for candidate, reservation := range c.recentQueries {
		anchor := reservation.dispatchedAt
		if anchor.IsZero() {
			anchor = reservation.pendingAt
		}
		age := now.Sub(anchor)
		if age < 0 {
			age = 0
		}
		if age >= duplicateRequestDelay {
			delete(c.recentQueries, candidate)
			continue
		}
		if candidate == key {
			return duplicateRequestDelay - age
		}
		if oldest.IsZero() || anchor.Before(oldest) {
			oldestKey, oldest = candidate, anchor
		}
	}
	if len(c.recentQueries) >= maxRecentQueries {
		delete(c.recentQueries, oldestKey)
	}
	c.recentQueries[key] = queryReservation{pendingAt: now}
	return 0
}

func (c *Client) releaseQuery(key [sha256.Size]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	reservation, ok := c.recentQueries[key]
	if ok && reservation.dispatchedAt.IsZero() {
		delete(c.recentQueries, key)
	}
}

func (c *Client) commitQueryDispatch(ctx context.Context, key [sha256.Size]byte) error {
	if err := ctx.Err(); err != nil {
		c.releaseQuery(key)
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	reservation, ok := c.recentQueries[key]
	if !ok || !reservation.dispatchedAt.IsZero() {
		return errors.New("stackexchange: invalid query reservation state")
	}
	reservation.dispatchedAt = c.now()
	c.recentQueries[key] = reservation
	return nil
}
