// Package wikipedia implements a no-key discovery adapter for the Wikimedia
// Action API. It identifies itself, bounds shared-service usage, and returns
// only page metadata for Fetchmark's normal live fetch/extract path.
package wikipedia

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	xhtml "golang.org/x/net/html"

	"github.com/staticvar/fetchmark/internal/adapters/providerbudget"
	"github.com/staticvar/fetchmark/internal/core/retryafter"
	"github.com/staticvar/fetchmark/internal/core/search"
)

const (
	defaultEndpoint     = "https://en.wikipedia.org/w/api.php"
	defaultBlockBackoff = 5 * time.Minute
	maxRetryAfter       = 24 * time.Hour
)

// Options defines global provider budgets. All limits are required so a
// misconfigured adapter cannot become unbounded.
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
	return fmt.Sprintf("wikipedia: upstream status %d", e.StatusCode)
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
	// Test seam used to prove cooldown handoff while the semaphore is full.
	beforeSemaphore func()
}

var _ search.Searcher = (*Client)(nil)
var _ search.BatchSearcher = (*Client)(nil)

var wikipediaQuestionWords = map[string]struct{}{
	"how": {}, "what": {}, "when": {}, "where": {}, "which": {}, "who": {}, "why": {},
}

var wikipediaQuestionStopWords = map[string]struct{}{
	"a": {}, "about": {}, "across": {}, "an": {}, "and": {}, "are": {}, "at": {},
	"between": {}, "can": {}, "could": {}, "did": {}, "do": {}, "does": {}, "during": {},
	"for": {}, "from": {}, "had": {}, "has": {}, "have": {}, "how": {}, "in": {}, "into": {},
	"is": {}, "it": {}, "of": {}, "on": {}, "or": {}, "over": {}, "s": {}, "should": {},
	"the": {}, "to": {}, "under": {}, "was": {}, "were": {}, "what": {}, "when": {},
	"where": {}, "which": {}, "who": {}, "why": {}, "with": {}, "would": {},
}

// New validates that requests can only target Wikimedia's Wikipedia API and
// that the automated client is contactably identified.
func New(options Options) (*Client, error) {
	if options.Endpoint == "" {
		options.Endpoint = defaultEndpoint
	}
	endpoint, err := url.Parse(options.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Path != "/w/api.php" || !isWikipediaHost(endpoint.Hostname()) {
		return nil, errors.New("wikipedia: endpoint must be an HTTPS wikipedia.org Action API URL")
	}
	if options.HTTPClient == nil {
		return nil, errors.New("wikipedia: HTTP client is required")
	}
	if !validUserAgent(options.UserAgent) {
		return nil, errors.New("wikipedia: descriptive contactable User-Agent is required")
	}
	if options.MaxResults < 1 || options.MaxResults > 50 || options.MaxBodyBytes < 1 || options.RatePerSecond <= 0 || options.Burst < 1 || options.MaxConcurrency < 1 {
		return nil, errors.New("wikipedia: result, body, rate, burst, and concurrency limits must be positive and bounded")
	}
	// Wikimedia publishes behavioral rather than fixed numeric ceilings. Keep
	// hard conservative caps so an operator override cannot create spikes.
	if options.RatePerSecond > 5 {
		options.RatePerSecond = 5
	}
	if options.MaxConcurrency > 2 {
		options.MaxConcurrency = 2
	}
	if options.Burst > options.MaxConcurrency {
		options.Burst = options.MaxConcurrency
	}
	budget, err := providerbudget.New(options.RatePerSecond, options.Burst, options.MaxConcurrency)
	if err != nil {
		return nil, fmt.Errorf("wikipedia: configure provider budget: %w", err)
	}
	return &Client{
		endpoint:     endpoint,
		httpClient:   options.HTTPClient,
		userAgent:    options.UserAgent,
		maxResults:   options.MaxResults,
		maxBodyBytes: options.MaxBodyBytes,
		budget:       budget,
		now:          time.Now,
	}, nil
}

// Search preserves the compatibility port.
func (c *Client) Search(ctx context.Context, q search.Query) ([]search.Hit, error) {
	batch, err := c.SearchBatch(ctx, q)
	if err != nil {
		return nil, err
	}
	return batch.Hits, nil
}

// SearchBatch performs one bounded Action API full-text search.
func (c *Client) SearchBatch(ctx context.Context, q search.Query) (search.SearchBatch, error) {
	started := time.Now()
	fail := func(reason string, retryable bool, retryAfter time.Duration, err error) (search.SearchBatch, error) {
		return search.SearchBatch{
			Provider: "wikipedia",
			Status:   search.BatchFailed,
			Diagnostics: []search.ProviderDiagnostic{{
				Provider:   "wikipedia",
				Instance:   c.endpoint.Hostname(),
				Source:     "action_api",
				Reason:     reason,
				Retryable:  retryable,
				RetryAfter: retryAfter,
			}},
			Duration: time.Since(started),
		}, err
	}
	if err := ctx.Err(); err != nil {
		return fail("canceled", false, 0, err)
	}
	endpoint, err := c.endpointForLanguage(q.Language)
	if err != nil {
		return fail("invalid_language", false, 0, err)
	}
	if retryAfter := c.cooldownRemaining(); retryAfter > 0 {
		err := &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: retryAfter}
		return fail("cooldown", true, retryAfter, err)
	}
	if c.beforeSemaphore != nil {
		c.beforeSemaphore()
	}
	release, err := c.budget.AcquireSlot(ctx)
	if err != nil {
		return fail("canceled", false, 0, err)
	}
	defer release()
	// A request ahead of us may have opened the cooldown while this request
	// waited for the provider semaphore. Recheck immediately before I/O.
	if retryAfter := c.cooldownRemaining(); retryAfter > 0 {
		err := &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: retryAfter}
		return fail("cooldown", true, retryAfter, err)
	}
	if err := c.budget.WaitRate(ctx); err != nil {
		return fail("canceled", false, 0, err)
	}
	// Another admitted request can open cooldown while this call waits for its
	// rate token when provider concurrency is greater than one.
	if retryAfter := c.cooldownRemaining(); retryAfter > 0 {
		err := &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: retryAfter}
		return fail("cooldown", true, retryAfter, err)
	}

	values := endpoint.Query()
	values.Set("action", "query")
	values.Set("list", "search")
	values.Set("srsearch", wikipediaSearchText(q))
	values.Set("srlimit", strconv.Itoa(c.resultLimit(q.MaxResults)))
	values.Set("srprop", "snippet|timestamp|wordcount")
	values.Set("format", "json")
	values.Set("formatversion", "2")
	endpoint.RawQuery = values.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return fail("request", false, 0, err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", c.userAgent)
	request.Header.Set("Api-User-Agent", c.userAgent)
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
		return fail("malformed", false, 0, fmt.Errorf("wikipedia: decode response: %w", err))
	}
	if payload.Error != nil {
		return fail("api_error", false, 0, fmt.Errorf("wikipedia: API error %s", payload.Error.Code))
	}
	hits := make([]search.Hit, 0, len(payload.Query.Search))
	for _, page := range payload.Query.Search {
		if page.Title == "" || page.PageID <= 0 {
			continue
		}
		pageURL := *endpoint
		pageURL.Path = "/wiki/" + strings.ReplaceAll(page.Title, " ", "_")
		pageURL.RawQuery = ""
		metadata := map[string]string{
			"page_id":     strconv.FormatInt(page.PageID, 10),
			"word_count":  strconv.Itoa(page.WordCount),
			"last_edit":   page.Timestamp,
			"source":      "Wikimedia Foundation",
			"license":     "CC-BY-SA",
			"license_url": "https://creativecommons.org/licenses/by-sa/4.0/",
		}
		hits = append(hits, search.Hit{
			URL:      pageURL.String(),
			Title:    page.Title,
			Snippet:  stripHTML(page.Snippet),
			Engines:  []string{"wikipedia"},
			Metadata: metadata,
		})
	}
	status := search.BatchAuthoritativeEmpty
	if len(hits) > 0 {
		status = search.BatchHealthy
	}
	return search.SearchBatch{
		Hits:     hits,
		Provider: "wikipedia",
		Instance: endpoint.Hostname(),
		Status:   status,
		Duration: time.Since(started),
	}, nil
}

// wikipediaSearchText removes question scaffolding that the Action API tends
// to treat as ranking terms. Short keyword queries and explicit exact-match
// requests retain their operator-provided text unchanged.
func wikipediaSearchText(q search.Query) string {
	raw := strings.TrimSpace(q.Q)
	if raw == "" || q.ExactMatch {
		return raw
	}
	terms := strings.FieldsFunc(raw, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '-'
	})
	if len(terms) < 2 {
		return raw
	}
	if _, ok := wikipediaQuestionWords[strings.ToLower(terms[0])]; !ok {
		return raw
	}

	hasBetween := false
	for _, term := range terms {
		if strings.EqualFold(term, "between") {
			hasBetween = true
			break
		}
	}
	compact := make([]string, 0, len(terms))
	for index, term := range terms {
		lower := strings.ToLower(term)
		if _, skip := wikipediaQuestionStopWords[lower]; skip {
			continue
		}
		if hasBetween && lower == "difference" {
			continue
		}
		if index == len(terms)-1 && (lower == "work" || lower == "works" || lower == "working") {
			continue
		}
		compact = append(compact, term)
	}
	if len(compact) < 2 {
		return raw
	}
	return strings.Join(compact, " ")
}

type apiResponse struct {
	Query struct {
		Search []struct {
			PageID    int64  `json:"pageid"`
			Title     string `json:"title"`
			Snippet   string `json:"snippet"`
			Timestamp string `json:"timestamp"`
			WordCount int    `json:"wordcount"`
		} `json:"search"`
	} `json:"query"`
	Error *struct {
		Code string `json:"code"`
		Info string `json:"info"`
	} `json:"error"`
}

func (c *Client) resultLimit(requested int) int {
	if requested > 0 && requested < c.maxResults {
		return requested
	}
	return c.maxResults
}

func (c *Client) endpointForLanguage(language string) (*url.URL, error) {
	endpoint := *c.endpoint
	language = strings.ToLower(strings.TrimSpace(language))
	if language == "" || language == "auto" {
		return &endpoint, nil
	}
	if !languagePattern.MatchString(language) {
		return nil, fmt.Errorf("wikipedia: unsupported language %q", language)
	}
	endpoint.Host = language + ".wikipedia.org"
	return &endpoint, nil
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
		return nil, fmt.Errorf("wikipedia: response exceeds %d bytes", limit)
	}
	return raw, nil
}

func stripHTML(fragment string) string {
	tokenizer := xhtml.NewTokenizer(strings.NewReader(fragment))
	var builder strings.Builder
	for {
		switch tokenizer.Next() {
		case xhtml.ErrorToken:
			return strings.Join(strings.Fields(html.UnescapeString(builder.String())), " ")
		case xhtml.TextToken:
			builder.Write(tokenizer.Text())
		}
	}
}

func validUserAgent(userAgent string) bool {
	lower := strings.ToLower(strings.TrimSpace(userAgent))
	hasContact := strings.Contains(lower, "https://") || strings.Contains(lower, "http://") || strings.Contains(lower, "mailto:")
	return len(lower) >= 12 && len(lower) <= 512 && !strings.ContainsAny(lower, "\r\n") && hasContact && strings.Contains(lower, "/") && strings.Contains(lower, "(") && strings.Contains(lower, ")") &&
		!strings.HasPrefix(lower, "go-http-client") && !strings.HasPrefix(lower, "curl/")
}

func isWikipediaHost(host string) bool {
	host = strings.ToLower(host)
	return strings.HasSuffix(host, ".wikipedia.org") && len(strings.TrimSuffix(host, ".wikipedia.org")) >= 2
}

var languagePattern = regexp.MustCompile(`^[a-z]{2,3}$`)
