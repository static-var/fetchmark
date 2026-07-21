// Package github implements bounded, metadata-only discovery through GitHub's
// public repository-search endpoint. It is an optional opportunistic lane: it
// never fetches repository contents and does not require a token or paid API.
package github

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
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
	defaultEndpoint      = "https://api.github.com/search/repositories"
	apiVersion           = "2026-03-10"
	defaultLimitBackoff  = time.Minute
	maxRetryAfter        = 24 * time.Hour
	maxProviderResults   = 20
	maxProviderRate      = 0.1
	maximumQueryBytes    = 220
	maximumProjectedTerm = 16
)

var activityRanges = map[string]time.Duration{
	"day":   24 * time.Hour,
	"week":  7 * 24 * time.Hour,
	"month": 30 * 24 * time.Hour,
	"year":  365 * 24 * time.Hour,
}

// Options defines the hard endpoint and process-wide shared-IP budgets.
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

// StatusError retains only bounded upstream scheduling evidence.
type StatusError struct {
	StatusCode int
	RetryAfter time.Duration
}

func (errorValue *StatusError) Error() string {
	return fmt.Sprintf("github: upstream status %d", errorValue.StatusCode)
}

// Client implements both the compatibility and provider-aware contracts.
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

// New pins requests to GitHub's official repository-search endpoint. Operator
// values may only tighten the public unauthenticated rate and concurrency caps.
func New(options Options) (*Client, error) {
	if options.Endpoint == "" {
		options.Endpoint = defaultEndpoint
	}
	endpoint, err := url.Parse(options.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host != "api.github.com" ||
		endpoint.Path != "/search/repositories" || endpoint.RawPath != "" || endpoint.RawQuery != "" ||
		endpoint.Fragment != "" || endpoint.User != nil {
		return nil, errors.New("github: endpoint must be the official HTTPS repository search API")
	}
	if options.HTTPClient == nil {
		return nil, errors.New("github: HTTP client is required")
	}
	if !validUserAgent(options.UserAgent) {
		return nil, errors.New("github: descriptive contactable User-Agent is required")
	}
	if options.MaxResults < 1 || options.MaxBodyBytes < 1 || options.RatePerSecond <= 0 ||
		options.Burst < 1 || options.MaxConcurrency < 1 {
		return nil, errors.New("github: result, body, rate, burst, and concurrency limits must be positive")
	}
	options.MaxResults = min(options.MaxResults, maxProviderResults)
	options.RatePerSecond = min(options.RatePerSecond, maxProviderRate)
	options.Burst = min(options.Burst, 1)
	options.MaxConcurrency = min(options.MaxConcurrency, 1)
	budget, err := providerbudget.New(options.RatePerSecond, options.Burst, options.MaxConcurrency)
	if err != nil {
		return nil, fmt.Errorf("github: configure provider budget: %w", err)
	}
	return &Client{
		endpoint: endpoint, httpClient: singleAttemptHTTPClient(options.HTTPClient), userAgent: options.UserAgent,
		maxResults: options.MaxResults, maxBodyBytes: options.MaxBodyBytes,
		budget: budget, now: time.Now,
	}, nil
}

// singleAttemptHTTPClient makes one rate token correspond to exactly one
// outbound request. Redirects and transparent replay after a stale reused
// connection would otherwise escape the provider gate.
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

// SearchBatch performs exactly one first-page metadata request. It deliberately
// avoids core API follow-ups so the separate search rate bucket is the only
// GitHub capacity Fetchmark consumes.
func (client *Client) SearchBatch(ctx context.Context, query search.Query) (search.SearchBatch, error) {
	started := time.Now()
	fail := func(reason, control string, retryable bool, retryAfter time.Duration, err error) (search.SearchBatch, error) {
		diagnostic := search.ProviderDiagnostic{
			Provider: "github", Instance: "api.github.com", Source: "repository_search",
			Reason: reason, Retryable: retryable, RetryAfter: retryAfter,
		}
		if control != "" {
			diagnostic.Source = control
		}
		return search.SearchBatch{
			Provider: "github", Instance: "api.github.com", Status: search.BatchFailed,
			Diagnostics: []search.ProviderDiagnostic{diagnostic}, Duration: time.Since(started),
		}, err
	}
	if err := ctx.Err(); err != nil {
		return fail("canceled", "", false, 0, err)
	}
	if control := unsupportedControl(query); control != "" {
		err := &search.UnsupportedControlError{Control: control, Reason: "GitHub repository search cannot preserve this search control"}
		return fail("unsupported_control", control, false, 0, err)
	}
	projected, err := projectQuery(query.Q)
	if err != nil {
		return fail("invalid_query", "query", false, 0, err)
	}
	if duration, exists := activityRanges[strings.ToLower(strings.TrimSpace(query.TimeRange))]; exists {
		projected += " pushed:>=" + client.now().UTC().Add(-duration).Format("2006-01-02")
	} else if strings.TrimSpace(query.TimeRange) != "" {
		err := &search.UnsupportedControlError{Control: "time_range", Reason: "GitHub supports day, week, month, or year repository activity windows"}
		return fail("unsupported_control", "time_range", false, 0, err)
	}
	if excludedByDomain(query.IncludeDomains, query.ExcludeDomains) {
		return search.SearchBatch{
			Provider: "github", Instance: "api.github.com", Status: search.BatchAuthoritativeEmpty,
			Duration: time.Since(started),
		}, nil
	}
	if retryAfter := client.cooldownRemaining(); retryAfter > 0 {
		err := &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: retryAfter}
		return fail("cooldown", "", true, retryAfter, err)
	}
	release, err := client.budget.AcquireSlot(ctx)
	if err != nil {
		return fail("canceled", "", false, 0, err)
	}
	defer release()
	if retryAfter := client.cooldownRemaining(); retryAfter > 0 {
		err := &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: retryAfter}
		return fail("cooldown", "", true, retryAfter, err)
	}
	if err := client.budget.WaitRate(ctx); err != nil {
		return fail("canceled", "", false, 0, err)
	}
	if retryAfter := client.cooldownRemaining(); retryAfter > 0 {
		err := &StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: retryAfter}
		return fail("cooldown", "", true, retryAfter, err)
	}

	endpoint := *client.endpoint
	values := endpoint.Query()
	values.Set("q", projected)
	values.Set("per_page", strconv.Itoa(client.resultLimit(query.MaxResults)))
	values.Set("page", "1")
	endpoint.RawQuery = values.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return fail("request", "", false, 0, err)
	}
	// A fresh connection prevents net/http from transparently replaying this GET
	// after a stale keep-alive failure; the provider gate therefore accounts for
	// every upstream attempt it permits.
	request.Close = true
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("X-GitHub-Api-Version", apiVersion)
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
		retryAfter := githubRetryAfter(response.Header, client.now())
		retryable := response.StatusCode == http.StatusForbidden || response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500
		if (response.StatusCode == http.StatusForbidden || response.StatusCode == http.StatusTooManyRequests) && retryAfter == 0 {
			retryAfter = defaultLimitBackoff
		}
		if retryAfter > 0 {
			client.openCooldown(retryAfter)
		}
		statusErr := &StatusError{StatusCode: response.StatusCode, RetryAfter: retryAfter}
		return fail("http_"+strconv.Itoa(response.StatusCode), "", retryable, retryAfter, statusErr)
	}
	raw, err := readBounded(response.Body, client.maxBodyBytes)
	if err != nil {
		return fail("oversized", "", false, 0, err)
	}
	var payload apiResponse
	if err := json.Unmarshal(raw, &payload); err != nil {
		return fail("malformed", "", false, 0, fmt.Errorf("github: decode response: %w", err))
	}
	if payload.Items == nil || payload.TotalCount < 0 {
		return fail("malformed", "", false, 0, errors.New("github: response is missing required repository fields"))
	}

	limit := client.resultLimit(query.MaxResults)
	hits := make([]search.Hit, 0, min(limit, len(payload.Items)))
	invalid := 0
	for _, item := range payload.Items {
		if len(hits) >= limit {
			break
		}
		hit, valid := mapItem(item)
		if !valid {
			invalid++
			continue
		}
		hits = append(hits, hit)
	}
	status := search.BatchHealthy
	diagnostics := make([]search.ProviderDiagnostic, 0, 2)
	if len(hits) == 0 {
		status = search.BatchAuthoritativeEmpty
		if payload.IncompleteResults || payload.TotalCount > 0 {
			status = search.BatchDegradedEmpty
		}
	}
	if payload.IncompleteResults {
		diagnostics = append(diagnostics, search.ProviderDiagnostic{
			Provider: "github", Instance: "api.github.com", Source: "repository_search", Reason: "incomplete_results", Retryable: true,
		})
		if len(hits) > 0 {
			status = search.BatchPartial
		}
	}
	if invalid > 0 {
		diagnostics = append(diagnostics, search.ProviderDiagnostic{
			Provider: "github", Instance: "api.github.com", Source: "repository_search", Reason: "malformed_results",
		})
		if len(hits) > 0 {
			status = search.BatchPartial
		} else {
			return fail("malformed_results", "", false, 0, errors.New("github: response contained no valid repositories"))
		}
	}
	return search.SearchBatch{
		Hits: hits, Provider: "github", Instance: "api.github.com", Status: status,
		Diagnostics: diagnostics, Duration: time.Since(started),
	}, nil
}

type apiResponse struct {
	TotalCount        int64     `json:"total_count"`
	IncompleteResults bool      `json:"incomplete_results"`
	Items             []apiItem `json:"items"`
}

type apiItem struct {
	ID              int64       `json:"id"`
	FullName        string      `json:"full_name"`
	HTMLURL         string      `json:"html_url"`
	Description     *string     `json:"description"`
	Topics          []string    `json:"topics"`
	License         *apiLicense `json:"license"`
	Language        *string     `json:"language"`
	UpdatedAt       string      `json:"updated_at"`
	PushedAt        string      `json:"pushed_at"`
	StargazersCount int64       `json:"stargazers_count"`
	ForksCount      int64       `json:"forks_count"`
	Archived        bool        `json:"archived"`
	Disabled        bool        `json:"disabled"`
	Visibility      string      `json:"visibility"`
	Score           float64     `json:"score"`
	Owner           apiOwner    `json:"owner"`
}

type apiOwner struct {
	Login string `json:"login"`
}

type apiLicense struct {
	SPDXID string `json:"spdx_id"`
	Name   string `json:"name"`
}

func mapItem(item apiItem) (search.Hit, bool) {
	parsed, err := url.Parse(strings.TrimSpace(item.HTMLURL))
	if err != nil || parsed.Scheme != "https" || !strings.EqualFold(parsed.Host, "github.com") || parsed.Port() != "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" || item.ID <= 0 || item.Disabled || item.Archived ||
		strings.TrimSpace(item.FullName) == "" || strings.TrimSpace(item.Owner.Login) == "" || item.Visibility != "public" {
		return search.Hit{}, false
	}
	pathParts := strings.Split(strings.Trim(parsed.EscapedPath(), "/"), "/")
	if len(pathParts) != 2 || !strings.EqualFold(pathParts[0], item.Owner.Login) || !strings.EqualFold(pathParts[0]+"/"+pathParts[1], item.FullName) {
		return search.Hit{}, false
	}
	metadata := map[string]string{
		"source": "GitHub", "repository_id": strconv.FormatInt(item.ID, 10),
		"owner": item.Owner.Login, "full_name": item.FullName,
		"stargazers_count": strconv.FormatInt(item.StargazersCount, 10),
		"forks_count":      strconv.FormatInt(item.ForksCount, 10),
		"score":            strconv.FormatFloat(item.Score, 'f', -1, 64),
	}
	if value := cleanText(item.UpdatedAt); value != "" {
		metadata["updated_at"] = value
	}
	if value := cleanText(item.PushedAt); value != "" {
		metadata["pushed_at"] = value
	}
	if item.Language != nil {
		if value := cleanText(*item.Language); value != "" {
			metadata["language"] = value
		}
	}
	if item.License != nil {
		if value := cleanText(item.License.SPDXID); value != "" && value != "NOASSERTION" {
			metadata["license_spdx"] = value
		}
		if value := cleanText(item.License.Name); value != "" {
			metadata["license_name"] = value
		}
	}
	topics := cleanTopics(item.Topics)
	if len(topics) > 0 {
		metadata["topics"] = strings.Join(topics, ",")
	}
	snippet := ""
	if item.Description != nil {
		snippet = cleanText(*item.Description)
	}
	parsed.RawPath = ""
	return search.Hit{
		URL: parsed.String(), Title: cleanText(item.FullName), Snippet: snippet,
		Engines: []string{"github"}, Metadata: metadata,
	}, true
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
	if query.ExactMatch {
		return "exact_match"
	}
	return ""
}

var projectedTokenPattern = regexp.MustCompile(`^[\pL\pN][\pL\pN._+\-#]*$`)

var queryStopwords = map[string]struct{}{
	"a": {}, "an": {}, "and": {}, "are": {}, "do": {}, "does": {}, "for": {}, "how": {}, "in": {}, "is": {},
	"not": {}, "of": {}, "on": {}, "or": {}, "the": {}, "to": {}, "what": {}, "when": {}, "where": {}, "which": {}, "with": {},
}

func projectQuery(raw string) (string, error) {
	terms := make([]string, 0, maximumProjectedTerm)
	for _, field := range strings.Fields(raw) {
		field = strings.TrimFunc(field, func(character rune) bool {
			return !unicode.IsLetter(character) && !unicode.IsDigit(character) && !strings.ContainsRune("._+-#", character)
		})
		if field == "" || !projectedTokenPattern.MatchString(field) {
			continue
		}
		if _, stopword := queryStopwords[strings.ToLower(field)]; stopword {
			continue
		}
		candidate := strings.Join(append(append([]string(nil), terms...), field), " ")
		if len(candidate) > maximumQueryBytes || !utf8.ValidString(candidate) {
			break
		}
		terms = append(terms, field)
		if len(terms) == maximumProjectedTerm {
			break
		}
	}
	if len(terms) == 0 {
		return "", errors.New("github: query has no searchable terms")
	}
	return strings.Join(terms, " ") + " in:name,description,readme is:public archived:false mirror:false", nil
}

func cleanTopics(raw []string) []string {
	set := make(map[string]struct{}, len(raw))
	for _, item := range raw {
		item = strings.ToLower(cleanText(item))
		if item != "" && len(item) <= 64 && projectedTokenPattern.MatchString(item) {
			set[item] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for item := range set {
		result = append(result, item)
	}
	sort.Strings(result)
	return result
}

func excludedByDomain(includes, excludes []string) bool {
	if len(includes) > 0 {
		allowed := false
		for _, item := range includes {
			if domainName(item) == "github.com" {
				allowed = true
				break
			}
		}
		if !allowed {
			return true
		}
	}
	for _, item := range excludes {
		if domainName(item) == "github.com" {
			return true
		}
	}
	return false
}

func domainName(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if !strings.Contains(value, "://") {
		value = "https://" + value
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return ""
	}
	return strings.TrimPrefix(parsed.Hostname(), "www.")
}

func githubRetryAfter(headers http.Header, now time.Time) time.Duration {
	if parsed := retryafter.Parse(headers.Get("Retry-After"), now, maxRetryAfter); parsed > 0 {
		return parsed
	}
	if headers.Get("X-RateLimit-Remaining") != "0" {
		return 0
	}
	reset, err := strconv.ParseInt(headers.Get("X-RateLimit-Reset"), 10, 64)
	if err != nil {
		return 0
	}
	duration := time.Unix(reset, 0).Sub(now)
	if duration <= 0 {
		return 0
	}
	return min(duration, maxRetryAfter)
}

func (client *Client) resultLimit(requested int) int {
	if requested > 0 && requested < client.maxResults {
		return requested
	}
	return client.maxResults
}

func readBounded(reader io.Reader, maximum int64) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(reader, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maximum {
		return nil, errors.New("github: response exceeds body limit")
	}
	return raw, nil
}

func cleanText(value string) string { return strings.Join(strings.Fields(value), " ") }

func validUserAgent(value string) bool {
	value = strings.TrimSpace(value)
	return len(value) >= 12 && len(value) <= 512 && !strings.ContainsAny(value, "\r\n") &&
		(strings.Contains(value, "http://") || strings.Contains(value, "https://") || strings.Contains(value, "mailto:"))
}

func (client *Client) cooldownRemaining() time.Duration {
	client.mu.Lock()
	defer client.mu.Unlock()
	remaining := client.cooldownUntil.Sub(client.now())
	if remaining <= 0 {
		return 0
	}
	return remaining
}

func (client *Client) openCooldown(duration time.Duration) {
	if duration <= 0 {
		duration = defaultLimitBackoff
	}
	duration = min(duration, maxRetryAfter)
	client.mu.Lock()
	defer client.mu.Unlock()
	until := client.now().Add(duration)
	if until.After(client.cooldownUntil) {
		client.cooldownUntil = until
	}
}
