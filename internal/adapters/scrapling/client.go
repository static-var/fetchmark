// Package scrapling implements bounded discovery through an operator-controlled
// Scrapling browser sidecar. The sidecar may visit only its compiled-in search
// engine endpoints; result content still flows through Fetchmark's normal
// fetch, robots, noindex, and SSRF controls.
package scrapling

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/staticvar/fetchmark/internal/adapters/providerbudget"
	"github.com/staticvar/fetchmark/internal/core/search"
)

const (
	searchPath             = "/v1/search"
	maxProviderResults     = 20
	maxProviderRate        = 4.0
	maxProviderBurst       = 4
	maxProviderConcurrency = 4
	maximumQueryBytes      = 2048
	maximumTitleBytes      = 1024
	maximumSnippetBytes    = 4096
	maximumDiagnosticCount = 16
	maximumRetryAfter      = 24 * time.Hour
)

var supportedEngines = map[string]struct{}{
	"google": {}, "duckduckgo": {}, "brave": {},
}

// Options binds the adapter to one trusted sidecar origin. Plain HTTP must be
// explicitly enabled and should be confined to a private Docker network.
type Options struct {
	Endpoint          string
	HTTPClient        *http.Client
	AllowInsecureHTTP bool
	MaxResults        int
	MaxBodyBytes      int64
	RatePerSecond     float64
	Burst             int
	MaxConcurrency    int
}

// StatusError reports a non-success response from the fixed sidecar endpoint.
type StatusError struct {
	StatusCode int
}

func (statusError *StatusError) Error() string {
	return fmt.Sprintf("scrapling: sidecar status %d", statusError.StatusCode)
}

// Client implements both discovery contracts.
type Client struct {
	endpoint     *url.URL
	httpClient   *http.Client
	maxResults   int
	maxBodyBytes int64
	budget       *providerbudget.Gate
}

var _ search.Searcher = (*Client)(nil)
var _ search.BatchSearcher = (*Client)(nil)

// New validates a fixed origin and clamps scheduling to the sidecar's bounded
// persistent-page pool even if a source pack requests wider browser fan-out.
func New(options Options) (*Client, error) {
	if options.Endpoint == "" || strings.TrimSpace(options.Endpoint) != options.Endpoint {
		return nil, errors.New("scrapling: endpoint origin is required without surrounding whitespace")
	}
	endpoint, err := url.Parse(options.Endpoint)
	if err != nil || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" || endpoint.RawPath != "" || (endpoint.Path != "" && endpoint.Path != "/") {
		return nil, errors.New("scrapling: endpoint must be an origin without credentials, path, query, or fragment")
	}
	if endpoint.Scheme != "https" && !(endpoint.Scheme == "http" && options.AllowInsecureHTTP) {
		return nil, errors.New("scrapling: endpoint must use HTTPS unless insecure HTTP is explicitly enabled")
	}
	if options.HTTPClient == nil {
		return nil, errors.New("scrapling: HTTP client is required")
	}
	if options.MaxResults < 1 || options.MaxBodyBytes < 1 || options.RatePerSecond <= 0 || options.Burst < 1 || options.MaxConcurrency < 1 {
		return nil, errors.New("scrapling: result, body, rate, burst, and concurrency limits must be positive")
	}
	options.MaxResults = min(options.MaxResults, maxProviderResults)
	options.RatePerSecond = min(options.RatePerSecond, maxProviderRate)
	options.Burst = min(options.Burst, maxProviderBurst)
	options.MaxConcurrency = min(options.MaxConcurrency, maxProviderConcurrency)
	budget, err := providerbudget.New(options.RatePerSecond, options.Burst, options.MaxConcurrency)
	if err != nil {
		return nil, fmt.Errorf("scrapling: configure provider budget: %w", err)
	}
	endpoint.Path = searchPath
	return &Client{
		endpoint: endpoint, httpClient: singleAttemptHTTPClient(options.HTTPClient, endpoint),
		maxResults: options.MaxResults, maxBodyBytes: options.MaxBodyBytes, budget: budget,
	}, nil
}

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
	if request == nil || request.URL == nil || request.URL.User != nil || request.URL.Scheme != transport.scheme || !strings.EqualFold(request.URL.Host, transport.host) || request.URL.Path != transport.path || request.URL.RawQuery != "" || request.URL.Fragment != "" {
		return nil, errors.New("scrapling: request is outside configured origin and search path")
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

func (client *Client) SearchBatch(ctx context.Context, query search.Query) (search.SearchBatch, error) {
	started := time.Now()
	fail := func(source, reason string, retryable bool, err error) (search.SearchBatch, error) {
		return search.SearchBatch{
			Provider: "scrapling", Instance: client.endpoint.Host, Status: search.BatchFailed,
			Diagnostics: []search.ProviderDiagnostic{{
				Provider: "scrapling", Instance: client.endpoint.Host, Source: source,
				Reason: reason, Retryable: retryable,
			}},
			Duration: time.Since(started),
		}, err
	}
	if err := ctx.Err(); err != nil {
		return fail("search", "canceled", false, err)
	}
	if control := unsupportedControl(query); control != "" {
		err := &search.UnsupportedControlError{Control: control, Reason: "Scrapling browser discovery cannot preserve this search control"}
		return fail(control, "unsupported_control", false, err)
	}
	queryText := strings.Join(strings.Fields(query.Q), " ")
	if queryText == "" || len(queryText) > maximumQueryBytes || !utf8.ValidString(queryText) {
		return fail("query", "invalid_query", false, errors.New("scrapling: query is empty, invalid, or oversized"))
	}
	engines, err := normalizedEngines(query.Engines)
	if err != nil {
		return fail("engines", "unsupported_control", false, err)
	}
	release, err := client.budget.AcquireSlot(ctx)
	if err != nil {
		return fail("search", "canceled", false, err)
	}
	defer release()
	if err := client.budget.WaitRate(ctx); err != nil {
		return fail("search", "canceled", false, err)
	}

	payload := searchRequest{Query: queryText, Engines: engines, MaxResults: client.resultLimit(query.MaxResults)}
	body, err := json.Marshal(payload)
	if err != nil {
		return fail("search", "request", false, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return fail("search", "request", false, err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "Fetchmark/Scrapling-Sidecar")
	response, err := client.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return fail("search", "canceled", false, ctx.Err())
		}
		return fail("search", "network", true, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fail("search", "http_"+strconv.Itoa(response.StatusCode), response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500, &StatusError{StatusCode: response.StatusCode})
	}
	raw, err := readBounded(response.Body, client.maxBodyBytes)
	if err != nil {
		return fail("search", "oversized", false, err)
	}
	var envelope searchResponse
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fail("search", "malformed", false, fmt.Errorf("scrapling: decode response: %w", err))
	}
	if envelope.Provider != "scrapling" || !search.ValidBatchStatus(envelope.Status) {
		return fail("search", "malformed", false, errors.New("scrapling: invalid provider or status"))
	}

	hits := make([]search.Hit, 0, min(payload.MaxResults, len(envelope.Results)))
	seen := make(map[string]struct{}, len(envelope.Results))
	invalidRows := 0
	for _, result := range envelope.Results {
		if len(hits) == payload.MaxResults {
			break
		}
		parsed, valid := validResultURL(result.URL)
		title := cleanText(result.Title, maximumTitleBytes)
		engine := strings.ToLower(strings.TrimSpace(result.Engine))
		if _, supported := supportedEngines[engine]; !supported || !valid || title == "" || result.Rank < 1 {
			invalidRows++
			continue
		}
		canonical := parsed.String()
		if _, duplicate := seen[canonical]; duplicate {
			continue
		}
		seen[canonical] = struct{}{}
		hits = append(hits, search.Hit{
			URL: canonical, Title: title, Snippet: cleanText(result.Snippet, maximumSnippetBytes),
			Engines: []string{engine}, Metadata: map[string]string{
				"source": "Scrapling", "scrapling_engine": engine, "scrapling_rank": strconv.Itoa(result.Rank),
			},
		})
	}
	diagnostics := mapDiagnostics(envelope.Diagnostics, client.endpoint.Host)
	status := envelope.Status
	if invalidRows > 0 {
		diagnostics = appendBoundedDiagnostic(diagnostics, search.ProviderDiagnostic{
			Provider: "scrapling", Instance: client.endpoint.Host, Source: "search", Reason: "malformed_results",
		})
		if len(hits) > 0 {
			status = search.BatchPartial
		} else {
			status = search.BatchDegradedEmpty
		}
	}
	if !statusConsistent(status, len(hits)) {
		return fail("search", "malformed", false, errors.New("scrapling: status and result count disagree"))
	}
	return search.SearchBatch{
		Hits: hits, Provider: "scrapling", Instance: client.endpoint.Host, Status: status,
		Diagnostics: diagnostics, Duration: time.Since(started),
	}, nil
}

type searchRequest struct {
	Query      string   `json:"query"`
	Engines    []string `json:"engines"`
	MaxResults int      `json:"max_results"`
}

type searchResponse struct {
	Provider    string              `json:"provider"`
	Status      search.BatchStatus  `json:"status"`
	Results     []sidecarResult     `json:"results"`
	Diagnostics []sidecarDiagnostic `json:"diagnostics"`
}

type sidecarResult struct {
	URL     string `json:"url"`
	Title   string `json:"title"`
	Snippet string `json:"snippet"`
	Engine  string `json:"engine"`
	Rank    int    `json:"rank"`
}

type sidecarDiagnostic struct {
	Source       string           `json:"source"`
	Reason       string           `json:"reason"`
	Retryable    bool             `json:"retryable"`
	RetryAfterMS int64            `json:"retry_after_ms"`
	Fallback     *sidecarFallback `json:"fallback"`
}

type sidecarFallback struct {
	Format    string `json:"format"`
	Content   string `json:"content"`
	Truncated bool   `json:"truncated"`
}

func normalizedEngines(raw []string) ([]string, error) {
	if len(raw) == 0 {
		return []string{"google", "duckduckgo", "brave"}, nil
	}
	engines := make([]string, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for _, item := range raw {
		engine := strings.ToLower(strings.TrimSpace(item))
		if _, supported := supportedEngines[engine]; !supported {
			return nil, &search.UnsupportedControlError{Control: "engines", Reason: "unsupported Scrapling engine " + engine}
		}
		if _, duplicate := seen[engine]; duplicate {
			continue
		}
		seen[engine] = struct{}{}
		engines = append(engines, engine)
	}
	if len(engines) == 0 {
		return nil, &search.UnsupportedControlError{Control: "engines", Reason: "no supported Scrapling engines"}
	}
	return engines, nil
}

func unsupportedControl(query search.Query) string {
	switch {
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
	case len(query.IncludeDomains) > 0:
		return "include_domains"
	case len(query.ExcludeDomains) > 0:
		return "exclude_domains"
	default:
		return ""
	}
}

func validResultURL(raw string) (*url.URL, bool) {
	if raw == "" || len(raw) > 8192 || !utf8.ValidString(raw) {
		return nil, false
	}
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.User != nil || parsed.Hostname() == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, false
	}
	return parsed, true
}

func cleanText(value string, maximumBytes int) string {
	if !utf8.ValidString(value) {
		return ""
	}
	value = strings.Join(strings.Fields(value), " ")
	if value == "" || len(value) > maximumBytes || strings.ContainsRune(value, '\x00') {
		return ""
	}
	return value
}

func mapDiagnostics(raw []sidecarDiagnostic, instance string) []search.ProviderDiagnostic {
	diagnostics := make([]search.ProviderDiagnostic, 0, min(len(raw), maximumDiagnosticCount))
	for _, item := range raw {
		if len(diagnostics) == maximumDiagnosticCount {
			break
		}
		source := strings.ToLower(strings.TrimSpace(item.Source))
		reason := strings.ToLower(strings.TrimSpace(item.Reason))
		if _, supported := supportedEngines[source]; !supported || !validToken(reason) {
			continue
		}
		retryAfter := time.Duration(item.RetryAfterMS) * time.Millisecond
		if item.RetryAfterMS < 0 || item.RetryAfterMS > maximumRetryAfter.Milliseconds() {
			retryAfter = maximumRetryAfter
		}
		diagnostic := search.ProviderDiagnostic{
			Provider: "scrapling", Instance: instance, Source: source, Reason: reason,
			Retryable: item.Retryable, RetryAfter: retryAfter,
		}
		if fallback := item.Fallback; fallback != nil && fallback.Format == "cleaned_dom" &&
			fallback.Content != "" && len(fallback.Content) <= search.MaxDiscoveryFallbackBytes &&
			utf8.ValidString(fallback.Content) && !strings.ContainsRune(fallback.Content, '\x00') {
			diagnostic.Fallback = &search.ParseFallback{
				Format: fallback.Format, Content: fallback.Content, Truncated: fallback.Truncated,
			}
		}
		diagnostics = append(diagnostics, diagnostic)
	}
	return diagnostics
}

func appendBoundedDiagnostic(diagnostics []search.ProviderDiagnostic, diagnostic search.ProviderDiagnostic) []search.ProviderDiagnostic {
	if len(diagnostics) < maximumDiagnosticCount {
		return append(diagnostics, diagnostic)
	}
	return diagnostics
}

func validToken(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return true
}

func statusConsistent(status search.BatchStatus, hitCount int) bool {
	if hitCount > 0 {
		return status == search.BatchHealthy || status == search.BatchPartial
	}
	return status == search.BatchDegradedEmpty || status == search.BatchAuthoritativeEmpty || status == search.BatchFailed
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
		return nil, errors.New("scrapling: response exceeds body limit")
	}
	return raw, nil
}
