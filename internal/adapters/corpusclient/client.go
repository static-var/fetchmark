// Package corpusclient submits focused admissions to a fixed Fetchmark origin.
// It is intentionally narrower than the public parse API: Fetchmark remains the
// policy-enforcing fetch, extraction, and persistence boundary, and may return
// only bounded outbound-link metadata when the crawler explicitly requests it.
package corpusclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/core/retryafter"
)

const (
	defaultTimeout          = 20 * time.Second
	defaultMaxResponseBytes = int64(1 << 20)
	maxRetryAfter           = 24 * time.Hour
	maxAdmissionBytes       = 900 << 10
	maxOutboundLinks        = 64
	admissionPath           = "/admin/corpus/focused-admissions"
)

var (
	// ErrResponseTooLarge indicates that Fetchmark exceeded the configured
	// response budget. The body is never retained in the returned error.
	ErrResponseTooLarge = errors.New("corpusclient: response too large")
	// ErrInvalidResponse indicates a response that does not match the strict
	// content-free admission-result contract.
	ErrInvalidResponse = errors.New("corpusclient: invalid response")
)

// FailureClass tells an operator loop whether retrying can be useful.
type FailureClass string

const (
	// FailureFatal denotes authentication or server-mode/configuration errors
	// that require operator action (401, 403, and 409).
	FailureFatal FailureClass = "fatal"
	// FailureTransient denotes transport errors, rate limiting, and server
	// failures that may succeed after a bounded delay.
	FailureTransient FailureClass = "transient"
	// FailurePermanent denotes other client/protocol errors that should not be
	// retried unchanged.
	FailurePermanent FailureClass = "permanent"
)

// AdmissionStatus is the bounded outcome vocabulary returned by Fetchmark.
type AdmissionStatus string

const (
	StatusAdmitted AdmissionStatus = "admitted"
	StatusRejected AdmissionStatus = "rejected"
	StatusFailed   AdmissionStatus = "failed"
)

// AdmissionResult preserves each URL's outcome, including partial batches in
// which some URLs were admitted and others were rejected or failed.
type AdmissionResult struct {
	URL           string          `json:"url"`
	Status        AdmissionStatus `json:"status"`
	Reason        string          `json:"reason,omitempty"`
	OutboundLinks *[]string       `json:"outbound_links,omitempty"`
}

// AdmissionResponse is Fetchmark's content-free batch mutation response.
type AdmissionResponse struct {
	Count   int               `json:"count"`
	Results []AdmissionResult `json:"results"`
}

// FocusedAdmission is a URL plus the validated job path policy that Fetchmark
// must re-enforce on every redirect. It intentionally carries no page bytes.
type FocusedAdmission struct {
	URL                 string   `json:"url"`
	AllowedPathPrefixes []string `json:"allowed_path_prefixes"`
	DeniedPathPrefixes  []string `json:"denied_path_prefixes,omitempty"`
	DeniedURLs          []string `json:"denied_urls,omitempty"`
	MaxOutboundLinks    int      `json:"max_outbound_links,omitempty"`
}

// AdmissionError contains only bounded scheduling metadata. It intentionally
// excludes response bodies, request bodies, and the API key.
type AdmissionError struct {
	Class      FailureClass
	StatusCode int
	RetryAfter time.Duration
	underlying error
}

func (e *AdmissionError) Error() string {
	if e.StatusCode != 0 {
		return fmt.Sprintf("corpusclient: admission request failed: class=%s status=%d", e.Class, e.StatusCode)
	}
	return fmt.Sprintf("corpusclient: admission request failed: class=%s", e.Class)
}

// Unwrap exposes the underlying transport or protocol error without adding
// secret-bearing request data to Error's stable text.
func (e *AdmissionError) Unwrap() error { return e.underlying }

// Options defines the fixed local Fetchmark control-plane boundary.
type Options struct {
	// BaseURL must be an HTTP(S) origin with no credentials, query, fragment,
	// or path other than an optional slash.
	BaseURL string
	APIKey  string
	// HTTPClient optionally supplies a transport. It is copied before redirect
	// policy is replaced, so caller-owned clients are not mutated.
	HTTPClient *http.Client
	// Timeout bounds the complete request. Zero defaults to 20 seconds.
	Timeout time.Duration
	// MaxResponseBytes bounds both success and error bodies. Zero defaults to
	// one MiB.
	MaxResponseBytes int64
}

// Client is safe for concurrent use.
type Client struct {
	endpoint         string
	apiKey           string
	httpClient       *http.Client
	timeout          time.Duration
	maxResponseBytes int64
	now              func() time.Time
}

// New validates the fixed origin and creates a redirect-refusing client.
func New(options Options) (*Client, error) {
	base, err := parseBaseOrigin(options.BaseURL)
	if err != nil {
		return nil, err
	}
	if options.APIKey == "" || strings.ContainsAny(options.APIKey, "\r\n") {
		return nil, errors.New("corpusclient: API key is required and must be a valid header value")
	}
	if options.Timeout < 0 {
		return nil, errors.New("corpusclient: timeout must not be negative")
	}
	if options.Timeout == 0 {
		options.Timeout = defaultTimeout
	}
	if options.MaxResponseBytes < 0 {
		return nil, errors.New("corpusclient: response limit must not be negative")
	}
	if options.MaxResponseBytes == 0 {
		options.MaxResponseBytes = defaultMaxResponseBytes
	}

	client := &http.Client{}
	if options.HTTPClient != nil {
		copy := *options.HTTPClient
		client = &copy
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

	endpoint := *base
	endpoint.Path = admissionPath
	endpoint.RawPath = ""
	return &Client{
		endpoint:         endpoint.String(),
		apiKey:           options.APIKey,
		httpClient:       client,
		timeout:          options.Timeout,
		maxResponseBytes: options.MaxResponseBytes,
		now:              time.Now,
	}, nil
}

// Admit submits one URL-and-policy admission. The method does not retry: the focused
// crawler owns scheduling and can use AdmissionError to apply host-wide
// backoff without duplicating mutations.
func (c *Client) Admit(ctx context.Context, admission FocusedAdmission) (AdmissionResponse, error) {
	if strings.TrimSpace(admission.URL) == "" || len(admission.AllowedPathPrefixes) == 0 {
		return AdmissionResponse{}, errors.New("corpusclient: URL and allowed path prefixes are required")
	}
	if admission.MaxOutboundLinks < 0 || admission.MaxOutboundLinks > maxOutboundLinks {
		return AdmissionResponse{}, fmt.Errorf("corpusclient: max outbound links must be between 0 and %d", maxOutboundLinks)
	}
	body, err := json.Marshal(admission)
	if err != nil {
		return AdmissionResponse{}, fmt.Errorf("corpusclient: encode request: %w", err)
	}
	if len(body) > maxAdmissionBytes {
		return AdmissionResponse{}, errors.New("corpusclient: focused admission policy exceeds request budget")
	}

	requestContext, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return AdmissionResponse{}, fmt.Errorf("corpusclient: create request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("X-API-Key", c.apiKey)

	response, err := c.httpClient.Do(request)
	if err != nil {
		return AdmissionResponse{}, &AdmissionError{Class: FailureTransient, underlying: err}
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		class := classifyStatus(response.StatusCode)
		retryAfter := parseRetryAfter(response.Header.Get("Retry-After"), c.now())
		_, readErr := readBounded(response.Body, c.maxResponseBytes)
		if readErr != nil {
			err = readErr
		} else {
			err = fmt.Errorf("unexpected HTTP status %d", response.StatusCode)
		}
		return AdmissionResponse{}, &AdmissionError{
			Class: class, StatusCode: response.StatusCode, RetryAfter: retryAfter, underlying: err,
		}
	}

	raw, err := readBounded(response.Body, c.maxResponseBytes)
	if err != nil {
		class := FailureTransient
		if errors.Is(err, ErrResponseTooLarge) {
			class = FailurePermanent
		}
		return AdmissionResponse{}, &AdmissionError{Class: class, StatusCode: response.StatusCode, underlying: err}
	}
	decoded, err := decodeResponse(raw, admission.MaxOutboundLinks)
	if err != nil {
		return AdmissionResponse{}, &AdmissionError{Class: FailurePermanent, StatusCode: response.StatusCode, underlying: err}
	}
	if decoded.Count != 1 || len(decoded.Results) != 1 || decoded.Results[0].URL != admission.URL {
		return AdmissionResponse{}, &AdmissionError{Class: FailurePermanent, StatusCode: response.StatusCode, underlying: ErrInvalidResponse}
	}
	return decoded, nil
}

func parseBaseOrigin(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed == nil || parsed.Opaque != "" || parsed.Hostname() == "" || parsed.User != nil ||
		(!strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https")) ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("corpusclient: base URL must be an HTTP(S) origin without credentials, query, fragment, or base path")
	}
	// Force validation of a malformed explicit port before constructing the
	// request URL. URL.Port reports an empty string for no port.
	if strings.Contains(parsed.Host, ":") {
		if _, err := url.ParseRequestURI(parsed.String()); err != nil {
			return nil, errors.New("corpusclient: base URL has an invalid authority")
		}
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	parsed.Path = ""
	parsed.RawPath = ""
	return parsed, nil
}

func classifyStatus(status int) FailureClass {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusConflict:
		return FailureFatal
	case http.StatusTooManyRequests:
		return FailureTransient
	default:
		if status >= 500 && status <= 599 {
			return FailureTransient
		}
		return FailurePermanent
	}
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	return retryafter.Parse(value, now, maxRetryAfter)
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, fmt.Errorf("corpusclient: read response: %w", err)
	}
	if int64(len(raw)) > limit {
		return nil, ErrResponseTooLarge
	}
	return raw, nil
}

func decodeResponse(raw []byte, requestedOutboundLinks int) (AdmissionResponse, error) {
	type wireAdmissionResult struct {
		URL           string          `json:"url"`
		Status        AdmissionStatus `json:"status"`
		Reason        string          `json:"reason,omitempty"`
		OutboundLinks json.RawMessage `json:"outbound_links,omitempty"`
	}
	type wireResponse struct {
		Count   *int                   `json:"count"`
		Results *[]wireAdmissionResult `json:"results"`
	}
	var wire wireResponse
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return AdmissionResponse{}, fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return AdmissionResponse{}, fmt.Errorf("%w: response must contain exactly one JSON value", ErrInvalidResponse)
	}
	if wire.Count == nil || wire.Results == nil || *wire.Count < 0 || *wire.Count != len(*wire.Results) {
		return AdmissionResponse{}, fmt.Errorf("%w: count and results do not agree", ErrInvalidResponse)
	}
	results := make([]AdmissionResult, 0, len(*wire.Results))
	for _, wireResult := range *wire.Results {
		result := AdmissionResult{URL: wireResult.URL, Status: wireResult.Status, Reason: wireResult.Reason}
		if len(wireResult.OutboundLinks) > 0 {
			if bytes.Equal(bytes.TrimSpace(wireResult.OutboundLinks), []byte("null")) {
				return AdmissionResponse{}, fmt.Errorf("%w: outbound links must be an array", ErrInvalidResponse)
			}
			var links []string
			if err := json.Unmarshal(wireResult.OutboundLinks, &links); err != nil || links == nil {
				return AdmissionResponse{}, fmt.Errorf("%w: outbound links must be an array", ErrInvalidResponse)
			}
			result.OutboundLinks = &links
		}
		resultURL, err := url.Parse(result.URL)
		if err != nil || len(result.URL) > 2048 || resultURL.Hostname() == "" || resultURL.User != nil || resultURL.Fragment != "" ||
			(!strings.EqualFold(resultURL.Scheme, "http") && !strings.EqualFold(resultURL.Scheme, "https")) {
			return AdmissionResponse{}, fmt.Errorf("%w: result URL must be an absolute HTTP(S) URL", ErrInvalidResponse)
		}
		switch result.Status {
		case StatusAdmitted, StatusRejected, StatusFailed:
		default:
			return AdmissionResponse{}, fmt.Errorf("%w: unknown result status", ErrInvalidResponse)
		}
		if err := validateOutboundLinks(result, requestedOutboundLinks); err != nil {
			return AdmissionResponse{}, err
		}
		results = append(results, result)
	}
	return AdmissionResponse{Count: *wire.Count, Results: results}, nil
}

func validateOutboundLinks(result AdmissionResult, requested int) error {
	if result.OutboundLinks == nil {
		return nil
	}
	links := *result.OutboundLinks
	if result.Status != StatusAdmitted || requested <= 0 || len(links) > requested || len(links) > maxOutboundLinks {
		return fmt.Errorf("%w: outbound links exceed the requested admission contract", ErrInvalidResponse)
	}
	seen := make(map[string]struct{}, len(links))
	for _, raw := range links {
		if raw == "" || len(raw) > 2048 {
			return fmt.Errorf("%w: outbound link is empty or oversized", ErrInvalidResponse)
		}
		parsed, err := url.Parse(raw)
		if err != nil || parsed == nil || parsed.Opaque != "" || parsed.User != nil || parsed.Hostname() == "" || parsed.Fragment != "" ||
			(!strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https")) {
			return fmt.Errorf("%w: outbound link must be an absolute HTTP(S) URL without credentials or fragments", ErrInvalidResponse)
		}
		canonical, err := cache.CanonicalURL(raw)
		if err != nil || canonical != raw {
			return fmt.Errorf("%w: outbound link must be canonical", ErrInvalidResponse)
		}
		if _, duplicate := seen[canonical]; duplicate {
			return fmt.Errorf("%w: duplicate outbound link", ErrInvalidResponse)
		}
		seen[canonical] = struct{}{}
	}
	return nil
}
