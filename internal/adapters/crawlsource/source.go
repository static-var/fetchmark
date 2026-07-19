// Package crawlsource fetches configured sitemap and XML feed documents for
// Fetchmark's optional focused-ingestion worker. It owns only the bounded live
// HTTP exchange: persistence, scheduling, and XML candidate extraction remain
// in their respective core/adapters packages.
package crawlsource

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/staticvar/fetchmark/internal/adapters/egress"
	"github.com/staticvar/fetchmark/internal/adapters/robots"
	"github.com/staticvar/fetchmark/internal/core/retryafter"
)

const (
	defaultTimeout                = 30 * time.Second
	defaultMaxWireBytes           = int64(8 << 20)
	defaultMaxDecompressedBytes   = int64(50 << 20)
	defaultMaxResponseHeaderBytes = int64(64 << 10)
	defaultMaxHeaderValueBytes    = 8 << 10
	defaultMaxRetryAfter          = 24 * time.Hour
	hardMaxTimeout                = 10 * time.Minute
	hardMaxWireBytes              = int64(50 << 20)
	hardMaxDecompressedBytes      = int64(50 << 20)
	hardMaxResponseHeaderBytes    = int64(1 << 20)
	hardMaxHeaderValueBytes       = 64 << 10
	hardMaxRetryAfter             = 7 * 24 * time.Hour
	hardMaxFreshness              = 365 * 24 * time.Hour
	// The crawler's validated operator contact URI may be up to 2,048 bytes.
	// Leave room for the stable product/version prefix while remaining below
	// the response/request header value bounds used by this adapter.
	maxUserAgentBytes              = 4 << 10
	maxURLBytes                    = 2048
	maxPathPrefixes                = 256
	maxPathPrefixBytes             = 2048
	maxCombinedPathPrefixBytes     = 64 << 10
	maxConditionalHeaderValueBytes = 8 << 10
)

// Reason is a stable machine-readable failure reason. HTTP error statuses are
// not failures at this layer: they are returned in Result for the scheduler to
// classify without the adapter silently retrying them.
type Reason string

const (
	ReasonInvalidOptions        Reason = "invalid_options"
	ReasonInvalidRequest        Reason = "invalid_request"
	ReasonEgress                Reason = "egress_blocked"
	ReasonRobotsUnreachable     Reason = "robots_unreachable"
	ReasonRobotsDisallowed      Reason = "robots_disallowed"
	ReasonRedirectScope         Reason = "redirect_scope"
	ReasonTransport             Reason = "transport"
	ReasonHeadersTooLarge       Reason = "headers_too_large"
	ReasonContentEncoding       Reason = "content_encoding"
	ReasonNestedEncoding        Reason = "nested_encoding"
	ReasonWireTooLarge          Reason = "wire_too_large"
	ReasonDecompressedTooLarge  Reason = "decompressed_too_large"
	ReasonContentType           Reason = "content_type"
	ReasonMalformedXML          Reason = "malformed_xml"
	ReasonXMLRoot               Reason = "unsupported_xml_root"
	ReasonUnexpectedNotModified Reason = "unexpected_not_modified"
)

// Error reports why no usable source response could be produced.
type Error struct {
	Reason Reason
	URL    string
	Err    error
}

func (e *Error) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("crawl source: %s (%s)", e.Reason, e.URL)
	}
	return fmt.Sprintf("crawl source: %s (%s): %v", e.Reason, e.URL, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// RobotsEvaluator is satisfied by *robots.Checker. Production callers should
// construct that checker from the same egress policy used by Client so robots
// retrieval and source retrieval share one network trust boundary.
type RobotsEvaluator interface {
	Evaluate(context.Context, string, string) robots.Decision
}

// Options fixes all live-fetch policy for a Client. Policy should be
// egress.DefaultExternal in production. The client constructs and owns the
// policy's HTTP transport so dial-time rebinding checks and response-header
// limits cannot be bypassed by an arbitrary injected RoundTripper.
type Options struct {
	Policy egress.Policy
	Robots RobotsEvaluator

	UserAgent              string
	Timeout                time.Duration
	MaxWireBytes           int64
	MaxDecompressedBytes   int64
	MaxResponseHeaderBytes int64
	MaxHeaderValueBytes    int
	MaxRetryAfter          time.Duration

	// Now exists only for deterministic metadata tests.
	Now func() time.Time
}

// Request identifies one explicitly configured discovery source. Every
// redirect must remain on the original normalized origin and inside at least
// one AllowedPathPrefixes entry.
type Request struct {
	URL                 string
	AllowedPathPrefixes []string
	IfNoneMatch         string
	IfModifiedSince     string
	// Per-request limits may tighten but never widen Client's maxima. Zero
	// inherits the client limit, allowing callers with homogeneous roots to
	// omit them.
	MaxWireBytes         int64
	MaxDecompressedBytes int64
}

// Freshness is a conservative interpretation of response cache metadata.
// FreshUntil equals ObservedAt when metadata is absent, malformed, no-store,
// or no-cache. Revalidate preserves the semantic distinction of no-cache.
type Freshness struct {
	FreshUntil time.Time
	NoStore    bool
	Revalidate bool
}

// Result is the bounded transport result. WireBytes counts response entity
// bytes after HTTP transfer framing and before content decompression;
// DecompressedBytes counts the verified XML bytes returned in Body. On 304,
// empty ETag/LastModified values mean the caller should retain its stored
// validators; Freshness.NoStore instead requires discarding them. No-store
// responses always clear validators in this result.
type Result struct {
	Status            int
	Body              []byte
	ContentType       string
	FinalURL          string
	ETag              string
	LastModified      string
	NotModified       bool
	XMLRoot           string
	Freshness         Freshness
	RetryAfter        time.Duration
	WireBytes         int64
	DecompressedBytes int64
	ObservedAt        time.Time
}

// Client performs exactly one HTTP attempt per Fetch call. It is safe for
// concurrent use.
type Client struct {
	policy               egress.Policy
	robots               RobotsEvaluator
	userAgent            string
	httpClient           *http.Client
	maxWireBytes         int64
	maxDecompressedBytes int64
	maxHeaderBytes       int64
	maxHeaderValueBytes  int
	maxRetryAfter        time.Duration
	now                  func() time.Time
}

// New constructs a source client with explicit robots and egress policies.
func New(options Options) (*Client, error) {
	options.UserAgent = strings.TrimSpace(options.UserAgent)
	if options.UserAgent == "" || len(options.UserAgent) > maxUserAgentBytes || containsHeaderControl(options.UserAgent) {
		return nil, failure(ReasonInvalidOptions, "", errors.New("a fixed, valid User-Agent is required"))
	}
	if options.Robots == nil {
		return nil, failure(ReasonInvalidOptions, "", errors.New("robots evaluator is required"))
	}
	if options.Timeout < 0 || options.MaxWireBytes < 0 || options.MaxDecompressedBytes < 0 ||
		options.MaxResponseHeaderBytes < 0 || options.MaxHeaderValueBytes < 0 || options.MaxRetryAfter < 0 {
		return nil, failure(ReasonInvalidOptions, "", errors.New("policy budgets cannot be negative"))
	}
	if options.Timeout == 0 {
		options.Timeout = defaultTimeout
	}
	if options.MaxWireBytes == 0 {
		options.MaxWireBytes = defaultMaxWireBytes
	}
	if options.MaxDecompressedBytes == 0 {
		options.MaxDecompressedBytes = defaultMaxDecompressedBytes
	}
	if options.MaxResponseHeaderBytes == 0 {
		options.MaxResponseHeaderBytes = defaultMaxResponseHeaderBytes
	}
	if options.MaxHeaderValueBytes == 0 {
		options.MaxHeaderValueBytes = defaultMaxHeaderValueBytes
	}
	if options.MaxRetryAfter == 0 {
		options.MaxRetryAfter = defaultMaxRetryAfter
	}
	if options.Timeout > hardMaxTimeout || options.MaxWireBytes > hardMaxWireBytes ||
		options.MaxDecompressedBytes > hardMaxDecompressedBytes || options.MaxResponseHeaderBytes > hardMaxResponseHeaderBytes ||
		options.MaxHeaderValueBytes > hardMaxHeaderValueBytes || int64(options.MaxHeaderValueBytes) > options.MaxResponseHeaderBytes ||
		options.MaxRetryAfter > hardMaxRetryAfter {
		return nil, failure(ReasonInvalidOptions, "", errors.New("policy budget exceeds its hard safety maximum"))
	}
	if options.Now == nil {
		options.Now = time.Now
	}

	httpClient := options.Policy.HTTPClient(options.Timeout)
	transport, ok := httpClient.Transport.(*http.Transport)
	if !ok {
		return nil, failure(ReasonInvalidOptions, "", errors.New("egress policy did not produce an HTTP transport"))
	}
	transport.MaxResponseHeaderBytes = options.MaxResponseHeaderBytes
	// Fetch sets Accept-Encoding explicitly and accounts for the compressed
	// entity body itself. Disable transparent decompression as a second guard.
	transport.DisableCompression = true

	return &Client{
		policy: options.Policy, robots: options.Robots, userAgent: options.UserAgent,
		httpClient: httpClient, maxWireBytes: options.MaxWireBytes,
		maxDecompressedBytes: options.MaxDecompressedBytes,
		maxHeaderBytes:       options.MaxResponseHeaderBytes,
		maxHeaderValueBytes:  options.MaxHeaderValueBytes,
		maxRetryAfter:        options.MaxRetryAfter, now: options.Now,
	}, nil
}

// Fetch performs one non-retrying, robots-aware request.
func (c *Client) Fetch(ctx context.Context, request Request) (Result, error) {
	observedAt := c.now().UTC()
	result := Result{ObservedAt: observedAt, Freshness: Freshness{FreshUntil: observedAt}}

	original, scope, err := validateRequest(request)
	if err != nil {
		return result, failure(ReasonInvalidRequest, request.URL, err)
	}
	wireLimit, decompressedLimit, err := c.requestLimits(request)
	if err != nil {
		return result, failure(ReasonInvalidRequest, request.URL, err)
	}
	if err := c.policy.Validate(ctx, original.String()); err != nil {
		return result, failure(ReasonEgress, original.String(), err)
	}
	if err := c.checkRobots(ctx, original.String()); err != nil {
		return result, err
	}

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, original.String(), nil)
	if err != nil {
		return result, failure(ReasonInvalidRequest, request.URL, err)
	}
	httpRequest.Header.Set("User-Agent", c.userAgent)
	httpRequest.Header.Set("Accept", "application/xml,text/xml,application/rss+xml,application/atom+xml,application/sitemap+xml,application/gzip;q=0.8")
	httpRequest.Header.Set("Accept-Encoding", "gzip")
	if request.IfNoneMatch != "" {
		httpRequest.Header.Set("If-None-Match", request.IfNoneMatch)
	}
	if request.IfModifiedSince != "" {
		httpRequest.Header.Set("If-Modified-Since", request.IfModifiedSince)
	}

	requestClient := *c.httpClient
	policyRedirect := c.httpClient.CheckRedirect
	requestClient.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		// net/http copies initial headers before this callback. Conditionals
		// describe only the configured source representation, never a target.
		next.Header.Del("If-None-Match")
		next.Header.Del("If-Modified-Since")
		if !scope.allows(next.URL) {
			return errRedirectScope
		}
		if policyRedirect != nil {
			if err := policyRedirect(next, via); err != nil {
				return err
			}
		}
		if err := c.checkRobots(next.Context(), next.URL.String()); err != nil {
			return err
		}
		return nil
	}

	response, err := requestClient.Do(httpRequest)
	if err != nil {
		return result, classifyTransportError(original.String(), err)
	}
	defer response.Body.Close()
	// Freshness is measured when response headers are observed, not before
	// DNS/robots/network latency. Error results retain the call-start time.
	observedAt = c.now().UTC()
	result.ObservedAt = observedAt
	result.Freshness.FreshUntil = observedAt

	result.Status = response.StatusCode
	result.FinalURL = response.Request.URL.String()
	if err := c.validateHeaders(response.Header); err != nil {
		return result, failure(ReasonHeadersTooLarge, result.FinalURL, err)
	}

	contentType, err := singleHeader(response.Header, "Content-Type")
	if err != nil {
		return result, failure(ReasonHeadersTooLarge, result.FinalURL, err)
	}
	result.ContentType = normalizedMediaType(contentType)
	if result.ETag, err = singleHeader(response.Header, "ETag"); err != nil {
		return result, failure(ReasonHeadersTooLarge, result.FinalURL, err)
	}
	if result.LastModified, err = singleHeader(response.Header, "Last-Modified"); err != nil {
		return result, failure(ReasonHeadersTooLarge, result.FinalURL, err)
	}
	result.Freshness = parseFreshness(response.Header, observedAt)
	result.RetryAfter = parseRetryAfter(response.Header, observedAt, c.maxRetryAfter)
	if result.Freshness.NoStore || result.FinalURL != original.String() {
		result.ETag = ""
		result.LastModified = ""
	}

	if response.StatusCode == http.StatusNotModified {
		if request.IfNoneMatch == "" && request.IfModifiedSince == "" || result.FinalURL != original.String() {
			return result, failure(ReasonUnexpectedNotModified, result.FinalURL, errors.New("304 is valid only for the original conditional URL"))
		}
		result.NotModified = true
		return result, nil
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		// Status classification belongs to the scheduler. Avoid buffering
		// potentially hostile HTML error documents here.
		return result, nil
	}

	mediaType, payloadGzip, err := classifyContentType(contentType)
	if err != nil {
		return result, failure(ReasonContentType, result.FinalURL, err)
	}
	result.ContentType = mediaType
	contentGzip, err := contentEncodingIsGzip(response.Header)
	if err != nil {
		return result, failure(ReasonContentEncoding, result.FinalURL, err)
	}
	if contentGzip && payloadGzip {
		return result, failure(ReasonNestedEncoding, result.FinalURL, errors.New("HTTP and payload gzip layers cannot be combined"))
	}

	wire, err := readBounded(response.Body, wireLimit)
	result.WireBytes = int64(len(wire))
	if err != nil {
		if errors.Is(err, errWireLimit) {
			return result, failure(ReasonWireTooLarge, result.FinalURL, err)
		}
		return result, failure(ReasonTransport, result.FinalURL, err)
	}
	body := wire
	if contentGzip || payloadGzip {
		body, err = decompressGzip(wire, decompressedLimit)
		result.DecompressedBytes = int64(len(body))
		if err != nil {
			if errors.Is(err, errNestedGzip) {
				return result, failure(ReasonNestedEncoding, result.FinalURL, err)
			}
			if errors.Is(err, errDecompressedLimit) {
				return result, failure(ReasonDecompressedTooLarge, result.FinalURL, err)
			}
			return result, failure(ReasonContentEncoding, result.FinalURL, err)
		}
	} else {
		if hasGzipMagic(body) {
			return result, failure(ReasonContentEncoding, result.FinalURL, errors.New("undeclared gzip payload"))
		}
		result.DecompressedBytes = int64(len(body))
		if int64(len(body)) > decompressedLimit {
			return result, failure(ReasonDecompressedTooLarge, result.FinalURL, errDecompressedLimit)
		}
	}

	root, err := verifyXML(body)
	if err != nil {
		var rootErr *unsupportedRootError
		if errors.As(err, &rootErr) {
			return result, failure(ReasonXMLRoot, result.FinalURL, err)
		}
		return result, failure(ReasonMalformedXML, result.FinalURL, err)
	}
	result.Body = body
	result.XMLRoot = root
	return result, nil
}

func (c *Client) requestLimits(request Request) (int64, int64, error) {
	if request.MaxWireBytes < 0 || request.MaxDecompressedBytes < 0 {
		return 0, 0, errors.New("request byte limits cannot be negative")
	}
	wireLimit := request.MaxWireBytes
	if wireLimit == 0 {
		wireLimit = c.maxWireBytes
	}
	decompressedLimit := request.MaxDecompressedBytes
	if decompressedLimit == 0 {
		decompressedLimit = c.maxDecompressedBytes
	}
	if wireLimit > c.maxWireBytes || decompressedLimit > c.maxDecompressedBytes {
		return 0, 0, errors.New("request byte limits cannot exceed client maxima")
	}
	return wireLimit, decompressedLimit, nil
}

func (c *Client) checkRobots(ctx context.Context, rawURL string) error {
	decision := c.robots.Evaluate(ctx, c.userAgent, rawURL)
	if decision.Err != nil {
		return failure(ReasonRobotsUnreachable, rawURL, decision.Err)
	}
	if !decision.Allowed {
		return failure(ReasonRobotsDisallowed, rawURL, errors.New("robots.txt disallows source"))
	}
	return nil
}

func (c *Client) validateHeaders(header http.Header) error {
	var total int64
	for name, values := range header {
		total += int64(len(name) + 4)
		for _, value := range values {
			if len(value) > c.maxHeaderValueBytes {
				return fmt.Errorf("header %q exceeds value limit", name)
			}
			total += int64(len(value) + 2)
			if total > c.maxHeaderBytes {
				return errors.New("response headers exceed total limit")
			}
		}
	}
	return nil
}

type sourceScope struct {
	scheme   string
	hostname string
	port     string
	prefixes []string
}

func validateRequest(request Request) (*url.URL, sourceScope, error) {
	if len(request.URL) == 0 || len(request.URL) > maxURLBytes {
		return nil, sourceScope{}, errors.New("source URL must be 1..2048 bytes")
	}
	parsed, err := url.Parse(request.URL)
	if err != nil || parsed.User != nil || parsed.Opaque != "" || parsed.Hostname() == "" || parsed.Fragment != "" ||
		(!strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https")) {
		return nil, sourceScope{}, errors.New("source URL must be absolute HTTP(S) without credentials or fragments")
	}
	pathValue, ok := unambiguousPath(parsed)
	if !ok {
		return nil, sourceScope{}, errors.New("source URL path is ambiguous")
	}
	if len(request.AllowedPathPrefixes) == 0 || len(request.AllowedPathPrefixes) > maxPathPrefixes {
		return nil, sourceScope{}, errors.New("an explicit bounded source path allowlist is required")
	}
	prefixes := make([]string, len(request.AllowedPathPrefixes))
	total := 0
	seen := make(map[string]struct{}, len(prefixes))
	for index, prefix := range request.AllowedPathPrefixes {
		normalized, valid := normalizedPrefix(prefix)
		if !valid {
			return nil, sourceScope{}, fmt.Errorf("allowed path prefix %d is not normalized", index)
		}
		total += len(normalized)
		if total > maxCombinedPathPrefixBytes {
			return nil, sourceScope{}, errors.New("allowed path prefixes exceed combined byte limit")
		}
		if _, duplicate := seen[normalized]; duplicate {
			return nil, sourceScope{}, fmt.Errorf("allowed path prefix %d is duplicated", index)
		}
		seen[normalized] = struct{}{}
		prefixes[index] = normalized
	}
	if err := validateConditional("If-None-Match", request.IfNoneMatch); err != nil {
		return nil, sourceScope{}, err
	}
	if err := validateConditional("If-Modified-Since", request.IfModifiedSince); err != nil {
		return nil, sourceScope{}, err
	}
	scope := sourceScope{
		scheme: strings.ToLower(parsed.Scheme), hostname: strings.ToLower(parsed.Hostname()),
		port: normalizedPort(parsed), prefixes: prefixes,
	}
	if !scope.allowsPath(pathValue) {
		return nil, sourceScope{}, errors.New("source URL is outside its path allowlist")
	}
	return parsed, scope, nil
}

func (scope sourceScope) allows(candidate *url.URL) bool {
	if candidate == nil || candidate.User != nil || candidate.Opaque != "" || candidate.Fragment != "" ||
		!strings.EqualFold(candidate.Scheme, scope.scheme) || !strings.EqualFold(candidate.Hostname(), scope.hostname) || normalizedPort(candidate) != scope.port {
		return false
	}
	pathValue, ok := unambiguousPath(candidate)
	return ok && scope.allowsPath(pathValue)
}

func (scope sourceScope) allowsPath(pathValue string) bool {
	for _, prefix := range scope.prefixes {
		if strings.HasPrefix(pathValue, prefix) {
			return true
		}
	}
	return false
}

func normalizedPrefix(prefix string) (string, bool) {
	if prefix == "" || len(prefix) > maxPathPrefixBytes || !strings.HasPrefix(prefix, "/") ||
		strings.ContainsAny(prefix, "?#\\%") || !utf8.ValidString(prefix) {
		return "", false
	}
	for _, segment := range strings.Split(prefix, "/") {
		if segment == "." || segment == ".." {
			return "", false
		}
	}
	return prefix, true
}

func unambiguousPath(candidate *url.URL) (string, bool) {
	if candidate == nil {
		return "", false
	}
	escaped := strings.ToLower(candidate.EscapedPath())
	if strings.Contains(escaped, "%2f") || strings.Contains(escaped, "%5c") || strings.Contains(candidate.Path, `\`) || !utf8.ValidString(candidate.Path) {
		return "", false
	}
	pathValue := candidate.Path
	if pathValue == "" {
		pathValue = "/"
	}
	if !strings.HasPrefix(pathValue, "/") {
		return "", false
	}
	for _, segment := range strings.Split(pathValue, "/") {
		if segment == "." || segment == ".." {
			return "", false
		}
	}
	return pathValue, true
}

func normalizedPort(candidate *url.URL) string {
	if port := candidate.Port(); port != "" {
		return port
	}
	switch strings.ToLower(candidate.Scheme) {
	case "http":
		return "80"
	case "https":
		return "443"
	default:
		return ""
	}
}

func validateConditional(name, value string) error {
	if len(value) > maxConditionalHeaderValueBytes || containsHeaderControl(value) {
		return fmt.Errorf("%s is not a valid bounded header value", name)
	}
	return nil
}

func containsHeaderControl(value string) bool {
	for _, character := range value {
		if character == '\r' || character == '\n' || character == 0 || character == 0x7f {
			return true
		}
	}
	return false
}

func singleHeader(header http.Header, name string) (string, error) {
	values := header.Values(name)
	if len(values) > 1 {
		return "", fmt.Errorf("header %q must be singular", name)
	}
	if len(values) == 0 {
		return "", nil
	}
	return values[0], nil
}

func normalizedMediaType(raw string) string {
	mediaType, _, err := mime.ParseMediaType(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(mediaType)
}

func classifyContentType(raw string) (mediaType string, payloadGzip bool, err error) {
	mediaType, _, err = mime.ParseMediaType(raw)
	if err != nil {
		return "", false, errors.New("valid Content-Type is required")
	}
	mediaType = strings.ToLower(mediaType)
	switch mediaType {
	case "application/gzip", "application/x-gzip":
		return mediaType, true, nil
	case "application/xml", "text/xml", "application/rss+xml", "application/atom+xml", "application/sitemap+xml":
		return mediaType, false, nil
	default:
		if strings.HasSuffix(mediaType, "+xml") {
			return mediaType, false, nil
		}
		return mediaType, false, fmt.Errorf("unsupported discovery source media type %q", mediaType)
	}
}

func contentEncodingIsGzip(header http.Header) (bool, error) {
	values := header.Values("Content-Encoding")
	if len(values) == 0 {
		return false, nil
	}
	combined := strings.TrimSpace(strings.Join(values, ","))
	if strings.EqualFold(combined, "identity") || combined == "" {
		return false, nil
	}
	if strings.EqualFold(combined, "gzip") {
		return true, nil
	}
	return false, fmt.Errorf("unsupported or nested Content-Encoding %q", combined)
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return body, err
	}
	if int64(len(body)) > limit {
		return body, fmt.Errorf("%w: body exceeds %d bytes", errWireLimit, limit)
	}
	return body, nil
}

var (
	errRedirectScope     = errors.New("crawl source: redirect outside configured scope")
	errWireLimit         = errors.New("crawl source: response body exceeds wire limit")
	errNestedGzip        = errors.New("crawl source: nested gzip is not allowed")
	errDecompressedLimit = errors.New("crawl source: decompressed body exceeds limit")
)

func decompressGzip(wire []byte, limit int64) ([]byte, error) {
	compressed := bytes.NewReader(wire)
	reader, err := gzip.NewReader(compressed)
	if err != nil {
		return nil, err
	}
	// A discovery response is one representation, not a concatenation of
	// independently compressed documents. Stopping after the first member lets
	// us detect and reject trailing members explicitly.
	reader.Multistream(false)
	body, readErr := io.ReadAll(io.LimitReader(reader, limit+1))
	closeErr := reader.Close()
	if readErr != nil {
		return body, readErr
	}
	if closeErr != nil {
		return body, closeErr
	}
	if int64(len(body)) > limit {
		return body, errDecompressedLimit
	}
	if compressed.Len() != 0 {
		return body, errNestedGzip
	}
	if hasGzipMagic(body) {
		return body, errNestedGzip
	}
	return body, nil
}

func hasGzipMagic(body []byte) bool {
	return len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b
}

type unsupportedRootError struct{ root string }

func (e *unsupportedRootError) Error() string { return fmt.Sprintf("unsupported XML root %q", e.root) }

func verifyXML(body []byte) (string, error) {
	decoder := xml.NewDecoder(bytes.NewReader(body))
	root := ""
	depth := 0
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", err
		}
		switch token := token.(type) {
		case xml.StartElement:
			if depth == 0 {
				if root != "" {
					return "", errors.New("XML document contains multiple roots")
				}
				root = strings.ToLower(token.Name.Local)
			}
			depth++
		case xml.EndElement:
			depth--
			if depth < 0 {
				return "", errors.New("XML document has an unmatched closing element")
			}
		case xml.CharData:
			if depth == 0 && strings.TrimSpace(string(token)) != "" {
				return "", errors.New("XML document contains text outside its root")
			}
		}
	}
	if root == "" || depth != 0 {
		return "", errors.New("XML document has no root element")
	}
	switch root {
	case "urlset", "sitemapindex", "rss", "feed":
		return root, nil
	default:
		return "", &unsupportedRootError{root: root}
	}
}

func parseFreshness(header http.Header, observed time.Time) Freshness {
	freshness := Freshness{FreshUntil: observed}
	cacheControl := strings.Join(header.Values("Cache-Control"), ",")
	if cacheControl != "" {
		var maxAge *int64
		malformedMaxAge := false
		for _, rawDirective := range strings.Split(cacheControl, ",") {
			directive := strings.TrimSpace(rawDirective)
			name, value, hasValue := strings.Cut(directive, "=")
			switch strings.ToLower(strings.TrimSpace(name)) {
			case "no-store":
				freshness.NoStore = true
			case "no-cache":
				freshness.Revalidate = true
			case "max-age":
				if !hasValue {
					malformedMaxAge = true
					continue
				}
				seconds, err := strconv.ParseInt(strings.Trim(strings.TrimSpace(value), `"`), 10, 64)
				if err != nil || seconds < 0 {
					malformedMaxAge = true
					continue
				}
				if hardMaximum := int64(hardMaxFreshness / time.Second); seconds > hardMaximum {
					seconds = hardMaximum
				}
				if maxAge == nil || seconds < *maxAge {
					copy := seconds
					maxAge = &copy
				}
			}
		}
		if freshness.NoStore || freshness.Revalidate || malformedMaxAge {
			return freshness
		}
		if maxAge != nil {
			age, valid := responseAge(header, observed, *maxAge)
			if !valid {
				return freshness
			}
			remaining := *maxAge - age
			freshness.FreshUntil = safeAddSeconds(observed, remaining)
			return freshness
		}
	}
	if strings.EqualFold(strings.TrimSpace(header.Get("Pragma")), "no-cache") {
		freshness.Revalidate = true
		return freshness
	}
	if expires, err := http.ParseTime(header.Get("Expires")); err == nil && expires.After(observed) {
		maximum := observed.Add(hardMaxFreshness)
		if expires.After(maximum) {
			expires = maximum
		}
		freshness.FreshUntil = expires.UTC()
	}
	return freshness
}

func responseAge(header http.Header, observed time.Time, maximum int64) (int64, bool) {
	ageSeconds := int64(0)
	ageValues := header.Values("Age")
	if len(ageValues) > 1 {
		return 0, false
	}
	if len(ageValues) == 1 {
		parsed, err := strconv.ParseInt(strings.TrimSpace(ageValues[0]), 10, 64)
		if err != nil || parsed < 0 {
			return 0, false
		}
		ageSeconds = parsed
	}
	dateValues := header.Values("Date")
	if len(dateValues) > 1 {
		return 0, false
	}
	if len(dateValues) == 1 {
		responseDate, err := http.ParseTime(dateValues[0])
		if err != nil {
			return 0, false
		}
		if elapsed := observed.Sub(responseDate); elapsed > 0 {
			apparentAge := int64(elapsed / time.Second)
			if elapsed%time.Second != 0 {
				apparentAge++
			}
			if apparentAge > ageSeconds {
				ageSeconds = apparentAge
			}
		}
	}
	if ageSeconds > maximum {
		ageSeconds = maximum
	}
	return ageSeconds, true
}

func safeAddSeconds(now time.Time, seconds int64) time.Time {
	maxSeconds := int64(hardMaxFreshness / time.Second)
	if seconds > maxSeconds {
		seconds = maxSeconds
	}
	return now.Add(time.Duration(seconds) * time.Second)
}

func parseRetryAfter(header http.Header, observed time.Time, maximum time.Duration) time.Duration {
	return retryafter.Parse(header.Get("Retry-After"), observed, maximum)
}

func classifyTransportError(rawURL string, err error) error {
	var sourceErr *Error
	if errors.As(err, &sourceErr) {
		return sourceErr
	}
	if errors.Is(err, errRedirectScope) {
		return failure(ReasonRedirectScope, rawURL, err)
	}
	var egressErr *egress.Error
	if errors.As(err, &egressErr) {
		return failure(ReasonEgress, rawURL, err)
	}
	return failure(ReasonTransport, rawURL, err)
}

func failure(reason Reason, rawURL string, err error) *Error {
	return &Error{Reason: reason, URL: rawURL, Err: err}
}
