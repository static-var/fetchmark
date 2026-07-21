// Package fetcher performs bounded-concurrency HTTP fetches subject to an
// egress policy, robots.txt, per-host politeness, and a set of safety
// budgets (body size, decompressed size, MIME allowlist, redirects).
//
// The fetcher is intentionally built on net/http rather than resty: the
// retry/backoff loop we need is ~30 LOC, and avoiding the dependency
// keeps the binary smaller and the behaviour easier to audit given the
// security-sensitive nature of the component.
package fetcher

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/adapters/egress"
	"github.com/staticvar/fetchmark/internal/adapters/robots"
	"github.com/staticvar/fetchmark/internal/obs"
)

// Budgets bound every outbound fetch. All fields must be > 0 except
// AllowedMIME which may be nil (meaning "accept anything"). MIME matching
// is prefix-based on the detected type, case-insensitive.
type Budgets struct {
	MaxBodyBytes           int64
	MaxDecompressedBytes   int64
	MaxResponseHeaderBytes int64
	MaxRedirects           int
	HeaderTimeout          time.Duration
	FetchTimeout           time.Duration
	PerHostConcurrency     int
	GlobalConcurrency      int
	Retries                int
	AllowedMIME            []string
}

// Request describes a single fetch. ProxyURL/UserAgent are admin-only in
// the HTTP layer; at this layer we accept them unconditionally.
type Request struct {
	URL                  string
	ProxyURL             string
	UserAgent            string
	RespectRobots        bool
	Timeout              time.Duration
	MaxBodyBytes         int64
	MaxDecompressedBytes int64
	MaxTotalBytes        int64
	// FocusedRedirectScope is reserved for the focused-ingestion control plane.
	// Every redirect must retain the original scheme and authority and satisfy
	// this explicit path policy. Ambiguous paths are rejected before a request.
	FocusedRedirectScope *RedirectScope
	// Conditional validators are raw origin-provided values. They are never
	// forwarded to a different origin across redirects.
	IfNoneMatch     string
	IfModifiedSince string
}

// RedirectScope is a declarative, deny-first path boundary. Path prefixes use
// URL escaped-path form, matching the focused crawler configuration.
type RedirectScope struct {
	AllowedPathPrefixes []string
	DeniedPathPrefixes  []string
	DeniedURLs          []string
}

// Result is the fetcher's output. A non-empty Unsupported or non-nil Err
// both indicate the body is not usable; callers should branch on Err
// first.
type Result struct {
	URL                 string
	FinalURL            string
	Status              int
	ContentType         string
	XRobotsTag          []string
	ETag                string
	LastModified        string
	NotModified         bool
	RobotsAllowed       bool
	RobotsAuthoritative bool
	ObservedAt          time.Time
	Body                []byte
	FromCache           bool
	FetchMS             int64
	BytesRead           int64
	UAUsed              string
	ProxyUsed           string
	Unsupported         string
	Err                 error
}

// Unsupported reason constants — stable, used in metric labels.
const (
	ReasonRobots          = "robots_disallowed"
	ReasonNonHTML         = "non_html"
	ReasonTooLarge        = "too_large"
	ReasonDecompressLarge = "decompressed_too_large"
	ReasonEgress          = "egress_blocked"
	ReasonRequestBudget   = "request_byte_budget"
	ReasonRedirectScope   = "redirect_scope"

	maxHostGates                  = 1024
	maxProxyClients               = 32
	defaultMaxResponseHeaderBytes = 64 << 10
)

var (
	errRedirectRobots = errors.New("fetcher: redirect target disallowed by robots.txt")
	errRedirectScope  = errors.New("fetcher: redirect target outside focused scope")
)

// Fetcher is the concurrency-bounded HTTP worker pool. Safe for
// concurrent use.
type Fetcher struct {
	policy     egress.Policy
	budgets    Budgets
	robots     *robots.Checker
	defaultUA  string
	userAgents []string // pool (when robots off and pool provided)
	respectRbt bool

	clientMu sync.Mutex
	clients  map[string]*http.Client // keyed by proxy URL, "" = default
	proxies  []string

	globalSem chan struct{}

	hostsMu          sync.Mutex
	hosts            map[string]*hostGate
	overflowHostGate chan struct{}
}

// Options collects construction-time inputs. policy, budgets, robots and
// defaultUA are required.
type Options struct {
	Policy        egress.Policy
	Budgets       Budgets
	Robots        *robots.Checker
	DefaultUA     string
	UserAgentPool []string
	RespectRobots bool
}

// New builds a Fetcher.
func New(o Options) (*Fetcher, error) {
	if o.Budgets.MaxBodyBytes <= 0 || o.Budgets.MaxDecompressedBytes <= 0 {
		return nil, errors.New("fetcher: byte budgets must be > 0")
	}
	if o.Budgets.MaxResponseHeaderBytes <= 0 {
		o.Budgets.MaxResponseHeaderBytes = defaultMaxResponseHeaderBytes
	}
	if o.Budgets.PerHostConcurrency <= 0 {
		o.Budgets.PerHostConcurrency = 2
	}
	if o.Budgets.GlobalConcurrency <= 0 {
		o.Budgets.GlobalConcurrency = 10
	}
	if o.Budgets.FetchTimeout <= 0 {
		o.Budgets.FetchTimeout = 8 * time.Second
	}
	if o.Budgets.HeaderTimeout <= 0 {
		o.Budgets.HeaderTimeout = 5 * time.Second
	}
	if o.DefaultUA == "" {
		return nil, errors.New("fetcher: DefaultUA required")
	}
	return &Fetcher{
		policy:           o.Policy,
		budgets:          o.Budgets,
		robots:           o.Robots,
		defaultUA:        o.DefaultUA,
		userAgents:       o.UserAgentPool,
		respectRbt:       o.RespectRobots,
		clients:          map[string]*http.Client{},
		globalSem:        make(chan struct{}, o.Budgets.GlobalConcurrency),
		hosts:            map[string]*hostGate{},
		overflowHostGate: make(chan struct{}, o.Budgets.PerHostConcurrency),
	}, nil
}

// FetchMany runs requests through a fixed-size worker pool sized by
// Budgets.GlobalConcurrency. Ordering of the returned slice matches reqs.
// Unlike a goroutine-per-URL approach this bounds memory and scheduler
// pressure even when the caller asks for a large batch.
func (f *Fetcher) FetchMany(ctx context.Context, reqs []Request) []Result {
	out := make([]Result, len(reqs))
	if len(reqs) == 0 {
		return out
	}
	workers := cap(f.globalSem)
	if workers <= 0 {
		workers = 8
	}
	if workers > len(reqs) {
		workers = len(reqs)
	}
	type job struct {
		i int
		r Request
	}
	jobs := make(chan job)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for j := range jobs {
				out[j.i] = f.Fetch(ctx, j.r)
			}
		}()
	}
	for i, r := range reqs {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return out
		case jobs <- job{i: i, r: r}:
		}
	}
	close(jobs)
	wg.Wait()
	return out
}

// Fetch performs a single request through all gates.
func (f *Fetcher) Fetch(ctx context.Context, r Request) Result {
	start := time.Now()
	res := Result{URL: r.URL, ObservedAt: start.UTC()}

	u, err := url.Parse(r.URL)
	if err != nil {
		res.Err = fmt.Errorf("parse url: %w", err)
		return res
	}
	var redirectScope *focusedRedirectScope
	if r.FocusedRedirectScope != nil {
		redirectScope, err = newFocusedRedirectScope(u, *r.FocusedRedirectScope)
		if err != nil {
			res.Unsupported = ReasonRedirectScope
			return res
		}
	}
	if err := f.policy.Validate(ctx, r.URL); err != nil {
		res.Unsupported = ReasonEgress
		res.Err = err
		reason := "validate"
		var eErr *egress.Error
		if errors.As(err, &eErr) {
			reason = eErr.Reason
		}
		obs.EgressRejects.WithLabelValues(reason).Inc()
		return res
	}

	// Global gate bounds complete fetch work. Per-host gates are acquired for
	// one HTTP hop at a time below so reciprocal redirects cannot lock-invert.
	select {
	case f.globalSem <- struct{}{}:
	case <-ctx.Done():
		res.Err = ctx.Err()
		return res
	}
	defer func() { <-f.globalSem }()

	ua := f.chooseUA(r.UserAgent, r.RespectRobots)
	res.UAUsed = ua

	if f.respectRbt && r.RespectRobots && f.robots != nil {
		releaseRobotsGate, err := f.acquireHostGate(ctx, u.Hostname())
		if err != nil {
			res.Err = err
			return res
		}
		decision := f.robots.Evaluate(ctx, ua, r.URL)
		releaseRobotsGate()
		res.RobotsAllowed = decision.Allowed
		res.RobotsAuthoritative = decision.Authoritative
		if !decision.Allowed {
			res.Unsupported = ReasonRobots
			res.FetchMS = time.Since(start).Milliseconds()
			obs.RobotsBlocks.Inc()
			return res
		}
	}

	client, err := f.clientFor(r.ProxyURL)
	if err != nil {
		res.Err = err
		return res
	}
	res.ProxyUsed = r.ProxyURL

	timeout := r.Timeout
	if timeout <= 0 {
		timeout = f.budgets.FetchTimeout
	}
	fctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	response, bytesRead := f.doWithRetry(
		fctx, client, r.URL, ua, r.IfNoneMatch, r.IfModifiedSince,
		r.MaxBodyBytes, r.MaxDecompressedBytes, r.MaxTotalBytes, f.respectRbt && r.RespectRobots, redirectScope,
	)
	res.Status = response.status
	res.FinalURL = response.finalURL
	res.ContentType = response.contentType
	res.XRobotsTag = response.xRobotsTag
	res.ETag = response.etag
	res.LastModified = response.lastModified
	res.NotModified = response.notModified
	res.Unsupported = response.reason
	res.Err = response.err
	res.Body = response.body
	res.BytesRead = bytesRead
	res.FetchMS = time.Since(start).Milliseconds()
	return res
}

type responseData struct {
	body         []byte
	status       int
	finalURL     string
	contentType  string
	xRobotsTag   []string
	etag         string
	lastModified string
	notModified  bool
	reason       string
	err          error
}

func (f *Fetcher) doWithRetry(ctx context.Context, client *http.Client, rawURL, ua, ifNoneMatch, ifModifiedSince string, maxBodyBytes, maxDecompressedBytes, maxTotalBytes int64, respectRobots bool, redirectScope *focusedRedirectScope) (responseData, int64) {
	var last responseData
	var bytesRead int64
	retries := f.budgets.Retries
	if retries < 0 {
		retries = 0
	}
	for attempt := 0; attempt <= retries; attempt++ {
		if maxTotalBytes > 0 && bytesRead >= maxTotalBytes {
			last.body = nil
			last.reason = ReasonRequestBudget
			last.err = nil
			return last, bytesRead
		}
		if attempt > 0 {
			backoff := time.Duration(1<<attempt)*200*time.Millisecond +
				time.Duration(rand.Intn(100))*time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				last.body = nil
				last.err = ctx.Err()
				return last, bytesRead
			}
		}
		current := f.doOnce(ctx, client, rawURL, ua, ifNoneMatch, ifModifiedSince, maxBodyBytes, maxDecompressedBytes, maxTotalBytes, respectRobots, redirectScope, &bytesRead)
		if current.notModified {
			return current, bytesRead
		}
		if current.err == nil && current.reason == "" && current.status >= 200 && current.status < 300 {
			return current, bytesRead
		}
		retryableStatus := isRetryableStatus(current.status)
		if current.reason != "" && !retryableStatus {
			current.body = nil
			return current, bytesRead
		}
		// Only retry on transient conditions (5xx, 429, network).
		if current.err == nil && !retryableStatus {
			return current, bytesRead
		}
		last = current
	}
	if last.err == nil && last.status != 0 {
		// Retries exhausted on 5xx/429 but no transport error. Surface as
		// a terminal error so the pipeline labels it fetch_failed rather
		// than silently falling through to a non-2xx "success".
		last.err = fmt.Errorf("upstream returned %d after %d retries", last.status, retries)
	}
	last.body = nil
	return last, bytesRead
}

func isRetryableStatus(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500
}

func (f *Fetcher) doOnce(ctx context.Context, client *http.Client, rawURL, ua, ifNoneMatch, ifModifiedSince string, requestMaxBody, requestMaxDecompressed, requestMaxTotal int64, respectRobots bool, redirectScope *focusedRedirectScope, bytesRead *int64) responseData {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return responseData{err: err}
	}
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,*/*;q=0.8")
	req.Header.Set("Accept-Encoding", "gzip")
	if ifNoneMatch != "" {
		req.Header.Set("If-None-Match", ifNoneMatch)
	}
	if ifModifiedSince != "" {
		req.Header.Set("If-Modified-Since", ifModifiedSince)
	}

	requestClient := *client
	previousCheckRedirect := client.CheckRedirect
	currentHost := strings.ToLower(req.URL.Hostname())
	currentRelease, err := f.acquireHostGate(ctx, currentHost)
	if err != nil {
		return responseData{err: err}
	}
	defer func() {
		if currentRelease != nil {
			currentRelease()
		}
	}()
	requestClient.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		if len(via) > 0 {
			next.Header.Del("If-None-Match")
			next.Header.Del("If-Modified-Since")
		}
		if redirectScope != nil && !redirectScope.allows(next.URL) {
			return errRedirectScope
		}
		if previousCheckRedirect != nil {
			if err := previousCheckRedirect(next, via); err != nil {
				return err
			}
		}
		host := strings.ToLower(next.URL.Hostname())
		if host != currentHost {
			currentRelease()
			currentRelease = nil
			release, err := f.acquireHostGate(next.Context(), host)
			if err != nil {
				return err
			}
			currentHost = host
			currentRelease = release
		}
		if respectRobots && f.robots != nil {
			decision := f.robots.Evaluate(next.Context(), ua, next.URL.String())
			if !decision.Allowed {
				return errRedirectRobots
			}
		}
		return nil
	}
	resp, err := requestClient.Do(req)
	if err != nil {
		if errors.Is(err, errRedirectScope) {
			return responseData{reason: ReasonRedirectScope}
		}
		if errors.Is(err, errRedirectRobots) {
			obs.RobotsBlocks.Inc()
			return responseData{reason: ReasonRobots}
		}
		return responseData{err: err}
	}
	defer resp.Body.Close()
	response := responseData{
		status: resp.StatusCode, finalURL: resp.Request.URL.String(), contentType: resp.Header.Get("Content-Type"),
		xRobotsTag: append([]string(nil), resp.Header.Values("X-Robots-Tag")...),
		etag:       resp.Header.Get("ETag"), lastModified: resp.Header.Get("Last-Modified"),
	}
	if resp.StatusCode == http.StatusNotModified {
		if (ifNoneMatch == "" && ifModifiedSince == "") || !sameOrigin(req.URL, resp.Request.URL) || req.URL.String() != resp.Request.URL.String() {
			response.err = errors.New("fetcher: unexpected 304 without a matching conditional representation")
			return response
		}
		response.notModified = true
		return response
	}

	maxBody := f.budgets.MaxBodyBytes
	if requestMaxBody > 0 && requestMaxBody < maxBody {
		maxBody = requestMaxBody
	}
	maxDecompressed := f.budgets.MaxDecompressedBytes
	if requestMaxDecompressed > 0 && requestMaxDecompressed < maxDecompressed {
		maxDecompressed = requestMaxDecompressed
	}
	bodyLimitedByRequest := false
	if requestMaxTotal > 0 {
		remaining := requestMaxTotal - *bytesRead
		if remaining <= 0 {
			response.reason = ReasonRequestBudget
			return response
		}
		if remaining < maxBody {
			maxBody = remaining
			bodyLimitedByRequest = true
		}
	}
	// Enforce max compressed body size via LimitReader before decompression.
	limited := io.LimitReader(resp.Body, maxBody+1)
	raw, err := io.ReadAll(limited)
	consumedRaw := minInt64(int64(len(raw)), maxBody)
	*bytesRead += consumedRaw
	if err != nil {
		response.err = err
		return response
	}
	if int64(len(raw)) > maxBody {
		if bodyLimitedByRequest {
			response.reason = ReasonRequestBudget
			return response
		}
		response.reason = ReasonTooLarge
		return response
	}

	// Decompress if gzipped, bounded.
	body := raw
	if strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		gr, gerr := gzip.NewReader(bytes.NewReader(raw))
		if gerr != nil {
			response.err = gerr
			return response
		}
		defer gr.Close()
		decompressedLimitedByRequest := false
		if requestMaxTotal > 0 {
			remaining := requestMaxTotal - *bytesRead
			if remaining <= 0 {
				response.reason = ReasonRequestBudget
				return response
			}
			if remaining < maxDecompressed {
				maxDecompressed = remaining
				decompressedLimitedByRequest = true
			}
		}
		decompressed, derr := io.ReadAll(io.LimitReader(gr, maxDecompressed+1))
		*bytesRead += minInt64(int64(len(decompressed)), maxDecompressed)
		if derr != nil {
			response.err = derr
			return response
		}
		if int64(len(decompressed)) > maxDecompressed {
			if decompressedLimitedByRequest {
				response.reason = ReasonRequestBudget
				return response
			}
			response.reason = ReasonDecompressLarge
			return response
		}
		body = decompressed
	}

	// Sniff on the body bytes (first 512). Trust sniff over header —
	// hostile servers lie.
	sniffed := http.DetectContentType(body)
	final := pickCT(resp.Header.Get("Content-Type"), sniffed)

	if !f.mimeAllowed(sniffed) {
		response.contentType = final
		response.reason = ReasonNonHTML
		return response
	}
	response.body = body
	response.contentType = final
	return response
}

func sameOrigin(left, right *url.URL) bool {
	if left == nil || right == nil {
		return false
	}
	return strings.EqualFold(left.Scheme, right.Scheme) && strings.EqualFold(left.Host, right.Host)
}

type focusedRedirectScope struct {
	scheme          string
	hostname        string
	port            string
	allowedPrefixes []string
	deniedPrefixes  []string
	deniedURLs      map[string]struct{}
}

func newFocusedRedirectScope(original *url.URL, policy RedirectScope) (*focusedRedirectScope, error) {
	if original == nil || original.User != nil || original.Opaque != "" || original.Hostname() == "" ||
		(!strings.EqualFold(original.Scheme, "http") && !strings.EqualFold(original.Scheme, "https")) ||
		len(policy.AllowedPathPrefixes) == 0 {
		return nil, errRedirectScope
	}
	escapedPath, ok := unambiguousRedirectPath(original)
	if !ok {
		return nil, errRedirectScope
	}
	scope := &focusedRedirectScope{
		scheme: strings.ToLower(original.Scheme), hostname: strings.ToLower(original.Hostname()),
		port: normalizedURLPort(original), allowedPrefixes: append([]string(nil), policy.AllowedPathPrefixes...),
		deniedPrefixes: append([]string(nil), policy.DeniedPathPrefixes...), deniedURLs: make(map[string]struct{}, len(policy.DeniedURLs)),
	}
	for index, prefix := range scope.allowedPrefixes {
		normalized, ok := unambiguousPolicyPrefix(prefix)
		if !ok {
			return nil, errRedirectScope
		}
		scope.allowedPrefixes[index] = normalized
	}
	for index, prefix := range scope.deniedPrefixes {
		normalized, ok := unambiguousPolicyPrefix(prefix)
		if !ok {
			return nil, errRedirectScope
		}
		scope.deniedPrefixes[index] = normalized
	}
	for _, raw := range policy.DeniedURLs {
		canonical, err := canonicalFocusedURL(raw)
		if err != nil {
			return nil, errRedirectScope
		}
		scope.deniedURLs[canonical] = struct{}{}
	}
	if !scope.allowsPath(original, escapedPath) {
		return nil, errRedirectScope
	}
	return scope, nil
}

func (scope *focusedRedirectScope) allows(candidate *url.URL) bool {
	if scope == nil || candidate == nil || candidate.User != nil || candidate.Opaque != "" || strings.ToLower(candidate.Scheme) != scope.scheme ||
		!strings.EqualFold(candidate.Hostname(), scope.hostname) || normalizedURLPort(candidate) != scope.port {
		return false
	}
	escapedPath, ok := unambiguousRedirectPath(candidate)
	return ok && scope.allowsPath(candidate, escapedPath)
}

func (scope *focusedRedirectScope) allowsPath(candidate *url.URL, escapedPath string) bool {
	canonical, err := canonicalFocusedURL(candidate.String())
	if err != nil {
		return false
	}
	if _, denied := scope.deniedURLs[canonical]; denied {
		return false
	}
	for _, prefix := range scope.deniedPrefixes {
		if strings.HasPrefix(escapedPath, prefix) {
			return false
		}
	}
	for _, prefix := range scope.allowedPrefixes {
		if strings.HasPrefix(escapedPath, prefix) {
			return true
		}
	}
	return false
}

func unambiguousRedirectPath(candidate *url.URL) (string, bool) {
	if candidate == nil {
		return "", false
	}
	escapedPath := candidate.EscapedPath()
	if escapedPath == "" {
		escapedPath = "/"
	}
	escaped := strings.ToLower(escapedPath)
	if strings.Contains(escaped, "%2f") || strings.Contains(escaped, "%5c") || strings.Contains(candidate.Path, `\`) {
		return "", false
	}
	pathValue := candidate.Path
	if pathValue == "" {
		pathValue = "/"
	}
	if !strings.HasPrefix(pathValue, "/") {
		return "", false
	}
	if !utf8.ValidString(pathValue) {
		return "", false
	}
	for _, segment := range strings.Split(pathValue, "/") {
		if segment == "." || segment == ".." {
			return "", false
		}
	}
	return pathValue, true
}

func unambiguousPolicyPrefix(prefix string) (string, bool) {
	if prefix == "" || !strings.HasPrefix(prefix, "/") || strings.ContainsAny(prefix, "?#\\%") {
		return "", false
	}
	parsed, err := url.Parse("https://scope.invalid" + prefix)
	if err != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", false
	}
	pathValue, ok := unambiguousRedirectPath(parsed)
	if !ok {
		return "", false
	}
	return pathValue, true
}

func canonicalFocusedURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed == nil || parsed.User != nil || parsed.Opaque != "" || parsed.Hostname() == "" ||
		(!strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https")) {
		return "", errRedirectScope
	}
	pathValue, ok := unambiguousRedirectPath(parsed)
	if !ok {
		return "", errRedirectScope
	}
	copy := *parsed
	copy.Path = pathValue
	copy.RawPath = ""
	copy.Fragment = ""
	return cache.CanonicalURL(copy.String())
}

func normalizedURLPort(candidate *url.URL) string {
	if candidate == nil {
		return ""
	}
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

func (f *Fetcher) mimeAllowed(detected string) bool {
	if len(f.budgets.AllowedMIME) == 0 {
		return true
	}
	detected = strings.ToLower(strings.TrimSpace(strings.SplitN(detected, ";", 2)[0]))
	for _, a := range f.budgets.AllowedMIME {
		if strings.EqualFold(strings.TrimSpace(a), detected) {
			return true
		}
	}
	return false
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func pickCT(header, sniffed string) string {
	if header != "" {
		return header
	}
	return sniffed
}

func (f *Fetcher) chooseUA(override string, respectRobots bool) string {
	if override != "" {
		return override
	}
	// When robots is respected, use a stable declared UA so site owners
	// can pattern-match reliably. The UA pool is only engaged when the
	// operator has explicitly turned robots off.
	if respectRobots || f.respectRbt || len(f.userAgents) == 0 {
		return f.defaultUA
	}
	return f.userAgents[rand.Intn(len(f.userAgents))]
}

type hostGate struct {
	sem    chan struct{}
	active int
}

func (f *Fetcher) acquireHostGate(ctx context.Context, host string) (func(), error) {
	g, overflow := f.reserveHostGate(host)
	select {
	case g <- struct{}{}:
		return func() {
			<-g
			if !overflow {
				f.releaseHostGate(host)
			}
		}, nil
	case <-ctx.Done():
		if !overflow {
			f.releaseHostGate(host)
		}
		return nil, ctx.Err()
	}
}

func (f *Fetcher) reserveHostGate(host string) (chan struct{}, bool) {
	f.hostsMu.Lock()
	defer f.hostsMu.Unlock()
	if g, ok := f.hosts[host]; ok {
		g.active++
		return g.sem, false
	}
	if len(f.hosts) >= maxHostGates {
		for h, g := range f.hosts {
			if g.active == 0 && len(g.sem) == 0 {
				delete(f.hosts, h)
				break
			}
		}
	}
	if len(f.hosts) >= maxHostGates {
		return f.overflowHostGate, true
	}
	g := &hostGate{sem: make(chan struct{}, f.budgets.PerHostConcurrency), active: 1}
	f.hosts[host] = g
	return g.sem, false
}

func (f *Fetcher) releaseHostGate(host string) {
	f.hostsMu.Lock()
	defer f.hostsMu.Unlock()
	if g, ok := f.hosts[host]; ok && g.active > 0 {
		g.active--
	}
}

func (f *Fetcher) gateFor(host string) chan struct{} {
	f.hostsMu.Lock()
	defer f.hostsMu.Unlock()
	if g, ok := f.hosts[host]; ok {
		return g.sem
	}
	if len(f.hosts) >= maxHostGates {
		for h, g := range f.hosts {
			if g.active == 0 && len(g.sem) == 0 {
				delete(f.hosts, h)
				break
			}
		}
	}
	if len(f.hosts) >= maxHostGates {
		return f.overflowHostGate
	}
	g := &hostGate{sem: make(chan struct{}, f.budgets.PerHostConcurrency)}
	f.hosts[host] = g
	return g.sem
}

func (f *Fetcher) hostGateCount() int {
	f.hostsMu.Lock()
	defer f.hostsMu.Unlock()
	return len(f.hosts)
}

// clientFor returns (and memoizes) an *http.Client subject to f.policy,
// optionally with a proxy installed. Empty proxy URL returns the default
// client.
func (f *Fetcher) clientFor(proxyURL string) (*http.Client, error) {
	f.clientMu.Lock()
	defer f.clientMu.Unlock()
	if c, ok := f.clients[proxyURL]; ok {
		return c, nil
	}
	client := f.policy.HTTPClient(0)
	if transport, ok := client.Transport.(*http.Transport); ok {
		transport.MaxResponseHeaderBytes = f.budgets.MaxResponseHeaderBytes
	}
	if proxyURL != "" {
		pu, err := url.Parse(proxyURL)
		if err != nil {
			return nil, fmt.Errorf("fetcher: parse proxy url: %w", err)
		}
		// Validate proxy URL itself against policy so it can't be
		// abused to bypass SSRF rules.
		if err := f.policy.Validate(context.Background(), proxyURL); err != nil {
			return nil, err
		}
		tr := f.policy.Transport()
		tr.MaxResponseHeaderBytes = f.budgets.MaxResponseHeaderBytes
		tr.Proxy = http.ProxyURL(pu)
		client.Transport = tr
	}
	if proxyURL != "" {
		f.rememberProxyClient(proxyURL)
	}
	f.clients[proxyURL] = client
	return client, nil
}

func (f *Fetcher) rememberProxyClient(proxyURL string) {
	if len(f.proxies) >= maxProxyClients {
		evict := f.proxies[0]
		copy(f.proxies, f.proxies[1:])
		f.proxies = f.proxies[:len(f.proxies)-1]
		delete(f.clients, evict)
	}
	f.proxies = append(f.proxies, proxyURL)
}
