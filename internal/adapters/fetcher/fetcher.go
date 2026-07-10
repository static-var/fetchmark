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

	"github.com/staticvar/fetchmark/internal/adapters/egress"
	"github.com/staticvar/fetchmark/internal/adapters/robots"
	"github.com/staticvar/fetchmark/internal/obs"
)

// Budgets bound every outbound fetch. All fields must be > 0 except
// AllowedMIME which may be nil (meaning "accept anything"). MIME matching
// is prefix-based on the detected type, case-insensitive.
type Budgets struct {
	MaxBodyBytes         int64
	MaxDecompressedBytes int64
	MaxRedirects         int
	HeaderTimeout        time.Duration
	FetchTimeout         time.Duration
	PerHostConcurrency   int
	GlobalConcurrency    int
	Retries              int
	AllowedMIME          []string
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
}

// Result is the fetcher's output. A non-empty Unsupported or non-nil Err
// both indicate the body is not usable; callers should branch on Err
// first.
type Result struct {
	URL         string
	Status      int
	ContentType string
	Body        []byte
	FromCache   bool
	FetchMS     int64
	BytesRead   int64
	UAUsed      string
	ProxyUsed   string
	Unsupported string
	Err         error
}

// Unsupported reason constants — stable, used in metric labels.
const (
	ReasonRobots          = "robots_disallowed"
	ReasonNonHTML         = "non_html"
	ReasonTooLarge        = "too_large"
	ReasonDecompressLarge = "decompressed_too_large"
	ReasonEgress          = "egress_blocked"
	ReasonRequestBudget   = "request_byte_budget"

	maxHostGates    = 1024
	maxProxyClients = 32
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
	res := Result{URL: r.URL}

	u, err := url.Parse(r.URL)
	if err != nil {
		res.Err = fmt.Errorf("parse url: %w", err)
		return res
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

	// Global + per-host gates.
	select {
	case f.globalSem <- struct{}{}:
	case <-ctx.Done():
		res.Err = ctx.Err()
		return res
	}
	defer func() { <-f.globalSem }()

	releaseHostGate, err := f.acquireHostGate(ctx, u.Hostname())
	if err != nil {
		res.Err = err
		return res
	}
	defer releaseHostGate()

	ua := f.chooseUA(r.UserAgent, r.RespectRobots)
	res.UAUsed = ua

	if f.respectRbt && r.RespectRobots && f.robots != nil {
		allowed, _ := f.robots.Allowed(ctx, ua, r.URL)
		if !allowed {
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

	body, status, ctype, reason, bytesRead, err := f.doWithRetry(fctx, client, r.URL, ua, r.MaxBodyBytes, r.MaxDecompressedBytes, r.MaxTotalBytes)
	res.Status = status
	res.ContentType = ctype
	res.Unsupported = reason
	res.Err = err
	res.Body = body
	res.BytesRead = bytesRead
	res.FetchMS = time.Since(start).Milliseconds()
	return res
}

func (f *Fetcher) doWithRetry(ctx context.Context, client *http.Client, rawURL, ua string, maxBodyBytes, maxDecompressedBytes, maxTotalBytes int64) ([]byte, int, string, string, int64, error) {
	var lastErr error
	var lastStatus int
	var lastCtype string
	var bytesRead int64
	retries := f.budgets.Retries
	if retries < 0 {
		retries = 0
	}
	for attempt := 0; attempt <= retries; attempt++ {
		if maxTotalBytes > 0 && bytesRead >= maxTotalBytes {
			return nil, lastStatus, lastCtype, ReasonRequestBudget, bytesRead, nil
		}
		if attempt > 0 {
			backoff := time.Duration(1<<attempt)*200*time.Millisecond +
				time.Duration(rand.Intn(100))*time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, lastStatus, lastCtype, "", bytesRead, ctx.Err()
			}
		}
		body, status, ctype, reason, err := f.doOnce(ctx, client, rawURL, ua, maxBodyBytes, maxDecompressedBytes, maxTotalBytes, &bytesRead)
		if err == nil && reason == "" && status >= 200 && status < 300 {
			return body, status, ctype, "", bytesRead, nil
		}
		retryableStatus := isRetryableStatus(status)
		if reason != "" && !retryableStatus {
			return nil, status, ctype, reason, bytesRead, nil
		}
		// Only retry on transient conditions (5xx, 429, network).
		if err == nil && !retryableStatus {
			return body, status, ctype, "", bytesRead, nil
		}
		lastErr = err
		lastStatus = status
		lastCtype = ctype
	}
	if lastErr == nil && lastStatus != 0 {
		// Retries exhausted on 5xx/429 but no transport error. Surface as
		// a terminal error so the pipeline labels it fetch_failed rather
		// than silently falling through to a non-2xx "success".
		lastErr = fmt.Errorf("upstream returned %d after %d retries", lastStatus, retries)
	}
	return nil, lastStatus, lastCtype, "", bytesRead, lastErr
}

func isRetryableStatus(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500
}

func (f *Fetcher) doOnce(ctx context.Context, client *http.Client, rawURL, ua string, requestMaxBody, requestMaxDecompressed, requestMaxTotal int64, bytesRead *int64) ([]byte, int, string, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, 0, "", "", err
	}
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,*/*;q=0.8")
	req.Header.Set("Accept-Encoding", "gzip")

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, "", "", err
	}
	defer resp.Body.Close()

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
			return nil, resp.StatusCode, resp.Header.Get("Content-Type"), ReasonRequestBudget, nil
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
		return nil, resp.StatusCode, resp.Header.Get("Content-Type"), "", err
	}
	if int64(len(raw)) > maxBody {
		if bodyLimitedByRequest {
			return nil, resp.StatusCode, resp.Header.Get("Content-Type"), ReasonRequestBudget, nil
		}
		return nil, resp.StatusCode, resp.Header.Get("Content-Type"), ReasonTooLarge, nil
	}

	// Decompress if gzipped, bounded.
	body := raw
	if strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		gr, gerr := gzip.NewReader(bytes.NewReader(raw))
		if gerr != nil {
			return nil, resp.StatusCode, resp.Header.Get("Content-Type"), "", gerr
		}
		defer gr.Close()
		decompressedLimitedByRequest := false
		if requestMaxTotal > 0 {
			remaining := requestMaxTotal - *bytesRead
			if remaining <= 0 {
				return nil, resp.StatusCode, resp.Header.Get("Content-Type"), ReasonRequestBudget, nil
			}
			if remaining < maxDecompressed {
				maxDecompressed = remaining
				decompressedLimitedByRequest = true
			}
		}
		decompressed, derr := io.ReadAll(io.LimitReader(gr, maxDecompressed+1))
		*bytesRead += minInt64(int64(len(decompressed)), maxDecompressed)
		if derr != nil {
			return nil, resp.StatusCode, resp.Header.Get("Content-Type"), "", derr
		}
		if int64(len(decompressed)) > maxDecompressed {
			if decompressedLimitedByRequest {
				return nil, resp.StatusCode, resp.Header.Get("Content-Type"), ReasonRequestBudget, nil
			}
			return nil, resp.StatusCode, resp.Header.Get("Content-Type"), ReasonDecompressLarge, nil
		}
		body = decompressed
	}

	// Sniff on the body bytes (first 512). Trust sniff over header —
	// hostile servers lie.
	sniffed := http.DetectContentType(body)
	final := pickCT(resp.Header.Get("Content-Type"), sniffed)

	if !f.mimeAllowed(sniffed) {
		return nil, resp.StatusCode, final, ReasonNonHTML, nil
	}
	return body, resp.StatusCode, final, "", nil
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
