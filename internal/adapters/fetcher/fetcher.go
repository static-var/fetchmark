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
	URL            string
	ProxyURL       string
	UserAgent      string
	RespectRobots  bool
	Timeout        time.Duration
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
)

// Fetcher is the concurrency-bounded HTTP worker pool. Safe for
// concurrent use.
type Fetcher struct {
	policy      egress.Policy
	budgets     Budgets
	robots      *robots.Checker
	defaultUA   string
	userAgents  []string // pool (when robots off and pool provided)
	respectRbt  bool

	clientMu sync.Mutex
	clients  map[string]*http.Client // keyed by proxy URL, "" = default

	globalSem chan struct{}

	hostsMu sync.Mutex
	hosts   map[string]chan struct{}
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
		policy:     o.Policy,
		budgets:    o.Budgets,
		robots:     o.Robots,
		defaultUA:  o.DefaultUA,
		userAgents: o.UserAgentPool,
		respectRbt: o.RespectRobots,
		clients:    map[string]*http.Client{},
		globalSem:  make(chan struct{}, o.Budgets.GlobalConcurrency),
		hosts:      map[string]chan struct{}{},
	}, nil
}

// FetchMany runs all requests in parallel, respecting global and
// per-host concurrency caps. Ordering of the returned slice matches reqs.
func (f *Fetcher) FetchMany(ctx context.Context, reqs []Request) []Result {
	out := make([]Result, len(reqs))
	var wg sync.WaitGroup
	for i, r := range reqs {
		wg.Add(1)
		go func(i int, r Request) {
			defer wg.Done()
			out[i] = f.Fetch(ctx, r)
		}(i, r)
	}
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

	hostGate := f.gateFor(u.Hostname())
	select {
	case hostGate <- struct{}{}:
	case <-ctx.Done():
		res.Err = ctx.Err()
		return res
	}
	defer func() { <-hostGate }()

	ua := f.chooseUA(r.UserAgent)
	res.UAUsed = ua

	if f.respectRbt && r.RespectRobots && f.robots != nil {
		allowed, _ := f.robots.Allowed(ctx, ua, r.URL)
		if !allowed {
			res.Unsupported = ReasonRobots
			res.FetchMS = time.Since(start).Milliseconds()
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

	body, status, ctype, reason, err := f.doWithRetry(fctx, client, r.URL, ua)
	res.Status = status
	res.ContentType = ctype
	res.Unsupported = reason
	res.Err = err
	res.Body = body
	res.FetchMS = time.Since(start).Milliseconds()
	return res
}

func (f *Fetcher) doWithRetry(ctx context.Context, client *http.Client, rawURL, ua string) ([]byte, int, string, string, error) {
	var lastErr error
	retries := f.budgets.Retries
	if retries < 0 {
		retries = 0
	}
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<attempt)*200*time.Millisecond +
				time.Duration(rand.Intn(100))*time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, 0, "", "", ctx.Err()
			}
		}
		body, status, ctype, reason, err := f.doOnce(ctx, client, rawURL, ua)
		if err == nil && reason == "" && status >= 200 && status < 300 {
			return body, status, ctype, "", nil
		}
		if reason != "" {
			return nil, status, ctype, reason, nil
		}
		// Only retry on transient conditions (5xx, 429, network).
		if err == nil && !(status == 429 || status >= 500) {
			return body, status, ctype, "", nil
		}
		lastErr = err
	}
	return nil, 0, "", "", lastErr
}

func (f *Fetcher) doOnce(ctx context.Context, client *http.Client, rawURL, ua string) ([]byte, int, string, string, error) {
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

	// Enforce max compressed body size via LimitReader before decompression.
	limited := io.LimitReader(resp.Body, f.budgets.MaxBodyBytes+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return nil, resp.StatusCode, resp.Header.Get("Content-Type"), "", err
	}
	if int64(len(raw)) > f.budgets.MaxBodyBytes {
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
		decompressed, derr := io.ReadAll(io.LimitReader(gr, f.budgets.MaxDecompressedBytes+1))
		if derr != nil {
			return nil, resp.StatusCode, resp.Header.Get("Content-Type"), "", derr
		}
		if int64(len(decompressed)) > f.budgets.MaxDecompressedBytes {
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

func pickCT(header, sniffed string) string {
	if header != "" {
		return header
	}
	return sniffed
}

func (f *Fetcher) chooseUA(override string) string {
	if override != "" {
		return override
	}
	// When robots is respected, use a stable declared UA so site owners
	// can pattern-match reliably. The UA pool is only engaged when the
	// operator has explicitly turned robots off.
	if f.respectRbt || len(f.userAgents) == 0 {
		return f.defaultUA
	}
	return f.userAgents[rand.Intn(len(f.userAgents))]
}

func (f *Fetcher) gateFor(host string) chan struct{} {
	f.hostsMu.Lock()
	defer f.hostsMu.Unlock()
	g, ok := f.hosts[host]
	if !ok {
		g = make(chan struct{}, f.budgets.PerHostConcurrency)
		f.hosts[host] = g
	}
	return g
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
	client := f.policy.HTTPClient(f.budgets.FetchTimeout)
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
	f.clients[proxyURL] = client
	return client, nil
}
