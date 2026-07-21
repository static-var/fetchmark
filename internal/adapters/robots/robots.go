// Package robots provides a cached robots.txt policy checker.
//
// The checker fetches /robots.txt per host (via the caller-supplied
// http.Client, which MUST already be subject to an egress policy), parses it
// with Fetchmark's bounded RFC 9309 matcher, and caches the result with a TTL.
// Network/5xx failures fail closed for the current request and are not cached,
// matching RFC 9309's "unreachable" rule without poisoning policy for the full
// TTL. A 4xx robots.txt is "unavailable" and therefore allows access.
package robots

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const MaxEvidencePolicyBytes = 512 << 10

// Checker resolves whether a given URL may be fetched by our user agent
// under the target site's robots.txt policy.
type Checker struct {
	client  *http.Client
	ttl     time.Duration
	maxSize int64

	mu    sync.Mutex
	cache map[string]entry
	sf    singleflight.Group
}

type entry struct {
	data    *policy
	fetched time.Time
}

// New constructs a Checker. client MUST apply an egress policy; ttl
// defaults to 1h when zero; maxSize defaults to 512 KiB when zero.
func New(client *http.Client, ttl time.Duration, maxSize int64) *Checker {
	if client == nil {
		client = http.DefaultClient
	}
	if ttl <= 0 {
		ttl = time.Hour
	}
	if maxSize <= 0 {
		maxSize = 512 * 1024
	}
	return &Checker{
		client:  client,
		ttl:     ttl,
		maxSize: maxSize,
		cache:   map[string]entry{},
	}
}

// Decision distinguishes parsed policy from the RFC 9309 complete-disallow
// assumption used when robots.txt is unreachable.
type Decision struct {
	Allowed       bool
	Authoritative bool
	Err           error
}

// Evaluate reports both the effective live-retrieval decision and whether it
// was based on an authoritative robots.txt response.
func (c *Checker) Evaluate(ctx context.Context, ua, rawURL string) Decision {
	u, err := url.Parse(rawURL)
	if err != nil {
		return Decision{Err: err}
	}
	if u.Scheme == "" || u.Host == "" {
		return Decision{Err: errors.New("robots: url missing scheme/host")}
	}
	productToken := robotsProductToken(ua)
	if productToken == "" {
		return Decision{Err: errors.New("robots: user agent product token is required")}
	}
	data, err := c.get(ctx, u.Scheme+"://"+u.Host, ua)
	if err != nil || data == nil {
		return Decision{Allowed: false, Authoritative: true, Err: err}
	}
	return Decision{Allowed: data.allowed(productToken, robotsRequestTarget(u)), Authoritative: true}
}

// EvaluatePolicy applies the same bounded RFC 9309 parser as Checker to exact
// already-fetched policy bytes. It exists for the separate publisher evidence
// adapter, which must hash and archive the policy representation it evaluated.
// Network status handling remains the caller's responsibility.
func EvaluatePolicy(body []byte, userAgent, rawURL string) (bool, error) {
	if len(body) > MaxEvidencePolicyBytes {
		return false, errors.New("robots: policy exceeds evidence byte limit")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return false, errors.New("robots: url missing scheme/host")
	}
	productToken := robotsProductToken(userAgent)
	if productToken == "" {
		return false, errors.New("robots: user agent product token is required")
	}
	data, err := parsePolicy(body)
	if err != nil {
		return false, err
	}
	return data.allowed(productToken, robotsRequestTarget(parsed)), nil
}

// Allowed reports whether ua may fetch rawURL. Network and server failures are
// complete disallow per RFC 9309; callers may inspect Evaluate for the cause.
func (c *Checker) Allowed(ctx context.Context, ua, rawURL string) (bool, error) {
	decision := c.Evaluate(ctx, ua, rawURL)
	if decision.Err != nil {
		return false, decision.Err
	}
	return decision.Allowed, nil
}

func (c *Checker) get(ctx context.Context, origin, ua string) (*policy, error) {
	c.mu.Lock()
	if e, ok := c.cache[origin]; ok && time.Since(e.fetched) < c.ttl {
		c.mu.Unlock()
		return e.data, nil
	}
	c.mu.Unlock()

	v, err, _ := c.sf.Do(origin, func() (any, error) {
		c.mu.Lock()
		if e, ok := c.cache[origin]; ok && time.Since(e.fetched) < c.ttl {
			c.mu.Unlock()
			return e.data, nil
		}
		c.mu.Unlock()

		data, err := c.fetch(ctx, origin, ua)
		// Do not turn transient failures or caller cancellation into a cached
		// allow decision. singleflight still coalesces concurrent attempts.
		if err == nil {
			now := time.Now()
			c.sweepExpired(now)
			c.mu.Lock()
			c.cache[origin] = entry{data: data, fetched: now}
			c.mu.Unlock()
		}
		return data, err
	})
	if v == nil {
		return nil, err
	}
	data, _ := v.(*policy)
	return data, err
}

func (c *Checker) sweepExpired(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for origin, e := range c.cache {
		if now.Sub(e.fetched) >= c.ttl {
			delete(c.cache, origin)
		}
	}
}

func (c *Checker) fetch(ctx context.Context, origin, ua string) (*policy, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/robots.txt", nil)
	if err != nil {
		return nil, err
	}
	if ua != "" {
		req.Header.Set("User-Agent", ua)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// RFC 9309 §2.3.1: 4xx is unavailable (access allowed), while 5xx is
	// unreachable (the caller assumes complete disallow).
	switch {
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		return parsePolicy(nil)
	case resp.StatusCode >= 500:
		return nil, fmt.Errorf("robots: upstream status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, c.maxSize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > c.maxSize {
		return nil, errors.New("robots: too large")
	}
	return parsePolicy(body)
}

// robotsProductToken returns the leading RFC 9309 product token from the
// descriptive HTTP User-Agent identification string.
func robotsProductToken(userAgent string) string {
	userAgent = strings.TrimSpace(userAgent)
	end := 0
	for end < len(userAgent) {
		char := userAgent[end]
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || char == '-' || char == '_' {
			end++
			continue
		}
		break
	}
	return userAgent[:end]
}

func robotsRequestTarget(parsed *url.URL) string {
	path := parsed.EscapedPath()
	if path == "" {
		path = "/"
	}
	if parsed.RawQuery != "" || parsed.ForceQuery {
		path += "?" + parsed.RawQuery
	}
	return path
}

// IsDisallowed is a convenience wrapper that maps (allowed, err) to a
// bool suitable for branching on the negative case.
func (c *Checker) IsDisallowed(ctx context.Context, ua, rawURL string) bool {
	allowed, err := c.Allowed(ctx, ua, rawURL)
	if err != nil {
		return true
	}
	return !allowed
}
