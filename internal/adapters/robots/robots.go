// Package robots provides a cached robots.txt policy checker.
//
// The checker fetches /robots.txt per host (via the caller-supplied
// http.Client, which MUST already be subject to an egress policy), parses
// it with github.com/temoto/robotstxt, and caches the result with a TTL.
// A fetch failure is cached as "allow all" — we deliberately fail open
// on unreachable robots.txt so a dead host doesn't also block its pages.
// Conversely, a 4xx robots.txt is treated per the spec (4xx => allowed).
package robots

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/temoto/robotstxt"
)

// Checker resolves whether a given URL may be fetched by our user agent
// under the target site's robots.txt policy.
type Checker struct {
	client  *http.Client
	ttl     time.Duration
	maxSize int64

	mu    sync.Mutex
	cache map[string]entry
}

type entry struct {
	data    *robotstxt.RobotsData
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

// Allowed reports whether ua may fetch rawURL. Any fetch/parse error is
// treated as "allow" — see package doc.
func (c *Checker) Allowed(ctx context.Context, ua, rawURL string) (bool, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false, err
	}
	if u.Scheme == "" || u.Host == "" {
		return false, errors.New("robots: url missing scheme/host")
	}
	data, err := c.get(ctx, u.Scheme+"://"+u.Host)
	if err != nil || data == nil {
		return true, nil
	}
	return data.TestAgent(u.EscapedPath(), ua), nil
}

func (c *Checker) get(ctx context.Context, origin string) (*robotstxt.RobotsData, error) {
	c.mu.Lock()
	if e, ok := c.cache[origin]; ok && time.Since(e.fetched) < c.ttl {
		c.mu.Unlock()
		return e.data, nil
	}
	c.mu.Unlock()

	data, err := c.fetch(ctx, origin)
	// Cache even failures (as nil data) so we don't hammer broken hosts.
	now := time.Now()
	c.sweepExpired(now)
	c.mu.Lock()
	c.cache[origin] = entry{data: data, fetched: now}
	c.mu.Unlock()
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

func (c *Checker) fetch(ctx context.Context, origin string) (*robotstxt.RobotsData, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/robots.txt", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Per RFC 9309 §2.3.1: 4xx => full allow, 5xx => full disallow (we
	// fail open on 5xx to match the stated policy in the package doc).
	switch {
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		return robotstxt.FromString("")
	case resp.StatusCode >= 500:
		return nil, nil // caller treats nil-data as allow
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, c.maxSize))
	if err != nil {
		return nil, err
	}
	return robotstxt.FromBytes(body)
}

// IsDisallowed is a convenience wrapper that maps (allowed, err) to a
// bool suitable for branching on the negative case.
func (c *Checker) IsDisallowed(ctx context.Context, ua, rawURL string) bool {
	allowed, err := c.Allowed(ctx, ua, rawURL)
	if err != nil {
		return false
	}
	return !allowed
}

var _ = strings.TrimSpace // keep strings import reserved for future use
