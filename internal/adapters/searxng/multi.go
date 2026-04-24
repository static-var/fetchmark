package searxng

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/staticvar/fetchmark/internal/core/search"
	"github.com/staticvar/fetchmark/internal/obs"
)

// defaultInstanceCooldown is used when no cooldown is supplied via
// NewMultiWithCooldown. Short enough to recover within a single user's
// retry window; long enough to not hammer a flapping backend.
const defaultInstanceCooldown = 30 * time.Second

// MultiClient fans a single Searcher contract out across N SearXNG
// instances. Strategy is round-robin among healthy instances with a
// post-failure cooldown; Ping returns nil when ANY instance is healthy.
//
// It always goes through this type even with a single upstream so the
// control flow is uniform and failover behaviour can be covered by one
// set of tests.
type MultiClient struct {
	clients []*Client
	labels  []string // human label per client for metrics (the base URL)

	mu       sync.Mutex
	cooldown []time.Time // clients[i] is unavailable until cooldown[i]

	cooldownDur time.Duration

	rr uint64 // round-robin counter; atomic
}

// NewMulti wraps one or more *Client instances using the default
// post-failure cooldown. Callers who want a tunable cooldown should use
// NewMultiWithCooldown.
func NewMulti(bases []string, httpc *http.Client) (*MultiClient, error) {
	return NewMultiWithCooldown(bases, httpc, defaultInstanceCooldown)
}

// NewMultiWithCooldown wraps one or more *Client instances. Ordering of
// bases becomes the initial round-robin order; the counter is atomic so
// the struct is safe for concurrent Search calls. A non-positive
// cooldown falls back to the default so operators cannot accidentally
// disable failover by misconfiguring the env var.
func NewMultiWithCooldown(bases []string, httpc *http.Client, cooldown time.Duration) (*MultiClient, error) {
	if len(bases) == 0 {
		return nil, errors.New("searxng: at least one base URL required")
	}
	if cooldown <= 0 {
		cooldown = defaultInstanceCooldown
	}
	cs := make([]*Client, 0, len(bases))
	labels := make([]string, 0, len(bases))
	for _, b := range bases {
		c, err := New(b, httpc)
		if err != nil {
			return nil, fmt.Errorf("searxng: instance %q: %w", b, err)
		}
		cs = append(cs, c)
		labels = append(labels, c.base.String())
	}
	mc := &MultiClient{
		clients:     cs,
		labels:      labels,
		cooldown:    make([]time.Time, len(cs)),
		cooldownDur: cooldown,
	}
	// Seed gauges at "up" so scrapes before the first Search still
	// report something meaningful.
	for _, l := range labels {
		obs.SearxngInstanceUp.WithLabelValues(l).Set(1)
	}
	return mc, nil
}

// Search tries healthy instances in round-robin order, marking an
// instance unhealthy on transport error or a context-deadline-exceeded
// from inside Search. The first non-error response wins. If every
// instance fails we return the last error so the caller sees a real
// upstream message rather than a synthetic one.
func (m *MultiClient) Search(ctx context.Context, q search.Query) ([]search.Hit, error) {
	order := m.healthyOrder()
	if len(order) == 0 {
		// All cooling down — try them all anyway, cheapest-first; we'd
		// rather serve a potentially-degraded response than refuse.
		order = m.fullOrder()
	}
	var lastErr error
	for _, idx := range order {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		hits, err := m.clients[idx].Search(ctx, q)
		if err == nil {
			m.markUp(idx)
			return hits, nil
		}
		lastErr = err
		m.markDown(idx)
	}
	if lastErr == nil {
		lastErr = errors.New("searxng: no instances available")
	}
	return nil, lastErr
}

// Ping passes if any instance answers. The readiness probe is meant to
// gate whether the service should receive traffic; as long as one
// upstream is reachable we can still serve requests.
func (m *MultiClient) Ping(ctx context.Context) error {
	var lastErr error
	for i, c := range m.clients {
		if err := c.Ping(ctx); err == nil {
			m.markUp(i)
			return nil
		} else {
			lastErr = err
			m.markDown(i)
		}
	}
	if lastErr == nil {
		lastErr = errors.New("searxng: no instances configured")
	}
	return lastErr
}

// healthyOrder returns indices of non-cooling-down instances starting
// from the next round-robin slot. Callers that find this empty fall
// back to fullOrder so a full outage still attempts every backend.
func (m *MultiClient) healthyOrder() []int {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	start := int(atomic.AddUint64(&m.rr, 1)-1) % len(m.clients)
	out := make([]int, 0, len(m.clients))
	for off := 0; off < len(m.clients); off++ {
		i := (start + off) % len(m.clients)
		if now.Before(m.cooldown[i]) {
			continue
		}
		out = append(out, i)
	}
	return out
}

func (m *MultiClient) fullOrder() []int {
	start := int(atomic.AddUint64(&m.rr, 1)-1) % len(m.clients)
	out := make([]int, len(m.clients))
	for off := 0; off < len(m.clients); off++ {
		out[off] = (start + off) % len(m.clients)
	}
	return out
}

func (m *MultiClient) markDown(i int) {
	m.mu.Lock()
	m.cooldown[i] = time.Now().Add(m.cooldownDur)
	m.mu.Unlock()
	obs.SearxngInstanceUp.WithLabelValues(m.labels[i]).Set(0)
}

func (m *MultiClient) markUp(i int) {
	m.mu.Lock()
	m.cooldown[i] = time.Time{}
	m.mu.Unlock()
	obs.SearxngInstanceUp.WithLabelValues(m.labels[i]).Set(1)
}
