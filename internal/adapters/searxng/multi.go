package searxng

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

const maxInstanceBackoff = 15 * time.Minute

const (
	qualityEWMAAlpha     = 0.2
	degradedCircuitAfter = 2
)

// PoolCoolingError reports that every configured instance is still inside a
// server-requested or locally calculated cooldown. Callers should retry no
// sooner than RetryAfter instead of bypassing the open circuits.
type PoolCoolingError struct {
	RetryAfter time.Duration
}

func (e *PoolCoolingError) Error() string {
	return fmt.Sprintf("searxng: all instances cooling down; retry after %s", e.RetryAfter.Round(time.Millisecond))
}

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

	mu              sync.Mutex
	cooldown        []time.Time // clients[i] is unavailable until cooldown[i]
	failures        []int
	qualityCooldown []time.Time
	degradedStreak  []int
	qualityEWMA     []float64

	cooldownDur time.Duration
	now         func() time.Time
	jitter      func(time.Duration, string, int) time.Duration

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
		labels = append(labels, c.instanceID())
	}
	mc := &MultiClient{
		clients:         cs,
		labels:          labels,
		cooldown:        make([]time.Time, len(cs)),
		failures:        make([]int, len(cs)),
		qualityCooldown: make([]time.Time, len(cs)),
		degradedStreak:  make([]int, len(cs)),
		qualityEWMA:     make([]float64, len(cs)),
		cooldownDur:     cooldown,
		now:             time.Now,
		jitter:          deterministicJitter,
	}
	// Seed gauges at "up" so scrapes before the first Search still
	// report something meaningful.
	for i, l := range labels {
		mc.qualityEWMA[i] = 1
		obs.SearxngInstanceUp.WithLabelValues(l).Set(1)
		obs.DiscoveryInstanceQuality.WithLabelValues("searxng", l).Set(1)
		obs.DiscoveryCircuitOpen.WithLabelValues("searxng", l, "transport").Set(0)
		obs.DiscoveryCircuitOpen.WithLabelValues("searxng", l, "quality").Set(0)
	}
	return mc, nil
}

// Search preserves the original Searcher contract while SearchBatch exposes
// which instance and underlying engines contributed to the outcome.
func (m *MultiClient) Search(ctx context.Context, q search.Query) ([]search.Hit, error) {
	batch, err := m.SearchBatch(ctx, q)
	if err != nil {
		return nil, err
	}
	return batch.Hits, nil
}

// SearchBatch tries healthy instances in round-robin order. A transport or
// retryable HTTP failure moves an instance into cooldown. A degraded-empty
// response remains transport-healthy but is not accepted as authoritative, so
// the pool continues to another instance and preserves its diagnostics.
func (m *MultiClient) SearchBatch(ctx context.Context, q search.Query) (search.SearchBatch, error) {
	started := time.Now()
	if err := ctx.Err(); err != nil {
		return search.SearchBatch{Provider: "searxng", Status: search.BatchFailed, Duration: time.Since(started)}, err
	}
	order := m.healthyOrder()
	if len(order) == 0 {
		if err := ctx.Err(); err != nil {
			return search.SearchBatch{Provider: "searxng", Status: search.BatchFailed, Duration: time.Since(started)}, err
		}
		return search.SearchBatch{
			Provider: "searxng",
			Status:   search.BatchFailed,
			Duration: time.Since(started),
		}, &PoolCoolingError{RetryAfter: m.nextRetryAfter()}
	}
	var lastErr error
	var degraded *search.SearchBatch
	var diagnostics []search.ProviderDiagnostic
	for _, idx := range order {
		if ctx.Err() != nil {
			return search.SearchBatch{
				Provider:    "searxng",
				Status:      search.BatchFailed,
				Diagnostics: diagnostics,
				Duration:    time.Since(started),
			}, ctx.Err()
		}
		batch, err := m.clients[idx].SearchBatch(ctx, q)
		if err == nil {
			m.recordBatch(idx, batch)
			batch.Diagnostics = appendProviderDiagnostics(diagnostics, batch.Diagnostics)
			batch = classifyAggregateBatch(batch)
			batch.Duration = time.Since(started)
			if batch.Status == search.BatchDegradedEmpty {
				copy := batch
				degraded = &copy
				diagnostics = copy.Diagnostics
				continue
			}
			return batch, nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return search.SearchBatch{
				Provider:    "searxng",
				Status:      search.BatchFailed,
				Diagnostics: diagnostics,
				Duration:    time.Since(started),
			}, context.Canceled
		}
		lastErr = err
		diagnostics = appendProviderDiagnostics(diagnostics, []search.ProviderDiagnostic{m.failureDiagnostic(idx, err)})
		if isRetryableSearchError(err) {
			m.markDown(idx, retryAfterFromError(err))
		} else {
			// A non-retryable HTTP response still proves the configured
			// instance is reachable; do not carry an old transport streak.
			m.markUp(idx)
		}
	}
	if degraded != nil {
		degraded.Diagnostics = diagnostics
		degraded.Duration = time.Since(started)
		return *degraded, nil
	}
	if lastErr == nil {
		lastErr = errors.New("searxng: no instances available")
	}
	return search.SearchBatch{
		Provider:    "searxng",
		Status:      search.BatchFailed,
		Diagnostics: diagnostics,
		Duration:    time.Since(started),
	}, lastErr
}

func (m *MultiClient) recordBatch(i int, batch search.SearchBatch) {
	if retryable, retryAfter := diagnosticRetryPolicy(batch.Diagnostics, m.labels[i]); retryable {
		m.markDown(i, retryAfter)
	} else {
		m.markUp(i)
	}
	now := m.now()
	observation := 1.0
	m.mu.Lock()
	switch batch.Status {
	case search.BatchHealthy:
		m.degradedStreak[i] = 0
		m.qualityCooldown[i] = time.Time{}
	case search.BatchPartial:
		observation = 0.5
		m.degradedStreak[i] = 0
		m.qualityCooldown[i] = time.Time{}
	case search.BatchDegradedEmpty:
		observation = 0
		m.degradedStreak[i]++
		if m.degradedStreak[i] >= degradedCircuitAfter {
			attempt := m.degradedStreak[i] - degradedCircuitAfter + 1
			m.qualityCooldown[i] = now.Add(m.jitter(exponentialBackoff(m.cooldownDur, attempt), m.labels[i]+"/quality", attempt))
		}
	case search.BatchAuthoritativeEmpty:
		// A genuine empty is query-dependent evidence, not an instance
		// failure. It must never open a shared provider circuit.
		observation = 1
		m.degradedStreak[i] = 0
		m.qualityCooldown[i] = time.Time{}
	}
	m.qualityEWMA[i] += qualityEWMAAlpha * (observation - m.qualityEWMA[i])
	score := m.qualityEWMA[i]
	open := now.Before(m.qualityCooldown[i])
	m.mu.Unlock()
	obs.DiscoveryInstanceQuality.WithLabelValues("searxng", m.labels[i]).Set(score)
	obs.DiscoveryCircuitOpen.WithLabelValues("searxng", m.labels[i], "quality").Set(boolFloat(open))
}

func diagnosticRetryPolicy(diagnostics []search.ProviderDiagnostic, instance string) (bool, time.Duration) {
	retryable := false
	var retryAfter time.Duration
	for _, diagnostic := range diagnostics {
		if diagnostic.Instance != instance || !diagnostic.Retryable {
			continue
		}
		retryable = true
		if diagnostic.RetryAfter > retryAfter {
			retryAfter = diagnostic.RetryAfter
		}
	}
	return retryable, retryAfter
}

func retryAfterFromError(err error) time.Duration {
	var statusErr *StatusError
	if errors.As(err, &statusErr) {
		return statusErr.RetryAfter
	}
	return 0
}

func (m *MultiClient) failureDiagnostic(idx int, err error) search.ProviderDiagnostic {
	return search.ProviderDiagnostic{
		Provider:   "searxng",
		Instance:   m.clients[idx].instanceID(),
		Source:     "instance",
		Reason:     normalizedFailureReason(err),
		Retryable:  isRetryableSearchError(err),
		RetryAfter: retryAfterFromError(err),
	}
}

func normalizedFailureReason(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, errResponseTooLarge):
		return "response_too_large"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "malformed_response"
	}
	var statusErr *StatusError
	if errors.As(err, &statusErr) {
		return fmt.Sprintf("http_%d", statusErr.StatusCode)
	}
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &syntaxErr) || errors.As(err, &typeErr) {
		return "malformed_response"
	}
	return "request_failed"
}

func classifyAggregateBatch(batch search.SearchBatch) search.SearchBatch {
	if len(batch.Diagnostics) == 0 {
		return batch
	}
	if len(batch.Hits) > 0 {
		batch.Status = search.BatchPartial
	} else {
		batch.Status = search.BatchDegradedEmpty
	}
	return batch
}

func appendProviderDiagnostics(base, added []search.ProviderDiagnostic) []search.ProviderDiagnostic {
	if len(base) == 0 {
		return append([]search.ProviderDiagnostic(nil), added...)
	}
	out := append([]search.ProviderDiagnostic(nil), base...)
	seen := make(map[string]int, len(base)+len(added))
	for i, diagnostic := range base {
		seen[diagnostic.Instance+"\x00"+diagnostic.Source+"\x00"+diagnostic.Reason] = i
	}
	for _, diagnostic := range added {
		key := diagnostic.Instance + "\x00" + diagnostic.Source + "\x00" + diagnostic.Reason
		if index, ok := seen[key]; ok {
			out[index].Retryable = out[index].Retryable || diagnostic.Retryable
			if diagnostic.RetryAfter > out[index].RetryAfter {
				out[index].RetryAfter = diagnostic.RetryAfter
			}
			continue
		}
		seen[key] = len(out)
		out = append(out, diagnostic)
	}
	return out
}

func isRetryableSearchError(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	var statusErr *StatusError
	if errors.As(err, &statusErr) {
		return statusErr.Retryable()
	}
	return true
}

// Ping passes if any instance answers. The readiness probe is meant to
// gate whether the service should receive traffic; as long as one
// upstream is reachable we can still serve requests.
func (m *MultiClient) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var lastErr error
	for i, c := range m.clients {
		if err := c.Ping(ctx); err == nil {
			m.markUp(i)
			return nil
		} else {
			if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
				return context.Canceled
			}
			lastErr = err
			m.markDown(i)
		}
	}
	if lastErr == nil {
		lastErr = errors.New("searxng: no instances configured")
	}
	return lastErr
}

// healthyOrder returns indices whose transport and response-quality circuits
// are both eligible, starting from the next round-robin slot.
func (m *MultiClient) healthyOrder() []int {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	start := int(atomic.AddUint64(&m.rr, 1)-1) % len(m.clients)
	out := make([]int, 0, len(m.clients))
	for off := 0; off < len(m.clients); off++ {
		i := (start + off) % len(m.clients)
		if !m.qualityCooldown[i].IsZero() && !now.Before(m.qualityCooldown[i]) {
			m.qualityCooldown[i] = time.Time{}
			obs.DiscoveryCircuitOpen.WithLabelValues("searxng", m.labels[i], "quality").Set(0)
		}
		if now.Before(m.cooldown[i]) || now.Before(m.qualityCooldown[i]) {
			continue
		}
		out = append(out, i)
	}
	return out
}

func (m *MultiClient) nextRetryAfter() time.Duration {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	var earliest time.Time
	for i, until := range m.cooldown {
		if m.qualityCooldown[i].After(until) {
			until = m.qualityCooldown[i]
		}
		if until.After(now) && (earliest.IsZero() || until.Before(earliest)) {
			earliest = until
		}
	}
	if earliest.IsZero() {
		return 0
	}
	return earliest.Sub(now)
}

func (m *MultiClient) markDown(i int, retryAfter ...time.Duration) {
	now := m.now()
	m.mu.Lock()
	m.failures[i]++
	backoff := exponentialBackoff(m.cooldownDur, m.failures[i])
	backoff = m.jitter(backoff, m.labels[i], m.failures[i])
	if len(retryAfter) > 0 && retryAfter[0] > backoff {
		backoff = retryAfter[0]
	}
	m.cooldown[i] = now.Add(backoff)
	m.mu.Unlock()
	obs.SearxngInstanceUp.WithLabelValues(m.labels[i]).Set(0)
	obs.DiscoveryCircuitOpen.WithLabelValues("searxng", m.labels[i], "transport").Set(1)
}

func (m *MultiClient) markUp(i int) {
	m.mu.Lock()
	m.cooldown[i] = time.Time{}
	m.failures[i] = 0
	m.mu.Unlock()
	obs.SearxngInstanceUp.WithLabelValues(m.labels[i]).Set(1)
	obs.DiscoveryCircuitOpen.WithLabelValues("searxng", m.labels[i], "transport").Set(0)
}

func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func exponentialBackoff(base time.Duration, failures int) time.Duration {
	if failures <= 1 {
		return base
	}
	limit := maxInstanceBackoff
	if base > limit {
		limit = base
	}
	backoff := base
	for attempt := 1; attempt < failures && backoff < limit; attempt++ {
		if backoff > limit/2 {
			return limit
		}
		backoff *= 2
	}
	if backoff > limit {
		return limit
	}
	return backoff
}

func deterministicJitter(duration time.Duration, label string, failures int) time.Duration {
	var hash uint64 = 1469598103934665603
	for i := 0; i < len(label); i++ {
		hash ^= uint64(label[i])
		hash *= 1099511628211
	}
	hash ^= uint64(failures)
	// Add 0..20% deterministic jitter so a process restart does not align all
	// configured instances while tests and operators retain reproducibility.
	return duration + time.Duration(float64(duration)*(float64(hash%201)/1000))
}
