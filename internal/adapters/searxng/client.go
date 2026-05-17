// Package searxng implements the Searcher interface against a SearXNG
// instance's JSON API.
//
// SearXNG's /search endpoint returns results ordered by engine priority,
// not by relevance, and carries no per-result score field. Re-ranking
// therefore happens in internal/core/rank, not here.
package searxng

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/staticvar/fetchmark/internal/core/search"
	"github.com/staticvar/fetchmark/internal/obs"
)

// Client is a thin typed wrapper around a SearXNG instance.
type Client struct {
	base     *url.URL
	http     *http.Client
	mu       sync.Mutex
	knownEng map[string]struct{}
}

// New constructs a Client. The provided HTTP client is used as-is, which
// lets callers install their own egress policy, proxy settings, and
// metrics middleware. If httpc is nil, a client with a 10s timeout is
// used.
func New(baseURL string, httpc *http.Client) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("searxng: parse base url: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, errors.New("searxng: base url must include scheme and host")
	}
	if httpc == nil {
		httpc = &http.Client{Timeout: 10 * time.Second}
	}
	return &Client{base: u, http: httpc, knownEng: map[string]struct{}{}}, nil
}

// response mirrors the subset of SearXNG's JSON we consume. The schema
// includes more fields (infoboxes, suggestions, answers) — deliberately
// ignored here to keep the contract small.
type response struct {
	Query               string      `json:"query"`
	NumberOfResults     int         `json:"number_of_results"`
	Results             []apiResult `json:"results"`
	UnresponsiveEngines [][]any     `json:"unresponsive_engines"`
}

type apiResult struct {
	URL           string          `json:"url"`
	Title         string          `json:"title"`
	Content       string          `json:"content"`
	Engine        string          `json:"engine"`
	Engines       []string        `json:"engines"`
	Category      string          `json:"category"`
	Date          json.RawMessage `json:"date"`
	Published     json.RawMessage `json:"published"`
	PublishedAt   json.RawMessage `json:"published_at"`
	PublishedDate json.RawMessage `json:"publishedDate"`
}

// Search runs a SearXNG query and returns hits in the order SearXNG
// provided them. Ordering is not relevance-ranked.
func (c *Client) Search(ctx context.Context, q search.Query) ([]search.Hit, error) {
	if strings.TrimSpace(q.Q) == "" {
		return nil, errors.New("searxng: empty query")
	}

	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + "/search"

	vals := url.Values{}
	vals.Set("q", q.Q)
	vals.Set("format", "json")
	if len(q.Engines) > 0 {
		vals.Set("engines", strings.Join(q.Engines, ","))
	}
	if len(q.Categories) > 0 {
		vals.Set("categories", strings.Join(q.Categories, ","))
	}
	if q.Language != "" {
		vals.Set("language", q.Language)
	}
	if q.TimeRange != "" {
		vals.Set("time_range", q.TimeRange)
	}
	out := make([]search.Hit, 0, q.MaxResults)
	var allResults []apiResult
	var allUnresponsive [][]any
	for page := 1; ; page++ {
		pageVals := cloneValues(vals)
		if q.MaxResults > 0 {
			pageVals.Set("pageno", strconv.Itoa(page))
		}
		u.RawQuery = pageVals.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")

		resp, err := c.http.Do(req)
		if err != nil {
			return nil, fmt.Errorf("searxng: %w", err)
		}

		var body response
		decodeErr := json.NewDecoder(resp.Body).Decode(&body)
		closeErr := resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("searxng: status %d", resp.StatusCode)
		}
		if decodeErr != nil {
			return nil, fmt.Errorf("searxng: decode: %w", decodeErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("searxng: close response: %w", closeErr)
		}

		allResults = append(allResults, body.Results...)
		allUnresponsive = append(allUnresponsive, body.UnresponsiveEngines...)
		if len(body.Results) == 0 {
			break
		}
		for _, r := range body.Results {
			out = append(out, hitFromAPIResult(r, len(out)+1))
			if q.MaxResults > 0 && len(out) >= q.MaxResults {
				c.updateEngineHealth(allResults, allUnresponsive)
				return out[:q.MaxResults], nil
			}
		}
		if q.MaxResults <= 0 {
			break
		}
	}

	c.updateEngineHealth(allResults, allUnresponsive)
	return out, nil
}

func cloneValues(vals url.Values) url.Values {
	out := make(url.Values, len(vals))
	for k, v := range vals {
		out[k] = append([]string(nil), v...)
	}
	return out
}

func hitFromAPIResult(r apiResult, rank int) search.Hit {
	engines := r.Engines
	if len(engines) == 0 && r.Engine != "" {
		engines = []string{r.Engine}
	}
	metadata := map[string]string{"original_rank": strconv.Itoa(rank)}
	if r.Category != "" {
		metadata["category"] = r.Category
	}
	publishedAt, rawPublished := decodePublishedAt(r)
	if rawDate, ok := decodeDateString(r.Date); ok && rawDate != "" {
		metadata["date"] = rawDate
	}
	if rawPublished != "" {
		metadata["published_at"] = rawPublished
	}
	return search.Hit{
		URL:         r.URL,
		Title:       r.Title,
		Snippet:     r.Content,
		Engines:     engines,
		PublishedAt: publishedAt,
		Metadata:    metadata,
	}
}

func decodePublishedAt(r apiResult) (*time.Time, string) {
	firstRaw := ""
	for _, raw := range []json.RawMessage{r.PublishedAt, r.PublishedDate, r.Published, r.Date} {
		if len(raw) == 0 || string(raw) == "null" {
			continue
		}
		text, ok := decodeDateString(raw)
		if !ok || text == "" {
			continue
		}
		if firstRaw == "" {
			firstRaw = text
		}
		if t, ok := parseDate(text); ok {
			return &t, text
		}
	}
	return nil, firstRaw
}

func decodeDateString(raw json.RawMessage) (string, bool) {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return strings.TrimSpace(s), true
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		return n.String(), true
	}
	return "", false
}

func parseDate(s string) (time.Time, bool) {
	if unix, err := strconv.ParseInt(s, 10, 64); err == nil {
		if unix > 1_000_000_000_000 {
			unix /= 1000
		}
		return time.Unix(unix, 0).UTC(), true
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, time.RFC1123Z, time.RFC1123, "2006-01-02", "2006-01-02 15:04:05", "Jan 2, 2006", "2 Jan 2006"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// updateEngineHealth mirrors SearXNG's per-engine availability into a
// Prometheus gauge. SearXNG exposes "unresponsive_engines" as a list of
// [name, reason] tuples; we track names across queries and clear the
// gauge on engines that came back. Lock held briefly; this is not on
// the hot path for latency.
func (c *Client) updateEngineHealth(results []apiResult, unresponsive [][]any) {
	bad := map[string]struct{}{}
	for _, entry := range unresponsive {
		if len(entry) == 0 {
			continue
		}
		if name, ok := entry[0].(string); ok && name != "" {
			bad[name] = struct{}{}
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, r := range results {
		if r.Engine != "" {
			c.knownEng[r.Engine] = struct{}{}
		}
		for _, e := range r.Engines {
			c.knownEng[e] = struct{}{}
		}
	}
	for name := range bad {
		c.knownEng[name] = struct{}{}
	}

	for name := range c.knownEng {
		v := 0.0
		if _, unhealthy := bad[name]; unhealthy {
			v = 1
		}
		obs.SearxngEngineUnresponsive.WithLabelValues(name).Set(v)
	}
}

// Ping performs a lightweight GET against SearXNG's root to verify
// reachability. It does not hit the /search endpoint to avoid loading
// external engines on every readiness probe.
func (c *Client) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base.String(), nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("searxng: ping: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return fmt.Errorf("searxng: ping status %d", resp.StatusCode)
	}
	return nil
}
