// Package obs wires Prometheus metrics into a single registry so every
// adapter can publish counters/histograms without importing the API
// layer. Metric names use the "fetchmark_" prefix per Prometheus
// conventions so they coexist cleanly with Go runtime metrics.
//
// Contract: every exported metric is process-global and safe for
// concurrent use. Labels are kept bounded (outcome, host-truncated,
// engine) to avoid cardinality blow-ups.
package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// HTTPRequestDuration records end-to-end HTTP handler latency keyed on
// method, route (chi pattern), and status class. Using the route pattern
// instead of the raw path keeps cardinality bounded.
var HTTPRequestDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "fetchmark_http_request_duration_seconds",
		Help:    "HTTP handler latency.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"method", "route", "status"},
)

// FetchOutcome counts outbound fetch attempts by coarse outcome.
var FetchOutcome = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_fetch_outcome_total",
		Help: "Outbound fetch outcomes.",
	},
	[]string{"outcome"}, // ok, client_error, server_error, network_error, egress_reject, robots_block, mime_reject, body_too_large
)

// FetchDuration records per-fetch latency for successful fetches.
var FetchDuration = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "fetchmark_fetch_duration_seconds",
		Help:    "Duration of a single outbound fetch (successful only).",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8, 15},
	},
)

// CacheEvents counts cache hits/misses per layer (fa=fetch artifact,
// fmt=rendered format).
var CacheEvents = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_cache_events_total",
		Help: "Cache hit/miss events.",
	},
	[]string{"layer", "event"}, // layer: fa|fmt, event: hit|miss|write
)

// EgressRejects counts egress-policy rejections by reason.
var EgressRejects = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_egress_rejects_total",
		Help: "Egress policy rejections, by reason.",
	},
	[]string{"reason"},
)

// RobotsBlocks counts fetches denied by robots.txt.
var RobotsBlocks = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "fetchmark_robots_blocks_total",
		Help: "Fetches denied by robots.txt.",
	},
)

// ExtractOutcome counts extractor outcomes.
var ExtractOutcome = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_extract_outcome_total",
		Help: "Extractor outcomes.",
	},
	[]string{"outcome"}, // ok, js_required, non_html, empty, error
)

// SearchQueryTotal counts /v1/search invocations by outcome.
var SearchQueryTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_search_total",
		Help: "Search endpoint invocations.",
	},
	[]string{"outcome"}, // ok, upstream_error, empty
)

// SearxngEngineUnresponsive is a gauge per engine: 1 when SearXNG
// reports the engine as unresponsive on the most recent query, 0 when
// healthy.
var SearxngEngineUnresponsive = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "fetchmark_searxng_engine_unresponsive",
		Help: "1 if the engine was unresponsive on the most recent query.",
	},
	[]string{"engine"},
)

// SearxngInstanceUp is a gauge per configured SearXNG backend URL:
// 1 when the instance responded successfully most recently, 0 when it
// is cooling down after a failure.
var SearxngInstanceUp = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "fetchmark_searxng_instance_up",
		Help: "1 if the SearXNG upstream instance is considered healthy.",
	},
	[]string{"instance"},
)

// RendererOutcome counts headless-render invocations by outcome.
var RendererOutcome = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_renderer_outcome_total",
		Help: "Headless renderer invocations.",
	},
	[]string{"outcome"}, // ok, error, disabled, skipped
)

// RendererDuration records renderer latency for successful calls.
var RendererDuration = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "fetchmark_renderer_duration_seconds",
		Help:    "Headless renderer call latency (successful only).",
		Buckets: []float64{0.25, 0.5, 1, 2, 4, 8, 15, 30, 60},
	},
)
