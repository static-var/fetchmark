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

// DiscoveryCacheEvents records bounded cache/coalescing state transitions.
// Event values are fixed by the discoverycache package; never use query data.
var DiscoveryCacheEvents = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_discovery_cache_events_total",
		Help: "Discovery cache and query coalescing events.",
	},
	[]string{"event"},
)

// DiscoveryCacheEntries reports retained batches summed across provider caches.
var DiscoveryCacheEntries = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "fetchmark_discovery_cache_entries",
		Help: "Current number of retained discovery cache entries across providers.",
	},
)

// DiscoveryCacheBytes reports estimated bytes summed across provider caches.
var DiscoveryCacheBytes = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "fetchmark_discovery_cache_bytes",
		Help: "Estimated bytes retained by in-process discovery caches across providers.",
	},
)

// DiscoveryCacheInflight reports fills and refreshes across provider caches.
var DiscoveryCacheInflight = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "fetchmark_discovery_cache_inflight",
		Help: "Current number of detached discovery fills and refreshes.",
	},
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

// LocalIndexMutationTotal counts bounded opt-in corpus mutations. Outcome is
// fixed to indexed, removed, or error and never contains a URL or provider.
var LocalIndexMutationTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_local_index_mutation_total",
		Help: "Opt-in local corpus mutations by outcome.",
	},
	[]string{"outcome"},
)

// LocalArtifactMutationTotal counts durable personal-corpus source lifecycle
// events. Labels are fixed and never contain URLs, hashes, or policy reasons.
var LocalArtifactMutationTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_local_artifact_mutation_total",
		Help: "Durable local artifact mutations by outcome.",
	},
	[]string{"outcome"}, // stored, revalidated, revoked, error
)

// LocalCuratedMutationTotal counts explicit admin-only curated admissions and
// takedowns. Both labels come from fixed code paths; URLs and policy details
// never become metric labels.
var LocalCuratedMutationTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_local_curated_mutation_total",
		Help: "Explicit curated corpus mutations by operation and outcome.",
	},
	[]string{"operation", "outcome"}, // operation: admit|takedown, outcome: ok|rejected|error
)

// LocalExpirySweepTotal counts scheduled personal-corpus sweeps. Both labels
// come from fixed process constants and never contain paths or document data.
var LocalExpirySweepTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_local_expiry_sweep_total",
		Help: "Scheduled local expiry sweeps by store and outcome.",
	},
	[]string{"store", "outcome"}, // store: index|artifact, outcome: ok|error
)

// LocalExpirySweepRemovedTotal counts versions physically removed by scheduled
// expiry work. Reads already exclude expired versions before physical cleanup.
var LocalExpirySweepRemovedTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_local_expiry_sweep_removed_total",
		Help: "Expired local versions physically removed by store.",
	},
	[]string{"store"}, // index|artifact
)

// SearchQueryTotal counts /v1/search invocations by outcome.
var SearchQueryTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_search_total",
		Help: "Search endpoint invocations.",
	},
	[]string{"outcome"}, // ok, upstream_error, empty
)

// DiscoveryBatchTotal counts provider attempts by their rich batch status.
// Instance labels come only from operator-configured providers and remain
// bounded by the process configuration.
var DiscoveryBatchTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_discovery_batch_total",
		Help: "Discovery provider attempts by batch status.",
	},
	[]string{"provider", "instance", "status"},
)

// DiscoveryBatchDuration records complete provider-attempt latency, including
// failed and degraded-empty calls.
var DiscoveryBatchDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "fetchmark_discovery_batch_duration_seconds",
		Help:    "Discovery provider attempt latency.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8, 15, 30},
	},
	[]string{"provider", "instance"},
)

// DiscoveryResultCount records the number of hits returned by each provider
// attempt. Zero remains observable separately from the status classification.
var DiscoveryResultCount = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "fetchmark_discovery_results",
		Help:    "Number of discovery hits returned by a provider attempt.",
		Buckets: []float64{0, 1, 3, 5, 10, 20, 50, 100},
	},
	[]string{"provider", "instance"},
)

// DiscoveryDiagnosticTotal counts source-level diagnostics without placing
// upstream-controlled source or reason text in metric labels.
var DiscoveryDiagnosticTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_discovery_diagnostic_total",
		Help: "Source-level diagnostics reported by discovery providers.",
	},
	[]string{"provider", "instance"},
)

// DiscoveryLaneTotal counts controlled broker lanes. Provider, lane, and
// variant labels come from the process-configured registry, never upstream
// response payloads.
var DiscoveryLaneTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_discovery_lane_total",
		Help: "Discovery lanes by trusted provider, lane, query variant, and outcome status.",
	},
	[]string{"provider", "lane", "variant", "status"},
)

// DiscoveryLaneDuration records wall time for each discovery lane.
var DiscoveryLaneDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "fetchmark_discovery_lane_duration_seconds",
		Help:    "Discovery lane latency by trusted provider, lane, and query variant.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8, 15, 30},
	},
	[]string{"provider", "lane", "variant"},
)

// DiscoverySourceCandidates observes unique canonical candidates contributed
// by a lane before cross-lane fusion.
var DiscoverySourceCandidates = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "fetchmark_discovery_source_candidates",
		Help:    "Unique canonical candidates supplied by a discovery lane.",
		Buckets: []float64{0, 1, 2, 3, 5, 10, 20, 50, 100},
	},
	[]string{"provider", "lane", "variant"},
)

// DiscoverySourceTopKContribution counts final returned results to which a
// source lane contributed. Overlap is intentional, so totals can exceed top-k.
var DiscoverySourceTopKContribution = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_discovery_source_topk_contribution_total",
		Help: "Final returned results attributed to a discovery provider, lane, and variant.",
	},
	[]string{"provider", "lane", "variant"},
)

// DiscoverySourceTopKUniqueDomains observes final domain diversity by source
// lane without placing hostnames in labels.
var DiscoverySourceTopKUniqueDomains = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "fetchmark_discovery_source_topk_unique_domains",
		Help:    "Unique final top-k domains attributed to a discovery provider, lane, and variant.",
		Buckets: []float64{0, 1, 2, 3, 5, 10, 20, 50},
	},
	[]string{"provider", "lane", "variant"},
)

// DiscoveryTopKUniqueDomains observes aggregate final domain diversity.
var DiscoveryTopKUniqueDomains = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "fetchmark_discovery_topk_unique_domains",
		Help:    "Unique domains in final brokered search results.",
		Buckets: []float64{0, 1, 2, 3, 5, 10, 20, 50},
	},
)

// DiscoveryInstanceQuality is an exponentially weighted response-quality
// score. It is deliberately separate from transport reachability.
var DiscoveryInstanceQuality = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "fetchmark_discovery_instance_quality",
		Help: "EWMA discovery response quality from 0 (degraded) to 1 (healthy).",
	},
	[]string{"provider", "instance"},
)

// DiscoveryCircuitOpen exposes bounded transport and response-quality circuit
// state without embedding failure text in labels.
var DiscoveryCircuitOpen = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "fetchmark_discovery_circuit_open",
		Help: "Whether a discovery circuit is currently open.",
	},
	[]string{"provider", "instance", "kind"},
)

// FederationRequestTotal counts the optional trusted-peer protocol by
// direction, operator-configured peer binding, and fixed local outcome. The
// special inbound peer label "untrusted" is used before authentication;
// queries, URLs, identities, nonces, and error text are never labels.
var FederationRequestTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_federation_request_total",
		Help: "Private federation requests by direction, trusted peer binding, and bounded outcome.",
	},
	[]string{"direction", "peer", "outcome"},
)

// FederationRequestDuration records complete signed-protocol request latency.
var FederationRequestDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "fetchmark_federation_request_duration_seconds",
		Help:    "Private federation request latency by direction and trusted peer binding.",
		Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	},
	[]string{"direction", "peer"},
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

// SearxngEngineQuality retains a smoothed source-quality signal per configured
// SearXNG instance instead of treating one response as permanent health.
var SearxngEngineQuality = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "fetchmark_searxng_engine_quality",
		Help: "EWMA SearXNG engine response quality from 0 (unresponsive) to 1 (healthy).",
	},
	[]string{"instance", "engine"},
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

// SummarizeOutcome counts /v1/summarize invocations by provider and
// outcome class. Outcome labels stay coarse so cardinality stays
// bounded (ok, fetch_failed, parse_empty, provider_auth,
// provider_rate_limit, provider_upstream, provider_timeout, error).
var SummarizeOutcome = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_summarize_total",
		Help: "Summarize endpoint invocations.",
	},
	[]string{"provider", "outcome"},
)

// SummarizeDuration records end-to-end summarize latency (including
// the fetch+parse step) for successful calls only.
var SummarizeDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "fetchmark_summarize_duration_seconds",
		Help:    "Summarize end-to-end latency (successful only).",
		Buckets: []float64{0.5, 1, 2, 4, 8, 15, 30, 60, 120},
	},
	[]string{"provider"},
)

// SummarizeTokens counts tokens consumed by summarize calls. Labels
// are bounded to provider and class (prompt, completion, reasoning).
var SummarizeTokens = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "fetchmark_summarize_tokens_total",
		Help: "Tokens consumed by summarize.",
	},
	[]string{"provider", "class"},
)
