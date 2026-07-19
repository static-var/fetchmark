# obs

Prometheus metrics. One package, one registry, bounded label cardinality.

## Entry points

- `metrics.go` — exported `prometheus.Counter` / `Histogram` vars plus
  `Register(reg)` which installs them on a caller-supplied registry.
  `internal/api` wires the default registry on `/metrics`.

## Invariants

- **Label cardinality is deliberately small**: outcome, configured provider
  instance, engine/source, route-pattern, cache-layer. Provider instances are
  bounded by process configuration. **Do not add per-result URL, query,
  per-API-key, or raw upstream-reason labels** — a scrape with unbounded input
  values will melt the server. If you need that detail, sample into logs.
- Route labels are chi's route pattern (`/v1/parse`), not the raw
  request URL. The middleware in `internal/api/middleware` handles
  this; do not re-derive it elsewhere.
- Histogram buckets are tuned for 10ms–30s fetch budgets; changing
  them is a breaking change for dashboards.
- Discovery metrics separate a zero result count from its batch status:
  `fetchmark_discovery_batch_total`,
  `fetchmark_discovery_batch_duration_seconds`,
  `fetchmark_discovery_results`, and
  `fetchmark_discovery_diagnostic_total`. Response quality and circuit state
  remain separate in `fetchmark_discovery_instance_quality`,
  `fetchmark_discovery_circuit_open`, and
  `fetchmark_searxng_engine_quality`. SearXNG engine observations retain at
  most 512 individual engine labels per configured instance; later unknown
  names aggregate into `__other__` rather than creating unbounded series.
- Discovery cache observability uses fixed event values in
  `fetchmark_discovery_cache_events_total` plus current entry, estimated-byte,
  and detached-flight gauges. Query text and cache keys never appear in labels.
- `fetchmark_local_index_mutation_total{outcome}` uses only the fixed outcomes
  `indexed`, `removed`, and `error`; URLs and retention reasons never become
  labels.
- `fetchmark_local_artifact_mutation_total{outcome}` uses only `stored`,
  `revalidated`, `revoked`, and `error`; URLs, validators, hashes, and policy
  reasons never become labels.
- `fetchmark_local_curated_mutation_total{operation,outcome}` uses the fixed
  operations `admit`/`takedown` and outcomes `ok`/`rejected`/`error`; submitted
  URLs and policy reasons never become labels.
- Runtime expiry uses `fetchmark_local_expiry_sweep_total{store,outcome}` and
  `fetchmark_local_expiry_sweep_removed_total{store}`. `store` is fixed to
  `index` or `artifact`, and `outcome` to `ok` or `error`; paths and document
  data never become labels.
- Broker fanout uses only process-registered provider kinds and lane IDs,
  controlled variant names, and enum statuses in labels.
  `fetchmark_discovery_lane_total` and
  `fetchmark_discovery_lane_duration_seconds` cover lane outcomes;
  `fetchmark_discovery_source_candidates` records canonical candidate counts.
  Final contribution is measured after extraction, reranking, and dedupe via
  `fetchmark_discovery_source_topk_contribution_total`, while source and
  aggregate unique-domain counts are numeric histogram observations. Exact and
  near-content duplicate attribution is unioned before these final metrics.
  Provider
  payload fields, engines, URLs, domains, queries, and diagnostic reasons are
  never used as labels.
- Private federation uses
  `fetchmark_federation_request_total{direction,peer,outcome}` and
  `fetchmark_federation_request_duration_seconds{direction,peer}`. `peer` is an
  operator-bounded trust binding; unauthenticated inbound traffic aggregates as
  `untrusted`. Outcomes are fixed local classes. Identity IDs, queries, URLs,
  nonces, signatures, HTTP error bodies, and raw failure text are never labels.
- `fetchmark-crawl -once` is a bounded process rather than a Prometheus server.
  Its JSON output separates fixed-field `source_summary` and
  `admission_summary` counters. Link expansion adds only aggregate snapshot,
  reported/eligible/dropped candidate, applied-edge, created/reactivated URL,
  and budget-skip counts. It never emits source URLs, hosts, job IDs,
  validators, or failure text as metric labels. Operators that translate this
  output into metrics should preserve that bounded schema.

## Tests

Covered implicitly by handler/pipeline tests that assert counters
advance. No dedicated `obs_test.go` — the package is a dumb registry.
