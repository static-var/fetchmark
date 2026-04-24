# obs

Prometheus metrics. One package, one registry, bounded label cardinality.

## Entry points

- `metrics.go` — exported `prometheus.Counter` / `Histogram` vars plus
  `Register(reg)` which installs them on a caller-supplied registry.
  `internal/api` wires the default registry on `/metrics`.

## Invariants

- **Label cardinality is deliberately small**: outcome, engine,
  route-pattern, cache-layer. **Do not add per-URL, per-API-key, or
  per-host labels** — a Prometheus scrape with 10k distinct URLs will
  melt the server. If you need per-host insight, sample into logs.
- Route labels are chi's route pattern (`/v1/parse`), not the raw
  request URL. The middleware in `internal/api/middleware` handles
  this; do not re-derive it elsewhere.
- Histogram buckets are tuned for 10ms–30s fetch budgets; changing
  them is a breaking change for dashboards.

## Tests

Covered implicitly by handler/pipeline tests that assert counters
advance. No dedicated `obs_test.go` — the package is a dumb registry.
