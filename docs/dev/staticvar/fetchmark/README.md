# Fetchmark — docs

These notes mirror the package layout under `internal/`. They are
concise by design: high-level behaviour, invariants, and the mapping
from a concept to its code.

See [`openapi.yaml`](../../../openapi.yaml) for the API contract and
[`tasks.md`](../../../tasks.md) for in-flight work.

## Module map

| Area | Package | Doc |
|------|---------|-----|
| HTTP surface, middleware | `internal/api` | [api.md](api.md) |
| Dashboard | `internal/api/dashboard` | [dashboard.md](dashboard.md) |
| SearXNG client | `internal/adapters/searxng` | [searxng.md](searxng.md) |
| URL fetcher (pool, budgets) | `internal/adapters/fetcher` | [fetcher.md](fetcher.md) |
| Egress / SSRF policy | `internal/adapters/egress` | [egress.md](egress.md) |
| Extractor (trafilatura+md) | `internal/adapters/extractor` | [extractor.md](extractor.md) |
| Robots.txt cache | `internal/adapters/robots` | [robots.md](robots.md) |
| Cache (versioned, 2-layer) | `internal/adapters/cache` | [cache.md](cache.md) |
| Pipeline orchestration | `internal/core/pipeline` | [pipeline.md](pipeline.md) |
| BM25 re-rank | `internal/core/rank` | [rank.md](rank.md) |
| Prometheus metrics | `internal/obs` | [obs.md](obs.md) |

## Request flow (condensed)

```
 POST /v1/search
       │
       ▼
 APIKey  →  RateLimiter  →  handler
       │                      │
       │                      ▼
       │            pipeline.Search(opts)
       │                      │
       │            ┌─────────┼──────────┐
       │            ▼         ▼          ▼
       │        searxng   fetcher    extractor
       │         client   (bounded,  (trafilatura
       │                   SSRF-gated, + html→md)
       │                   cache-aware)
       │                      │
       │                      ▼
       │                    cache
       │                      │
       │                      ▼
       │                 BM25 rerank
       ▼
   JSON results
```

## Invariants to respect when making changes

- **Egress policy is the single choke point** for every outbound HTTP
  the fetcher performs; do not instantiate a raw `http.Client` that
  bypasses `egress.Policy`.
- **Admin-gated flags** (`respect_robots=false`, `proxy_url`) are
  enforced in `api.buildOptions`; do not reinterpret them in the
  pipeline.
- **Cache keys are versioned** (`cache.ExtractorVersion`); bump when
  the extractor output shape changes.
- **Label cardinality**: metrics in `internal/obs` deliberately keep
  labels bounded (outcome, engine, route-pattern). Do not introduce
  per-URL or per-API-key labels.
