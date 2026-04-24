# Fetchmark tasks

Live backlog. Each phase is tracked in the session SQL database with
dependencies; this file mirrors it for humans.

## In progress

_(none — v1 scope landed)_

## Done (v1)

- P0 — Repo skeleton & config; `/healthz`, `/readyz`, `/metrics`, API-key
  + request-ID middleware, Dockerfile, compose (bundled + external).
- P1 — SearXNG adapter + `/v1/search v0`.
- P2a — Egress / SSRF policy.
- P2 — Parallel fetch pipeline (resty, proxy, retry, UA pool).
- P2b — Fetch safety budgets (body/decompressed caps, MIME sniff,
  header timeout, per-host concurrency).
- P3 — Content extraction to Markdown / JSON / cleaned HTML.
- P3a — Versioned two-layer cache + `singleflight`.
- P3b — Exact content dedupe + JS-required detection.
- P4 — BM25 re-rank with engine-diversity bonus.
- P5 — `/v1/parse` endpoint.
- P6 — Per-API-key rate limits (Redis + local fallback).
- P6a — Admin-only overrides & dashboard basic-auth.
- P7 — Read-only ops dashboard (html/template + HTMX).
- P8 / P8a / P8b — Prometheus metrics, SearXNG engine-health,
  request-ID propagation.
- P9 — `/v1/summarize` stub (501) + OpenAPI spec.
- P10 — `docs/` mirror scaffolded.
- P11 — GHCR multi-arch release workflow.

## Queued

- **Singleflight wiring in the pipeline cold path.** `Cache.Do` is
  exported and tested but not used by `pipeline.process`. Concurrent
  requests for the same uncached URL currently all hit origin. Needs
  a restructure of the per-URL fetch/extract/store path onto a worker
  pool keyed by `cache.ArtifactKey`.
- **True shared-state Redis rate limiter** (Lua token bucket). Today
  the Redis leg is a fixed-window INCR tied to `burst`, which
  cross-instance behaves as ~`burst` rps, not `ratePerSec` rps. Local
  token bucket still enforces `ratePerSec` per replica — good enough
  for v1 but worth upgrading.
- Per-package docs under `docs/dev/staticvar/fetchmark/*.md` (only the
  index exists today).
- README quickstart with concrete `docker compose up` snippet.
- Regression tests for: exhausted-retry terminal error path,
  `/v1/parse` admin-only override (server_test currently only covers
  search), ratelimit Redis-backed allow/deny semantics.

## Deferred (v2)

- Headless rendering for JS-heavy pages
- Simhash/minhash near-duplicate
- Multi-SearXNG failover
- Cross-instance Redis stampede lock
- SSE streaming of results
- LLM wiring for `/v1/summarize`
