# Fetchmark tasks

Live backlog. Each phase is tracked in the session SQL database with
dependencies; this file mirrors it for humans.

## In progress

- **P0 — Repo skeleton & config**: chi router, slog, env config,
  `/healthz`, `/readyz`, `/metrics`, Dockerfile, docker-compose (bundled +
  external), API-key + request-ID middleware.

## Queued

- P1 — SearXNG adapter + `/v1/search v0`
- P2a — Egress / SSRF policy module
- P2 — Parallel fetch pipeline (resty, proxy, retry, UA)
- P2b — Fetch safety budgets
- P3 — Content extraction (Markdown / JSON / cleaned HTML)
- P3a — Versioned cache + `singleflight`
- P3b — Content dedupe + JS-required detection
- P4 — Hand-rolled BM25 re-rank + engine-diversity bonus
- P5 — `/v1/parse` endpoint
- P6 — API-key rate limits
- P6a — Admin-only overrides & dashboard basic-auth
- P8 — Prometheus metrics
- P8a — SearXNG engine-health metrics
- P8b — Request-ID tracing hooks
- P7 — Ops dashboard (templ + HTMX)
- P9 — `/v1/summarize` stub + OpenAPI
- P10 — `docs/` mirror + this file
- P11 — GHCR multi-arch release

## Deferred (v2)

- Headless rendering for JS-heavy pages
- Simhash/minhash near-duplicate
- Multi-SearXNG failover
- Cross-instance Redis stampede lock
- SSE streaming of results
- LLM wiring for `/v1/summarize`
