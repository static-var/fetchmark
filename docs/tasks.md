# Fetchmark tasks

Live backlog. Each phase is tracked in the session SQL database with
dependencies; this file mirrors it for humans.

## In progress

_(none)_

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
- Review round 1 — GPT-5.4 security/perf fixes (`7f78d2a`): egress
  hop-by-hop revalidation, Accept-Encoding MIME sniff, token-bucket
  sweep, `crypto/rand` sampling, ctx-propagation through pipeline.

## Done (v2 R1)

- Q1 — Headless renderer adapter + pipeline render branch
  (explicit `render=true` and auto-upgrade on `js_required`).
  Separate cache key space so plain and rendered artifacts never
  shadow each other. [`f41eaee`]
- Q2 — Jaccard near-duplicate dedupe (3-gram word shingles, τ=0.85),
  run after the ranker so cluster winners use real BM25 scores.
  [`485307b`]
- Q3 — Multi-SearXNG failover with round-robin scheduling and 30s
  cooldown per instance; `fetchmark_searxng_instance_up` gauge.
  [`6497220`]
- Q4 — Cross-instance Redis stampede lock (CAS release via Lua) +
  per-URL cold-path rewrite that coalesces both intra-process
  (singleflight) and inter-process callers. [`c81f1fe`]
- Review round 2 — GPT-5.4 follow-up fixes (`7bb0f56`):
  - SSRF egress policy applied before renderer call.
  - Stampede-lock TTL/wait sized off `max(fetch, renderer)` timeout.
  - Rendered `js_required` artifacts no longer cached.
  - Ranker now runs before near-dup collapse.
- MIT license + user-facing README.

## Done (v2 R2)

- Q-a — Minimal per-package docs under `docs/dev/staticvar/fetchmark/`
  (api, dashboard, searxng, fetcher, egress, extractor, robots, cache,
  pipeline, rank, obs). Focus on invariants, not API reference.
- Q-b — Regression tests: fetcher exhausted-retry terminal error,
  `/v1/parse` admin-override table (non-admin proxy/robots → 403,
  admin → 200 + option propagation), Redis-backed ratelimit allow/deny
  + cross-key isolation. [`4d8fb8c`]
- Q-c — Configurable SearXNG cooldown via `FM_SEARXNG_COOLDOWN`
  (default 30s, `validate()` rejects ≤0). `NewMultiWithCooldown`
  constructor; non-positive runtime values clamp to default.
  [`89246fb`]
- Q-d — Second Redis stampede lock on `RenderedArtifactKey` in
  auto-render. TTL sized off a `Render=true` options clone because
  auto-render fires with `Render=false`. Post-lock re-apply clears
  `Title`/`Unsupported` so peer-populated rendered blobs propagate
  over js-required placeholders. [`2aec73c`]
- D-d — CJK-aware near-duplicate shingling: char bi-grams when the
  body is ≥30% CJK by non-whitespace/non-punct rune count, word
  3-grams otherwise. Uses `unicode.Is(Han|Hiragana|Katakana|Hangul)`.
  [`e6a2138`]

## v2-R3 — shipped

- R3-a — Summarizer Provider port with OpenAI (Chat Completions) and
  Anthropic (Messages) adapters using the official SDKs. Normalised
  Usage (prompt/completion/reasoning/total) and classified errors
  (auth/rate_limit/upstream/timeout/bad_request/network/cancelled) so
  the handler can map cleanly to HTTP statuses. Raw reasoning/thinking
  text is intentionally dropped — only token counts surface. [`f6ac633`]
- R3-b — Env-authoritative config with per-provider profiles
  (`FM_SUMMARIZE_{OPENAI,ANTHROPIC}_*`). [`9977399`]
- R3-c — Prometheus metrics:
  `fetchmark_summarize_outcome_total{provider,outcome}`,
  `fetchmark_summarize_duration_seconds{provider}`,
  `fetchmark_summarize_tokens_total{provider,class}`. [`cbb15a7`]
- R3-d — `/v1/summarize` wired: parses URL through the pipeline,
  validates non-empty content, delimits the page with `<page>…</page>`
  and instructs the model to treat it as untrusted. Admin JSON API at
  `/admin/summarize/{config,providers,providers/{name},default}` with
  merge-on-write semantics so operators don't retype API keys to tweak
  a model. Dashboard partial lists configured providers. [`b914670`]
- R3-e — Summarizer registry bootstrapped from env in main; a
  dedicated http.Client is used so the LLM path sidesteps the Fetcher's
  SSRF egress policy (trusted upstream). [`aab401a`]

## Declined / deferred with rationale

- Declined: keep Jaccard near-duplicate dedupe while FM_RESULTS_CAP <= 50;
  revisit SimHash/MinHash only if the cap rises or profiling shows dedupe
  latency matters.
- Deferred pending user demand: SSE streaming of search results;
  synchronous /v1/search remains the supported contract.
- Deferred: cross-instance summarizer config replication via Redis
  pub/sub. Env vars remain authoritative; admin overrides are
  in-process and revert on restart. Operators running multiple
  replicas should promote permanent changes to env and redeploy.
