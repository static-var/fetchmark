# api

HTTP surface. Routes, request decoding, option-building, handler glue.

## Entry points

- `server.go` — `New(deps)` wires chi router with APIKey + rate-limit
  middleware. Health/ready/metrics are unauthenticated.
- `handlers.go` — `searchHandler`, `parseHandler`, `summarizeHandler`
  plus `buildOptions` which is the single place request JSON becomes
  `pipeline.Options`.
- `summarize.go` — `/v1/summarize` parses one URL through the normal
  pipeline, validates usable content, and calls the configured OpenAI-
  or Anthropic-compatible provider. An empty registry returns
  `summarize_not_configured` (503).
- `corpus_admin.go` — admin-only curated URL admission and sticky
  curated/archive takedown endpoints. They expose a narrow `CorpusCurator`
  dependency rather than the ordinary `PipelineRunner` request surface.
- `evaluation_configuration.go` — validates and serves the bounded non-secret
  startup-resolved configuration manifest. Native search responses carry only
  its exact SHA-256 identity.

## Invariants

- `buildOptions` is the only code path allowed to translate
  `respect_robots=false` and `proxy_url` from the request body into
  `pipeline.Options`. Both are gated by admin API keys supplied through
  normal `Authorization: Bearer` or `X-API-Key` auth; non-admin callers
  receive 403 instead of silent downgrades. The pipeline trusts
  `Options` — do not re-validate inside it.
- Response shape is frozen by `docs/openapi.yaml`. Break it and the
  dashboard + external clients break.
- Native search results expose additive, deterministic
  `provider/lane/variant` provenance. Federation peer bindings are redacted to
  the generic `federation` tuple. Vendor compatibility translators use their
  own response structs and must not expose native metadata or provenance.
- Required source attribution is carried independently of vendor JSON. Wiby
  pages emit a `via` Link relation; arXiv pages emit its requested
  acknowledgment, while CC0 identity remains scoped to native result metadata;
  Stack Overflow pages emit `via` and CC BY-SA `license` relations. Downstream UIs remain
  responsible for visibly identifying Stack Overflow when displaying Stack
  Exchange API results. Response-level license relations never imply rights to
  unrelated fetched article bodies.
- Native `/v1/search` success and search-error envelopes expose a bounded
  `discovery` report with aggregate status plus deterministic lane outcomes.
  Lane provider/identity comes from the validated plan; raw adapter provider
  strings, instance addresses, and error text are never serialized. Diagnostic
  tokens, retry delays, lane counts, and list cardinalities are bounded.
  Discovery evidence describes URL discovery only and must not be interpreted
  as fetch, robots, extraction, or final relevance success. Tavily, Exa, and
  Brave response bodies intentionally omit this native object.
- Authenticated `GET /v1/evaluation/configuration` returns the exact manifest
  bytes used to compute `X-Fetchmark-Configuration-SHA256`. Invalid, oversized,
  or mismatched startup evidence fails closed with 503 and is never advertised
  on search responses. The manifest excludes secrets, endpoints, paths,
  contacts, User-Agent values, and raw private source bindings. Endpoint
  fingerprints first remove URL userinfo, query values, and fragments;
  credential/query and provider-identity shape is represented only by bounded
  counts or booleans, while non-secret routing and private-binding policy uses
  SHA-256 digests.
- Curated admission accepts only URLs, forces fresh policy-checked retrieval,
  and returns bodyless bounded outcomes. Its optional batch-level
  `safety_classification` is a validated operator assertion, not an inferred
  page label. Do not add raw content, proxy, renderer, user-agent, or
  robots-bypass controls to this surface.
- Focused crawler admission is a separate one-URL route. It accepts only the
  URL and validated allow/deny path policy, never content or a safety assertion.
  The fetcher must enforce same scheme/authority and deny-first decoded-path
  matching before every redirect request.
- Archive mode accepts takedowns through the same URL-only route but never
  accepts explicit admissions; permitted archive versions are fed only by the
  ordinary policy-checked retrieval path.
- Summarize metrics must match `internal/obs`: `fetchmark_summarize_total`,
  `fetchmark_summarize_duration_seconds`, and
  `fetchmark_summarize_tokens_total`.

## Tests

- `server_test.go` — admin-override gating (table-driven),
  malformed-JSON 400, rate-limit 429, auth 401.
- `summarize_test.go` — happy path, unsupported/empty content, override
  gates, provider caps, and admin summarize config mutations.
- `middleware/ratelimit_test.go` — in-memory and Redis allow/deny.
