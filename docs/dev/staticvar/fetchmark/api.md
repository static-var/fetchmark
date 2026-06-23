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

## Invariants

- `buildOptions` is the only code path allowed to translate
  `respect_robots=false` and `proxy_url` from the request body into
  `pipeline.Options`. Both are gated by admin API keys supplied through
  normal `Authorization: Bearer` or `X-API-Key` auth; non-admin callers
  receive 403 instead of silent downgrades. The pipeline trusts
  `Options` — do not re-validate inside it.
- Response shape is frozen by `docs/openapi.yaml`. Break it and the
  dashboard + external clients break.
- Summarize metrics must match `internal/obs`: `fetchmark_summarize_total`,
  `fetchmark_summarize_duration_seconds`, and
  `fetchmark_summarize_tokens_total`.

## Tests

- `server_test.go` — admin-override gating (table-driven),
  malformed-JSON 400, rate-limit 429, auth 401.
- `summarize_test.go` — happy path, unsupported/empty content, override
  gates, provider caps, and admin summarize config mutations.
- `middleware/ratelimit_test.go` — in-memory and Redis allow/deny.
