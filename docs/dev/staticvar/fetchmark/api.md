# api

HTTP surface. Routes, request decoding, option-building, handler glue.

## Entry points

- `server.go` — `New(deps)` wires chi router with APIKey + rate-limit
  middleware. Health/ready/metrics are unauthenticated.
- `handlers.go` — `searchHandler`, `parseHandler`, `summarizeHandler`
  (501), plus `buildOptions` which is the single place request JSON
  becomes `pipeline.Options`.

## Invariants

- `buildOptions` is the only code path allowed to translate
  `respect_robots=false` and `proxy_url` from the request body into
  `pipeline.Options`. Both are admin-gated via `X-Admin-Key`; non-admin
  callers receive 403 instead of silent downgrades. The pipeline
  trusts `Options` — do not re-validate inside it.
- Response shape is frozen by `docs/openapi.yaml`. Break it and the
  dashboard + external clients break.

## Tests

- `server_test.go` — admin-override gating (table-driven),
  malformed-JSON 400, rate-limit 429, auth 401.
- `middleware/ratelimit_test.go` — in-memory and Redis allow/deny.
