# dashboard

Static-asset dashboard mounted at `/dashboard`. Read-only ops view over
the same `/healthz`, `/readyz`, and `/metrics` data the API already
exposes, plus redacted runtime config and summarize provider names.

## Entry points

- `dashboard.go` — `Handler()` returns an `http.Handler` that serves
  the embedded `web/` tree via `embed.FS`.

## Invariants

- No privileged data on this surface. It uses public health/metrics data
  and redacted config only; it never calls admin-gated routes or Redis
  directly.
- Assets are embedded at build time; do not read from disk at runtime.
