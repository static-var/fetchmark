# dashboard

Static-asset dashboard mounted at `/dashboard`. Read-only health view
over the same `/healthz`, `/readyz`, and `/metrics` endpoints the API
already exposes.

## Entry points

- `dashboard.go` — `Handler()` returns an `http.Handler` that serves
  the embedded `web/` tree via `embed.FS`.

## Invariants

- No privileged data on this surface. It hits the public health
  endpoints, never admin-gated routes or Redis directly.
- Assets are embedded at build time; do not read from disk at runtime.
