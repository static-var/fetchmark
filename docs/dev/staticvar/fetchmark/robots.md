# robots

robots.txt fetch + cache + policy check. Admin-gated bypass lives in
`internal/api`, not here.

## Entry points

- `robots.go` — `New(fetcher, ttl)` returns a `Cache` with
  `Allowed(ctx, url, ua) (bool, error)`.

## Invariants

- Cache is keyed on origin (`scheme://host[:port]`), not full URL.
- Misses fetch `/robots.txt` via the egress-gated fetcher; a
  404/403/5xx from robots.txt is treated as "allow" (standard policy).
- Do not short-circuit this check in the pipeline. If the caller
  wants a bypass, they set `respect_robots=false` which is admin-gated
  at the API layer and arrives as `Options.RespectRobots = false`.

## Tests

- `robots_test.go` — allow/deny parsing, UA matching, cache TTL,
  fetch-error fallback behavior.
