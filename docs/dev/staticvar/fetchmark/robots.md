# robots

robots.txt fetch + cache + policy check. Admin-gated bypass lives in
`internal/api`, not here.

## Entry points

- `robots.go` — `New(client, ttl, maxSize)` returns a `Checker` with
  `Allowed(ctx, ua, url)` and richer `Evaluate` decision surfaces.

## Invariants

- Cache is keyed on origin (`scheme://host[:port]`), not full URL.
- Misses fetch `/robots.txt` via an egress-gated client. RFC 9309 treats 4xx as
  unavailable (access allowed), while network/5xx unreachable states require
  complete disallow. Failures are not cached, so a later request retries.
- Do not short-circuit this check in the pipeline. If the caller
  wants a bypass, they set `respect_robots=false` which is admin-gated
  at the API layer and arrives as `Options.RespectRobots = false`.

## Tests

- `robots_test.go` — allow/deny parsing, UA matching, cache TTL,
  fetch-error fallback behavior.
