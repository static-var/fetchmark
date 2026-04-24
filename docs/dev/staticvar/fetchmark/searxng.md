# searxng

SearXNG JSON-API client with multi-instance failover.

## Entry points

- `client.go` — single-instance HTTP client (`Search(ctx, query,
  opts)`).
- `multi.go` — `NewMultiWithCooldown(bases, httpc, cooldown)` wraps N
  clients with round-robin selection and a per-instance cooldown on
  failure. `NewMulti` keeps the 30s default.

## Invariants

- Cooldown must be > 0; non-positive values clamp to the default.
  `config.validate()` rejects `FM_SEARXNG_COOLDOWN <= 0` so we fail
  fast at boot instead of surprising operators at request time.
- Round-robin state is per-`MultiClient`; do not share across
  pipelines or you defeat the isolation.
- Treat HTTP 4xx from SearXNG as terminal (bad query) — only 5xx and
  network errors trigger failover + cooldown.

## Tests

- `client_test.go` — protocol surface, engine-filter, timeout.
- `multi_test.go` — round-robin, failover, cooldown expiry with
  real-time windows (50ms), non-positive clamp.
