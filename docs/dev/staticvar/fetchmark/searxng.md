# searxng

Optional SearXNG JSON-API discovery adapter with multi-instance failover.

## Entry points

- `client.go` — single-instance HTTP client (`Search(ctx, query)`) plus
  the richer `SearchBatch` contract.
- `multi.go` — `NewMultiWithCooldown(bases, httpc, cooldown)` wraps N
  clients with round-robin selection, distinct transport/quality circuits,
  EWMA quality, and bounded backoff. `NewMulti` keeps the 30s base.

## Invariants

- When `searxng` is enabled, at least one URL is required and cooldown must be
  > 0; non-positive adapter values clamp to the default while config fails fast
  at boot. When it is absent from `FM_DISCOVERY_ENABLED_SOURCES`, Fetchmark
  clears default URLs and does not construct or ping this adapter.
- Round-robin state is per-`MultiClient`; do not share across
  pipelines or you defeat the isolation.
- 403, 408, 429, 5xx, malformed responses, timeouts, and network errors open
  the transport circuit. Other 4xx responses may fall through but prove the
  instance reachable and clear an old transport failure streak.
- Backoff doubles after consecutive failures, is capped locally, and gets
  deterministic 0–20% jitter. A valid `Retry-After` delta or HTTP date extends
  the skip window (bounded to 24h). If every instance is cooling, Fetchmark
  returns `PoolCoolingError` with the earliest retry delay instead of bypassing
  the circuit and hammering upstreams.
- HTTP success does not imply healthy discovery. An empty response with
  `unresponsive_engines` is `degraded_empty` and the pool tries another
  instance. Empty responses without diagnostics are currently treated as
  `authoritative_empty`; this is a conservative operational policy, not a
  guarantee from SearXNG's undocumented JSON schema.
- A degraded-empty response keeps the instance transport-up. Provider
  degradation and transport health must not be collapsed into one signal.
- Two consecutive degraded-empty responses open a short response-quality
  circuit. A genuine authoritative empty is query-dependent evidence and never
  opens a shared instance circuit. Hits or authoritative empties reset the
  degraded streak. Instance and bounded per-engine EWMA gauges preserve gradual
  quality evidence independently of binary transport reachability.
- `Search` remains the compatibility port. New provider-aware orchestration
  should use `SearchBatch` and preserve its diagnostics.
- Explicit engine selection is the adapter-specific compatibility control. The
  discovery registry routes it here even when another provider is primary; if
  this adapter is disabled, the request fails as a typed unsupported control.
- Readiness pings this pool only when it is the configured primary. An enabled
  secondary pool remains opportunistic and cannot make the whole process
  unready.
- Failed attempts add normalized instance diagnostics. A subsequent empty is
  degraded rather than authoritative, while fallback hits are partial. A page
  2-or-later failure retains earlier hits with a page-level diagnostic; a
  first-page failure still follows normal pool failover.

## Tests

- `client_test.go` — protocol surface, diagnostics, response
  classification, engine-filter, timeout.
- `multi_test.go` — round-robin, transport and degraded-empty failover,
  Retry-After, exponential backoff, transport/quality circuits, cooldown
  expiry with real-time windows (50ms), and non-positive clamp.
