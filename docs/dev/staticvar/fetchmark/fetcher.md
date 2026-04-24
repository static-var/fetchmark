# fetcher

Bounded-concurrency URL fetcher with retries, per-host budgeting, and
egress gating.

## Entry points

- `fetcher.go` — `New(pool, egressPolicy, ua, ...)` builds a Fetcher.
- `Fetch(ctx, url, opts)` — single-shot; respects `opts.Proxy` and
  returns `(bytes, finalURL, contentType, err)`.

## Invariants

- **Every outbound HTTP flows through `egress.Policy`**. No naked
  `http.Client` or `http.DefaultTransport`. SSRF protection is here;
  do not route around it.
- Retries are bounded (default 2 → 3 total attempts). Terminal error
  format: `"upstream returned %d after %d retries"`.
- Per-host semaphore is sized at build time; breaking it breaks the
  politeness contract operators rely on.
- `opts.Proxy` is a per-request override of `egress.Policy.Proxy`; the
  policy's allowlist still applies.

## Tests

- `fetcher_test.go` — success, 4xx terminal, retry exhaustion,
  proxy-url routing, SSRF rejection at the egress layer.
