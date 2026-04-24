# egress

The single choke point for outbound HTTP. SSRF filter, allow/deny
lists, proxy configuration, TLS policy.

## Entry points

- `policy.go` — `Policy{Proxy, Denylist, Allowlist}` and
  `Policy.Client()` which returns an `*http.Client` with the resolver
  pre-wired to reject private/loopback/link-local.
- `resolver.go` — DNS resolution that rejects RFC1918, 127/8, ::1,
  fe80::/10, and the metadata IPs (169.254.169.254, fd00:ec2::254).

## Invariants

- **Private-range rejection happens on the resolved IP, not the
  hostname**. A public hostname that resolves to RFC1918 must fail.
- `Policy.Client()` is the only constructor allowed for outbound HTTP
  in the fetcher and renderer paths.
- `Denylist` is host-based; `Allowlist` is an allow-override for
  operators who intentionally run against a private SearXNG over a
  known-safe network.

## Tests

- `policy_test.go` — loopback reject, metadata IP reject, CNAME
  chase, allowlist override, proxy-url honored.
