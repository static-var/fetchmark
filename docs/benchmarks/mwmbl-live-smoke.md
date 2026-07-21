# Native Mwmbl end-to-end smoke

Recorded 2026-07-18 on `darwin/arm64`. This is a one-query contract and runtime
probe, not a relevance baseline, load test, or parity claim.

## Configuration

- Fetchmark built from the current worktree and bound to loopback only.
- `FM_DISCOVERY_ENABLED_SOURCES=mwmbl`
- `FM_DISCOVERY_PRIMARY_SOURCE=mwmbl`
- SearXNG URLs empty; local corpus disabled; Redis deliberately unavailable so
  the normal bounded in-memory fallback was exercised.
- Native Mwmbl defaults: fixed official no-key endpoint, one concurrent
  request, 0.2 requests/second, 20 discovery results before Fetchmark's
  requested cap and live retrieval.
- Query: `independent open web search`, basic depth, three requested results.

## Result

The first run exposed a typed-nil startup wiring defect: disabled local
persistence had boxed `(*bleveindex.Index)(nil)` into the local-search
interface, so broker fanout panicked. After binding local writer/searcher and
artifact interfaces only when their concrete stores exist, a red-to-green
unit regression and the repeated live probe passed.

The repeated request returned HTTP 200 with three results in 6.95 seconds. All
three were discovered through engine `mwmbl`, fetched and extracted through
Fetchmark's ordinary SSRF/robots-aware path, and carried typed provenance:

```json
{"provider":"mwmbl","lane":"mwmbl-general","variant":"original"}
```

The response was 98,041 bytes because the native default response includes
extracted representations. The temporary server then shut down cleanly.

## Interpretation

This proves that a CPU-only process can boot and serve a real search with no
SearXNG process, paid search API, local index, or external cache. It does not
establish representative Mwmbl relevance or coverage: the three returned
documents included two closely related GitHub repositories, so the fixed
120-query evaluator and human labels remain the gate before enabling Mwmbl by
default or tuning its neutral fusion weight.
