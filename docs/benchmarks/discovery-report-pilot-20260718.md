# Native discovery-report end-to-end pilot

Recorded 2026-07-18 on `darwin/arm64`. This is a six-query transport and
evidence pilot, not a relevance judgment or commercial-index parity claim.

## Configuration

- Current uncommitted worktree based on revision
  `18cdf18a3ecfdb0a6ea276a1ac4f72b373349ef4`.
- `FM_DISCOVERY_ENABLED_SOURCES=wiby,mwmbl,wikipedia,crossref`.
- `FM_DISCOVERY_PRIMARY_SOURCE=wiby`.
- SearXNG URLs empty; local corpus and summarizers disabled.
- Redis deliberately unavailable, exercising the bounded in-memory fallback.
- `fetchmark-eval -per-intent 1 -concurrency 1`, with a 75-second request
  timeout and 10-minute complete-run timeout.

The preserved raw artifact is
[`eval/baselines/native-open-discovery-report-pilot-20260718.jsonl`](../../eval/baselines/native-open-discovery-report-pilot-20260718.jsonl).
It is reproducible offline with:

```bash
go run ./cmd/fetchmark-eval \
  -records eval/baselines/native-open-discovery-report-pilot-20260718.jsonl
```

## Evidence result

| Measure | Result |
| --- | ---: |
| attempted / complete | 6 / true |
| HTTP success | 6/6 |
| discovery-report coverage | 6/6 (100%) |
| aggregate status | 5 healthy, 1 authoritative empty |
| non-empty | 5/6 (83.3%) |
| returned results | 33 |
| unique domains | 22 |
| extracted results | 18/33 (54.5%) |
| typed-provenance coverage | 33/33 (100%) |
| p50 / p95 latency | 6.620 s / 51.239 s |

Every record retained aggregate status plus ordered provider/lane/variant
outcomes. The empty long-tail case is now distinguishable as
`authoritative_empty`: both Wiby and Mwmbl returned authoritative empty lanes.
Other requests retained healthy and authoritative-empty lane combinations,
including Wiby, Mwmbl, Wikipedia, and Crossref. The reports contain only
bounded planner identities, counts, durations, statuses, and normalized
diagnostics; no provider instance address or raw error string is present.

This pilot did not encounter a provider diagnostic or HTTP search failure, so
the non-2xx preservation path remains proved by deterministic handler,
evaluator, and artifact-loader tests rather than this live observation. The
next evaluation gate remains the complete 120-query baseline with blind
relevance labels.
