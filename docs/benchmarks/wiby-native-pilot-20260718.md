# Native Wiby end-to-end pilot

Recorded 2026-07-18 on `darwin/arm64`. This is a deterministic 12-query
coverage pilot plus a one-query compatibility smoke, not a relevance judgment
or a commercial-index parity claim.

## Configuration

- Current uncommitted worktree based on revision
  `18cdf18a3ecfdb0a6ea276a1ac4f72b373349ef4`.
- `FM_DISCOVERY_ENABLED_SOURCES=wiby,mwmbl,wikipedia,crossref`
- `FM_DISCOVERY_PRIMARY_SOURCE=wiby`
- SearXNG URLs empty; local corpus and summarizers disabled.
- Redis deliberately unavailable, exercising the bounded in-memory fallback.
- `fetchmark-eval -per-intent 2 -concurrency 1`, with a 75-second request
  timeout and 12-minute complete-run timeout.

The preserved raw artifact is
[`eval/baselines/native-open-wiby-pilot-20260718-serial.jsonl`](../../eval/baselines/native-open-wiby-pilot-20260718-serial.jsonl).
It is reproducible offline with:

```bash
go run ./cmd/fetchmark-eval \
  -records eval/baselines/native-open-wiby-pilot-20260718-serial.jsonl
```

## Runtime and attribution smoke

A basic native request for `personal homepages handmade web directories` with
moderate safe search returned HTTP 200 and one extracted Wiby result. The
result carried typed provenance:

```json
{"provider":"wiby","lane":"wiby-general","variant":"original"}
```

The native response also carried
`Link: <https://wiby.me/>; rel="via"; title="Wiby"`. Repeating the query through
the Tavily compatibility route returned its established vendor JSON keys and
the same header; no native provenance or Wiby-specific field was added to the
vendor body. Unit contract tests cover the same success behavior and prove
that native, Tavily, Exa, and Brave 507 responses omit the header.

## Balanced pilot

| Measure | Result |
| --- | ---: |
| attempted / complete | 12 / true |
| HTTP success | 12/12 |
| non-empty | 7/12 (58.3%) |
| returned results | 44 |
| unique domains | 33 |
| extracted results | 27/44 (61.4%) |
| typed-provenance coverage | 44/44 (100%) |
| p50 / p95 latency | 4.359 s / 45.232 s |

Non-empty coverage was 2/2 fresh, 2/2 knowledge, and 1/2 each for general,
developer, and research. Both long-tail cases remained empty. All 12 requests
returned HTTP 200; as with the earlier pilot, the artifact does not preserve
broker batch status or provider diagnostics, so empty cases cannot be
classified more narrowly.

| Provider | Results | Unique domains |
| --- | ---: | ---: |
| Wiby | 14 | 13 |
| Mwmbl | 19 | 18 |
| Crossref | 10 | 1 |
| Wikipedia | 1 | 1 |

Twelve of Wiby's 14 returned results were extracted successfully. Wiby alone
supplied the non-empty general case and the non-empty developer case in this
run. It did not contribute to either long-tail case.

## Comparison and decision

The earlier serial native-open pilot without Wiby recorded 11/12 HTTP
successes, 5/12 non-empty cases, 32 results from 21 domains, and 16/32
extractions. This Wiby-enabled run observed 12/12, 7/12, 44 results from 33
domains, and 27/44 extractions. The runs use the same fixed case selector but
depend on live providers and pages, so the difference is evidence of useful
incremental coverage in this observation, not proof that Wiby caused every
change.

No default-enable or fusion-weight change follows from this artifact. Several
Wiby results in the newly covered general case were only loosely related to
the query, and no blind human labels exist yet. The next evidence gate remains
the complete baseline with preserved broker diagnostics and relevance labels;
the long-tail gap also remains open.
