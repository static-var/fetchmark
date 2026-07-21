# Native-open 120-query baseline

Recorded 2026-07-18 on `darwin/arm64`. These are complete fixed-suite live
runs for capacity and coverage evidence. They are not commercial-index parity
claims, and the serial run is not yet a relevance judgment because its blind
labels remain unfilled.

## Configuration

- Current uncommitted worktree based on revision
  `18cdf18a3ecfdb0a6ea276a1ac4f72b373349ef4`.
- `FM_DISCOVERY_ENABLED_SOURCES=wiby,mwmbl,wikipedia,crossref`.
- `FM_DISCOVERY_PRIMARY_SOURCE=wiby`.
- `FM_DISCOVERY_MAX_INFLIGHT` used its default aggregate budget of 16, divided
  into four independent in-flight cache slots per enabled provider.
- SearXNG URLs empty; local corpus and summarizers disabled.
- Redis deliberately unavailable, exercising the bounded in-memory fallback.
- The suite contains 120 stable cases: 20 each for general, fresh, long-tail,
  developer, research, and knowledge intent.
- The capacity run used evaluator concurrency 4 and a 75-second request
  timeout. The quality run restarted the server to clear process-local state,
  then used concurrency 1 and a 90-second request timeout.
- The remediated capacity run used the original concurrency-4 and 75-second
  evaluator budgets after cold-fill cancellation and low-rate lane headroom
  were corrected. Wiby and Mwmbl remained capped at one concurrent request,
  0.2 requests/second, and burst one.

The preserved artifacts are:

- [`eval/baselines/native-open-discovery-baseline-20260718.jsonl`](../../eval/baselines/native-open-discovery-baseline-20260718.jsonl)
  — concurrency-4 capacity run.
- [`eval/baselines/native-open-discovery-baseline-serial-20260718.jsonl`](../../eval/baselines/native-open-discovery-baseline-serial-20260718.jsonl)
  — serial quality run.
- [`eval/baselines/native-open-discovery-baseline-remediated-20260718.jsonl`](../../eval/baselines/native-open-discovery-baseline-remediated-20260718.jsonl)
  — concurrency-4 capacity run after remediation.
- [`eval/baselines/native-open-discovery-baseline-serial-label-template-20260718.jsonl`](../../eval/baselines/native-open-discovery-baseline-serial-label-template-20260718.jsonl)
  — 378-row blind template; every grade is still `null`.

All three run artifacts reproduce offline with `fetchmark-eval -records`.

## Controlled concurrency result

| Measure | original concurrency 4 | serial concurrency 1 | remediated concurrency 4 |
| --- | ---: | ---: | ---: |
| attempted / complete | 120 / true | 120 / true | 120 / true |
| HTTP success | 35/120 | 120/120 | 120/120 |
| errors | 85 | 0 | 0 |
| non-empty | 21/120 (17.5%) | 65/120 (54.2%) | 65/120 (54.2%) |
| returned results | 179 | 378 | 378 |
| unique domains | 27 | 200 | 169 |
| extracted results | 60/179 (33.5%) | 255/378 (67.5%) | 304/378 (80.4%) |
| typed-provenance coverage | 179/179 | 378/378 | 378/378 |
| discovery-report coverage | 118/120 | 120/120 | 120/120 |
| p50 / p95 latency | 8.002 s / 51.694 s | 5.069 s / 17.126 s | 19.611 s / 47.130 s |

The concurrency-4 run returned 83 HTTP 502 responses and two client
timeouts. Wiby and Mwmbl each recorded 94 failed lanes, dominated by 51
cancellations and 43 timeouts apiece. This is evidence that the conservative
shared-provider admission and eight-second lane budgets do not sustain four
simultaneous full-suite requests. It must not be used as a relevance baseline.

After a fresh process restart, the same suite completed serially without one
HTTP or provider diagnostic failure. The large difference is therefore a
controlled capacity observation, not evidence that the queries or providers
changed between runs.

The failure had two coupled mechanisms. Each unique cold cache miss started a
detached provider fill with a 30-second worker budget, while the waiting lane
stopped after eight seconds. Once its only caller left, that cold fill kept
holding or waiting for scarce provider admission. Orphans accumulated to the
four-slot Wiby and Mwmbl cache partitions (from the configured 16-flight
aggregate), after which new calls bypassed coalescing and timed out directly.
This explains the original split between 43 `timeout` and 51
`canceled` diagnostics per low-rate provider: the former were abandoned cold
waiters and the latter were direct overload calls.

Cold fills now stop when their final waiter leaves, while a shared fill remains
alive for other identical waiters and stale refreshes remain detached. The
default Wiby and Mwmbl lane budget is now 30 seconds, enough for bounded
admission queueing at concurrency four. Their upstream safeguards did not
change: one concurrent call, 0.2 calls/second, and burst one.

The complete remediated concurrency-4 run finished 120/120 requests without an
HTTP error or Wiby/Mwmbl failure diagnostic. One Crossref original lane had a
typed `network` diagnostic; its request remained a successful partial result.
The higher 19.611-second median is the explicit latency cost of queueing safely
instead of failing at eight seconds. Live source drift and the later Wikipedia
robots fix make domain/extraction counts unsuitable for causal quality claims;
the error and lane-diagnostic deltas are the capacity proof.

## Serial coverage

| Intent | Non-empty | Results | Authoritative empty |
| --- | ---: | ---: | ---: |
| general | 15/20 | 65 | 5 |
| fresh | 18/20 | 122 | 2 |
| long-tail | 3/20 | 4 | 17 |
| developer | 5/20 | 9 | 15 |
| research | 10/20 | 100 | 10 |
| knowledge | 14/20 | 78 | 6 |

Long-tail and developer coverage remain the clearest discovery gaps. Only
three cases matched their configured expected-domain proxies, producing five
expected-domain hits total; this proxy is intentionally narrow and is not a
replacement for judgments.

| Provider | Attributed results | Extracted | Unique domains |
| --- | ---: | ---: | ---: |
| Wiby | 140 | 128 | 106 |
| Mwmbl | 124 | 84 | 94 |
| Crossref | 90 | 43 | 1 |
| Wikipedia | 24 | 0 | 1 |

Provider counts measure contribution, not relevance. Wiby contributed the
most results and domains, but the earlier pilot already showed loosely related
general-web results. All 24 Wikipedia-attributed results were marked
`robots_disallowed`; Crossref's DOI results included 41 fetch failures, five
robots denials, and one non-HTML response. Those extraction outcomes require
separate policy/runtime diagnosis and must not be hidden inside discovery
coverage.

The Wikipedia outcome was diagnosed after preserving the baseline. Wikimedia's
live policy contains a complete-disallow group for the distinct product token
`Fetch`; the robots dependency's legacy prefix lookup incorrectly applied that
group to Fetchmark's `Fetchmark/0.1` identification. Fetchmark now selects
groups by the exact leading RFC 9309 product token while continuing to send its
full descriptive HTTP User-Agent. A fresh Wikipedia-only end-to-end request for
`Chelyabinsk meteor` returned HTTP 200 with three results; the top
`/wiki/Chelyabinsk_meteor` result contained extracted content rather than a
robots denial. The preserved baseline remains unchanged, so its zero Wikipedia
extractions still accurately describe the pre-fix run.

## Decision gate

The complete live baseline requirement is now met for this native-open
configuration. The preserved diagnostics prove the original concurrency-4
failure and its remediation; the serial artifact remains the only run bound to
the existing blind quality template. That template contains 378 rows across
the 65 non-empty cases, contains no provider/source/engine fields, and has no
filled grades.

No default-enable, source-weight, or provider-budget change follows from these
artifacts yet. Human 0–3 judgments and the resulting NDCG@10/MRR report remain
required before tuning relevance weights. The isolated Crossref network and
extraction findings remain independent follow-up work.
