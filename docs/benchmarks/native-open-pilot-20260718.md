# Native-open representative pilot

Recorded 2026-07-18 on `darwin/arm64`. This is a deterministic 12-query pilot,
not a commercial-index parity claim or a substitute for the complete 120-query
baseline and human relevance labels.

## Configuration

- Current uncommitted worktree based on revision
  `18cdf18a3ecfdb0a6ea276a1ac4f72b373349ef4`.
- `FM_DISCOVERY_ENABLED_SOURCES=mwmbl,wikipedia,crossref`
- `FM_DISCOVERY_PRIMARY_SOURCE=mwmbl`
- SearXNG URLs empty; local corpus and summarizers disabled.
- Redis deliberately unavailable, exercising the bounded in-memory fallback.
- `fetchmark-eval -per-intent 2 -concurrency 1`, selecting the first two
  validated cases from each of the six intents while preserving suite order.
- Evaluator request timeout 75 seconds and complete-run timeout 12 minutes.

The preserved raw artifact is
[`eval/baselines/native-open-pilot-20260718-serial.jsonl`](../../eval/baselines/native-open-pilot-20260718-serial.jsonl).
It is loadable with:

```bash
go run ./cmd/fetchmark-eval \
  -records eval/baselines/native-open-pilot-20260718-serial.jsonl
```

## Results

| Measure | Result |
| --- | ---: |
| attempted / complete | 12 / true |
| HTTP success | 11/12 |
| non-empty | 5/12 (41.7%) |
| returned results | 32 |
| unique domains | 21 |
| extracted results | 16/32 (50.0%) |
| typed-provenance coverage | 32/32 (100%) |
| p50 / p95 latency | 5.018 s / 39.573 s |

Non-empty coverage was 2/2 fresh, 2/2 knowledge, and 1/2 research. Both
general and both long-tail cases returned HTTP 200 with zero results. Developer
returned one HTTP 200 empty and one HTTP 502 after 8.001 seconds. The native
API artifact does not preserve the broker's authoritative/degraded-empty status
or provider diagnostics, so those outcomes cannot be classified more narrowly
from this evidence.

| Provider | Results | Unique domains | Extracted | Robots-disallowed | Fetch-failed |
| --- | ---: | ---: | ---: | ---: | ---: |
| Mwmbl | 20 | 19 | 16 | 2 | 2 |
| Crossref | 10 | 1 | 0 | 1 | 9 |
| Wikipedia | 2 | 1 | 0 | 2 | 0 |

Crossref results use DOI resolver URLs, so nine live retrievals failed and one
was robots-disallowed even though discovery metadata remained usable. The two
Wikipedia results were also retained as discovery metadata after robots policy
blocked page retrieval. These are extraction outcomes, not provenance gaps.

## Contention finding

A preceding authenticated diagnostic used evaluator concurrency 2 against the
same process. It succeeded for only 6/12 requests, also returned 32 results but
from four domains, and extracted one result. Six requests failed with HTTP 502
after approximately eight seconds. Its raw artifact is preserved as
[`eval/baselines/native-open-pilot-20260718-concurrency2.jsonl`](../../eval/baselines/native-open-pilot-20260718-concurrency2.jsonl).

This comparison is consistent with top-level requests competing for Mwmbl's
deliberate one-request/five-second process budget, but the API artifact lacks
the provider diagnostics needed to prove that each 502 came from that queue.

The serial artifact is therefore the comparable pilot configuration for this
source set. The concurrent diagnostic is an orchestration warning and a reason
to preserve provider scheduling diagnostics in future evaluation artifacts.

### Admission-gate follow-up

The initial provider gate spent a rate token before acquiring a concurrency
slot. A queued request could therefore reserve a future rate slot, continue
waiting for concurrency, and later bunch with another upstream request. The
shared gate now acquires provider concurrency first and spends rate only at the
upstream-call boundary. A deterministic unit regression verifies that a call
whose concurrency wait expires does not consume the next rate token.

Repeating the same concurrency-2 pilot after that change produced 8/12 HTTP
successes, 32 results from 12 unique domains, and 7/32 extractions. Four HTTP
502 responses near eight seconds remain. The follow-up artifact is
[`eval/baselines/native-open-gated-pilot-20260718-concurrency2.jsonl`](../../eval/baselines/native-open-gated-pilot-20260718-concurrency2.jsonl).

This is an observed improvement over the preceding 6/12, four-domain,
one-extraction run, not proof that scheduling alone caused every difference:
both artifacts depend on live providers and pages. It does establish that the
gate no longer wastes rate capacity before concurrency admission. Remaining
502s still require preserved queue/provider diagnostics or another eligible
lane; simply enlarging source deadlines would hide the bottleneck.

## Interpretation and next gate

The run proves that the SearX-free broker can return diverse, fully attributed
web results and extract half of them without a paid API, local model, or local
index. It also quantifies the current gaps: no coverage in this general,
long-tail, or developer slice; weak research relevance from unrefined Crossref
queries; expensive DOI retrieval; and shared-budget contention under parallel
requests.

Do not tune fusion weights from 12 unlabeled queries. The next evidence-driven
slice should add another independent general/developer discovery provider or
fix shared-provider scheduling first, then repeat the same artifact with blind
relevance labels before changing default weights.
