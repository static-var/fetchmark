# Native Stack Overflow developer pilot

Recorded 2026-07-18 on `darwin/arm64`. This is a complete serial run of the
20 fixed developer cases against the opt-in native Stack Exchange lane alone.
It measures discovery coverage and extraction behavior; it is not a relevance
judgment or a commercial-index parity claim.

## Configuration

- Current uncommitted worktree based on revision
  `18cdf18a3ecfdb0a6ea276a1ac4f72b373349ef4`.
- `FM_DISCOVERY_ENABLED_SOURCES=stackexchange` and
  `FM_DISCOVERY_PRIMARY_SOURCE=stackexchange`.
- Only the `developer` pack was enabled; SearXNG URLs, local corpus, and
  summarizers were disabled.
- Redis was deliberately unavailable, exercising the bounded in-memory
  fallback.
- `fetchmark-eval -intent developer -concurrency 1`, with a 75-second request
  timeout and 30-minute complete-run timeout.
- The provider retained its built-in one-call concurrency, 0.2-request/second,
  burst-one, ten-result, one-page, and anonymous no-key constraints.

The preserved evidence is:

- [`eval/baselines/native-open-stackexchange-developer-pilot-20260718.jsonl`](../../eval/baselines/native-open-stackexchange-developer-pilot-20260718.jsonl)
  — 20-row live run, SHA-256
  `31c9a68367192e1a052079abd354d76b165488d14dadca624c78cc05252433d8`.
- [`eval/baselines/native-open-stackexchange-developer-pilot-label-template-20260718.jsonl`](../../eval/baselines/native-open-stackexchange-developer-pilot-label-template-20260718.jsonl)
  — 200-row blind template, SHA-256
  `cdf34dd2c543888278b9100ab21d7bc33740bef138f3e69479ab6541265338f8`.

The template has 200 `null` grades and no provider/source/engine fields. The run
artifact reproduces offline with `fetchmark-eval -records` and contains neither
the evaluation key nor a local filesystem path.

The recorded revision is the clean base commit, not an identity for the
uncommitted implementation used by the live server. Repository history alone
therefore cannot reconstruct the exact live code state; the hashes above prove
artifact integrity, and offline reproduction means deterministic re-analysis
of those observations rather than recreation of the live run. Future dirty-tree
baselines need an explicit runtime fingerprint before they can claim exact
live-binary attribution. Fetchmark added executable response fingerprints after
this pilot; this preserved artifact predates that field and remains unchanged.

Independent review after the run hardened mandatory cooldown handling on
unreadable or malformed responses, duplicate admission ordering, redirect
pinning, native `week` validation, and provider timestamp validation. The
preserved artifact was not rewritten or presented as a post-hardening rerun; it
remains the exact observation produced by the earlier pilot binary.

## Result

| Measure | Result |
| --- | ---: |
| attempted / complete | 20 / true |
| HTTP success | 20/20 |
| non-empty | 20/20 (100%) |
| returned results | 200 |
| unique domains | 1 |
| extracted results | 7/200 (3.5%) |
| typed-provenance coverage | 200/200 (100%) |
| discovery-report coverage | 20/20 (100%) |
| healthy provider lanes | 20/20 |
| p50 / p95 latency | 4.952 s / 5.936 s |

Every case produced ten synthesized `stackoverflow.com/questions/{id}` URLs,
and every lane completed as `healthy` without a provider diagnostic. All 193
non-extracted observations were classified `fetch_failed`; this artifact does
not retain a narrower transport reason, so the failure must not be attributed
to robots policy, rate limiting, or another cause without separate evidence.

The 100% non-empty result materially closes the earlier native-open baseline's
5/20 developer discovery gap for this isolated source. It does not establish
relevance. A qualitative top-result inspection shows strong topical matches for
Go error wrapping, `AbortController`, BuildKit, Kubernetes probes, GitHub
Actions secrets, and several other cases, but clear lexical drift for Rust
`Pin`, React hydration, Git partial clone, PostgreSQL `SKIP LOCKED`, WASI, and
others. Some extracted pages were themselves off-topic.

## Pilot decision gate

At the pilot boundary, Stack Overflow was useful as an opt-in URL-discovery
complement, but the pilot
does not justify default enablement or a fusion-weight change. The reasons are
the unfilled blind relevance judgments, one-domain diversity, mixed observed
ranking quality, anonymous shared-IP quota, and 3.5% extraction success.

Before tuning the lane, complete the blind 0–3 labels and compare a combined
native-open-plus-StackExchange developer run. The adapter continues to use the official
[`/similar`](https://api.stackexchange.com/docs/similar) method, obey
[`backoff`](https://api.stackexchange.com/docs/throttle), and carry the source
and licensing relations required for downstream visible attribution.

## Post-pilot fetch diagnosis and remediation

The preserved pilot above remains unchanged. A separate live reproduction on
2026-07-18 isolated its page-fetch failures before extraction: Stack Overflow
returned HTTP 403 and a Cloudflare `Just a moment...` challenge document to
Fetchmark's Go HTTP client, while robots evaluation allowed the URL. Fetchmark
does not impersonate a browser, solve the challenge, or route around it.

The native adapter now requests `filter=withbody` from the same official,
bounded `/similar` call. It exposes a body only when the row also carries a
`content_license`; the pipeline extracts that transient question HTML without
fetching the public page and never writes it to the local corpus or artifact
store. Missing or unlicensed bodies remain discovery-only failures and never
fall through to the challenged page. Provider extraction shares Fetchmark's
process-wide artifact worker limit. A one-query end-to-end smoke returned nine
healthy candidates and the requested top three all had extracted main text and
Markdown. Metrics recorded
nine successful extractions, zero robots blocks, and no page-fetch outcome.
Native metadata retains the exact license and owner; compatibility responses
add whitelisted exact-license and author Link relations, and Exa-compatible
content carries the owner name.
This closes the identified transport/extraction mechanism for API-supplied
question bodies, but it is not a replacement for relevance judgments.

## Provider-content rerun

A separate complete serial rerun on 2026-07-18 used the remediated provider-
content path against the same 20 fixed developer cases. It retained the same
anonymous provider limits and isolated Stack Exchange configuration, required
one executable fingerprint across every response, and wrote a new artifact
rather than modifying the pilot above.

The preserved evidence is:

- [`eval/baselines/native-open-stackexchange-provider-content-developer-20260718.jsonl`](../../eval/baselines/native-open-stackexchange-provider-content-developer-20260718.jsonl)
  — 20-row live run, SHA-256
  `8c0f49725d39486cc2593546ed305a15b266c3ee401b564e157cb9f6950e6741`.
- [`eval/baselines/native-open-stackexchange-provider-content-developer-label-template-20260718.jsonl`](../../eval/baselines/native-open-stackexchange-provider-content-developer-label-template-20260718.jsonl)
  — 200-row blind template, SHA-256
  `abf8e6317ae2f0fd05c244dfbb593ef85ee058b02d98033dc6ced0bffc3d029c`.

Every observation identifies the same server executable SHA-256,
`b51f5a26ebff1cb530687ea5ec698dfdea5a0bfbd1f6888f2cacde92a3f1d4c6`,
and configuration `native-stackexchange-provider-content-serial-v2`. Both the
run and the original pilot reproduce offline with `fetchmark-eval -records`.

| Measure | pre-fix pilot | provider-content rerun |
| --- | ---: | ---: |
| attempted / complete | 20 / true | 20 / true |
| HTTP success | 20/20 | 20/20 |
| non-empty | 20/20 | 20/20 |
| returned results | 200 | 200 |
| extracted results | 7/200 (3.5%) | 190/200 (95%) |
| typed-provenance coverage | 200/200 | 200/200 |
| healthy provider lanes | 20/20 | 20/20 |
| p50 / p95 latency | 4.952 s / 5.936 s | 4.939 s / 5.299 s |

The isolated server's final metrics recorded 190 successful and ten failed
extractions, zero robots blocks, a successful-fetch histogram count of zero,
and no `fetchmark_fetch_outcome_total` series. The ten remaining result
failures are classified `extract_failed`; the run does not justify assigning a
narrower cause. This evidence proves the rerun did not fall through to public
Stack Overflow result-page fetches while closing the observed extraction gap
for licensed API-supplied bodies.

The new blind template contains 200 null grades and no provider identity. The
rerun therefore changes the extraction conclusion, not the relevance or
default-enable decision. Stack Exchange remains opt-in and neutral-weighted
until those judgments and a combined native-open-plus-StackExchange developer
comparison establish its relevance contribution and ranking tradeoffs.

## Combined native-open comparison

A third fingerprinted serial run enabled `wiby,mwmbl,stackexchange`, used Wiby
as the primary source, and selected the `general-open,developer` packs. SearXNG,
the local corpus, and summarizers remained disabled. The embedded pack routes
Wiby and Mwmbl through the always-on general lane and Stack Exchange through
the developer lane, so this run measures their actual top-ten fusion result
without changing a source weight.

The preserved evidence is:

- [`eval/baselines/native-open-plus-stackexchange-developer-20260718.jsonl`](../../eval/baselines/native-open-plus-stackexchange-developer-20260718.jsonl)
  — 20-row live run, SHA-256
  `36cb25ec7c2109e1e5d91d2ab50cd5834e10eed1abe0e84a83f91e84c14ec3cd`.
- [`eval/baselines/native-open-plus-stackexchange-developer-label-template-20260718.jsonl`](../../eval/baselines/native-open-plus-stackexchange-developer-label-template-20260718.jsonl)
  — 200-row blind template, SHA-256
  `c97f616eb2eb2bb096a81915ceef61483cd807e95922ff32d1f7a6b53d078f6e`.

All observations carry the same executable SHA-256 as the isolated rerun and
configuration `native-open-wiby-mwmbl-stackexchange-developer-serial-v1`.
Offline replay validates the complete run.

| Measure | isolated Stack Exchange | combined native-open |
| --- | ---: | ---: |
| HTTP success / non-empty | 20/20 | 20/20 |
| returned results | 200 | 200 |
| unique domains | 1 | 8 |
| extracted results | 190/200 (95%) | 190/200 (95%) |
| expected-domain hits | 0 | 1 |
| p50 / p95 latency | 4.939 s / 5.299 s | 4.858 s / 8.050 s |

| Final-result provider | Results | Unique domains |
| --- | ---: | ---: |
| Stack Exchange | 193 | 1 |
| Wiby | 5 | 5 |
| Mwmbl | 2 | 2 |

Every aggregate discovery report was healthy. Stack Exchange was healthy for
20/20 lanes; Mwmbl was healthy for three and authoritative-empty for 17; Wiby
was healthy for two and authoritative-empty for 18. The seven final open-web
results all extracted. Whole-run metrics, which include candidates removed
before the final top ten, recorded seven successful public fetches, one fetch
error, one robots block, 197 successful extractions, and ten extraction errors.

A non-blind inspection found directly topical Mwmbl results for Jetpack Compose
nested scrolling and BuildKit cache mounts plus a topical Wiby SQLite WAL
result. Four Wiby results inserted for the Go error-wrapping query were visibly
unrelated. This is mixed qualitative evidence, not a scored relevance result,
and live source drift prevents treating the two runs as a controlled causal
experiment.

The combined template has 200 null grades and omits provider identity. The run
observed seven final open-web results, eight domains, and a higher p95 than the
isolated run, but it does not justify a weight or default change. The next
decision gate is complete blind grading of both current templates followed by
NDCG@10, MRR, and source-contribution comparison.
