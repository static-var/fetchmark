# Native GitHub repository developer pilot

Recorded 2026-07-19 on `darwin/arm64`. This is a complete serial run of the
20 fixed developer cases against the opt-in anonymous GitHub repository-search
lane alone. It measures repository discovery and Fetchmark extraction; it is
not a relevance judgment, general-web benchmark, or commercial-index parity
claim.

## Configuration and evidence

- Current uncommitted worktree based on revision
  `18cdf18a3ecfdb0a6ea276a1ac4f72b373349ef4`.
- `FM_DISCOVERY_ENABLED_SOURCES=github` and
  `FM_DISCOVERY_PRIMARY_SOURCE=github`.
- Only the `developer` pack was enabled. SearXNG, local persistence,
  summarizers, and GitHub authentication were disabled.
- Redis was deliberately unavailable, exercising the bounded in-memory
  fallback.
- `fetchmark-eval -intent developer -concurrency 1`, with a 60-second request
  timeout and 15-minute complete-run timeout.
- The provider retained its hard anonymous ceilings: one first-page request,
  20 candidates, one in-flight request, burst one, and 0.1 requests/second.
  This was one Fetchmark process; the limiter is process-local, so multiple
  replicas sharing an egress IP must divide the upstream anonymous allowance.

The preserved evidence is:

- [`eval/baselines/native-open-github-developer-20260719.jsonl`](../../eval/baselines/native-open-github-developer-20260719.jsonl)
  — 20-row live run, SHA-256
  `9e3a412bc31276f8329486dda9575db4fbb81a03fbc911015cb87d5b3001cf90`.
- [`eval/baselines/native-open-github-developer-configuration-20260719.json`](../../eval/baselines/native-open-github-developer-configuration-20260719.json)
  — exact bounded resolved configuration, SHA-256
  `06960683e800d3530cd46cb3223964087a0bc19653a736d3239711538357167d`.
- [`eval/baselines/native-open-github-developer-label-template-20260719.jsonl`](../../eval/baselines/native-open-github-developer-label-template-20260719.jsonl)
  — 186-row blind template, SHA-256
  `c98dc1bcc8931f439a693a125a7ce369f04b2b5a8ea47cc113717cea526150d3`.

Every observation identifies the same server executable SHA-256,
`6024cde3d66dbb3114c52c838abec21ab4688732f30b0a09298bedbf9eefe434`,
and configuration `native-github-developer-serial-v1`. Offline replay validates
the complete run. The blind template contains 186 `null` judgments and no
provider/source/engine fields.

## Result

| Measure | Result |
| --- | ---: |
| attempted / complete | 20 / true |
| HTTP success | 20/20 |
| non-empty | 20/20 (100%) |
| returned results | 186 |
| unique domains | 1 |
| extracted results | 186/186 (100%) |
| typed-provenance coverage | 186/186 (100%) |
| discovery-report coverage | 20/20 (100%) |
| healthy GitHub lanes | 20/20 |
| expected official-document domains | 0 |
| p50 / p95 latency | 10.212 s / 14.308 s |

All final URLs were `github.com` repositories. The provider supplied no
publication timestamp: the artifact's populated `published_at` values came
from subsequent GitHub page extraction and must not be interpreted as
repository activity, release freshness, or a publication-date guarantee.

The run closes isolated developer non-empty and extraction gaps for this
source, but not the quality gate. The expected-domain proxy remains zero
because the suite expects authoritative documentation hosts rather than code
repositories. Qualitative inspection found relevant Go error libraries and
Python asyncio examples alongside interview guides, tutorial collections, and
unrelated repositories. This is useful implementation/source discovery but
mixed direct-answer evidence.

## Post-pilot hardening boundary

The preserved executable fingerprint identifies the exact binary used for the
20-case run. After that run, two narrowly scoped safeguards were added without
rewriting its observations: caller-supplied uppercase Boolean operators are
removed alongside field qualifiers during lexical projection, and the copied
HTTP transport disables connection reuse and HTTP/2 so one provider rate token
cannot escape through a transparent replay. Deterministic red/green tests cover
both changes. The artifact therefore remains evidence for the recorded binary;
the post-pilot safeguards are proven by tests, not retroactively attributed to
that live run.

## Decision gate

GitHub remains disabled by default and neutral-weighted. The pilot proves the
anonymous no-key path, strict provider evidence, rate-safe serial execution,
and ordinary Fetchmark fetch/extraction path. It does not justify replacing
documentation providers, enabling the lane by default, or claiming long-tail
or general-web improvement.

The next quality decision requires blind 0–3 judgments and a same-binary fused
comparison against the existing open developer lanes. The adapter continues to
pin GitHub's official repository-search endpoint and current API version, make
no provider-side core-API or README-content requests, and treat detected
repository licenses as metadata rather than blanket reuse permission. Selected
public result pages still pass through Fetchmark's ordinary live fetch policy.
