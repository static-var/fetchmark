# Fetchmark evaluation

`queries.jsonl` is the fixed Phase 0 discovery baseline. It contains 120 cases,
with 20 each for general web, freshness, obscure long-tail, developer,
research, and knowledge intent.

Validate the corpus without network access:

```bash
make eval-check
```

Live evaluation is always explicit and writes raw JSONL to a new file:

```bash
FM_EVAL_API_KEY="$FM_API_KEY" go run ./cmd/fetchmark-eval \
  -live \
  -endpoint http://127.0.0.1:8080/v1/search \
  -revision "$(git rev-parse HEAD)" \
  -configuration-id general-open-v1 \
  -require-build-sha256 \
  -configuration-manifest-output fetchmark-eval-configuration-$(date -u +%Y%m%dT%H%M%SZ).json \
  -require-configuration-sha256 \
  -output fetchmark-eval-$(date -u +%Y%m%dT%H%M%SZ).jsonl
```

The evaluator refuses an existing output path. Use `-per-intent N` for a
representative pilot that selects the first N cases from each intent while
preserving suite order. Use `-intent developer` to run all cases in one
canonical intent, or `-limit N` only for a prefix smoke run. These three
selectors are mutually exclusive. Use `-concurrency` to set the bounded worker
count, and `-request-timeout` or `-run-timeout` for explicit budgets.
Authentication is read only from
`FM_EVAL_API_KEY`; it is never written into records.

`-revision` and `-configuration-id` are optional for short smoke runs, but
should always be supplied for comparison baselines. Baselines should also use
`-require-build-sha256`. Fetchmark hashes the executable file resolved for its
process at startup and returns the canonical digest on native search responses
in `X-Fetchmark-Build-SHA256`; the evaluator copies it into every HTTP
observation and the deterministic summary. A run is incomplete when the
required identity is missing, malformed, changes, or is present on only some
responses. Requests that receive no HTTP response retain no build identity and
do not create false mixed-version evidence. The digest identifies the resolved
executable artifact, including a dirty-tree build, but is not a hash of the
mapped process image, runtime configuration, or source tree. It therefore
cannot make the source tree reconstructable.
`configuration-id` remains a separate operator-supplied comparison boundary.
Identifiers are bounded and cannot contain whitespace.

For reproducible baselines, `-configuration-manifest-output` fetches the exact
authenticated v1 manifest from `/v1/evaluation/configuration` before sending
any search request. Both output paths use exclusive creation. Fetchmark hashes
the exact manifest bytes and emits that lowercase SHA-256 in
`X-Fetchmark-Configuration-SHA256` on native search responses. The evaluator
binds every HTTP observation to the fetched digest and rejects missing,
malformed, mixed, partially present, or unexpected values. A failed manifest
fetch removes both newly created empty outputs; it never overwrites an existing
file. `-require-configuration-sha256` can enforce the response identity without
saving the manifest, but saving it is the stronger baseline workflow.

The manifest contains resolved source packs, source weights and budgets,
retrieval/cache/persistence/renderer controls, and redacted network-policy
identities. It never contains API keys, contact or User-Agent values, raw
endpoint addresses, filesystem paths, or raw federation/open-pack binding
names. Endpoint fingerprints strip URL userinfo, query values, and fragments;
only credential/query presence and cardinality remain. Provider identity and
renderer-token values are represented only by booleans/counts, while host and
private binding policies use SHA-256 identities. Its `state_limitations`
field is explicit because live upstream data, provider health, cache contents,
local corpus contents, open-pack revisions, and peer indexes are state rather
than reconstructable configuration. Preserve the manifest beside the run,
labels, executable digest, and summary; it improves attribution but does not
turn a changing web into a deterministic fixture.

Each case has a stable ID, one canonical intent, tags, a query, result count,
and search depth. Optional expected domains are relevance proxies for
authoritative sources, not an assertion that other domains are irrelevant.
Freshness-sensitive cases are marked explicitly.

Each output record contains the request identity, duration, HTTP outcome,
normalized error, non-empty/result/domain counts, extraction and publication
signals, expected-domain matches, and compact result observations. Exact typed
broker provenance comes from Fetchmark's `provenance` tuples; it is never
guessed from engine names. Schema-v1 artifacts containing only the legacy
`rrf_sources` lane/variant projection remain loadable, but their provider is
marked unavailable and excluded from provider overlap. The summary printed to stdout
includes overall and per-intent counts, non-empty and extraction rates, global
unique domains, p50/p95 latency, error classes, provenance coverage, per-source
and per-lane top-result contribution, source-pair overlap, and unique domains
per source. Malformed provenance remains a separate visible count. It also
reports attempted/latency-sample counts and `complete`; cases canceled before
an HTTP attempt do not contribute artificial zeroes to percentiles.

When the target server supports native discovery evidence, every record also
retains the aggregate classification and ordered bounded lane reports. This
includes HTTP search failures whose native error envelope contains a report.
Offline summaries expose report coverage across attempted requests, aggregate
status counts, provider/lane/variant status counts, and normalized diagnostic
reason counts. Raw provider instance addresses and error strings are never
part of the report. Older artifacts remain valid with zero report coverage and
no build or configuration digest. Strict offline loading accepts those legacy
all-empty states but rejects invalid, mixed, or partially populated
`build_sha256` or `configuration_sha256` evidence.

## Offline relevance judgments

Generate a new self-contained offline labeling workbench from an existing run:

```bash
go run ./cmd/fetchmark-eval \
  -records fetchmark-eval-20260718T000000Z.jsonl \
  -label-ui fetchmark-labels-20260718T000000Z.html
```

Open the HTML file in a browser. It contains no external assets or automatic
network requests; opening a displayed result link remains an explicit user
action. Grade with the 0–3 buttons or keyboard, use the evidence rail to jump
between results, and export drafts while work is incomplete. The 200-cell rail
uses one roving tab stop with arrow/Home/End navigation; auto-advance announces
the new result through a polite live region. Draft import is
bound to the exact run, case, URL, order, and optional template metadata.
Browser-local recovery additionally binds every immutable template field and
is opportunistic, so an exported draft is the durable
resume point. `Export completed labels` stays disabled until every row has a
grade and emits scorer-compatible JSONL.

Provider identities and provenance are absent from both the encoded workbench
payload and its exports. The workbench path uses exclusive creation and an
existing file is never overwritten. It does not send grades back to Fetchmark
or retain them anywhere except browser storage and files the reviewer chooses
to export.

For a raw editing workflow, create a new blind labeling template instead:

```bash
go run ./cmd/fetchmark-eval \
  -records fetchmark-eval-20260718T000000Z.jsonl \
  -label-template fetchmark-labels-20260718T000000Z.jsonl
```

Fill each `relevance: null` with an integer grade: `0` irrelevant, `1`
marginal, `2` relevant, or `3` highly relevant. Rows are bound to the exact
run, case, and returned URL. Provider identities are deliberately omitted from
the template to reduce source-name bias. Both output modes use exclusive
creation; an existing file is never overwritten.

Score the completed labels and write the reproducible combined report:

```bash
go run ./cmd/fetchmark-eval \
  -records fetchmark-eval-20260718T000000Z.jsonl \
  -labels fetchmark-labels-20260718T000000Z.jsonl \
  > fetchmark-summary-20260718T000000Z.json
```

Offline loading rejects unknown fields, mixed runs, duplicate cases/results,
invalid URLs, inconsistent counts, unknown judgments, duplicate judgments,
unfilled grades, and artifacts that exceed the shared per-row or aggregate
budgets. A run is accepted only when its generated label template fits those
same limits. Relevant-hit coverage is the operative discovery-quality measure:
a query is covered only when at least one supplied judgment is grade 2 or 3.
Raw non-empty coverage remains an availability diagnostic and is not evidence
that Fetchmark answered the query. Precision@5, NDCG@10, and reciprocal rank
include only successful non-empty cases whose returned results are fully
labeled; a missing label is never silently converted to grade zero. Relevant
result counts and mean grades are also reported by exact source and lane.

Live runs depend on current web and deployment state, so they are evidence
artifacts rather than unit-test expectations. Preserve the raw run JSONL,
completed labels, and emitted summary together when comparing runs.

Versioned live evidence belongs in `eval/baselines/` with a dated configuration
note under `docs/benchmarks/`. The first native-open representative pilot is
documented in
[`docs/benchmarks/native-open-pilot-20260718.md`](../docs/benchmarks/native-open-pilot-20260718.md).
The opt-in Wiby follow-up is documented in
[`docs/benchmarks/wiby-native-pilot-20260718.md`](../docs/benchmarks/wiby-native-pilot-20260718.md).
The first end-to-end native discovery-report pilot is documented in
[`docs/benchmarks/discovery-report-pilot-20260718.md`](../docs/benchmarks/discovery-report-pilot-20260718.md).
The original and remediated concurrency-4 capacity runs, serial 120-query
quality baseline, and unfilled blind label template are documented in
[`docs/benchmarks/native-open-full-baseline-20260718.md`](../docs/benchmarks/native-open-full-baseline-20260718.md).
The isolated 20-case Stack Overflow developer pilot, provider-content rerun,
combined native-open comparison, and their separate unfilled 200-row blind
templates are documented in
[`docs/benchmarks/stackexchange-developer-pilot-20260718.md`](../docs/benchmarks/stackexchange-developer-pilot-20260718.md).
