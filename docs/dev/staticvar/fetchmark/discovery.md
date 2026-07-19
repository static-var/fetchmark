# discovery

Fetchmark's discovery layer is evolving from a SearXNG-only URL source into a
provider-aware broker. The product boundary remains free and self-hosted: paid
search APIs, per-query tolls, proxy lists, CAPTCHA solving, and mandatory local
models are not discovery dependencies.

## Contracts

`internal/core/search.Searcher` stays as the compatibility port:

```go
Search(context.Context, Query) ([]Hit, error)
```

Providers can additionally implement `BatchSearcher` and return `SearchBatch`.
The richer batch owns provider identity, instance identity, source diagnostics,
duration, and one of these statuses:

| Status | Invariant | Orchestration policy |
|--------|-----------|----------------------|
| `healthy` | hits, no known degradation | accept |
| `partial` | hits plus source diagnostics | accept hits and retain diagnostics |
| `degraded_empty` | no hits plus source diagnostics | try another source; never authoritative negative-cache |
| `authoritative_empty` | no hits and no known diagnostics | accept as empty under the provider's policy |
| `failed` | call returned an error | apply error policy/failover |

`Partial()` is derived from `Status`; it is not stored independently. This
keeps contradictory batch states unrepresentable. Adapters migrate
incrementally by making `Search` delegate to `SearchBatch`; legacy pipeline and
test searchers do not need to implement the richer interface.

SearXNG's JSON diagnostics are user-facing `[engine, reason]` pairs rather
than a versioned machine schema. Fetchmark therefore uses the presence of a
diagnostic—not English reason text—to distinguish degraded empty. Raw reason
text is preserved for operators but must not become an unbounded metric label.

## Phase 0: evaluation harness

Build the harness before broad provider expansion so improvements are measured
against a fixed baseline.

1. Start with versioned JSONL query fixtures split across `general`, `fresh`,
   `long_tail`, `developer`, `research`, and `knowledge`. Grow toward 100–200
   queries while keeping stable IDs and intent labels.
2. Keep fixture validation and aggregation deterministic. Put network probes in
   a separate opt-in command with explicit endpoint, timeout, concurrency, and
   output path.
3. Record per query: non-empty outcome, unique domains, extraction success,
   publication/fetch freshness where known, p50/p95 latency inputs, normalized
   errors, and strict run-bound human relevance labels.
4. Retain exact typed broker provenance for provider/lane contribution,
   source-pair overlap, and source domain diversity. Never infer it from engine
   names or URLs.
5. Retain the aggregate batch classification and every bounded planned-lane
   outcome on both successful and failed native HTTP responses. Preserve only
   normalized diagnostic tokens; never publish provider-instance addresses or
   raw error text.
6. Store raw run JSONL plus a small reproducible summary. Never make live web
   behavior a unit-test success condition.

The initial harness implements this boundary:

- `eval/queries.jsonl` contains 120 validated cases, balanced across the six
  intents above.
- `cmd/fetchmark-eval` validates the suite offline by default. Network requests
  require the explicit `-live` flag, and live mode requires a new `-output`
  path opened without overwrite permission.
- `FM_EVAL_API_KEY` supplies authentication without putting a key in command
  arguments or output. Concurrency and request/run deadlines are bounded.
- Raw ordered records retain URLs, domains, source engines, extraction and
  publication signals, expected-domain matches, latency, HTTP status, and
  normalized errors. The deterministic summary reports non-empty and
  extraction rates, unique domains, p50/p95 latency, error classes, and
  per-intent outcomes. Attempted and latency-sample counts plus an explicit
  completeness flag keep queued cancellations out of latency percentiles.
- Controlled result provenance is decoded into typed provider/lane/variant
  tuples. Legacy `rrf_sources` artifacts remain loadable but explicitly mark
  provider identity unavailable instead of inferring it from a lane name.
  Deterministic offline reports expose provenance coverage, malformed rows,
  source/lane contribution, pair overlap, and per-source unique domains.
- Native responses include a separate bounded `discovery` report. Live records
  retain it even for non-2xx search failures, and offline summaries report
  evidence coverage, aggregate status counts, exact lane/status counts, and
  normalized provider diagnostic reasons. Legacy artifacts and older servers
  without the additive object remain loadable; zero report coverage is
  explicit rather than reclassifying an HTTP 200 empty response.
- Existing run JSONL is loaded strictly for offline analysis. The CLI creates
  blind, exclusive relevance-label templates and a self-contained offline HTML
  workbench. The workbench supports keyboard grading, a roving-tabindex result
  rail, live result announcements, durable draft import/export, strict
  run/result binding, and completed scorer-compatible
  JSONL without including provider identity or loading external assets. The
  evaluator scores completed 0–3 labels with explicit coverage, NDCG@10,
  reciprocal rank, and source/lane relevance. Missing labels never become
  implicit zeroes.
- Live observations can embed the exact Fetchmark revision and a bounded
  deployment configuration identifier so comparison artifacts are attributable.
- Reproducible baselines can first capture the authenticated bounded v1
  resolved-configuration manifest. Its exact digest is required consistently
  across native search responses; invalid, mixed, partially present, or
  unexpected evidence makes the run incomplete. Raw keys, addresses, paths,
  contacts, User-Agent values, and private federation/open-pack binding names
  are excluded. Endpoint fingerprints remove userinfo, query values, and
  fragments; their sensitive shape is count/boolean-only, while non-secret
  routing and private-binding policy uses SHA-256.
- `make eval-check` validates fixtures only; it never performs a live request.

## Phase 1: reliability slices

Implement in reviewable order:

1. Provider batches, SearXNG diagnostics, and degraded-empty failover.
2. Bounded metrics for status, result count, duration, normalized failure
   class, unique domains, and later top-k contribution.
3. Separate transport health from query-quality health; add EWMA and circuits
   for CAPTCHA, access denial, rate limiting, timeouts, malformed responses,
   and repeated zero-result degradation.
4. Honor Retry-After where present and otherwise use conservative exponential
   backoff with jitter. Do not retry operator/client errors aggressively.
5. Coalesce identical in-flight discovery queries and add discovery caching
   with stale-while-revalidate. Never negative-cache degraded-empty as an
   authoritative empty.
6. For advanced depth, fan out within a request budget and combine diverse
   provider batches using canonical URL deduplication and provider-aware
   weighted reciprocal-rank fusion.

The initial SearXNG slice also retains normalized failed-instance provenance:
an empty fallback cannot be classified as authoritative when another attempted
instance failed. If a later result page fails after earlier pages produced
hits, those hits return as `partial` with a page-level diagnostic instead of
being discarded as a total failure.

Transport and response quality now have separate circuits. Retryable HTTP and
network failures use bounded exponential backoff with deterministic jitter and
honor `Retry-After`; an all-open pool does not bypass that source protection.
Repeated diagnostically degraded responses open a short quality circuit, while
authoritative empties remain query-scoped outcomes and do not penalize the
shared source. EWMA instance and bounded engine gauges retain gradual evidence
instead of conflating one response with permanent health.

`internal/adapters/discoverycache` now decorates the provider pool before the
pipeline. Identical cold queries join one timeout-bounded flight; each caller
can still cancel independently, and the provider work stops when its final
cold waiter leaves. Canceling one waiter cannot disrupt another identical
waiter. Stale refreshes remain detached because their cached response has
already been served. Healthy non-empty batches are
retained for 30 seconds by default, followed by a two-minute
stale-while-revalidate window. Partial, degraded-empty, authoritative-empty,
failed, canceled, and oversized outcomes are coalesced for current waiters but
never retained. Entry count, estimated bytes, per-entry size, and detached
flights are all bounded. When the flight budget is full, cold queries use the
caller context without cache admission and stale queries skip refresh.

Advanced depth now plans an ordered cross-product of controlled query variants
and the process-configured discovery-source registry. A fixed worker pool
(`FM_ADVANCED_SEARCH_CONCURRENCY`, default 4, maximum 16) executes those lanes
concurrently while storing outcomes in planner order. One failed lane does not
discard usable healthy or partial hits from another; an empty completed lane
also remains authoritative over unrelated failures. If every lane fails, the
first planner-ordered error is returned. Parent cancellation or deadline always
wins and all workers finish before orchestration returns.

Fusion canonicalizes URLs before weighted reciprocal-rank fusion. Source,
pack-lane, and variant weights are explicit registry inputs; the current built-in
registry includes non-neutral historical defaults. New opt-in lanes, including
arXiv and Stack Exchange, stay at 1.0 until evaluation data justifies tuning.
Each fused candidate retains controlled `provider/lane/variant` provenance.
Canonical URL, exact-content, and near-duplicate merges union and sort those
tuples before final ranking. The native API exposes this additive typed field;
vendor compatibility responses do not. The legacy `rrf_sources` metadata key
remains a lane/variant projection during migration. Final top-k contribution
and domain-diversity metrics read post-dedupe tuples rather than a pre-fetch URL
side map. Basic depth uses the configured source-pack plan,
but stays CPU/latency bounded by issuing only the original query once per
planned provider (plus the optional local-index lane), under the configured
worker limit. It does not generate exact/freshness/docs variants. Explicit
engine selection remains pinned to an enabled SearXNG source independently of
which provider is configured as the ordinary primary.

## Phase 2: source packs and native open lanes

Advanced search now uses a deterministic intent classifier and a strict,
versioned source-pack registry. The embedded v1 registry declares five packs:
`general-open`, `developer`, `research`, `knowledge`, and `fresh`. Pack order is
stable and affects lane order; weighted reciprocal-rank fusion remains the
only cross-lane merge policy.

The registry separates trusted process bindings from declarative controls:

- JSON may select only compiled source kinds (`searxng`, `wikipedia`,
  `crossref`, `arxiv`, `mwmbl`, `wiby`, `stackexchange`, `github`, `pubmed`, `yacy`, and explicitly bound `openpack` or `federation`
  sources), controlled variants, engine/category names, weights, result
  caps, timeouts, and global provider budgets. The ten fixed provider
  kinds must use their canonical singleton IDs; custom aliases cannot multiply
  their process-wide budgets. Open-pack and federation bindings retain distinct
  operator IDs.
- `FM_DISCOVERY_PACK_FILE` overrides the embedded JSON. Files are capped at
  1 MiB, decoded with unknown fields rejected, and fully validated before the
  server starts.
- `FM_DISCOVERY_ENABLED_PACKS` and `FM_DISCOVERY_ENABLED_SOURCES` are strict
  allowlists. `FM_DISCOVERY_PRIMARY_SOURCE` must name an enabled source and
  defaults to `searxng`, preserving existing deployments. Its pack lanes are
  planned first; when no matching pack includes it, its globally bounded source
  is prepended rather than demoted to an empty-plan-only fallback. Unknown,
  disabled-primary, and enabled-but-unbound sources fail startup instead of
  silently weakening a plan.
- The total process-local discovery cache capacity is divided between enabled
  providers. Each provider keeps independent coalescing and stale refresh, so
  one source cannot satisfy or suppress another source's query.
- Declarative rate, burst, and concurrency budgets are process-wide. Native
  adapters enforce them directly; the SearXNG pool is wrapped by the same
  contract so basic and advanced requests share one bounded budget. Admission
  acquires a concurrency slot, rechecks any provider cooldown, and only then
  spends rate capacity; cancellation and cooldown rejection release the slot.
- SearXNG is constructed only when `searxng` is enabled. It is a hard
  readiness dependency only while it is the configured primary; when enabled
  merely as a secondary opportunistic lane, its failures remain query-local.
  Its URLs and cooldown are ignored when disabled.
- Explicit `engines` requests remain pinned to enabled SearXNG. Without that
  source, the planner returns a typed `UnsupportedControlError`, exposed by the
  native API as HTTP 400 instead of letting a native provider ignore it.
- Both basic and advanced searches use the enabled pack plan. Basic uses one
  original lane per provider; advanced additionally uses the pack's controlled
  variants. Local-index, signed open-pack, and trusted federation adapters
  participate through the same planner when configured.

The Wikipedia adapter uses the official Action API with a descriptive
contactable identity, global rate/concurrency bounds, response-size limits,
language-host validation, Retry-After cooldowns, and rich batch diagnostics.
It returns page metadata and CC-BY-SA attribution metadata for the normal
Fetchmark fetch/extract path. See the [Action API tutorial](https://www.mediawiki.org/wiki/API%3ATutorial/en),
[Wikimedia API usage guidelines](https://foundation.wikimedia.org/wiki/Policy%3AWikimedia_Foundation_API_Usage_Guidelines/en),
and [User-Agent policy](https://foundation.wikimedia.org/wiki/Policy%3AWikimedia_Foundation_User-Agent_Policy).

The Crossref adapter uses the no-key `/works` API with public-pool ceilings;
setting `FM_CROSSREF_MAILTO` opts into Crossref's polite pool while the adapter
still caps concurrency and request rate to the published limits. It returns
DOI, title, authors, venue, publisher, type, date, and license metadata. It does
not copy publisher abstracts into snippets because abstract copyright can
differ from Crossref's open bibliographic metadata. See Crossref's
[access and authentication](https://www.crossref.org/documentation/retrieve-metadata/rest-api/access-and-authentication/),
[REST API tips](https://www.crossref.org/documentation/retrieve-metadata/rest-api/tips-for-using-the-crossref-rest-api/),
and [metadata documentation](https://www.crossref.org/documentation/retrieve-metadata/rest-api/).

The arXiv adapter is an opt-in `research` lane using the fixed public Atom
endpoint with no key or per-query toll. It converts natural-language input to
a bounded conjunction of literal `all:` terms, supports exact phrases and
native submitted-date windows, requests one first page, and accepts only
validated arXiv abstract identifiers. Atom PDF links are ignored. Returned
titles, abstracts, authors, categories, and dates are descriptive metadata;
article bodies are never injected as provider content and still cross the
ordinary Fetchmark SSRF, robots, noindex, and extraction boundary.

The adapter hard-caps every process to one connection and one request per
three seconds, never retries implicitly, and honors bounded `Retry-After`
cooldowns. That limiter cannot coordinate separate processes: arXiv applies
the ceiling across all machines under the caller's control, so operators must
enable this source in only one replica or enforce an equivalent shared gate.
Explicit engine/category, non-auto language, safe-search, and domain controls
are rejected rather than silently weakened. The source remains neutral-weight
and disabled by default until the fixed evaluation suite establishes its
incremental relevance.

Results carry the exact requested acknowledgment, “Thank you to arXiv for use
of its open access interoperability,” in native metadata and an HTTP `Link`
header. CC0 identity remains explicitly scoped to each native result's
`metadata_license` and `metadata_license_url`; it is never emitted as a license
for the mixed response or linked papers and PDFs. Tavily and Exa requests can
use this lane when their controls permit it. The lane returns a typed
unsupported-control outcome for Brave requests because the arXiv API cannot
preserve Brave's default language and safe-search semantics; other lanes
continue independently. See
arXiv's official [API access](https://info.arxiv.org/help/api/index.html),
[manual](https://info.arxiv.org/help/api/user-manual.html), and
[terms](https://info.arxiv.org/help/api/tou.html).

The Mwmbl adapter uses the fixed official no-key JSON search endpoint as an
opt-in `general-open` lane. It stays out of the default
`FM_DISCOVERY_ENABLED_SOURCES` list because Mwmbl describes its index as a
small proof of concept and publishes no numeric public-service quota. The
built-in source therefore starts at a neutral fusion weight, permits one
in-flight request, defaults to 0.2 requests/second, honors bounded
`Retry-After` cooldowns, and emits partial/failed diagnostics for malformed
rows. Custom registries must keep the singleton source ID `mwmbl`, preventing
multiple bindings from bypassing the process-wide public-service budget. It
shares the provider admission gate used by other native and declaratively
budgeted sources: concurrency admission and a fresh cooldown check happen
before rate admission, so a queued or canceled request cannot reserve a future
token and later burst at the upstream boundary. Its 30-second lane budget
allows bounded queueing at those deliberately conservative rates. It supports
only the original query and post-response include/exclude domain filtering; engine/category,
language, time-range, nonzero safe-search, and exact-match controls return a
typed unsupported-control error for this lane. See the [SearXNG Mwmbl engine documentation](https://docs.searxng.org/dev/engines/online/mwmbl.html)
and its [official-API adapter](https://github.com/searxng/searxng/blob/master/searx/engines/mwmbl.py).

The Wiby adapter is a second opt-in `general-open` lane using the fixed
official no-key JSON endpoint. Wiby's public guide describes a 12-result page
and publishes no numeric service quota, so Fetchmark caps each response at 12,
allows one in-flight request, and defaults to 0.2 requests/second. It shares
the same bounded body, Retry-After cooldown, admission, diagnostic, cache, and
domain-filtering contracts as the other native providers, including the
30-second lane budget needed for bounded admission queueing. The adapter never
sends Wiby's `nsfw` parameter: moderate safe search uses the service's filtered
default, while off and strict modes return a typed unsupported-control error
for this lane. Language, time-range, category, engine, and exact-match controls
are likewise rejected rather than ignored. The fixed source must retain the
singleton ID `wiby` in custom registries.

Wiby's [JSON API terms](https://wiby.me/json/) ask API consumers to link back
to Wiby with displayed results. Fetchmark preserves compatibility response
bodies and emits the registered Web Linking relation as
`Link: <https://wiby.me/>; rel="via"; title="Wiby"` whenever the returned search
page includes a Wiby result. This applies to native, Tavily, Exa, and Brave
search routes; the header is absent when that response page has no Wiby result.
See Wiby's [self-hosting and API guide](https://wiby.me/about/guide.html).

The Stack Exchange adapter is an opt-in `developer` lane fixed to the official
Stack Overflow `/2.3/similar` API. It treats the Fetchmark query as a
hypothetical question title, requests one relevance-ordered page, caps the
response at ten questions, and synthesizes result URLs from validated positive
question IDs rather than trusting provider-returned origins. The singleton
source ID is `stackexchange`; built-in budgets allow one in-flight request and
default to 0.2 requests/second with a 30-second lane budget.

The request uses the official `withbody` filter. When a row includes non-empty
question HTML and a `content_license`, the hit carries that HTML as transient
provider content. The core extractor consumes it without fetching the public
question page, then clears it before serialization. Ordinary search never
writes this provider content to the local corpus or artifact store. Missing or
unlicensed body data remains a discovery-only result with `extract_failed`; it
never falls through to a public-page crawl or gets treated as licensed content.

The API's common JSON wrapper is scheduling evidence. A valid `backoff` opens a
method-wide cooldown even on HTTP 200; reported quota exhaustion suppresses new
calls conservatively. HTTP `Retry-After` is retained before body parsing so an
oversized or unreadable throttling response cannot discard the wait signal. A
bounded recent-query set rejects semantically identical provider requests
inside one minute of provider dispatch before spending rate capacity, while
cancellation before dispatch releases the pending reservation. No numeric
anonymous daily quota is assumed because it is shared by public IP and
communicated dynamically.
The adapter supports original-query title matching, English/auto language,
day/week/month/year creation windows, moderate safe search, and local
include/exclude domain filtering. It rejects categories, explicit engines,
non-English language, off/strict safe search, and exact-match semantics rather
than pretending the provider preserved them.

Stack Exchange's [API terms](https://stackoverflow.com/legal/api-terms-of-use)
require visible source identification, while contribution license versions can
vary by date. Each hit retains the provider's `content_license` plus the
[licensing guide](https://stackoverflow.com/help/licensing), Stack Overflow
source metadata, and a Stack Overflow result URL. Native and compatibility
routes also emit `via` and generic CC BY-SA `license` Link relations. A response
with API-supplied content also receives a whitelisted exact-version
`license` relation and an `author` relation when the API supplied a positive
owner ID. Exa-compatible results carry the owner name in their author field.
Any UI displaying these results must visibly identify Stack Overflow; a protocol
header alone cannot force a downstream presentation choice. See the official
[`/similar` method](https://api.stackexchange.com/docs/similar),
[wrapper](https://api.stackexchange.com/docs/wrapper), and
[throttle rules](https://api.stackexchange.com/docs/throttle).

The GitHub adapter is an opt-in `developer` lane fixed to the official public
repository-search endpoint. It requires no token and makes one first-page
request only: no core-API follow-ups, README downloads, repository contents,
or provider documents enter the discovery response. Fetchmark may separately
fetch a selected public result page through its ordinary SSRF, robots, noindex,
extraction, and retention gates. Fetchmark maps the canonical public
repository URL, full name, description, topics, detected repository license,
language, activity timestamps, and popularity counts as bounded discovery
metadata. A detected repository license is provenance, not proof that every
file or description is reusable; repository activity is never represented as
a publication date.

User query text is projected into at most 16 bounded lexical terms before
adapter-owned public, non-archived, non-mirror qualifiers are appended. This
prevents user text from injecting GitHub search qualifiers. The original lane
supports optional day/week/month/year windows as repository `pushed` activity,
not document freshness. Human-language, exact-match, engine/category, and
safe-search controls return a typed unsupported-control error rather than being
silently weakened.

The built-in anonymous budget is one in-flight request, burst one, and 0.1
requests/second—six per minute, below GitHub's documented ten-per-minute
unauthenticated search allowance so shared-IP capacity retains headroom. HTTP
403/429 honors bounded `Retry-After` or rate-reset evidence and otherwise opens
a conservative one-minute cooldown. GitHub's `incomplete_results` flag becomes
partial or degraded-empty provider evidence; a complete empty response remains
authoritative for repository search. The adapter pins the current API version,
refuses redirects, and stays disabled and neutral-weight until fixed-suite
relevance evidence justifies a change. See the official
[repository search contract](https://docs.github.com/en/rest/search/search#search-repositories),
[rate-limit documentation](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api),
and [repository licensing guidance](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/licensing-a-repository).

That gate and cooldown are process-local, while GitHub's anonymous allowance
is associated with the shared egress IP. Operators running multiple Fetchmark
processes behind one NAT must lower/divide the declarative per-source rate so
their aggregate stays within the upstream bucket; the built-in 0.1 rate is not
a cluster-wide quota coordinator.

The PubMed adapter is an opt-in `research` lane fixed to NCBI's HTTPS
E-utilities origin. Enabling it requires `FM_PUBMED_EMAIL` to be a plain valid
operator/developer address. One search issues one bounded ESearch call and, only
when PMIDs exist, one batched ESummary call for at most 20 identifiers. Query
projection removes caller Boolean/field syntax, and the adapter synthesizes
only canonical `https://pubmed.ncbi.nlm.nih.gov/{PMID}/` result URLs.

The response maps bounded bibliographic metadata—title, authors, journal,
publication date/type, language, DOI/PMCID, and record status—but never calls
EFetch or requests abstracts/full text. It supplies a transient metadata-only
provider document so the ordinary pipeline does not fetch the result page;
that document is cleared after extraction and never enters local persistence.
This keeps publisher/author-owned abstracts outside the provider boundary.

The built-in anonymous budget is one in-flight search, burst one, and two
E-utility calls per second, below NCBI's documented three-per-second unauthenticated
ceiling. It is process-local while the upstream allowance is associated with
the public IP; replicas sharing egress must divide the declarative rate. HTTP
403/429, bounded `Retry-After`, and NCBI's JSON rate-limit error open a
conservative cooldown. The lane supports original natural-language search,
day/week/month/year publication windows, and local domain filtering. Exact,
language, safe-search, category, and engine controls are rejected rather than
silently weakened.

Consequently, PubMed does not contribute to Brave-compatible searches: Brave
always supplies its default English-language and moderate-safe-search controls,
which E-utilities cannot preserve. Other planned lanes continue independently;
a PubMed-only process cannot serve the Brave search route.

Native metadata marks observation-time staleness and links NLM's terms and
disclaimer. Native and compatibility responses containing PubMed results also
emit `via` and `terms-of-service` Web Linking relations. Applications must
clearly acknowledge NLM, avoid implying endorsement, make staleness evident,
and treat linked articles/abstracts as separately copyrighted. See the official
[E-utilities contract](https://www.ncbi.nlm.nih.gov/books/NBK25497/),
[parameter reference](https://www.ncbi.nlm.nih.gov/books/NBK25499/), and
[NLM data terms](https://www.nlm.nih.gov/databases/download.html).

The YaCy adapter is an opt-in `general-open` lane bound to one explicitly
configured operator-controlled origin through `FM_YACY_URL`. HTTPS is required
unless the operator sets `FM_YACY_ALLOW_INSECURE_HTTP=true` for a trusted
private service network. Origin URLs cannot carry userinfo, path, query, or
fragment. Runtime construction gives the adapter a separate private-capable,
exact-host-allowlisted client; it refuses redirects and never reuses that
client for result-page fetching.

Each search makes one bounded first-page `/yacysearch.json` request with
`contentdom=text`, `verify=false`, navigation disabled, and at most the
declarative result cap. The built-in source permits one in-flight request and
0.2 requests per second. An operator pack may raise an owned local node to at
most ten requests per second, while global mode is always capped at 0.2. Query
text is projected to bounded lexical terms;
YaCy descriptions are untrusted markup stripped to bounded plain text. Result
URLs receive local include/exclude-domain filtering before entering the normal
SSRF/robots/noindex extraction pipeline.

`FM_YACY_RESOURCE=local` searches only that node's index and classifies a valid
empty response as authoritative for that corpus. `global` permits the node to
send the query to its configured YaCy peers, so operators must consent to that
privacy boundary. Global empties remain `degraded_empty`; a response reporting
effective local mode becomes `global_downgraded`, with hits retained as
partial. Missing effective-resource evidence and malformed rows likewise
degrade health instead of being hidden. Engine/category, language, time-range,
safe-search, and exact-match controls are rejected rather than silently
weakened, so the lane cannot contribute to Brave's default-controlled route.

YaCy stays disabled and neutral-weighted until a populated operator-owned node
completes the fixed suite with fingerprinted contribution, latency, domain, and
blind relevance evidence. Global mode is optional federation, not a proxy or
an authoritative whole-web empty. See the official [project](https://yacy.net/),
[FAQ](https://yacy.net/faq/), and
[search API](https://wiki.yacy.net/index.php/Dev%3AAPIyacysearch).

OpenAlex is intentionally absent from the default live no-toll lane: its live
API now has metered daily budgets. Its downloadable snapshot remains a future
candidate for separately operated pack ingestion rather than ordinary query
traffic. See [OpenAlex authentication and pricing](https://developers.openalex.org/api-reference/authentication).

## Phase 4: opt-in local lexical flywheel

`FM_LOCAL_CORPUS_MODE=ephemeral` opens a process-local Bleve index.
`FM_LOCAL_CORPUS_MODE=personal` requires separate absolute
`FM_LOCAL_INDEX_PATH` and `FM_LOCAL_ARTIFACT_PATH` directories, opens a
persistent Bleve/Scorch projection, and stores permitted source representations
under a bounded content-addressed filesystem repository. `curated` uses the
same bounded persistent stores but only accepts explicit fresh URL admissions
through the authenticated admin surface. `archive` adds bounded immutable
multi-version history and conditional revalidation, while retaining the same
robots, noindex/noarchive, takedown, and storage-budget gates. Disabled remains
the default. Local search runs beside live discovery for basic and advanced
requests and participates in the existing weighted canonical RRF merge.

Retention is fail-closed. Only cold live retrievals with an authoritative
robots.txt allow decision and no applicable response-header or HTML
noindex/noarchive are
searchable. Unknown/blocked/noindex/takedown observations are persisted as
bodyless tombstones. Per-URL observation timestamps prevent an older concurrent
fetch from overwriting a newer tombstone, including after restart. Cache hits
are never promoted without fresh policy evidence. On a later cold miss, an
intact unexpired personal artifact may supply same-agent ETag and Last-Modified
validators. A 304 reuses only that hash-verified source after fresh policy
checks; redirect, proxy, renderer, user-agent override, corrupt, missing, or
expired cases bypass or fall back to an unconditional retrieval. Renderer-only
fetches can revoke on explicit HTML noindex/noarchive but never grant retention because
their response headers are unavailable.

The index stores bounded discovery fields and a snippet, not full bodies.
Headings and fragment-free HTTP(S) outbound links come only from the extractor's
reader-view content tree; hidden boilerplate, non-web schemes, credentials,
duplicates, oversized values, and unbounded lists are rejected. The SHA-256 of
the raw retained representation is shared by the artifact pointer and Bleve
projection, while these indexing-only fields remain absent from public response
JSON and the ordinary response cache. The index uses BM25 lexical search and
normalized host-ancestry/path-segment term filters, so no embedding service,
GPU, CGo runtime, or LLM is required. Safe-search
requests admit only explicitly safe-classified documents; current pipeline-fed
documents remain unclassified and therefore require safesearch 0. Curated
admission may attach an explicit batch-level `safe`, `unsafe`, or
`unclassified` operator assertion; no page-text classifier runs implicitly.

Personal mode keeps the latest immutable per-URL source version with a sliding
expiry. Startup validates current hashes, purges unreachable objects, and fails
closed on missing/corrupt current data. The adapter enforces exclusive
single-owner access and is unencrypted at rest. Curated mode requires an admin
key and robots enforcement;
ordinary request paths cannot admit permitted content. The
`/admin/corpus/admissions` POST accepts URL-only batches, bypasses hot cache and
conditional reuse, disables renderer/proxy/UA overrides, and returns bodyless
bounded outcomes after fresh policy evidence and artifact-before-index commit.
Its optional `safety_classification` is an operator assertion for the entire
batch and defaults to `unclassified`.
The separate `/admin/corpus/focused-admissions` POST accepts one URL plus its
validated allow/deny path policy. It re-enforces same-scheme/authority and
deny-first decoded-path scope on every redirect before any bytes are retained.
The `/admin/corpus/takedowns` POST is available in curated and archive modes;
it tombstones the index and purges all retained source versions even when one
store reports an error. Archive mode automatically records distinct permitted
source hashes under aggregate/per-URL version quotas, keeps Bleve current-only,
and never silently evicts history. It requires a new schema-v2 artifact
directory and does not imply WARC or legal-grade preservation.
Later phases can add more native/open-pack sources. A separately operated
focused crawler, compact signed index packs, and the first private trusted
URL-only federation lane now exist as explicit opt-in components. See
[federation.md](federation.md) for the federation disclosure and trust model.
None requires every installation to crawl the web or run embeddings/LLMs.

## Safety boundaries

- Fetch/extract/ingest paths continue to enforce SSRF policy, RFC 9309 robots,
  noindex/X-Robots-Tag, source rate limits, and explicit retention policy.
- Crawling is optional and off the request path, with operator identity,
  contact, per-host budgets, opt-out, and takedown support.
- Open index packs distribute compact discovery metadata by default, not full
  page bodies whose licenses or directives do not permit redistribution.
- SearXNG remains one opportunistic source lane, not the source of truth.
