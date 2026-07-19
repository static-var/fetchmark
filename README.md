# Fetchmark

**A self-hostable, Docker-first search API with Tavily, Exa, and Brave compatibility routes.**

Fetchmark combines provider-aware open discovery, a parallel fetch+extract
pipeline, and a BM25 re-ranker in one small Go binary. SearXNG is one
opportunistic discovery lane alongside native open sources. Point it at a query,
get back ranked results with clean Markdown, structured JSON, and cleaned
HTML — ready for RAG, LLM context, or downstream processing.

[![Go](https://img.shields.io/badge/go-1.26.5-00ADD8)](go.mod)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Docker](https://img.shields.io/badge/docker-ready-2496ED)](deploy/docker-compose.yml)

---

## Why Fetchmark

- **You own the stack.** No paid search API, API key, or per-query toll. Shared
  open sources still retain their own courteous rate limits. Run it on your
  laptop, in a homelab, or in your VPC.
- **Small and boring.** Static Go binaries, including separate opt-in crawler
  and pack-management commands. Redis falls back to a bounded in-memory cache,
  and SearXNG can be omitted in favor of native, local-pack, or trusted-peer
  discovery providers. The production image is distroless.
- **Batteries included.** SSRF-safe egress, per-key rate limits, Redis-backed
  artifact cache with in-memory fallback, cross-instance stampede protection,
  Prometheus metrics, a read-only ops dashboard, OpenAPI spec.
- **LLM-friendly output.** Clean Markdown, cleaned HTML, structured
  extraction metadata (`content.main_text`, author, published_at), and a
  `js_required` flag so you know when a page needs a headless renderer.

---

## Quickstart

```bash
git clone https://github.com/staticvar/fetchmark && cd fetchmark
cp .env.example .env
printf 'FM_API_KEYS=%s\n' "$(openssl rand -hex 32)" > .env
printf 'FM_ADMIN_API_KEYS=\n' >> .env
docker compose -f deploy/docker-compose.yml up -d --build
```

Fetchmark listens on `:8080`. Redis and a bundled SearXNG come up alongside.
Use the generated key from `.env` for requests:

```bash
export FM_API_KEY=$(grep '^FM_API_KEYS=' .env | cut -d= -f2)
```

Generate replacement API keys with `openssl rand -hex 32`.

```bash
# Health
curl -s localhost:8080/healthz

# Search — meta-search + fetch + extract + rank
curl -s -X POST localhost:8080/v1/search \
  -H "Authorization: Bearer $FM_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query":"BM25 ranking algorithm","max_results":5}' | jq

# Parse arbitrary URLs — skip search, get clean content
curl -s -X POST localhost:8080/v1/parse \
  -H "Authorization: Bearer $FM_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"urls":["https://example.com"],"query":"example"}' | jq
```

A successful response contains `results[]` with `title`, top-level
`markdown`/`html`, nested `content.main_text`/`content.cleaned_html`,
`author`, `published_at`, BM25 `score`, and a `from_cache` flag.

---

## Endpoints

| Method | Path              | Purpose                                              |
| ------ | ----------------- | ---------------------------------------------------- |
| POST   | `/v1/search`      | Open discovery → parallel fetch → extract → BM25 rank |
| POST   | `/v1/parse`       | Fetch + extract arbitrary URLs; ranks if `query` set |
| POST   | `/v1/summarize`   | Parse + LLM summary (OpenAI-/Anthropic-compatible)   |
| GET    | `/v1/evaluation/configuration` | Authenticated non-secret resolved evaluation manifest |
| POST   | `/compat/tavily/search` | Tavily-compatible open-search subset           |
| POST   | `/compat/exa/search` | Exa-compatible search subset                       |
| POST   | `/compat/exa/contents` | Exa-compatible URL content retrieval              |
| GET    | `/compat/brave/res/v1/web/search` | Brave-compatible web search subset    |
| GET    | `/healthz`        | Liveness probe                                       |
| GET    | `/readyz`         | Deep-check: primary SearXNG and Redis when configured |
| GET    | `/metrics`        | Prometheus exposition                                |
| GET    | `/dashboard/`     | Read-only ops view (Basic Auth, opt-in)              |

See [`docs/openapi.yaml`](docs/openapi.yaml) for the wire contract and
[`docs/compatibility.md`](docs/compatibility.md) for the exact support matrix,
client base URLs, and intentionally unsupported vendor fields.
See [`open-index-packs.md`](docs/dev/staticvar/fetchmark/open-index-packs.md)
for the optional signed local discovery-pack workflow.
Pack publishers should also read
[`open-index-pack-publishing.md`](docs/dev/staticvar/fetchmark/open-index-pack-publishing.md)
for the isolated offline builder and evidence boundary.
See [`federation.md`](docs/dev/staticvar/fetchmark/federation.md) for the
optional private, allowlisted, URL-only index federation workflow.

### Request knobs

`/v1/search` accepts:

```jsonc
{
  "query": "...",              // required on /v1/search
  "max_results": 10,           // caps returned list
  "engines": ["google","duckduckgo"],
  "categories": ["general","news"],
  "language": "en",
  "time_range": "year",        // day, week, month, year; provider support varies
  "safesearch": 1,             // 0 off, 1 moderate, 2 strict
  "include_domains": ["go.dev"],
  "exclude_domains": ["reddit.com"],
  "exact_match": false,
  "search_depth": "basic",     // fast, ultra-fast, basic, advanced
  "chunks_per_source": 2,       // return up to 3 query-focused chunks/result
  "formats": ["markdown","json","html"],
  "render":  false,            // force headless render path (opt-in)
  "respect_robots": true,
  "timeout_ms": 8000,
  "proxy_url": "http://..."    // admin API key required
}
```

`/v1/parse` accepts `urls`, `query`, `formats`, `render`,
`respect_robots`, `timeout_ms`, and admin-only `proxy_url`.

### Authentication

```
Authorization: Bearer <key>
```

or the equivalent `X-API-Key: <key>` header. Generate API keys with
`openssl rand -hex 32` and set them in `FM_API_KEYS` before starting the
compose stack. Admin keys are optional; leave `FM_ADMIN_API_KEYS=` empty unless
you need admin-only fields (`proxy_url`, dashboard access). If you do enable
admin access, set `FM_ADMIN_API_KEYS` explicitly to one or more generated keys.

---

## Features

| Feature                                 | Status |
| --------------------------------------- | :----: |
| SearXNG meta-search                     |   ✅   |
| Multi-SearXNG failover (round-robin)    |   ✅   |
| Discovery coalescing + stale refresh    |   ✅   |
| Declarative intent-based source packs   |   ✅   |
| Native Wikipedia + Crossref discovery   |   ✅   |
| Native arXiv research discovery (opt-in) |   ✅   |
| Native Mwmbl discovery (opt-in)         |   ✅   |
| Parallel fetch (per-host concurrency)   |   ✅   |
| SSRF / egress policy (private IP block) |   ✅   |
| Robots.txt + User-Agent pool            |   ✅   |
| Markdown / JSON / cleaned-HTML output   |   ✅   |
| BM25 re-ranking with engine diversity   |   ✅   |
| Exact-SHA + Jaccard near-dup dedupe     |   ✅   |
| CJK-aware shingling for near-dup        |   ✅   |
| Two-layer cache (Redis + in-memory)     |   ✅   |
| Cross-instance stampede lock            |   ✅   |
| Per-key rate limiting                   |   ✅   |
| Prometheus metrics + request IDs        |   ✅   |
| Ops dashboard (HTMX, read-only)         |   ✅   |
| Headless rendering (opt-in)             |   ✅   |
| Proxy URL passthrough (admin)           |   ✅   |
| LLM summarisation endpoint              |   ✅   |
| Tavily / Exa / Brave compatibility paths |   ✅   |
| Opt-in persistent local lexical index    |   ✅   |
| Signed local open-index pack lane        |   ✅   |
| Private trusted URL-only federation      |   ✅   |
| SSE streaming                           |  ⏳    |

---

## Configuration

All config is environment-driven. Copy `.env.example` and edit.

| Variable                     | Default                  | Purpose                               |
| ---------------------------- | ------------------------ | ------------------------------------- |
| `FM_API_KEYS`                | required                 | Comma-separated allowed API keys      |
| `FM_ADMIN_API_KEYS`          | empty                    | Optional admin-only keys; set explicitly when used |
| `FM_SEARXNG_URL`             | `http://searxng:8080`    | Single SearXNG instance               |
| `FM_SEARXNG_URLS`            | _(unset)_                | Comma-separated list for failover     |
| `FM_SEARXNG_COOLDOWN`        | `30s`                    | Base transport/quality circuit backoff; honors Retry-After |
| `FM_DISCOVERY_CACHE_TTL` / `_STALE_TTL` | `30s` / `2m` | Fresh and additional stale discovery windows |
| `FM_DISCOVERY_CACHE_ENTRIES` / `_BYTES` | `256` / `16 MiB` | Process-local discovery cache bounds |
| `FM_DISCOVERY_CACHE_MAX_ENTRY_BYTES` | `1 MiB` | Maximum retained discovery batch |
| `FM_DISCOVERY_REFRESH_TIMEOUT` / `FM_DISCOVERY_MAX_INFLIGHT` | `30s` / `16` | Detached fill lifetime and concurrency bounds |
| `FM_ADVANCED_SEARCH_CONCURRENCY` | `4` | Maximum concurrent advanced discovery lanes (1–16) |
| `FM_DISCOVERY_PACK_FILE` | _(built in)_ | Optional strict v1 JSON source-pack registry |
| `FM_DISCOVERY_ENABLED_PACKS` | all built-ins | Ordered source-pack allowlist |
| `FM_DISCOVERY_ENABLED_SOURCES` | `searxng,wikipedia,crossref` | Strict source allowlist; append opt-in `arxiv`, `mwmbl`, `wiby`, `stackexchange`, `github`, `pubmed`, `yacy`, and/or `feedindex`, or omit `searxng` for a SearX-free process |
| `FM_DISCOVERY_PRIMARY_SOURCE` | `searxng` | Enabled first/fallback source; defaults preserve SearXNG-first behavior |
| `FM_DISCOVERY_PROVIDER_MAX_BODY` | `2 MiB` | Maximum native-provider response body |
| `FM_OPEN_PACK_REGISTRY_FILE` | _(unset)_ | Absolute process-owned, non-writable trust registry; open-pack sources stay opt-in |
| `FM_FEED_INDEX_FILE` | _(unset)_ | Absolute, immutable RSS/Atom metadata snapshot; required only when opt-in `feedindex` discovery is enabled |
| `FM_FEDERATION_IDENTITY_FILE` / `_TRUST_REGISTRY_FILE` | _(unset)_ | Paired absolute secure files for explicitly enabled trusted peers |
| `FM_FEDERATION_LISTEN_ADDR` | _(unset)_ | Separate plain-HTTP listener for a private TLS ingress; requires curated mode |
| `FM_CONTACT` | _(unset)_ | Optional contact appended to provider identity; default UA already has a project URL |
| `FM_CROSSREF_MAILTO` | _(unset)_ | Optional plain email for Crossref's polite pool |
| `FM_PUBMED_EMAIL` | _(unset)_ | Required plain operator/developer email when the native PubMed lane is enabled |
| `FM_YACY_URL` / `_RESOURCE` | _(unset)_ / `local` | Explicit operator-controlled YaCy origin and `local` or `global` index mode |
| `FM_YACY_ALLOW_INSECURE_HTTP` | `false` | Explicitly permit plain HTTP only on a trusted private service network |
| `FM_REDIS_URL`               | `redis://redis:6379/0`   | Redis cache/rate-limit state; falls back to in-memory when unreachable |
| `FM_RATE_LIMIT_PER_SEC` / `_BURST` | `5` / `20`         | Default per-key token bucket          |
| `FM_ARTIFACT_CONCURRENCY`    | `3`                      | Process-wide cold artifact workers    |
| `FM_MAX_REQUEST_SOURCE_BYTES` / `_OUTPUT_BYTES` | `64 MiB` / `128 MiB` | Aggregate request byte budgets |
| `FM_MEMORY_CACHE_ENTRIES` / `_BYTES` | `512` / `128 MiB` | In-memory fallback capacity           |
| `FM_CACHE_MAX_VALUE_BYTES`   | `8 MiB`                  | Maximum admitted cache artifact       |
| `FM_LOCAL_CORPUS_MODE`       | `disabled`               | `disabled`, process-local `ephemeral`, persistent `personal`, admin-fed `curated`, or bounded `archive` |
| `FM_LOCAL_CORPUS_MAX_AGE`    | `720h`                   | Sliding personal-mode retention window |
| `FM_LOCAL_EXPIRY_SWEEP_INTERVAL` | `15m` | Maximum healthy physical-cleanup lag for expired personal data |
| `FM_LOCAL_INDEX_PATH`        | _(unset)_                | Absolute Bleve directory required by persistent modes |
| `FM_LOCAL_INDEX_MAX_DOCUMENT_BYTES` | `2 MiB` | Per-document index admission limit |
| `FM_LOCAL_INDEX_MAX_BYTES` / `_MAX_DOCUMENTS` | `1 GiB` / `100000` | Logical index-content and document-count limits |
| `FM_LOCAL_ARTIFACT_PATH`     | _(unset)_                | Separate absolute source directory required by persistent modes |
| `FM_LOCAL_ARTIFACT_MAX_BYTES` / `_MAX_VALUE_BYTES` | `1 GiB` / `20 MiB` | Aggregate and per-source durable quotas |
| `FM_LOCAL_ARTIFACT_MAX_ENTRIES` | `100000` | URL-state/tombstone cardinality limit |
| `FM_LOCAL_ARCHIVE_MAX_VERSIONS` | `100000` | Aggregate distinct-version limit in archive mode |
| `FM_LOCAL_ARCHIVE_MAX_VERSIONS_PER_URL` | `16` | Distinct-version limit for one canonical URL in archive mode |
| `FM_RENDERER_URL`            | _(unset)_                | Enable headless render path           |
| `FM_RENDERER_EGRESS_PROXY_URL` | required with renderer | Browser traffic proxy enforced per dial |
| `FM_RENDERER_AUTO`           | `false`                  | Auto-upgrade js_required pages        |
| `FM_DASHBOARD_USER` / `_PASSWORD` | _(unset)_           | Enable `/dashboard/` when both set    |
| `FM_COMPAT_ANSWER_PROVIDER` | _(unset)_ | Explicit summarizer profile for Tavily `include_answer: advanced` |
| `FM_COMPAT_ANSWER_MAX_TOKENS` / `_TIMEOUT` | `512` / `30s` | Optional advanced-answer bounds |
| `FM_LOG_LEVEL`               | `info`                   | `debug` / `info` / `warn` / `error`   |

Full list: [`internal/config/config.go`](internal/config/config.go).

### External SearXNG

If you already run SearXNG, point at it:

```bash
export FM_SEARXNG_URL=https://my.searxng.example
docker compose -f deploy/docker-compose.external.yml up -d --build
```

### Run without SearXNG

SearXNG is an optional discovery lane rather than a boot dependency. Select an
enabled native or configured open-pack/federation source as the fallback and
leave both SearXNG URL variables empty:

```bash
export FM_DISCOVERY_ENABLED_SOURCES=wikipedia,crossref
export FM_DISCOVERY_PRIMARY_SOURCE=wikipedia
export FM_SEARXNG_URL=
export FM_SEARXNG_URLS=
docker compose -f deploy/docker-compose.external.yml up -d --build
```

Basic and advanced searches both use the enabled source-pack plan. Basic depth
issues at most one original-query lane per planned provider and uses the same
weighted RRF merge without generating advanced query variants. An explicit
`engines` request still means a SearXNG engine selection; without enabled
SearXNG, the native API returns HTTP 400 with `error=unsupported_control` and
`control=engines` rather than silently ignoring the control.

### Enable the native Mwmbl lane

Mwmbl is compiled into the built-in `general-open` pack but stays disabled by
default. Opt in explicitly when this small independent web index is useful:

```bash
export FM_DISCOVERY_ENABLED_SOURCES=searxng,wikipedia,crossref,mwmbl
```

Fetchmark uses Mwmbl's fixed official no-key JSON endpoint with a contactable
User-Agent, one concurrent request, and a conservative default of one request
per five seconds. The lane supports the original query plus Fetchmark-side
include/exclude domain filtering. Requests requiring engine/category,
language, time-range, nonzero safe-search, or exact-match semantics are rejected
for this lane so another compatible lane can answer instead.

### Enable the native Wiby lane

Wiby is also compiled into `general-open` and disabled by default. Opt in to
add its independently operated small-web index without adding a paid API or a
SearXNG dependency:

```bash
export FM_DISCOVERY_ENABLED_SOURCES=searxng,wikipedia,crossref,wiby
```

Fetchmark calls Wiby's fixed official no-key JSON endpoint with a contactable
User-Agent, one concurrent request, and a conservative default of one request
per five seconds. It never requests Wiby's opt-in NSFW results. Moderate safe
search therefore uses Wiby's filtered default; off or strict safe-search and
other controls Wiby cannot preserve are rejected for this lane so another
compatible source can answer.

Wiby's API terms ask consumers to link back when displaying its results.
Native, Tavily, Exa, and Brave search responses therefore include
`Link: <https://wiby.me/>; rel="via"; title="Wiby"` when the returned page
contains at least one Wiby result. The header is absent otherwise, and vendor
JSON response shapes do not change.

### Enable the native Stack Overflow lane

The `developer` pack includes an opt-in Stack Exchange API lane fixed to Stack
Overflow. It does not add a paid API or require an account key:

```bash
export FM_DISCOVERY_ENABLED_SOURCES=searxng,wikipedia,crossref,stackexchange
```

Fetchmark submits the full question as a hypothetical title to the official
`/2.3/similar` endpoint with the API's `withbody` filter, synthesizes only
`stackoverflow.com/questions/{id}` result URLs, and never pages implicitly.
Licensed question HTML returned by that same bounded API response is extracted
without scraping the result page; it is transient response input and is not
added to the local corpus or artifact store. Missing or unlicensed API bodies
remain discovery-only extraction failures and never fall through to a public
page fetch. The built-in source is capped at ten results, one concurrent
request, and one request per five seconds. It
obeys JSON `backoff`, stops after reported quota exhaustion, and suppresses a
semantically identical provider request for at least one minute. Anonymous
quota is shared by public IP and is intentionally not treated as a stable
numeric allowance.

Stack Exchange requires visible source attribution. Native results retain
Stack Overflow metadata and URLs; native, Tavily, Exa, and Brave responses also
emit Stack Overflow `via` and CC BY-SA `license` Web Linking relations whenever
that response page contains one of its results. API-supplied content adds its
whitelisted exact license version and author profile relation when available;
Exa-compatible content also carries the owner name. A UI displaying those
results must visibly identify Stack Overflow as the source. The lane remains disabled
by default: the preserved 20-case developer pilot found 20/20 non-empty
discovery but mixed unlabelled relevance. Its 7/200 extraction result predates
the provider-content path and remains historical evidence rather than a claim
about the current implementation; relevance judgments still gate enablement.

### Enable the native arXiv lane

The `research` pack includes an opt-in metadata-only lane using arXiv's public
Atom API. It requires no API key and adds no per-query charge:

```bash
export FM_DISCOVERY_ENABLED_SOURCES=searxng,wikipedia,crossref,arxiv
```

Fetchmark sends only bounded literal-term queries to the fixed
`https://export.arxiv.org/api/query` endpoint, requests at most one first page,
and emits canonical `https://arxiv.org/abs/...` URLs. Titles, abstracts,
authors, categories, and dates are descriptive metadata; the adapter never
returns PDFs or a provider body. Normal Fetchmark SSRF, robots, noindex, fetch,
and extraction policy therefore remains authoritative for page content.

The source is hard-capped to one connection and one request every three
seconds per process. arXiv applies that ceiling across all machines under an
operator's control, so only one replica may enable this lane unless the
operator supplies equivalent external coordination. Advanced queries can
queue multiple variants inside the configured 15-second lane budget; there is
no implicit pagination or retry.

Responses containing arXiv results preserve the requested acknowledgment,
“Thank you to arXiv for use of its open access interoperability,” through an
HTTP `Link` header without altering vendor JSON. CC0 identity is scoped to the
native result's `metadata_license` and `metadata_license_url` fields; it is not
attached to the mixed HTTP representation. Article bodies and linked PDFs
retain their own rights. Tavily and Exa requests can use this lane when their
controls permit it. The lane cannot contribute to Brave requests because
Brave's default language and safe-search semantics cannot be preserved by the
arXiv API; other planned providers continue independently. See the official
[API access page](https://info.arxiv.org/help/api/index.html),
[user manual](https://info.arxiv.org/help/api/user-manual.html), and
[API terms](https://info.arxiv.org/help/api/tou.html).

### Enable the native PubMed lane

The `research` pack also includes an opt-in PubMed lane. E-utilities is free
and needs no API key at the built-in rate, but NCBI expects distributed
software to send a real operator/developer email:

```bash
export FM_PUBMED_EMAIL=operator@example.org
export FM_DISCOVERY_ENABLED_SOURCES=searxng,wikipedia,crossref,pubmed
```

Each search uses one bounded ESearch request followed by one batched ESummary
request for at most 20 PMIDs. Fetchmark synthesizes only canonical
`https://pubmed.ncbi.nlm.nih.gov/{PMID}/` URLs and maps bibliographic metadata:
title, authors, journal, publication date/type, language, DOI/PMCID, and record
status. It never requests abstracts or full text. A transient metadata-only
document prevents the normal pipeline from fetching the PubMed result page and
is cleared before serialization without entering the local corpus or artifact
store.

The built-in source permits one search at a time and two E-utility requests per
second with burst one, below NCBI's anonymous three-request-per-second ceiling.
That gate is process-local while NCBI applies its limit per public IP, so
multi-instance operators must divide the configured source rate. HTTP 403/429,
bounded `Retry-After`, and the documented JSON rate-limit error open a provider
cooldown. Exact match, language, safe-search, engine, and category controls are
rejected rather than weakened; day/week/month/year publication windows and
local domain filtering are supported.

PubMed therefore does not contribute to Brave-compatible searches, whose
protocol defaults always require English-language and moderate-safe-search
semantics. Other configured lanes continue independently; a PubMed-only
process cannot serve the Brave search route.

Native metadata and compatibility response Link headers retain PubMed/NLM
source identity plus NLM's terms, disclaimer, and copyright notice. Abstracts
and linked articles have their own rights; downstream UIs must preserve the
source acknowledgment, avoid implying NLM endorsement, and disclose that the
query-time metadata may become stale. See NCBI's
[E-utilities usage rules](https://www.ncbi.nlm.nih.gov/books/NBK25497/),
[parameter reference](https://www.ncbi.nlm.nih.gov/books/NBK25499/), and
[NLM data terms](https://www.nlm.nih.gov/databases/download.html).

### Enable an operator-controlled YaCy lane

The `general-open` pack includes a disabled, neutral-weight YaCy lane. It binds
to one node selected and operated by you; Fetchmark never chooses random public
YaCy nodes or uses them as traffic proxies. For a Compose-internal HTTP node:

```bash
export FM_YACY_URL=http://yacy:8090
export FM_YACY_ALLOW_INSECURE_HTTP=true
export FM_YACY_RESOURCE=local
export FM_DISCOVERY_ENABLED_SOURCES=searxng,wikipedia,crossref,yacy
```

HTTPS is required by default. Plain HTTP needs the explicit flag above and
should remain on a private service network. The origin cannot contain
credentials, a path, query, or fragment. Fetchmark uses a separate
host-allowlisted infrastructure client and refuses redirects; discovered result
URLs still pass through the ordinary public-web SSRF, robots, noindex, and
extraction path.

The adapter makes one first-page `/yacysearch.json` request, caps results at 20
under the built-in pack, runs at one request per five seconds with one request
in flight, and sends `verify=false` so YaCy does not fetch result pages merely
to verify or generate snippets. Descriptions are treated as untrusted display
text and stripped to a bounded plain-text hint. A custom pack may raise a local
node's rate up to ten requests per second; global mode stays hard-capped at one
request per five seconds to protect the shared peer network.

`FM_YACY_RESOURCE=local` searches only the selected node's index; a valid empty
response is authoritative only for that local corpus. `global` asks that node
to federate with its YaCy peers, which discloses query terms to that peer
network. Global empties are always degraded rather than authoritative, and a
server-reported fallback to local mode is retained as `global_downgraded`
diagnostic evidence. Missing resource evidence and malformed rows also prevent
a fully healthy classification.

The lane remains disabled because an adapter does not create an index. Before
enabling or weighting it by default, run the fixed evaluation suite against a
populated operator-controlled node and retain the runtime fingerprint, source
contribution, latency, domain, and blind relevance evidence. See the official
[YaCy project](https://yacy.net/), [FAQ](https://yacy.net/faq/), and
[search API](https://wiki.yacy.net/index.php/Dev%3AAPIyacysearch).

### Persistent local index (optional)

The local flywheel is disabled by default. With Compose, enable it on the
pre-mounted persistent volume:

```bash
FM_LOCAL_CORPUS_MODE=personal \
FM_LOCAL_INDEX_PATH=/var/lib/fetchmark/index \
FM_LOCAL_ARTIFACT_PATH=/var/lib/fetchmark/artifacts \
docker compose -f deploy/docker-compose.yml up -d --build
```

Cold live retrievals are indexed only after an authoritative robots.txt
decision and no applicable `X-Robots-Tag` or HTML `noindex`/`noarchive`. Denials and
takedowns leave bodyless ordered tombstones, so an older concurrent fetch
cannot resurrect content. Cache hits and renderer-only retrievals are not
promoted without fresh header evidence. Personal mode stores permitted source
bytes in a separate content-addressed directory and uses ETag/Last-Modified
revalidation after the ordinary response cache expires. Expired entries stop
participating in reads immediately and a single sequential background sweep
physically reclaims both stores at `FM_LOCAL_EXPIRY_SWEEP_INTERVAL`. Both
directories are unencrypted local data. The artifact adapter holds an exclusive
OS lock for its lifetime and rejects a second writer; operators must still
protect the directory from unrelated filesystem mutation. `ephemeral` keeps
only a process-local lexical index.
`curated` requires both persistent paths, `FM_RESPECT_ROBOTS=true`, and at
least one `FM_ADMIN_API_KEYS` value. Ordinary search and parse requests never
admit permitted content in this mode. An admin explicitly submits URL-only
batches to `POST /admin/corpus/admissions`; Fetchmark bypasses its response
cache, performs a fresh SSRF-checked retrieval with the configured policy user
agent, and admits only authoritative robots-allowed, indexable, extractable
`200` responses. Admissions default to safety-unclassified; an operator may
explicitly assert `safe` or `unsafe` for the whole batch with
`safety_classification`. Fetchmark never infers that assertion from untrusted
page text. `POST /admin/corpus/takedowns` writes a sticky tombstone and
purges the retained source. Both responses are bodyless per-URL outcomes.
Curated entries do not expire automatically but remain bounded by the existing
index/artifact byte and cardinality quotas.

`archive` is a separate opt-in artifact schema. Ordinary policy-permitted live
retrievals feed it automatically, while Bleve remains a current-version-only
search projection. The artifact store retains one immutable manifest and body
per distinct source hash, bounded by aggregate bytes, URL states,
`FM_LOCAL_ARCHIVE_MAX_VERSIONS`, and
`FM_LOCAL_ARCHIVE_MAX_VERSIONS_PER_URL`. Repeat bytes and `304` validation do
not consume another version; quota exhaustion rejects new retention without
evicting prior history. Robots, noindex, noarchive, and authenticated takedown
replace the current pointer with a bodyless tombstone before purging all
versions for that URL. Archive requires both paths, robots enforcement, and an
admin key. Use a new empty artifact directory: schema-v1 personal/curated stores
are not migrated implicitly. This is bounded application history, not a WARC or
legal-preservation system, and no public history-serving API is exposed.

The index remains lexical and CPU-only;
embeddings and local LLMs are not required. See [ADR 0002](docs/adr/0002-personal-corpus-artifacts-and-revalidation.md).

Automatically pipeline-fed documents remain safety-unclassified. They
participate in queries with safesearch `0`; moderate/strict safe-search queries
admit only documents explicitly asserted `safe` through curated admission.

### Optional focused ingestion

`fetchmark-crawl` is a separate, curated-only ingestion worker. It never opens
the Bleve index or artifact directory. Instead, it keeps a dedicated bounded
bbolt frontier and submits one URL at a time to the authenticated
`POST /admin/corpus/focused-admissions` boundary together with the job's
validated allow/deny path policy. Fetchmark re-enforces that policy on every
redirect, requires the original scheme and authority, performs the fresh
SSRF-checked, robots-aware fetch, and enforces noindex/noarchive before storing
anything. No page body or safety assertion crosses this control-plane request.
Archive admission remains automatic-only.

Start from `deploy/crawler.example.json`, replace the non-routable example
scope, and validate it without network or state access:

```bash
go run ./cmd/fetchmark-crawl \
  -config "$PWD/deploy/crawler.example.json" \
  -dry-run
```

For one bounded live run, use absolute paths and a dedicated key that is also
listed in `FM_ADMIN_API_KEYS` on the Fetchmark server:

```bash
mkdir -p "$PWD/.fetchmark-crawler"
FM_CRAWLER_FETCHMARK_URL=http://127.0.0.1:8080 \
FM_CRAWLER_ADMIN_API_KEY=replace-with-dedicated-admin-key \
go run ./cmd/fetchmark-crawl \
  -config "$PWD/deploy/crawler.json" \
  -state "$PWD/.fetchmark-crawler/frontier.db" \
  -once
```

The job file is strict version 3. Page identities, source identities,
page memberships, source edges, and one-hop link edges have separate global
caps. Domains and path prefixes are explicit;
wildcards, credentialed URLs, unknown fields, duplicate canonical seeds, and
unbounded counts or intervals are rejected. Removed seeds become disabled
frontier records rather than being silently deleted. Successes refresh on
`refresh_interval`, policy rejections use `rejection_recheck_interval`, and
transient failures use bounded deterministic backoff plus server
`Retry-After`. Authentication, mode, fixed-endpoint, and protocol-contract
errors stop the run. The API key is environment-only and the fixed-origin
client refuses redirects.
Per-authority dispatch reservations are atomic and persistent; restarts and
jobs sharing an authority honor the larger of the previous and current minimum
interval. Ambiguous dot-segment or encoded-separator paths are rejected. Focused
page redirects are deliberately same-scheme, so even HTTP-to-HTTPS redirects
are rejected and rescheduled as bounded policy outcomes.
The state database's direct parent must be a trusted directory that is not
group- or world-writable. Disabled identities count against the frontier cap;
normal runs never evict them. If seed churn exhausts that lifetime identity
budget, archive the separate frontier database and choose a new state path (or
use the adapter's explicit disabled-entry prune in an operator tool).

The `crawl` Compose profile uses its own `fetchmark-crawler-data` volume and no
corpus volume. Copy `deploy/crawler.example.json` to `deploy/crawler.json`, edit
it, configure `FM_CRAWLER_ADMIN_API_KEY`, then run:

```bash
docker compose -f deploy/docker-compose.yml --profile crawl run --build --rm crawler
```

Before each page-admission pass, the worker polls due configured sitemap,
sitemap-index, RSS, and Atom sources. Source requests use a stable contactable
identity, the public-internet SSRF/rebinding policy, RFC 9309 checks, strict
same-origin/path redirects, per-root compressed/decompressed limits, and the
same persistent per-authority pacing as page admissions. Successful source
snapshots atomically replace page and child-source memberships; 304 responses
keep the last snapshot, while fetch and parse failures leave it intact and
schedule a bounded retry. ETag/Last-Modified and conservative HTTP freshness
are persisted. Discovered pages still cross the normal Fetchmark admission
boundary before retention.

Optional `link_expansion` asks that same admission boundary for at most 64
canonical reader-view links on a bounded number of directly owned pages per
run. Accepted snapshots are applied atomically in the schema-v6 frontier.
Expansion is deliberately one hop: a link-only child cannot publish another
snapshot unless it later becomes an explicit seed or gains active sitemap/feed
ownership. Links outside the job's domain/path policy, self-links, `rel=nofollow`
anchors, and pages carrying applicable `nofollow` robots metadata are not
followed. Transport/storage failures preserve the last snapshot; an admitted
empty snapshot or authoritative policy rejection clears it. Operator-owned
developer, research, news, and knowledge starting configurations live under
`deploy/crawler-packs/` and never activate automatically.
WebSub remains deferred because its inbound callback, lease, and signature
lifecycle is a separate security surface. See
[ADR 0004](docs/adr/0004-separate-focused-ingestion.md) and
[ADR 0005](docs/adr/0005-polite-discovery-source-polling.md), plus
[ADR 0006](docs/adr/0006-bounded-one-hop-link-expansion.md).

### Headless rendering (optional)

JS-heavy pages (SPAs, lazy-loaded content) can be sent through a headless
browser. Enable the `render` profile:

```bash
FM_RENDERER_URL=http://chromium:3000/content \
FM_RENDERER_TOKEN=$(openssl rand -hex 32) \
docker compose -f deploy/docker-compose.yml --profile render up -d --build
```

Compose routes every Chromium request through Fetchmark's private egress
proxy on port 8081. For external renderer deployments, set
`FM_RENDERER_EGRESS_PROXY_URL` to a proxy reachable by the renderer and keep
that proxy private; startup fails closed when rendering is enabled without it.

Then pass `"render": true` on `/v1/parse`, or set `FM_RENDERER_AUTO=true` to
auto-upgrade any page the extractor flags as `js_required`.

---

## Architecture

```
┌────────────┐  ┌──────────┐   ┌─────────────┐    ┌────────────┐
│  /v1/*     │─▶│ Pipeline │──▶│   Fetcher   │───▶│  Targets   │
│  handlers  │  │          │   │  (egress,   │    │ (internet) │
└────────────┘  │  ┌────┐  │   │   proxy,    │    └────────────┘
                │  │rank│  │   │   robots)   │
                │  └────┘  │   └─────────────┘
                │  ┌────┐  │   ┌─────────────┐    ┌────────────┐
                │  │dedu│  │   │  Extractor  │    │  SearXNG   │
                │  └────┘  │   │ (markdown)  │    │ (1..N      │
                │  ┌────┐  │   └─────────────┘    │  failover) │
                │  │cach│◀─┼───▶┌───────────┐     └────────────┘
                │  └────┘  │    │   Redis   │
                └──────────┘    │ (cache +  │
                                │ ratelim + │
                                │  lock)    │
                                └───────────┘
```

Layered along the classic ports & adapters pattern:

- `internal/core/pipeline` — orchestration
- `internal/core/discovery`, `.../rank`, `.../search`, `.../model` — domain
- `internal/adapters/{searxng,wikipedia,crossref,arxiv,mwmbl,wiby,stackexchange,github,pubmed,yacy,searchbudget,discoverycache,fetcher,extractor,renderer,cache,egress,robots}` — IO
- `internal/api` — presentation (handlers, middleware, dashboard)

Package docs live under [`docs/`](docs/).

---

## Development

```bash
make test       # go test -race ./...
make build      # binary at bin/fetchmark
make run        # run locally against env vars
make eval-check # validate the fixed offline evaluation suite
make docker     # build distroless image
```

Live evaluation is separate from unit tests and opt-in. It refuses to
overwrite an existing result file:

```bash
FM_EVAL_API_KEY="$FM_API_KEY" go run ./cmd/fetchmark-eval \
  -live \
  -endpoint http://127.0.0.1:8080/v1/search \
  -revision "$(git rev-parse HEAD)" \
  -configuration-id general-open-v1 \
  -require-build-sha256 \
  -configuration-manifest-output eval-configuration-$(date -u +%Y%m%dT%H%M%SZ).json \
  -require-configuration-sha256 \
  -output eval-run-$(date -u +%Y%m%dT%H%M%SZ).jsonl
```

The evaluator retains the startup-resolved executable artifact SHA-256, the
exact non-secret resolved-configuration manifest and digest, plus typed broker
provenance for source contribution and overlap reports. The configuration ID
remains an explicit, separate operator boundary. Private endpoints, keys,
contacts, paths, and federation/open-pack binding names are never written to
the manifest. It can also generate blind
0–3 relevance-label templates, produce a self-contained keyboard-friendly
offline labeling workbench, and score completed labels without a service or
model. The versioned 120-query baseline, artifact schema, and judgment workflow
are documented in
[`eval/README.md`](eval/README.md).

The full suite runs in <10s and is race-enabled. Add new behaviour with a
test first; see [`internal/core/pipeline/*_test.go`](internal/core/pipeline)
for patterns (stub adapters, table-driven cases).

---

## Operations

- **Metrics.** Prometheus exposition at `/metrics`. Key series:
  `fetchmark_fetch_outcome_total`, `fetchmark_extract_outcome_total`,
  `fetchmark_cache_events_total`, `fetchmark_searxng_instance_up`,
  `fetchmark_discovery_batch_total`,
  `fetchmark_discovery_batch_duration_seconds`,
  `fetchmark_discovery_results`,
  `fetchmark_discovery_diagnostic_total`,
  `fetchmark_discovery_cache_events_total`,
  `fetchmark_renderer_outcome_total`, `fetchmark_summarize_total`,
  `fetchmark_summarize_duration_seconds`,
  `fetchmark_summarize_tokens_total`, HTTP latency histograms.
- **Dashboard.** Set `FM_DASHBOARD_USER` + `FM_DASHBOARD_PASSWORD` to enable
  `/dashboard/`. Shows health, readiness, selected metrics, SearXNG health,
  redacted runtime config, and summarize provider names. Read-only; no
  mutating actions.
- **Logs.** Structured JSON via `log/slog`, with a request ID on every line.
- **Graceful shutdown.** `SIGTERM` drains in-flight requests, flushes
  metrics, and closes Redis.

---

## Security

- SSRF-hardened egress policy (private/link-local/loopback blocked by
  default, redirect chain revalidated, scheme-downgrade refused). Chromium
  rendering uses a connection-time egress proxy so redirects, subresources,
  and second DNS resolutions receive the same checks.
- Body + decompressed size caps enforced pre-read.
- Admin-only `proxy_url` passthrough: non-admin keys get 403.
- API keys are compared with a constant-time check.
- MIT-licensed; zero telemetry, zero phone-home.

Found a security issue? Please open a **private** security advisory on the
repository.

---

## Roadmap

Live backlog: [`docs/tasks.md`](docs/tasks.md). Open items:

- SSE streaming of search results (deferred pending demand)
- SimHash/MinHash dedupe (declined while result cap stays ≤ 50)
- Cross-instance summarizer config replication via Redis pub/sub (deferred;
  env vars remain authoritative, per-instance admin overrides are in-process)

### Summarize — configuration

The `/v1/summarize` endpoint accepts a `url` and calls the configured LLM
through either the OpenAI or Anthropic wire format. Any proxy that speaks
the OpenAI Chat Completions shape works (SubSandwich, Groq, Together,
Azure OpenAI, etc.). Configure via env:

```bash
# Required to enable the endpoint (at least one provider):
FM_SUMMARIZE_OPENAI_BASE_URL=http://localhost:4141/v1/
FM_SUMMARIZE_OPENAI_API_KEY=placeholder       # sent literally; use your key otherwise
FM_SUMMARIZE_OPENAI_MODEL=glm-5.1
FM_SUMMARIZE_OPENAI_TIMEOUT=60s
FM_SUMMARIZE_OPENAI_MAX_TOKENS=1024
FM_SUMMARIZE_OPENAI_THINKING=false            # set true + THINK_EFFORT for o-series
FM_SUMMARIZE_OPENAI_THINK_EFFORT=medium       # low|medium|high

FM_SUMMARIZE_ANTHROPIC_BASE_URL=https://api.anthropic.com/
FM_SUMMARIZE_ANTHROPIC_API_KEY=sk-ant-...
FM_SUMMARIZE_ANTHROPIC_MODEL=claude-3-5-sonnet-latest
FM_SUMMARIZE_ANTHROPIC_MAX_TOKENS=1024
FM_SUMMARIZE_ANTHROPIC_THINKING=false
FM_SUMMARIZE_ANTHROPIC_THINK_BUDGET=2048      # >= 1024 required by Anthropic

FM_SUMMARIZE_DEFAULT_PROVIDER=openai          # "openai" or "anthropic"
```

Per-request overrides (`provider`, `model`, `max_tokens`, `temperature`,
`instructions`, `thinking`, `timeout_ms`) are supported on the POST body.
For non-admin API keys, `provider`, `model`, and `thinking` overrides are
disabled unless explicitly allowed by config. `max_tokens`, `timeout_ms`,
`instructions`, and `thinking.budget_tokens` caps apply to all callers,
including admins. Admins can also upsert providers at runtime via
`/admin/summarize/providers` (see the OpenAPI spec); those overrides live
in-process and revert to env on restart.

**Docker compose + SubSandwich:** when running Fetchmark in the compose
stack and SubSandwich on the host, point the base URL at
`http://host.docker.internal:4141/v1/`.

---

## License

[MIT](LICENSE).
