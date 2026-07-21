# Fetchmark tasks

Live backlog. Each phase is tracked in the session SQL database with
dependencies; this file mirrors it for humans.

## In progress

- Open discovery Phase 0 — the initial balanced 120-query suite and opt-in live
  evaluator now record non-empty rate, domains, extraction success,
  publication/expected-domain relevance proxies, latency, errors, and raw
  engine evidence without making live probes part of unit tests. Typed
  provider/lane/variant provenance now survives canonical, exact-content, and
  near-duplicate fusion and drives provider/lane contribution, provider-pair overlap, and
  unique-domain reports. Strict offline artifacts support blind run-bound 0–3
  human labels, explicit coverage, NDCG@10/MRR, and source-level relevance. A
  self-contained offline workbench now makes those judgments practical with
  keyboard grading, a one-tab-stop arrow-navigable evidence rail, live
  auto-advance announcements, fully bound browser-local recovery, draft
  import/export, strict run binding, and scorer-compatible completed JSONL
  while omitting provider identity;
  live runs can retain their revision and configuration identity. The server
  now fingerprints its startup-resolved executable artifact in native search
  responses, and baseline runs can require one consistent SHA-256 across all
  HTTP observations while preserving legacy artifacts that predate this
  evidence. Baselines can now fetch the exact bounded, non-secret
  startup-resolved configuration manifest before a run and require its digest
  consistently across every native HTTP response. Private endpoints, paths,
  contacts, User-Agent values, and federation/open-pack binding names are
  excluded. Endpoint userinfo/query values are removed before fingerprinting;
  sensitive configuration shape is count/boolean-only, private bindings are
  digest-only, and dynamic state limitations remain explicit. A
  deterministic `-per-intent` selector, a single-category `-intent` selector,
  and three preserved 12-query native-open artifacts provide the first balanced
  pilot comparison. Complete 120-query
  original/remediated concurrency-4 and serial native-open artifacts now
  establish the capacity boundary, its correction, and the first full quality
  baseline. Both the serial and remediated capacity runs completed 120/120
  requests with 65 non-empty cases and full discovery evidence. The serial run's
  exclusive 378-row blind template remains unfilled. Remaining: complete human
  judgments and use the resulting evidence to tune source weights. Native
  success and failure envelopes now preserve bounded aggregate/lane discovery
  status and normalized diagnostics; evaluator artifacts validate and
  summarize this evidence without exposing provider instances or raw errors.
- Open discovery Phase 1 — richer provider batches, SearXNG degraded-empty
  failover, failed-attempt provenance, and late-page partial recovery are
  implemented alongside per-instance/per-engine EWMA, separate
  transport/quality circuits, Retry-After, bounded exponential backoff, and a
  bounded process-local discovery decorator for identical-query coalescing and
  stale-while-revalidate. Provider, federation, corpus-client, and focused-crawl
  adapters now share one RFC-aware Retry-After parser that clamps untrusted
  decimal seconds before duration conversion. Native and declaratively budgeted
  providers also share one admission gate that acquires concurrency before
  spending rate capacity, preventing queued/canceled calls from reserving future
  tokens and later bursting upstream. A canceled cold fill now releases provider
  work after its final waiter leaves without disrupting another identical waiter
  or detached stale refresh. Low-rate Wiby/Mwmbl lanes allow 30 seconds for safe
  admission queueing while retaining their one-call, 0.2-request/second, burst-one
  upstream limits. Empty, partial, failed, canceled, and oversized outcomes are
  never retained. Advanced depth now executes an ordered,
  concurrency-bounded source/variant lane plan, preserves successful and
  partial lanes across unrelated failures, performs weighted canonical RRF,
  and measures final top-k source contribution and domain diversity with
  bounded labels.
- Open discovery Phase 2 — strict v1 declarative source packs now route stable
  general/developer/research/knowledge/fresh intents. Basic and advanced search
  now share the provider plan: basic runs one original lane per provider while
  advanced adds controlled variants. Discovery includes
  native Wikipedia and Crossref lanes with fixed safe endpoints, contactable
  identity, global source budgets, bounded bodies, cooldowns, diagnostics, and
  independent cache/coalescing partitions. An opt-in native arXiv research
  lane now adds metadata-only Atom discovery with literal query construction,
  canonical abstract URLs, strict one-request-per-three-seconds and
  single-attempt/single-connection caps, bounded cooldowns, scoped metadata
  licensing, and response acknowledgment. It
  remains neutral-weight and disabled by default pending evaluation. An
  opt-in native Mwmbl general-web
  lane now adds a fixed official no-key endpoint, conservative shared-service
  budgets, honest unsupported-control handling, and typed partial/failure
  diagnostics without changing the default enabled sources. A second opt-in
  native Wiby lane adds a 12-result small-web source with the same conservative
  shared-service controls and conditional HTTP Link attribution, while leaving
  compatibility JSON untouched. An opt-in Stack Overflow-only developer lane
  now uses the official Stack Exchange `/similar` API with fixed-origin result
  synthesis, one-page limits, JSON backoff/quota cooldowns, a one-minute
  duplicate-request guard, and required source/license attribution. A live
  diagnosis showed the ordinary Go page fetch receiving a Cloudflare 403
  challenge, so the adapter now requests licensed question HTML through the
  official API's `withbody` filter. The pipeline extracts that transient body
  without scraping the result URL or retaining it in local persistence. A
  one-query end-to-end smoke extracted all three returned top results with no
  page-fetch outcome or robots block. A separate fingerprinted rerun of all 20
  fixed developer cases completed 20/20 requests, returned 200 results, and
  extracted 190/200 without a public-page fetch outcome. The rerun's new
  200-row blind template remains unfilled. A same-binary combined run with
  Wiby and Mwmbl also completed 20/20, kept 190/200 extraction coverage, and
  expanded the final set from one to eight domains; Stack Exchange still
  supplied 193/200 final results. Its separate blind template is also unfilled.
  The
  developer classifier now recognizes the technology families represented by
  the fixed suite instead of routing only a small subset into that pack. The
  preserved serial developer pilot returned 200 typed-provenance URLs across
  20/20 non-empty cases with healthy discovery lanes, but only 7/200 pages
  extracted and qualitative inspection found mixed ranking relevance. The
  original pilot's 200-row blind template also remains unfilled, so the Stack
  Exchange lane remains
  disabled by default and its weight remains neutral. SearXNG remains
  the default first source but is optional; another enabled primary can run
  with empty SearXNG URLs, and engine pinning without it returns a typed 400.
  Remaining: more
  evaluated open-source adapters and operator-tuned weights based on Phase 0
  run artifacts, including whether Mwmbl and Wiby materially improve general-web
  coverage and relevance. A one-query SearX-free live smoke is recorded in
  `docs/benchmarks/mwmbl-live-smoke.md`; it proves the end-to-end runtime path,
  not representative quality. A balanced 12-query SearX-free pilot and its
  concurrency follow-ups are recorded in
  `docs/benchmarks/native-open-pilot-20260718.md`; they expose major
  general/developer/long-tail coverage gaps and do not justify weight changes.
  The opt-in Wiby follow-up in
  `docs/benchmarks/wiby-native-pilot-20260718.md` raises the observed balanced
  pilot to 7/12 non-empty cases and 33 unique domains, but weak unlabelled
  general relevance and the unchanged long-tail gap still preclude a default
  enable or weight change. The complete serial run in
  `docs/benchmarks/native-open-full-baseline-20260718.md` records 65/120
  non-empty cases and 200 domains, while still exposing only 3/20 long-tail and
  5/20 developer coverage. The original concurrency-4 companion failed 85
  requests; after orphan cancellation and bounded queue headroom, its complete
  concurrency-4 rerun passed 120/120 with no Wiby/Mwmbl failure diagnostic and
  unchanged provider rate/concurrency limits. Relevance labels remain unfilled,
  so neither weights nor provider defaults change yet.
  The isolated Stack Overflow developer pilot and provider-content rerun are
  recorded in
  `docs/benchmarks/stackexchange-developer-pilot-20260718.md`; it closes the
  observed URL-discovery and API-content extraction gaps for that source but
  does not establish relevance. The original artifact's 7/200 extraction count
  predates the provider-content remediation and remains unchanged; the separate
  fingerprinted rerun records 190/200. The combined run adds seven open-web
  results but shows mixed qualitative relevance and a higher p95 latency, so it
  also does not justify a weight or default change before blind scoring.
  An additional opt-in native GitHub repository lane now uses the official
  anonymous search endpoint with a pinned API version, qualifier-safe bounded
  query projection, one-page metadata-only responses, strict redirect refusal,
  incomplete-result diagnostics, and shared-IP rate/reset cooldowns. It makes
  no provider-side core-API or README-content calls; selected result pages use
  the ordinary Fetchmark fetch policy. The rate gate is process-local, so
  multi-instance operators sharing egress must lower each source-pack rate.
  Its fingerprinted 20-case developer
  pilot completed 20/20 requests, returned and extracted 186 repository pages,
  and preserved healthy typed discovery evidence throughout, but every final
  result remained on `github.com` and the official-document proxy stayed at
  zero. The 186-row blind template is unfilled, so the source remains disabled
  and neutral-weighted. Post-pilot tests additionally prevent caller Boolean
  operator injection and bind one rate token to one non-replayed HTTP attempt;
  the preserved live artifact is not rewritten or misidentified as using those
  later safeguards. The evidence and decision gate are recorded in
  `docs/benchmarks/github-developer-pilot-20260719.md`.
  An opt-in native PubMed research lane now uses the fixed no-key NCBI
  E-utilities origin with a required operator/developer email, bounded lexical
  query projection, one ESearch plus one batched ESummary request, conservative
  shared-IP budgets, JSON/HTTP cooldown evidence, and canonical PubMed URLs.
  It maps bibliographic metadata into a transient no-fetch provider document
  but never requests abstracts or full text, so ordinary search cannot retain
  publisher/author-owned abstracts through this adapter. Native metadata and
  compatibility Link relations preserve NLM attribution, terms, disclaimer,
  copyright, and observation-time staleness boundaries. Exact, language,
  safe-search, engine, and category semantics are rejected rather than
  weakened. The source remains disabled and neutral-weighted until a
  fingerprinted 20-case research pilot and blind relevance template are
  preserved; an unretained feasibility probe is not sufficient to change
  defaults.
  An opt-in native YaCy general-web lane now binds one explicitly configured
  operator-controlled node, keeps private infrastructure access separate from
  result-page egress, refuses redirects, disables YaCy result verification,
  and reports local/global effective-resource evidence. Local empty is
  authoritative only for that node's corpus; global empty is degraded, and a
  server fallback to local becomes a typed `global_downgraded` partial result.
  The lane remains disabled and neutral-weighted because the adapter creates no
  index and no populated-node suite has been retained. Remaining: benchmark a
  pinned operator-owned YaCy node before any default or weight change.
- Open discovery Phase 3 — explicit Tavily, Exa, and Brave compatibility
  prefixes translate into one canonical Fetchmark search path. Tavily supports
  deterministic cited answers and an explicitly configured optional local LLM;
  Exa supports search plus per-URL contents statuses; Brave supports its core
  web-search query and response shapes. Vendor-specific auth, rate-limit, and
  response-budget errors are preserved. A pinned opt-in loopback smoke now
  verifies Tavily Python 0.7.26 search, Exa Python 2.16.0 search/contents, and
  Brave's documented Python HTTP request against the real Fetchmark router
  without vendor credentials or calls. Remaining: broaden the contract corpus
  only where Fetchmark can preserve semantics, test additional official client
  languages where configurable, and refresh the pinned smoke deliberately as
  vendor SDKs evolve.
- Open discovery Phase 4 — an opt-in CGo-free Bleve index now runs in parallel
  with live discovery for basic and advanced searches, and cold permitted
  retrievals feed it through a fail-closed retention port. Response-header and
  HTML noindex, authoritative robots decisions, bodyless ordered tombstones,
  safe-search exclusion for unclassified documents, canonical replacement,
  bounded documents/filter tokens, and reopen persistence are covered by race
  tests. Disabled/ephemeral/personal modes are now explicit; personal mode adds
  an expiring, quota-bounded, content-addressed filesystem source store with
  atomic URL pointers, schema/startup repair, ETag/Last-Modified conditional
  revalidation, one-shot unconditional fallback, and coordinated noindex/
  takedown revocation. A single cancelable worker now physically sweeps expired
  index and artifact entries with fixed-label metrics. The reader-view extractor
  now feeds bounded headings and resolved HTTP(S) outbound links into the index,
  while one raw-source SHA-256 identifies both the artifact and its Bleve
  projection without changing public response JSON. Bleve rejects incompatible
  mapping schemas instead of opening them silently. Curated mode now provides
  admin-only fresh URL admission and sticky takedown routes while ordinary
  requests remain revocation-only. Curated batches may also carry a validated
  operator safety assertion while automatic content remains fail-closed as
  unclassified. A reproducible 100/1,000-URL artifact baseline exposed the
  full-tree hot-path accounting cost; checked mutation-local deltas now keep
  successful-write allocations flat across those sizes while complete scans
  remain for repair/error paths. The artifact store now rejects concurrent
  owners and repairs validated crash-temp files. Archive mode adds bounded
  distinct-version history and exact per-URL purge while keeping Bleve
  current-only. An exact Linux/arm64 container comparison now records a 20.45%
  main-binary and 26.22% compressed main-layer increase over the pre-Bleve
  revision while separating the three optional companion binaries from that
  cost. A network-free Linux/arm64 lifecycle gate now exercises 10k, 50k, and
  the public-cap 100k individual reconciliations. The 100k run sustained 171
  documents/second, produced a 358.73-MB closed index, reopened in 850.71 ms,
  searched at 2.689-ms p50 and 4.271-ms p95, and peaked at 593.47 MB under a
  1-GiB cgroup. The 10k control occupied 40.17 MB and peaked at 106.65 MB under
  512 MiB. Public document and logical-byte caps remain unchanged.
- Open discovery Phase 5 — a separate curated-only `fetchmark-crawl` worker now
  validates strict scoped jobs, persists a bounded exclusive bbolt frontier,
  applies per-job/per-host scheduling and bounded retries, and sends one
  URL plus its validated allow/deny path policy at a time through the dedicated
  fixed-origin authenticated focused-admission boundary. Fetchmark constrains
  every redirect to the original scheme/authority and that policy. Host pacing
  is atomic and persistent across jobs and restarts. Dry-run has no network or state effects; the optional
  Compose profile owns only its separate state volume. Configured sitemap,
  sitemap-index, RSS, and Atom sources now poll before page admissions through
  external SSRF/rebinding protection, RFC 9309 checks, exact redirect scope,
  per-root compression limits, validators, freshness, `Retry-After`, and the
  same persistent host budget. Atomic source snapshots feed root-scoped page
  and child-source ownership while 304 and failure outcomes preserve the last
  successful snapshot. Strict config v3 and frontier schema v6 now add atomic
  one-hop reader-view link snapshots with independent edge caps, direct-owner
  gating, nofollow handling, scope filtering, and non-recursive derived page
  scheduling. Versioned developer/research/news/knowledge operator templates
  live under `deploy/crawler-packs/` and never activate automatically. WebSub
  remains a later optional sidecar.
- Open discovery Phase 6 — a neutral signed manifest plus content-addressed
  NDJSON/Zstandard shard format now keeps pack distribution independent of
  Bleve. An offline `fetchmark-pack` command verifies exact Ed25519 publisher,
  digest, revision, count, and validity bindings from a strict operator
  registry, then builds and atomically activates an immutable CPU-only lexical
  projection. Explicit `openpack` sources participate only in advanced source
  packs, remain disabled by default, fail closed for safe-search, and still
  pass discovered URLs through Fetchmark's live SSRF/robots/noindex pipeline.
  Profiling reduced the deterministic 10,000-record median install to 1.006
  seconds at about 9,945 records/s, with a roughly 15.36-MB projection and
  about 0.730 ms per sampled query on an Apple M4 Pro. A test-only 100,000
  record run completed in 9.08 seconds with a 139-MB projection, 361-MiB peak
  RSS, 11.63-ms cold open, and about 6.93 ms per broad sampled query; the public
  cap remains 10,000. Multi-batch cancellation and late duplicate-record
  corruption now prove transactional cleanup of both active and staging paths.
  Staging-cleanup failures are joined into the install error instead of being
  silently discarded. Before activation, the installer reopens the closed
  projection and verifies the complete marker identity, validity window, and
  signed record count against Bleve's document count.
  Path validation accepts writable sticky ancestors only at canonical
  root-owned operating-system temporary roots; all existing components and
  projection directories must be owned by the effective user or root, closing
  the arbitrary-sticky-parent activation race.
  Operators can lower a hard 512-MiB logical projection activation ceiling, and
  both verification and installation report the measured projection bytes. The
  limit is deliberately documented as an activation gate rather than a
  filesystem quota. A reproducible varied 100k harness now runs the exact
  install/reopen/search path under a network-disabled Linux container while an
  unexported test policy preserves the public 10k cap. Three 512-MiB runs had a
  403.02-MiB median and 405.36-MiB maximum cgroup peak, a 12.82-second median
  install, and a 206.51-MB median projection. A varied 10k control passed under
  192 MiB with a 131.30-MiB peak; the documented lightweight recommendation is
  256 MiB, while a future 100k operator profile should reserve at least 640 MiB.
  A separate private test policy now proves four-shard installation at 250k,
  500k, and one million records without changing any public limit. The 1M run
  built a 1.930-GB projection in 110.435 seconds at 9,055 records/s, completed
  96 searches at 5.592-ms p50 and 10.652-ms p95, and peaked at 2.505 GiB under
  an exact 5-GiB cgroup limit. A separate offline publisher companion now
  consumes an exact digest-pinned, admission-enriched Common Crawl URL Index
  export; enforces current robots/noindex evidence, explicit URL-metadata
  rights evidence, exclusion and per-host policy; emits URL-only manifest v2
  snapshots; signs both the portable manifest and an audit report; and proves
  lightweight output through the public consumer path. Build admission is
  bound to the actual execution clock and final activation, and every staging
  write and consumer read stays under a descriptor-anchored root through an
  atomic no-replace rename. It is isolated from the ordinary image by a
  dedicated network-free builder image. A strict streaming normalizer now
  turns pinned CDX API JSON or raw CDXJ header exports into deterministic,
  versioned metadata JSONL with exact input/output digests and atomic
  no-overwrite activation; cancellation remains authoritative through final
  EOF, post-commit faults return path-and-digest recovery evidence, and local
  publisher builds use their executable digest instead of an ambiguous `dev`
  signing identity. The normalizer deliberately emits no permission evidence.
  ADR 0009 now fixes a separately operated, public-egress-only evidence
  collector boundary and the strict core policy/digest contract validates its
  contact-bearing robots identity, bounded validity, URL-metadata-only rights
  basis, canonical evidence URI, and exact local evidence digest. The live
  foundation now also strictly decodes normalized rows, evaluates bounded
  exact robots policy bytes including the path query, and emits deterministic
  body-free response/noindex evidence with applicable header and metadata
  controls. A separate `fetchmark-pack-evidence` command now runs the
  public-only live observer with per-hop host pacing, bounded concurrency and
  network-only timeouts, fail-closed robots handling, clean-EOF bounded charset
  decoding and noindex evaluation, effective-host `Retry-After` cooldowns for
  robots and page 429 responses, and no automatic retries. Its ordered runner
  shares the builder's capture preflight and emits only fully admitted builder
  candidates, one body-free observation per normalized row, deduplicated
  content-addressed robots representations, and a digest-bound report through
  descriptor-anchored atomic no-replace bundle activation. Page redirects are
  recorded but not followed until target-path robots evidence can be modeled;
  page bodies and exact rights-evidence bytes are never retained. The command
  and its networked container remain separate from both the ordinary server
  and network-free signer.
  Direct local flat URL Index Parquet parts now stream into the same
  deterministic normalized-v1 boundary without Athena, DuckDB, or a hosted
  query service. The reader accepts the current UTC `TIMESTAMP_MILLIS`
  configured by Common Crawl and decodes legacy Spark `INT96` capture times
  explicitly, pins the official core physical schema while allowing additive columns,
  preflights allocation-, depth-, and token-bounded footer and page Thrift plus
  bounded row-group and non-overlapping column-chunk metadata with cancelable
  reads, hashes the complete input before and after decoding, and preserves
  physical row order through the existing atomic no-overwrite command path.
  A bounded live metadata probe of an official 1.607-GB WARC part confirmed
  20,522,545 rows, 12 row groups, 30 columns, a 57,995-byte footer, current UTC
  millisecond timestamps, and a 192,343,532-byte largest uncompressed column
  chunk within those limits; it deliberately did not claim a full decode.
  Registry v2 and the standalone consumer now also pin a signed delta's exact
  snapshot parent, operation count, and final projection count. Manual verify
  and install rebuild a new immutable projection from both reverified portable
  bundles, reject delta chains and invalid tombstones, preserve the parent, and
  require explicit operator rollback through a registry change and restart. A
  network-free publisher path now verifies the exact parent snapshot, compares
  it with a newly admitted target through a descriptor-anchored disk-backed
  store, deterministically emits signed upserts/tombstones, binds target and
  operation accounting in a domain-separated audit report, and reruns the
  public materialization gate for lightweight deltas. An independent
  public-key-only command rechecks both portable bundles, operation membership,
  stream digest, final projection count, and consumer result.
  An offline TUF channel selector now consumes an out-of-band pinned root and
  operator-supplied metadata directory through the maintained go-tuf client,
  persists rollback-resistant trusted metadata and accepted root rotations,
  pins the original bootstrap digest under an exclusive state lock, reports a
  signed target absence, and verifies an exact local pack manifest before any
  registry or installation decision. It has no runtime network path and does
  not mutate registries or installed objects.
  An independently reviewed `channel-fetch` step now requires explicit network
  consent and a fixed canonical HTTPS target base, preflights public-only egress before trust
  state changes, authenticates the exact TUF manifest target, and streams only
  its strict detached signature and content-addressed shards into a bounded,
  single-attempt/single-connection, fsynced, atomically activated no-overwrite
  bundle. Cancellation and manifest validity are rechecked immediately before
  activation. Signed absence performs no artifact request. Retrieval remains
  distinct from publisher-key verification, registry mutation, installation,
  deletion, and hot reload.
  A separate network-free `registry-candidate` step now closes the circular
  hand-edit gap for existing sources: it preserves the current publisher key,
  pack identity, object root, and exact delta parent; fully materializes the
  TUF-selected bundle; and atomically writes a deterministic no-overwrite
  version-2 candidate with exact base/candidate registry digests. Existing
  `verify` and `install` consume that candidate while the live registry and old
  object remain untouched. Signed absence writes no candidate.
  Explicit `registry-activate` and `registry-rollback` commands now apply one
  exact source transition under a persistent descriptor-anchored advisory lock.
  They compare exact current/candidate byte digests, open the destination
  immutable projection, retain mode-0600 no-overwrite rollback bytes, recheck
  the live file immediately before atomic replacement, sync and read back the
  result, and distinguish prepared from committed recovery evidence. Activation
  never deletes either object or hot-reloads Fetchmark; both directions report
  that an operator restart is required. Competing candidates have a single
  winner, lock waits honor cancellation, and a real install/activate/restart/
  rollback lifecycle preserves the already-open projection until restart.
  A deterministic bounded bulk selector has now been independently reviewed,
  and a pinned current Common Crawl part produced byte-identical repeat
  selections. A direct multi-part selector now applies the same globally
  host-diverse ranking across 2--1,024 exact raw Parquet parts, binds a sorted
  digest/row/byte descriptor set, rejects duplicate or changing inputs before
  output, and requires an explicit `format-prototype-v1` profile above the
  unchanged 10,000-row collector ceiling. A retained two-part run processed
  13,433,428 real rows twice in opposite input orders and produced the same
  66,576-row, 30,515,106-byte output and SHA-256. Its observed 201.78 source
  rows per unique selected host motivated a prototype-only two-billion-row raw
  scan ceiling while the collector profile remains at 50 million. A separate
  offline partition command now strictly validates up to five million selected
  rows, requires global selected-host uniqueness, preserves exact bytes and
  order, and atomically emits digest-bound, globally ranged inputs of at most
  10,000 rows for the unchanged live collector. Jobs remain serial because
  distinct origins can share robots redirect targets and current pacing is
  process-local. A deterministic network-free merger now matches unordered
  bundles to exact partitions, revalidates every input/report/observation/
  candidate/robots artifact twice, restores global candidate order, requires
  common collector and policy identity, and atomically emits a digest-bound
  aggregate report. Evidence-bound snapshot builds carry that exact report and
  sign its digest under BuildReport v3 while legacy v2 verification remains
  available. Collector bundles remain unauthenticated and delta BuildReport v1
  is not aggregate-evidence-bound. Remaining before an operator-facing scale
  increase: a durable shared effective-host/`Retry-After` gate, worker
  authentication and scheduling/resumption inside the evidence window, a
  retained real 1--5 million-row selection and admission run, and a repeat-run
  build plus resource-profile decision. A network-free publisher path now generates distinct
  TUF role keys, a two-of-two signed bootstrap root, exact consistent-snapshot
  targets/snapshot/timestamp metadata, and immutable no-overwrite mirror stages
  only after full pack verification. Exact previous-generation verification
  supports strictly advancing publication and signed withdrawal within the
  caller-selected chain. A separate local mirror activation command now verifies
  the pinned root, full live and candidate chains, publisher signature and
  shards; copies only immutable dependencies; and compare-and-swaps the exact
  live timestamp digest as the sole pack-content commit point. Hash-prefixed detached
  signatures remove the cross-generation manifest/signature race. Prepared and
  committed failures carry exact recovery evidence, competing sibling branches
  have one winner, and old objects are retained. A separately reviewed offline
  root ceremony now gives each custodian one no-overwrite key, independently
  validates the exact root-only transition before signing, verifies both old
  and new two-of-two thresholds, migrates the publisher to a rootless
  operational identity, and carries the bounded public root chain through
  staging, mirror activation, and the existing consumer. Remote upload
  protocols and public pack distribution remain later explicit work.
- Open discovery Phase 7 — ADR 0008 fixes the first private federation boundary:
  URL-only curated-index results, dedicated Ed25519 node identities, exact HTTPS
  peer bindings, signed request/response bodies, bounded replay protection, a
  separate inbound listener, and no proxying, chaining, passages, or public
  peer discovery. The strict protocol, secure identity/trust-file loading,
  no-overwrite key utility, isolated curated-index listener, proxy-free HTTPS
  peer adapter, source-pack binding, rate/concurrency budgets, replay cache,
  bounded metrics, and real-TLS end-to-end tests are implemented. Federation
  remains disabled by default and every peer URL still passes through the
  ordinary live SSRF/robots/noindex pipeline. Remaining: multi-node operational
  benchmarks, deliberate key-rotation overlap, private-mesh deployment
  exercises, and research into reputation/privacy defenses before considering
  any broader federation model.

## Done (v1)

- P0 — Repo skeleton & config; `/healthz`, `/readyz`, `/metrics`, API-key
  + request-ID middleware, Dockerfile, compose (bundled + external).
- P1 — SearXNG adapter + `/v1/search v0`.
- P2a — Egress / SSRF policy.
- P2 — Parallel fetch pipeline (resty, proxy, retry, UA pool).
- P2b — Fetch safety budgets (body/decompressed caps, MIME sniff,
  header timeout, per-host concurrency).
- P3 — Content extraction to Markdown / JSON / cleaned HTML.
- P3a — Versioned two-layer cache + `singleflight`.
- P3b — Exact content dedupe + JS-required detection.
- P4 — BM25 re-rank with engine-diversity bonus.
- P5 — `/v1/parse` endpoint.
- P6 — Per-API-key rate limits (Redis + local fallback).
- P6a — Admin-only overrides & dashboard basic-auth.
- P7 — Read-only ops dashboard (html/template + HTMX).
- P8 / P8a / P8b — Prometheus metrics, SearXNG engine-health,
  request-ID propagation.
- P9 — Initial `/v1/summarize` placeholder (later replaced in v2-R3) +
  OpenAPI spec.
- P10 — `docs/` mirror scaffolded.
- P11 — GHCR multi-arch release workflow.
- Review round 1 — GPT-5.4 security/perf fixes (`7f78d2a`): egress
  hop-by-hop revalidation, Accept-Encoding MIME sniff, token-bucket
  sweep, `crypto/rand` sampling, ctx-propagation through pipeline.

## Done (v2 R1)

- Q1 — Headless renderer adapter + pipeline render branch
  (explicit `render=true` and auto-upgrade on `js_required`).
  Separate cache key space so plain and rendered artifacts never
  shadow each other. [`f41eaee`]
- Q2 — Jaccard near-duplicate dedupe (3-gram word shingles, τ=0.85),
  run after the ranker so cluster winners use real BM25 scores.
  [`485307b`]
- Q3 — Multi-SearXNG failover with round-robin scheduling and 30s
  cooldown per instance; `fetchmark_searxng_instance_up` gauge.
  [`6497220`]
- Q4 — Cross-instance Redis stampede lock (CAS release via Lua) +
  per-URL cold-path rewrite that coalesces both intra-process
  (singleflight) and inter-process callers. [`c81f1fe`]
- Review round 2 — GPT-5.4 follow-up fixes (`7bb0f56`):
  - SSRF egress policy applied before renderer call.
  - Stampede-lock TTL/wait sized off `max(fetch, renderer)` timeout.
  - Rendered `js_required` artifacts no longer cached.
  - Ranker now runs before near-dup collapse.
- MIT license + user-facing README.

## Done (v2 R2)

- Q-a — Minimal per-package docs under `docs/dev/staticvar/fetchmark/`
  (api, dashboard, searxng, fetcher, egress, extractor, robots, cache,
  pipeline, rank, obs). Focus on invariants, not API reference.
- Q-b — Regression tests: fetcher exhausted-retry terminal error,
  `/v1/parse` admin-override table (non-admin proxy/robots → 403,
  admin → 200 + option propagation), Redis-backed ratelimit allow/deny
  + cross-key isolation. [`4d8fb8c`]
- Q-c — Configurable SearXNG cooldown via `FM_SEARXNG_COOLDOWN`
  (default 30s, `validate()` rejects ≤0). `NewMultiWithCooldown`
  constructor; non-positive runtime values clamp to default.
  [`89246fb`]
- Q-d — Second Redis stampede lock on `RenderedArtifactKey` in
  auto-render. TTL sized off a `Render=true` options clone because
  auto-render fires with `Render=false`. Post-lock re-apply clears
  `Title`/`Unsupported` so peer-populated rendered blobs propagate
  over js-required placeholders. [`2aec73c`]
- D-d — CJK-aware near-duplicate shingling: char bi-grams when the
  body is ≥30% CJK by non-whitespace/non-punct rune count, word
  3-grams otherwise. Uses `unicode.Is(Han|Hiragana|Katakana|Hangul)`.
  [`e6a2138`]

## v2-R3 — shipped

- R3-a — Summarizer Provider port with OpenAI (Chat Completions) and
  Anthropic (Messages) adapters using the official SDKs. Normalised
  Usage (prompt/completion/reasoning/total) and classified errors
  (auth/rate_limit/upstream/timeout/bad_request/network/cancelled) so
  the handler can map cleanly to HTTP statuses. Raw reasoning/thinking
  text is intentionally dropped — only token counts surface. [`f6ac633`]
- R3-b — Env-authoritative config with per-provider profiles
  (`FM_SUMMARIZE_{OPENAI,ANTHROPIC}_*`). [`9977399`]
- R3-c — Prometheus metrics:
  `fetchmark_summarize_total{provider,outcome}`,
  `fetchmark_summarize_duration_seconds{provider}`,
  `fetchmark_summarize_tokens_total{provider,class}`. [`cbb15a7`]
- R3-d — `/v1/summarize` wired: parses URL through the pipeline,
  validates non-empty content, delimits the page with `<page>…</page>`
  and instructs the model to treat it as untrusted. Admin JSON API at
  `/admin/summarize/{config,providers,providers/{name},default}` with
  merge-on-write semantics so operators don't retype API keys to tweak
  a model. Dashboard partial lists configured providers. [`b914670`]
- R3-e — Summarizer registry bootstrapped from env in main; a
  dedicated http.Client is used so the LLM path sidesteps the Fetcher's
  SSRF egress policy (trusted upstream). [`aab401a`]

## Declined / deferred with rationale

- Declined: keep Jaccard near-duplicate dedupe while FM_RESULTS_CAP <= 50;
  revisit SimHash/MinHash only if the cap rises or profiling shows dedupe
  latency matters.
- Deferred pending user demand: SSE streaming of search results;
  synchronous /v1/search remains the supported contract.
- Deferred: cross-instance summarizer config replication via Redis
  pub/sub. Env vars remain authoritative; admin overrides are
  in-process and revert on restart. Operators running multiple
  replicas should promote permanent changes to env and redeploy.
