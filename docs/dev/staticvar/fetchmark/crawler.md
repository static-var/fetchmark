# Focused ingestion

`fetchmark-crawl` is an optional process for bounded curated-corpus ingestion.
It owns a separate bbolt frontier and communicates with Fetchmark only through
the admin URL-admission API. It must never open the server's index, artifact,
cache, or discovery stores.

The executable uses strict configuration version 3. Each configuration requires
an HTTPS or direct `mailto:` `contact_uri`, separate hard caps for page-frontier
and discovery-source identities, independent page-membership and source-edge
caps, a separate one-hop link-edge cap, and at least one explicit page seed or
discovery source per job. The same
decoder and bounds apply before both dry-run and live modes; decoding performs
no network or state access.

Its strict job policy is defined in `internal/core/focusedcrawl/config.go`; scheduling and outcome
transitions are in `runner.go`. The frontier adapter provides exclusive
ownership, no-follow opening, hard cardinality, atomic leases with generations,
restart recovery, refresh scheduling, and explicit-only disabled pruning.
`internal/adapters/corpusclient` pins one Fetchmark origin, refuses redirects,
bounds and validates responses, and classifies fatal/transient/permanent
failures. The command sends one URL plus its job's validated allow/deny path
policy per request and keeps the API key in the environment. The server checks
that policy again before every redirect, with exact original scheme/authority
and deny-first decoded-path matching. The frontier persists atomic host
reservations, so politeness intervals survive restarts and use the larger
interval across jobs sharing an authority.

`internal/crawler/discovery.go` is the pure source-document parser. It parses
already-fetched sitemap, sitemap-index, RSS, and Atom XML under document,
nesting, entry, and URL limits; resolves web-only URLs; canonicalizes and
deduplicates in order; bounds recognized records and Atom link fanout during
streaming inspection before XML slice allocation; and exposes sitemap sources
separately from page candidates. It performs no network or storage access.
Live HTTP belongs to `internal/adapters/crawlsource`; source scheduling and
membership replacement belong to `internal/core/focusedcrawl/source_runner.go`
and the schema-v6 frontier. Existing schema-v5 state is migrated transactionally.

Version 3 accepts optional `discovery_sources` with the following shape:

```json
{
  "url": "https://example.org/sitemaps/docs.xml.gz",
  "kind": "sitemap",
  "allowed_source_path_prefixes": ["/sitemaps/"],
  "poll_interval": "6h",
  "error_recheck_interval": "1h",
  "max_compressed_bytes": 2097152,
  "max_decompressed_bytes": 8388608,
  "max_entries": 10000,
  "max_child_sources": 128,
  "max_depth": 2
}
```

`kind` is `auto`, `sitemap`, or `feed`. A configured source is canonicalized
with the same host, port, query, credential, and ambiguous-path rules as page
seeds. Its host must be in the job's explicit domain policy, and its path must
match an `allowed_source_path_prefixes` entry. Source paths are deliberately
separate from page paths: `/sitemaps/docs.xml` may enumerate pages under
`/docs/`, but every resulting page will still have to pass the page policy.
Canonical source URLs are unique across the whole configuration.

The local hard limits are 64 configured roots and 64 source polls per job/run,
4 MiB compressed, 16 MiB decompressed, 10,000 records, 256 child sources, and
four sitemap-index levels. Poll intervals are 5 minutes through 30 days; error
rechecks are 5 minutes through 24 hours. These local document limits are
intentionally below the Sitemap protocol maxima of 50,000 records and 50 MiB
uncompressed. `max_discovery_sources` is the global lifetime identity budget
for configured roots and nested sources. Disabled identities and their last
successful snapshots remain in the frontier until explicit operator pruning;
they are not silently evicted.

`max_discovery_page_memberships` and `max_discovery_source_edges` independently
bound the many-to-many relationships. They may each be `0..1,000,000`, but must
be positive when any discovery source is configured. They are deliberately not
derived from `max_frontier_urls`: one canonical page may be owned by several
sources, and a source graph may contain several parent edges per identity.

## One-hop link expansion

A job may opt in explicitly:

```json
"link_expansion": {
  "max_links_per_page": 32,
  "max_pages_per_run": 50
}
```

`max_links_per_page` is `1..64`. `max_pages_per_run` is
`1..min(max_urls_per_run, 1000)`. Top-level `max_link_edges` is
`0..1,000,000` and must be positive when any job enables expansion. The page
budget selects complete snapshots; Fetchmark never partially replaces a
snapshot merely because the run is near a candidate budget.
To lower `max_link_edges` below currently retained edges, first disable or
change the affected job policies under the old cap (or `0`) so synchronization
clears their snapshots, then apply the lower cap; startup otherwise fails
closed without deleting relationships.

Only an explicit seed or a page currently owned by a reachable sitemap/feed
may request a link snapshot. Link-only children are scheduled through the same
admission and host-pacing paths but cannot recursively publish links. The API
returns only a bounded ordered list of canonical URLs after permitted
retention—never bodies, snippets, anchor text, or safety assertions. The worker
then re-applies the job's deny-first domain/path policy. Self-links, out-of-scope
links, `rel=nofollow` anchors, and applicable header/HTML `nofollow` directives
are excluded.

Admitted and authoritative-empty snapshots exact-replace parent ownership in
one lease-generation-guarded transaction. Authoritative policy rejection
clears it. Transport, protocol, storage, cancellation, and exhausted-attempt
failures preserve the prior snapshot. Shared children remain active while any
direct or link parent owns them. Historical edges become inactive when a
parent loses direct ownership and reactivate if that ownership returns.

Four non-activating operator templates live under `deploy/crawler-packs/`.
They use only `example.invalid` targets and must be copied, edited, and reviewed
for the real site's robots policy, terms, licenses, paths, and budgets.

With `-once`, due sources are polled before due page admissions. The source
adapter owns a public-internet egress transport and performs SSRF and DNS
rebinding checks for the original URL and every redirect. It checks robots
before each hop, keeps redirects on the configured source origin/path scope,
does not retry internally, and enforces both worker-wide and per-root wire and
decompressed limits. Only a well-formed supported XML root with an XML/feed
media type reaches the parser. HTML challenges, nested or multi-member gzip,
and oversized or malformed bodies are rejected.

The frontier stores source validators, successful-snapshot state, schedules,
page memberships, and the root-scoped child-source graph in one bbolt database.
A verified HTTP 200 exact-replaces one source's memberships atomically,
including an authoritative empty snapshot. A 304 is accepted only after a
successful snapshot and preserves memberships. Fetch, robots, MIME, size, and
parse failures retain the last snapshot and schedule a bounded recheck;
`Retry-After` can only extend that delay. `Cache-Control: no-store` clears
validators. Root removal makes its graph inactive without destroying the
snapshot, so re-adding it is deterministic. Shared pages remain active while
any reachable source or explicit seed owns them.

Pages enumerated by a sitemap must remain on that sitemap root's scheme and
authority as well as satisfying the job page policy. RSS/Atom entry links may
use another explicitly allowed job domain; they still pass the full page
admission boundary. Cross-origin sitemap delegation is not implemented.

Each run emits separate `source_summary` and `admission_summary` JSON objects.
Source failures are counted without URL, host, or job labels. A source
transport or document failure does not suppress admissions already owned by
the frontier; database or configuration invariant failures stop the run.

A feed or sitemap is untrusted discovery input, not fetch permission or a
retention grant. Final page admission always stays behind Fetchmark's existing
robots, noindex/noarchive, SSRF, extraction, quota, and takedown checks.

Still out of scope: recursive or arbitrary-depth link following, JSON Feed, RSS 1.0/RDF, WebSub,
cross-origin sitemap delegation, and `application/octet-stream` gzip guessing.
