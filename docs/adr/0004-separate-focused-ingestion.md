# ADR 0004: Separate, bounded focused ingestion

Status: accepted; extended by ADR 0005 and ADR 0006

## Context

Fetchmark needs an optional way to improve a curated local corpus without
putting broad crawling on the API request path or requiring every installation
to crawl the web. The API process already owns the security-sensitive page
retrieval and retention decisions: SSRF-safe egress, redirect checks,
robots.txt, `X-Robots-Tag`, HTML noindex/noarchive, extraction, quotas,
tombstones, and takedowns. Its persistent artifact store also has exclusive
single-process ownership.

A crawler that opens those stores directly would duplicate policy, violate
ownership, and create a second mutation path. A crawler that forwards bodies
would let untrusted content cross the retention boundary without Fetchmark's
normal checks.

## Decision

Focused ingestion runs as the separate `fetchmark-crawl` executable and is
optional. The initial executable is curated-only and has two explicit modes:

- `-dry-run` strictly validates and canonicalizes the job plan without network
  access or opening state;
- `-once` performs one bounded scheduler pass and exits.

The worker owns only a dedicated bbolt frontier. It never opens Fetchmark's
Bleve index, artifact path, Redis cache, or SearXNG configuration. Each due URL
is sent alone to the fixed-origin, admin-authenticated
`POST /admin/corpus/focused-admissions` route. The body contains one URL plus
the configured allow/deny path prefixes and denied URLs; it contains no page
content and omits `safety_classification`, so the worker cannot infer or assert
content safety. Fetchmark validates the policy again and enforces it before
every redirect request. Redirects must keep the original scheme and authority,
satisfy the allow list, and miss every deny rule. Dot-segment, backslash,
encoded-separator, and percent-encoding ambiguity is normalized or rejected
before network access. Fetchmark remains responsible for page retrieval and
every retention decision. Archive mode is not accepted by this route and
remains automatic-only.

The job format is strict and versioned. It requires explicit domains, path
prefixes, per-run caps, refresh and rejection intervals, a minimum per-host
interval, and an attempt limit. Subdomains require an explicit opt-in. Removed
entries are disabled instead of deleted. The frontier has a
hard entry cap, atomic leases, restart recovery, exclusive ownership, schema
validation, stale-worker-resistant lease generations, and no silent eviction.
Per-authority dispatch state is also atomic and persistent. Calls from another
job or a restarted worker wait for the larger of the previous and current
minimum interval; reserving a dispatch persists before the HTTP call so a crash
cannot erase politeness state.
Its direct parent must be a trusted non-group/world-writable directory.
Disabled identities continue to consume the lifetime cap until an operator
archives/replaces the separate database or a future operator surface invokes
the adapter's explicit prune operation; ordinary synchronization never deletes
them.

Admissions are sequential in this first slice. Successful and rejected entries
are scheduled for later re-evaluation. Transient failures use bounded
exponential backoff with deterministic jitter and honor bounded `Retry-After`.
Authentication, authorization, curated-mode conflicts, and fixed-endpoint or
protocol-contract failures stop the run.
Storage failures pause the run rather than hot-looping against a full corpus.

The client accepts one fixed HTTP(S) Fetchmark origin, refuses redirects, caps
response bytes, strictly validates response JSON, and never includes the admin
key or response body in errors. Secrets remain environment-only.

## Discovery documents

The first supporting library parses bounded sitemap, sitemap-index, RSS, and
Atom XML into canonical web-only page/source candidates. It enforces document,
streaming record, Atom link-fanout, nesting, and URL byte limits before
unbounded XML slices can be allocated, plus ordered deduplication,
malformed-document failure, and credential/non-web URL rejection. A sitemap or feed remains only a source
of untrusted candidates; it is not permission to fetch, evidence of freshness,
or permission to retain.

Live document polling is deliberately the next slice, not part of the initial
seed executable. It must add external egress and redirect checks, robots
evaluation for source documents and eventual pages, decompression limits,
same-origin/path sitemap scoping, conditional validators, HTTP freshness,
`Retry-After`, and a contactable crawler identity before it is enabled.

ADR 0005 implements and supersedes this deferred live-polling boundary while
leaving the separate-process and URL-only admission decisions here unchanged.

WebSub is deferred to an optional sidecar. It requires inbound callback
verification, expiring subscriptions, secrets/signatures, renewal, and public
reachability, which is a materially different lifecycle for private or
NAT-only installations.

## Consequences

- Operators can refresh a focused curated corpus with no paid search API and no
  per-query cost.
- Ordinary Fetchmark installations do no crawl work unless the operator runs
  the separate executable/profile.
- Corpus policy and storage ownership stay centralized in the API process.
- The first slice was a durable seed ingestion worker. ADR 0006 adds bounded
  one-hop expansion; recursive crawling and whole-web coverage remain non-goals.
- The dedicated frontier contains URLs and scheduling outcomes; operators must
  treat its volume as private local data.
- A dedicated admin key is recommended because admissions consume the server's
  normal key-specific rate budget.

## Standards baseline for the next slice

- RFC 9309 for robots exclusion, including fail-closed behavior when robots is
  unreachable and a normal cache lifetime no longer than 24 hours.
- The Sitemap protocol limits of 50,000 entries and 50 MiB uncompressed data,
  with stricter operator-configured bounds permitted.
- RFC 4287 for Atom, the RSS 2.0.11 specification for RSS hints, and RFC
  9110/9111 plus RFC 6585 for validators, freshness, 429, and `Retry-After`.

These protocols do not make feed HTML, remote URLs, GUIDs, sitemap metadata, or
publisher content trustworthy.
