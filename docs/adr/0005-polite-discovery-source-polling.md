# ADR 0005: Polite, persistent discovery-source polling

Status: accepted

## Context

The separate focused-ingestion worker established in ADR 0004 can admit
explicit URLs without sharing corpus storage with the API process. Operators
also need a bounded way to keep a configured documentation, publication, or
news scope current from sitemaps and feeds. A discovery document is untrusted
input: it is neither fetch permission nor a retention grant, and a failed poll
must not erase the last useful view of a source.

Source polling must remain optional and must not turn ordinary API requests
into crawl work. It must not use paid APIs, proxies, CAPTCHA bypasses, the
operator's Mac as an exit node, or a mandatory local model.

## Decision

`fetchmark-crawl -once` polls due configured discovery sources before it admits
due pages. The source-polling slice introduced configuration version 2 with a
contact URI, a global source
identity cap, independent page-membership and source-edge caps, per-job poll
limits, and per-root origin/path, interval, byte, record, child, and depth
limits. Source-only jobs are valid.
ADR 0006 subsequently advances the combined format to version 3.

The source HTTP adapter owns its external egress transport. The original URL
and every redirect are subject to public-address SSRF and dial-time DNS
rebinding checks. Robots policy is checked before each request. Redirects must
stay on the configured source's normalized scheme, authority, and allowed path
prefix. The adapter performs no internal retry, strips conditional headers on
redirect, bounds response headers and bodies, permits at most one gzip layer,
and accepts only a well-formed single sitemap/RSS/Atom XML root with an XML or
feed media type. Per-request byte limits may tighten but never widen the
worker-wide client limits.

The worker uses a stable `FetchmarkCrawler/<version> (+<contact_uri>)` identity.
RFC 9309's unavailable/unreachable distinction is retained: ordinary robots
4xx responses allow access, while network and 5xx failures fail closed. Robots
state is cached for at most 24 hours.

Discovery-source state lives in the same exclusively owned bbolt frontier as
page scheduling, but in separate schema-v5 records and indexes. It persists:

- configured and root-scoped child source identities;
- validators and whether a successful snapshot exists;
- due, retry, lease, attempt, and generation state;
- exact source-to-page memberships;
- a bounded many-to-many child-source graph;
- persistent per-authority dispatch reservations shared with page admissions.

A verified HTTP 200 replaces one source's page memberships, child edges,
validators, and schedule in one transaction. An empty document is an
authoritative empty snapshot. A 304 is invalid until a successful snapshot
exists; afterward it exact-preserves memberships and updates the schedule.
Ordinary 304 responses merge non-empty validators in the runner, while
`Cache-Control: no-store` clears them. Fetch, policy, MIME, size, decompression,
and parse failures preserve the prior snapshot and validators and schedule a
bounded retry. `Retry-After` may extend but never shorten that delay.

Reachability is computed from configured roots through graph edges. Removing a
root makes its unreachable graph and page ownership inactive without deleting
the last snapshot. Re-adding the root restores it deterministically. A page is
active while owned by an explicit seed or at least one reachable source.
Lifetime source, page-membership, and source-edge caps fail atomically without
evicting unrelated state.

Discovered pages are only candidates. They must satisfy the job's page scope
and then pass through the authenticated URL-only focused-admission API. The API
process independently rechecks SSRF, redirects, robots, noindex, noarchive,
extraction, quotas, revocations, and takedowns before persistence.

## Scheduling and observability

The next successful poll is no earlier than both the configured interval and
conservative HTTP freshness. Failures use the configured error-recheck floor,
bounded deterministic backoff, and applicable `Retry-After`. All dispatches
reserve the persistent authority budget before network access.

The bounded process emits separate fixed-field `source_summary` and
`admission_summary` JSON objects. URLs, hosts, validators, queries, and job IDs
are not metric labels.

## Consequences

- Operators can improve a focused corpus from open publisher surfaces with no
  paid search dependency or per-query cost.
- Ordinary Fetchmark installations remain CPU-only and do no crawl work unless
  the operator runs the separate worker.
- A temporary source failure does not erase already discovered ownership or
  suppress page work already present in the frontier.
- The frontier retains private local URL and scheduling data and requires the
  same backup/privacy treatment as other crawler state.
- Strict MIME handling intentionally does not guess gzipped XML served as
  `application/octet-stream`.
- Cross-origin sitemap delegation, recursive link following, JSON Feed, RSS
  1.0/RDF, and WebSub remain out of scope for this slice. ADR 0006 separately
  adds one-hop links from directly owned pages.

## Standards baseline

- RFC 9309 for robots exclusion.
- The Sitemap protocol, with stricter local limits than its 50,000-record and
  50 MiB uncompressed maxima.
- RFC 4287 for Atom and RSS 2.0.11 for RSS polling input.
- RFC 9110/9111 and RFC 6585 for conditional requests, freshness, 429, and
  `Retry-After`.

WebSub remains a possible optional sidecar because inbound callback
verification, expiring leases, signatures, renewal, and public reachability are
a separate security and operating lifecycle.
