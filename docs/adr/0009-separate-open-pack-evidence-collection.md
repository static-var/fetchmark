# ADR 0009: Separate open-pack admission evidence collection

Status: accepted

## Context

The open-pack normalizer emits deterministic Common Crawl URL metadata but no
permission assertions. The network-free publisher builder intentionally accepts
only candidates carrying current RFC 9309, noindex, and rights evidence. A
publisher therefore needs a narrow bridge between those two stages.

That bridge is security- and policy-sensitive. Its input may contain arbitrary
public URLs; it must not become an SSRF path, a general crawler, a challenge
bypass tool, or an implicit license oracle. Fetching every page in a multi-
million-record prototype inside a 24-hour evidence window would also be neither
polite nor operationally credible. The first implementation needs an honest
bounded scope.

Fetchmark already has the useful primitives: external egress policy rejects
private, loopback, link-local, CGNAT, and ULA destinations and revalidates every
dial and redirect; the fetcher bounds headers, compressed and decompressed
bodies, redirects, time, and concurrency; the robots checker applies RFC 9309
and caches for no more than 24 hours; and the local-corpus policy parses
applicable `X-Robots-Tag` and HTML robots metadata. None of those components
currently produces the exact replay evidence required by a pack publisher.

## Decision

Live pack admission will be a separate, explicitly operated
`fetchmark-pack-evidence` command and container. It will not be linked into the
ordinary server, the optional focused crawler, or the network-free
`fetchmark-pack-build` image.

The first profile is limited to the ordinary lightweight pack ceiling of
10,000 normalized rows. Larger evidence runs require a later distributed or
incremental design backed by real politeness and completion measurements; the
format-prototype ceiling is not permission to contact five million pages in one
run.

Collection uses a stable contact-bearing product-token user agent, the default
external egress policy, fail-closed robots handling, bounded response headers
and representations, per-host concurrency one, a configured minimum interval
between requests to the same host, bounded global concurrency, explicit
timeouts, and no proxies, browser impersonation, CAPTCHA solving, or retries
that ignore `Retry-After`. Robots redirects are egress-checked, paced, and
recorded. The initial profile records but does not follow page redirects,
because contacting a new path or origin would require another robots decision;
every redirecting page is therefore non-admissible without contacting its
target.

The request timeout starts only after a request leaves the host pacing queue,
so a valid minimum host interval cannot consume the network budget before
dispatch. A robots `429` response is never treated as the RFC 9309 missing-file
case: it fails closed, records a bounded `Retry-After`, extends the effective
responding host's cooldown, and suppresses the page request. Page `429`
responses apply the same effective-host cooldown before any later candidate.
URLs containing userinfo are rejected before network access. Audit evidence
retains the exact effective robots request URI even when canonicalization would
reorder or remove query parameters.

The collector consumes normalized metadata plus a strict operator-reviewed
rights policy and the exact local rights-evidence file. It never infers a
license from Common Crawl inclusion or page text. The rights policy names one
of the builder-supported bases, a canonical evidence URI, a bounded notice,
and the intended `url_metadata` field set; the collector computes the evidence
file SHA-256 itself and stamps a validity window of at most 24 hours.

An indexing decision may be `indexable` only after the bounded representation
reader reaches a clean EOF. A response truncated by cancellation, byte limits,
content-length mismatch, decompression failure, or any other read error is not
negative evidence for noindex and fails closed.
Declared and detected HTML encodings are decoded into bounded UTF-8 before
metadata parsing. Unsupported charsets, malformed UTF-8, decoded expansion
beyond the representation ceiling, and HTML parser errors all fail closed.

Before network access, the collector and builder share one capture preflight
for status 200, declared and detected HTML MIME, digest and WARC location,
capture timestamp, language shape, and public queryless URL eligibility. The
bundle writer repeats that preflight as an output invariant, so a faulty row
observer cannot emit an immediately unbuildable candidate.

Output is a descriptor-anchored, no-overwrite evidence bundle activated as one
directory. It contains:

- admission-enriched candidate JSONL for the network-free builder;
- a strict observation JSONL stream binding each input row to robots status,
  original and final robots URI, exact policy-representation digest, page
  status/final URL, canonical relevant-header digest, representation digest,
  parser version, applicable header and metadata directives, outcomes, and
  timestamps;
- content-addressed robots policy representations needed to replay the access
  decision;
- a report binding exact input, configuration, rights-evidence, candidate,
  observation, and robots-object digests and aggregate outcome counts.

Page representations are processed ephemerally and are never stored in the
bundle. The observation archive retains only their SHA-256 and the applicable
robots directive values needed to audit the noindex decision. The publisher
must keep the exact rights-evidence file under its own retention policy; the
bundle binds it by digest but does not redistribute it.

An interrupted or failed run does not publish a partial bundle. ADR 0020 now
defines deterministic, globally host-disjoint, line-exact inputs for many
unchanged 10,000-row collectors. Resumption, distributed leasing, worker
identity, signing evidence bundles, and verified merging remain later
extensions; they must preserve the partition manifest's exact input-row
identity, time windows, per-host pacing, and deterministic final ordering.

## Consequences

- Ordinary Fetchmark installations remain lightweight and perform no pack-
  publishing crawl.
- The network-free builder continues to consume immutable local inputs and
  cannot silently reach the web while signing.
- Robots and noindex decisions become reviewable without retaining or
  redistributing page bodies.
- Rights remain an explicit publisher assertion backed by exact operator-
  supplied evidence, not an automated legal conclusion.
- The first collector can produce genuinely buildable lightweight candidates,
  but it does not make a one-to-five-million-document run operationally or
  ethically credible. That scale requires measured distributed collection or
  another conservative evidence model before any public cap changes.

## References

- [RFC 9309: Robots Exclusion Protocol](https://www.rfc-editor.org/rfc/rfc9309.html)
- [Common Crawl CDXJ Index](https://commoncrawl.org/cdxj-index)
- [Common Crawl terms of use](https://commoncrawl.org/terms-of-use)
- [ADR 0007: Neutral signed open index packs](0007-neutral-signed-open-index-packs.md)
- [ADR 0020: Bounded open-pack admission partitions](0020-bounded-open-pack-admission-partitions.md)
