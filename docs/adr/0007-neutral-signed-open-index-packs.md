# ADR 0007: Neutral signed open index packs

Status: accepted

## Context

Common Crawl is useful bootstrap data, but its CDXJ and Parquet URL indexes
locate captured URLs and WARC records; they are not a live full-text keyword
search service. A Fetchmark pack publisher must select records offline, range
fetch and inspect captures only when the intended fields require it, apply
exclusion and rights policy, and build a compact discovery artifact. A
URL-metadata-only publisher does not need WARC bodies. Ordinary Fetchmark
installations must not need Common Crawl, a crawler, cloud analytics, a paid
API, embeddings, or an LLM.

The exchange format must remain inspectable and independent of Fetchmark's
current storage engine. Distributing a serialized Bleve directory would bind
the format to Bleve's on-disk schema. Distributing a custom bbolt posting index
would require Fetchmark to maintain another analyzer, scorer, posting format,
and storage ABI beside the existing CPU-only Bleve path.

Common Crawl also does not grant a blanket open license over publisher
content. Its terms require downstream users to respect third-party rights.
Historical inclusion in a crawl is not current robots, noindex, license, or
redistribution permission.

## Decision

Fetchmark open index packs use a strict versioned manifest plus independently
compressed, content-addressed NDJSON shards. Shards contain bounded discovery
metadata, never complete page bodies. A conservative broad-web record may
contain canonical URL metadata, capture time, language, content digest,
provenance, and normalized quality/freshness signals. Title, heading, anchor,
or sketch enrichment requires an explicit pack rights policy and attribution.

The manifest records the pack identity and revision, creation and expiry
times, publisher and takedown contact, rights notice, build inputs and policy
versions, record totals, and each shard's relative path, compressed and
decompressed size, record count, and SHA-256 digest. Snapshot and later delta
artifacts are immutable. A delta must identify its exact parent-manifest
digest and may add, replace, or tombstone canonical URLs.

Publishers sign the exact manifest bytes with Ed25519 using the domain-separated
payload:

```text
fetchmark-open-index-pack-manifest-v1\n || exact manifest.json bytes
```

Trusted public keys are pinned outside the pack in operator configuration;
embedded or remotely discovered keys are not trust roots. Key IDs are derived
from SHA-256 of the public key. The first consumer slice verifies exact
manifest bytes, signature, expected identity and digest, time bounds, shard
paths, declared resource limits, and all shard hashes before importing any
record.

Manifest schema v1 binds enriched lexical records to the frozen
`canonicalurl.V1` contract shared with Fetchmark's cache. Schema v2 retains the
same signed envelope and canonicalization contract but requires conservative
URL-only upserts. A v2 URL-metadata record carries no title, headings, anchor
terms, salient sketch, page-body digest, or Common Crawl WARC digest. Its local
Bleve projection derives bounded lexical terms from the canonical host and path
without distributing query parameters or claiming page-content rights. The v1
record contract remains frozen and consumers continue to accept it.

Registry bindings independently pin the signed digest, revision, record count,
creation time, and expiry time.
Loading a registry is a filesystem trust boundary: arbitrary symlinks,
non-owner files, and group/world-writable files or non-sticky parents fail
closed.

Verified shards are streamed through bounded Zstandard decoding into a new
private Bleve projection. The projection has its own rebuildable schema and is
activated atomically under the exact manifest digest only after record counts,
duplicates, fields, and projection markers validate. Runtime search opens only
complete installed projections and exposes them as an explicitly enabled
advanced-discovery lane. Pack authority scores are retained as evidence but do
not bypass operator-controlled source weights or live reranking. Runtime pack
search rechecks the validity window for every query and bypasses the discovery
response cache so a result cannot outlive pack expiry.

The publisher-side `fetchmark-pack-build` companion is separate from the server
and consumer image. Its version-2 build policy separates the exact digest-
pinned Common Crawl source descriptor from the exact admission-enriched
candidate-stream digest; treating the candidate bytes as though they were the
bytes at the source URI is invalid. It consumes separately produced, current
RFC 9309, noindex, rights, and exclusion evidence. It performs no network
access, emits only v2 URL-metadata snapshots, verifies every produced shard,
signs the manifest and a separate build report, and runs the public 10,000-
record consumer path before marking a lightweight build installable. Its
private identity is never embedded in the pack or ordinary Fetchmark binaries.

The builder verifier applies the same canonical provenance boundary even to
independently authored, correctly re-signed bundles: exactly one source input,
different source and candidate digests, and exactly one provenance entry per
record whose source name matches that input and whose source URI is a canonical
Common Crawl WARC artifact. Generic manifest schema support does not weaken
these publisher-specific checks.

The publisher toolchain now normalizes and deterministically selects a bounded
local Common Crawl URL Index part, and a separate command can collect live
admission evidence. ADR 0010 adds manual consumer-side materialization of one
delta against an exact signed snapshot, and ADR 0011 adds deterministic
publisher-generated deltas. ADR 0012 adopts TUF for an offline,
operator-supplied metadata selection step with a pinned root and durable
rollback state. It still performs no automatic download, registry promotion,
installation, deletion, or hot reload.

## Consequences

- The signed interchange remains auditable and rebuildable across local-index
  schema changes.
- Installation costs one offline projection build and temporary extra disk,
  while query-time search remains local, CPU-only, and free of per-query tolls.
- Signature verification proves publisher/key possession and artifact
  integrity, not accuracy, safety, quality, current crawl permission, or legal
  redistribution rights.
- Exact digest pinning and expiry provide a narrow consumer boundary. The
  offline TUF selector adds root rotation, threshold trust, rollback checks,
  and signed target withdrawal; mirrors, BitTorrent/OCI publication, artifact
  download, and automatic registry promotion remain later work.
- Final result retrieval still passes through Fetchmark's live SSRF, robots,
  noindex/X-Robots-Tag, extraction, and reranking boundaries.
- A synthetic one-million-record consumer prototype now establishes bounded
  import/index size, cold start, query latency, and cgroup peak memory without
  changing the public 10,000-record limit. A real 1–5 million-document build
  still requires selection yield, range-request locality, licensing and
  exclusion rates, repeat-run variance, and an explicit opt-in resource profile.
- A build report proves which exact candidate stream, source input descriptor,
  policy, exclusions, selection counts, and shards a publisher signed. It
  cannot make self-asserted robots, noindex, or rights evidence authoritative;
  an auditable evidence collector and real-corpus publishing exercise are still
  required.

## References

- [Common Crawl overview](https://commoncrawl.org/overview)
- [Common Crawl CDXJ Index](https://commoncrawl.org/cdxj-index)
- [Common Crawl URL Index](https://commoncrawl.org/url-index)
- [Common Crawl Web Graphs](https://commoncrawl.org/web-graphs)
- [Common Crawl terms of use](https://commoncrawl.org/terms-of-use)
- [RFC 8032: Ed25519](https://www.rfc-editor.org/rfc/rfc8032.html)
- [RFC 8878: Zstandard](https://www.rfc-editor.org/rfc/rfc8878.html)
- [The Update Framework specification](https://theupdateframework.github.io/specification/latest/)
