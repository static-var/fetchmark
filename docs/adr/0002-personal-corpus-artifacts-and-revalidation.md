# ADR 0002: personal corpus artifacts and conditional revalidation

Status: accepted for the Phase 4 personal-mode slice.

## Context

The Bleve index is a rebuildable discovery projection. It does not store source
bytes or HTTP validators, while the Redis/in-memory response cache expires and
is not a durable ownership boundary. A safe HTTP `304 Not Modified` response
can only reuse the exact stored representation that was validated; an empty 304
is not new content.

Persistent source retention is privacy-sensitive. It must remain disabled by
default, expire predictably, reject unknown/noindex/robots-blocked content, and
make takedowns durable. It must also remain CPU-only and usable without another
service, embeddings, or an LLM.

## Decision

Fetchmark exposes explicit local corpus modes:

- `disabled` (default): no local discovery or source persistence;
- `ephemeral`: a process-local lexical index, lost on restart;
- `personal`: persistent lexical discovery plus durable source artifacts with a
  configurable sliding expiry;
- `curated`: persistent current-version storage with no automatic expiry;
  permitted writes require an explicit authenticated fresh-fetch admission;
- `archive`: a separate bounded multi-version artifact schema described by
  [ADR 0003](0003-bounded-archive-history.md); Bleve remains current-only.

Personal mode requires separate, non-overlapping absolute directories for the
Bleve index and source artifacts. The filesystem artifact store uses:

```text
<artifact-path>/
  schema.json
  urls/<sha256-canonical-url>.json
  objects/<sha256-canonical-url>/<sha256-source-bytes>.body
```

Objects are immutable and content-addressed within one URL. URL-controlled text
never becomes a path segment. Directories use mode `0700`, files use `0600`,
and current pointers advance through a synced temporary file plus atomic rename.
Per-URL addressing avoids cross-URL reference counting so an applicable noindex
or takedown can purge that URL exactly.

Only a cold `200` retrieval with authoritative robots permission, no applicable
`X-Robots-Tag`/HTML noindex or noarchive, and usable extraction may be persisted. The source
object is committed before the Bleve projection becomes searchable. Revocation
orders the operations in reverse safety order: tombstone Bleve first, make the
artifact pointer unreadable, then remove source objects. Takedowns remain sticky
across restart and routine fetching cannot clear them.

The Bleve projection and current artifact pointer share the SHA-256 of the raw
retained representation. Bounded normalized headings and fragment-free HTTP(S)
outbound links are derived only from the extracted reader-view tree. They are
indexing metadata, excluded from public content JSON and response-cache blobs;
any later crawler must still reapply egress, robots, and host-budget checks
before following a retained link.

Curated mode requires both persistent paths, at least one admin API key, and
robots enforcement. Ordinary search/parse retrievals still apply revocations
but cannot admit permitted content. The URL-only admin admission path forces a
fresh SSRF-checked fetch with the configured policy user agent and no proxy,
renderer, conditional reuse, or raw-content input. It commits the artifact
before the Bleve projection and reports only bounded bodyless outcomes. The
admin takedown path establishes a sticky Bleve tombstone before revoking source
bytes. Admission defaults to safety-unclassified and may carry only a validated
batch-level operator assertion of `safe`, `unsafe`, or `unclassified`;
Fetchmark never infers safety from untrusted page text. The assertion is stored
with both the artifact pointer and Bleve projection so an index rebuild does
not silently discard its source-of-truth metadata. Current-version byte,
document, and entry quotas bound curated storage; archive semantics are not
inferred from the absence of an expiry timestamp.

On a hot-cache miss, personal mode may send the current raw ETag and
Last-Modified values only when the stored representation is intact, unexpired,
was retrieved with the same policy user agent, has no redirect-effective URL,
and the request does not use rendering, a proxy, a different policy user agent,
or disabled robots enforcement. A repeated explicit override is eligible only
when it exactly matches the retained policy agent. Conditional headers are
stripped on redirects.

A 304 is bodyless and terminal in the fetcher. The pipeline rechecks that the
same artifact is still current, re-evaluates current robots/header signals plus
stored HTML metadata, hash-verifies and re-extracts the bounded source, advances
validation/expiry metadata, refreshes Bleve, and repopulates the ordinary hot
cache. If the artifact disappeared or changed, Fetchmark makes exactly one
unconditional request; it never extracts an empty 304. Fresh noindex or
noarchive revokes both persistent surfaces and prior content is not reused.
Redirect targets receive their own robots.txt decision and per-host politeness
admission before Fetchmark follows the hop.

This follows the HTTP conditional-request and 304 semantics in
[RFC 9110 sections 13 and 15.4.5](https://www.rfc-editor.org/rfc/rfc9110.html).

## Recovery and limits

Startup validates the artifact schema and every permitted current pointer. It
fails closed on a missing, oversized, or hash-mismatched current body and removes
unreferenced objects left by interrupted writes. Bleve separately validates its
mapping schema marker and refuses an incompatible directory with a rebuild
instruction.

Personal source storage accounts for pointer metadata and bodies under an
aggregate logical byte quota, a per-artifact limit, and a URL-state cardinality
limit. Bleve separately bounds admitted logical content and document count.
Expired documents become synchronously ineligible for reads/search; expired
artifacts are physically reclaimed during startup repair, replacement, or
revocation. Personal mode also runs one non-overlapping sequential sweep at a
configurable interval, using one UTC boundary for Bleve first and artifacts
second. Shutdown cancels and joins this worker before either store closes.

The artifact adapter holds a nonblocking exclusive OS lock for the store
lifetime and rejects concurrent owners. Files are locally private but not
encrypted at rest. Operators remain responsible for filesystem permissions,
backups, and lawful retention. Shared multi-writer storage,
encryption/key management, and explicit takedown lifting remain future work.

## Alternatives

- Reusing Redis was rejected because Redis is optional, entries expire, and the
  fallback is process memory.
- Storing source bytes in Bleve was rejected because it couples privacy deletion
  and version ownership to a rebuildable search projection.
- Global content deduplication was deferred because exact takedown would require
  durable reference counting and transactional garbage collection.
- Serving stale artifacts after validation errors was rejected because changed
  noindex/robots policy could be bypassed.
