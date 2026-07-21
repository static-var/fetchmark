# ADR 0003: bounded archive history

Status: accepted for the Phase 4 archive slice.

## Context

Personal and curated retention own one current source representation per URL.
Archive operators need a bounded record of distinct representations without
turning Bleve into a historical database, silently evicting retained history,
or making ordinary installations crawl the web. Retention must remain opt-in,
CPU-only, robots/noindex/noarchive aware, and exactly purgeable per URL.

## Decision

Archive uses a distinct artifact schema and a new empty artifact directory:

```text
<artifact-path>/
  schema.json                         # version 2, mode archive
  urls/<url-id>.json                  # current pointer and commit set
  objects/<url-id>/<content-hash>.body
  versions/<url-id>/<content-hash>.json
```

The ordered hashes in the current pointer are the committed history. Bodies
and immutable manifests written before a failed pointer advance are orphans and
startup repair removes them. A new distinct permitted representation commits
body, then manifest, then pointer. Repeat bytes and conditional revalidation
update current validation metadata without consuming another version. A return
to a previously retained hash selects that committed version without creating a
duplicate.

Bleve remains a rebuildable, current-version-only lexical projection. Archive
history is accessible only through the internal artifact reader contract; this
slice adds no public history endpoint.

Archive admission is automatic only after the same authoritative robots,
noindex/noarchive, SSRF, extraction, and source-size checks as personal mode.
Explicit admin admission remains curated-only. Authenticated admin takedown is
available in both curated and archive modes.

New distinct versions are rejected atomically when aggregate bytes, URL-state
entries, aggregate versions, or per-URL versions would exceed configured
limits. The store does not evict older history to make room. Opening with quotas
below retained state fails closed.

Revocation first makes the current pointer bodyless with an empty commit set,
then purges that URL's object and manifest directories. Startup repair completes
interrupted purge and removes uncommitted files. Missing, corrupt, mismatched,
symlinked, or non-regular committed state fails open rather than serving a
partial history.

The adapter holds an exclusive OS ownership lock, so two Fetchmark processes
cannot maintain divergent cached quota accounting for one directory. Operators
must still protect the unencrypted directory from unrelated mutation.

## Compatibility and limits

Schema-v1 personal/curated directories are not migrated automatically and
schema-v2 archive directories cannot be opened as personal or curated. This
avoids an implicit, rollback-hostile retention change. Operators select a new
empty artifact directory when enabling archive mode.

This is distinct-content application history. It is not WARC, a repeated
observation timeline, shared multi-writer storage, a legal-preservation system,
or a public redistribution mechanism. Encryption, explicit takedown lifting,
and history export are outside this slice.
