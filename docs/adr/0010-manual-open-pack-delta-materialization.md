# ADR 0010: Manual open-pack delta materialization

Status: accepted

## Context

ADR 0007 defines immutable signed snapshots and deltas but intentionally left
delta application to later lifecycle work. Rebuilding and redistributing a full
snapshot for every small removal or metadata correction is wasteful. At the
same time, accepting a delta against an installed Bleve directory would treat a
local disposable projection as signed source material and make rollback depend
on mutable state.

This slice must not become an automatic updater. Fetchmark has no remote trust
root, download channel, revocation feed, garbage collector, or hot-reload
protocol. Those require a broader rollback and multi-publisher security model.

## Decision

The standalone, network-free `fetchmark-pack` consumer may manually verify and
materialize one signed delta against one exact signed snapshot bundle. Registry
version 2 pins:

- artifact kind and target manifest digest;
- exact parent snapshot manifest digest for a delta;
- signed manifest operation count;
- expected final projection record count; and
- the existing publisher key, pack, revision, creation, expiry, and installed
  object path.

Registry version 1 remains snapshot-only and wire-compatible. Versioned count
fields cannot be mixed: version 1 uses `record_count`; version 2 uses
`manifest_record_count` and `projection_record_count`.

The operator supplies both portable bundle directories. The consumer reverifies
the parent manifest, signature, shard descriptors, and shard bytes. It never
reads an installed Bleve projection as parent input. The first implementation
requires an exact snapshot parent, the same manifest schema, pack ID, signing
key, languages, and policy, and a target revision exactly one greater than the
parent. The delta validity window must be fully contained by the parent's.

Parent and delta resources are bounded both independently and in aggregate.
Delta records may replace existing canonical URLs, add new URLs, or tombstone
existing URLs. A tombstone for a URL absent from the parent fails closed. The
consumer streams the verified parent, applies the delta in memory-bounded form,
and builds a completely new private Bleve projection. It verifies the closed
projection and exact final count before a no-replace rename under the delta
manifest digest. Failure or cancellation leaves the parent projection and
portable bundles unchanged.

Both `verify` and `install` require `-parent-bundle` for a delta and forbid it
for a snapshot. Results report signed `operation_count` separately from final
`record_count`.

Rollback is explicit and offline: retain the parent bundle and installed parent
object, change the operator registry back to that exact binding, and restart
Fetchmark. The command does not change registry files, select a revision,
delete objects, hot-swap a running server, or fetch artifacts.

## Consequences

- Small signed corrections and removals no longer require distributing a full
  replacement snapshot to operators who opt into the manual workflow.
- Every active projection is still derived only from portable signed input and
  can be rebuilt independently of Bleve's storage format.
- Delta-on-delta chains are not supported. A publisher that needs a later
  revision must currently publish a new snapshot parent or a delta against the
  retained snapshot.
- Publisher tooling now emits deterministic snapshot-parent deltas under
  [ADR 0011](0011-publisher-generated-open-pack-deltas.md). Automatic download,
  registry promotion, mirrors, hot reload, rollback selection, and garbage
  collection remain separate work. ADR 0012 adds offline TUF metadata
  verification and signed target withdrawal without changing this manual
  materialization boundary.
- Keeping both parent and target objects temporarily increases disk use, but it
  preserves a simple operator-controlled rollback path.

## References

- [ADR 0007: Neutral signed open index packs](0007-neutral-signed-open-index-packs.md)
- [ADR 0011: Publisher-generated open-pack snapshot deltas](0011-publisher-generated-open-pack-deltas.md)
- [ADR 0012: Offline TUF open-pack channel selection](0012-offline-tuf-open-pack-channel-selection.md)
- [Open index pack operator guide](../dev/staticvar/fetchmark/open-index-packs.md)
- [The Update Framework specification](https://theupdateframework.github.io/specification/latest/)
