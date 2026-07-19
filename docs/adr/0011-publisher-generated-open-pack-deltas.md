# ADR 0011: Publisher-generated open-pack snapshot deltas

Status: accepted

## Context

ADR 0010 added a manual consumer for a correctly authored signed delta, but
left publishers without a deterministic way to derive that artifact from the
same admission-enriched input used for snapshots. Hand-authored operation
streams cannot prove that removals, replacements, operation totals, and the
resulting projection count describe one exact newly admitted target set.

The producer must remain an offline supply-side tool. It must not turn pack
publication into an updater, weaken the exact snapshot-parent boundary, depend
on hosted services, or require an ordinary Fetchmark node to hold publisher
state.

## Decision

`fetchmark-pack-build build-delta` accepts one exact signed builder-produced
snapshot, one revision-next target selection, the current exclusions, and the
same private publisher identity. It reverifies the parent through an anchored
directory descriptor before comparing records.

Parent and target records are written to separate buckets in an exclusively
opened bbolt database inside the private, descriptor-anchored staging bundle.
This keeps comparison disk-backed. A sorted cursor merge emits exactly one
operation per affected canonical URL:

- a metadata-free tombstone when a parent URL is absent from the target;
- an upsert when a target URL is new or its canonical record changed; and
- no operation when the canonical records are identical.

An empty operation stream is rejected. The delta must retain the parent's pack
ID, format version, profile, publisher metadata, signing key, languages, and
policy; advance the revision by exactly one; and keep its complete validity
window within the parent's. Parent and output directory trees may not overlap.
The private comparison database is closed and removed before signing and
atomic no-replace activation.

The signed manifest binds the exact parent manifest digest and operation
count. A separately domain-separated delta build report binds the target
selection evidence, upsert and tombstone totals, final projection count,
ordered shard evidence, and operation-stream digest. Lightweight deltas must
also pass the actual public snapshot-plus-delta materializer before the report
can claim `ordinary_installable`.

`fetchmark-pack-build verify-delta-run` uses only the public identity and both
portable bundles. It reauthenticates them, checks operation uniqueness and
parent membership, recomputes the stream digest and final count, and reruns the
public materializer for lightweight artifacts.

Neither command downloads, publishes, promotes, revokes, deletes, or mutates a
registry or an older bundle. Delta chains remain unsupported.

## Consequences

- Publishers can produce small reproducible corrections without manually
  constructing a signed operation stream.
- A signed audit report distinguishes operation count from the resulting
  projection count and preserves the exact target admission evidence.
- Format-prototype comparisons use local disk proportional to parent plus
  target state; adequate temporary storage remains an operator requirement.
- ADR 0012 adds offline TUF channel selection and signed target withdrawal.
  Registry promotion, artifact download, rollback selection, mirrors, and
  garbage collection remain explicit later work.

## References

- [ADR 0007: Neutral signed open index packs](0007-neutral-signed-open-index-packs.md)
- [ADR 0010: Manual open-pack delta materialization](0010-manual-open-pack-delta-materialization.md)
- [ADR 0012: Offline TUF open-pack channel selection](0012-offline-tuf-open-pack-channel-selection.md)
- [Publisher guide](../dev/staticvar/fetchmark/open-index-pack-publishing.md)
- [Open index pack operator guide](../dev/staticvar/fetchmark/open-index-packs.md)
