# ADR 0014: Verified open-pack registry candidates

Status: accepted

## Context

An operator registry must pin a pack revision before `fetchmark-pack verify` or
`install` will trust it. After ADR 0013, a TUF channel can authenticate and
retrieve a newer exact bundle, but the operator previously had to hand-edit
the registry before publisher-signature and projection verification could run.
That creates a circular and error-prone promotion step.

Installing an immutable projection, replacing a live registry file, and
restarting Fetchmark also cannot be one filesystem transaction. Fetchmark
snapshots the registry and opens projections only at process startup. Those
state changes must remain explicit and recoverable.

## Decision

`fetchmark-pack registry-candidate` is a network-free command that generates a
new registry file for one existing source. It requires the current registry,
the pinned TUF root and local metadata/state directories, the exact target path
and local bundle, and a new `-out` path.

The command first runs the same rollback-resistant TUF selection as
`channel-select`. Signed target absence returns `status: "absent"` and writes
nothing. A selected revision may advance only when:

- the source already exists in the current registry;
- its pack ID and publisher signing key are unchanged;
- the manifest digest and revision advance;
- manifest creation time advances;
- the installed `objects` directory is unchanged;
- a delta names the exact currently bound snapshot parent; and
- a delta supplies an explicit final `-projection-record-count`.

Snapshot manifest and projection counts come from the signed manifest. A
signed delta contains operation count, not final projection count, so the
operator supplies that count and the ordinary delta materializer proves it by
rebuilding the exact parent plus delta. Publisher build reports are not a
consumer trust root.

Before writing the candidate, the command fully materializes the selected
bundle in a private temporary root under the current publisher key and exact
TUF-selected identity, validity, digest, revision, and counts. It cleans that
temporary projection and then writes deterministic registry version 2 JSON to
a private mode-0600 staging file. The candidate is activated with an atomic
descriptor-anchored no-replace rename and the parent directory is synced. An
existing path is never overwritten. The command reports SHA-256 digests of the
exact current and candidate registry bytes for a later compare-and-swap
activation step.

If the no-replace rename succeeds but a later directory sync, descriptor close,
or read-back check fails, the command reports a committed-candidate error with
the exact path and both registry digests. The operator must inspect and verify
that candidate before retrying; a blind retry would correctly encounter the
existing no-overwrite path and obscure whether the first attempt was durable.

Candidate generation does not install into the live object store, replace the
current registry, disable an enabled source, delete an artifact, restart the
server, enroll or rotate a publisher key, or access the network. Operators run
the existing `verify` and `install` commands against the candidate next.
Registry activation remains a distinct step. The compare-and-swap command
defined by ADR 0016 confirms the installed object, compares the exact current
and candidate registry digests, retains exact rollback bytes, and reports that
a restart is required.

## Consequences

- A new revision can be authenticated and projected before it becomes active,
  without weakening the current publisher trust binding.
- Version-1 registries are deterministically represented as version 2 in the
  candidate while preserving unrelated publishers and source bindings.
- The current registry and prior immutable object remain the rollback source.
  Rollback is useful only while the prior manifest remains valid.
- Signed channel withdrawal remains a signal, not an automatic config change.
  Disabling an enabled source requires coordinated discovery configuration and
  restart.
- Compare-and-swap registry activation is a separate explicit command;
  publisher-side mirror synchronization and garbage collection remain separate
  work.

## References

- [ADR 0007: Neutral signed open index packs](0007-neutral-signed-open-index-packs.md)
- [ADR 0010: Manual open-pack delta materialization](0010-manual-open-pack-delta-materialization.md)
- [ADR 0012: Offline TUF open-pack channel selection](0012-offline-tuf-open-pack-channel-selection.md)
- [ADR 0013: Opt-in bounded open-pack artifact retrieval](0013-opt-in-bounded-open-pack-retrieval.md)
- [ADR 0016: Atomic open-pack registry activation and rollback](0016-atomic-open-pack-registry-activation.md)
