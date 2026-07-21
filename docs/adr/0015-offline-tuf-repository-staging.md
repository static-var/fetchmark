# ADR 0015: Offline TUF repository generation and immutable mirror staging

Status: accepted

## Context

ADR 0012 introduced a rollback-resistant TUF consumer, but Fetchmark did not
produce the repository metadata that consumer expects. Operators had to create
root, targets, snapshot, and timestamp metadata with external tooling and
manually reproduce Fetchmark's strict target custom metadata and consistent-
snapshot artifact layout.

Publishing directly to a mutable remote mirror would mix offline signing,
filesystem durability, network credentials, and public cutover in one command.
It would also make a partially uploaded repository visible if timestamp were
published before its immutable dependencies.

## Decision

`fetchmark-pack-build channel-keygen` creates a private, mode-0600 repository
identity containing distinct Ed25519 keys for root, targets, snapshot, and
timestamp. Root uses two keys and a two-of-two threshold; the other roles use
one key each. Both root keys currently live in the same offline identity file,
so this provides TUF threshold semantics but not an independently operated
quorum. The identity embeds the exact signed bootstrap-root bytes generated at
key creation; later publisher binaries reuse and validate those immutable bytes
instead of reserializing the trust anchor. The private file is fsynced and
atomically claimed without overwrite. Splitting key custody and root rotation
remain later ceremonies.

`fetchmark-pack-build channel-stage` performs no network access. It accepts the
private repository identity, one already signed portable pack bundle, the pack
publisher's public identity, exact metadata expiries, one logical
`packs/.../manifest.json` target, and a new output directory.

Before TUF signing, the command runs the full public publisher verifier. A
snapshot must pass the ordinary build, shard, report, and materialization
checks. A delta additionally requires and revalidates its exact parent snapshot
and final projection. The staging adapter independently rechecks the manifest
signature, current validity, shard sizes, and shard hashes, and requires the
manifest SHA-256 returned by that full verifier. A concurrently replaced valid
bundle therefore cannot be signed unless it has the exact fully verified
manifest identity.

Every repository uses `consistent_snapshot: true` and the following immutable
layout:

```text
<generation>/
  root.json
  metadata/
    1.root.json
    timestamp.json
    <version>.snapshot.json
    <version>.targets.json
  targets/
    packs/<channel>/<manifest-sha256>.manifest.json
    packs/<channel>/<manifest-sha256>.manifest.ed25519
    packs/<channel>/shards/sha256/<shard-sha256>.ndjson.zst
```

The root is the out-of-band bootstrap document. Targets binds the exact
manifest length, SHA-256, and strict Fetchmark custom metadata. Snapshot binds
the exact serialized targets metadata bytes; timestamp binds the exact
serialized snapshot bytes. Signed references remain unprefixed. Timestamp is
written last in the private stage because it is the publication commit point
when an operator later mirrors the generation.

The initial synchronized targets/snapshot/timestamp version must be 1. A later
generation requires the exact previous immutable generation and advances all
three roles by exactly one. The previous root and metadata signatures are
verified. A present successor must preserve pack ID while advancing revision
and manifest digest. A delta must name the exact previously advertised
snapshot parent. `-withdraw` creates a higher-version authenticated absence and
forbids bundle or publisher-key inputs. Withdrawal is terminal for the current
single-target repository identity: a later generation may renew the signed
absence, but cannot reintroduce a target because the immediately previous
metadata no longer carries an application rollback floor. Reintroduction needs
a future explicit history-bearing profile or a newly bootstrapped repository.

`-previous` names a caller-selected local generation. Staging proves monotonic
application state only relative to that selected parent; it does not know which
generation is currently published. Two version-2 branches can therefore be
created from version 1. Publishing a later version derived from a stale branch
after a higher pack revision has already gone live could advance TUF metadata
while rolling the application revision back. ADR 0017's local mirror activation
therefore compares the live timestamp digest with the exact expected previous
timestamp before switching it and rejects stale siblings. Remote/object-store
publication must provide an equivalent compare-and-swap boundary rather than
copying a caller-selected stage blindly.

The entire private mode-0700 generation is fsynced and activated with an atomic
descriptor-anchored no-replace rename. Cancellation immediately before rename
writes nothing. A failure after rename returns a typed committed-stage error
with the output path and exact root, targets, snapshot, timestamp, and manifest
digests. Operators inspect that generation instead of blindly retrying. The
timestamp must retain at least fifteen minutes of validity both when staging
starts and immediately before activation.

The command never uploads, overwrites, activates, deletes, hot-reloads, or
changes an operator registry. Operators distribute `root.json` out of band and
copy immutable metadata/targets to their own HTTPS mirrors. A remote publication
mechanism must upload immutable targets and versioned metadata first and switch
unversioned `timestamp.json` last.

The remote manifest and detached publisher signature are both named with the
exact manifest SHA-256. The portable bundle still uses `manifest.ed25519`, but
the immutable repository copy is `<manifest-sha256>.manifest.ed25519`. This
lets one mutable mirror retain multiple generations without exposing an old
manifest with a new signature during timestamp cutover. Older immutable
objects remain available for in-flight clients and rollback protection.

## Consequences

- Fetchmark now produces a repository that its existing `channel-select` and
  `channel-fetch` contracts can consume without hand-authored metadata.
- TUF channel keys remain separate from pack publisher keys.
- Exact prior-generation validation prevents metadata-version advancement from
  disguising a pack revision rollback within the caller-selected chain. It is
  not a substitute for compare-and-swap publication against the live head;
  ADR 0017 supplies that separate activation boundary.
- A complete generation can be carried across an air gap or mirrored with
  ordinary self-hosted file distribution.
- Root rotation and split root-key custody are supplied by ADR 0018. Delegated/
  multi-target repositories, retention cleanup, remote upload, OCI/BitTorrent
  distribution, and public service operation remain explicit future work.

## References

- [ADR 0012: Offline TUF open-pack channel selection](0012-offline-tuf-open-pack-channel-selection.md)
- [ADR 0013: Opt-in bounded open-pack artifact retrieval](0013-opt-in-bounded-open-pack-retrieval.md)
- [ADR 0014: Verified open-pack registry candidates](0014-verified-open-pack-registry-candidates.md)
- [ADR 0017: Atomic local TUF mirror-head activation](0017-atomic-tuf-mirror-head-activation.md)
- [ADR 0018: Split-custody TUF root rotation](0018-split-custody-tuf-root-rotation.md)
- [The Update Framework specification](https://theupdateframework.github.io/specification/latest/)
- [TUF metadata roles](https://theupdateframework.io/docs/metadata/)
