# ADR 0018: Split-custody TUF root rotation

Status: accepted.

## Context

The original offline repository identity contained both keys for a two-of-two
root threshold. That bootstrap is safer than a one-key root, but one copied
identity file still compromises the entire threshold. TUF root updates are
portable signed metadata: rejecting a malicious transition only at final
assembly is too late if a custodian has already emitted a valid signature.

Fetchmark also needs to keep its original out-of-band `root.json` stable while
publisher generations and mirrors carry later numbered roots. The online
targets, snapshot, and timestamp keys must not change as a side effect of this
root-only ceremony.

## Decision

Fetchmark supports a strict, offline, full two-key root rotation:

1. Each new custodian generates one Ed25519 key in a separate private signer
   document. Only its public document is sent to the coordinator.
2. The coordinator prepares a deterministic unsigned successor from the exact
   current signed root and two public signer documents. The successor advances
   by one version, fully replaces both root keys, preserves all three online
   role bindings byte-for-byte, and retains the current supported TUF 1.x spec
   version.
3. Every old and new custodian independently receives the exact current root,
   unsigned candidate, both expected SHA-256 digests, repository ID, and a
   fresh local clock. Before emitting a signature, the signer validates the
   complete fixed Ed25519 profile, sequential root-only transition, candidate
   publication horizon, repository binding, and that its key belongs to
   exactly one side of the transition.
4. Assembly accepts four unique key contributions and verifies both the old
   and new two-of-two thresholds. It produces deterministic signed root bytes.
5. Applying that root creates a new version-2 operational identity containing
   the original public bootstrap, the exact ordered public root updates, and
   only the targets/snapshot/timestamp private keys. It contains no root private
   key. Later rotations use the separately held current custodian files.

The first migration has an explicit legacy signing command because the two old
keys begin in one version-1 identity. It writes both contributions as one
bounded artifact. Later assemblies take four individual custodian artifacts
and do not accept or infer a legacy identity.

All private keys, candidates, contributions, assembled roots, and migrated
identities are created at new paths with mode 0600 and never overwrite an
existing artifact. A committed command-output failure reports the exact path
and digest for recovery. A deterministic public-export command derives the
same public custodian document from an already committed private signer, so a
lost keygen result stream never forces key replacement or custom tooling.
Fetchmark never automatically deletes the legacy
identity; the operator retires it only after the rotated stage and mirror have
been independently verified.

An expired current root may authorize a sequential fresh successor when both
old keys remain available, as allowed by TUF. The candidate must retain at
least Fetchmark's minimum publication horizon at preparation, every signature,
assembly, identity application, repository staging, and mirror cutover.

Repository stages preserve `root.json` as the exact out-of-band bootstrap and
emit every `metadata/N.root.json` update. A previous generation may contain a
strict prefix of the identity chain. Mirrors reject gaps, divergent prefixes,
and more than 32 rotations, and verify metadata under the active root.

## Consequences

- Compromise of the operational staging identity no longer compromises root
  trust. Root rotation requires both separately held current custodians.
- Custodians can validate a proposal without trusting the coordinator or the
  Fetchmark assembler.
- Existing consumers continue from the original pinned bootstrap and cache
  accepted rotations through the standard TUF update flow.
- A numbered root can become visible while mirror dependencies are prepared,
  before the timestamp content-head compare-and-swap. This is a valid
  dual-threshold trust transition and does not change online role bindings;
  timestamp remains the sole advertised pack-content commit point.
- Custodian recovery, hardware-token integration, and formal multi-party
  operational drills remain operator work.

## References

- [ADR 0012: Offline TUF open-pack channel selection](0012-offline-tuf-open-pack-channel-selection.md)
- [ADR 0015: Offline TUF repository staging](0015-offline-tuf-repository-staging.md)
- [ADR 0017: Atomic local TUF mirror-head activation](0017-atomic-tuf-mirror-head-activation.md)
- [TUF specification: update the root role](https://theupdateframework.github.io/specification/latest/#update-the-root-role)
