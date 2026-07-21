# ADR 0017: Atomic local TUF mirror-head activation

Status: accepted

## Context

ADR 0015 creates complete immutable TUF repository generations, but a staged
generation is not necessarily descended from the live public head. Two
publishers can both stage version 2 from version 1. Copying either stage over a
mirror without comparing the live timestamp can therefore advance TUF metadata
while publishing the wrong application branch.

TUF makes unversioned `metadata/timestamp.json` the repository commit point.
Every object it authenticates must be durable before that file changes. The
manifest and detached publisher signature must also be generation-safe: both
remote filenames now include the exact manifest SHA-256, while portable bundles
retain their local `manifest.json` and `manifest.ed25519` names.

## Decision

`fetchmark-pack-build channel-mirror-activate` performs a filesystem-only,
exact compare-and-swap against an existing operator-owned mirror. It accepts an
out-of-band pinned root, one immutable candidate generation, the pack publisher
public identity, and exact candidate/current timestamp digests. It never takes
a TUF private identity and performs no signing or network access.

Initial publication is explicit:

```sh
fetchmark-pack-build channel-mirror-activate \
  -trusted-root /secure/channel-root.json \
  -mirror /srv/fetchmark-channel \
  -candidate /publisher/channel-generations/generation-1 \
  -target packs/developer/manifest.json \
  -public-identity /secure/publisher-public.json \
  -expected-candidate-timestamp-sha256 CANDIDATE_TIMESTAMP_SHA256 \
  -initialize-empty
```

`-initialize-empty` requires an absent live timestamp and a present version-1
target. It is mutually exclusive with an expected current digest. Forward
activation instead requires distinct exact current and candidate timestamp
digests:

```sh
fetchmark-pack-build channel-mirror-activate \
  -trusted-root /secure/channel-root.json \
  -mirror /srv/fetchmark-channel \
  -candidate /publisher/channel-generations/generation-2 \
  -target packs/developer/manifest.json \
  -public-identity /secure/publisher-public.json \
  -expected-current-timestamp-sha256 LIVE_TIMESTAMP_SHA256 \
  -expected-candidate-timestamp-sha256 CANDIDATE_TIMESTAMP_SHA256
```

The command verifies the candidate before taking the mirror lock: exact pinned
root bytes and self-signature, consistent-snapshot profile, timestamp/snapshot/
targets role signatures and byte bindings, synchronized versions, expiry
horizon, strict single target or authenticated absence, publisher signature,
manifest identity and validity, and every content-addressed shard.

It then anchors the mirror directory and acquires a persistent private advisory
lock. Under that lock it:

1. reads the live timestamp exactly, requiring either explicit absence or the
   expected current digest;
2. verifies the complete live chain and pack artifacts;
3. requires the candidate metadata version to equal live plus one, preserves
   pack identity, advances revision and manifest digest, enforces an exact
   snapshot parent for deltas, and forbids target reintroduction after signed
   withdrawal;
4. copies or verifies immutable root, versioned metadata, hash-prefixed
   manifest and detached signature, and content-addressed shards without
   overwriting different bytes;
5. syncs those dependencies, rereads the exact live head, checks cancellation,
   samples a fresh clock, and revalidates the prepared candidate from the
   mirror bytes;
6. writes and syncs a private timestamp staging file, atomically creates or
   replaces `metadata/timestamp.json`, syncs the mirror, and reads back the
   exact committed bytes.

The mirror retains old versioned metadata and target objects. Manifest,
signature, and shards can safely share one stable target-base URL because all
three are immutable and content-addressed. No generation deletion or automatic
garbage collection is performed.

The advisory lock serializes cooperating commands. The final exact reread
detects unrelated edits made before replacement, but cannot prevent an
administrator from racing after that read. Operators must use this command
consistently for the full cooperating compare-and-swap boundary.

## Failure and recovery states

- Before any immutable dependency commits, failure leaves the head untouched.
- After immutable dependencies are durable but before the timestamp rename, a
  typed prepared error reports the mirror and candidate digest. Those safe,
  invisible dependencies remain reusable; the live head is unchanged.
- Once the timestamp rename succeeds, any descriptor-close, sync, mirror-path,
  readback, or JSON-output failure is a typed committed error with exact
  from/to timestamp evidence. A successful rename is authoritative even when
  closing its directory later fails.
- An exact retry returns `already_applied` only after revalidating the complete
  active chain, publisher signature, manifest, and shards.
- Competing sibling candidates from the same head cannot both commit: after one
  wins, the other fails its exact expected-current comparison.
- A prepared-but-uncommitted generation intentionally claims its next-version
  `N.targets.json` and `N.snapshot.json` names without overwrite. An exact retry
  reuses them. A different sibling at the same version fails closed until an
  operator inspects the unchanged head and explicitly removes the abandoned
  unauthenticated versioned files or chooses a clean mirror. The command never
  makes that destructive recovery decision automatically.

The operation never uploads, signs, deletes, rolls metadata back, restarts the
server, or changes an open-pack registry. A withdrawal is a higher-version
forward publication. Reintroducing a withdrawn target or reverting to older
TUF metadata requires a separately designed trust decision.

## Consequences

- Publisher-side application rollback protection is now tied to the exact live
  mirror head instead of a caller-selected staging parent.
- One fixed metadata and target base can serve successive generations without
  a manifest/signature race.
- Timestamp remains the sole advertised pack-content head. A dual-threshold
  numbered root update can become visible while immutable dependencies are
  prepared; ADR 0018 constrains it to preserve every online role binding.
- Remote/object-store upload protocols, public distribution, and garbage
  collection remain separate work.

## References

- [ADR 0012: Offline TUF open-pack channel selection](0012-offline-tuf-open-pack-channel-selection.md)
- [ADR 0013: Opt-in bounded open-pack retrieval](0013-opt-in-bounded-open-pack-retrieval.md)
- [ADR 0015: Offline TUF repository staging](0015-offline-tuf-repository-staging.md)
- [ADR 0016: Atomic open-pack registry activation](0016-atomic-open-pack-registry-activation.md)
- [ADR 0018: Split-custody TUF root rotation](0018-split-custody-tuf-root-rotation.md)
