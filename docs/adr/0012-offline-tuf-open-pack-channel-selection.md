# ADR 0012: Offline TUF open-pack channel selection

Status: accepted

## Context

ADR 0007 requires automatic multi-publisher updates to use The Update
Framework rather than a Fetchmark-specific partial update protocol. Signed
pack manifests and operator registries already provide exact artifact and
publisher-key bindings, but they do not provide threshold repository trust,
root rotation, freshness, rollback resistance, or a signed way to withdraw a
previously advertised release.

The first channel step must not silently add networking or automatic
installation. An ordinary Fetchmark node must remain usable without a hosted
service, and operators must retain control over which root, metadata snapshot,
bundle, registry change, and installed object they accept.

## Decision

`fetchmark-pack channel-select` implements the standard TUF client workflow
using the maintained `go-tuf/v2` client. Its runtime fetcher is local-only: it
accepts the exact top-level metadata filenames requested by the verifier from
one descriptor-anchored operator-supplied directory and cannot issue an HTTP
request. The command accepts:

- an exact root document pinned out of band;
- an existing private directory for durable trusted TUF state;
- an operator-supplied metadata directory;
- one top-level `packs/.../manifest.json` target path; and
- a local portable pack bundle when that target is present.

The state directory is a trust boundary. A private bbolt file stores the
SHA-256 of the original bootstrap root and holds an exclusive process lock for
the complete selection. A later invocation must present the same bootstrap
root bytes. The client uses its cached, potentially rotated trusted root on
subsequent runs, so an old bootstrap root cannot undo an accepted root
rotation. TUF timestamp, snapshot, targets, and root metadata remain in the
client's durable local cache, preserving rollback and freeze checks across
invocations. The root and state paths must be clean, non-symlink,
owner-controlled filesystem paths. Bootstrap root, candidate metadata, durable
state, and bundle trees must be disjoint so a verifier write cannot replace an
input or its own trust anchor. All supplied path preconditions are checked
before the state database is created or trusted metadata advances. Before the
maintained client reads an existing cache, every cached role is checked as an
owner-controlled, regular, non-symlink file under its role-specific size cap.

The maintained client may accept and persist a root or another rollback floor
before a later role makes the refresh fail. Fetchmark therefore syncs every
trusted role that exists plus the state directory after both successful and
failed refreshes. A refresh error and any durability error are returned
together; the command never reports an ordinary verification failure while
silently losing evidence that accepted trust state may not be durable.

The first Fetchmark channel profile permits only the top-level targets role;
the presence of delegations is rejected rather than silently ignored.
The selected target is the exact open-pack `manifest.json`, not a mutable
registry and not an installed Bleve directory. Its TUF custom field is strict:

```json
{
  "fetchmark_open_pack": {
    "schema": 1,
    "pack_id": "developer-en",
    "kind": "snapshot",
    "revision": 3,
    "manifest_sha256": "<64 lowercase hexadecimal characters>"
  }
}
```

A delta also declares its exact `parent_manifest_sha256`. Unknown or duplicate
fields fail closed. The custom digest must equal the TUF target SHA-256 and the
digest of the local `manifest.json`. Pack ID, kind, revision, parent digest,
and validity window must match the decoded manifest. The selected manifest
must be currently valid.

An authenticated absence from the current top-level targets role returns
`status: "absent"` without requiring a bundle. Operators may treat absence of
a previously known path as a revocation signal, but the command does not
delete, deactivate, or rewrite anything. A present target returns
`status: "selected"` only after the local manifest bytes match the TUF target.

TUF channel trust and pack publisher trust remain separate. Channel selection
does not verify the detached pack signature or shards; the existing
registry-pinned `fetchmark-pack verify` and `install` commands retain that
responsibility. Selection never changes an operator registry, activates an
object, deletes a rollback artifact, or hot-reloads Fetchmark.

## Consequences

- Operators can carry TUF metadata and bundles across an air gap and obtain
  standard threshold, expiry, rollback, root-rotation, and signed-withdrawal
  semantics without enabling network access in the consumer.
- Trusted metadata may advance even if the local bundle is missing or has the
  wrong bytes. Supplying the exact bundle and repeating the same selection is
  safe; equal timestamp metadata reuses the trusted cached snapshot.
- Target delegation, mirror selection, registry generation/promotion,
  installation, hot reload, and garbage collection remain separate work. ADR
  0013 adds only an explicit bounded single-origin artifact retrieval step.
- The first profile deliberately selects one exact manifest at a time. ADR
  0013 reuses the same pinned-root state for explicit bounded target-artifact
  retrieval while keeping metadata acquisition outside the command.

## References

- [ADR 0007: Neutral signed open index packs](0007-neutral-signed-open-index-packs.md)
- [ADR 0010: Manual open-pack delta materialization](0010-manual-open-pack-delta-materialization.md)
- [ADR 0011: Publisher-generated open-pack snapshot deltas](0011-publisher-generated-open-pack-deltas.md)
- [ADR 0013: Opt-in bounded open-pack artifact retrieval](0013-opt-in-bounded-open-pack-retrieval.md)
- [The Update Framework specification](https://theupdateframework.github.io/specification/latest/)
- [go-tuf v2](https://github.com/theupdateframework/go-tuf)
