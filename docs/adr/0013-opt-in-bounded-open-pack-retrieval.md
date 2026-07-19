# ADR 0013: Opt-in bounded open-pack artifact retrieval

Status: accepted

## Context

ADR 0012 deliberately made the first TUF channel selector local-only. That
established pinned-root freshness, rollback, rotation, and signed-withdrawal
semantics without silently turning an ordinary Fetchmark installation into a
networked updater. Operators still had to carry an exact portable bundle into
place before selection could verify its manifest.

The next useful operator step is retrieval, not automatic installation. It
must preserve explicit consent, Fetchmark's public-egress boundary, portable
pack limits, TUF manifest authenticity, publisher-trust separation, and
no-overwrite filesystem activation.

## Decision

`fetchmark-pack channel-fetch` is a separate opt-in command. It runs only when
the operator supplies `-allow-network`, an exact canonical HTTPS
`-target-base-url`, and a new absolute `-output` path. TUF metadata remains an
operator-supplied local input: the command does not discover or download root,
timestamp, snapshot, or targets metadata.

The command performs the same pinned-root TUF refresh as `channel-select`.
When the selected target is absent, it returns authenticated `status:
"absent"` without making an artifact request or creating an output directory.
When present, retrieval proceeds in this order:

1. Fetch the exact selected `manifest.json` target. Consistent-snapshot
   repositories use the selected manifest SHA-256 filename prefix. The TUF
   target length and every target hash are verified before the manifest is
   accepted.
2. Strictly decode the channel-bound open-pack manifest and fetch its bounded
   `<manifest-sha256>.manifest.ed25519`, storing it as the portable bundle's
   `manifest.ed25519`. The detached signature schema and key ID must match the
   manifest. Cryptographic publisher authentication remains the responsibility
   of the registry-pinned `verify` or `install` command.
3. Fetch only the manifest-declared content-addressed shard paths. Each shard
   is streamed directly to a private staging file and must match its exact
   compressed length and SHA-256.
4. Sync files and directories, then atomically rename the complete staging
   tree into a previously nonexistent output path with no-replace semantics.
   Immediately before that rename, recheck cancellation and the selected
   manifest's validity window. Failure before activation removes the
   command-owned staging directory. Once the rename succeeds, any later
   descriptor-close, parent-identity, or durability error returns the committed
   path explicitly so an operator never has to infer whether activation won.

The default retrieval profile permits one shard and 66 MiB across manifest,
signature, and shards. Operators may deliberately raise the limits, but never
above 64 shards or 1 GiB per invocation. Downloads are sequential, use no
automatic retry, disable HTTP connection reuse so Go cannot transparently
replay an idempotent request on a stale pooled connection, accept only identity
transfer encoding, and have a five-minute per-request timeout in addition to
process cancellation.

The target base must be a canonical HTTPS directory URL without credentials,
query, fragment, or escaped path. Every request remains on that derived fixed
origin. Fetchmark's external egress policy resolves the base before trusted
state changes, rejects private, loopback, link-local, CGNAT, and other unsafe
addresses, rechecks the connected address against DNS rebinding, and rejects
the first redirect. The output may not be inside the trusted-state or metadata
tree.

`channel-fetch` never edits an operator registry, authenticates a publisher
key, installs a Bleve projection, changes an active source, deletes an older
artifact, or hot-reloads Fetchmark. A successful result means only that the
portable local bundle is the exact channel-selected manifest plus
content-addressed bytes. Operators must still review the registry binding and
run `fetchmark-pack verify` or `install`.

## Consequences

- Air-gapped selection remains available and unchanged through
  `channel-select`; networking is neither implicit nor required.
- Mirrors can distribute ordinary portable bundle files without becoming a
  paid per-query dependency or a trusted proxy for page traffic.
- A malicious mirror can waste one bounded request or supply an invalid
  detached signature, but it cannot substitute the manifest or shards and
  cannot cause installation. Publisher authentication fails later unless the
  detached signature verifies under the operator registry.
- Repository metadata publishing, mirror synchronization, registry
  generation/promotion, rollback policy, installation, and garbage collection
  remain separate explicit workflows.

## References

- [ADR 0007: Neutral signed open index packs](0007-neutral-signed-open-index-packs.md)
- [ADR 0012: Offline TUF open-pack channel selection](0012-offline-tuf-open-pack-channel-selection.md)
- [The Update Framework specification](https://theupdateframework.github.io/specification/latest/)
