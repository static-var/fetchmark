# Open index packs

Open index packs are an optional, local discovery lane. They let an operator
install compact signed URL metadata and build a CPU-only Bleve projection
without turning Fetchmark into a crawler, requiring a hosted registry, or
adding a per-query service dependency. Packs are never enabled by default.

## Trust and format boundary

A bundle contains:

```text
manifest.json
manifest.ed25519
shards/sha256/<compressed-sha256>.ndjson.zst
```

Publisher-built bundles may also contain a separately signed
`build-report.json` and `build-report.ed25519`. The consumer ignores those
audit files; `fetchmark-pack-build verify-run` validates snapshot reports and
`verify-delta-run` validates delta reports plus their exact signed parent.

The detached Ed25519 envelope signs these exact bytes:

```text
fetchmark-open-index-pack-manifest-v1\n || manifest.json
```

The manifest binds the publisher key ID, pack ID and revision, validity
window, build and exclusion policies, rights/takedown notice, record total,
and each shard's compressed and decompressed SHA-256, size, and record count.
The neutral NDJSON records are the distribution contract. The generated Bleve
directory is a local, disposable projection and is never a signed interchange
format.

Manifest schema v1 snapshot records may contain canonical URL, title, headings,
anchor terms, a short salient sketch, language, publication/fetch dates,
content digest, bounded quality evidence, and provenance. They cannot contain
page bodies. Schema v2 requires URL-only records for conservative
metadata packs. The current publisher builder emits v2 URL-metadata snapshots,
and local projections derive bounded lexical terms from canonical host/path
components; the distributed record does not manufacture anchor terms or copy
the Common Crawl WARC digest into a page-body digest. Publisher-supplied
authority and freshness values are retained for evaluation but never boost
Fetchmark's lexical score.

Pack schemas v1 and v2 freeze Fetchmark's `canonicalurl.V1` normalization contract.
Changing canonical URL rules requires a new schema/canonicalization version;
the cache and pack verifier share the same core implementation.

## Operator registry

Trust is pinned outside the bundle. `FM_OPEN_PACK_REGISTRY_FILE` names a strict
versioned JSON file such as:

```json
{
  "version": 1,
  "publishers": [
    {
      "id": "example-publisher",
      "key_id": "<64 lowercase SHA-256 characters>",
      "ed25519_public_key": "<canonical base64 for 32 bytes>"
    }
  ],
  "bindings": [
    {
      "source_id": "openpack-developer",
      "pack_id": "developer-en",
      "publisher_id": "example-publisher",
      "manifest_sha256": "<64 lowercase SHA-256 characters>",
      "revision": 1,
      "record_count": 10000,
      "created_at": "2026-07-18T00:00:00Z",
      "expires_at": "2026-08-18T00:00:00Z",
      "installed_path": "/var/lib/fetchmark/open-packs/objects/<manifest_sha256>"
    }
  ]
}
```

The registry is capped at 1 MiB and 16 publishers/bindings. Unknown fields,
duplicate JSON keys, nulls, trailing JSON, invalid key material, unreferenced
publishers, and paths outside the exact `objects/<manifest digest>` shape fail
startup. The manifest digest, revision, record count, creation time, and expiry
time are exact pins; accepting any different artifact requires an operator
registry change. The registry must be a regular non-symlink file owned by the
Fetchmark process user and must not be writable by group or world. Its
operator-controlled parent directories must not be group/world writable;
sticky operating-system temporary roots are the only writable-parent
exception. Every existing directory component must be owned by the effective
user or root; an arbitrary sticky directory is rejected because its owner could
replace child paths during installation. The server and `fetchmark-pack`
enforce the same checks. Projection roots and object directories apply the same
owner policy in addition to requiring private modes.

Registry version 1 remains the snapshot-only compatibility form above. Version
2 makes the signed artifact kind explicit and separates the number of signed
manifest operations from the number of documents expected in the resulting
projection. A delta binding has this shape:

```json
{
  "version": 2,
  "publishers": [
    {
      "id": "example-publisher",
      "key_id": "<64 lowercase SHA-256 characters>",
      "ed25519_public_key": "<canonical base64 for 32 bytes>"
    }
  ],
  "bindings": [
    {
      "source_id": "openpack-developer",
      "pack_id": "developer-en",
      "publisher_id": "example-publisher",
      "kind": "delta",
      "parent_manifest_sha256": "<exact signed snapshot digest>",
      "manifest_sha256": "<exact signed delta digest>",
      "revision": 2,
      "manifest_record_count": 300,
      "projection_record_count": 9800,
      "created_at": "2026-07-19T00:00:00Z",
      "expires_at": "2026-08-18T00:00:00Z",
      "installed_path": "/var/lib/fetchmark/open-packs/objects/<delta_manifest_sha256>"
    }
  ]
}
```

Version 2 snapshot bindings use `"kind":"snapshot"`, omit
`parent_manifest_sha256`, and require `manifest_record_count` and
`projection_record_count` to be equal. Version 1 fields cannot be mixed into a
version 2 registry.

## Offline verification and installation

Fetchmark does not download packs or discover publisher keys. Put a bundle and
registry on local storage, then run:

```sh
fetchmark-pack verify \
  -registry /var/lib/fetchmark/open-packs/registry.json \
  -source openpack-developer \
  -bundle /var/lib/fetchmark/open-packs/incoming/developer-en \
  -max-projection-bytes 536870912

fetchmark-pack install \
  -registry /var/lib/fetchmark/open-packs/registry.json \
  -source openpack-developer \
  -bundle /var/lib/fetchmark/open-packs/incoming/developer-en \
  -max-projection-bytes 536870912
```

`verify` builds the complete projection under an OS-private temporary root and
removes it after validation. `install` builds beside the final object store and
atomically activates `<root>/objects/<manifest digest>`. Neither command
overwrites an existing object. `-max-projection-bytes` is optional and defaults
to the hard 512-MiB ceiling shown above. An operator may lower it but cannot
raise it. Both commands report the measured logical size as
`projection_bytes`, the final `record_count`, and the signed
`operation_count`. Those counts are equal for snapshots.

To verify or install a registry-v2 delta, retain the exact signed parent
snapshot bundle and pass it explicitly:

```sh
fetchmark-pack verify \
  -registry /var/lib/fetchmark/open-packs/registry.json \
  -source openpack-developer \
  -parent-bundle /var/lib/fetchmark/open-packs/bundles/developer-en-r1 \
  -bundle /var/lib/fetchmark/open-packs/incoming/developer-en-r2-delta

fetchmark-pack install \
  -registry /var/lib/fetchmark/open-packs/registry.json \
  -source openpack-developer \
  -parent-bundle /var/lib/fetchmark/open-packs/bundles/developer-en-r1 \
  -bundle /var/lib/fetchmark/open-packs/incoming/developer-en-r2-delta
```

The parent is reverified from its portable bundle; an installed Bleve object is
never treated as signed source material. The first delta consumer accepts an
exact signed snapshot parent only, so delta-on-delta chains are rejected. The
delta must use the same pack ID, manifest version, publisher metadata and key,
languages, and policy, advance the revision by exactly one, and keep its complete validity
window inside the parent's. Tombstones must name an existing parent URL. Parent
and delta compressed/decompressed bytes are bounded together, and the final
projection remains subject to the public 10,000-record and 512-MiB limits.

Materialization writes a new immutable object under the delta manifest digest
and never mutates the parent projection. To roll back, point the operator
registry at the previously installed snapshot object and restart Fetchmark.
There is no automatic registry mutation, object deletion, hot swap, or garbage
collection; operators should retain both portable bundles and installed objects
until their rollback window closes.

Pack publication uses the separate, network-free `fetchmark-pack-build`
companion and image, which are not included in the ordinary server image. See
[publishing open index packs](open-index-pack-publishing.md) for the exact
input/evidence boundary, profiles, snapshot and delta commands, and independent
build-report verification. Publisher-generated deltas are deterministic,
snapshot-parent-only transitions; promotion into registry version 2 remains a
manual operator decision.

## Offline TUF channel selection

An operator may verify a carried or separately synchronized TUF metadata set
before editing a registry. Create a persistent private state directory once,
pin the initial root through a separate trusted path, and select one exact pack
manifest:

```sh
install -d -m 0700 /var/lib/fetchmark/open-packs/channel-state

fetchmark-pack channel-select \
  -trusted-root /etc/fetchmark/open-pack-channel/root.json \
  -metadata-dir /var/lib/fetchmark/open-packs/channel-metadata \
  -state-dir /var/lib/fetchmark/open-packs/channel-state \
  -target packs/developer/manifest.json \
  -bundle /var/lib/fetchmark/open-packs/incoming/developer-en-r3
```

This command performs no network access. It runs the TUF root, timestamp,
snapshot, and top-level targets workflow against the supplied metadata
directory, persists trusted metadata in `-state-dir`, and verifies that the
bundle's exact `manifest.json` is the selected target. The original bootstrap
root digest is pinned in the state directory; later runs must present the same
bootstrap bytes, while accepted TUF root rotations continue from the cached
trusted root. Reuse one state directory for one channel. The command holds an
exclusive lock and rejects a concurrent invocation. The bootstrap-root file,
candidate metadata directory, durable state directory, and bundle directory
must be disjoint; these paths are validated before the command creates or
advances channel state. Existing cached root, timestamp, snapshot, and targets
metadata must remain regular owner-controlled files within their documented
role limits. After any refresh attempt, including one that accepts a root
rotation before a later role fails, the command syncs every trusted role that
exists and the state directory. It reports refresh and durability failures
together when both occur.

For a signed availability or revocation check, omit `-bundle`. An absent target
returns `status: "absent"`. A present target without a bundle fails because
there are no local artifact bytes to verify. TUF metadata state can advance
before a missing or incorrect bundle is reported; copy the exact bundle and
repeat the same command. Equal metadata versions safely reuse the cached
trusted set.

Channel targets are top-level `packs/.../manifest.json` entries with strict
`fetchmark_open_pack` custom metadata binding schema 1, pack ID, artifact kind,
revision, manifest SHA-256, and a parent manifest SHA-256 for deltas. The TUF
target SHA-256, custom digest, and local manifest digest must be identical.
The first profile rejects target delegations.
Channel selection does not replace pack publisher trust: next create or review
the exact operator-registry binding, then run the existing `verify` and
`install` commands to authenticate the pack signature, shards, counts, and
projection.

### Opt-in bounded channel retrieval

To retrieve the selected portable bundle instead of carrying it manually, use
the separate network-enabled command and state consent explicitly:

```sh
fetchmark-pack channel-fetch \
  -allow-network \
  -trusted-root /etc/fetchmark/open-pack-channel/root.json \
  -metadata-dir /var/lib/fetchmark/open-packs/channel-metadata \
  -state-dir /var/lib/fetchmark/open-packs/channel-state \
  -target packs/developer/manifest.json \
  -target-base-url https://mirror.example.org/fetchmark/targets/ \
  -output /var/lib/fetchmark/open-packs/incoming/developer-en-r3
```

The command still reads all TUF metadata locally. It uses the public-only SSRF
policy for artifact requests, permits HTTPS only, follows no redirect, accepts
identity encoding only, disables connection reuse to prevent transparent
request replay, and remains on the supplied fixed host. It first authenticates
the exact TUF manifest target, then retrieves the strict detached signature and
only that manifest's content-addressed shards. The default aggregate limit is
66 MiB and one shard. Deliberate operator overrides may use
`-max-download-bytes` up to 1 GiB and `-max-shards` up to 64.

This retrieval contract does not carry publisher audit artifacts
`evidence-report.json`, `build-report.json`, or `build-report.ed25519`.
BuildReport-v3 verification therefore applies to the complete publisher-
retained bundle, while channel retrieval currently produces the smaller
manifest-authenticated consumer payload.

Output is a new private staging tree activated with an atomic no-replace rename
only after every byte, size, digest, durability, final cancellation, and
manifest-validity check succeeds. Signed target absence makes no artifact
request and creates no output. A present target failure leaves no activated
bundle unless atomic activation already succeeded; every post-commit close,
parent-identity, or durability failure returns the committed output path for
recovery inspection.

`status: "retrieved"` is not publisher authentication or installation. Review
the registry binding, then pass the retrieved path to `fetchmark-pack verify`
or `fetchmark-pack install`. Channel retrieval never edits the registry,
creates a projection, changes an active source, deletes an old bundle, or
hot-reloads the server. See [ADR 0013](../../../adr/0013-opt-in-bounded-open-pack-retrieval.md).

### Generate and prove a successor registry

For an existing source and publisher key, turn the exact local channel-selected
bundle into a reviewable registry candidate without changing the live registry:

```sh
fetchmark-pack registry-candidate \
  -registry /etc/fetchmark/open-packs.json \
  -source openpack-developer \
  -trusted-root /etc/fetchmark/open-pack-channel/root.json \
  -metadata-dir /var/lib/fetchmark/open-packs/channel-metadata \
  -state-dir /var/lib/fetchmark/open-packs/channel-state \
  -target packs/developer/manifest.json \
  -bundle /var/lib/fetchmark/open-packs/incoming/developer-en-r3 \
  -out /etc/fetchmark/open-packs.r3.candidate.json
```

For a delta, also provide the exact parent snapshot bundle and the operator's
expected final projection count:

```sh
  -parent-bundle /var/lib/fetchmark/open-packs/bundles/developer-en-r2 \
  -projection-record-count 9472
```

The command reruns TUF selection, requires the same pack ID and already pinned
publisher key, and fully materializes the selected snapshot or parent-plus-
delta in a private temporary root. Only then does it atomically create a
mode-0600, no-overwrite version-2 candidate. Its JSON result includes the exact
base and candidate registry SHA-256 digests. Signed target absence creates no
candidate.

If the command reports that the candidate was committed but completion failed,
use the reported path and both registry digests to inspect and run `verify`
against that file before retrying. Do not delete or blindly rerun over the
no-overwrite path: the atomic rename succeeded even though a later durability
or read-back check did not.

Prove and install through the existing consumer commands:

```sh
fetchmark-pack verify \
  -registry /etc/fetchmark/open-packs.r3.candidate.json \
  -source openpack-developer \
  -bundle /var/lib/fetchmark/open-packs/incoming/developer-en-r3

fetchmark-pack install \
  -registry /etc/fetchmark/open-packs.r3.candidate.json \
  -source openpack-developer \
  -bundle /var/lib/fetchmark/open-packs/incoming/developer-en-r3
```

Delta verification and installation repeat `-parent-bundle`. Candidate
generation and installation do not replace the live registry. After reviewing
the candidate and installing its immutable projection, activate it with the
exact digests reported by `registry-candidate`:

```sh
fetchmark-pack registry-activate \
  -registry /etc/fetchmark/open-packs.json \
  -source openpack-developer \
  -candidate /etc/fetchmark/open-packs.r3.candidate.json \
  -expected-current-sha256 BASE_REGISTRY_SHA256 \
  -expected-candidate-sha256 CANDIDATE_REGISTRY_SHA256 \
  -rollback-out /etc/fetchmark/open-packs.r2.rollback.json
```

The command opens the destination projection, serializes cooperating writers,
preserves the exact current registry bytes without overwriting an existing
different rollback file, rechecks the live bytes immediately before atomic
replacement, and reports `restart_required: true`. It does not restart or
signal the server. An already-running Fetchmark process continues using its
startup-opened projection until the operator performs a controlled restart.

If the new revision must be reverted, keep both immutable objects, stop or
restart the process as appropriate, and use the exact prior registry file:

```sh
fetchmark-pack registry-rollback \
  -registry /etc/fetchmark/open-packs.json \
  -source openpack-developer \
  -rollback /etc/fetchmark/open-packs.r2.rollback.json \
  -expected-current-sha256 ACTIVE_R3_REGISTRY_SHA256 \
  -expected-rollback-sha256 PRIOR_R2_REGISTRY_SHA256 \
  -rollback-out /etc/fetchmark/open-packs.r3.rollback.json
```

Rollback is accepted only as the exact reverse of the promotion and only while
the prior projection still opens within its signed validity window. It archives
the newer registry before restoring the prior bytes and also requires a server
restart. Prepared failures leave the live registry unchanged and report the
durable rollback archive; committed failures report exact live and rollback
digests for inspection. Never blindly retry or copy over either file. See
[ADR 0014](../../../adr/0014-verified-open-pack-registry-candidates.md) and
[ADR 0016](../../../adr/0016-atomic-open-pack-registry-activation.md).

The first consumer slice accepts one shard, at most 10,000 records, 64 MiB
compressed, 256 MiB decompressed, and a single non-dictionary Zstandard frame.
It verifies the compressed bytes before decoding, streams strict NDJSON under
independent record/field limits, rejects duplicate canonical URLs, writes a
private projection, verifies its identity marker, and only then performs a
no-replace rename. Verification reopens the closed Bleve projection, reads back
all marker fields, checks the exact manifest identity and validity window, and
requires its document count to equal the registry-pinned final projection count
plus the marker. For snapshots that count also equals the signed operation
count; for deltas the two counts may differ.
Every projection entry must then be a regular file and its checked aggregate
logical size must fit the selected activation ceiling. The ceiling prevents an oversized projection from being
activated; it is not a filesystem quota and does not promise that temporary
construction cannot exhaust an already-full volume. Failed or canceled imports,
including cancellation after several committed batches and late semantic
corruption, leave neither an active projection nor an `.import-*` or `.delta-*`
staging directory on ordinary return paths; a staging-cleanup failure is returned to the
operator rather than discarded. Abrupt process termination or an underlying
filesystem cleanup failure may still leave a private staging directory for
operator cleanup.
The derived projection currently uses schema v2 with 512-record writes and no
unused Bleve doc values; this is a rebuildable local format, not part of the
signed pack interchange contract.

## Measured operator profiles

The public installer remains capped at 10,000 records. A varied 10k signed-pack
control completed under a 192-MiB Linux cgroup with 131.30 MiB peak memory and a
23.73-MB projection; 256 MiB is the recommended lightweight container budget.
An operator may pair that profile with a lower `-max-projection-bytes` value to
reject unusually large projections before activation.

The opt-in scale harness also completed three 100,000-record runs under a
512-MiB limit. Median peak memory was 403.02 MiB and the median projection was
206.51 MB, with a maximum 405.36-MiB peak. This is test-only evidence, not a
supported public import size; a future 100k profile should reserve at least
640 MiB for headroom. See the
[varied container scale gate](../../../benchmarks/open-index-pack-varied-scale.md)
for exact commands, raw measurements, and limitations.

A separate test-only four-shard prototype completed 250k, 500k, and one-million
record gates under exact Linux cgroup limits. The one-million run built a
1.930-GB logical projection in 110.435 seconds, sustained 9,055 records/s, and
peaked at 2.505 GiB under a 5-GiB limit; search p50/p95 across 96 queries was
5.592/10.652 ms. These elevated limits are unexported and unreachable from the
public installer and CLI. See the
[one-million-record prototype](../../../benchmarks/open-index-pack-million-prototype.md)
for the runner, exact shard digests, raw evidence, and remaining real-corpus
work.

## Enabling a discovery lane

Add a source to an operator discovery-pack file and explicitly allow it:

```json
{
  "id": "openpack-developer",
  "kind": "openpack",
  "weight": 1,
  "max_results": 20,
  "timeout_ms": 2000,
  "max_concurrency": 2,
  "rate_per_second": 100,
  "burst": 10
}
```

Reference that source from a pack lane, set `FM_DISCOVERY_PACK_FILE`, add the
source ID to `FM_DISCOVERY_ENABLED_SOURCES`, and set
`FM_OPEN_PACK_REGISTRY_FILE`. The built-in source registry and default enabled
source list do not include open packs, so existing installations are
unchanged.

The server rechecks the signed validity window on every pack search. Open-pack
search is intentionally not placed behind the discovery response cache, so a
hit cannot remain usable after expiry or after the clock moves before the
signed creation time.

Open-pack results are discovery candidates only. Fetchmark still performs its
normal live SSRF checks, RFC 9309 robots handling, noindex/X-Robots-Tag checks,
fetch, extraction, deduplication, and reranking before a result is returned.
Safe-search values above zero exclude all current pack records because the
format intentionally provides no trusted safety classification.

## Current limitations

- Delta publication and materialization are snapshot-parent-only. The
  publisher builder emits and independently verifies signed deltas, but there
  is no delta chain, automatic registry promotion, installation, or
  garbage-collection service.
- Offline, operator-supplied TUF metadata can select or withdraw one exact pack
  manifest. Opt-in `channel-fetch` can retrieve its bounded bundle, and
  `registry-candidate` can create a verified no-overwrite successor registry.
  Explicit digest-bound `registry-activate` and `registry-rollback` commands
  apply a reviewed transition while retaining exact rollback bytes. There is
  still no automatic/background downloader or promotion service, remote root
  discovery, delegated-target profile, hot reload, garbage collection, remote
  upload protocol, or BitTorrent/OCI publishing. The offline publisher
  companion can generate a complete immutable single-target TUF repository
  stage and activate it into a local mirror by compare-and-swapping the exact
  live timestamp digest. Immutable objects are copied first and old objects are
  retained; transport from that local mirror remains operator-owned.
  The offline `-previous` check remains relative to the caller-selected
  generation, but `channel-mirror-activate` independently verifies the live
  chain and rejects stale sibling branches before timestamp replacement.
  In that publisher profile, signed withdrawal is terminal for the repository
  identity; target reintroduction requires a new explicit trust bootstrap.
- No Common Crawl processing on an ordinary Fetchmark node. The offline
  publisher tool can create a deterministic bounded evidence input from one
  exact local flat URL Index Parquet part, or a globally ranked prototype
  selection from several exact parts under an explicit 5-million-row cap.
  An offline partition step can turn that selection into deterministic,
  selected-host-disjoint, line-exact inputs for unchanged 10,000-row evidence
  collectors. A network-free verifier now restores exact global order, verifies
  every bundle and aggregate evidence relationship, and binds the report into
  signed snapshot BuildReport v3. Robots redirect targets still require shared
  pacing, so current partition jobs are serial. Download, job execution, and
  current admission evidence remain separate operator-owned work. Collector
  bundles are not worker-authenticated, and delta reports are not yet aggregate-
  evidence-bound. The pack builder still consumes admission-enriched candidates. Common Crawl URL indexes locate
  captures; they are not a live full-text search API.
- The one-million-record result is a synthetic, test-only consumer prototype;
  it is not a supported operator profile or evidence of real-corpus selection,
  licensing/exclusion quality, update distribution, or rollback behavior.
- A signature proves artifact integrity and publisher-key possession. It does
  not prove accuracy, safety, license validity, current robots permission, or
  commercial-index coverage/freshness parity.

See [ADR 0007](../../../adr/0007-neutral-signed-open-index-packs.md) for the
format decision and
[ADR 0010](../../../adr/0010-manual-open-pack-delta-materialization.md) for the
manual delta lifecycle boundary, and
[ADR 0011](../../../adr/0011-publisher-generated-open-pack-deltas.md) for the
deterministic producer boundary, and
[ADR 0012](../../../adr/0012-offline-tuf-open-pack-channel-selection.md) for the
offline update-channel trust boundary, and
[ADR 0016](../../../adr/0016-atomic-open-pack-registry-activation.md) for the
activation and rollback boundary. The initial measured consumer gate is
recorded in the [10k benchmark](../../../benchmarks/open-index-pack-10k.md).
