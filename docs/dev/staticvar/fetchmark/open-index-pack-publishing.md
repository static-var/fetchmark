# Publishing open index packs

`fetchmark-pack-build` is an offline publisher companion. It is intentionally
absent from the ordinary Fetchmark server image and does not perform network
requests. A publisher prepares and audits its inputs elsewhere, then gives the
builder immutable local files.

The current builder is a supply-side format and policy gate, not a Common Crawl
downloader or permission oracle. It cannot turn historical crawl inclusion into
current robots permission, indexability, or a license.

## Publisher boundary

Use a dedicated publisher host or job with network disabled during signing.
Keep the private identity outside the bundle, ordinary Fetchmark installations,
and any public artifact store. The separate container is built with:

```sh
docker build -f deploy/openpack-builder.Dockerfile \
  --build-arg VERSION=<immutable-source-revision> \
  -t fetchmark-pack-builder:dev .
```

Pass an immutable source revision for auditable publisher builds. If the
linked version is left at the local `dev` default, the command replaces it in
signed manifests and reports with `binary-sha256:<digest>`, where the digest
identifies the exact publisher executable. A publishable artifact therefore
never claims the non-unique builder identity `dev`.

Run it with `--network none`, a read-only root filesystem, an appropriately
sized private `/tmp`, read-only input mounts, and one writable output mount.
The runtime contains only the `fetchmark-pack-build` executable; it has no
server, crawler, standalone consumer command, or peer listener. The builder
does statically reuse the consumer installation code for its lightweight gate.

The image defaults to numeric UID/GID 65532. Secure inputs must be owned by the
effective container UID, and the output parent must be owned by that UID or
root without group/world write permission. For host bind mounts, explicitly
run with the numeric owner of the prepared files and output directory:

```sh
docker run --rm --network none --read-only \
  --user "$(id -u):$(id -g)" \
  --tmpfs /tmp:rw,noexec,nosuid,mode=1777,size=1g \
  --mount type=bind,src="$PWD/publisher",dst=/publisher,readonly \
  fetchmark-pack-builder:dev verify-run \
  -bundle /publisher/output/commoncrawl-url-en-r1 \
  -public-identity /publisher/publisher-public.json
```

The repository `.dockerignore` excludes local environment files, private-key
patterns, generated pack signatures, projections, and benchmark artifacts from
the Docker build context. A dedicated publisher checkout should still contain
no keys or build output.

Generate a no-overwrite Ed25519 identity once. The private document is created
with mode `0600`; stdout contains only the public identity:

```sh
umask 077
fetchmark-pack-build keygen \
  -publisher-id community-publisher \
  -out /secure/fetchmark/publisher-private.json \
  > /secure/fetchmark/publisher-public.json
```

Back up the private identity through the operator's normal encrypted secret
process. Do not put it in a repository, container layer, bundle, or registry.

## Normalize a URL Index or CDX export

Common Crawl publishes both a line-oriented
[CDXJ Index](https://commoncrawl.org/cdxj-index) and a bulk analytical
[URL Index](https://commoncrawl.org/url-index) stored as Parquet. The offline
normalizer accepts a local flat URL Index Parquet part, the current CDX API's
full JSON-object lines, or raw CDXJ header-plus-JSON lines. It never downloads
an index, queries Athena, or contacts Common Crawl. The operator must download
and pin the exact source artifact separately.

Normalize a local flat URL Index Parquet part with:

```sh
fetchmark-pack-build normalize \
  -format url-index-parquet-flat-v1 \
  -input /publisher/raw/part-00000.parquet \
  -out /publisher/work/commoncrawl-normalized-v1.jsonl
```

This path reads Common Crawl's
[flat Spark schema](https://github.com/commoncrawl/cc-index-table/blob/main/src/main/resources/schema/cc-index-schema-flat.json)
directly. It accepts the current UTC `TIMESTAMP_MILLIS` capture field configured
by Common Crawl's
[conversion script](https://github.com/commoncrawl/cc-index-table/blob/main/src/script/convert_url_index.sh)
and explicitly decodes legacy `INT96` parts; it never asks the
Parquet library to coerce between those representations. It requires the core
WARC fields and their compatible physical/logical types, including Spark's
plain physical `INT32` and signed-32 annotation for WARC offset/length, accepts
unrelated future columns, and permits older files to omit the optional
detected-MIME, charset, and language columns.
`crawl` and `subset` are Hive partition path values and are not required as
physical columns. A null digest or declared MIME cannot produce a valid
normalized candidate, so it fails the staged artifact with its row number
rather than being silently coerced or dropped.

Parquet ingestion is local, streaming, and bounded: the exact input is limited
to 4 GiB, footer metadata to 16 MiB, row/column/row-group counts are checked,
and footer and page-header Thrift declarations are allocation-, depth-, and
token-bounded by an iterative structural scan before the recursive Parquet
decoder sees them. Every projected column page is preflighted with
cancelable reads; overlapping chunk ranges, excessive dictionary cardinality,
and excessive aggregate chunk/page counts fail closed. Page and column-chunk
sizes are capped, page indexes and Bloom filters are skipped, and rows are
decoded in fixed batches and physical row order. The command hashes the
complete input both before and after decoding and rejects a changed file.
Run the separate publisher image with an explicit memory limit as an additional
operator boundary for all third-party binary data.

For an evidence run, select a bounded, host-diverse set directly from the
pinned Parquet part instead of normalizing all of its rows or taking a
physical-order prefix:

```sh
fetchmark-pack-build select-parquet \
  -input /publisher/raw/part-00000.parquet \
  -out /publisher/work/commoncrawl-selected-v1.jsonl \
  -max-records 1000 \
  -languages eng \
  -not-after 2024-06-01T00:00:00Z
```

`select-parquet` scans and hashes the complete source, applies the same pure
capture preflight used by the live evidence collector, and retains the lowest
domain-separated SHA-256 canonical-URL scores. It admits at most one URL per
host. With its default `collector-v1` profile it admits at most 10,000 total
rows, so its output cannot exceed the collector's strict input boundary. The
result is independent of physical row order. Equal
scores are resolved by canonical URL, newest capture, WARC filename, offset,
URL key, declared MIME, detected MIME, status, digest, length, languages, and
encoding, in that order. Languages must be sorted, unique tags and
`-not-after` must be an exact UTC
RFC 3339 second chosen by the operator for the pinned crawl input.

Malformed normalized metadata, ineligible capture metadata, disallowed
queries/MIME/status, captures after the cutoff, noncanonical URLs, and
unselected languages are counted as explicit rejection reasons instead of
aborting the complete scan. The command reports the exact input/output hashes
and bytes plus source, eligible, selected, eligible-but-not-selected, and
per-reason rejection counts. It uses the same private staging and atomic
no-replace activation contract as `normalize`.

For a real-corpus selection experiment spanning several locally pinned parts,
apply one global policy directly to the raw Parquet inputs:

```sh
fetchmark-pack-build select-parquet-parts \
  -input /publisher/raw/part-00000.parquet \
  -input /publisher/raw/part-00001.parquet \
  -input /publisher/raw/part-00002.parquet \
  -out /publisher/work/commoncrawl-selected-prototype-v1.jsonl \
  -profile format-prototype-v1 \
  -max-records 1000000 \
  -languages eng \
  -not-after 2026-06-19T00:00:00Z
```

`select-parquet-parts` accepts 2--1,024 exact local parts. The default
`collector-v1` profile permits at most 50 million aggregate source rows; the
explicit prototype profile permits at most 2 billion. It does not merge already-truncated candidate
files: doing that cannot prove a global top-K unless every part's omitted tail
is also proven. Instead, it schema-checks, fully hashes, scans, and rehashes
each raw part under one global heap and one-per-host policy before emitting any
bytes, then rehashes every part again after staged output and before atomic
activation. Duplicate exact parts fail closed. Part descriptors are sorted by
digest, so reversing input order changes neither the selected bytes nor the
complete report.

The `format-prototype-v1` selection profile is an explicit high-resource
opt-in capped at 5,000,000 selected rows. It only creates body-free normalized
capture candidates. It does not raise the 10,000-row live evidence-collector
limit, the lightweight pack/installer limits, or any ordinary Fetchmark
runtime limit, and its output is not buildable until the separate current
robots, noindex, rights, and exclusion gates have been satisfied. Run it in a
separate resource-limited publisher environment and retain the exact command
result as selection evidence.

The retained
[two-part real-corpus run](../../../benchmarks/common-crawl-url-index-multipart-selection-20260719.md)
processed 13,433,428 rows twice in opposite input orders and produced the same
66,576-row output and SHA-256. Its measured 201.78 source rows per selected
host motivated the prototype-only 2-billion-row scan ceiling; it did not reach
one million selected hosts.

Normalize an immutable API `output=json` export with:

```sh
fetchmark-pack-build normalize \
  -format cdx-api-json-v1 \
  -input /publisher/raw/commoncrawl-cdx.jsonl \
  -out /publisher/work/commoncrawl-normalized-v1.jsonl
```

For raw lines in `urlkey timestamp {json}` form, use
`-format cdxj-header-v1`. The command strictly pins the documented fields,
rejects unknown or duplicate JSON keys, null required values, malformed
timestamps and non-canonical decimal offsets/sizes, and bounds line and field
sizes. Older raw rows may omit `mime-detected`, `languages`, and `encoding`;
those fields remain absent, while an optional `sha1:` digest prefix is
normalized to the canonical 32-character base32 value. It emits deterministic
versioned JSONL and prints input/output byte
counts, record count, and both exact SHA-256 digests as its JSON result.

The output is created with mode `0600` through a private descriptor-anchored
staging file and an atomic no-replace hard-link activation. The hard-link
creation is the commit point. Errors before it remove the staging file; an
abrupt process stop can leave only the private staging name for operator
inspection. If a later cleanup, durability check, input close, or JSON result
write fails, the command reports
`normalized artifact committed but command completion failed` with the final
path and expected SHA-256. Do not blindly retry or remove that path:
hash any existing output, compare it with the reported digest, and retain the
artifact for operator recovery if its state is still uncertain.

Normalized rows deliberately contain no `fetchmark_admission`. Historical
crawl inclusion is not current permission. The still-separate evidence stage
must add current robots, indexing, and URL-metadata rights decisions—and must
resolve any missing metadata required by the pack policy—before the result can
be passed to `build`.

ADR 0009 fixes the separate networked `fetchmark-pack-evidence` boundary,
initially capped at 10,000 rows, with public-only egress, per-host pacing,
bounded retrieval, explicit operator rights evidence, and atomic evidence-
bundle output. The strict policy core
validates a contact-bearing RFC 9309 product token, a one-to-24-hour evidence
window, exactly `url_metadata`, a supported rights basis, canonical evidence
URI, bounded notice, and exact local rights-evidence digest. It also strictly
decodes normalized rows, evaluates exact bounded robots policy bytes including
the path query, and produces deterministic body-free noindex evidence binding
the status, final URL, content type, `X-Robots-Tag`, applicable HTML robots
metadata, representation digest, parser version, and observation window.

## Collect current admission evidence

For a selection above 10,000 rows, first create immutable collector-sized work
units without changing the network collector's safety ceiling:

```sh
fetchmark-pack-evidence partition \
  -input /publisher/work/commoncrawl-selected-prototype-v1.jsonl \
  -out /publisher/work/commoncrawl-admission-partitions-r1
```

The command accepts at most five million rows and eight GiB, strictly decodes
the whole normalized stream, requires canonical URLs and one globally unique
hostname, preserves every source byte and row order, and emits at most 10,000
rows per `shards/part-NNNNNN.jsonl`. `manifest.json` binds the exact source and
each shard by digest, bytes, records, and inclusive global row interval. The
ordered shard bytes reproduce the source exactly. The source is rehashed before
processing, after writing, and immediately before descriptor-anchored atomic
activation; malformed or changing input publishes nothing.

Each shard can then be passed to the unchanged `collect` command below. Run
them serially today. Global selected-host uniqueness prevents direct-origin
duplication, but distinct origins can redirect robots requests to one effective
host; separate processes do not yet share pacing or `Retry-After` state. The
network-free merger described below now verifies complete coverage and binds
aggregate evidence into signed snapshot BuildReport v3, but it does not provide
the durable shared effective-host gate, worker authentication,
leases/resumption, or scheduling inside one fresh evidence window. Do not run
partition collectors concurrently or claim a million-row admitted pack until
those operational gates and a retained real run exist. The current
real-selection partition proof is recorded in the
[admission partition benchmark](../../../benchmarks/open-index-pack-admission-partition-20260719.md).

Build the separate networked collector image with:

```sh
docker build -f deploy/openpack-evidence.Dockerfile \
  --build-arg VERSION=<immutable-source-revision> \
  -t fetchmark-pack-evidence:dev .
```

Start from `deploy/openpack-evidence.example.json` and replace its contact,
rights basis, evidence URI, and notice with an operator-reviewed policy. The
configuration strictly requires a 1--24 hour validity window, a host dispatch
interval from 1 second to 1 hour, global concurrency from 1 to 32, and a
request timeout from 1 to 120 seconds. These limits are digest-bound into the
report. Per-host concurrency remains hard-coded to one. The request timeout is
a network budget applied after the host pacing queue, so even an interval equal
to or longer than that timeout remains a valid policy.

The rights-evidence file is local supporting material for the operator's
assertion. The collector hashes it exactly but never copies its bytes into the
bundle. Common Crawl inclusion, page text, and robots permission are never
treated as a license.

```sh
fetchmark-pack-evidence collect \
  -config /publisher/openpack-evidence.json \
  -rights-evidence /publisher/reviewed-rights-evidence.txt \
  -input /publisher/work/commoncrawl-selected-v1.jsonl \
  -out /publisher/work/commoncrawl-evidence-r1
```

The production constructor always uses Fetchmark's public-only egress policy,
validates DNS again at dial time and on robots redirects, disables automatic
compression, ignores proxy environment variables, and performs no automatic
retries. Every actual robots redirect hop and page request passes through the
same per-host pacing transport. Robots redirects are followed up to the fixed
RFC-aware limit; page redirects are recorded but not followed, avoiding a
request to a path or origin whose robots policy was not evaluated. A
redirecting page is not admitted. Robots `429` responses fail closed, record a
bounded `Retry-After`, extend the effective responding host's cooldown, and
never trigger a page request. Page `429` responses apply the same cooldown
before any later candidate. Embedded URL credentials are rejected before
network access, and the audit stream preserves the exact effective robots URI.

For status-200 HTML, the collector performs bounded charset decoding before
robots metadata parsing. Unsupported charsets, malformed UTF-8, decoded
expansion beyond the representation limit, and HTML parser errors fail closed;
none can be interpreted as evidence that `noindex` was absent.

The collector emits one observation per strict input row in input order while
allowing bounded cross-host concurrency. Only fully robots-allowed, status-200,
HTML, canonical, indexable rows with a valid Common Crawl capture location and
non-future capture enter the builder candidate stream. Capture eligibility is
the same pure preflight used by the downstream selector and is checked again by
the bundle writer:

```text
candidates.jsonl
observations.jsonl
report.json
robots/sha256/<policy-sha256>
```

The report binds the exact config, rights-evidence, normalized input,
candidate, observation, and robots-object digests plus outcome counts. Page
bodies are evaluated ephemerally and never written. The complete bundle is
activated with a descriptor-relative atomic no-replace rename; late malformed
input, cancellation, expiry, or collection failure publishes nothing. A
failure after activation reports `pack evidence bundle committed but operation
completion failed` with the path and report SHA-256 for recovery.

This implementation has deterministic HTTP, bundle, input-partition, and
multi-bundle merge tests, but it has not yet produced the repository's first
reviewed real-corpus evidence run at the prototype scale. Keep the ordinary
server and the network-free signing builder separate from this networked job.

## Verify and merge partition bundles

After every serial collector job finishes, pass the immutable partition
directory and exactly one bundle per partition to the network-free merger.
Bundle arguments may be in any order; matching and final order come from exact
input descriptors and global ranges, not command-line position:

```sh
fetchmark-pack-evidence merge \
  -partitions /publisher/work/commoncrawl-admission-partitions-r1 \
  -bundle /publisher/work/evidence-part-000001-r1 \
  -bundle /publisher/work/evidence-part-000002-r1 \
  -out /publisher/work/commoncrawl-evidence-merged-r1
```

The command strictly rechecks every partition, collector report, observation,
admitted candidate, and robots object twice before atomic activation. Missing,
duplicate, extra, reordered, changed, stale, mixed-policy, or inconsistent
inputs publish nothing. Its deterministic output is:

```text
candidates.jsonl
merge-report.json
```

`merge-report.json` binds the exact partition manifest and ordered collector
report digests, but it does not copy their observations or robots objects.
Retain the partition directory and every exact source bundle for audit replay.
Collector reports are not worker-signed, so use only operator-owned or
explicitly allowlisted workers. This verifier does not make parallel collection
safe.

## Exact build inputs

An evidence-bound `build` uses five immutable files:

1. A strict version-2 JSON build specification that separately names the
   immutable Common Crawl source and the exact builder-candidate digest.
2. The admission-enriched candidate JSONL export whose SHA-256 equals
   `candidate_sha256`.
3. A strict, sorted exclusion JSON document.
4. The verified aggregate `merge-report.json` whose candidate descriptor binds
   item 2.
5. The private publisher identity.

The source digest and candidate digest are deliberately different facts. The
former identifies the exact Common Crawl artifact scanned by the selector; the
latter identifies `candidates.jsonl` emitted by the live evidence bundle. A
specification that repeats one digest in both places fails closed. For example:

```json
{
  "version": 2,
  "profile": "lightweight-v1",
  "pack_id": "commoncrawl-url-eng",
  "revision": 1,
  "created_at": "2026-07-19T08:00:00Z",
  "expires_at": "2026-08-18T08:00:00Z",
  "publisher": {
    "name": "Example publisher",
    "contact_uri": "mailto:operator@example.org",
    "takedown_uri": "https://publisher.example.org/takedown",
    "rights_notice": "URL metadata only; third-party rights remain applicable"
  },
  "languages": ["eng"],
  "max_records": 100,
  "max_records_per_host": 1,
  "max_permission_age_hours": 12,
  "robots_user_agent": "FetchmarkPackEvidence/1.0.0 (+mailto:operator@example.org)",
  "candidate_sha256": "1111111111111111111111111111111111111111111111111111111111111111",
  "inputs": [{
    "name": "CC-MAIN-2026-25 WARC URL Index part 00000",
    "uri": "https://data.commoncrawl.org/cc-index/table/cc-main/warc/crawl=CC-MAIN-2026-25/subset=warc/part-00000-b13edba3-e431-43c6-8915-a9f1c955272b.c000.gz.parquet",
    "retrieved_at": "2026-07-19T06:44:00Z",
    "sha256": "587124b0e999954ed06be5252442993231b6e65ac1ce973e8d36217980fdf25f",
    "rights_notice": "Common Crawl terms; URL metadata only"
  }]
}
```

Replace the illustrative `candidate_sha256` with `report.json`'s
`candidates.sha256` from the completed evidence bundle and set `created_at`
within 15 minutes of the actual build. The network-free builder rehashes the
candidate stream. It signs the distinct source descriptor as a publisher
assertion; the retained selector and collector reports provide the audit chain
from source digest, to normalized selection digest, to candidate digest.

The candidate stream is derived from the Common Crawl URL Index/CDXJ fields
`urlkey`, `timestamp`, `url`, `mime`, `mime-detected`, `status`, `digest`,
`length`, `offset`, `filename`, `languages`, and optional `encoding`. Every line
also requires a `fetchmark_admission` object produced by a separate live
evidence process. Unknown fields, duplicate JSON keys, null values, malformed
lines, input drift, and trailing data fail the entire build.

Admission has three independent decisions:

- `robots`: exact origin `/robots.txt`, the configured publisher user agent,
  an `allowed` outcome, observation window, and body SHA-256.
- `indexing`: final canonical URL, an `indexable` outcome, response-header and
  representation SHA-256 values, parser version, and observation window.
- `rights`: `permitted`, exactly `url_metadata`, one explicit basis, a canonical
  evidence URI and digest, observation and validity window, and a bounded
  rights notice.

Robots, indexing, and rights observations must be valid through the builder's
actual execution time, no older than the specification permits, and never span
more than 24 hours. The signed `created_at` must be within 15 minutes of build
startup, so backdating the specification cannot make expired evidence current.
The earliest admitted expiry is signed into the build report and checked again
after the consumer gate and immediately before activation, with at least one
minute of validity remaining. Observation timestamps later than the actual
execution clock are rejected even when `created_at` is within the permitted
clock-skew window. Rights bases are
limited to `explicit_license`, `publisher_permission`, `public_domain`, and
`url_metadata_policy`. A producer must define, review, and document what its
chosen basis means; the builder only validates the signed evidence shape.

Candidates must be public canonical HTTP(S) URLs with no userinfo or query
string, status 200, HTML as both declared and detected MIME, a valid Common
Crawl capture location, an allowed language, and a capture timestamp no later
than the build. Query-bearing URLs are excluded to avoid redistributing
potential identifiers or private search state. Exact URL/host exclusions,
duplicate URLs, per-host diversity, and pack capacity are applied after
admission. The exclusion document has this shape:

```json
{"version":1,"urls":[],"host_suffixes":[]}
```

Both arrays must be sorted and unique. URL entries must already use Fetchmark's
canonical URL v1 form. A host suffix excludes that host and every subdomain.
The exact exclusion bytes are digest-bound into the signed manifest and build
report; changing the file requires a new build.

## Profiles and output

The strict specification selects one profile:

- `lightweight-v1`: at most 10,000 records. It must produce one shard and pass
  an actual install through the public CPU-only consumer with the 512-MiB
  logical projection ceiling before the report says `ordinary_installable`.
- `format-prototype-v1`: at most 5,000,000 records. It exercises the neutral
  multi-shard format only and is never marked installable by an ordinary node.

The builder emits manifest schema v2 snapshots. Records contain canonical URL,
language, capture time, freshness, URL-level provenance, and a rights notice.
They intentionally omit title, headings, anchor terms, salient sketches,
page-body digests, and the Common Crawl SHA-1 WARC digest. Ordinary consumers
derive bounded search terms locally from the canonical host and path. Manifest
schema v1 remains the frozen enriched-record format and is still readable.

Build a new, non-existing output directory:

```sh
fetchmark-pack-build build \
  -spec /publisher/build-spec.json \
  -candidates /publisher/work/commoncrawl-evidence-merged-r1/candidates.jsonl \
  -exclusions /publisher/exclusions.json \
  -evidence-report /publisher/work/commoncrawl-evidence-merged-r1/merge-report.json \
  -identity /secure/fetchmark/publisher-private.json \
  -out /publisher/output/commoncrawl-url-en-r1
```

With `-evidence-report`, the builder emits signed BuildReport v3 under a distinct
signature domain and carries the exact report as `evidence-report.json`. It
requires the report's candidate digest/count, robots user agent, permission
window, candidate-derived rights identity, and selection input to match the
actual build, then repeats the evidence-window check immediately before atomic
activation. The manifest remains v2;
manifest-only verification does not prove this audit binding. Legacy
BuildReport v2 verification remains available for existing snapshots. The
documented scaled workflow always supplies the aggregate report.

The builder claims a descriptor-anchored lock in the validated output parent,
opens the private staging sibling as an `os.Root`, and performs every shard and
signature write, consumer read, verification, and directory sync through that
descriptor. It then uses a descriptor-relative operating-system no-replace
rename to activate the complete bundle and syncs the parent. It never
overwrites an existing bundle and fails if the named parent path stops referring
to the anchored directory. An abrupt process termination can leave the private
lock and staging sibling for operator inspection and cleanup; ordinary returned
errors remove them without following a replaced path ancestor.

The complete publisher-retained bundle adds three audit files to the ordinary
manifest/shard consumer payload:

```text
manifest.json
manifest.ed25519
evidence-report.json
build-report.json
build-report.ed25519
shards/sha256/<compressed-sha256>.ndjson.zst
```

The manifest signs the portable pack, the exact source input descriptor, and
the exact builder-candidate digest as separate fields. The independently
domain-separated report binds the publisher identity, profile, exact
manifest/candidate/policy/exclusion digests, aggregate selection and rejection
counts, earliest admitted evidence expiry, ordered shard evidence, and the
selected record-stream digest.
Backend-dependent Bleve projection bytes are
not signed because they can vary by environment. The `build` and `verify-run`
command results report their locally measured `projection_bytes`, and an
independent verifier reruns the public consumer gate.

The current TUF `channel-stage` and `channel-fetch` contract transports only
`manifest.json`, `manifest.ed25519`, and the content-addressed shards. It does
not transport `evidence-report.json`, `build-report.json`, or
`build-report.ed25519`. Preserve the complete publisher output separately when
BuildReport-v3 audit replay is required. A channel-fetched directory remains a
manifest-authenticated install payload, not a complete evidence-audit bundle.

Before publishing, verify with a public-only identity on an independent host:

```sh
fetchmark-pack-build verify-run \
  -bundle /publisher/output/commoncrawl-url-en-r1 \
  -public-identity /publisher/publisher-public.json
```

`verify-run` verifies both signatures and all bindings, rehashes and strictly
decodes every shard, recomputes the selected stream digest, and reruns the
ordinary consumer gate for lightweight packs. It never needs the private key.
It also requires exactly one source input, different source and candidate
digests, and one record-provenance entry whose source name matches that input
and whose URI identifies a canonical `data.commoncrawl.org`
`/crawl-data/...warc.gz` artifact.

## Publish a snapshot delta

`build-delta` compares one exact signed snapshot bundle with a newly admitted
target selection. The target spec must keep the same pack ID, profile,
publisher metadata, languages, and signing key; advance the revision by exactly
one; and keep its creation and expiry inside the parent's signed validity
window. The parent must be a snapshot produced by this builder. Delta chains
are not accepted.

Delta BuildReport v1 does not yet carry the aggregate evidence-report digest.
The current delta path revalidates every target candidate's admission but must
not be described as aggregate-evidence-bound. Adding a versioned delta report
binding is a separate persistent-format decision.

```sh
fetchmark-pack-build build-delta \
  -parent-bundle /publisher/output/commoncrawl-url-en-r1 \
  -spec /publisher/build-spec-r2.json \
  -candidates /publisher/candidates-r2.jsonl \
  -exclusions /publisher/exclusions-r2.json \
  -identity /secure/fetchmark/publisher-private.json \
  -out /publisher/output/commoncrawl-url-en-r2-delta
```

The builder independently verifies the parent before comparing it. Parent and
target records are stored in an exclusively opened, descriptor-anchored bbolt
file inside the private staging bundle, so comparison remains disk-backed at
the format-prototype scale. A lexicographic merge emits one operation per URL:
an upsert for a new or changed target record and a metadata-free tombstone for
a URL absent from the target. An identical target is rejected instead of
publishing a meaningless revision. The private comparison database is closed
and removed before the bundle is signed and activated.

The signed delta manifest pins the exact parent manifest digest and operation
count. Its separately domain-separated audit report binds the newly admitted
target selection, upsert/tombstone counts, final projection record count,
ordered shard evidence, and operation-stream digest. A `lightweight-v1` delta
must also materialize successfully through the public snapshot-plus-delta
consumer before `ordinary_installable` is true. The returned
`operation_count` and `projection_record_count` are intentionally distinct.

Verify the portable parent and delta using only the public identity on an
independent host:

```sh
fetchmark-pack-build verify-delta-run \
  -parent-bundle /publisher/output/commoncrawl-url-en-r1 \
  -bundle /publisher/output/commoncrawl-url-en-r2-delta \
  -public-identity /publisher/publisher-public.json
```

`verify-delta-run` reauthenticates both bundles, all shards, operation
uniqueness and membership, report bindings, the operation-stream digest, and
the computed final record count. For a lightweight delta it also rebuilds the
actual CPU-only projection from the portable parent and delta. Neither command
updates an operator registry, publishes files, or deletes an older bundle.
The parent and output directory trees must be disjoint even when two path
spellings resolve to the same directory. If bundle activation succeeds but a
later input close, directory sync, path recheck, or descriptor cleanup fails,
the command returns a committed-artifact error containing the canonical output
path plus parent-manifest, manifest, and report digests. Do not retry or remove
that output blindly; run the shown `verify-delta-run` command first.

## Offline TUF repository staging

Generate a dedicated offline channel identity. It is distinct from the pack
publisher identity:

```sh
fetchmark-pack-build channel-keygen \
  -repository-id fetchmark-community \
  -root-expires 2027-07-01T00:00:00Z \
  -out /publisher/keys/fetchmark-channel.bootstrap.json
```

The command atomically writes only the private mode-0600 identity and reports
the exact bootstrap-root digest without printing private material. The exact
signed bootstrap root is stored in that identity so later publisher versions
do not regenerate or reserialize the trust anchor. The initial immutable
repository generation includes those preserved public `root.json` bytes:

```sh
fetchmark-pack-build channel-stage \
  -identity /publisher/keys/fetchmark-channel.bootstrap.json \
  -bundle /publisher/output/developer-en-r3 \
  -public-identity /publisher/publisher-public.json \
  -target packs/developer/manifest.json \
  -metadata-version 1 \
  -timestamp-expires 2026-07-20T00:00:00Z \
  -snapshot-expires 2026-07-21T00:00:00Z \
  -targets-expires 2026-08-18T00:00:00Z \
  -out /publisher/channel-generations/generation-1
```

For a delta, add its exact `-parent-bundle`; the command repeats the public
snapshot-plus-delta materialization proof before signing channel metadata.
The bootstrap repository identity uses distinct role keys and a two-of-two
root threshold. Before routine publication, migrate those two bootstrap keys
to independently held custodians. Generate each new custodian key on its own
offline system. Key generation writes only the private mode-0600 file. Public
export is deterministic and independently recoverable even if keygen's result
stream is lost:

```sh
fetchmark-pack-build channel-root-keygen \
  -repository-id fetchmark-community \
  -signer-id custodian-a \
  -out /custodian-a/root.private.json

fetchmark-pack-build channel-root-public \
  -signer /custodian-a/root.private.json \
  -out /transfer/custodian-a.public.json
```

Repeat for custodian B. The coordinator prepares one deterministic unsigned
successor from the exact current root and the two public documents:

```sh
fetchmark-pack-build channel-root-prepare \
  -repository-id fetchmark-community \
  -current-root /publisher/channel-generations/generation-1/root.json \
  -new-signer /transfer/custodian-a.public.json \
  -new-signer /transfer/custodian-b.public.json \
  -root-expires 2028-07-19T00:00:00Z \
  -out /ceremony/2.root.unsigned.json
```

Give every custodian the exact current root, unsigned candidate, repository ID,
and the two digests reported by preparation. Each new custodian signs
independently; it refuses a candidate that changes online keys or does not
authorize that custodian:

```sh
fetchmark-pack-build channel-root-sign \
  -repository-id fetchmark-community \
  -current-root /publisher/channel-generations/generation-1/root.json \
  -candidate /ceremony/2.root.unsigned.json \
  -expected-current-root-sha256 CURRENT_ROOT_SHA256 \
  -expected-candidate-root-sha256 CANDIDATE_ROOT_SHA256 \
  -signer /custodian-a/root.private.json \
  -out /ceremony/custodian-a.contribution.json
```

For the first migration only, obtain the old threshold from the original
version-1 identity as one explicit two-contribution bundle:

```sh
fetchmark-pack-build channel-root-sign-legacy \
  -identity /publisher/keys/fetchmark-channel.bootstrap.json \
  -current-root /publisher/channel-generations/generation-1/root.json \
  -candidate /ceremony/2.root.unsigned.json \
  -expected-current-root-sha256 CURRENT_ROOT_SHA256 \
  -expected-candidate-root-sha256 CANDIDATE_ROOT_SHA256 \
  -out /ceremony/legacy.contributions.json
```

After both new contributions arrive, assemble both thresholds and create a
new operational identity without root private keys:

```sh
fetchmark-pack-build channel-root-assemble \
  -repository-id fetchmark-community \
  -current-root /publisher/channel-generations/generation-1/root.json \
  -candidate /ceremony/2.root.unsigned.json \
  -legacy-contributions /ceremony/legacy.contributions.json \
  -contribution /ceremony/custodian-a.contribution.json \
  -contribution /ceremony/custodian-b.contribution.json \
  -expected-current-root-sha256 CURRENT_ROOT_SHA256 \
  -expected-candidate-root-sha256 CANDIDATE_ROOT_SHA256 \
  -out /ceremony/2.root.json

fetchmark-pack-build channel-root-apply \
  -identity /publisher/keys/fetchmark-channel.bootstrap.json \
  -signed-root /ceremony/2.root.json \
  -expected-current-root-sha256 CURRENT_ROOT_SHA256 \
  -expected-signed-root-sha256 SIGNED_ROOT_SHA256 \
  -out /publisher/keys/fetchmark-channel.operational.json
```

Use the operational identity for later `channel-stage` commands. It retains
the exact public root chain and the three online metadata keys, but no root
private key. A later rotation uses four individual `-contribution` flags at
assembly instead of `-legacy-contributions`. Every ceremony output is
no-overwrite. Do not retire the legacy identity until the rotated repository
and mirror have been verified from the original bootstrap. See
[ADR 0018](../../../adr/0018-split-custody-tuf-root-rotation.md).

For a later v2-to-v3 rotation, generate and export new custodians C and D as
above, then use the four individual signers—current A/B and successor C/D:

```sh
fetchmark-pack-build channel-root-prepare \
  -repository-id fetchmark-community \
  -current-root /ceremony/2.root.json \
  -new-signer /transfer/custodian-c.public.json \
  -new-signer /transfer/custodian-d.public.json \
  -root-expires 2030-07-19T00:00:00Z \
  -out /ceremony/3.root.unsigned.json

fetchmark-pack-build channel-root-sign \
  -repository-id fetchmark-community -current-root /ceremony/2.root.json \
  -candidate /ceremony/3.root.unsigned.json \
  -expected-current-root-sha256 ROOT_V2_SHA256 \
  -expected-candidate-root-sha256 CANDIDATE_V3_SHA256 \
  -signer /custodian-a/root.private.json \
  -out /ceremony/a-v3.contribution.json
fetchmark-pack-build channel-root-sign \
  -repository-id fetchmark-community -current-root /ceremony/2.root.json \
  -candidate /ceremony/3.root.unsigned.json \
  -expected-current-root-sha256 ROOT_V2_SHA256 \
  -expected-candidate-root-sha256 CANDIDATE_V3_SHA256 \
  -signer /custodian-b/root.private.json \
  -out /ceremony/b-v3.contribution.json
fetchmark-pack-build channel-root-sign \
  -repository-id fetchmark-community -current-root /ceremony/2.root.json \
  -candidate /ceremony/3.root.unsigned.json \
  -expected-current-root-sha256 ROOT_V2_SHA256 \
  -expected-candidate-root-sha256 CANDIDATE_V3_SHA256 \
  -signer /custodian-c/root.private.json \
  -out /ceremony/c-v3.contribution.json
fetchmark-pack-build channel-root-sign \
  -repository-id fetchmark-community -current-root /ceremony/2.root.json \
  -candidate /ceremony/3.root.unsigned.json \
  -expected-current-root-sha256 ROOT_V2_SHA256 \
  -expected-candidate-root-sha256 CANDIDATE_V3_SHA256 \
  -signer /custodian-d/root.private.json \
  -out /ceremony/d-v3.contribution.json

fetchmark-pack-build channel-root-assemble \
  -repository-id fetchmark-community \
  -current-root /ceremony/2.root.json \
  -candidate /ceremony/3.root.unsigned.json \
  -contribution /ceremony/a-v3.contribution.json \
  -contribution /ceremony/b-v3.contribution.json \
  -contribution /ceremony/c-v3.contribution.json \
  -contribution /ceremony/d-v3.contribution.json \
  -expected-current-root-sha256 ROOT_V2_SHA256 \
  -expected-candidate-root-sha256 CANDIDATE_V3_SHA256 \
  -out /ceremony/3.root.json

fetchmark-pack-build channel-root-apply \
  -identity /publisher/keys/fetchmark-channel.operational.json \
  -signed-root /ceremony/3.root.json \
  -expected-current-root-sha256 ROOT_V2_SHA256 \
  -expected-signed-root-sha256 ROOT_V3_SHA256 \
  -out /publisher/keys/fetchmark-channel.operational-v3.json
```

A successor must name the previous immutable generation and advance the
synchronized metadata version by exactly one:

```sh
  -previous /publisher/channel-generations/generation-1 \
  -metadata-version 2
```

The previous root-chain prefix and all metadata signatures and hashes are
reverified. Each generation writes the original `root.json` plus every exact
numbered root update and signs online metadata under the active root.
Fetchmark rejects a successor that merely wraps an identical or older pack in
newer TUF metadata. This guarantee is relative to the `-previous` directory
you selected: the offline tool cannot know the live mirror head, and it permits
separate branches from one generation. Always name the latest published
generation and never publish branches out of order. A future remote cutover
must compare-and-swap the exact expected previous `timestamp.json` digest;
without that check, publishing a newer metadata version from a stale branch can
roll the advertised pack revision back. To publish authenticated target absence:

```sh
fetchmark-pack-build channel-stage \
  -identity /publisher/keys/fetchmark-channel.operational.json \
  -previous /publisher/channel-generations/generation-1 \
  -withdraw \
  -target packs/developer/manifest.json \
  -metadata-version 2 \
  -timestamp-expires 2026-07-20T00:00:00Z \
  -snapshot-expires 2026-07-21T00:00:00Z \
  -targets-expires 2026-08-18T00:00:00Z \
  -out /publisher/channel-generations/generation-2-withdrawn
```

Withdrawal is terminal for this single-target repository identity. Later
generations may renew the signed absence, but cannot publish a target because
the immediately previous metadata no longer proves the last pack rollback
floor. Reintroduction requires a future history-bearing profile or a separately
bootstrapped repository and operator trust decision.

Each stage is a private, fsynced, atomic no-overwrite directory. Distribute
`root.json` to operators through an authenticated out-of-band path. When
copying a generation to a self-hosted HTTPS mirror, make immutable hashed
targets and versioned metadata durable first and switch
`metadata/timestamp.json` last. The command itself has no network or remote-
mirror credentials. If it reports a committed-stage error, verify the reported
path and digests before retrying or removing it. See
[ADR 0015](../../../adr/0015-offline-tuf-repository-staging.md).
Timestamp expiry must remain at least fifteen minutes in the future both when
staging begins and at the final activation check.

Activate an initial local mirror only when its timestamp head is absent:

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

For later generations, replace `-initialize-empty` with both exact current and
candidate timestamp SHA-256 flags. The command verifies the pinned root,
complete bounded live and candidate root chains, pack transition, publisher signature,
manifest, and shards. It copies immutable objects first and atomically replaces
`metadata/timestamp.json` last under a persistent advisory lock. A prepared
error leaves the head unchanged; a committed error means the timestamp rename
won and must be inspected before retrying. The command has no network path,
accepts no TUF private key, deletes nothing, and does not change the Fetchmark
registry. Numbered root updates may become visible while immutable dependencies
are prepared, but they have both old and new thresholds and preserve every
online role key; timestamp remains the pack-content head. See
[ADR 0017](../../../adr/0017-atomic-tuf-mirror-head-activation.md).
An exact retry reuses already prepared immutable files. A different sibling at
the same next metadata version fails closed because its versioned metadata
names are already occupied; after proving the live timestamp never advanced,
an operator must explicitly remove those abandoned unauthenticated files or
use a clean mirror. The command never deletes them automatically.

The mirror copy of both the manifest and its detached signature is named with
the exact manifest SHA-256. Portable bundles continue to use
`manifest.ed25519`, while repositories use
`<manifest-sha256>.manifest.ed25519`. A mutable mirror can therefore retain old
and new objects through timestamp cutover without a manifest/signature race.

## Not implemented yet

- Remote Common Crawl partition discovery and download remain operator-owned;
  the deterministic bounded selectors accept one or several exact local
  flat-Parquet parts.
- A reviewed real-corpus evidence run and repeat-run comparison using the
  separate live collector.
- Common Crawl Web Graph quality selection or WARC range retrieval.
- A retained real 1--5 million-row multi-part selection, followed by a
  rights/robots/noindex-safe admission and repeat-build evaluation. The new
  selection profile alone does not satisfy that gate.
- Opt-out update service, remote/object-store mirror upload, or
  BitTorrent/OCI publication. Root-only rotation and split custody, local mirror-head CAS, and
  explicit registry activation/rollback are implemented. The
  publisher now creates complete immutable TUF repository generations, and the
  consumer can verify operator-supplied TUF metadata and
  select, retrieve, or withdraw one exact manifest. It can generate an
  independently materialized, no-overwrite successor registry under the
  already pinned publisher key. Metadata transport and remote upload remain
  publisher/operator work. Signed snapshot-
  parent deltas are publisher-generated and independently verifiable;
  activation and rollback remain explicit operator actions described in the
  [operator guide](open-index-packs.md).

Those are the next supply-side milestones. The synthetic million-record result
only proves the current consumer's bounded format path.
