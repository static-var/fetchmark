# Varied open-index-pack container scale gate

Date: 2026-07-19

This is a deterministic CPU-only scale probe for the signed open-index-pack
consumer. It measures a varied 100,000-record artifact under a real Linux cgroup
memory limit without raising Fetchmark's public 10,000-record import cap. The
subsequent bounded [one-million-record prototype](open-index-pack-million-prototype.md)
is recorded separately; neither result claims commercial-index parity.

## Reproducible harness

The opt-in `TestOpenPackScaleProfile` streams a deterministic fixture to disk,
signs its strict manifest, runs the normal shard verification, projection,
closed-index reopen, marker and document-count verification, activation, cold
open, and lexical search paths, then emits one schema-versioned JSON result. The
result includes the exact manifest, compressed-shard, and uncompressed-stream
SHA-256 digests.
Production `Install` and `Open` retain `MaxInstallRecords`; only the same-package
test can supply the larger unexported policy.

The fixture contains:

- 100,000 unique canonical URLs across 8,192 synthetic `.example` domains;
- eight sorted languages and eight source verticals;
- deterministic variation in titles, headings, anchor terms, salient sketches,
  paths, timestamps, scores, content digests, and provenance;
- one signed NDJSON/Zstandard snapshot shard with no page bodies.

`tools/openpack-scale.sh` cross-compiles the test binary for the Docker engine's
Linux architecture, builds a scratch image without network access, and runs it
with no network, a read-only root, no capabilities, no-new-privileges, a
128-process limit, an anonymous work volume, fixed CPUs, and equal memory/swap
limits. It passes the requested memory limit into the test as an exact byte
expectation; the test fails unless cgroup v2 `memory.max` matches and
`memory.peak` is nonzero and within that limit. From the repository root:

```sh
FM_OPENPACK_SCALE_RECORDS=100000 \
FM_OPENPACK_SCALE_MEMORY=512m \
FM_OPENPACK_SCALE_CPUS=2 \
make openpack-scale
```

The current harness intentionally accepts only 10,000 through 100,000 records;
the unchanged compressed, uncompressed, and projection limits make larger
values a different prototype rather than a meaningful success setting.

The local environment used OrbStack's Linux `arm64` Docker runtime, Go 1.26.5,
and two container CPUs. The test used cgroup v2 `memory.max` and `memory.peak`
as its authoritative limit and peak-memory evidence.

## Three-run 100k result

Every run completed all 96 searches with non-empty, correctly attributed
results and reopened the closed projection successfully.
The exact four emitted JSON lines are retained in
[`artifacts/open-index-pack-varied-scale-20260719.jsonl`](artifacts/open-index-pack-varied-scale-20260719.jsonl);
the first three rows are the 100k runs and the fourth is the 10k control.

| Metric | Run 1 | Run 2 | Run 3 | Median |
| --- | ---: | ---: | ---: | ---: |
| Memory limit | 512 MiB | 512 MiB | 512 MiB | 512 MiB |
| Cgroup peak memory | 403.02 MiB | 349.11 MiB | 405.36 MiB | 403.02 MiB |
| Install | 12.476 s | 12.821 s | 12.847 s | 12.821 s |
| Install throughput | 8,016/s | 7,800/s | 7,784/s | 7,800/s |
| Cumulative install allocation | 12.900 GB | 12.822 GB | 12.885 GB | 12.885 GB |
| Projection logical size | 206,511,621 B | 206,511,672 B | 206,511,544 B | 206,511,621 B |
| Projection allocated size | 206,508,032 B | 206,516,224 B | 206,516,224 B | 206,516,224 B |
| Cold open | 3.641 ms | 0.770 ms | 2.049 ms | 2.049 ms |
| Search p50 | 0.730 ms | 0.651 ms | 0.655 ms | 0.655 ms |
| Search p95 | 2.517 ms | 2.826 ms | 1.906 ms | 2.517 ms |

The signed input is 11,045,844 compressed bytes and 86,571,902 uncompressed
bytes. Small projection-size differences are Bleve implementation output, not
input variation; the signed fixture bytes are identical in every run.

The 512-MiB gate passed three times; the maximum observed peak left about
106.6 MiB of cgroup headroom. A future operator-facing 100k profile should
therefore budget at least 640 MiB rather than treating 512 MiB as a guaranteed
minimum. The public installer remains capped at 10,000 records.

## Lightweight 10k control

The same varied harness also completed once with 10,000 records, two CPUs, and
a 192-MiB cgroup limit:

| Metric | Result |
| --- | ---: |
| Cgroup peak / limit | 131.30 MiB / 192 MiB |
| Install | 1.385 s; 7,218 records/s |
| Projection logical size | 23,727,153 B |
| Cold open | 0.971 ms |
| Search p50 / p95 | 0.130 ms / 0.403 ms |

For operational headroom, 256 MiB remains the sensible lightweight container
budget. Operators can lower `-max-projection-bytes` when they want the install
to reject packs outside a tighter disk profile.

## Decision and next gate

This closes the prior varied-100k/low-memory evidence gap and provides explicit
10k and test-only 100k resource profiles. It does not justify silently raising
the ordinary cap: peak memory varied materially, the corpus is synthetic, and
the probe does not model pack creation from Common Crawl inputs or incremental
updates.

The next Phase 6 gate was a separate bounded one-million-record prototype with
deliberately larger test-only shard/projection policies, streaming fixture
generation, and the same required cgroup evidence; it has now passed. Common
Crawl selection/build tooling, signed deltas, and an explicit
distribution/update channel remain. Any public cap change must remain opt-in
and preserve the 10k/256-MiB lightweight profile.
