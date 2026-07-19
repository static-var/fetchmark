# Open-index-pack admission partition proof (2026-07-19)

## Claim under test

The offline admission partitioner must turn a real host-diverse Common Crawl
selection into unchanged 10,000-row collector inputs while preserving exact
bytes, order, coverage, and deterministic output. This is not a live
robots/noindex run and does not prove that the partitions can yet be merged
into one signed pack.

## Input

The input is the retained two-part `CC-MAIN-2026-25` selection described in
[the multi-part selection benchmark](common-crawl-url-index-multipart-selection-20260719.md):

- records: 66,576
- bytes: 30,515,106
- SHA-256: `6fb2f0be14c5de460af8c5206f9d8be8fc92f461a22a79a803efbe7b075a4e31`
- selection invariant: one canonical URL per hostname

The reviewed `fetchmark-pack-evidence` proof binary was 16,315,874 bytes with
SHA-256 `c8c004d30c2311f64ae448a2e1fa3de91822734350c0d3d86e3d71d7dd6feb09`.

It was built and run on `darwin/arm64` with Go 1.26.5:

```sh
env GOCACHE=/private/tmp/fetchmark-go-cache \
  go build \
  -o /private/tmp/fetchmark-pack-evidence-partition-current-v2 \
  ./cmd/fetchmark-pack-evidence
```

`go version -m` binds the executable to base revision
`18cdf18a3ecfdb0a6ea276a1ac4f72b373349ef4` with `vcs.modified=true`. The
relevant source/module digests at build time were:

- `cmd/fetchmark-pack-evidence/main.go`: `04dda37d405df86abd03d727973e10d5f14f47c139211e7715806b94d2750a3a`
- `internal/adapters/packevidence/partition.go`: `4fcb6f1e3eb5624a137c0dbc7da7da67bc6eb226fa04b251c8c0d8b2b73d111c`
- `internal/adapters/packevidence/collector.go`: `abfa6dd2c6feeeb4fd1af92b20c44fcaeab2fc4e350cb8c5e2ab3b7566bf6dad`
- `go.mod`: `a81709e6a68e5b0cd84e6608de9c867ef93594af78c77820aa6482840ce1a6f5`
- `go.sum`: `62f0c9b50fac6c5c6a6e5eb52bfc0a6c483bb16ef2aa615a8c78b6b94a3ef6a2`

The platform-specific 16-MiB proof executable is intentionally not retained in
the repository; the exact temporary path, size, digest, build metadata, source
digests, and outputs are retained here. Repository-root binaries are not
evidence for this benchmark.

## Command

```sh
fetchmark-pack-evidence partition \
  -input /publisher/commoncrawl-selected.jsonl \
  -out /publisher/admission-partitions-r1
```

The command was run twice into distinct new directories. Both runs emitted
seven partitions and the same manifest SHA-256:

`0df59d466bc21ea723b979beb6b852d53ed684f68fc90fd8a17ab9606c3f4960`

The first six partitions contain 10,000 rows each. The final partition covers
global rows 60,001--66,576 and contains 6,576 rows. All manifest and shard files
were byte-identical across the two runs.

Concatenating shard files in manifest order produced SHA-256
`6fb2f0be14c5de460af8c5206f9d8be8fc92f461a22a79a803efbe7b075a4e31`,
exactly matching the source. The manifest's partition row sum is 66,576 and its
inclusive global row intervals are contiguous from 1 through 66,576.

Machine-readable retained evidence is in
[`artifacts/open-index-pack-admission-partition-20260719.jsonl`](artifacts/open-index-pack-admission-partition-20260719.jsonl).

## Conclusion and remaining gate

Deterministic bounded work-unit creation is proven on the current real
selection. The live collector cap remains 10,000. Before a real million-row
pack can be claimed, Fetchmark still needs a network-free verifier/merger,
aggregate evidence binding in the signed build, durable scheduling and worker
identity, completion inside the evidence window, a retained current admission
run, and repeat build/consumer evidence.
