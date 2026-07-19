# Phase 4 persistent local-corpus scale

Date: 2026-07-19

This benchmark measures the optional persistent Bleve local corpus at 10,000,
50,000, and 100,000 documents. It exists to preserve an evidence-based
CPU-only profile; it does not raise the public 100,000-document or 1-GiB
logical-content limits.

## Boundary

The opt-in test exercises the production lifecycle directly:

1. create a schema-v3 persistent index;
2. generate a deterministic permitted document and call `Reconcile` once for
   each record, as the request and focused-ingestion paths do;
3. verify the live document count, close the index, and measure logical and
   allocated filesystem bytes;
4. reopen the closed index with the same public limits and verify its count;
5. warm 24 deterministic queries once, then measure 96 `SearchBatch` calls.

No benchmark-only batch loader is used. Index time therefore includes fixture
generation, canonicalization, admission, retained-record inspection, expiry
checking, and the Bleve write for every document. The generated corpus spans
2,048 `.example` domains, eight languages, eight verticals, and sixteen
topics. Every document is explicitly permitted and classified safe. No
network request is made, and the fixture is not evidence of real-web quality.

`index_logical_bytes` sums regular-file lengths after the first close.
`index_disk_bytes` sums the filesystem's reported 512-byte allocated blocks.
The cgroup peak includes the process and its filesystem cache. Search
percentiles use nearest-rank over the 96 post-warmup observations.

## Reproduction

The runner cross-compiles the real package test, puts only that binary in a
scratch image, and runs it with no network, a read-only root filesystem, no
Linux capabilities, no privilege escalation, a 128-process ceiling, and an
anonymous writable volume for the index.

The recorded runs used Docker 29.4.0 on Linux/arm64, Go 1.26.5,
`GOMAXPROCS=2`, and benchmark image
`sha256:f9083a3f63ab502978e6c3ee59a7e69c0d405bf59b8c6d8daa99d8489f620135`.

```sh
FM_LOCALCORPUS_SCALE_RECORDS=10000 tools/localcorpus-scale.sh
FM_LOCALCORPUS_SCALE_RECORDS=50000 tools/localcorpus-scale.sh
FM_LOCALCORPUS_SCALE_RECORDS=100000 tools/localcorpus-scale.sh
```

The default profiles use two CPUs and cgroup ceilings of 512 MiB, 768 MiB,
and 1 GiB respectively. `FM_LOCALCORPUS_SCALE_MEMORY` and
`FM_LOCALCORPUS_SCALE_CPUS` may override those explicit resource controls.
The Go test rejects every document count other than the three reviewed
profiles. Ordinary `go test ./...` runs skip the scale test.

Exact machine-readable observations are retained in
[`artifacts/local-corpus-scale-20260719.jsonl`](artifacts/local-corpus-scale-20260719.jsonl).

## Results

| Documents | Index time | Documents/s | Logical index | Allocated index | Reopen | Search p50 | Search p95 | Cgroup peak / limit |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 71.789 s | 139.30 | 40.17 MB | 39.30 MB | 68.87 ms | 0.387 ms | 0.767 ms | 106.65 / 536.87 MB |
| 50,000 | 301.601 s | 165.78 | 181.79 MB | 180.94 MB | 310.11 ms | 1.410 ms | 2.142 ms | 355.93 / 805.31 MB |
| 100,000 | 584.701 s | 171.03 | 358.73 MB | 357.78 MB | 850.71 ms | 2.689 ms | 4.271 ms | 593.47 / 1,073.74 MB |

Decimal megabytes are used in the table; the JSONL artifact preserves exact
byte counts and unrounded timings.

## Interpretation

The 10,000-document profile remains comfortably lightweight: the complete
process and page-cache peak is about 107 MB, the closed index occupies about
40 MB, reopen is below 70 ms, and warm lexical search remains below 1 ms at
p95. At 50,000 documents, storage and reopen time remain approximately linear;
warm search stays near 2 ms at p95. The observed 356-MB peak fits the reviewed
768-MiB profile with substantial headroom.

At the public 100,000-document cap, the closed index remains below 359 MB,
reopen remains below one second, and warm lexical search remains below 5 ms at
p95. The 593-MB process-and-cache peak fits the reviewed 1-GiB profile with
about 480 MB of headroom. The configured 1-GiB `MaxBytes` is a logical admitted
document budget, not a physical filesystem quota; this fixture consumed about
75 MB of that logical budget while Bleve's on-disk projection occupied about
359 MB.

Individual reconciliation sustains roughly 140--166 documents per second in
these runs. That is sufficient for the intended search-driven flywheel and a
polite focused crawler, but it is not a bulk web-index construction path.
Downloaded open packs intentionally retain their separate streaming/batched
projection builder and scale evidence.
