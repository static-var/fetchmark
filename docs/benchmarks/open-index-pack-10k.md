# Open index pack 10k consumer probe

Date: 2026-07-18

This is a deterministic, local performance probe of the initial 10,000-record open-index-pack consumer boundary. It is not a claim of 1 million-document readiness or commercial-index parity.

## Fixture and method

- 10,000 synthetic discovery-metadata records across 250 canonical domains and `en`/`fr` languages.
- One signed snapshot manifest and one NDJSON zstd shard. The Ed25519 key comes from a fixed seed and zstd uses concurrency 1.
- Manifest validity is fixed at 2026-07-01 through 2027-06-30 (less than the 365-day format maximum); acceptance time is fixed at 2026-07-18 UTC.
- Record generation, signing, compression, fixture writes, and setup installs are outside timed sections.
- Install uses a fresh private root for each iteration. Cold open creates a new Bleve handle and closes it. Search runs six lexical queries per iteration covering title, headings, anchor terms, salient sketch, include-domain filtering, and language filtering; every response must be non-empty and retain the expected provider/source identity.
- The installed logical size is the sum of regular-file lengths. Disk size uses filesystem block counts (`st_blocks * 512`) when the platform exposes them.
- No network access is used. The benchmark is excluded from ordinary unit-test execution unless `-bench` selects it.

## Environment

- Go: `go version go1.26.5 darwin/arm64`
- OS: macOS 27.0 (build 26A5378n)
- Architecture: arm64
- CPU: Apple M4 Pro

## Baseline exact one-shot run

From the repository root:

```sh
rtk proxy env GOCACHE=/private/tmp/fetchmark-go-build-cache go test ./internal/adapters/openpackindex -run '^$' -bench 'BenchmarkOpenPack(Install|ColdOpen|Search)10K$' -benchtime=1x -count=1 -benchmem
```

Raw output:

```text
goos: darwin
goarch: arm64
pkg: github.com/staticvar/fetchmark/internal/adapters/openpackindex
cpu: Apple M4 Pro
BenchmarkOpenPackInstall10K-12     	       1	2321957250 ns/op	    520650 compressed_B/op	  17260544 installed_disk_B/op	  17241154 installed_logical_B/op	      4307 records/s	   7020000 uncompressed_B/op	1525836304 B/op	23982380 allocs/op
BenchmarkOpenPackColdOpen10K-12   	       1	  14119375 ns/op	 2571168 B/op	    9518 allocs/op
BenchmarkOpenPackSearch10K-12     	       1	   4873167 ns/op	      1231 queries/s	  435720 B/op	    5105 allocs/op
PASS
ok  	github.com/staticvar/fetchmark/internal/adapters/openpackindex	7.504s
```

## Result

| Operation | One-shot result | Allocation result |
| --- | ---: | ---: |
| Install 10,000 records | 2.322 s; 4,307 records/s | 1,525,836,304 B; 23,982,380 allocations |
| Reopen and close projection | 14.119 ms | 2,571,168 B; 9,518 allocations |
| Six-query search suite | 4.873 ms total; 1,231 queries/s; about 0.812 ms/query | 435,720 B; 5,105 allocations |

The input shard is 520,650 bytes (0.497 MiB) compressed and 7,020,000 bytes (6.695 MiB) uncompressed. The installed immutable object is 17,241,154 logical bytes (16.442 MiB) and occupies 17,260,544 filesystem bytes (16.461 MiB) in this run.

This clean run verifies that the consumer can authenticate, bound, decode, project, reopen, and query the complete initial 10k record cap on a CPU-only local installation. The high cumulative install allocation count and bytes warrant profiling before increasing the cap.

## Profiling and optimized projection v1

A CPU/allocation profile attributed about 46% of install CPU to Scorch merge
work, 22% to batch construction, 13% to analysis, and 7% to the strict two-pass
JSON boundary. A compiled-binary batch sweep then measured three exact installs
per variant:

| Batch | Doc values | Median time | Records/s | B/op | Allocations/op | Max RSS |
| ---: | :---: | ---: | ---: | ---: | ---: | ---: |
| 64 | on | 4.701 s | 2,127 | 2,135,951,512 | 32,252,735 | not measured |
| 128 | on | 2.337 s | 4,279 | 1,521,289,616 | 23,846,441 | 101,924,864 B |
| 256 | on | 1.494 s | 6,693 | 1,225,782,712 | 19,636,459 | not measured |
| 512 | on | 1.038 s | 9,632 | 926,324,672 | 14,870,710 | 103,350,272 B |
| 512 | off | 1.022 s | 9,780 | 853,339,656 | 14,593,620 | 98,140,160 B |
| 1024 | on | 0.819 s | 12,209 | 795,705,704 | 12,562,213 | 128,811,008 B |
| 2048 | on | 0.905 s | 11,046 | 680,139,376 | 10,814,260 | 191,528,960 B |
| 4096 | on | 0.710 s | 14,087 | 670,400,080 | 10,679,268 | 276,889,600 B |

Fetchmark therefore uses batches of 512 for projection schema v1 and disables
doc values on fields that are never sorted or faceted. Although 1024 is the raw
throughput/RSS knee, its measured RSS is about 25% higher than 512; 2048 and
4096 require disproportionately more live memory for modest or unstable gains.
The conservative 512 default targets the dominant merge churn without making
the live batch an unnecessarily large fraction of a lightweight install.
Disabling doc values reduced installed size and allocation while preserving
lexical, exact-phrase, domain, language, date, stored-field, reopen, and search
behavior. This storage-only optimization is read-compatible with projections
built by the earlier v1 implementation, so their marker remains valid and an
identical signed pack does not require destructive reinstallation.

A clean three-sample run of the selected configuration produced a median
10,000-record install of 1.006 seconds (9,945 records/s), 848,459,536 allocated
bytes, and 14,571,856 allocations. The immutable projection was about 15.36 MB
logical in those runs. Relative to the 128/doc-values baseline on the same
machine, that is roughly 57% less elapsed time, 44% less cumulative allocation,
and 39% fewer allocations.

Ten-iteration cold-open and 100-iteration search checks on the optimized
projection gave:

| Operation | Median result | Median allocation result |
| --- | ---: | ---: |
| Reopen and close projection | 11.190 ms | 2,495,996 B; 8,005 allocations |
| Six-query search suite | 4.377 ms total; 1,371 queries/s; about 0.730 ms/query | 637,566 B; 10,049 allocations |

The optimized result is retained; the 10,000-record public/import cap is not
raised by this benchmark.

## Isolated 100k scale experiment

An isolated copy of the benchmark harness raised only its test fixture and
internal acceptance limit to 100,000 records. Repository code and the public
10,000-record cap were unchanged. It used the selected 512/no-doc-values shape,
the normal signed-manifest verification and install path, and a 1 GiB stop
threshold.

| Metric | 100k result |
| --- | ---: |
| Signed install | 9.08 s; 11,017 records/s |
| Cumulative allocation | 8,321,332,776 B; 144,954,455 allocations |
| Input | 5,251,144 B compressed; 70,200,000 B uncompressed |
| Installed projection | 138,960,105 B logical; 139,001,856 B on disk |
| Maximum RSS / Darwin peak footprint | 360.9 MiB / 290.9 MiB |
| Cold open | 11.63 ms; 1,846,624 B; 6,779 allocations |
| Six-query search mix | 41.60 ms; 144.2 queries/s; about 6.93 ms/query |

All 120 measured search executions returned results and retained the expected
provider, pack instance, and source identity. The temporary full package suite
also passed. Install time, allocation, and disk size scaled approximately
linearly from 10k; the broad high-document-frequency synthetic queries made
search work scale with corpus size as expected.

This is evidence that a staged 100k profile is technically plausible, not
authority to raise the default. The fixture is unusually repetitive and
compressible. A production cap change still requires a varied corpus, a
low-memory container run, 100k cancellation/corruption cleanup checks, and
explicit disk-budget/operator profiles that preserve the lightweight 10k
installation.

## Caveats and next gate

- `-benchtime=1x` is an exact smoke/performance probe, not a statistically stable benchmark. It provides no variance, confidence interval, p50, or p95.
- Results depend on hardware, filesystem, Go/Bleve versions, thermal state, and cache state. Installed projection size may vary with index internals even though the source fixture is deterministic.
- “Cold open” means opening with no existing application handle; it does not flush the operating-system page cache or simulate a reboot.
- Search uses synthetic, intentionally discoverable terms against an already-open index. It does not include HTTP handling, live discovery, fetching, extraction, or reranking.
- `B/op` is cumulative allocation volume, not peak resident memory.

The varied 100,000-record low-memory gate, transactional cleanup checks, and
operator profiles are now recorded in the
[container scale follow-up](open-index-pack-varied-scale.md). The public/import
cap remains unchanged. The next scale gate is an explicit one-million-record
prototype; this original probe does not establish that readiness.
