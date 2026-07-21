# One-million-record open-index-pack prototype

Date: 2026-07-19

This is a bounded, synthetic proof that Fetchmark's neutral signed pack format
and local CPU-only projection path can consume one million discovery records.
It is not a supported production profile, a Common Crawl build, or evidence of
commercial-index parity. The public installer remains limited to one shard,
10,000 records, 64 MiB compressed input, 256 MiB decompressed input, and a
512-MiB logical projection.

## Isolated prototype boundary

`TestOpenPackMillionPrototype` is an opt-in same-package test. It alone supplies
an unexported four-shard policy with a requested-record ceiling of at most one
million, 256 MiB aggregate compressed input, 1 GiB aggregate decompressed input,
a 3-GiB projection ceiling, and a 512-MiB decoder budget. `Install`, `Open`, and
`fetchmark-pack` cannot select those limits. A unit test pins the ordinary
production policy to the exported public constants.

The deterministic fixture streams four content-addressed NDJSON/Zstandard
shards to disk. It contains unique canonical URLs across 8,192 `.example`
domains, eight languages, and varied verticals and lexical fields, without page
bodies. The run verifies every signed compressed/decompressed digest, rejects
cross-shard URL duplication, constructs the projection, closes and reopens it,
checks the full identity marker and exact document count, and executes 96
non-empty correctly attributed searches.

The runner cross-compiles a scratch Linux image, disables networking, uses a
read-only root, drops all capabilities, enables no-new-privileges, limits PIDs,
and provides only an anonymous work volume. The test fails unless cgroup v2
`memory.max` exactly matches the requested limit and `memory.peak` is nonzero
and no greater than that limit. From the repository root:

```sh
FM_OPENPACK_PROTOTYPE_RECORDS=1000000 \
FM_OPENPACK_PROTOTYPE_MEMORY=5g \
FM_OPENPACK_PROTOTYPE_CPUS=4 \
make openpack-million-prototype
```

## Stepped results

All runs used OrbStack's Linux `arm64` Docker runtime, Go 1.26.5, four container
CPUs, and the exact cgroup limits below. The complete schema-versioned results,
including every shard and stream SHA-256 digest, are retained in
[`artifacts/open-index-pack-million-prototype-20260719.jsonl`](artifacts/open-index-pack-million-prototype-20260719.jsonl).

| Metric | 250k gate | 500k gate | 1M gate |
| --- | ---: | ---: | ---: |
| Shards | 4 | 4 | 4 |
| Compressed input | 27,628,961 B | 55,228,178 B | 110,442,829 B |
| Decompressed input | 216,429,670 B | 432,859,299 B | 865,718,538 B |
| Projection logical size | 499,640,443 B | 995,234,822 B | 1,930,149,052 B |
| Install | 26.964 s | 54.941 s | 110.435 s |
| Install throughput | 9,272/s | 9,101/s | 9,055/s |
| Cgroup peak / limit | 772.00 MiB / 2 GiB | 1.854 GiB / 3 GiB | 2.505 GiB / 5 GiB |
| Cold open | 1.250 ms | 1.521 ms | 1.077 ms |
| Search p50 | 1.076 ms | 3.113 ms | 5.592 ms |
| Search p95 | 1.988 ms | 5.959 ms | 10.652 ms |

The one-million run's manifest digest is
`52f4d37192ea2450bf33a30c03ad7d8209960d19691f988d6f024dad08c4255d`.
Its four signed shards contain exactly 250,000 records each. The 110.4-second
install and 2.505-GiB peak establish feasibility on this synthetic fixture, but
the peak is far outside Fetchmark's lightweight ordinary-install target.

## Decision and remaining gate

The bounded one-million-record consumer prototype is complete. It justifies
continuing Phase 6 work on corpus selection and pack creation; it does not by
itself justify raising the public import cap. A production scale profile needs
an explicit opt-in configuration and operator resource contract, real-corpus
quality and exclusion evidence, repeat-run variance, rollback/update lifecycle,
and retention of the existing 10k/256-MiB lightweight path.

The offline publisher now has an explicit `format-prototype-v1` multi-Parquet
selector that can apply one global deterministic policy to as many as 5
million selected rows without changing ordinary limits. The next evidence gate
is a retained real 1–5 million-row selection using that command, followed by a
rights/robots/noindex-safe admission design and repeatable build. No such real
million-row artifact has yet been retained. Common Crawl quality signals and
targeted range retrieval also remain future selection inputs. Signed delta
application, update distribution, rollback/revocation, and public pack hosting
remain separate follow-on work.
