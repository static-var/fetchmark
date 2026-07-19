# Phase 4 container-image footprint

Recorded 2026-07-19 on Apple M4 Pro (`darwin/arm64`) with OrbStack Docker
29.4.0 (`linux/arm64`, 12 CPUs, 8,393,830,400 bytes reported memory). This is
an artifact-size comparison, not a runtime-memory or search-latency benchmark.

## Compared artifacts

The baseline was built without changing the worktree, using the exact committed
`18cdf18a3ecfdb0a6ea276a1ac4f72b373349ef4` archive as Docker stdin:

```sh
git archive HEAD | docker build \
  --file deploy/Dockerfile \
  --tag fetchmark:baseline-18cdf18 \
  --build-arg VERSION=18cdf18 -
```

The current image was built from the active Phase 0--7 worktree:

```sh
docker build \
  --file deploy/Dockerfile \
  --tag fetchmark:phase4-measure-20260719 \
  --build-arg VERSION=18cdf18-phase4-measure .
```

Neither image was pushed or deployed. The current worktree is not a release
revision, so its OCI digest identifies this exact measured artifact; the tag or
embedded version string alone does not reproduce it.

| Measure | Baseline `18cdf18` | Current worktree | Change |
| --- | ---: | ---: | ---: |
| OCI index digest | `sha256:1ab6417ad5dd1a7ca324df2eb6a0fe3b0041213c623e3b7a7091dff47a792349` | `sha256:a6ea028b433ea7be665a021dcbff670c77357b300e85fecd73b78b612e0e0b27` | n/a |
| Compressed OCI layer bytes | 11,618,649 | 23,866,789 | +12,248,140 (+105.42%) |
| Docker virtual size | 55.7 MB | 99.8 MB | +44.1 MB (+79.2%) |
| Main `/fetchmark` bytes | 37,814,434 | 45,547,682 | +7,733,248 (+20.45%) |
| Main compressed layer bytes | 10,806,786 | 13,640,848 | +2,834,062 (+26.22%) |

The current image deliberately also contains the optional operator commands
introduced after the baseline. Their exact uncompressed/compressed layer sizes
were:

| Binary | Uncompressed bytes | Compressed layer bytes |
| --- | ---: | ---: |
| `/fetchmark-crawl` | 6,881,442 | 2,868,632 |
| `/fetchmark-pack` | 14,418,082 | 5,411,363 |
| `/fetchmark-peer` | 2,818,210 | 1,133,776 |

## Interpretation

The main executable comparison bounds the whole server-side growth since the
pre-Bleve revision while holding the base image constant. It does **not**
isolate Bleve: the current binary also contains the later discovery providers,
compatibility routes, evaluator evidence, crawler admission, open-pack runtime,
and federation client wiring. Its 20.45% uncompressed and 26.22% compressed
growth is material but still compatible with the lightweight, CPU-only
installation goal. ADR 0001's earlier same-slice macOS comparison is the
narrower estimate of Bleve wiring alone.

The 105.42% total compressed-layer increase likewise must not be attributed to
Bleve: 9,413,771 compressed bytes come from the three separately invoked
crawler, pack, and peer binaries. They share Go dependencies conceptually but
static linking duplicates those bytes in the all-in-one runtime image. A later
image packaging decision can split operator tools into optional images without
changing Fetchmark's no-per-query-cost or CPU-only behavior.

This gate closes the previously unmeasured image-size question. It does not
measure startup RSS, persistent-index growth, indexing throughput, or
end-to-end search latency at larger local-corpus cardinalities; those remain
separate Phase 4 benchmarks.
