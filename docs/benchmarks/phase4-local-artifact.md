# Phase 4 local-artifact baseline

Recorded 2026-07-18 on an Apple M4 Pro (`darwin/arm64`) before archive schema
work. These one-iteration numbers are directional regression anchors, not a
statistical performance claim.

```bash
GOCACHE=/private/tmp/fetchmark-go-build-cache go test \
  ./internal/adapters/localartifact \
  -run '^$' -bench 'BenchmarkStore' -benchtime=1x -count=1 -benchmem
```

| Operation | URL states | ns/op | bytes/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Put | 100 | 18,006,750 | 442,840 | 2,686 |
| Put | 1,000 | 43,236,917 | 4,175,984 | 24,308 |
| Revalidate | 100 | 11,079,417 | 441,104 | 2,583 |
| Revalidate | 1,000 | 31,902,750 | 4,231,840 | 24,206 |
| Revoke | 100 | 11,863,208 | 434,408 | 2,598 |
| Revoke | 1,000 | 42,369,791 | 4,197,072 | 24,215 |
| Reopen + repair | 100 | 21,414,167 | 1,326,128 | 11,674 |
| Reopen + repair | 1,000 | 183,016,166 | 13,109,280 | 115,202 |

The benchmark fixture creates valid committed pointers and bodies directly,
then performs one complete accounting scan before timing. Timed mutations use
the production adapter.

After replacing routine full-tree recounts with checked mutation-local deltas,
the same command produced:

| Operation | URL states | ns/op | bytes/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Put | 100 | 17,131,625 | 16,856 | 209 |
| Put | 1,000 | 16,076,542 | 18,696 | 211 |
| Revalidate | 100 | 7,862,792 | 8,576 | 106 |
| Revalidate | 1,000 | 7,150,000 | 10,624 | 110 |
| Revoke | 100 | 7,480,875 | 11,944 | 145 |
| Revoke | 1,000 | 8,227,417 | 11,944 | 145 |

## Interpretation

The baseline's near-tenfold allocation growth between 100 and 1,000 URL states
confirmed the full-tree accounting bottleneck. After the change, allocation
counts remain effectively flat and operation latency is dominated by synced
filesystem commits rather than corpus cardinality. Startup repair retains its
complete validation/hash scan; partial-I/O recovery still conservatively
recounts. A regression test compares cached accounting with a full measurement
after put, replacement, revalidation, revocation, and new-tombstone mutations.
These benchmarks remain the archive-schema gate for future
100/1,000/greater-corpus comparisons.
