# ADR 0019: Direct multi-part Common Crawl selection

Status: accepted.

## Context

The first real-corpus selector applied a deterministic one-per-host sample to
one exact Common Crawl flat URL Index Parquet part and deliberately capped its
output at the live evidence collector's 10,000-row boundary. The synthetic
consumer prototype proved that the neutral pack format can carry one million
records, but it did not provide a real-corpus selection path.

Combining independently truncated per-part JSONL files is insufficient. A
global top-K result is only provable if each local selection is exhaustive or
retains at least the same K under exactly the same policy. A bare normalized
stream cannot prove what its source part omitted.

## Decision

Fetchmark selects multiple parts directly from their exact raw Parquet bytes:

1. The operator supplies 2--1,024 local non-symlink parts and one explicit
   language, cutoff, profile, and maximum-record policy.
2. Every part passes the existing bounded envelope, Thrift, schema, page, and
   column checks and is fully hashed before decoding.
3. One global domain-separated canonical-URL ranking retains at most one URL
   per host across every part. No intermediate truncated selection is trusted.
4. Every part is fully rehashed after all decoding and again after staged
   output. Duplicate exact parts or any changed part invalidate the operation
   before atomic activation; a change detected before output emits no bytes.
5. The report binds a digest-sorted set of exact part SHA-256, byte, and row
   descriptors. Therefore input enumeration order changes neither output nor
   report.
6. The default `collector-v1` profile remains capped at 10,000 selected rows.
   An explicit `format-prototype-v1` profile permits at most 5,000,000 selected
   rows. Raw scanning remains bounded at 50,000,000 aggregate rows for the
   collector profile and 2,000,000,000 for the prototype profile.

The prototype profile only emits normalized historical URL/capture metadata.
It does not bypass or raise the separate live evidence collector, current
robots/noindex checks, rights review, exclusion policy, lightweight pack
profile, or ordinary installer limits.

## Consequences

- Fetchmark can produce a reproducible real-corpus candidate set across enough
  Common Crawl parts to exercise the million-row format path.
- The selection is intentionally high-resource and operator-run; heap and host
  state scale with the selected-record ceiling.
- Remote partition discovery, download/range orchestration, quality-signal
  selection, scaled current-admission evidence, and a retained real million-row
  build remain separate gates.
- Ordinary Fetchmark installations still perform no Common Crawl processing.
- The first retained two-part run selected 66,576 unique hosts from 13,433,428
  source rows; this observed density informed the prototype raw-scan bound but
  is not a million-row proof.

## References

- [ADR 0007: Neutral signed open-index packs](0007-neutral-signed-open-index-packs.md)
- [ADR 0009: Separate open-pack evidence collection](0009-separate-open-pack-evidence-collection.md)
- [One-million-record open-index-pack prototype](../benchmarks/open-index-pack-million-prototype.md)
- [Two-part real-corpus selection](../benchmarks/common-crawl-url-index-multipart-selection-20260719.md)
