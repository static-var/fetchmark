# ADR 0020: Bounded open-pack admission partitions

Status: accepted

## Context

The deterministic Common Crawl selector can produce one to five million
host-diverse normalized rows, while the live admission collector deliberately
accepts at most 10,000 rows. Raising that collector cap would make one networked
process retain too many robots objects, create an impractical all-or-nothing
run, and risk exhausting the maximum 24-hour evidence window.

Running arbitrary slices in parallel is not sufficient. A publisher must prove
that every selected input row belongs to exactly one collector job, that the
jobs preserve deterministic final order, and that no selected origin appears
in two workers. That last property does not cover effective hosts reached by a
robots redirect.

## Decision

`fetchmark-pack-evidence partition` is an offline preparation step for scaled
admission. It consumes one exact normalized-v1 JSONL file and atomically emits
a no-overwrite directory containing sequential collector inputs and
`manifest.json`.

The partitioner:

- accepts at most five million rows and eight GiB;
- strictly decodes every normalized row and requires a canonical URL;
- requires one globally unique selected hostname across the complete input;
- preserves each line and newline byte exactly and preserves source order;
- emits at most 10,000 rows per shard, leaving the live collector cap
  unchanged;
- records the exact input digest, bytes, and rows plus each shard's path,
  digest, bytes, records, and inclusive global row interval;
- verifies that ordered shard bytes reproduce the exact source digest;
- hashes the source before processing, after shard creation, and immediately
  before activation, then revalidates the production CLI's open descriptor and
  source pathname identity, size, and modification time; and
- publishes nothing on malformed input, duplicate hosts, cancellation, source
  mutation, partial writes, or destination collision.

Sequential shards are intentional. The upstream selector already emits at
most one URL per selected host, and the partitioner rechecks that invariant
globally. This prevents direct-origin duplication, but distinct origins can
redirect `/robots.txt` to the same effective host. Because pacing and
`Retry-After` state are currently process-local, operators must not collect
partitions concurrently until a durable shared effective-host gate exists. The
partition step does not relax per-request SSRF, robots, noindex, timeout, or
rights policy.

This partition format is preparation, not a distributed collector claim.
[ADR 0021](0021-verified-open-pack-evidence-merge.md) now defines the
network-free verifier/merger and signed snapshot evidence binding. Worker
identity, leases/resumption, scheduling within the evidence window, and
continuous refresh/tombstone policy remain separate decisions. The scheduler
must also coordinate actual robots redirect hosts and their `Retry-After`
cooldowns across workers before parallel collection is permitted.

## Consequences

- A million-row selection becomes 100 independently bounded collector inputs;
  five million rows becomes 500.
- Ordinary Fetchmark nodes and the ordinary 10,000-row collector path remain
  unchanged.
- Future scheduling can be designed around explicit immutable work units
  rather than implicit byte ranges; current jobs remain serial without a
  shared effective-host gate.
- Publishers still cannot claim a compliant million-row pack until a real
  current evidence run, rights review, repeat build, and consumer proof exist.
- The hostname map is a high-resource offline publisher cost; it is never
  allocated by the API server or an ordinary pack consumer.

## References

- [ADR 0009: Separate open-pack admission evidence collection](0009-separate-open-pack-evidence-collection.md)
- [ADR 0019: Direct multi-part Common Crawl selection](0019-direct-multipart-common-crawl-selection.md)
- [RFC 9309: Robots Exclusion Protocol](https://www.rfc-editor.org/rfc/rfc9309.html)
