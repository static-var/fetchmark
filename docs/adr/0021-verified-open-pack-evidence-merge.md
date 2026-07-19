# ADR 0021: Verified open-pack evidence merge and signed snapshot binding

Status: accepted

## Context

ADR 0020 introduced deterministic collector-sized partitions without raising
the live collector's 10,000-row ceiling. A publisher still could not safely
concatenate collector candidate files: bundle paths were not ordered, local
observation row numbers restarted at one, and no artifact independently proved
complete partition coverage, candidate/observation lockstep, common policy
identity, or current permission evidence.

The portable manifest v2 already binds the exact candidate stream, selection
policy, exclusions, and source artifact. Changing that manifest version solely
to add publisher audit evidence would also change the consumer record-format
contract. The separately signed build report is the narrower publisher-audit
boundary.

## Decision

`fetchmark-pack-evidence merge` is a network-free, deterministic verification
step. It accepts one exact partition directory, exactly one collector bundle
for each partition, and a new output directory. Bundle arguments may be in any
order; the merger matches them by the input digest, byte count, and row count
declared in each collector report and restores partition-manifest order.

Before publishing anything, the merger:

- strictly validates the partition manifest, canonical shard paths, contiguous
  global row intervals, row and byte totals, exact shard digests, global
  hostname uniqueness, and reproduction of the original input digest;
- strictly validates every collector report and all declared candidate,
  observation, and content-addressed robots artifacts through anchored regular
  files;
- scans each partition input and observation stream in lockstep, verifies local
  row identity, exact input-line digest and URL, consumes one candidate only for
  an admitted observation, and requires exact capture metadata and matching
  robots/indexing/rights evidence;
- recomputes outcome and rejection counts, referenced robots objects, candidate
  bytes, and the earliest admitted permission expiry;
- requires one collector version, config digest, rights-evidence digest, robots
  user agent, and rights semantic identity across all admitted bundles;
- rejects stale evidence unless more than the existing one-minute builder
  handoff remains; and
- performs the complete source verification a second time immediately before a
  descriptor-anchored atomic no-replace activation.

The output contains only `candidates.jsonl` and `merge-report.json`. It does not
copy page bodies, rights-evidence bytes, observations, or potentially millions
of robots files. The aggregate report binds the partition manifest, original
input, merged candidate stream, ordered global ranges, every collector report
digest, source artifact descriptors, accounting, shared policy identity, and
earliest permission expiry. Exact source bundles must therefore be retained for
audit replay.

An evidence-bound snapshot build receives the exact aggregate report alongside
the merged candidate stream. It revalidates the report, candidate digest and
count, robots user agent, exact candidate-derived rights identity, selection
accounting, and evidence window both before signing and immediately before
activation. It carries the exact bytes as `evidence-report.json` and emits
signed BuildReport v3 with `evidence_report_sha256`. The independent publisher-
bundle verifier rehashes and validates that file, its selection bindings, and
the aggregate rights notice against every signed record. Manifest v2 remains
unchanged, and its standalone signature does not claim to bind the aggregate
report.

Legacy signed BuildReport v2 verification remains supported for existing
snapshots. New builds without an aggregate report still use that legacy report
shape through the library compatibility path; the documented scaled publisher
workflow supplies the report and produces v3. Delta BuildReport v1 does not yet
carry aggregate evidence and must not be described as evidence-bound.

## Consequences

- Candidate concatenation is no longer an operator-trust step; exact ordering,
  coverage, evidence relationships, and source drift are checked by code.
- The complete publisher-retained snapshot bundle can prove which aggregate
  audit report its publisher signed without expanding the manifest format.
  The current TUF channel transport intentionally distributes only the
  manifest, detached manifest signature, and shards; channel-retrieved consumer
  bundles are not BuildReport-v3 audit bundles until that target contract is
  versioned to carry the three audit files.
- BuildReport signatures use distinct v2 and v3 domain separators, preventing a
  report from being reinterpreted across schemas.
- Collector reports remain unsigned. The merger independently re-evaluates
  archived robots policies and retained header/derived metadata directives,
  but page representations are intentionally not retained, so it cannot prove
  that a worker extracted the metadata values faithfully. Verification proves
  bounded internal consistency, not remote worker identity or the truth of
  legal assertions. Current use is limited to operator-owned or explicitly
  allowlisted workers.
- This decision does not authorize concurrent partition collection. Effective
  robots redirect hosts, pacing, `Retry-After`, leases, and restart state remain
  process-local until the next orchestration decision.
- A real 1--5 million-row selection/admission/repeat-build run and its resource
  profile remain required before raising an operator-facing pack profile.

## References

- [ADR 0009: Separate open-pack admission evidence collection](0009-separate-open-pack-evidence-collection.md)
- [ADR 0020: Bounded open-pack admission partitions](0020-bounded-open-pack-admission-partitions.md)
- [RFC 9309: Robots Exclusion Protocol](https://www.rfc-editor.org/rfc/rfc9309.html)
