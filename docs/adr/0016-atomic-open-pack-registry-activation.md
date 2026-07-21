# ADR 0016: Atomic open-pack registry activation and rollback

Status: accepted

## Context

ADR 0014 produces a verified successor registry and the existing installer
materializes its immutable projection without changing the live registry. The
remaining operator step previously required an ad hoc file replacement. That
could overwrite a concurrent configuration change, lose the exact previous
bytes needed for rollback, or report an ordinary failure after the replacement
had already committed.

Registry replacement cannot hot-reload an already running Fetchmark process.
The server snapshots the registry and opens each projection at startup, so a
safe lifecycle must separate file activation, process restart, and later
garbage collection.

## Decision

`fetchmark-pack registry-activate` performs an explicit compare-and-swap for
one existing source:

```sh
fetchmark-pack registry-activate \
  -registry /etc/fetchmark/open-packs.json \
  -source openpack-developer \
  -candidate /etc/fetchmark/open-packs.r3.candidate.json \
  -expected-current-sha256 BASE_REGISTRY_SHA256 \
  -expected-candidate-sha256 CANDIDATE_REGISTRY_SHA256 \
  -rollback-out /etc/fetchmark/open-packs.r2.rollback.json
```

Both digests bind the exact raw files, not only their decoded JSON. Before
locking, the command verifies the supplied candidate digest and strict registry
schema. It then acquires a persistent private advisory lock whose name is
derived from the active registry filename. The lock file is opened through the
descriptor-anchored active parent and is never removed during normal operation;
first-use creation handles competing creators.

While holding that lock, the command:

1. reads the active registry twice through the anchored parent and requires the
   exact expected current digest;
2. proves the candidate is precisely one legal `Promote` transition for the
   named source, preserving publisher trust and every unrelated binding;
3. samples the adapter-owned clock after acquiring the lock, then opens and
   closes the destination immutable projection with every registry identity,
   count, validity, and signing-key expectation;
4. writes the exact current registry bytes to a mode-0600, no-overwrite rollback
   path in the same directory and syncs the directory;
5. writes and syncs a private candidate staging file;
6. rereads and compares the exact current bytes, rechecks cancellation, samples
   a fresh clock, and reopens the destination projection immediately before
   replacement;
7. atomically replaces the active filename, syncs its parent, and verifies the
   exact candidate bytes through the anchored descriptor.

The lock serializes cooperating `fetchmark-pack` processes. It deliberately
does not claim to stop an administrator or unrelated tool from editing the
registry directly. The final byte comparison still detects such an edit before
replacement; operators must use the command consistently for a complete
multi-process compare-and-swap boundary.

Successful output reports `status: "activated"`, the mode and source, exact
from/to and rollback digests, revisions, paths, and
`restart_required: true`. An exact retry after a successful replacement returns
`status: "already_applied"` only when the expected rollback bytes still exist,
the transition remains valid, and the active destination projection still
opens.

Rollback uses the same primitive and requires the exact prior registry file:

```sh
fetchmark-pack registry-rollback \
  -registry /etc/fetchmark/open-packs.json \
  -source openpack-developer \
  -rollback /etc/fetchmark/open-packs.r2.rollback.json \
  -expected-current-sha256 ACTIVE_R3_REGISTRY_SHA256 \
  -expected-rollback-sha256 PRIOR_R2_REGISTRY_SHA256 \
  -rollback-out /etc/fetchmark/open-packs.r3.rollback.json
```

Rollback is accepted only as the exact reverse of a legal promotion, and only
while the prior immutable projection still opens and is within its signed
validity window. It archives the exact newer registry bytes before restoring
the prior bytes and returns `status: "rolled_back"` with
`restart_required: true`.

## Failure and recovery states

- Before rollback preparation, failure leaves the live registry unchanged and
  creates no rollback output.
- After durable rollback preparation but before replacement, a typed prepared
  error reports the rollback path and digest. The live registry remains the
  exact old bytes; the rollback file is retained for inspection and
  `restart_required` remains false.
- After atomic replacement, any close, sync, parent-identity, read-back, or
  result-output failure is a typed committed error. It reports the live path,
  exact from/to digests, rollback path and digest, and restart requirement. The
  operator must inspect those files before retrying.
- An existing rollback output is reused only when it is byte-for-byte identical
  to the registry being archived. A different or malformed file is never
  overwritten.
- Waiting for the advisory lock and cancellation during the final compare both
  leave the live registry unchanged. Cancellation after preparation retains and
  reports the exact rollback archive.

The command never deletes the candidate, prior or new projection, portable
bundle, or rollback archive. It never restarts or signals Fetchmark and never
performs network access. A running process continues using the projection it
opened at startup until the operator performs a controlled restart. Artifact
retention and garbage collection remain later, separately authorized actions.

## Consequences

- Registry activation and rollback are deterministic, reviewable, and
  recoverable without making the API server a configuration writer.
- Exact rollback bytes remain available across canonical JSON representation
  changes.
- Competing candidates from the same base cannot both commit through the
  cooperating command boundary.
- Operators must retain both immutable projections and restart Fetchmark after
  either activation or rollback.
- ADR 0017 supplies publisher-side local mirror-head compare-and-swap, and ADR
  0018 supplies split-custody root rotation. Remote upload protocols, public
  distribution, and artifact garbage collection remain separate work.

## References

- [ADR 0007: Neutral signed open index packs](0007-neutral-signed-open-index-packs.md)
- [ADR 0010: Manual open-pack delta materialization](0010-manual-open-pack-delta-materialization.md)
- [ADR 0014: Verified open-pack registry candidates](0014-verified-open-pack-registry-candidates.md)
- [ADR 0015: Offline TUF repository staging](0015-offline-tuf-repository-staging.md)
- [ADR 0017: Atomic local TUF mirror-head activation](0017-atomic-tuf-mirror-head-activation.md)
