# ADR 0001: Bleve for the opt-in lexical discovery index

Status: accepted for the Phase 4 prototype.

## Constraints

The default Fetchmark binary must remain CPU-only, self-hosted, static,
CGo-free, and useful without embeddings or an LLM. Persistence is opt-in. The
index is a discovery accelerator, not the authoritative content store: complete
immutable artifacts and URL-version pointers remain separate concerns.

## Decision

Use Bleve v2 with its recommended Scorch index for the first embedded lexical
index. Bleve is an Apache-2.0 Go library with stored typed fields, BM25 lexical
scoring, result pagination, and highlighting. It can open a directory in the
same process and does not require another service or CGo.

The adapter requires an explicit `permitted` indexing disposition. Unknown,
robots-blocked, either noindex disposition, and takedowns become bodyless
timestamped tombstones. A newer tombstone cannot be overwritten by an older
permitted retrieval, including after process restart. Search queries always
filter to permitted documents.

Automatic pipeline feeding is limited to cold live retrievals, where both HTML
robots metadata and `X-Robots-Tag` are available. Cached artifacts are never
promoted into the corpus without fresh policy evidence. Renderer-only
retrievals can revoke on explicit HTML noindex/noarchive but never grant retention because
the renderer adapter does not expose response headers.

Primary references:

- [Bleve package and API](https://pkg.go.dev/github.com/blevesearch/bleve/v2)
- [Bleve package structure](https://blevesearch.com/docs/Package-Structure/)

## Alternatives considered

- SQLite FTS5 through `modernc.org/sqlite`: CGo-free, transactional, and
  attractive when retention/version metadata becomes relational. It adds a
  translated SQLite runtime and schema/migration layer before Fetchmark needs
  them. Re-evaluate if URL versions and retention policies outgrow the artifact
  store.
- A custom Pebble/Bolt inverted index: smaller conceptual surface but would
  require implementing analyzers, posting lists, scoring, compaction, and
  corruption recovery. That is undifferentiated risk.
- Meilisearch, Typesense, or OpenSearch: capable, but add a mandatory service
  and violate the lightweight single-binary target.
- Embedding/vector databases: optional semantic retrieval may be layered later;
  it cannot replace the required CPU-only lexical path.

## Consequences

- The stripped CGo-free macOS binary grew from 39,861,378 bytes to 46,588,130
  bytes when runtime wiring pulled Bleve into `cmd/fetchmark`: +6,726,752 bytes,
  or about 16.9%. The later Phase 4 container gate measured the main
  Linux/arm64 binary at 45,547,682 bytes versus 37,814,434 bytes for the exact
  pre-Bleve baseline (+20.45%); its compressed OCI layer grew 26.22%. That later
  comparison bounds all main-server worktree growth, not Bleve alone. The
  all-in-one image grew further because it also bundles the separate crawler,
  pack, and peer commands. See the
  [container-image footprint](../benchmarks/phase4-container-image.md) for exact
  digests, commands, and limitations.
- The index stores discovery fields and a bounded snippet, not full page bodies.
- Normalized host-ancestry and bounded host/path-segment tokens make domain
  filters term-based rather than regular expressions over unique URL terms.
- Safe-search requests only admit documents explicitly classified `safe`.
  Automatic pipeline-fed documents are unclassified. Curated admission may
  carry a validated operator assertion (`safe`, `unsafe`, or `unclassified`),
  but Fetchmark never infers safety from untrusted page text.
- A deterministic Linux/arm64 lifecycle gate now exercises 10,000, 50,000,
  and 100,000 individual production-path reconciliations. At the public cap,
  the closed projection occupied 358.73 MB, reopen took 850.71 ms, warm search
  measured 2.689-ms p50 and 4.271-ms p95, and the complete process/page-cache
  peak was 593.47 MB under a 1-GiB cgroup. The 10,000-document control peaked
  at 106.65 MB and occupied 40.17 MB. See the
  [persistent local-corpus scale gate](../benchmarks/phase4-local-corpus-scale.md).
- The adapter writes a schema-v3 marker and refuses older or unmarked
  directories with an explicit rebuild error. An in-place migration tool is
  still required before changing the mapping for long-lived deployments.
