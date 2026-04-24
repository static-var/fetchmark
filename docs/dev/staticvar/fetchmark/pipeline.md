# pipeline

The orchestrator. Takes `Options`, runs SearXNG → fetch → extract →
optional auto-render → dedupe → BM25, and returns
`[]model.SearchResult`.

## Entry points

- `pipeline.go`
  - `Search(ctx, Options)` — full path: SearXNG query + enrichment.
  - `Parse(ctx, Options)` — skips SearXNG; enriches the caller-
    supplied URL list.
  - `process` — the shared enrichment fan-out (bounded concurrency
    via errgroup + semaphore).
  - `fetchAndExtract` / `renderAndExtract` / `tryAutoRender` — the
    three fetch variants, with auto-render firing only when the
    plain-fetch result is empty or flagged js-required.
- `dedupe.go` — `dedupeByContentSHA` (exact) and
  `dedupeNearDuplicates` (Jaccard over word-3-gram or CJK char bigram
  shingles).

## Invariants

- **`Options` is trusted.** The pipeline does not re-check admin
  gates; `internal/api.buildOptions` already enforced them. Duplicate
  checks here would mean two sources of truth.
- **Auto-render holds its own Redis lock** on `RenderedArtifactKey`,
  sized with `Render=true` so the critical-section budget includes
  the renderer timeout. The plain-key lock in `doFetch` doesn't cover
  this path because auto-render only fires after a plain-fetch
  returned an empty/js-required artifact.
- **`applyContent` only fills empty fields.** To propagate
  peer-populated rendered blobs over a js-required placeholder,
  callers must clear `r.Title` and `r.Unsupported` first. This is
  done in the auto-render post-lock re-apply block.
- Near-dup dedupe runs **after** BM25 rerank so cluster winners are
  selected on real BM25 scores, not on ingest order.

## Tests

- `pipeline_test.go`, `parse_test.go`, `renderer_test.go` — fetch
  modes, cache hit/miss, SSRF, admin gate propagation, auto-render
  rendered-key stampede lock.
- `dedupe_test.go` — exact + near-dup (incl. CJK) behaviour.
