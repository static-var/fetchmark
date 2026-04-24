# rank

BM25 re-ranker over `model.SearchResult`. Operates on title +
mainText with a stopword list.

## Entry points

- `rank.go` — `Rerank(query, results) []model.SearchResult`.
  Pure function; caller passes the deduped slice and receives a
  new slice ordered by score.

## Invariants

- Zero-length query → identity (no reorder). The pipeline relies on
  this for the `Parse` path where there's no query term.
- Score is written to `SearchResult.Score`; callers who compare
  scores across two different rerank invocations should not — BM25
  IDF is corpus-local.
- Tokenisation is ASCII-first; the CJK dedupe branch is in
  `pipeline/dedupe.go`, not here. If CJK ranking becomes a
  requirement, add a separate tokeniser and pick it based on the
  dedupe heuristic.

## Tests

- `rank_test.go` — ordering by term frequency, stopword exclusion,
  empty-query passthrough.
