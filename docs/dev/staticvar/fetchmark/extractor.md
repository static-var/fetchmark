# extractor

Boilerplate removal + HTML→Markdown. Produces the `model.Content`
blob (title, mainText, markdown, unsupported marker) that the rest of
the pipeline feeds on.

## Entry points

- `extractor.go` — `Extract(html, url) (*model.Content, error)`.
  Wraps go-trafilatura for main-text + html-to-markdown for the
  rendered `format=markdown` output.

## Invariants

- Output shape is versioned. Bump `cache.ExtractorVersion` if any
  field semantic changes; cache keys pin the current version so
  stale entries are not read after an upgrade.
- Empty main-text is not an error — the pipeline uses it as one of
  the auto-render trigger signals, together with a JS-required heuristic.
- Non-HTML content types (PDF, binary) set `Content.Unsupported` and
  return early; do not feed them through the HTML path.

## Tests

- `extractor_test.go` — title extraction, main-text, unsupported
  content-type marking, JS-required detection stubs.
