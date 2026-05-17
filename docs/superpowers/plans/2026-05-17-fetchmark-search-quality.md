# Fetchmark Search Quality Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve Fetchmark search quality versus Tavily by over-fetching candidates, applying quality-aware ranking, and exposing SearXNG search controls/metadata.

**Architecture:** Keep SearXNG as the candidate provider, but separate the requested result count from the internal candidate pool. Preserve upstream metadata through `search.Hit` into `model.SearchResult`, then let the ranker combine BM25 with deterministic quality signals before final trimming.

**Tech Stack:** Go stdlib, existing Fetchmark pipeline/ranker/API patterns, chi HTTP tests, httptest SearXNG adapter tests.

---

## File Structure

- Modify `internal/core/search/search.go`: add `TimeRange`, `Metadata`, and optional `PublishedAt` to search-layer types.
- Modify `internal/adapters/searxng/client.go`: pass `time_range`, preserve category/original rank/date metadata.
- Modify `internal/core/pipeline/pipeline.go`: add `Categories`, `Language`, `TimeRange`, `CandidateCap`; over-fetch before processing; copy hit metadata.
- Modify `internal/api/handlers.go`: accept categories/language/time_range; set candidate cap from config.
- Modify `internal/core/rank/rank.go`: add deterministic quality adjustments.
- Modify tests in `internal/core/rank/rank_test.go`, `internal/core/pipeline/pipeline_test.go`, `internal/adapters/searxng/client_test.go`, `internal/api/server_test.go`.
- Modify `docs/openapi.yaml`: document new request fields and metadata.

---

### Task 1: Wire API Search Controls and Metadata

**Files:**
- Modify: `internal/core/search/search.go`
- Modify: `internal/adapters/searxng/client.go`
- Modify: `internal/core/pipeline/pipeline.go`
- Modify: `internal/api/handlers.go`
- Test: `internal/adapters/searxng/client_test.go`
- Test: `internal/core/pipeline/pipeline_test.go`
- Test: `internal/api/server_test.go`

- [ ] **Step 1: Write failing SearXNG adapter test**

Add/extend a test in `internal/adapters/searxng/client_test.go` that creates a test server, calls `Search` with:

```go
search.Query{
    Q: "bird species",
    Categories: []string{"general", "news"},
    Language: "en",
    TimeRange: "year",
}
```

Assert the request query includes `categories=general,news`, `language=en`, and `time_range=year`. Return two JSON results with categories. Assert returned hits include `Metadata["category"]` and `Metadata["original_rank"]` values `"1"`, `"2"`.

- [ ] **Step 2: Run adapter test and verify failure**

Run:

```bash
go test ./internal/adapters/searxng -run 'Test.*Search' -count=1
```

Expected: FAIL because `TimeRange` and metadata are not implemented.

- [ ] **Step 3: Implement search type and SearXNG changes**

In `internal/core/search/search.go`, add:

```go
TimeRange string
Metadata map[string]string
```

Use `TimeRange` on `Query` and `Metadata` on `Hit`.

In `internal/adapters/searxng/client.go`, add `time_range` query param when set and populate metadata:

```go
if q.TimeRange != "" {
    vals.Set("time_range", q.TimeRange)
}

metadata := map[string]string{"original_rank": strconv.Itoa(i + 1)}
if r.Category != "" {
    metadata["category"] = r.Category
}
```

- [ ] **Step 4: Write failing pipeline/API propagation tests**

Add tests asserting `/v1/search` accepts `categories`, `language`, and `time_range`, and a stub searcher receives them. Also assert `hitsToResults` copies `Metadata`.

- [ ] **Step 5: Implement API and pipeline propagation**

Add fields to `searchRequest` and `pipeline.Options`:

```go
Categories []string `json:"categories,omitempty"`
Language string `json:"language,omitempty"`
TimeRange string `json:"time_range,omitempty"`
CandidateCap int
```

Pass them from handler to options and from options to `search.Query`. Copy hit metadata in `hitsToResults`.

- [ ] **Step 6: Run focused tests**

```bash
go test ./internal/api ./internal/core/pipeline ./internal/adapters/searxng -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/core/search/search.go internal/adapters/searxng/client.go internal/core/pipeline/pipeline.go internal/api/handlers.go internal/adapters/searxng/client_test.go internal/core/pipeline/pipeline_test.go internal/api/server_test.go
git commit -m "feat: wire search controls and metadata"
```

---

### Task 2: Over-fetch Candidates Before Ranking

**Files:**
- Modify: `internal/core/pipeline/pipeline.go`
- Modify: `internal/api/handlers.go`
- Test: `internal/core/pipeline/pipeline_test.go`
- Test: `internal/api/server_test.go`

- [ ] **Step 1: Update failing pipeline tests**

Replace/adjust `TestSearchCapsCandidatesBeforeFetch` so `MaxResults=5` and `CandidateCap=15`; assert the fetcher sees up to 15 candidates and the final response length is at most 5. Add duplicate-before-cap test where early duplicate URLs do not starve later unique candidates.

- [ ] **Step 2: Run test and verify failure**

```bash
go test ./internal/core/pipeline -run 'TestSearch.*Cap|TestSearch.*Duplicate' -count=1
```

Expected: FAIL because current code trims to `MaxResults` before processing.

- [ ] **Step 3: Implement candidate cap helper**

In `pipeline.go`, replace early `MaxResults` trim with:

```go
cap := o.CandidateCap
if cap <= 0 {
    cap = o.MaxResults
}
if cap > 0 && len(hits) > cap {
    hits = hits[:cap]
}
```

Keep final trim to `MaxResults` after ranking/dedupe.

- [ ] **Step 4: Set API candidate cap**

In the search handler after resolving requested max results, set:

```go
candidateCap := maxResults * 3
if candidateCap < maxResults {
    candidateCap = maxResults
}
if candidateCap > d.Config.ResultsCap {
    candidateCap = d.Config.ResultsCap
}
opts.CandidateCap = candidateCap
```

- [ ] **Step 5: Run focused tests**

```bash
go test ./internal/core/pipeline ./internal/api -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/core/pipeline/pipeline.go internal/api/handlers.go internal/core/pipeline/pipeline_test.go internal/api/server_test.go
git commit -m "feat: overfetch search candidates before ranking"
```

---

### Task 3: Add Quality-Aware Ranking

**Files:**
- Modify: `internal/core/rank/rank.go`
- Test: `internal/core/rank/rank_test.go`

- [ ] **Step 1: Write failing ranker tests**

Add tests for these cases:

```go
func TestRankerPenalizesUnsupportedResults(t *testing.T) {}
func TestRankerPenalizesSocialResultsForFreshnessQueries(t *testing.T) {}
func TestRankerPenalizesHomepagesForFreshnessQueries(t *testing.T) {}
func TestRankerBoostsRecentArticleLikeResults(t *testing.T) {}
func TestRankerKeepsReferenceUsefulForEvergreenQueries(t *testing.T) {}
```

Use queries like `recent bird species discovery` for freshness behavior and `bird species taxonomy` for evergreen behavior.

- [ ] **Step 2: Run rank tests and verify failure**

```bash
go test ./internal/core/rank -count=1
```

Expected: FAIL for new quality tests.

- [ ] **Step 3: Implement ranking helpers**

Add deterministic helpers in `rank.go`:

```go
func isFreshnessQuery(query string) bool
func isSocialHost(host string) bool
func isHomepage(rawURL string) bool
func isReferenceURL(rawURL string) bool
func isArticleLike(r model.SearchResult) bool
func qualityAdjustment(query string, r model.SearchResult) float64
```

Use `net/url`, `strings`, and `time` only.

- [ ] **Step 4: Apply quality adjustment**

After BM25 and before final sort, multiply or add adjustments so:

- unsupported results receive a strong demotion;
- social hosts are demoted for freshness queries;
- homepages are demoted for freshness queries;
- reference/wiki pages are mildly demoted for freshness queries but not evergreen queries;
- recent article-like pages get a modest boost.

Keep adjustments bounded so BM25 still matters.

- [ ] **Step 5: Run focused tests**

```bash
go test ./internal/core/rank -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/core/rank/rank.go internal/core/rank/rank_test.go
git commit -m "feat: add quality-aware search ranking"
```

---

### Task 4: Update OpenAPI Docs and Run Full Verification

**Files:**
- Modify: `docs/openapi.yaml`

- [ ] **Step 1: Update OpenAPI**

Add `categories`, `language`, `time_range`, and `render` to `SearchRequest`. Add `metadata` to `SearchResult`, documenting `category` and `original_rank`.

- [ ] **Step 2: Run full tests**

```bash
go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 3: Run build**

```bash
go build ./cmd/fetchmark
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add docs/openapi.yaml
git commit -m "docs: document search quality controls"
```
