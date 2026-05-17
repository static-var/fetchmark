# Fetchmark Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Fetchmark’s documented security/config knobs real, cap expensive work, harden CI/deploy defaults, and reduce operational risk.

**Architecture:** Keep the current ports-and-adapters structure. Add small, testable constructors/helpers instead of large rewrites. Prioritize behavior-preserving hardening: config wiring, limits, tests, and CI gates before larger refactors.

**Tech Stack:** Go 1.26, chi HTTP handlers, go-redis/miniredis, x/time/rate, GitHub Actions, Docker Compose.

---

## File Structure

- Modify `internal/adapters/egress/policy.go`: add `ResponseHeaderTimeout`, apply it in `Transport`, and keep redirect/dial enforcement centralized.
- Modify `cmd/fetchmark/main.go`: wire `Config` into external egress policy, close cache/Redis, add server read/write timeouts.
- Modify `internal/config/config.go`: add summarize cap config and validation.
- Modify `internal/api/summarize.go`: enforce public summarize override caps.
- Modify `internal/core/pipeline/pipeline.go`: cap search candidates before fetch/extract and later implement response format filtering.
- Modify `internal/api/middleware/ratelimit.go`: replace Redis fixed-window limiter with a Redis token bucket matching local semantics.
- Modify `.github/workflows/ci.yml`, `.github/workflows/release.yml`, `Makefile`: add CI verification gates.
- Modify `.env.example`, `deploy/docker-compose.yml`, `deploy/docker-compose.external.yml`: remove production-dangerous credential defaults and pin/secure renderer defaults.
- Add or extend tests beside affected packages.

---

## Phase 1: Critical security/config correctness

### Task 1: Wire egress config into runtime policy

**Files:**
- Modify: `cmd/fetchmark/main.go`
- Test: `internal/adapters/egress/policy_test.go`

- [ ] **Step 1: Add a focused policy test for allow/deny behavior**

Add to `internal/adapters/egress/policy_test.go`:

```go
func TestPolicyHostAllowDenyLists(t *testing.T) {
	ctx := context.Background()

	p := DefaultInternal()
	p.HostAllowlist = []string{"allowed.example"}
	p.HostDenylist = []string{"blocked.example"}

	if err := p.Validate(ctx, "http://allowed.example/page"); err != nil {
		t.Fatalf("allowed host rejected: %v", err)
	}

	var e *Error
	if err := p.Validate(ctx, "http://other.example/page"); !errors.As(err, &e) || e.Reason != ReasonHostNotAllow {
		t.Fatalf("expected host_not_allowlisted, got %#v", err)
	}

	if err := p.Validate(ctx, "http://blocked.example/page"); !errors.As(err, &e) || e.Reason != ReasonHostDenied {
		t.Fatalf("expected host_denied, got %#v", err)
	}
}
```

Ensure imports include `context` and `errors`.

- [ ] **Step 2: Run the test**

Run:

```bash
go test ./internal/adapters/egress -run TestPolicyHostAllowDenyLists -count=1
```

Expected: PASS. This confirms policy behavior before wiring.

- [ ] **Step 3: Wire config values in `cmd/fetchmark/main.go`**

Replace:

```go
external := egress.DefaultExternal()
```

with:

```go
external := egress.DefaultExternal()
external.HostAllowlist = cfg.HostAllowlist
external.HostDenylist = cfg.HostDenylist
external.MaxRedirects = cfg.MaxRedirects
external.DialTimeout = cfg.HeaderTimeout
external.ResponseHeaderTimeout = cfg.HeaderTimeout
```

This requires Task 2’s `ResponseHeaderTimeout` field before the full package compiles. If executing strictly task-by-task, do Task 2 immediately after this task before running `go test ./...`.

- [ ] **Step 4: Commit**

```bash
git add cmd/fetchmark/main.go internal/adapters/egress/policy_test.go
git commit -m "fix: wire egress host policy config"
```

### Task 2: Apply header timeout and redirect config in egress transport

**Files:**
- Modify: `internal/adapters/egress/policy.go`
- Test: `internal/adapters/egress/policy_test.go`

- [ ] **Step 1: Write redirect limit test**

Add to `internal/adapters/egress/policy_test.go`:

```go
func TestHTTPClientHonorsMaxRedirects(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, srv.URL+"/next", http.StatusFound)
	}))
	defer srv.Close()

	p := DefaultInternal()
	p.MaxRedirects = 1
	client := p.HTTPClient(time.Second)

	_, err := client.Get(srv.URL + "/start")
	var e *Error
	if !errors.As(err, &e) || e.Reason != ReasonTooManyHops {
		t.Fatalf("expected too_many_redirects, got %#v", err)
	}
}
```

Ensure imports include `net/http`, `net/http/httptest`, `time`, and `errors`.

- [ ] **Step 2: Write response header timeout test**

Add to `internal/adapters/egress/policy_test.go`:

```go
func TestTransportHonorsResponseHeaderTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := DefaultInternal()
	p.ResponseHeaderTimeout = 25 * time.Millisecond
	client := p.HTTPClient(time.Second)

	_, err := client.Get(srv.URL)
	if err == nil {
		t.Fatal("expected response header timeout error")
	}
	if !strings.Contains(err.Error(), "timeout awaiting response headers") {
		t.Fatalf("expected response header timeout, got %v", err)
	}
}
```

Ensure imports include `strings`.

- [ ] **Step 3: Run tests to verify the header timeout test fails before implementation**

Run:

```bash
go test ./internal/adapters/egress -run 'TestHTTPClientHonorsMaxRedirects|TestTransportHonorsResponseHeaderTimeout' -count=1
```

Expected: redirect test may pass; header timeout test FAILS until implementation.

- [ ] **Step 4: Implement `ResponseHeaderTimeout`**

In `internal/adapters/egress/policy.go`, add to `Policy`:

```go
// ResponseHeaderTimeout bounds the time waiting for upstream headers.
// Zero means no header-specific timeout beyond the client timeout.
ResponseHeaderTimeout time.Duration
```

In `Transport()`, add to `http.Transport` literal:

```go
ResponseHeaderTimeout: p.ResponseHeaderTimeout,
```

- [ ] **Step 5: Run tests**

```bash
go test ./internal/adapters/egress -count=1
go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/adapters/egress/policy.go internal/adapters/egress/policy_test.go cmd/fetchmark/main.go
git commit -m "fix: honor egress timeout and redirect config"
```

### Task 3: Add HTTP server read/write timeouts and lifecycle cleanup

**Files:**
- Modify: `cmd/fetchmark/main.go`

- [ ] **Step 1: Add cache and Redis close calls**

After:

```go
c := cache.New(rdb, cfg.CacheTTL)
```

add:

```go
defer c.Close()
if rdb != nil {
	defer func() {
		if err := rdb.Close(); err != nil {
			log.Warn("redis close failed", "err", err)
		}
	}()
}
```

- [ ] **Step 2: Add server body/read/write timeouts**

Replace the server literal with:

```go
srv := &http.Server{
	Addr:              cfg.ListenAddr,
	Handler:           handler,
	ReadHeaderTimeout: 5 * time.Second,
	ReadTimeout:       15 * time.Second,
	WriteTimeout:      90 * time.Second,
	IdleTimeout:       60 * time.Second,
}
```

- [ ] **Step 3: Verify**

```bash
go test ./cmd/fetchmark ./... -count=1
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add cmd/fetchmark/main.go
git commit -m "fix: add server timeouts and close resources"
```

---

## Phase 2: Expensive-work limits

### Task 4: Cap search candidates before fetch/extract

**Files:**
- Modify: `internal/core/pipeline/pipeline.go`
- Test: `internal/core/pipeline/pipeline_test.go`

- [ ] **Step 1: Find the search flow**

Open `internal/core/pipeline/pipeline.go` and locate `func (p *Pipeline) Search`. Identify where SearXNG results are returned and before they are passed into parse/fetch work.

- [ ] **Step 2: Add failing test**

Add a test using the existing fake searcher/fetcher patterns in `internal/core/pipeline/pipeline_test.go`. The test must create more upstream search results than `Options.MaxResults` and assert the fetcher only sees `MaxResults` URLs:

```go
func TestSearchCapsCandidatesBeforeFetch(t *testing.T) {
	searcher := &fakeSearcher{results: []search.Result{
		{URL: "https://example.com/1", Title: "one"},
		{URL: "https://example.com/2", Title: "two"},
		{URL: "https://example.com/3", Title: "three"},
	}}
	fetcher := &recordingFetcher{body: []byte("<html><body>hello world</body></html>")}
	p := &Pipeline{
		Searcher:  searcher,
		Fetcher:   fetcher,
		Extractor: extractor.New(true),
		Ranker:    rank.New(),
	}

	_, err := p.Search(context.Background(), Options{Query: "hello", MaxResults: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(fetcher.urls); got != 1 {
		t.Fatalf("expected 1 fetched candidate, got %d: %v", got, fetcher.urls)
	}
}
```

If the package uses different fake names, add small local fakes in the test file with the exact interfaces from `Pipeline`.

- [ ] **Step 3: Run test to verify failure**

```bash
go test ./internal/core/pipeline -run TestSearchCapsCandidatesBeforeFetch -count=1
```

Expected: FAIL because all candidates are fetched.

- [ ] **Step 4: Implement candidate cap**

In `Search`, after upstream search results are available and before parse/fetch, add:

```go
if opts.MaxResults > 0 && len(hits) > opts.MaxResults {
	hits = hits[:opts.MaxResults]
}
```

Use the actual local variable name for the upstream hits slice.

- [ ] **Step 5: Verify**

```bash
go test ./internal/core/pipeline -run TestSearchCapsCandidatesBeforeFetch -count=1
go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/core/pipeline/pipeline.go internal/core/pipeline/pipeline_test.go
git commit -m "fix: cap search candidates before fetch"
```

### Task 5: Enforce summarize override caps

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/api/summarize.go`
- Test: `internal/api/summarize_test.go`

- [ ] **Step 1: Add config fields**

In `Config`, add near summarize config:

```go
SummarizeMaxTokensCap       int           `env:"FM_SUMMARIZE_MAX_TOKENS_CAP"       envDefault:"4096"`
SummarizeMaxTimeout         time.Duration `env:"FM_SUMMARIZE_MAX_TIMEOUT"          envDefault:"120s"`
SummarizeMaxInstructionsLen int           `env:"FM_SUMMARIZE_MAX_INSTRUCTIONS_LEN" envDefault:"4000"`
SummarizeAllowModelOverride bool          `env:"FM_SUMMARIZE_ALLOW_MODEL_OVERRIDE" envDefault:"false"`
```

In `validate()`, add:

```go
if c.SummarizeMaxTokensCap <= 0 {
	return errors.New("FM_SUMMARIZE_MAX_TOKENS_CAP must be > 0")
}
if c.SummarizeMaxTimeout <= 0 {
	return errors.New("FM_SUMMARIZE_MAX_TIMEOUT must be > 0")
}
if c.SummarizeMaxInstructionsLen < 0 {
	return errors.New("FM_SUMMARIZE_MAX_INSTRUCTIONS_LEN must be >= 0")
}
```

- [ ] **Step 2: Add API tests**

In `internal/api/summarize_test.go`, add table cases that POST `/v1/summarize` as a non-admin key and expect `400` for:

```json
{"urls":["https://example.com"],"max_tokens":4097}
```

when cap is `4096`,

```json
{"urls":["https://example.com"],"timeout_ms":121000}
```

when max timeout is `120s`, and

```json
{"urls":["https://example.com"],"model":"expensive-model"}
```

when model override is disabled.

- [ ] **Step 3: Run tests to verify failure**

```bash
go test ./internal/api -run Summarize -count=1
```

Expected: FAIL until caps are implemented.

- [ ] **Step 4: Implement validation in summarize handler**

In `internal/api/summarize.go`, after decoding the request and before provider execution, add checks equivalent to:

```go
if req.MaxTokens > d.Config.SummarizeMaxTokensCap {
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": "max_tokens exceeds configured cap"})
	return
}
if req.TimeoutMS > 0 && time.Duration(req.TimeoutMS)*time.Millisecond > d.Config.SummarizeMaxTimeout {
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": "timeout_ms exceeds configured cap"})
	return
}
if !d.Config.SummarizeAllowModelOverride && req.Model != "" && !principal.Admin {
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": "model override disabled"})
	return
}
if d.Config.SummarizeMaxInstructionsLen >= 0 && len(req.Instructions) > d.Config.SummarizeMaxInstructionsLen {
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": "instructions too long"})
	return
}
```

Use the existing principal variable/function in the handler.

- [ ] **Step 5: Verify**

```bash
go test ./internal/config ./internal/api -count=1
go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/config/config.go internal/api/summarize.go internal/api/summarize_test.go
git commit -m "fix: cap summarize request overrides"
```

---

## Phase 3: Rate limiting and response-size correctness

### Task 6: Replace Redis fixed-window rate limit with token bucket

**Files:**
- Modify: `internal/api/middleware/ratelimit.go`
- Test: `internal/api/middleware/ratelimit_test.go`

- [ ] **Step 1: Add Redis behavior test**

Add a test using miniredis that creates `RateLimiter(1, 2, rdb)`, sends two immediate requests successfully, expects the third immediate request to return `429`, waits just over one second, then expects one request to pass.

Expected assertions:

```go
if rr.Code != http.StatusOK { t.Fatalf("first request = %d", rr.Code) }
if rr.Code != http.StatusOK { t.Fatalf("second request = %d", rr.Code) }
if rr.Code != http.StatusTooManyRequests { t.Fatalf("third request = %d", rr.Code) }
time.Sleep(1100 * time.Millisecond)
if rr.Code != http.StatusOK { t.Fatalf("refilled request = %d", rr.Code) }
```

- [ ] **Step 2: Run test**

```bash
go test ./internal/api/middleware -run RateLimiter -count=1
```

Expected: current Redis behavior may pass for this simple case; add an additional assertion with `RateLimiter(0.5, 2, rdb)` where refill should require two seconds. That case should FAIL under one-second fixed-window behavior.

- [ ] **Step 3: Implement Redis token bucket Lua script**

Replace `redisAllow` with a script that stores token count and timestamp in one Redis hash. Use milliseconds and pass rate/burst as ARGV. Pseudocode to encode directly as Lua string:

```lua
local key = KEYS[1]
local now = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then tokens = burst end
if ts == nil then ts = now end
local delta = math.max(0, now - ts) / 1000
local filled = math.min(burst, tokens + delta * rate)
local allowed = 0
if filled >= 1 then
  filled = filled - 1
  allowed = 1
end
redis.call('HMSET', key, 'tokens', filled, 'ts', now)
redis.call('PEXPIRE', key, ttl)
return allowed
```

Hash the API key as before. Use TTL `ceil((burst/rate)*2000)` milliseconds with a minimum of `2000`.

- [ ] **Step 4: Verify**

```bash
go test ./internal/api/middleware -count=1
go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/api/middleware/ratelimit.go internal/api/middleware/ratelimit_test.go
git commit -m "fix: use redis token bucket rate limiting"
```

### Task 7: Implement `formats` filtering and avoid duplicate large fields

**Files:**
- Modify: `internal/core/model/model.go`
- Modify: `internal/core/pipeline/pipeline.go`
- Test: `internal/api/server_test.go` or `internal/core/pipeline/pipeline_test.go`

- [ ] **Step 1: Add API test for formats**

Add a parse test that requests:

```json
{"urls":["https://example.com"],"formats":["markdown"]}
```

and asserts response result includes `markdown` but omits or empties `cleaned_html`/`html` fields according to the chosen model shape.

- [ ] **Step 2: Run test to verify failure**

```bash
go test ./internal/api -run Parse -count=1
```

Expected: FAIL because formats is ignored.

- [ ] **Step 3: Implement format filtering**

Add a helper in `internal/core/pipeline/pipeline.go`:

```go
func applyFormats(results []model.Result, formats []string) []model.Result {
	if len(formats) == 0 {
		return results
	}
	want := map[string]bool{}
	for _, f := range formats {
		want[strings.ToLower(strings.TrimSpace(f))] = true
	}
	for i := range results {
		if !want["markdown"] {
			results[i].Markdown = ""
			results[i].Content.Markdown = ""
		}
		if !want["html"] {
			results[i].CleanedHTML = ""
			results[i].Content.CleanedHTML = ""
		}
		if !want["json"] {
			results[i].Content.MainText = ""
		}
	}
	return results
}
```

Call it immediately before returning from `Search` and `Parse`.

- [ ] **Step 4: Verify**

```bash
go test ./internal/core/pipeline ./internal/api -count=1
go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/core/pipeline/pipeline.go internal/core/model/model.go internal/api/server_test.go
git commit -m "feat: honor response formats option"
```

---

## Phase 4: CI and deployment hardening

### Task 8: Add CI quality gates

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `Makefile`

- [ ] **Step 1: Add Makefile targets**

Add:

```make
fmt-check:
	@test -z "$$(gofmt -l .)"

tidy-check:
	@go mod tidy
	@git diff --exit-code go.mod go.sum
```

- [ ] **Step 2: Update CI**

Change `.github/workflows/ci.yml` steps to include:

```yaml
      - run: make fmt-check
      - run: go mod tidy
      - run: git diff --exit-code go.mod go.sum
      - run: go vet ./...
      - run: go test -race -coverprofile=coverage.out -count=1 ./...
      - run: go build ./cmd/fetchmark
      - name: Install govulncheck
        run: go install golang.org/x/vuln/cmd/govulncheck@latest
      - run: govulncheck ./...
```

- [ ] **Step 3: Verify locally**

```bash
make fmt-check
go mod tidy
git diff --exit-code go.mod go.sum
go vet ./...
go test -race -coverprofile=coverage.out -count=1 ./...
go build ./cmd/fetchmark
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml Makefile go.mod go.sum
git commit -m "ci: add formatting build and vulnerability gates"
```

### Task 9: Gate release workflow on verification

**Files:**
- Modify: `.github/workflows/release.yml`

- [ ] **Step 1: Add pre-publish verification steps**

Before Docker build/push steps, add:

```yaml
      - run: go vet ./...
      - run: go test -race -count=1 ./...
      - run: go build ./cmd/fetchmark
```

Ensure `actions/setup-go@v5` is present before these commands.

- [ ] **Step 2: Verify YAML shape**

```bash
git diff -- .github/workflows/release.yml
```

Expected: release workflow checks out code, sets up Go, verifies, then publishes.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "ci: verify before release publish"
```

### Task 10: Harden deployment defaults

**Files:**
- Modify: `.env.example`
- Modify: `deploy/docker-compose.yml`
- Modify: `deploy/docker-compose.external.yml`
- Modify: `README.md`

- [ ] **Step 1: Remove known credential defaults from compose**

Replace any `${FM_API_KEYS:-dev-key}` with `${FM_API_KEYS:?set FM_API_KEYS}`.

Replace any `${FM_ADMIN_API_KEYS:-dev-admin}` with `${FM_ADMIN_API_KEYS:-}`.

- [ ] **Step 2: Pin renderer image and require token when renderer profile is enabled**

In `deploy/docker-compose.yml`, replace:

```yaml
image: ghcr.io/browserless/chromium:latest
```

with a concrete version tag currently accepted by the project, for example:

```yaml
image: ghcr.io/browserless/chromium:v2.24.3
```

Replace renderer token default:

```yaml
TOKEN: "${FM_RENDERER_TOKEN:-}"
```

with:

```yaml
TOKEN: "${FM_RENDERER_TOKEN:?set FM_RENDERER_TOKEN when renderer is enabled}"
```

- [ ] **Step 3: Update docs**

In `README.md`, update quickstart to instruct:

```bash
openssl rand -hex 32
```

for API keys and mention admin keys are optional but must be explicitly set.

- [ ] **Step 4: Verify compose config**

```bash
FM_API_KEYS=test-key docker compose -f deploy/docker-compose.yml config >/tmp/fetchmark-compose.yml
```

Expected: succeeds for non-renderer profile. For renderer profile, verify missing `FM_RENDERER_TOKEN` fails with a clear message.

- [ ] **Step 5: Commit**

```bash
git add .env.example deploy/docker-compose.yml deploy/docker-compose.external.yml README.md
git commit -m "chore: harden deployment defaults"
```

---

## Phase 5: Memory growth and flaky test cleanup

### Task 11: Add bounds to per-host gates and robots cache

**Files:**
- Modify: `internal/adapters/fetcher/fetcher.go`
- Modify: `internal/adapters/robots/robots.go`
- Test: `internal/adapters/fetcher/fetcher_test.go`
- Test: `internal/adapters/robots/robots_test.go`

- [ ] **Step 1: Add host gate count helper and test**

Add unexported helper to fetcher:

```go
func (f *Fetcher) hostGateCount() int {
	f.hostsMu.Lock()
	defer f.hostsMu.Unlock()
	return len(f.hosts)
}
```

Add a test that calls `gateFor` for many hosts and asserts the count does not exceed a new constant:

```go
const maxHostGates = 1024
```

- [ ] **Step 2: Implement simple cap**

In `gateFor`, before adding a new host when `len(f.hosts) >= maxHostGates`, delete one arbitrary existing host that currently has an empty semaphore:

```go
for h, sem := range f.hosts {
	if len(sem) == 0 {
		delete(f.hosts, h)
		break
	}
}
```

If still at cap, reuse a shared overflow semaphore field on `Fetcher`.

- [ ] **Step 3: Add robots cache sweep**

In `robots.Checker`, add a method:

```go
func (c *Checker) sweepExpired(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, ent := range c.cache {
		if now.After(ent.expires) {
			delete(c.cache, key)
		}
	}
}
```

Call it opportunistically every N requests or before inserting a new cache entry.

- [ ] **Step 4: Verify**

```bash
go test ./internal/adapters/fetcher ./internal/adapters/robots -count=1
go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/adapters/fetcher/fetcher.go internal/adapters/fetcher/fetcher_test.go internal/adapters/robots/robots.go internal/adapters/robots/robots_test.go
git commit -m "fix: bound host and robots caches"
```

### Task 12: Replace short sleeps in fragile tests

**Files:**
- Modify: `internal/core/pipeline/renderer_test.go`
- Modify: `internal/adapters/searxng/multi_test.go`
- Modify: `internal/adapters/cache/cache_test.go`

- [ ] **Step 1: Replace goroutine ordering sleeps with channels**

Where tests use `time.Sleep(25 * time.Millisecond)` to wait for a goroutine to acquire a lock, add a channel such as:

```go
started := make(chan struct{})
release := make(chan struct{})
```

Signal `close(started)` when the fake dependency reaches the critical point, wait on `<-started`, then release with `close(release)`.

- [ ] **Step 2: Increase TTL/cooldown test margins where clock injection is not worth it**

For tests that verify expiry with `500ms` TTL and `600ms` sleep, use smaller TTL with a polling helper:

```go
requireEventually(t, time.Second, func() bool {
	// condition that must become true
})
```

Implement local helper:

```go
func requireEventually(t *testing.T, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition did not become true before timeout")
}
```

- [ ] **Step 3: Verify repeatedly**

```bash
go test ./internal/core/pipeline ./internal/adapters/searxng ./internal/adapters/cache -count=20
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/core/pipeline/renderer_test.go internal/adapters/searxng/multi_test.go internal/adapters/cache/cache_test.go
git commit -m "test: remove fragile timing sleeps"
```

---

## Final Verification

- [ ] Run full verification:

```bash
make fmt-check
go mod tidy
git diff --exit-code go.mod go.sum
go vet ./...
go test -race -coverprofile=coverage.out -count=1 ./...
go build ./cmd/fetchmark
```

- [ ] Run compose config verification:

```bash
FM_API_KEYS=test-key docker compose -f deploy/docker-compose.yml config >/tmp/fetchmark-compose.yml
```

- [ ] Review docs for changed behavior:

```bash
git diff -- README.md docs/openapi.yaml .env.example deploy/docker-compose.yml deploy/docker-compose.external.yml
```

- [ ] Commit final doc/OpenAPI corrections if any:

```bash
git add README.md docs/openapi.yaml .env.example deploy/docker-compose.yml deploy/docker-compose.external.yml
git commit -m "docs: align hardening behavior"
```

## Self-Review

- Spec coverage: covers config wiring, timeout/redirect behavior, search candidate caps, summarize caps, Redis rate limiting, formats behavior, CI/release gates, deployment defaults, memory growth, and flaky tests.
- Placeholder scan: no task is left as an undefined future item; each task includes concrete files, commands, and expected results.
- Type consistency: new config fields are referenced from `config.Config`; egress field is `ResponseHeaderTimeout`; rate limiter behavior remains behind `RateLimiter` and `redisAllow`.
