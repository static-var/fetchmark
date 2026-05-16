package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/adapters/extractor"
	"github.com/staticvar/fetchmark/internal/adapters/fetcher"
	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/search"
)

// jsAwareExtractor flags raw bodies containing "JS-SHIM" as
// js_required so the pipeline's auto-render branch engages; rendered
// bodies containing "RENDERED" extract cleanly.
type jsAwareExtractor struct{}

func (jsAwareExtractor) Extract(raw []byte, url string) (*model.Content, error) {
	s := string(raw)
	if strings.Contains(s, "JS-SHIM") {
		return &model.Content{URL: url, UnsupportedReason: extractor.ReasonJSRequired}, nil
	}
	return &model.Content{URL: url, Title: "Rendered", MainText: s, Markdown: "# " + s}, nil
}

type stubRenderer struct {
	body []byte
	err  error
	hits int
}

func (s *stubRenderer) Render(_ context.Context, _ string) ([]byte, error) {
	s.hits++
	if s.err != nil {
		return nil, s.err
	}
	return s.body, nil
}

func TestPipeline_RenderExplicit_UsesRenderer(t *testing.T) {
	rend := &stubRenderer{body: []byte("RENDERED ok")}
	p := &Pipeline{
		Fetcher: stubFetcher{resp: map[string]fetcher.Result{
			"https://spa.example/": {Status: 200, Body: []byte("JS-SHIM")},
		}},
		Extractor: jsAwareExtractor{},
		Cache:     cache.New(nil, 0),
		Renderer:  rend,
	}
	out := p.Parse(context.Background(), Options{URLs: []string{"https://spa.example/"}, Render: true})
	if len(out) != 1 {
		t.Fatalf("got %d results", len(out))
	}
	if rend.hits != 1 {
		t.Fatalf("renderer hits=%d want 1", rend.hits)
	}
	if out[0].Title != "Rendered" {
		t.Fatalf("render path did not produce rendered content: %+v", out[0])
	}
	if out[0].Unsupported == extractor.ReasonJSRequired {
		t.Fatalf("still flagged js_required after render: %+v", out[0])
	}
}

func TestPipeline_RenderExplicit_UsesRenderedCache(t *testing.T) {
	c := cache.New(nil, 0)
	u := "https://spa.example/a"
	canon, _ := cache.CanonicalURL(u)
	blob := []byte(`{"url":"` + canon + `","title":"CachedRender","text":"x","markdown":"#x"}`)
	if err := c.Set(context.Background(), cache.RenderedArtifactKey(canon), blob); err != nil {
		t.Fatal(err)
	}
	rend := &stubRenderer{body: []byte("should not be called")}
	p := &Pipeline{
		Fetcher:   stubFetcher{resp: map[string]fetcher.Result{}},
		Extractor: jsAwareExtractor{},
		Cache:     c,
		Renderer:  rend,
	}
	out := p.Parse(context.Background(), Options{URLs: []string{u}, Render: true})
	if len(out) != 1 || !out[0].FromCache || out[0].Title != "CachedRender" {
		t.Fatalf("rendered cache path failed: %+v", out)
	}
	if rend.hits != 0 {
		t.Fatalf("renderer should not be called on cache hit")
	}
}

func TestPipeline_RendererAuto_UpgradesJSRequired(t *testing.T) {
	rend := &stubRenderer{body: []byte("RENDERED body")}
	p := &Pipeline{
		Fetcher: stubFetcher{resp: map[string]fetcher.Result{
			"https://spa.example/b": {Status: 200, Body: []byte("JS-SHIM")},
		}},
		Extractor:    jsAwareExtractor{},
		Cache:        cache.New(nil, 0),
		Renderer:     rend,
		RendererAuto: true,
	}
	out := p.Parse(context.Background(), Options{URLs: []string{"https://spa.example/b"}})
	if rend.hits != 1 {
		t.Fatalf("auto-render did not trigger; hits=%d", rend.hits)
	}
	if out[0].Title != "Rendered" {
		t.Fatalf("expected rendered content to win, got %+v", out[0])
	}
}

func TestPipeline_RendererAutoOff_KeepsJSRequired(t *testing.T) {
	rend := &stubRenderer{body: []byte("RENDERED body")}
	p := &Pipeline{
		Fetcher: stubFetcher{resp: map[string]fetcher.Result{
			"https://spa.example/c": {Status: 200, Body: []byte("JS-SHIM")},
		}},
		Extractor: jsAwareExtractor{},
		Cache:     cache.New(nil, 0),
		Renderer:  rend, // auto disabled
	}
	out := p.Parse(context.Background(), Options{URLs: []string{"https://spa.example/c"}})
	if rend.hits != 0 {
		t.Fatalf("renderer should stay silent without auto/opts.Render; hits=%d", rend.hits)
	}
	if out[0].Unsupported != extractor.ReasonJSRequired {
		t.Fatalf("expected js_required to stick, got %+v", out[0])
	}
}

// TestPipeline_RendererAuto_RenderedKeyLock proves the rendered-key lock
// coalesces concurrent auto-render upgrades across pipelines (replicas).
// The instrumented cache exposes the moment the second caller is waiting
// on that lock, so the assertion is made under true contention without a
// scheduler-dependent sleep.
func TestPipeline_RendererAuto_RenderedKeyLock(t *testing.T) {
	url := "https://spa.example/shared"
	shared := newInstrumentedLockCache()
	rend := &slowRenderer{body: []byte("RENDERED body"), block: make(chan struct{}), entered: make(chan struct{})}

	newPipe := func() *Pipeline {
		return &Pipeline{
			Extractor:       jsAwareExtractor{},
			Cache:           shared,
			Renderer:        rend,
			RendererAuto:    true,
			RendererTimeout: 5 * time.Second,
		}
	}
	p1, p2 := newPipe(), newPipe()

	r1 := &model.SearchResult{URL: url, Unsupported: extractor.ReasonJSRequired}
	r2 := &model.SearchResult{URL: url, Unsupported: extractor.ReasonJSRequired}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = p1.tryAutoRender(context.Background(), Options{Timeout: time.Second}, r1, false)
	}()
	waitForReceive(t, rend.entered, time.Second, "first renderer invocation")

	go func() {
		defer wg.Done()
		_, _ = p2.tryAutoRender(context.Background(), Options{Timeout: time.Second}, r2, false)
	}()
	waitForReceive(t, shared.waiterBlocked, time.Second, "second caller to block on rendered-key lock")

	if got := rend.hits.Load(); got != 1 {
		t.Fatalf("second caller rendered while waiting on rendered-key lock; hits=%d", got)
	}

	close(rend.block)
	waitForWaitGroup(t, &wg, time.Second, "rendered-key lock callers")

	if got := rend.hits.Load(); got != 1 {
		t.Fatalf("rendered-key lock failed to coalesce; renderer hits=%d, want 1", got)
	}
	if r1.Title != "Rendered" || r2.Title != "Rendered" {
		t.Fatalf("both callers must observe rendered content; r1.Title=%q r2.Title=%q", r1.Title, r2.Title)
	}
}

func TestPipeline_RendererAuto_RedisBackedRenderedKeyLock(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	c1 := cache.New(rdb, time.Minute)
	t.Cleanup(c1.Close)
	c2 := cache.New(rdb, time.Minute)
	t.Cleanup(c2.Close)
	url := "https://spa.example/redis-shared"
	renderedKey := cache.RenderedArtifactKey(url)
	renderLockKey := "fm:lock:" + renderedKey
	rend := &slowRenderer{body: []byte("RENDERED body"), block: make(chan struct{}), entered: make(chan struct{})}
	newPipe := func(c *cache.Cache) *Pipeline {
		return &Pipeline{
			Extractor:       jsAwareExtractor{},
			Cache:           c,
			Renderer:        rend,
			RendererAuto:    true,
			RendererTimeout: 5 * time.Second,
		}
	}
	p1, p2 := newPipe(c1), newPipe(c2)
	r1 := &model.SearchResult{URL: url, Unsupported: extractor.ReasonJSRequired}
	r2 := &model.SearchResult{URL: url, Unsupported: extractor.ReasonJSRequired}

	type result struct{ err error }
	results := make(chan result, 2)
	go func() {
		_, err := p1.tryAutoRender(context.Background(), Options{Timeout: time.Second}, r1, false)
		results <- result{err: err}
	}()
	waitForReceive(t, rend.entered, time.Second, "first renderer invocation")
	requireEventually(t, time.Second, func() bool {
		n, err := rdb.Exists(context.Background(), renderLockKey).Result()
		return err == nil && n == 1
	})

	go func() {
		_, err := p2.tryAutoRender(context.Background(), Options{Timeout: time.Second}, r2, false)
		results <- result{err: err}
	}()
	assertNoReceive(t, results, 50*time.Millisecond, "rendered-key caller returned before render release")

	close(rend.block)
	for i := 0; i < 2; i++ {
		res := waitForReceive(t, results, time.Second, "redis rendered-key caller result")
		if res.err != nil {
			t.Fatalf("tryAutoRender: %v", res.err)
		}
	}
	if got := rend.hits.Load(); got != 1 {
		t.Fatalf("redis rendered-key lock failed to coalesce; renderer hits=%d, want 1", got)
	}
	if r1.Title != "Rendered" || r2.Title != "Rendered" {
		t.Fatalf("both callers must observe rendered content; r1.Title=%q r2.Title=%q", r1.Title, r2.Title)
	}
}

func requireEventually(t *testing.T, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ok() {
		return
	}
	t.Fatalf("condition not met within %s", timeout)
}

func waitForReceive[T any](t *testing.T, ch <-chan T, timeout time.Duration, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func assertNoReceive[T any](t *testing.T, ch <-chan T, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal(msg)
	case <-time.After(timeout):
	}
}

func waitForWaitGroup(t *testing.T, wg *sync.WaitGroup, timeout time.Duration, what string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	waitForReceive(t, done, timeout, what)
}

type instrumentedLockCache struct {
	mu            sync.Mutex
	cond          *sync.Cond
	mem           map[string][]byte
	locked        map[string]bool
	waiterBlocked chan struct{}
	waiterOnce    sync.Once
}

func newInstrumentedLockCache() *instrumentedLockCache {
	c := &instrumentedLockCache{
		mem:           make(map[string][]byte),
		locked:        make(map[string]bool),
		waiterBlocked: make(chan struct{}),
	}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *instrumentedLockCache) Get(_ context.Context, key string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v := c.mem[key]; v != nil {
		return append([]byte(nil), v...), nil
	}
	return nil, nil
}

func (c *instrumentedLockCache) Set(_ context.Context, key string, val []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mem[key] = append([]byte(nil), val...)
	return nil
}

func (c *instrumentedLockCache) Do(_ string, fn func() (any, error)) (any, error, bool) {
	v, err := fn()
	return v, err, false
}

func (c *instrumentedLockCache) WithLock(ctx context.Context, key string, _ cache.LockOptions, fn func(context.Context) ([]byte, error)) ([]byte, error) {
	c.mu.Lock()
	for c.locked[key] {
		c.waiterOnce.Do(func() { close(c.waiterBlocked) })
		c.cond.Wait()
	}
	c.locked[key] = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.locked, key)
		c.cond.Broadcast()
		c.mu.Unlock()
	}()

	return fn(ctx)
}

type slowRenderer struct {
	body        []byte
	block       chan struct{}
	entered     chan struct{}
	enteredOnce sync.Once
	hits        atomic.Int64
}

func (s *slowRenderer) Render(ctx context.Context, _ string) ([]byte, error) {
	s.hits.Add(1)
	if s.entered != nil {
		s.enteredOnce.Do(func() { close(s.entered) })
	}
	select {
	case <-s.block:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.body, nil
}

func TestPipeline_RenderExplicit_ErrorFlagsResult(t *testing.T) {
	rend := &stubRenderer{err: errors.New("boom")}
	p := &Pipeline{
		Fetcher:   stubFetcher{resp: map[string]fetcher.Result{}},
		Extractor: jsAwareExtractor{},
		Cache:     cache.New(nil, 0),
		Renderer:  rend,
	}
	out := p.Parse(context.Background(), Options{URLs: []string{"https://spa.example/d"}, Render: true})
	if len(out) != 1 || out[0].Unsupported != "render_failed" {
		t.Fatalf("expected render_failed marker; got %+v", out)
	}
}

// Sanity: exercising jsAwareExtractor with a search.Hit surface mirrors
// real search flow; nothing to assert beyond no-crash.
func TestPipeline_RenderExplicit_SearchFlow(t *testing.T) {
	rend := &stubRenderer{body: []byte("RENDERED news")}
	p := &Pipeline{
		Searcher: stubSearcher{hits: []search.Hit{{URL: "https://spa.example/n"}}},
		Fetcher: stubFetcher{resp: map[string]fetcher.Result{
			"https://spa.example/n": {Status: 200, Body: []byte("JS-SHIM")},
		}},
		Extractor: jsAwareExtractor{},
		Cache:     cache.New(nil, 0),
		Renderer:  rend,
	}
	out, err := p.Search(context.Background(), Options{Query: "news", Render: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].Title != "Rendered" {
		t.Fatalf("search+render path broken: %+v", out)
	}
}

func TestPipeline_RenderExplicit_EgressRejected(t *testing.T) {
	rend := &stubRenderer{body: []byte("RENDERED oops")}
	blocked := errors.New("blocked")
	p := &Pipeline{
		Fetcher:        stubFetcher{resp: map[string]fetcher.Result{}},
		Extractor:      jsAwareExtractor{},
		Cache:          cache.New(nil, 0),
		Renderer:       rend,
		EgressValidate: func(_ context.Context, _ string) error { return blocked },
	}
	out := p.Parse(context.Background(), Options{URLs: []string{"http://127.0.0.1/"}, Render: true})
	if len(out) != 1 || out[0].Unsupported != "egress_reject" {
		t.Fatalf("expected egress_reject; got %+v", out)
	}
	if rend.hits != 0 {
		t.Fatalf("renderer must not be invoked when egress rejects; hits=%d", rend.hits)
	}
}

func TestPipeline_Render_JSRequiredResult_NotCached(t *testing.T) {
	// Renderer returns a body that the extractor ALSO marks as
	// js_required. The rendered artifact must not be cached, or
	// subsequent explicit renders would be served a useless placeholder.
	rend := &stubRenderer{body: []byte("JS-SHIM still")}
	p := &Pipeline{
		Fetcher:   stubFetcher{resp: map[string]fetcher.Result{}},
		Extractor: jsAwareExtractor{},
		Cache:     cache.New(nil, 0),
		Renderer:  rend,
	}
	_ = p.Parse(context.Background(), Options{URLs: []string{"https://spa.example/js"}, Render: true})
	if rend.hits != 1 {
		t.Fatalf("first render should hit once; got %d", rend.hits)
	}
	_ = p.Parse(context.Background(), Options{URLs: []string{"https://spa.example/js"}, Render: true})
	if rend.hits != 2 {
		t.Fatalf("js_required rendered blob must not be cached; expected hits=2 got %d", rend.hits)
	}
}

// TestPipeline_RendererAuto_CacheHitOverwritesPlaceholder proves the
// pre-lock rendered-cache-hit branch of tryAutoRender replaces a stale
// js_required placeholder with the rendered title/markdown. Regression
// from GPT-5.4 round-3 review: applyContent is fill-only and was being
// called on an r that already carried js_required placeholder fields,
// so cache hits silently kept the placeholder.
func TestPipeline_RendererAuto_CacheHitOverwritesPlaceholder(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	c := cache.New(rdb, time.Minute)
	url := "https://spa.example/prewarmed"

	// Pre-warm the rendered cache as if a prior request already
	// populated it. No renderer is wired so the only way r can come
	// back with the rendered Title is via the cache-hit branch.
	blob, _ := json.Marshal(model.Content{Title: "Rendered", Markdown: "rendered md"})
	if err := c.Set(context.Background(), cache.RenderedArtifactKey(url), blob); err != nil {
		t.Fatalf("cache set: %v", err)
	}

	p := &Pipeline{Extractor: jsAwareExtractor{}, Cache: c, RendererAuto: true}
	r := &model.SearchResult{
		URL:         url,
		Title:       "Loading…",
		Unsupported: extractor.ReasonJSRequired,
	}
	if _, err := p.tryAutoRender(context.Background(), Options{Timeout: time.Second}, r, false); err != nil {
		t.Fatalf("tryAutoRender: %v", err)
	}
	if r.Title != "Rendered" {
		t.Fatalf("cache-hit branch must overwrite placeholder Title; got %q", r.Title)
	}
	if r.Unsupported != "" {
		t.Fatalf("cache-hit branch must clear js_required placeholder; got %q", r.Unsupported)
	}
	if !r.FromCache {
		t.Fatal("FromCache should be set on rendered-cache hit")
	}
}
