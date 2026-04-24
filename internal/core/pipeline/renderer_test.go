package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

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
