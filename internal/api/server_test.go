package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/config"
	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/pipeline"
)

type fakePipeline struct {
	searchCalls int
	parseCalls  int
	lastOpts    pipeline.Options
	results     []model.SearchResult
	err         error
}

func (f *fakePipeline) Search(_ context.Context, o pipeline.Options) ([]model.SearchResult, error) {
	f.searchCalls++
	f.lastOpts = o
	return f.results, f.err
}
func (f *fakePipeline) Parse(_ context.Context, o pipeline.Options) []model.SearchResult {
	f.parseCalls++
	f.lastOpts = o
	return f.results
}

func newTestRouter(ready func() error) (http.Handler, *fakePipeline) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Config{
		APIKeys:       []string{"k1"},
		AdminAPIKeys:  []string{"admin1"},
		MaxResults:    10,
		ResultsCap:    50,
		RespectRobots: true,
	}
	p := &fakePipeline{results: []model.SearchResult{{URL: "https://x/y", Title: "t"}}}
	return NewRouter(Deps{Log: log, Config: cfg, Pipeline: p, ReadyCheck: ready}), p
}

func TestHealthz(t *testing.T) {
	r, _ := newTestRouter(nil)
	req := httptest.NewRequest("GET", "/healthz", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestReadyz_Unready(t *testing.T) {
	r, _ := newTestRouter(func() error { return errors.New("redis down") })
	req := httptest.NewRequest("GET", "/readyz", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "redis down") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestV1RequiresAPIKey(t *testing.T) {
	r, _ := newTestRouter(nil)
	req := httptest.NewRequest("POST", "/v1/search", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestSearch_MissingQueryIs400(t *testing.T) {
	r, _ := newTestRouter(nil)
	req := httptest.NewRequest("POST", "/v1/search", strings.NewReader(`{}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestSearch_Success(t *testing.T) {
	r, p := newTestRouter(nil)
	req := httptest.NewRequest("POST", "/v1/search", strings.NewReader(`{"query":"go"}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if p.searchCalls != 1 {
		t.Fatalf("search calls = %d", p.searchCalls)
	}
	if p.lastOpts.RespectRobots != true {
		t.Fatal("default respect_robots should be true")
	}
}

func TestSearch_ProxyRequiresAdmin(t *testing.T) {
	r, _ := newTestRouter(nil)
	body := strings.NewReader(`{"query":"go","proxy_url":"http://proxy:8080"}`)
	req := httptest.NewRequest("POST", "/v1/search", body)
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestSearch_ProxyAllowedForAdmin(t *testing.T) {
	r, p := newTestRouter(nil)
	body := strings.NewReader(`{"query":"go","proxy_url":"http://proxy:8080","respect_robots":false}`)
	req := httptest.NewRequest("POST", "/v1/search", body)
	req.Header.Set("X-API-Key", "admin1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if p.lastOpts.ProxyURL == "" || p.lastOpts.RespectRobots != false {
		t.Fatalf("admin overrides not applied: %+v", p.lastOpts)
	}
}

func TestSummarize_Returns501WithSchema(t *testing.T) {
	r, _ := newTestRouter(nil)
	req := httptest.NewRequest("POST", "/v1/summarize", strings.NewReader("{}"))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "schema") {
		t.Fatalf("body missing schema: %s", rec.Body.String())
	}
}

func TestParse_Success(t *testing.T) {
	r, p := newTestRouter(nil)
	body := strings.NewReader(`{"urls":["https://x/y"]}`)
	req := httptest.NewRequest("POST", "/v1/parse", body)
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || p.parseCalls != 1 {
		t.Fatalf("status=%d calls=%d body=%s", rec.Code, p.parseCalls, rec.Body.String())
	}
}

// Admin-only overrides on /v1/parse must 403 for non-admin keys and
// propagate through to pipeline.Options when the caller is admin.
// Mirrors TestSearch_Proxy* but for the parse route; catches regressions
// where buildOptions is wired only into the search handler.
func TestParse_AdminOverridesTable(t *testing.T) {
	type tc struct {
		name   string
		apiKey string
		body   string
		want   int
		assert func(t *testing.T, p *fakePipeline)
	}
	cases := []tc{
		{
			name:   "non_admin_proxy_rejected",
			apiKey: "k1",
			body:   `{"urls":["https://x/y"],"proxy_url":"http://proxy:8080"}`,
			want:   http.StatusForbidden,
		},
		{
			name:   "non_admin_robots_false_rejected",
			apiKey: "k1",
			body:   `{"urls":["https://x/y"],"respect_robots":false}`,
			want:   http.StatusForbidden,
		},
		{
			name:   "admin_proxy_accepted",
			apiKey: "admin1",
			body:   `{"urls":["https://x/y"],"proxy_url":"http://proxy:8080"}`,
			want:   http.StatusOK,
			assert: func(t *testing.T, p *fakePipeline) {
				if p.lastOpts.ProxyURL != "http://proxy:8080" {
					t.Fatalf("proxy not propagated: %q", p.lastOpts.ProxyURL)
				}
			},
		},
		{
			name:   "admin_robots_false_accepted",
			apiKey: "admin1",
			body:   `{"urls":["https://x/y"],"respect_robots":false}`,
			want:   http.StatusOK,
			assert: func(t *testing.T, p *fakePipeline) {
				if p.lastOpts.RespectRobots {
					t.Fatalf("respect_robots override not propagated")
				}
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, p := newTestRouter(nil)
			req := httptest.NewRequest("POST", "/v1/parse", strings.NewReader(c.body))
			req.Header.Set("X-API-Key", c.apiKey)
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != c.want {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, c.want, rec.Body.String())
			}
			if c.assert != nil {
				c.assert(t, p)
			}
		})
	}
}
