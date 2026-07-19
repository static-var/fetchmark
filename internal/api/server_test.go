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
	"time"

	"github.com/staticvar/fetchmark/internal/config"
	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/pipeline"
	"github.com/staticvar/fetchmark/internal/core/search"
)

type fakePipeline struct {
	searchCalls      int
	parseCalls       int
	lastOpts         pipeline.Options
	results          []model.SearchResult
	err              error
	parseDeadline    time.Time
	parseHasDeadline bool
}

type detailedFakePipeline struct {
	*fakePipeline
	output pipeline.SearchOutput
	err    error
}

func (f *detailedFakePipeline) SearchDetailed(context.Context, pipeline.Options) (pipeline.SearchOutput, error) {
	return f.output, f.err
}

func (f *fakePipeline) Search(_ context.Context, o pipeline.Options) ([]model.SearchResult, error) {
	f.searchCalls++
	f.lastOpts = o
	return f.results, f.err
}
func (f *fakePipeline) Parse(ctx context.Context, o pipeline.Options) []model.SearchResult {
	f.parseDeadline, f.parseHasDeadline = ctx.Deadline()
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

func TestRouterExposesCanonicalBuildSHA256(t *testing.T) {
	const buildSHA256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	router := NewRouter(Deps{
		Log:         log,
		Config:      config.Config{APIKeys: []string{"k1"}, ResultsCap: 50},
		Pipeline:    &fakePipeline{},
		BuildSHA256: strings.ToUpper(buildSHA256),
	})

	for _, test := range []struct {
		name   string
		apiKey string
	}{
		{name: "success", apiKey: "k1"},
		{name: "authentication failure"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(`{"query":"birds"}`))
			if test.apiKey != "" {
				request.Header.Set("X-API-Key", test.apiKey)
			}
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if got := recorder.Header().Get("X-Fetchmark-Build-SHA256"); got != buildSHA256 {
				t.Fatalf("build header = %q, want %q", got, buildSHA256)
			}
		})
	}

	healthRequest := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	healthRecorder := httptest.NewRecorder()
	router.ServeHTTP(healthRecorder, healthRequest)
	if got := healthRecorder.Header().Get("X-Fetchmark-Build-SHA256"); got != "" {
		t.Fatalf("health build header = %q, want omitted outside native search", got)
	}
}

func TestRouterOmitsInvalidBuildSHA256(t *testing.T) {
	router := NewRouter(Deps{
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		Config:      config.Config{APIKeys: []string{"k1"}, ResultsCap: 50},
		Pipeline:    &fakePipeline{},
		BuildSHA256: "not-a-digest",
	})
	request := httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(`{"query":"birds"}`))
	request.Header.Set("X-API-Key", "k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if got := recorder.Header().Get("X-Fetchmark-Build-SHA256"); got != "" {
		t.Fatalf("invalid build header = %q, want omitted", got)
	}
}

func TestParseResponseHonorsSerializedByteBudget(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe := &fakePipeline{results: []model.SearchResult{{
		URL:      "https://example.com",
		Markdown: strings.Repeat("x", 1024),
	}}}
	router := NewRouter(Deps{
		Log: log,
		Config: config.Config{
			APIKeys:               []string{"k1"},
			ResultsCap:            50,
			MaxRequestOutputBytes: 128,
		},
		Pipeline: pipe,
	})
	req := httptest.NewRequest("POST", "/v1/parse", strings.NewReader(`{"urls":["https://example.com"]}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusInsufficientStorage {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if int64(rec.Body.Len()) > 128 {
		t.Fatalf("response bytes=%d, limit=128", rec.Body.Len())
	}
}

func TestNativeSearchResponseExposesTypedDiscoveryProvenance(t *testing.T) {
	router, pipe := newTestRouter(nil)
	pipe.results[0].Provenance = []model.DiscoveryProvenance{{Provider: "searxng", Lane: "searxng-open", Variant: "original"}}
	req := httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(`{"query":"birds"}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"provenance":[{"provider":"searxng","lane":"searxng-open","variant":"original"}]`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestNativeSearchResponseExposesBoundedDiscoveryReport(t *testing.T) {
	report := search.DiscoveryReport{
		Status: search.BatchPartial,
		Lanes: []search.DiscoveryLaneReport{{
			Provider: "wiby", Lane: "wiby-general", Variant: "original",
			Status: search.BatchPartial, CandidateCount: 2, DurationMS: 25,
			Diagnostics: []search.DiscoveryDiagnostic{{Source: "official_api", Reason: "malformed_results"}},
		}},
	}
	base := &fakePipeline{results: []model.SearchResult{{URL: "https://example.com"}}}
	router := NewRouter(Deps{
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Config:   config.Config{APIKeys: []string{"k1"}, ResultsCap: 50, MaxRequestOutputBytes: 1 << 20},
		Pipeline: &detailedFakePipeline{fakePipeline: base, output: pipeline.SearchOutput{Results: base.results, Discovery: report}},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(`{"query":"birds"}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"discovery":{"status":"partial","lanes":[{"provider":"wiby"`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestNativeSearchFailureRetainsDiscoveryReport(t *testing.T) {
	report := search.DiscoveryReport{
		Status: search.BatchFailed,
		Lanes: []search.DiscoveryLaneReport{{
			Provider: "mwmbl", Lane: "mwmbl-general", Variant: "original", Status: search.BatchFailed,
			DurationMS: 8000, Diagnostics: []search.DiscoveryDiagnostic{{Reason: "timeout", Retryable: true}},
		}},
	}
	router := NewRouter(Deps{
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Config:   config.Config{APIKeys: []string{"k1"}, ResultsCap: 50, MaxRequestOutputBytes: 1 << 20},
		Pipeline: &detailedFakePipeline{fakePipeline: &fakePipeline{}, output: pipeline.SearchOutput{Discovery: report}, err: context.DeadlineExceeded},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(`{"query":"birds"}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway || !strings.Contains(rec.Body.String(), `"error":"search_failed"`) || !strings.Contains(rec.Body.String(), `"discovery":{"status":"failed"`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestNativeSearchFailureRetainsPartialDiscoveryReport(t *testing.T) {
	report := search.DiscoveryReport{Status: search.BatchPartial, Lanes: []search.DiscoveryLaneReport{
		{Provider: "wiby", Lane: "wiby-general", Variant: "original", Status: search.BatchHealthy, CandidateCount: 1},
		{Provider: "mwmbl", Lane: "mwmbl-general", Variant: "original", Status: search.BatchFailed, Diagnostics: []search.DiscoveryDiagnostic{{Reason: "canceled"}}},
	}}
	router := NewRouter(Deps{
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Config:   config.Config{APIKeys: []string{"k1"}, ResultsCap: 50, MaxRequestOutputBytes: 1 << 20},
		Pipeline: &detailedFakePipeline{fakePipeline: &fakePipeline{}, output: pipeline.SearchOutput{Discovery: report}, err: context.DeadlineExceeded},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(`{"query":"birds"}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway || !strings.Contains(rec.Body.String(), `"discovery":{"status":"partial"`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestNativeSearchFailsClosedOnInvalidDetailedRunnerReport(t *testing.T) {
	tests := map[string]search.DiscoveryReport{
		"raw identity": {
			Status: search.BatchFailed,
			Lanes: []search.DiscoveryLaneReport{{
				Provider: "http://private.internal", Lane: "raw error text", Variant: "original",
				Status: search.BatchFailed, Diagnostics: []search.DiscoveryDiagnostic{{Reason: "dial tcp 10.0.0.1:8080"}},
			}},
		},
		"too many diagnostics": {
			Status: search.BatchDegradedEmpty,
			Lanes: []search.DiscoveryLaneReport{{
				Provider: "wiby", Lane: "wiby-general", Variant: "original", Status: search.BatchDegradedEmpty,
				Diagnostics: make([]search.DiscoveryDiagnostic, search.MaxDiscoveryDiagnosticsPerLane+1),
			}},
		},
		"inconsistent aggregate": {
			Status: search.BatchHealthy,
			Lanes: []search.DiscoveryLaneReport{{
				Provider: "wiby", Lane: "wiby-general", Variant: "original", Status: search.BatchFailed,
			}},
		},
	}
	for name, report := range tests {
		t.Run(name, func(t *testing.T) {
			base := &fakePipeline{results: []model.SearchResult{{URL: "https://example.com"}}}
			router := NewRouter(Deps{
				Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
				Config:   config.Config{APIKeys: []string{"k1"}, ResultsCap: 50, MaxRequestOutputBytes: 1 << 20},
				Pipeline: &detailedFakePipeline{fakePipeline: base, output: pipeline.SearchOutput{Results: base.results, Discovery: report}},
			})
			req := httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(`{"query":"birds"}`))
			req.Header.Set("X-API-Key", "k1")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), `"discovery"`) || strings.Contains(rec.Body.String(), "private.internal") || strings.Contains(rec.Body.String(), "dial tcp") {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestNativeSearchErrorFailsClosedOnInvalidDetailedRunnerReport(t *testing.T) {
	report := search.DiscoveryReport{Status: search.BatchFailed, Lanes: []search.DiscoveryLaneReport{{
		Provider: "http://private.internal", Lane: "raw error text", Variant: "original", Status: search.BatchFailed,
		Diagnostics: []search.DiscoveryDiagnostic{{Reason: "dial tcp 10.0.0.1:8080"}},
	}}}
	router := NewRouter(Deps{
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Config:   config.Config{APIKeys: []string{"k1"}, ResultsCap: 50, MaxRequestOutputBytes: 1 << 20},
		Pipeline: &detailedFakePipeline{fakePipeline: &fakePipeline{}, output: pipeline.SearchOutput{Discovery: report}, err: context.DeadlineExceeded},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(`{"query":"birds"}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway || strings.Contains(rec.Body.String(), `"discovery"`) || strings.Contains(rec.Body.String(), "private.internal") || strings.Contains(rec.Body.String(), "dial tcp") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestNativeSearchAttributesWibyResultsWithLinkHeader(t *testing.T) {
	router, pipe := newTestRouter(nil)
	pipe.results[0].Provenance = []model.DiscoveryProvenance{{Provider: "wiby", Lane: "wiby-general", Variant: "original"}}
	req := httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(`{"query":"birds"}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Header().Get("Link") != wibyAttributionLink {
		t.Fatalf("status=%d link=%q body=%s", rec.Code, rec.Header().Get("Link"), rec.Body.String())
	}
}

func TestNativeSearchAttributesStackExchangeResultsWithLinkHeader(t *testing.T) {
	router, pipe := newTestRouter(nil)
	pipe.results[0].URL = "https://stackoverflow.com/questions/1"
	pipe.results[0].Provenance = []model.DiscoveryProvenance{{Provider: "stackexchange", Lane: "stackexchange-developer", Variant: "original"}}
	req := httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(`{"query":"birds"}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Header().Get("Link") != stackExchangeAttributionLink {
		t.Fatalf("status=%d link=%q body=%s", rec.Code, rec.Header().Get("Link"), rec.Body.String())
	}
}

func TestNativeSearchAttributesArxivResultsWithLinkHeader(t *testing.T) {
	router, pipe := newTestRouter(nil)
	pipe.results[0].URL = "https://arxiv.org/abs/2401.01234"
	pipe.results[0].Provenance = []model.DiscoveryProvenance{{Provider: "arxiv", Lane: "arxiv-research", Variant: "original"}}
	req := httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(`{"query":"retrieval research"}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Header().Get("Link") != arxivAttributionLink {
		t.Fatalf("status=%d link=%q body=%s", rec.Code, rec.Header().Get("Link"), rec.Body.String())
	}
}

func TestNativeSearchResponseBudgetDoesNotAttributeWibyError(t *testing.T) {
	pipe := &fakePipeline{results: []model.SearchResult{{
		URL: "https://example.com", Markdown: strings.Repeat("x", 1024),
		Provenance: []model.DiscoveryProvenance{{Provider: "wiby", Lane: "wiby-general", Variant: "original"}},
	}}}
	router := NewRouter(Deps{
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Config: config.Config{
			APIKeys: []string{"k1"}, ResultsCap: 50, MaxRequestOutputBytes: 128,
		},
		Pipeline: pipe,
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(`{"query":"birds"}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusInsufficientStorage || rec.Header().Get("Link") != "" {
		t.Fatalf("status=%d unexpected link=%q body=%s", rec.Code, rec.Header().Get("Link"), rec.Body.String())
	}
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

func TestDashboardUsesConfiguredVersion(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Config{
		DashboardUser:     "u",
		DashboardPassword: "p",
		RedisURL:          "redis://:secret@redis:6379/0",
	}
	r := NewRouter(Deps{Log: log, Config: cfg, Version: "test-version"})
	req := httptest.NewRequest("GET", "/dashboard", nil)
	req.SetBasicAuth("u", "p")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "version test-version") {
		t.Fatalf("dashboard did not render configured version: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "in-memory fallback") {
		t.Fatalf("dashboard did not show in-memory cache mode: %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("dashboard leaked redis secret while in fallback mode: %s", rec.Body.String())
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

func TestSearch_UnsupportedEngineControlIsTypedClientError(t *testing.T) {
	r, p := newTestRouter(nil)
	p.err = &search.UnsupportedControlError{Control: "engines", Reason: "no enabled SearXNG discovery source"}
	req := httptest.NewRequest("POST", "/v1/search", strings.NewReader(`{"query":"birds","engines":["mwmbl"]}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !strings.Contains(body, `"error":"unsupported_control"`) || !strings.Contains(body, `"control":"engines"`) {
		t.Fatalf("body = %s", body)
	}
}

func TestSearch_PropagatesSearchControls(t *testing.T) {
	r, p := newTestRouter(nil)
	body := strings.NewReader(`{"query":"birds","categories":["general","news"],"language":"en","time_range":"year","safesearch":1,"include_domains":["a.example"],"exclude_domains":["b.example"],"exact_match":true,"search_depth":"advanced","chunks_per_source":2}`)
	req := httptest.NewRequest("POST", "/v1/search", body)
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if len(p.lastOpts.Categories) != 2 || p.lastOpts.Categories[0] != "general" || p.lastOpts.Categories[1] != "news" || p.lastOpts.Language != "en" || p.lastOpts.TimeRange != "year" {
		t.Fatalf("search controls not propagated: %+v", p.lastOpts)
	}
	if p.lastOpts.SafeSearch == nil || *p.lastOpts.SafeSearch != 1 || len(p.lastOpts.IncludeDomains) != 1 || p.lastOpts.IncludeDomains[0] != "a.example" || len(p.lastOpts.ExcludeDomains) != 1 || p.lastOpts.ExcludeDomains[0] != "b.example" || !p.lastOpts.ExactMatch || p.lastOpts.SearchDepth != "advanced" || p.lastOpts.ChunksPerSource != 2 {
		t.Fatalf("advanced controls not propagated: %+v", p.lastOpts)
	}
	if p.lastOpts.CandidateCap != 50 {
		t.Fatalf("advanced candidate cap = %d, want 50", p.lastOpts.CandidateCap)
	}
}

func TestSearch_InvalidSearchControlsReturn400(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "unsupported time range", body: `{"query":"birds","time_range":"hour"}`},
		{name: "unsupported safesearch", body: `{"query":"birds","safesearch":3}`},
		{name: "unsupported search depth", body: `{"query":"birds","search_depth":"deep"}`},
		{name: "unsupported chunks per source", body: `{"query":"birds","chunks_per_source":4}`},
		{name: "malformed include domain", body: `{"query":"birds","include_domains":["%"]}`},
		{name: "malformed exclude domain", body: `{"query":"birds","exclude_domains":["https://"]}`},
		{name: "unsupported format", body: `{"query":"birds","formats":["markdown","pdf"]}`},
		{name: "negative timeout", body: `{"query":"birds","timeout_ms":-1}`},
		{name: "timeout over cap", body: `{"query":"birds","timeout_ms":60001}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := newTestRouter(nil)
			req := httptest.NewRequest("POST", "/v1/search", strings.NewReader(tc.body))
			req.Header.Set("X-API-Key", "k1")
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestParse_InvalidRequestValuesReturn400(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "unsupported format", body: `{"urls":["https://x/y"],"formats":["pdf"]}`},
		{name: "invalid url", body: `{"urls":["not-a-url"]}`},
		{name: "mixed invalid url", body: `{"urls":["https://x/y","file:///etc/passwd"]}`},
		{name: "negative timeout", body: `{"urls":["https://x/y"],"timeout_ms":-1}`},
		{name: "timeout over cap", body: `{"urls":["https://x/y"],"timeout_ms":60001}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := newTestRouter(nil)
			req := httptest.NewRequest("POST", "/v1/parse", strings.NewReader(tc.body))
			req.Header.Set("X-API-Key", "k1")
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestSearch_NormalizesSearchControlValues(t *testing.T) {
	r, p := newTestRouter(nil)
	req := httptest.NewRequest("POST", "/v1/search", strings.NewReader(`{"query":"birds","time_range":" Week ","search_depth":" Advanced "}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if p.lastOpts.TimeRange != "week" {
		t.Fatalf("time range = %q, want normalized week", p.lastOpts.TimeRange)
	}
	if p.lastOpts.SearchDepth != "advanced" {
		t.Fatalf("search depth = %q, want normalized advanced", p.lastOpts.SearchDepth)
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
	if p.lastOpts.MaxResults != 10 {
		t.Fatalf("max results = %d, want 10", p.lastOpts.MaxResults)
	}
	if p.lastOpts.CandidateCap != 30 {
		t.Fatalf("candidate cap = %d, want 30", p.lastOpts.CandidateCap)
	}
}

func TestSearch_CandidateCapIsClampedToResultsCap(t *testing.T) {
	r, p := newTestRouter(nil)
	req := httptest.NewRequest("POST", "/v1/search", strings.NewReader(`{"query":"go","max_results":20}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if p.lastOpts.MaxResults != 20 {
		t.Fatalf("max results = %d, want 20", p.lastOpts.MaxResults)
	}
	if p.lastOpts.CandidateCap != 50 {
		t.Fatalf("candidate cap = %d, want 50", p.lastOpts.CandidateCap)
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

func TestSummarize_NotConfiguredReturns503(t *testing.T) {
	// With no summarizer registry configured the endpoint must fail
	// fast with 503 so callers can surface a clear configuration hint
	// instead of timing out against nil providers.
	r, _ := newTestRouter(nil)
	req := httptest.NewRequest("POST", "/v1/summarize", strings.NewReader(`{"url":"https://x/y"}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "summarize_not_configured") {
		t.Fatalf("body missing hint: %s", rec.Body.String())
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
