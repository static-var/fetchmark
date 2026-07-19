package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/config"
	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/pipeline"
	"github.com/staticvar/fetchmark/internal/core/search"
)

func TestCompatibilityRateLimitsKeepVendorErrorShapes(t *testing.T) {
	tests := []struct {
		name, method, path, body, header, expected string
	}{
		{"tavily", http.MethodPost, "/compat/tavily/search", `{"query":"x"}`, "Authorization", `"detail"`},
		{"exa", http.MethodPost, "/compat/exa/search", `{"query":"x"}`, "x-api-key", `{"error":"rate limit exceeded"}`},
		{"brave", http.MethodGet, "/compat/brave/res/v1/web/search?q=x", "", "X-Subscription-Token", `"code":"RATE_LIMITED"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			router := compatibilityRouter(config.Config{
				APIKeys: []string{"k1"}, MaxResults: 10, ResultsCap: 50, RespectRobots: true,
				MaxRequestOutputBytes: 1 << 20, RateLimitPerSec: 0.0001, RateLimitBurst: 1,
			}, &fakePipeline{})
			call := func() *httptest.ResponseRecorder {
				request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
				if test.header == "Authorization" {
					request.Header.Set(test.header, "Bearer k1")
				} else {
					request.Header.Set(test.header, "k1")
				}
				recorder := httptest.NewRecorder()
				router.ServeHTTP(recorder, request)
				return recorder
			}
			if first := call(); first.Code != http.StatusOK {
				t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
			}
			limited := call()
			if limited.Code != http.StatusTooManyRequests || limited.Header().Get("Retry-After") != "1" || !strings.Contains(limited.Body.String(), test.expected) {
				t.Fatalf("limited status=%d headers=%v body=%s", limited.Code, limited.Header(), limited.Body.String())
			}
			if test.name == "exa" && strings.TrimSpace(limited.Body.String()) != test.expected {
				t.Fatalf("Exa 429 body=%q want exact simple envelope %q", limited.Body.String(), test.expected)
			}
		})
	}
}

func TestCompatibilityResponseBudgetsKeepVendorErrorShapes(t *testing.T) {
	tests := []struct {
		name, method, path, body, header, expected string
	}{
		{"tavily", http.MethodPost, "/compat/tavily/search", `{"query":"x"}`, "Authorization", `"detail"`},
		{"exa", http.MethodPost, "/compat/exa/search", `{"query":"x","contents":{"text":true}}`, "x-api-key", `"tag":"RESPONSE_TOO_LARGE"`},
		{"brave", http.MethodGet, "/compat/brave/res/v1/web/search?q=x&count=1", "", "X-Subscription-Token", `"code":"RESPONSE_TOO_LARGE"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pipe := &fakePipeline{results: []model.SearchResult{{
				URL: "https://example.com", Title: "large", Snippet: strings.Repeat("x", 2000), Markdown: strings.Repeat("x", 2000),
				Provenance: []model.DiscoveryProvenance{{Provider: "wiby", Lane: "wiby-general", Variant: "original"}},
			}}}
			router := compatibilityRouter(config.Config{
				APIKeys: []string{"k1"}, MaxResults: 10, ResultsCap: 50, RespectRobots: true,
				MaxRequestOutputBytes: 256,
			}, pipe)
			request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
			if test.header == "Authorization" {
				request.Header.Set(test.header, "Bearer k1")
			} else {
				request.Header.Set(test.header, "k1")
			}
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusInsufficientStorage || !strings.Contains(recorder.Body.String(), test.expected) {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
			if recorder.Header().Get("Link") != "" {
				t.Fatalf("507 response unexpectedly attributed Wiby: %q", recorder.Header().Get("Link"))
			}
			if test.name == "exa" {
				assertExaErrorEnvelope(t, recorder, http.StatusInsufficientStorage, "RESPONSE_TOO_LARGE")
			}
		})
	}
}

func TestCompatibilityResponsesDoNotExposeNativeDiscoveryProvenance(t *testing.T) {
	pipe := &fakePipeline{results: []model.SearchResult{{
		URL: "https://example.com", Title: "Example", Snippet: "Example result",
		Metadata:   map[string]string{"federation_peer": "must-not-leak"},
		Provenance: []model.DiscoveryProvenance{{Provider: "federation", Lane: "federation", Variant: "original"}},
	}}}
	router := compatibilityRouter(config.Config{
		APIKeys: []string{"k1"}, MaxResults: 10, ResultsCap: 50, RespectRobots: true, MaxRequestOutputBytes: 1 << 20,
	}, pipe)
	tests := []struct{ method, path, body, header string }{
		{http.MethodPost, "/compat/tavily/search", `{"query":"x"}`, "Authorization"},
		{http.MethodPost, "/compat/exa/search", `{"query":"x"}`, "x-api-key"},
		{http.MethodGet, "/compat/brave/res/v1/web/search?q=x", "", "X-Subscription-Token"},
	}
	for _, test := range tests {
		req := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
		if test.header == "Authorization" {
			req.Header.Set(test.header, "Bearer k1")
		} else {
			req.Header.Set(test.header, "k1")
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "provenance") || strings.Contains(rec.Body.String(), "federation_peer") {
			t.Fatalf("%s %s status=%d body=%s", test.method, test.path, rec.Code, rec.Body.String())
		}
	}
}

func TestCompatibilityResponsesDoNotExposeNativeDiscoveryReport(t *testing.T) {
	base := &fakePipeline{results: []model.SearchResult{{URL: "https://example.com", Title: "Example"}}}
	report := search.DiscoveryReport{Status: search.BatchPartial, Lanes: []search.DiscoveryLaneReport{{
		Provider: "wiby", Lane: "wiby-general", Variant: "original", Status: search.BatchPartial,
		CandidateCount: 1, Diagnostics: []search.DiscoveryDiagnostic{{Reason: "malformed_results"}},
	}}}
	router := compatibilityRouter(config.Config{
		APIKeys: []string{"k1"}, MaxResults: 10, ResultsCap: 50, RespectRobots: true, MaxRequestOutputBytes: 1 << 20,
	}, &detailedFakePipeline{fakePipeline: base, output: pipeline.SearchOutput{Results: base.results, Discovery: report}})
	tests := []struct{ method, path, body, header string }{
		{http.MethodPost, "/compat/tavily/search", `{"query":"x"}`, "Authorization"},
		{http.MethodPost, "/compat/exa/search", `{"query":"x"}`, "x-api-key"},
		{http.MethodGet, "/compat/brave/res/v1/web/search?q=x", "", "X-Subscription-Token"},
	}
	for _, test := range tests {
		req := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
		if test.header == "Authorization" {
			req.Header.Set(test.header, "Bearer k1")
		} else {
			req.Header.Set(test.header, "k1")
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "discovery") || strings.Contains(rec.Body.String(), "malformed_results") {
			t.Fatalf("%s %s status=%d body=%s", test.method, test.path, rec.Code, rec.Body.String())
		}
	}
}

func TestCompatibilityErrorsDoNotExposeNativeDiscoveryReport(t *testing.T) {
	report := search.DiscoveryReport{Status: search.BatchFailed, Lanes: []search.DiscoveryLaneReport{{
		Provider: "mwmbl", Lane: "mwmbl-general", Variant: "original", Status: search.BatchFailed,
		Diagnostics: []search.DiscoveryDiagnostic{{Reason: "timeout", Retryable: true}},
	}}}
	router := compatibilityRouter(config.Config{
		APIKeys: []string{"k1"}, MaxResults: 10, ResultsCap: 50, RespectRobots: true, MaxRequestOutputBytes: 1 << 20,
	}, &detailedFakePipeline{fakePipeline: &fakePipeline{}, output: pipeline.SearchOutput{Discovery: report}, err: context.DeadlineExceeded})
	tests := []struct{ method, path, body, header string }{
		{http.MethodPost, "/compat/tavily/search", `{"query":"x"}`, "Authorization"},
		{http.MethodPost, "/compat/exa/search", `{"query":"x"}`, "x-api-key"},
		{http.MethodGet, "/compat/brave/res/v1/web/search?q=x", "", "X-Subscription-Token"},
	}
	for _, test := range tests {
		req := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
		if test.header == "Authorization" {
			req.Header.Set(test.header, "Bearer k1")
		} else {
			req.Header.Set(test.header, "k1")
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadGateway || strings.Contains(rec.Body.String(), "discovery") || strings.Contains(rec.Body.String(), "timeout") {
			t.Fatalf("%s %s status=%d body=%s", test.method, test.path, rec.Code, rec.Body.String())
		}
	}
}

func TestCompatibilitySearchResponsesAttributeWibyWithoutChangingVendorJSON(t *testing.T) {
	pipe := &fakePipeline{results: []model.SearchResult{{
		URL: "https://example.com", Title: "Example", Snippet: "Example result",
		Provenance: []model.DiscoveryProvenance{{Provider: "wiby", Lane: "wiby-general", Variant: "original"}},
	}}}
	router := compatibilityRouter(config.Config{
		APIKeys: []string{"k1"}, MaxResults: 10, ResultsCap: 50, RespectRobots: true, MaxRequestOutputBytes: 1 << 20,
	}, pipe)
	tests := []struct{ method, path, body, header string }{
		{http.MethodPost, "/compat/tavily/search", `{"query":"x"}`, "Authorization"},
		{http.MethodPost, "/compat/exa/search", `{"query":"x"}`, "x-api-key"},
		{http.MethodGet, "/compat/brave/res/v1/web/search?q=x", "", "X-Subscription-Token"},
	}
	for _, test := range tests {
		req := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
		if test.header == "Authorization" {
			req.Header.Set(test.header, "Bearer k1")
		} else {
			req.Header.Set(test.header, "k1")
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || rec.Header().Get("Link") != wibyAttributionLink {
			t.Fatalf("%s %s status=%d link=%q body=%s", test.method, test.path, rec.Code, rec.Header().Get("Link"), rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "provenance") || strings.Contains(rec.Body.String(), "wiby-general") {
			t.Fatalf("vendor JSON changed: %s", rec.Body.String())
		}
	}
}

func TestCompatibilitySearchResponsesAttributeStackExchangeWithoutChangingVendorJSON(t *testing.T) {
	pipe := &fakePipeline{results: []model.SearchResult{{
		URL: "https://stackoverflow.com/questions/1", Title: "Example", Snippet: "Example result",
		Provenance: []model.DiscoveryProvenance{{Provider: "stackexchange", Lane: "stackexchange-developer", Variant: "original"}},
	}}}
	router := compatibilityRouter(config.Config{
		APIKeys: []string{"k1"}, MaxResults: 10, ResultsCap: 50, RespectRobots: true, MaxRequestOutputBytes: 1 << 20,
	}, pipe)
	tests := []struct{ method, path, body, header string }{
		{http.MethodPost, "/compat/tavily/search", `{"query":"x"}`, "Authorization"},
		{http.MethodPost, "/compat/exa/search", `{"query":"x"}`, "x-api-key"},
		{http.MethodGet, "/compat/brave/res/v1/web/search?q=x", "", "X-Subscription-Token"},
	}
	for _, test := range tests {
		req := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
		if test.header == "Authorization" {
			req.Header.Set(test.header, "Bearer k1")
		} else {
			req.Header.Set(test.header, "k1")
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || rec.Header().Get("Link") != stackExchangeAttributionLink {
			t.Fatalf("%s %s status=%d link=%q body=%s", test.method, test.path, rec.Code, rec.Header().Get("Link"), rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "provenance") || strings.Contains(rec.Body.String(), "stackexchange-developer") {
			t.Fatalf("vendor JSON changed: %s", rec.Body.String())
		}
	}
}

func TestTavilyAndExaSearchResponsesAttributeArxivWithoutChangingVendorJSON(t *testing.T) {
	pipe := &fakePipeline{results: []model.SearchResult{{
		URL: "https://arxiv.org/abs/2401.01234", Title: "Example", Snippet: "Example result",
		Provenance: []model.DiscoveryProvenance{{Provider: "arxiv", Lane: "arxiv-research", Variant: "original"}},
	}}}
	router := compatibilityRouter(config.Config{
		APIKeys: []string{"k1"}, MaxResults: 10, ResultsCap: 50, RespectRobots: true, MaxRequestOutputBytes: 1 << 20,
	}, pipe)
	tests := []struct{ method, path, body, header string }{
		{http.MethodPost, "/compat/tavily/search", `{"query":"x"}`, "Authorization"},
		{http.MethodPost, "/compat/exa/search", `{"query":"x"}`, "x-api-key"},
	}
	for _, test := range tests {
		req := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
		if test.header == "Authorization" {
			req.Header.Set(test.header, "Bearer k1")
		} else {
			req.Header.Set(test.header, "k1")
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || rec.Header().Get("Link") != arxivAttributionLink {
			t.Fatalf("%s %s status=%d link=%q body=%s", test.method, test.path, rec.Code, rec.Header().Get("Link"), rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "provenance") || strings.Contains(rec.Body.String(), "arxiv-research") {
			t.Fatalf("vendor JSON changed: %s", rec.Body.String())
		}
	}
}

func TestSearchResponseDoesNotAttributeWibyWhenPageHasNoWibyResult(t *testing.T) {
	pipe := &fakePipeline{results: []model.SearchResult{{
		URL: "https://example.com", Provenance: []model.DiscoveryProvenance{{Provider: "mwmbl", Lane: "mwmbl-general", Variant: "original"}},
	}}}
	router := compatibilityRouter(config.Config{
		APIKeys: []string{"k1"}, MaxResults: 10, ResultsCap: 50, RespectRobots: true, MaxRequestOutputBytes: 1 << 20,
	}, pipe)
	req := httptest.NewRequest(http.MethodPost, "/compat/tavily/search", strings.NewReader(`{"query":"x"}`))
	req.Header.Set("Authorization", "Bearer k1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Header().Get("Link") != "" {
		t.Fatalf("status=%d unexpected link=%q", rec.Code, rec.Header().Get("Link"))
	}
}

func TestBraveSearchDoesNotAttributeWibyOutsideRequestedPage(t *testing.T) {
	pipe := &fakePipeline{results: []model.SearchResult{
		{
			URL: "https://wiby.example", Title: "Wiby result",
			Provenance: []model.DiscoveryProvenance{{Provider: "wiby", Lane: "wiby-general", Variant: "original"}},
		},
		{
			URL: "https://other.example", Title: "Requested page result",
			Provenance: []model.DiscoveryProvenance{{Provider: "mwmbl", Lane: "mwmbl-general", Variant: "original"}},
		},
	}}
	router := compatibilityRouter(config.Config{
		APIKeys: []string{"k1"}, MaxResults: 10, ResultsCap: 50, RespectRobots: true, MaxRequestOutputBytes: 1 << 20,
	}, pipe)
	req := httptest.NewRequest(http.MethodGet, "/compat/brave/res/v1/web/search?q=x&count=1&offset=1", nil)
	req.Header.Set("X-Subscription-Token", "k1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Header().Get("Link") != "" || strings.Contains(rec.Body.String(), "wiby.example") || !strings.Contains(rec.Body.String(), "other.example") {
		t.Fatalf("status=%d link=%q body=%s", rec.Code, rec.Header().Get("Link"), rec.Body.String())
	}
}

func compatibilityRouter(cfg config.Config, pipe PipelineRunner) http.Handler {
	return NewRouter(Deps{
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Config: cfg, Pipeline: pipe,
	})
}
