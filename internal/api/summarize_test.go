package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/adapters/summarizer"
	"github.com/staticvar/fetchmark/internal/config"
	"github.com/staticvar/fetchmark/internal/core/model"
)

// stubProvider implements summarizer.Provider for API-layer tests
// without touching the real OpenAI/Anthropic SDKs.
type stubProvider struct {
	name string
	kind summarizer.Kind
	resp summarizer.Response
	err  error
	last summarizer.Request
}

func (s *stubProvider) Kind() summarizer.Kind                                { return s.kind }
func (s *stubProvider) Name() string                                         { return s.name }
func (s *stubProvider) Summarize(_ context.Context, r summarizer.Request) (summarizer.Response, error) {
	s.last = r
	if s.err != nil {
		return summarizer.Response{}, s.err
	}
	out := s.resp
	if out.Model == "" {
		out.Model = r.Model
	}
	return out, nil
}

func withSummarizer(t *testing.T, stub *stubProvider) (http.Handler, *fakePipeline, *summarizer.Registry) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Config{
		APIKeys:       []string{"k1"},
		AdminAPIKeys:  []string{"admin1"},
		MaxResults:    10,
		ResultsCap:    50,
		RespectRobots: true,
	}
	pipe := &fakePipeline{results: []model.SearchResult{{
		URL:   "https://example.com/a",
		Title: "Example",
		Content: &model.Content{
			Title:    "Example",
			Markdown: "Hello world. This page has enough text to summarize.",
		},
	}}}
	reg := summarizer.NewRegistry(func(c summarizer.ProviderConfig, _ *http.Client) summarizer.Provider {
		return stub
	}, nil)
	if err := reg.Set(summarizer.ProviderConfig{
		Name:    stub.name,
		Kind:    stub.kind,
		BaseURL: "https://api.test/v1/",
		APIKey:  "sk-test-1234",
		Model:   "test-model",
	}); err != nil {
		t.Fatalf("reg.Set: %v", err)
	}
	router := NewRouter(Deps{
		Log: log, Config: cfg, Pipeline: pipe, Summarizers: reg,
	})
	return router, pipe, reg
}

func TestSummarize_HappyPath(t *testing.T) {
	stub := &stubProvider{
		name: "openai", kind: summarizer.KindOpenAI,
		resp: summarizer.Response{Summary: "TLDR: hello.", Usage: summarizer.Usage{PromptTokens: 10, CompletionTokens: 3, TotalTokens: 13}},
	}
	r, _, _ := withSummarizer(t, stub)
	body := strings.NewReader(`{"url":"https://example.com/a","format":"bullets"}`)
	req := httptest.NewRequest("POST", "/v1/summarize", body)
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var out summarizeResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Summary != "TLDR: hello." || out.Provider != "openai" || out.Model != "test-model" {
		t.Fatalf("unexpected response: %+v", out)
	}
	if !strings.Contains(stub.last.UserPrompt, "<page>") || !strings.Contains(stub.last.UserPrompt, "Hello world") {
		t.Fatalf("prompt not delimited with page content: %q", stub.last.UserPrompt)
	}
	if !strings.Contains(stub.last.UserPrompt, "bullets") {
		t.Fatalf("format not propagated: %q", stub.last.UserPrompt)
	}
}

func TestSummarize_EmptyContentIs422(t *testing.T) {
	stub := &stubProvider{name: "openai", kind: summarizer.KindOpenAI}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Config{APIKeys: []string{"k1"}, MaxResults: 10, ResultsCap: 50, RespectRobots: true}
	pipe := &fakePipeline{results: []model.SearchResult{{URL: "https://example.com/a", Content: &model.Content{}}}}
	reg := summarizer.NewRegistry(func(c summarizer.ProviderConfig, _ *http.Client) summarizer.Provider { return stub }, nil)
	_ = reg.Set(summarizer.ProviderConfig{Name: "openai", Kind: summarizer.KindOpenAI, BaseURL: "https://api.test/v1/", APIKey: "k", Model: "m"})
	r := NewRouter(Deps{Log: log, Config: cfg, Pipeline: pipe, Summarizers: reg})

	req := httptest.NewRequest("POST", "/v1/summarize", strings.NewReader(`{"url":"https://example.com/a"}`))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestAdminSummarize_GetDropsAPIKey(t *testing.T) {
	// API keys have json:"-" so they must never appear in admin GET
	// output in any form. Redact() additionally masks for any surface
	// that chooses to include them explicitly.
	stub := &stubProvider{name: "openai", kind: summarizer.KindOpenAI}
	r, _, _ := withSummarizer(t, stub)
	req := httptest.NewRequest("GET", "/admin/summarize/config", nil)
	req.Header.Set("X-API-Key", "admin1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "sk-test-1234") {
		t.Fatalf("admin GET leaked raw api key: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "\"openai\"") {
		t.Fatalf("provider missing: %s", rec.Body.String())
	}
}

func TestAdminSummarize_NonAdminRejected(t *testing.T) {
	// Non-admin keys must not pass the admin subtree. We scope that
	// subtree's APIKey middleware to admin keys only, so k1 (ops key)
	// returns 401 invalid_api_key rather than 403.
	stub := &stubProvider{name: "openai", kind: summarizer.KindOpenAI}
	r, _, _ := withSummarizer(t, stub)
	req := httptest.NewRequest("GET", "/admin/summarize/config", nil)
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("non-admin must be rejected, got %d", rec.Code)
	}
}
