package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/summarizer"
	"github.com/staticvar/fetchmark/internal/config"
	"github.com/staticvar/fetchmark/internal/core/model"
)

func TestTavilyCompatSearchTranslatesCanonicalPipeline(t *testing.T) {
	pipe := &fakePipeline{results: []model.SearchResult{{
		URL: "https://example.com/a", Title: "Example", Snippet: "A useful result.",
		Markdown: "# Example\n\nA useful result.", Score: 12.5,
	}}}
	router := compatTestRouter(pipe)
	request := httptest.NewRequest(http.MethodPost, "/compat/tavily/search", strings.NewReader(`{
		"query":"open search", "search_depth":"advanced", "max_results":5,
		"time_range":"month", "include_domains":["example.com"],
		"exclude_domains":["blocked.example"], "chunks_per_source":2,
		"include_raw_content":"markdown", "include_answer":true, "include_usage":true,
		"exact_match":true
	}`))
	request.Header.Set("Authorization", "Bearer k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if pipe.searchCalls != 1 || pipe.lastOpts.SearchDepth != "advanced" || pipe.lastOpts.MaxResults != 5 || pipe.lastOpts.TimeRange != "month" || pipe.lastOpts.ChunksPerSource != 2 || !pipe.lastOpts.ExactMatch {
		t.Fatalf("pipeline options = %+v calls=%d", pipe.lastOpts, pipe.searchCalls)
	}
	if len(pipe.lastOpts.IncludeDomains) != 1 || len(pipe.lastOpts.ExcludeDomains) != 1 {
		t.Fatalf("domain filters = %+v", pipe.lastOpts)
	}
	var response struct {
		Query     string `json:"query"`
		Answer    string `json:"answer"`
		RequestID string `json:"request_id"`
		Images    []any  `json:"images"`
		Results   []struct {
			URL        string  `json:"url"`
			Content    string  `json:"content"`
			Score      float64 `json:"score"`
			RawContent *string `json:"raw_content"`
		} `json:"results"`
		Usage map[string]int `json:"usage"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Query != "open search" || response.RequestID == "" || len(response.Results) != 1 || response.Results[0].URL != "https://example.com/a" || response.Results[0].Content != "A useful result." {
		t.Fatalf("response = %+v", response)
	}
	if response.Images == nil || len(response.Images) != 0 {
		t.Fatalf("images = %#v, want present empty array", response.Images)
	}
	if response.Results[0].RawContent == nil || *response.Results[0].RawContent != "# Example\n\nA useful result." {
		t.Fatalf("raw_content = %#v", response.Results[0].RawContent)
	}
	if response.Answer == "" || !strings.Contains(response.Answer, "[1]") {
		t.Fatalf("extractive answer = %q", response.Answer)
	}
	if response.Usage["credits"] != 0 {
		t.Fatalf("usage = %v", response.Usage)
	}
}

func TestTavilyCompatBasicExactMatchReachesCanonicalPipelineUnchanged(t *testing.T) {
	pipe := &fakePipeline{}
	router := compatTestRouter(pipe)
	const query = "current SQLite WAL behavior"
	request := httptest.NewRequest(http.MethodPost, "/compat/tavily/search", strings.NewReader(`{
		"query":"current SQLite WAL behavior", "search_depth":"basic", "exact_match":true
	}`))
	request.Header.Set("Authorization", "Bearer k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if pipe.searchCalls != 1 || pipe.lastOpts.Query != query || pipe.lastOpts.SearchDepth != "basic" || !pipe.lastOpts.ExactMatch {
		t.Fatalf("pipeline options = %+v calls=%d", pipe.lastOpts, pipe.searchCalls)
	}
}

func TestTavilyCompatAdvancedExactMatchReachesCanonicalPipelineUnchanged(t *testing.T) {
	pipe := &fakePipeline{}
	router := compatTestRouter(pipe)
	const query = "current SQLite WAL behavior"
	request := httptest.NewRequest(http.MethodPost, "/compat/tavily/search", strings.NewReader(`{
		"query":"current SQLite WAL behavior", "search_depth":"advanced", "exact_match":true
	}`))
	request.Header.Set("Authorization", "Bearer k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if pipe.searchCalls != 1 || pipe.lastOpts.Query != query || pipe.lastOpts.SearchDepth != "advanced" || !pipe.lastOpts.ExactMatch {
		t.Fatalf("pipeline options = %+v calls=%d", pipe.lastOpts, pipe.searchCalls)
	}
}

func TestTavilyCompatRejectsUnsupportedSemanticControls(t *testing.T) {
	router := compatTestRouter(&fakePipeline{})
	request := httptest.NewRequest(http.MethodPost, "/compat/tavily/search", strings.NewReader(`{"query":"x","include_images":true}`))
	request.Header.Set("Authorization", "Bearer k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "include_images") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestTavilyCompatUsesVendorAuthAndErrorShape(t *testing.T) {
	router := compatTestRouter(&fakePipeline{})
	request := httptest.NewRequest(http.MethodPost, "/compat/tavily/search", strings.NewReader(`{"query":"x"}`))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized || !strings.Contains(recorder.Body.String(), `"detail"`) {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestTavilyCompatDoesNotExposeDiscoverySnippetAsRawContent(t *testing.T) {
	pipe := &fakePipeline{results: []model.SearchResult{{
		URL: "https://example.com", Snippet: "discovery-only snippet", Unsupported: "robots_disallowed",
	}}}
	router := compatTestRouter(pipe)
	request := httptest.NewRequest(http.MethodPost, "/compat/tavily/search", strings.NewReader(`{"query":"x","include_raw_content":"text"}`))
	request.Header.Set("Authorization", "Bearer k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Results []struct {
			RawContent *string `json:"raw_content"`
		} `json:"results"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 1 || response.Results[0].RawContent != nil {
		t.Fatalf("response = %+v", response)
	}
}

func TestTavilyCompatAdvancedAnswerUsesConfiguredLocalProvider(t *testing.T) {
	stub := &stubProvider{name: "local", kind: summarizer.KindOpenAI, resp: summarizer.Response{Summary: "A grounded answer. [99]"}}
	registry := summarizer.NewRegistry(func(summarizer.ProviderConfig, *http.Client) summarizer.Provider { return stub }, nil)
	if err := registry.Set(summarizer.ProviderConfig{
		Name: "local", Kind: summarizer.KindOpenAI, BaseURL: "http://localhost:11434/v1/",
		APIKey: "local", Model: "local-model",
	}); err != nil {
		t.Fatal(err)
	}
	pipe := &fakePipeline{results: []model.SearchResult{{
		URL: "https://example.com/source", Title: "Source", Snippet: "Grounded source text.",
	}}}
	router := NewRouter(Deps{
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Config: config.Config{
			APIKeys: []string{"k1"}, MaxResults: 10, ResultsCap: 50, RespectRobots: true,
			MaxRequestOutputBytes: 1 << 20, CompatAnswerProvider: "local",
			CompatAnswerMaxTokens: 321, CompatAnswerTimeout: 2 * time.Second,
		},
		Pipeline: pipe, Summarizers: registry,
	})
	request := httptest.NewRequest(http.MethodPost, "/compat/tavily/search", strings.NewReader(`{"query":"what?","include_answer":"advanced"}`))
	request.Header.Set("Authorization", "Bearer k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"answer":"A grounded answer. [1]"`) {
		t.Fatalf("body=%s", recorder.Body.String())
	}
	if stub.last.Model != "local-model" || stub.last.MaxTokens != 321 || !strings.Contains(stub.last.UserPrompt, "Grounded source text") {
		t.Fatalf("request = %+v", stub.last)
	}
	if !stub.hasDeadline {
		t.Fatal("expected compatibility answer deadline")
	}
}

func TestTavilyCompatRejectsExplicitZeroCardinalityControls(t *testing.T) {
	for _, body := range []string{`{"query":"x","max_results":0}`, `{"query":"x","chunks_per_source":0}`} {
		router := compatTestRouter(&fakePipeline{})
		request := httptest.NewRequest(http.MethodPost, "/compat/tavily/search", strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer k1")
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("body=%s status=%d response=%s", body, recorder.Code, recorder.Body.String())
		}
	}
}

func TestDeterministicExtractiveAnswerUsesDecimalCitationIndexes(t *testing.T) {
	results := make([]model.SearchResult, 11)
	results[10] = model.SearchResult{Snippet: "A supported source sentence that is long enough to cite correctly."}
	answer := deterministicExtractiveAnswer(results)
	if !strings.Contains(answer, "[11]") {
		t.Fatalf("answer = %q", answer)
	}
}

func TestCompatibilityAnswerCitationsAreLimitedToPromptedSources(t *testing.T) {
	answer, valid := sanitizeCompatibilityCitations("Supported [2], absent [6], bogus [99].", 5)
	if !valid || strings.Contains(answer, "[6]") || strings.Contains(answer, "[99]") || !strings.Contains(answer, "[2]") {
		t.Fatalf("answer=%q valid=%v", answer, valid)
	}
}

func TestTavilyCompatAdvancedAnswerRequiresExplicitProvider(t *testing.T) {
	router := compatTestRouter(&fakePipeline{results: []model.SearchResult{{URL: "https://example.com", Snippet: "text"}}})
	request := httptest.NewRequest(http.MethodPost, "/compat/tavily/search", strings.NewReader(`{"query":"x","include_answer":"advanced"}`))
	request.Header.Set("Authorization", "Bearer k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), "not configured") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func compatTestRouter(pipe PipelineRunner) http.Handler {
	return NewRouter(Deps{
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Config: config.Config{
			APIKeys: []string{"k1"}, MaxResults: 10, ResultsCap: 50,
			RespectRobots: true, MaxRequestOutputBytes: 1 << 20,
		},
		Pipeline: pipe,
	})
}
