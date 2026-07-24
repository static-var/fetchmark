package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/config"
	"github.com/staticvar/fetchmark/internal/core/model"
)

func assertExaErrorEnvelope(t *testing.T, recorder *httptest.ResponseRecorder, status int, tag string) map[string]json.RawMessage {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("status=%d want=%d body=%s", recorder.Code, status, recorder.Body.String())
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode error envelope: %v; body=%s", err, recorder.Body.String())
	}
	if len(payload) != 3 {
		t.Fatalf("error envelope keys=%v, want exactly requestId,error,tag", payload)
	}
	for _, key := range []string{"requestId", "error", "tag"} {
		if _, ok := payload[key]; !ok {
			t.Fatalf("error envelope missing %q: %v", key, payload)
		}
	}
	var requestID, message, actualTag string
	if err := json.Unmarshal(payload["requestId"], &requestID); err != nil || requestID == "" {
		t.Fatalf("requestId=%s err=%v", payload["requestId"], err)
	}
	if err := json.Unmarshal(payload["error"], &message); err != nil || message == "" {
		t.Fatalf("error=%s err=%v", payload["error"], err)
	}
	if err := json.Unmarshal(payload["tag"], &actualTag); err != nil || actualTag != tag {
		t.Fatalf("tag=%q want=%q err=%v", actualTag, tag, err)
	}
	return payload
}

func TestExaCompatSearchMapsSearchAndContents(t *testing.T) {
	pipe := &fakePipeline{results: []model.SearchResult{{
		URL: "https://example.com/a", Title: "Example", Author: "A", Markdown: "Full page text.", Score: 0.9,
		Chunks: []model.ContentChunk{{Text: "focused passage", Score: 0.8}},
	}}}
	router := compatTestRouter(pipe)
	request := httptest.NewRequest(http.MethodPost, "/compat/exa/search", strings.NewReader(`{
		"query":"open discovery", "numResults":4, "type":"auto",
		"includeDomains":["example.com"], "contents":{"text":{"maxCharacters":20},"highlights":true}
	}`))
	request.Header.Set("x-api-key", "k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if pipe.lastOpts.MaxResults != 4 || pipe.lastOpts.ChunksPerSource != 3 || len(pipe.lastOpts.IncludeDomains) != 1 {
		t.Fatalf("options = %+v", pipe.lastOpts)
	}
	var response exaSearchResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.RequestID == "" || len(response.Results) != 1 || response.Results[0].ID != "https://example.com/a" || response.Results[0].Author != "A" || response.Results[0].Text == nil || *response.Results[0].Text != "Full page text." {
		t.Fatalf("response = %+v", response)
	}
	if len(response.Results[0].Highlights) != 1 || response.Results[0].Highlights[0] != "focused passage" {
		t.Fatalf("highlights = %+v", response.Results[0])
	}
}

func TestExaCompatContentsReturnsPerURLStatuses(t *testing.T) {
	pipe := &fakePipeline{results: []model.SearchResult{
		{URL: "https://example.com/ok", Title: "OK", Markdown: "parsed page", FromCache: true},
		{URL: "https://example.com/blocked", Unsupported: "robots_disallowed"},
	}}
	router := compatTestRouter(pipe)
	request := httptest.NewRequest(http.MethodPost, "/compat/exa/contents", strings.NewReader(`{
		"urls":["https://example.com/ok","https://example.com/blocked"], "text":true, "highlights":{"query":"needle"}
	}`))
	request.Header.Set("Authorization", "Bearer k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if pipe.parseCalls != 1 || pipe.lastOpts.Query != "needle" || pipe.lastOpts.ChunksPerSource != 3 || !pipe.lastOpts.PreserveURLResults || len(pipe.lastOpts.URLs) != 2 {
		t.Fatalf("parse options = %+v calls=%d", pipe.lastOpts, pipe.parseCalls)
	}
	var response exaContentsResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 1 || response.Results[0].Text == nil || *response.Results[0].Text != "parsed page" {
		t.Fatalf("results = %+v", response.Results)
	}
	if len(response.Statuses) != 2 || response.Statuses[0].Status != "success" || response.Statuses[0].Source != "cached" || response.Statuses[1].Error == nil || response.Statuses[1].Error.HTTPStatusCode == nil || *response.Statuses[1].Error.HTTPStatusCode != http.StatusForbidden || response.Statuses[1].Error.Tag != "SOURCE_NOT_AVAILABLE" {
		t.Fatalf("statuses = %+v", response.Statuses)
	}
}

func TestExaCompatRejectsUnsupportedSemanticMode(t *testing.T) {
	router := compatTestRouter(&fakePipeline{})
	request := httptest.NewRequest(http.MethodPost, "/compat/exa/search", strings.NewReader(`{"query":"x","type":"deep"}`))
	request.Header.Set("x-api-key", "k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	assertExaErrorEnvelope(t, recorder, http.StatusBadRequest, "INVALID_REQUEST_BODY")
	if !strings.Contains(recorder.Body.String(), "semantic") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestExaCompatDoesNotExposeDiscoverySnippetAsExtractedText(t *testing.T) {
	pipe := &fakePipeline{results: []model.SearchResult{{
		URL: "https://example.com", Snippet: "discovery-only snippet", Unsupported: "fetch_failed",
	}}}
	router := compatTestRouter(pipe)
	request := httptest.NewRequest(http.MethodPost, "/compat/exa/search", strings.NewReader(`{"query":"x","contents":{"text":true,"highlights":true}}`))
	request.Header.Set("x-api-key", "k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response exaSearchResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 1 || response.Results[0].Text != nil || len(response.Results[0].Highlights) != 0 {
		t.Fatalf("response = %+v", response)
	}
}

func TestExaCompatContentsRequiresExactlyOneIdentifierList(t *testing.T) {
	router := compatTestRouter(&fakePipeline{})
	request := httptest.NewRequest(http.MethodPost, "/compat/exa/contents", strings.NewReader(`{"ids":["https://a.example"],"urls":["https://b.example"]}`))
	request.Header.Set("x-api-key", "k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	assertExaErrorEnvelope(t, recorder, http.StatusBadRequest, "INVALID_REQUEST")
	if !strings.Contains(recorder.Body.String(), "exactly one") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestExaCompatUsesVendorAuthShape(t *testing.T) {
	for _, test := range []struct {
		name string
		key  string
	}{
		{name: "missing"},
		{name: "invalid", key: "wrong"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/compat/exa/search", strings.NewReader(`{"query":"x"}`))
			if test.key != "" {
				request.Header.Set("x-api-key", test.key)
			}
			recorder := httptest.NewRecorder()
			compatTestRouter(&fakePipeline{}).ServeHTTP(recorder, request)
			assertExaErrorEnvelope(t, recorder, http.StatusUnauthorized, "INVALID_API_KEY")
		})
	}
}

func TestExaCompatErrorTagsMatchExaWireContract(t *testing.T) {
	tests := []struct {
		name string
		path string
		body string
		tag  string
	}{
		{name: "malformed body", path: "/compat/exa/search", body: `{"query":`, tag: "INVALID_REQUEST_BODY"},
		{name: "trailing JSON value", path: "/compat/exa/search", body: `{"query":"x"}{}`, tag: "INVALID_REQUEST_BODY"},
		{name: "missing query", path: "/compat/exa/search", body: `{}`, tag: "INVALID_REQUEST_BODY"},
		{name: "missing content identifiers", path: "/compat/exa/contents", body: `{}`, tag: "INVALID_REQUEST_BODY"},
		{name: "empty content identifiers", path: "/compat/exa/contents", body: `{"urls":[]}`, tag: "INVALID_REQUEST_BODY"},
		{name: "conflicting search controls", path: "/compat/exa/search", body: `{"query":"one","contents":{"highlights":{"query":"two"}}}`, tag: "INVALID_REQUEST"},
		{name: "official search freshness conflict", path: "/compat/exa/search", body: `{"query":"x","contents":{"livecrawl":"always","maxAgeHours":0}}`, tag: "INVALID_REQUEST"},
		{name: "official contents freshness conflict", path: "/compat/exa/contents", body: `{"urls":["https://example.com"],"livecrawl":"always","maxAgeHours":0}`, tag: "INVALID_REQUEST"},
		{name: "invalid URL", path: "/compat/exa/contents", body: `{"urls":["not a URL"]}`, tag: "INVALID_URLS"},
		{name: "zero numResults", path: "/compat/exa/search", body: `{"query":"x","numResults":0}`, tag: "INVALID_NUM_RESULTS"},
		{name: "numResults above Exa maximum", path: "/compat/exa/search", body: `{"query":"x","numResults":101}`, tag: "INVALID_NUM_RESULTS"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
			request.Header.Set("x-api-key", "k1")
			recorder := httptest.NewRecorder()
			compatTestRouter(&fakePipeline{}).ServeHTTP(recorder, request)
			assertExaErrorEnvelope(t, recorder, http.StatusBadRequest, test.tag)
		})
	}
}

func TestExaContentsRejectsOversizedHighlightsQueryBeforePipelineWork(t *testing.T) {
	const publicQueryRuneLimit = 400
	pipe := &fakePipeline{}
	request := httptest.NewRequest(http.MethodPost, "/compat/exa/contents", strings.NewReader(
		`{"urls":["https://example.com"],"highlights":{"query":"`+
			strings.Repeat("x", publicQueryRuneLimit+1)+`"}}`,
	))
	request.Header.Set("x-api-key", "k1")
	recorder := httptest.NewRecorder()

	compatTestRouter(pipe).ServeHTTP(recorder, request)

	assertExaErrorEnvelope(t, recorder, http.StatusBadRequest, "INVALID_REQUEST_BODY")
	if pipe.parseCalls != 0 {
		t.Fatalf("pipeline parse calls=%d", pipe.parseCalls)
	}
}

func TestExaCompatValidNumResultsAboveInstanceCapIsExceeded(t *testing.T) {
	router := NewRouter(Deps{
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Config: config.Config{
			APIKeys: []string{"k1"}, MaxResults: 10, ResultsCap: 10,
			RespectRobots: true, MaxRequestOutputBytes: 1 << 20,
		},
		Pipeline: &fakePipeline{},
	})
	request := httptest.NewRequest(http.MethodPost, "/compat/exa/search", strings.NewReader(`{"query":"x","numResults":11}`))
	request.Header.Set("x-api-key", "k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	assertExaErrorEnvelope(t, recorder, http.StatusBadRequest, "NUM_RESULTS_EXCEEDED")
}

func TestExaCompatDoesNotExposeUpstreamErrorDetails(t *testing.T) {
	router := compatTestRouter(&fakePipeline{err: errors.New(`Get "http://searxng.internal/search?q=private-query": connection refused`)})
	request := httptest.NewRequest(http.MethodPost, "/compat/exa/search", strings.NewReader(`{"query":"x"}`))
	request.Header.Set("x-api-key", "k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	assertExaErrorEnvelope(t, recorder, http.StatusBadGateway, "INTERNAL_ERROR")
	if strings.Contains(recorder.Body.String(), "searxng.internal") || strings.Contains(recorder.Body.String(), "private-query") {
		t.Fatalf("public Exa error leaked upstream details: %s", recorder.Body.String())
	}
}

func TestExaCompatNullContentControlsRemainDisabled(t *testing.T) {
	pipe := &fakePipeline{results: []model.SearchResult{{URL: "https://example.com", Markdown: "text"}}}
	router := compatTestRouter(pipe)
	request := httptest.NewRequest(http.MethodPost, "/compat/exa/search", strings.NewReader(`{"query":"x","contents":{"text":null,"highlights":null}}`))
	request.Header.Set("x-api-key", "k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || strings.Contains(recorder.Body.String(), `"text":`) || strings.Contains(recorder.Body.String(), `"highlights":`) {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestExaCompatContentsHighlightsRequireQuery(t *testing.T) {
	router := compatTestRouter(&fakePipeline{})
	request := httptest.NewRequest(http.MethodPost, "/compat/exa/contents", strings.NewReader(`{"urls":["https://example.com"],"highlights":true}`))
	request.Header.Set("x-api-key", "k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "highlights.query") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestExaCompatContentsMapsDeduplicatedInputByCanonicalURL(t *testing.T) {
	pipe := &fakePipeline{results: []model.SearchResult{{URL: "https://example.com/page", Markdown: "text"}}}
	router := compatTestRouter(pipe)
	request := httptest.NewRequest(http.MethodPost, "/compat/exa/contents", strings.NewReader(`{
		"urls":["https://EXAMPLE.com:443/page#one","https://example.com/page#two"],"text":true
	}`))
	request.Header.Set("x-api-key", "k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response exaContentsResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 2 || len(response.Statuses) != 2 || response.Statuses[1].Status != "success" {
		t.Fatalf("response = %+v", response)
	}
}
