package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/core/model"
)

func TestBraveCompatSearchMapsQueryPaginationAndResults(t *testing.T) {
	pipe := &fakePipeline{results: []model.SearchResult{
		{URL: "https://example.com/one", Title: "One", Snippet: "first"},
		{URL: "https://example.com/two", Title: "Two", Snippet: "second"},
		{URL: "https://example.com/three", Title: "Three", Snippet: "third", Content: &model.Content{Language: "en"}},
		{URL: "https://example.com/four", Title: "Four", Snippet: "fourth"},
	}}
	router := compatTestRouter(pipe)
	request := httptest.NewRequest(http.MethodGet, "/compat/brave/res/v1/web/search?q=open+search&count=2&offset=1&freshness=pm&safesearch=strict&search_lang=en", nil)
	request.Header.Set("X-Subscription-Token", "k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if pipe.lastOpts.MaxResults != 5 || pipe.lastOpts.TimeRange != "month" || pipe.lastOpts.Language != "en" || pipe.lastOpts.SafeSearch == nil || *pipe.lastOpts.SafeSearch != 2 {
		t.Fatalf("options = %+v", pipe.lastOpts)
	}
	var response braveSearchResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Type != "search" || response.Query.Original != "open search" || len(response.Web.Results) != 2 || response.Web.Results[0].URL != "https://example.com/three" || response.Web.Results[0].Language != "en" {
		t.Fatalf("response = %+v", response)
	}
}

func TestBraveCompatRejectsPageBeyondInstanceCap(t *testing.T) {
	router := compatTestRouter(&fakePipeline{})
	request := httptest.NewRequest(http.MethodGet, "/compat/brave/res/v1/web/search?q=x&count=20&offset=2", nil)
	request.Header.Set("X-Subscription-Token", "k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnprocessableEntity || !strings.Contains(recorder.Body.String(), `"type":"ErrorResponse"`) || !strings.Contains(recorder.Body.String(), "instance result limit") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestBraveCompatRejectsUnsupportedControls(t *testing.T) {
	router := compatTestRouter(&fakePipeline{})
	request := httptest.NewRequest(http.MethodGet, "/compat/brave/res/v1/web/search?q=x&result_filter=web", nil)
	request.Header.Set("X-Subscription-Token", "k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnprocessableEntity || !strings.Contains(recorder.Body.String(), "result_filter") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestBraveCompatUsesVendorAuthShape(t *testing.T) {
	router := compatTestRouter(&fakePipeline{})
	request := httptest.NewRequest(http.MethodGet, "/compat/brave/res/v1/web/search?q=x", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized || !strings.Contains(recorder.Body.String(), `"code":"SUBSCRIPTION_TOKEN_INVALID"`) || strings.Contains(recorder.Body.String(), `"id":""`) {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestBraveCompatAppliesDocumentedDefaults(t *testing.T) {
	pipe := &fakePipeline{}
	router := compatTestRouter(pipe)
	request := httptest.NewRequest(http.MethodGet, "/compat/brave/res/v1/web/search?q=x", nil)
	request.Header.Set("X-Subscription-Token", "k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if pipe.lastOpts.Language != "en" || pipe.lastOpts.SafeSearch == nil || *pipe.lastOpts.SafeSearch != 1 || pipe.lastOpts.MaxResults != 21 {
		t.Fatalf("options = %+v", pipe.lastOpts)
	}
}
