package searxng

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/core/search"
)

const fixture = `{
  "query": "golang",
  "number_of_results": 2,
  "results": [
    {"url": "https://go.dev", "title": "The Go Programming Language", "content": "Go is...", "engine": "google", "engines": ["google","bing"], "category": "general"},
    {"url": "https://golang.org", "title": "golang.org", "content": "redirect", "engine": "duckduckgo"}
  ],
  "unresponsive_engines": []
}`

func newStub(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c, err := New(srv.URL, srv.Client())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestSearch_Success(t *testing.T) {
	var gotQuery string
	c := newStub(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/search" {
			t.Errorf("path = %q", r.URL.Path)
		}
		gotQuery = r.URL.Query().Get("q")
		if r.URL.Query().Get("format") != "json" {
			t.Errorf("format missing")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fixture))
	})
	hits, err := c.Search(context.Background(), search.Query{Q: "golang", Engines: []string{"google", "bing"}})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if gotQuery != "golang" {
		t.Errorf("query = %q", gotQuery)
	}
	if len(hits) != 2 {
		t.Fatalf("hits = %d", len(hits))
	}
	if hits[0].URL != "https://go.dev" || len(hits[0].Engines) != 2 {
		t.Errorf("first hit = %+v", hits[0])
	}
	if hits[1].Engines[0] != "duckduckgo" {
		t.Errorf("second hit engines = %v", hits[1].Engines)
	}
}

func TestSearch_EmptyQuery(t *testing.T) {
	c := newStub(t, func(w http.ResponseWriter, r *http.Request) {})
	if _, err := c.Search(context.Background(), search.Query{Q: "   "}); err == nil {
		t.Fatal("expected error for empty query")
	}
}

func TestSearch_UpstreamError(t *testing.T) {
	c := newStub(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	})
	_, err := c.Search(context.Background(), search.Query{Q: "x"})
	if err == nil || !strings.Contains(err.Error(), "502") {
		t.Fatalf("err = %v", err)
	}
}

func TestNew_BadURL(t *testing.T) {
	if _, err := New("notaurl", nil); err == nil {
		t.Fatal("expected error")
	}
}
