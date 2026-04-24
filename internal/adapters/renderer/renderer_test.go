package renderer

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPRenderer_RawHTMLBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(b), `"url":"https://example.com"`) {
			t.Errorf("unexpected body: %s", b)
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html><body>hi</body></html>"))
	}))
	t.Cleanup(srv.Close)

	r, err := NewHTTP(Options{Endpoint: srv.URL, Client: srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	out, err := r.Render(context.Background(), "https://example.com")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "<body>hi") {
		t.Errorf("got %q", out)
	}
}

func TestHTTPRenderer_UnwrapsJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(map[string]string{"html": "<p>ok</p>"})
	}))
	t.Cleanup(srv.Close)
	r, _ := NewHTTP(Options{Endpoint: srv.URL, Client: srv.Client()})
	out, err := r.Render(context.Background(), "https://x")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(out)) != "<p>ok</p>" {
		t.Errorf("got %q", out)
	}
}

func TestHTTPRenderer_RejectsOversizedBody(t *testing.T) {
	big := strings.Repeat("x", 2048)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(big))
	}))
	t.Cleanup(srv.Close)
	r, _ := NewHTTP(Options{Endpoint: srv.URL, Client: srv.Client(), MaxBody: 1024})
	if _, err := r.Render(context.Background(), "https://x"); err == nil {
		t.Fatal("expected body-limit error")
	}
}

func TestHTTPRenderer_PropagatesHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)
	r, _ := NewHTTP(Options{Endpoint: srv.URL, Client: srv.Client()})
	if _, err := r.Render(context.Background(), "https://x"); err == nil {
		t.Fatal("expected status-code error")
	}
}

func TestNewHTTP_RequiresEndpoint(t *testing.T) {
	if _, err := NewHTTP(Options{}); err == nil {
		t.Fatal("expected error on empty endpoint")
	}
}
