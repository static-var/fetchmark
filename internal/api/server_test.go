package api

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/config"
)

func newTestRouter(ready func() error) http.Handler {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Config{
		APIKeys:      []string{"k1"},
		AdminAPIKeys: []string{"admin1"},
	}
	return NewRouter(Deps{Log: log, Config: cfg, ReadyCheck: ready})
}

func TestHealthz(t *testing.T) {
	r := newTestRouter(nil)
	req := httptest.NewRequest("GET", "/healthz", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestReadyz_Unready(t *testing.T) {
	r := newTestRouter(func() error { return errors.New("redis down") })
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
	r := newTestRouter(nil)
	req := httptest.NewRequest("POST", "/v1/search", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestV1NotImplemented(t *testing.T) {
	r := newTestRouter(nil)
	req := httptest.NewRequest("POST", "/v1/search", strings.NewReader("{}"))
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
}
