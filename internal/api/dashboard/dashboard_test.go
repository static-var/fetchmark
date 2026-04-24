package dashboard

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
)

// testMux adapts chi.Router to the tiny interface dashboard.Mount
// expects, with no real side effects beyond registering routes.
func mountOnChi(user, pass string, d Deps) http.Handler {
	r := chi.NewRouter()
	Mount(r, user, pass, d)
	return r
}

func TestMount_NoCredsMeansNoRoutes(t *testing.T) {
	h := mountOnChi("", "", Deps{Gatherer: prometheus.DefaultGatherer})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/dashboard", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("dashboard must not be mounted without creds; code=%d", rec.Code)
	}
}

func TestMount_RequiresBasicAuth(t *testing.T) {
	h := mountOnChi("u", "p", Deps{Gatherer: prometheus.DefaultGatherer})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/dashboard", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without creds, got %d", rec.Code)
	}
	if !strings.Contains(rec.Header().Get("WWW-Authenticate"), "fetchmark") {
		t.Fatalf("missing WWW-Authenticate header")
	}
}

func TestMount_OKWithCorrectCreds(t *testing.T) {
	h := mountOnChi("u", "p", Deps{Gatherer: prometheus.DefaultGatherer, Version: "v"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/dashboard", nil)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("u:p")))
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "Fetchmark") {
		t.Fatalf("body missing brand: %s", rec.Body.String())
	}
}
