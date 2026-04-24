package api

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/staticvar/fetchmark/internal/api/middleware"
	"github.com/staticvar/fetchmark/internal/config"
)

// Deps bundles the external collaborators an API server needs. New
// collaborators (search, parse, cache, etc.) will extend this struct as
// later phases land; keeping them here rather than as globals preserves
// testability.
type Deps struct {
	Log    *slog.Logger
	Config config.Config
	// ReadyCheck reports whether hard dependencies (Redis, SearXNG) are
	// reachable. Returning nil means ready; non-nil is rendered as the
	// failure reason on /readyz.
	ReadyCheck func() error
}

// NewRouter wires the full HTTP surface for Fetchmark. It is invoked once
// from cmd/fetchmark and from integration tests.
func NewRouter(d Deps) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.Logger(d.Log))
	r.Use(chimw.Recoverer)

	r.Get("/healthz", healthz)
	r.Get("/readyz", readyz(d.ReadyCheck))
	r.Handle("/metrics", promhttp.Handler())

	r.Route("/v1", func(r chi.Router) {
		r.Use(middleware.APIKey(d.Config.APIKeys, d.Config.AdminAPIKeys))
		r.Post("/search", notImplemented("search"))
		r.Post("/parse", notImplemented("parse"))
		r.Post("/summarize", notImplemented("summarize"))
	})

	return r
}

func healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func readyz(check func() error) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if check == nil {
			writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
			return
		}
		if err := check(); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"status": "unready",
				"reason": err.Error(),
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

func notImplemented(op string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusNotImplemented, map[string]string{
			"error":     "not_implemented",
			"operation": op,
		})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
