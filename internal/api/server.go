package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/staticvar/fetchmark/internal/adapters/summarizer"
	"github.com/staticvar/fetchmark/internal/api/dashboard"
	"github.com/staticvar/fetchmark/internal/api/middleware"
	"github.com/staticvar/fetchmark/internal/config"
	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/pipeline"
)

// Deps bundles the external collaborators an API server needs.
type Deps struct {
	Log      *slog.Logger
	Config   config.Config
	Pipeline PipelineRunner
	// Redis is optional; when set it backs cross-instance rate limiting.
	Redis *redis.Client
	// ReadyCheck reports whether hard dependencies (Redis, SearXNG) are
	// reachable. Returning nil means ready; non-nil is rendered as the
	// failure reason on /readyz.
	ReadyCheck func() error

	// Summarizers, when non-nil, backs /v1/summarize and the admin
	// config endpoints. A nil or empty registry turns summarize into
	// a 503 "not configured" response.
	Summarizers *summarizer.Registry
}

// PipelineRunner is the subset of *pipeline.Pipeline the API layer uses;
// kept as an interface to preserve handler testability.
type PipelineRunner interface {
	Search(ctx context.Context, o pipeline.Options) ([]model.SearchResult, error)
	Parse(ctx context.Context, o pipeline.Options) []model.SearchResult
}

// NewRouter wires the full HTTP surface for Fetchmark. It is invoked once
// from cmd/fetchmark and from integration tests.
func NewRouter(d Deps) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.Logger(d.Log))
	r.Use(middleware.Metrics)
	r.Use(chimw.Recoverer)

	r.Get("/healthz", healthz)
	r.Get("/readyz", readyz(d.ReadyCheck))
	r.Handle("/metrics", promhttp.Handler())

	r.Route("/v1", func(r chi.Router) {
		r.Use(middleware.APIKey(d.Config.APIKeys, d.Config.AdminAPIKeys))
		r.Use(middleware.RateLimiter(d.Config.RateLimitPerSec, d.Config.RateLimitBurst, d.Redis))
		r.Post("/search", searchHandler(d))
		r.Post("/parse", parseHandler(d))
		r.Post("/summarize", summarizeHandler(d))
	})

	// Admin surface. Mounted only when admin keys are configured so
	// there is no unauthenticated path to probe. The admin middleware
	// rejects non-admin callers with 403.
	if len(d.Config.AdminAPIKeys) > 0 {
		r.Route("/admin", func(r chi.Router) {
			r.Use(middleware.APIKey(d.Config.AdminAPIKeys, d.Config.AdminAPIKeys))
			r.Use(middleware.RateLimiter(d.Config.RateLimitPerSec, d.Config.RateLimitBurst, d.Redis))
			r.Get("/summarize/config", adminSummarizeGet(d))
			r.Put("/summarize/providers", adminSummarizeProviderPut(d))
			r.Delete("/summarize/providers/{name}", adminSummarizeProviderDelete(d))
			r.Put("/summarize/default", adminSummarizeDefaultPut(d))
		})
	}

	dashboard.Mount(r, d.Config.DashboardUser, d.Config.DashboardPassword, dashboard.Deps{
		Gatherer:   prometheus.DefaultGatherer,
		SearxngURL: d.Config.SearxngURL,
		RedisURL:   redactRedis(d.Config.RedisURL),
		Version:    "0.1",
	})

	return r
}

// redactRedis removes user:pass from a redis URL before it reaches the
// dashboard header so operators can screenshot the page without leaking
// credentials.
func redactRedis(raw string) string {
	if raw == "" {
		return ""
	}
	if i := strings.Index(raw, "@"); i >= 0 {
		if j := strings.Index(raw, "://"); j >= 0 && j+3 < i {
			return raw[:j+3] + "…@" + raw[i+1:]
		}
	}
	return raw
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
