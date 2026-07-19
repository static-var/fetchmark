package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	"github.com/staticvar/fetchmark/internal/buildidentity"
	"github.com/staticvar/fetchmark/internal/config"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/pipeline"
	"github.com/staticvar/fetchmark/internal/evaluationmanifest"
)

// Deps bundles the external collaborators an API server needs.
type Deps struct {
	Log      *slog.Logger
	Config   config.Config
	Pipeline PipelineRunner
	Version  string
	// BuildSHA256 identifies the executable artifact resolved at process
	// startup. Invalid or absent values are omitted from search responses.
	BuildSHA256 string
	// EvaluationConfiguration is a bounded non-secret manifest of the
	// resolved controls that affect search evaluation. Its digest is emitted on
	// native search responses so artifacts can prove one configuration.
	EvaluationConfiguration    []byte
	EvaluationConfigurationSHA string
	// Redis is optional; when set it backs cross-instance rate limiting.
	Redis *redis.Client
	// ReadyCheck reports whether configured hard dependencies are reachable.
	// Redis is checked when active; SearXNG is checked only when it is the
	// configured primary. Returning nil means ready; non-nil is rendered as the
	// failure reason on /readyz.
	ReadyCheck func() error

	// Summarizers, when non-nil, backs /v1/summarize and the admin
	// config endpoints. A nil or empty registry turns summarize into
	// a 503 "not configured" response.
	Summarizers *summarizer.Registry
	// CorpusCurator is the explicit admin-only mutation boundary. It remains
	// separate from PipelineRunner so ordinary native and compatibility routes
	// cannot request curated persistence.
	CorpusCurator CorpusCurator
}

type CorpusCurator interface {
	AdmitCurated(context.Context, []string) []pipeline.CuratedMutationResult
	AdmitCuratedClassified(context.Context, []string, localcorpus.SafetyClassification) []pipeline.CuratedMutationResult
	AdmitCuratedFocused(context.Context, pipeline.FocusedAdmission) []pipeline.CuratedMutationResult
	TakedownCurated(context.Context, []string) []pipeline.CuratedMutationResult
}

// PipelineRunner is the subset of *pipeline.Pipeline the API layer uses;
// kept as an interface to preserve handler testability.
type PipelineRunner interface {
	Search(ctx context.Context, o pipeline.Options) ([]model.SearchResult, error)
	Parse(ctx context.Context, o pipeline.Options) []model.SearchResult
}

// DetailedPipelineRunner is an optional additive search contract. Keeping it
// separate preserves test and adapter compatibility while production retains
// provider-aware discovery evidence.
type DetailedPipelineRunner interface {
	SearchDetailed(ctx context.Context, o pipeline.Options) (pipeline.SearchOutput, error)
}

// NewRouter wires the full HTTP surface for Fetchmark. It is invoked once
// from cmd/fetchmark and from integration tests.
func NewRouter(d Deps) http.Handler {
	r := chi.NewRouter()

	if buildSHA256, ok := buildidentity.Parse(d.BuildSHA256); ok {
		r.Use(nativeSearchResponseHeader(buildSHA256))
	}
	if _, configurationSHA256, ok := evaluationConfigurationEvidence(d); ok {
		r.Use(nativeSearchConfigurationHeader(configurationSHA256))
	}
	r.Use(middleware.RequestID)
	r.Use(middleware.Logger(d.Log))
	r.Use(middleware.Metrics)
	r.Use(chimw.Recoverer)
	rateLimits := middleware.NewRateLimiterGroup(d.Config.RateLimitPerSec, d.Config.RateLimitBurst, d.Redis)

	r.Get("/healthz", healthz)
	r.Get("/readyz", readyz(d.ReadyCheck))
	r.Handle("/metrics", promhttp.Handler())

	r.Route("/v1", func(r chi.Router) {
		r.Use(middleware.APIKey(d.Config.APIKeys, d.Config.AdminAPIKeys))
		r.Use(rateLimits.Middleware(nil))
		r.Post("/search", searchHandler(d))
		r.Get("/evaluation/configuration", evaluationConfigurationHandler(d))
		r.Post("/parse", parseHandler(d))
		r.Post("/summarize", summarizeHandler(d))
	})

	r.Route("/compat/tavily", func(r chi.Router) {
		r.Use(middleware.APIKeyCustom(d.Config.APIKeys, d.Config.AdminAPIKeys, bearerKey, tavilyAuthError))
		r.Use(rateLimits.Middleware(func(w http.ResponseWriter, _ *http.Request) {
			writeTavilyError(w, http.StatusTooManyRequests, "rate limit exceeded")
		}))
		r.Post("/search", tavilySearchHandler(d))
	})

	r.Route("/compat/exa", func(r chi.Router) {
		r.Use(middleware.APIKeyCustom(d.Config.APIKeys, d.Config.AdminAPIKeys, exaAPIKey, exaAuthError))
		r.Use(rateLimits.Middleware(func(w http.ResponseWriter, request *http.Request) {
			writeExaError(w, request, http.StatusTooManyRequests, "rate limit exceeded", "RATE_LIMITED")
		}))
		r.Post("/search", exaSearchHandler(d))
		r.Post("/contents", exaContentsHandler(d))
	})

	r.Route("/compat/brave", func(r chi.Router) {
		r.Use(middleware.APIKeyCustom(d.Config.APIKeys, d.Config.AdminAPIKeys, headerKey("X-Subscription-Token"), braveAuthError))
		r.Use(rateLimits.Middleware(func(w http.ResponseWriter, request *http.Request) {
			writeBraveError(w, request, http.StatusTooManyRequests, "rate limit exceeded", "RATE_LIMITED")
		}))
		r.Get("/res/v1/web/search", braveSearchHandler(d))
	})

	// Admin surface. Mounted only when admin keys are configured so
	// there is no unauthenticated path to probe. The admin middleware
	// rejects non-admin callers with 403.
	if len(d.Config.AdminAPIKeys) > 0 {
		r.Route("/admin", func(r chi.Router) {
			r.Use(middleware.APIKey(d.Config.AdminAPIKeys, d.Config.AdminAPIKeys))
			r.Use(rateLimits.Middleware(nil))
			r.Get("/summarize/config", adminSummarizeGet(d))
			r.Put("/summarize/providers", adminSummarizeProviderPut(d))
			r.Delete("/summarize/providers/{name}", adminSummarizeProviderDelete(d))
			r.Put("/summarize/default", adminSummarizeDefaultPut(d))
			r.Post("/corpus/admissions", adminCorpusAdmission(d))
			r.Post("/corpus/focused-admissions", adminCorpusFocusedAdmission(d))
			r.Post("/corpus/takedowns", adminCorpusTakedown(d))
		})
	}

	version := d.Version
	if version == "" {
		version = "dev"
	}
	redisMode := redactRedis(d.Config.RedisURL)
	if d.Redis == nil {
		redisMode = "in-memory fallback"
	}
	dashboard.Mount(r, d.Config.DashboardUser, d.Config.DashboardPassword, dashboard.Deps{
		Gatherer:    prometheus.DefaultGatherer,
		SearxngURL:  d.Config.SearxngURL,
		RedisURL:    redisMode,
		Version:     version,
		Summarizers: summarizerDashboardView(d.Summarizers),
	})

	return r
}

func nativeSearchResponseHeader(value string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.URL.Path == "/v1/search" {
				writer.Header().Set(buildidentity.HeaderName, value)
			}
			next.ServeHTTP(writer, request)
		})
	}
}

func nativeSearchConfigurationHeader(value string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.URL.Path == "/v1/search" {
				writer.Header().Set(evaluationmanifest.HeaderName, value)
			}
			next.ServeHTTP(writer, request)
		})
	}
}

// summarizerDashboardView adapts *summarizer.Registry to the narrow
// dashboard.SummarizerView interface without leaking API keys. We do
// the type-level wiring here (not in dashboard/) so the dashboard
// package stays free of the summarizer import.
func summarizerDashboardView(reg *summarizer.Registry) dashboard.SummarizerView {
	if reg == nil {
		return nil
	}
	return summarizerAdapter{reg: reg}
}

type summarizerAdapter struct{ reg *summarizer.Registry }

func (s summarizerAdapter) Snapshot() (out []dashboard.SummarizerProvider, def string) {
	cfgs, d := s.reg.Snapshot()
	out = make([]dashboard.SummarizerProvider, 0, len(cfgs))
	for _, c := range cfgs {
		out = append(out, dashboard.SummarizerProvider{
			Name:    c.Name,
			Kind:    string(c.Kind),
			BaseURL: c.BaseURL,
			Model:   c.Model,
		})
	}
	return out, d
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

var errResponseBudget = errors.New("response exceeds byte budget")

type limitedBuffer struct {
	bytes.Buffer
	limit int64
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.limit > 0 && int64(b.Len()+len(p)) > b.limit {
		return 0, errResponseBudget
	}
	return b.Buffer.Write(p)
}

func writeJSONBounded(w http.ResponseWriter, status int, v any, maxBytes int64) {
	writeJSONBoundedWithSuccessHeaders(w, status, v, maxBytes, nil)
}

func writeJSONBoundedWithSuccessHeaders(w http.ResponseWriter, status int, v any, maxBytes int64, successHeaders func(http.Header)) {
	writeJSONBoundedCustomWithSuccessHeaders(w, status, v, maxBytes, http.StatusInsufficientStorage, map[string]string{"error": "response_byte_budget"}, successHeaders)
}

func writeJSONBoundedCustom(w http.ResponseWriter, status int, v any, maxBytes int64, fallbackStatus int, fallback any) {
	writeJSONBoundedCustomWithSuccessHeaders(w, status, v, maxBytes, fallbackStatus, fallback, nil)
}

func writeJSONBoundedCustomWithSuccessHeaders(w http.ResponseWriter, status int, v any, maxBytes int64, fallbackStatus int, fallback any, successHeaders func(http.Header)) {
	if maxBytes <= 0 {
		if successHeaders != nil {
			successHeaders(w.Header())
		}
		writeJSON(w, status, v)
		return
	}
	var body limitedBuffer
	body.limit = maxBytes
	if err := json.NewEncoder(&body).Encode(v); err != nil {
		var fallbackBody limitedBuffer
		fallbackBody.limit = maxBytes
		if encodeErr := json.NewEncoder(&fallbackBody).Encode(fallback); encodeErr != nil {
			fallbackBody.Reset()
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(fallbackStatus)
		_, _ = w.Write(fallbackBody.Bytes())
		return
	}
	if successHeaders != nil {
		successHeaders(w.Header())
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body.Bytes())
}
