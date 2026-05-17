package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/staticvar/fetchmark/internal/api/middleware"
	"github.com/staticvar/fetchmark/internal/core/pipeline"
	"github.com/staticvar/fetchmark/internal/obs"
)

// searchRequest is the JSON body for POST /v1/search.
type searchRequest struct {
	Query         string   `json:"query"`
	Engines       []string `json:"engines,omitempty"`
	Categories    []string `json:"categories,omitempty"`
	Language      string   `json:"language,omitempty"`
	TimeRange     string   `json:"time_range,omitempty"`
	MaxResults    int      `json:"max_results,omitempty"`
	Formats       []string `json:"formats,omitempty"`
	TimeoutMS     int      `json:"timeout_ms,omitempty"`
	RespectRobots *bool    `json:"respect_robots,omitempty"`
	ProxyURL      string   `json:"proxy_url,omitempty"`
	Render        *bool    `json:"render,omitempty"`
}

// parseRequest is the JSON body for POST /v1/parse.
type parseRequest struct {
	URLs          []string `json:"urls"`
	Query         string   `json:"query,omitempty"`
	Formats       []string `json:"formats,omitempty"`
	TimeoutMS     int      `json:"timeout_ms,omitempty"`
	RespectRobots *bool    `json:"respect_robots,omitempty"`
	ProxyURL      string   `json:"proxy_url,omitempty"`
	Render        *bool    `json:"render,omitempty"`
}

// errBadRequest is used by decodeJSON to signal client-side failures.
var errBadRequest = errors.New("bad_request")

func decodeJSON(r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(nil, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return errBadRequest
	}
	return nil
}

// buildOptions maps a request's common options onto pipeline.Options and
// enforces admin-only gates on proxy_url and respect_robots=false.
func buildOptions(r *http.Request, defaultRobots bool, proxy, ua string, respect *bool, timeoutMS int, maxResults int, formats, engines []string, query string, urls []string, render *bool, categories []string, language, timeRange string) (pipeline.Options, error) {
	p := middleware.PrincipalFrom(r.Context())
	opts := pipeline.Options{
		Query:         query,
		URLs:          urls,
		Engines:       engines,
		Categories:    categories,
		Language:      language,
		TimeRange:     timeRange,
		MaxResults:    maxResults,
		Formats:       formats,
		AdminRequest:  p.Admin,
		RespectRobots: defaultRobots,
		UserAgent:     ua,
	}
	if timeoutMS > 0 {
		opts.Timeout = time.Duration(timeoutMS) * time.Millisecond
	}
	if respect != nil {
		// Disabling robots requires admin; enabling is fine for anyone.
		if !*respect && !p.Admin {
			return opts, errors.New("forbidden: respect_robots=false requires admin key")
		}
		opts.RespectRobots = *respect
	}
	if proxy != "" {
		if !p.Admin {
			return opts, errors.New("forbidden: proxy_url requires admin key")
		}
		opts.ProxyURL = proxy
	}
	if render != nil {
		opts.Render = *render
	}
	return opts, nil
}

func searchHandler(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req searchRequest
		if err := decodeJSON(r, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request"})
			return
		}
		if req.Query == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query required"})
			return
		}
		if d.Pipeline == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "pipeline_not_ready"})
			return
		}

		max := req.MaxResults
		if max <= 0 {
			max = d.Config.MaxResults
		}
		if max > d.Config.ResultsCap {
			max = d.Config.ResultsCap
		}
		opts, err := buildOptions(r, d.Config.RespectRobots, req.ProxyURL, "", req.RespectRobots,
			req.TimeoutMS, max, req.Formats, req.Engines, req.Query, nil, req.Render, req.Categories, req.Language, req.TimeRange)
		if err != nil {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			return
		}
		candidateCap := max * 3
		if candidateCap < max {
			candidateCap = max
		}
		if candidateCap > d.Config.ResultsCap {
			candidateCap = d.Config.ResultsCap
		}
		opts.CandidateCap = candidateCap

		out, err := d.Pipeline.Search(r.Context(), opts)
		if err != nil {
			obs.SearchQueryTotal.WithLabelValues("upstream_error").Inc()
			d.Log.Error("search failed", "err", err)
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "search_failed"})
			return
		}
		if len(out) == 0 {
			obs.SearchQueryTotal.WithLabelValues("empty").Inc()
		} else {
			obs.SearchQueryTotal.WithLabelValues("ok").Inc()
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"query":   req.Query,
			"count":   len(out),
			"results": out,
		})
	}
}

func parseHandler(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req parseRequest
		if err := decodeJSON(r, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request"})
			return
		}
		if len(req.URLs) == 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "urls required"})
			return
		}
		if cap := d.Config.ResultsCap; cap > 0 && len(req.URLs) > cap {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error":       "too_many_urls",
				"limit":       cap,
				"urls_given":  len(req.URLs),
				"description": "request cap is FM_RESULTS_CAP",
			})
			return
		}
		if d.Pipeline == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "pipeline_not_ready"})
			return
		}
		opts, err := buildOptions(r, d.Config.RespectRobots, req.ProxyURL, "", req.RespectRobots,
			req.TimeoutMS, 0, req.Formats, nil, req.Query, req.URLs, req.Render, nil, "", "")
		if err != nil {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			return
		}
		out := d.Pipeline.Parse(r.Context(), opts)
		writeJSON(w, http.StatusOK, map[string]any{
			"count":   len(out),
			"results": out,
		})
	}
}

func summarizeHandlerStub(_ Deps) http.HandlerFunc {
	// Retained only to keep the old stub referenced if the new handler
	// is removed for a rollback. Not wired by NewRouter.
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusNotImplemented, map[string]any{"error": "not_implemented"})
	}
}
