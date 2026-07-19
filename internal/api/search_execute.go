package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/search"
	"github.com/staticvar/fetchmark/internal/obs"
)

// canonicalSearchResponse is the single internal result consumed by the
// native API and every vendor compatibility translator.
type canonicalSearchResponse struct {
	Query     string
	Results   []model.SearchResult
	Discovery *search.DiscoveryReport
}

type searchExecutionError struct {
	Status    int
	Code      string
	Cause     error
	Discovery *search.DiscoveryReport
}

func (e *searchExecutionError) Error() string {
	if e.Cause != nil {
		return e.Cause.Error()
	}
	return e.Code
}

// executeCanonicalSearch owns validation, option construction, caps, metrics,
// and the pipeline call. Wire-specific handlers only decode/translate before
// this boundary and encode/translate after it.
func executeCanonicalSearch(d Deps, r *http.Request, req searchRequest) (canonicalSearchResponse, *searchExecutionError) {
	req.Query = strings.TrimSpace(req.Query)
	if req.Query == "" {
		return canonicalSearchResponse{}, &searchExecutionError{Status: http.StatusBadRequest, Code: "query_required"}
	}
	if err := validateSearchControls(req); err != nil {
		return canonicalSearchResponse{}, &searchExecutionError{Status: http.StatusBadRequest, Code: "invalid_request", Cause: err}
	}
	normalizeSearchControls(&req)
	if d.Pipeline == nil {
		return canonicalSearchResponse{}, &searchExecutionError{Status: http.StatusServiceUnavailable, Code: "pipeline_not_ready"}
	}

	maxResults := req.MaxResults
	if maxResults <= 0 {
		maxResults = d.Config.MaxResults
	}
	if maxResults <= 0 {
		maxResults = 10
	}
	if d.Config.ResultsCap > 0 && maxResults > d.Config.ResultsCap {
		maxResults = d.Config.ResultsCap
	}
	opts, err := buildOptions(r, d.Config.RespectRobots, req.ProxyURL, "", req.RespectRobots,
		req.TimeoutMS, maxResults, req.Formats, req.Engines, req.Query, nil, req.Render,
		req.Categories, req.Language, req.TimeRange, req.SafeSearch, req.IncludeDomains,
		req.ExcludeDomains, req.ExactMatch, req.SearchDepth, req.ChunksPerSource)
	if err != nil {
		return canonicalSearchResponse{}, &searchExecutionError{Status: http.StatusForbidden, Code: "forbidden", Cause: err}
	}
	candidateCap := maxResults * candidateMultiplier(req.SearchDepth)
	if candidateCap < maxResults {
		candidateCap = maxResults
	}
	if d.Config.ResultsCap > 0 && candidateCap > d.Config.ResultsCap {
		candidateCap = d.Config.ResultsCap
	}
	opts.CandidateCap = candidateCap

	var results []model.SearchResult
	var discoveryReport *search.DiscoveryReport
	var searchErr error
	if detailed, ok := d.Pipeline.(DetailedPipelineRunner); ok {
		output, err := detailed.SearchDetailed(r.Context(), opts)
		results, searchErr = output.Results, err
		if report, validationErr := search.ValidatedDiscoveryReport(output.Discovery); validationErr == nil {
			discoveryReport = &report
		} else if d.Log != nil {
			d.Log.Warn("dropping invalid discovery report")
		}
	} else {
		results, searchErr = d.Pipeline.Search(r.Context(), opts)
	}
	if searchErr != nil {
		var unsupported *search.UnsupportedControlError
		if errors.As(searchErr, &unsupported) {
			return canonicalSearchResponse{}, &searchExecutionError{Status: http.StatusBadRequest, Code: "unsupported_control", Cause: unsupported, Discovery: discoveryReport}
		}
		obs.SearchQueryTotal.WithLabelValues("upstream_error").Inc()
		if d.Log != nil {
			d.Log.Error("search failed", "err", searchErr)
		}
		return canonicalSearchResponse{}, &searchExecutionError{Status: http.StatusBadGateway, Code: "search_failed", Cause: searchErr, Discovery: discoveryReport}
	}
	if len(results) == 0 {
		obs.SearchQueryTotal.WithLabelValues("empty").Inc()
	} else {
		obs.SearchQueryTotal.WithLabelValues("ok").Inc()
	}
	return canonicalSearchResponse{Query: req.Query, Results: results, Discovery: discoveryReport}, nil
}

func nativeSearchError(err *searchExecutionError) map[string]any {
	if err == nil {
		return nil
	}
	payload := map[string]any{}
	switch err.Code {
	case "query_required":
		payload["error"] = "query required"
	case "invalid_request", "forbidden":
		payload["error"] = err.Error()
	case "unsupported_control":
		control := ""
		var unsupported *search.UnsupportedControlError
		if errors.As(err.Cause, &unsupported) {
			control = unsupported.Control
		}
		payload["error"] = "unsupported_control"
		payload["control"] = control
		payload["description"] = err.Error()
	default:
		payload["error"] = err.Code
	}
	if err.Discovery != nil {
		payload["discovery"] = err.Discovery
	}
	return payload
}

var _ error = (*searchExecutionError)(nil)
