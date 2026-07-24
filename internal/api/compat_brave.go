package api

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/staticvar/fetchmark/internal/api/middleware"
	"github.com/staticvar/fetchmark/internal/core/model"
)

type braveSearchResponse struct {
	Type  string             `json:"type"`
	Query braveQueryResponse `json:"query"`
	Web   braveWebResponse   `json:"web"`
}

type braveQueryResponse struct {
	Original             string `json:"original"`
	MoreResultsAvailable bool   `json:"more_results_available"`
}

type braveWebResponse struct {
	Type    string              `json:"type"`
	Results []braveSearchResult `json:"results"`
}

type braveSearchResult struct {
	Type           string `json:"type"`
	Title          string `json:"title"`
	URL            string `json:"url"`
	Description    string `json:"description"`
	PageAge        string `json:"page_age,omitempty"`
	Language       string `json:"language,omitempty"`
	FamilyFriendly bool   `json:"family_friendly,omitempty"`
}

func braveSearchHandler(d Deps) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		canonical, count, offset, err := translateBraveRequest(request.URL.Query(), d.Config.ResultsCap)
		if err != nil {
			writeBraveError(writer, request, http.StatusUnprocessableEntity, err.Error(), "INVALID_URL")
			return
		}
		executed, executionErr := executeCanonicalSearch(d, request, canonical)
		if executionErr != nil {
			writeBraveError(writer, request, executionErr.Status, executionErr.Error(), braveErrorCode(executionErr.Status))
			return
		}
		start := offset * count
		end := start + count
		if start > len(executed.Results) {
			start = len(executed.Results)
		}
		if end > len(executed.Results) {
			end = len(executed.Results)
		}
		page := executed.Results[start:end]
		response := braveSearchResponse{
			Type: "search",
			Query: braveQueryResponse{
				Original: canonical.Query, MoreResultsAvailable: len(executed.Results) > end,
			},
			Web: braveWebResponse{Type: "search", Results: mapBraveResults(page)},
		}
		writeJSONBoundedCustomWithSuccessHeaders(writer, http.StatusOK, response, d.Config.MaxRequestOutputBytes,
			http.StatusInsufficientStorage, braveErrorPayload(request, http.StatusInsufficientStorage, "response byte budget exceeded", "RESPONSE_TOO_LARGE"), func(header http.Header) {
				addDiscoveryAttributionHeaders(header, page)
			})
	}
}

func translateBraveRequest(query url.Values, cap int) (searchRequest, int, int, error) {
	if field := unsupportedBraveParameter(query); field != "" {
		return searchRequest{}, 0, 0, fmt.Errorf("%s is not supported", field)
	}
	q := strings.TrimSpace(query.Get("q"))
	if q == "" {
		return searchRequest{}, 0, 0, errors.New("q is required")
	}
	if utf8.RuneCountInString(q) > maxQueryRunes || len(strings.Fields(q)) > 50 {
		return searchRequest{}, 0, 0, errors.New("q must be at most 400 characters and 50 words")
	}
	count, err := braveIntParameter(query, "count", 20, 1, 20)
	if err != nil {
		return searchRequest{}, 0, 0, err
	}
	offset, err := braveIntParameter(query, "offset", 0, 0, 9)
	if err != nil {
		return searchRequest{}, 0, 0, err
	}
	requestedPageEnd := (offset + 1) * count
	if cap > 0 && requestedPageEnd > cap {
		return searchRequest{}, 0, 0, fmt.Errorf("requested page exceeds this Fetchmark instance result limit of %d", cap)
	}
	retrievalCount := requestedPageEnd
	if cap <= 0 || retrievalCount < cap {
		retrievalCount++
	}
	language := strings.ToLower(strings.TrimSpace(query.Get("search_lang")))
	if language == "" {
		language = "en"
	} else if !validBraveSearchLanguages[language] {
		return searchRequest{}, 0, 0, errors.New("search_lang is not a supported Brave language")
	}
	if country := strings.TrimSpace(query.Get("country")); country != "" {
		return searchRequest{}, 0, 0, errors.New("country filtering is not supported")
	}
	timeRange := ""
	switch freshness := strings.ToLower(strings.TrimSpace(query.Get("freshness"))); freshness {
	case "":
	case "pd":
		timeRange = "day"
	case "pm":
		timeRange = "month"
	case "py":
		timeRange = "year"
	case "pw":
		return searchRequest{}, 0, 0, errors.New("freshness=pw is not supported")
	default:
		return searchRequest{}, 0, 0, errors.New("custom freshness date ranges are not supported")
	}
	moderate := 1
	safeSearch := &moderate
	if raw := strings.ToLower(strings.TrimSpace(query.Get("safesearch"))); raw != "" {
		value := 0
		switch raw {
		case "off":
		case "moderate":
			value = 1
		case "strict":
			value = 2
		default:
			return searchRequest{}, 0, 0, errors.New("safesearch must be off, moderate, or strict")
		}
		safeSearch = &value
	}
	if raw := query.Get("spellcheck"); raw != "" {
		if _, err := strconv.ParseBool(raw); err != nil {
			return searchRequest{}, 0, 0, errors.New("spellcheck must be true or false")
		}
	}
	return searchRequest{
		Query: q, Language: language, TimeRange: timeRange, SafeSearch: safeSearch,
		MaxResults: retrievalCount, SearchDepth: "basic", Formats: []string{"json", "markdown", "html"},
	}, count, offset, nil
}

func unsupportedBraveParameter(query url.Values) string {
	supported := map[string]bool{
		"q": true, "count": true, "offset": true, "search_lang": true, "freshness": true,
		"safesearch": true, "spellcheck": true,
	}
	for key := range query {
		if !supported[key] {
			return key
		}
	}
	return ""
}

func braveIntParameter(query url.Values, name string, fallback, minimum, maximum int) (int, error) {
	raw := strings.TrimSpace(query.Get(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be between %d and %d", name, minimum, maximum)
	}
	return value, nil
}

func mapBraveResults(results []model.SearchResult) []braveSearchResult {
	mapped := make([]braveSearchResult, 0, len(results))
	for _, result := range results {
		language := ""
		if result.Content != nil {
			language = result.Content.Language
		}
		mapped = append(mapped, braveSearchResult{
			Type: "search_result", Title: titleOf(result), URL: result.URL, Description: resultSummary(result),
			PageAge: exaPublishedDate(result), Language: language,
		})
	}
	return mapped
}

func writeBraveError(writer http.ResponseWriter, request *http.Request, status int, detail, code string) {
	writeJSON(writer, status, braveErrorPayload(request, status, detail, code))
}

func braveErrorPayload(request *http.Request, status int, detail, code string) map[string]any {
	return map[string]any{
		"type": "ErrorResponse",
		"error": map[string]any{
			"id": middleware.IDFrom(request.Context()), "status": status, "detail": detail,
			"meta": map[string]any{}, "code": code,
		},
		"time": time.Now().Unix(),
	}
}

func braveAuthError(writer http.ResponseWriter, request *http.Request, status int, code string) {
	detail := "missing or invalid subscription token"
	if code == "missing_api_key" {
		detail = "missing subscription token"
	}
	writeJSON(writer, status, map[string]any{
		"type":  "ErrorResponse",
		"error": map[string]any{"id": middleware.IDFrom(request.Context()), "status": status, "detail": detail, "meta": map[string]any{}, "code": "SUBSCRIPTION_TOKEN_INVALID"},
		"time":  time.Now().Unix(),
	})
}

var validBraveSearchLanguages = map[string]bool{
	"ar": true, "eu": true, "bn": true, "bg": true, "ca": true, "zh-hans": true, "zh-hant": true,
	"hr": true, "cs": true, "da": true, "nl": true, "en": true, "en-gb": true, "et": true,
	"fi": true, "fr": true, "gl": true, "de": true, "el": true, "gu": true, "he": true,
	"hi": true, "hu": true, "is": true, "it": true, "ja": true, "jp": true, "kn": true,
	"ko": true, "lv": true, "lt": true, "ms": true, "ml": true, "mr": true, "nb": true,
	"pl": true, "pt-br": true, "pt-pt": true, "pa": true, "ro": true, "ru": true, "sr": true,
	"sk": true, "sl": true, "es": true, "sv": true, "ta": true, "te": true, "th": true,
	"tr": true, "uk": true, "vi": true,
}

func braveErrorCode(status int) string {
	if status == http.StatusTooManyRequests {
		return "RATE_LIMITED"
	}
	return "INTERNAL"
}
