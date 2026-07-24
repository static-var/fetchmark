package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/adapters/fetcher"
	"github.com/staticvar/fetchmark/internal/api/middleware"
	"github.com/staticvar/fetchmark/internal/core/model"
)

type exaSearchRequest struct {
	Query              string              `json:"query"`
	IncludeDomains     []string            `json:"includeDomains,omitempty"`
	ExcludeDomains     []string            `json:"excludeDomains,omitempty"`
	NumResults         *int                `json:"numResults,omitempty"`
	Type               string              `json:"type,omitempty"`
	Contents           *exaContentsOptions `json:"contents,omitempty"`
	Category           json.RawMessage     `json:"category,omitempty"`
	StartCrawlDate     json.RawMessage     `json:"startCrawlDate,omitempty"`
	EndCrawlDate       json.RawMessage     `json:"endCrawlDate,omitempty"`
	StartPublishedDate json.RawMessage     `json:"startPublishedDate,omitempty"`
	EndPublishedDate   json.RawMessage     `json:"endPublishedDate,omitempty"`
	Context            json.RawMessage     `json:"context,omitempty"`
	Moderation         json.RawMessage     `json:"moderation,omitempty"`
	AdditionalQueries  json.RawMessage     `json:"additionalQueries,omitempty"`
	UserLocation       json.RawMessage     `json:"userLocation,omitempty"`
	Compliance         json.RawMessage     `json:"compliance,omitempty"`
	OutputSchema       json.RawMessage     `json:"outputSchema,omitempty"`
	SystemPrompt       json.RawMessage     `json:"systemPrompt,omitempty"`
	Stream             json.RawMessage     `json:"stream,omitempty"`
}

type exaContentsOptions struct {
	Text             exaContentControl `json:"text,omitempty"`
	Highlights       exaContentControl `json:"highlights,omitempty"`
	Summary          json.RawMessage   `json:"summary,omitempty"`
	Subpages         json.RawMessage   `json:"subpages,omitempty"`
	SubpageTarget    json.RawMessage   `json:"subpageTarget,omitempty"`
	Extras           json.RawMessage   `json:"extras,omitempty"`
	Livecrawl        json.RawMessage   `json:"livecrawl,omitempty"`
	LivecrawlTimeout json.RawMessage   `json:"livecrawlTimeout,omitempty"`
	MaxAgeHours      json.RawMessage   `json:"maxAgeHours,omitempty"`
}

type exaContentControl struct {
	Set           bool
	Enabled       bool
	MaxCharacters int
	Query         string
}

type exaRequestError struct {
	tag     string
	message string
}

func (err *exaRequestError) Error() string { return err.message }

func newExaRequestError(tag, message string) error {
	return &exaRequestError{tag: tag, message: message}
}

func exaRequestErrorTag(err error) string {
	var requestErr *exaRequestError
	if errors.As(err, &requestErr) {
		return requestErr.tag
	}
	return "INVALID_REQUEST_BODY"
}

func (control *exaContentControl) UnmarshalJSON(raw []byte) error {
	control.Set = true
	if bytes.Equal(raw, []byte("true")) {
		control.Enabled = true
		return nil
	}
	if bytes.Equal(raw, []byte("false")) {
		return nil
	}
	if bytes.Equal(raw, []byte("null")) {
		return nil
	}
	var value struct {
		MaxCharacters *int   `json:"maxCharacters,omitempty"`
		Query         string `json:"query,omitempty"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return errors.New("must be a boolean or supported options object")
	}
	if value.MaxCharacters != nil && (*value.MaxCharacters < 1 || *value.MaxCharacters > 10_000) {
		return errors.New("maxCharacters must be between 1 and 10000")
	}
	control.Enabled = true
	if value.MaxCharacters != nil {
		control.MaxCharacters = *value.MaxCharacters
	}
	control.Query = strings.TrimSpace(value.Query)
	return nil
}

type exaSearchResponse struct {
	RequestID string            `json:"requestId"`
	Results   []exaSearchResult `json:"results"`
}

type exaSearchResult struct {
	Title           string    `json:"title"`
	URL             string    `json:"url"`
	PublishedDate   string    `json:"publishedDate,omitempty"`
	Author          string    `json:"author,omitempty"`
	ID              string    `json:"id"`
	Text            *string   `json:"text,omitempty"`
	Highlights      []string  `json:"highlights,omitempty"`
	HighlightScores []float64 `json:"highlightScores,omitempty"`
}

type exaContentsRequest struct {
	IDs         *[]string         `json:"ids,omitempty"`
	URLs        *[]string         `json:"urls,omitempty"`
	Text        exaContentControl `json:"text,omitempty"`
	Highlights  exaContentControl `json:"highlights,omitempty"`
	Summary     json.RawMessage   `json:"summary,omitempty"`
	Extras      json.RawMessage   `json:"extras,omitempty"`
	Subpages    json.RawMessage   `json:"subpages,omitempty"`
	Livecrawl   json.RawMessage   `json:"livecrawl,omitempty"`
	MaxAgeHours json.RawMessage   `json:"maxAgeHours,omitempty"`
}

type exaContentsResponse struct {
	RequestID string             `json:"requestId"`
	Results   []exaContentResult `json:"results"`
	Statuses  []exaContentStatus `json:"statuses"`
}

type exaContentResult struct {
	ID              string    `json:"id"`
	URL             string    `json:"url"`
	Title           string    `json:"title,omitempty"`
	PublishedDate   string    `json:"publishedDate,omitempty"`
	Author          string    `json:"author,omitempty"`
	Text            *string   `json:"text,omitempty"`
	Highlights      []string  `json:"highlights,omitempty"`
	HighlightScores []float64 `json:"highlightScores,omitempty"`
}

type exaContentStatus struct {
	ID     string          `json:"id"`
	Status string          `json:"status"`
	Source string          `json:"source,omitempty"`
	Error  *exaStatusError `json:"error,omitempty"`
}

type exaStatusError struct {
	Tag            string `json:"tag"`
	HTTPStatusCode *int   `json:"httpStatusCode"`
}

func exaSearchHandler(d Deps) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		var vendorRequest exaSearchRequest
		if err := decodeExaJSON(request, &vendorRequest); err != nil {
			writeExaError(writer, request, http.StatusBadRequest, "invalid request body", "INVALID_REQUEST_BODY")
			return
		}
		canonical, err := translateExaSearchRequest(vendorRequest, d.Config.ResultsCap)
		if err != nil {
			writeExaError(writer, request, http.StatusBadRequest, err.Error(), exaRequestErrorTag(err))
			return
		}
		executed, executionErr := executeCanonicalSearch(d, request, canonical)
		if executionErr != nil {
			writeExaError(writer, request, executionErr.Status, exaExecutionErrorMessage(executionErr), exaErrorTag(executionErr.Status))
			return
		}
		response := exaSearchResponse{RequestID: middleware.IDFrom(request.Context())}
		response.Results = make([]exaSearchResult, 0, len(executed.Results))
		for _, result := range executed.Results {
			response.Results = append(response.Results, mapExaSearchResult(result, vendorRequest.Contents))
		}
		writeJSONBoundedCustomWithSuccessHeaders(writer, http.StatusOK, response, d.Config.MaxRequestOutputBytes,
			http.StatusInsufficientStorage, exaErrorPayload(request, "response byte budget exceeded", "RESPONSE_TOO_LARGE"), func(header http.Header) {
				addDiscoveryAttributionHeaders(header, executed.Results)
			})
	}
}

func translateExaSearchRequest(request exaSearchRequest, cap int) (searchRequest, error) {
	if strings.TrimSpace(request.Query) == "" {
		return searchRequest{}, newExaRequestError("INVALID_REQUEST_BODY", "query is required")
	}
	if field := exaUnsupportedSearchField(request); field != "" {
		return searchRequest{}, newExaRequestError("INVALID_REQUEST_BODY", fmt.Sprintf("%s is not supported", field))
	}
	searchType := strings.ToLower(strings.TrimSpace(request.Type))
	if searchType != "" && searchType != "auto" {
		return searchRequest{}, newExaRequestError("INVALID_REQUEST_BODY", "type must be auto; semantic Exa search modes are not supported")
	}
	count := 10
	if request.NumResults != nil {
		count = *request.NumResults
	}
	if count < 1 || count > 100 {
		return searchRequest{}, newExaRequestError("INVALID_NUM_RESULTS", "numResults must be between 1 and 100")
	}
	if cap > 0 && count > cap {
		return searchRequest{}, newExaRequestError("NUM_RESULTS_EXCEEDED", fmt.Sprintf("numResults exceeds this Fetchmark instance limit of %d", cap))
	}
	if len(request.IncludeDomains) > 1200 || len(request.ExcludeDomains) > 1200 {
		return searchRequest{}, newExaRequestError("INVALID_REQUEST_BODY", "includeDomains and excludeDomains cannot contain more than 1200 entries")
	}
	chunks := 0
	if request.Contents != nil {
		if exaFreshnessControlsConflict(request.Contents.Livecrawl, request.Contents.MaxAgeHours) {
			return searchRequest{}, newExaRequestError("INVALID_REQUEST", "contents.livecrawl and contents.maxAgeHours cannot be used together")
		}
		if field := request.Contents.unsupportedField(); field != "" {
			return searchRequest{}, newExaRequestError("INVALID_REQUEST_BODY", fmt.Sprintf("contents.%s is not supported", field))
		}
		if request.Contents.Highlights.Enabled {
			if request.Contents.Highlights.Query != "" && request.Contents.Highlights.Query != strings.TrimSpace(request.Query) {
				return searchRequest{}, newExaRequestError("INVALID_REQUEST", "contents.highlights.query must match query on this compatibility route")
			}
			chunks = 3
		}
	}
	return searchRequest{
		Query: request.Query, IncludeDomains: request.IncludeDomains, ExcludeDomains: request.ExcludeDomains,
		MaxResults: count, SearchDepth: "basic", ChunksPerSource: chunks,
		Formats: []string{"json", "markdown", "html"},
	}, nil
}

func exaUnsupportedSearchField(request exaSearchRequest) string {
	fields := []struct {
		name string
		raw  json.RawMessage
	}{
		{"category", request.Category}, {"startCrawlDate", request.StartCrawlDate}, {"endCrawlDate", request.EndCrawlDate},
		{"startPublishedDate", request.StartPublishedDate}, {"endPublishedDate", request.EndPublishedDate}, {"context", request.Context},
		{"moderation", request.Moderation}, {"additionalQueries", request.AdditionalQueries}, {"userLocation", request.UserLocation},
		{"compliance", request.Compliance}, {"outputSchema", request.OutputSchema}, {"systemPrompt", request.SystemPrompt}, {"stream", request.Stream},
	}
	for _, field := range fields {
		if rawValueSet(field.raw) {
			return field.name
		}
	}
	return ""
}

func (options exaContentsOptions) unsupportedField() string {
	fields := []struct {
		name string
		raw  json.RawMessage
	}{
		{"summary", options.Summary}, {"subpages", options.Subpages}, {"subpageTarget", options.SubpageTarget},
		{"extras", options.Extras}, {"livecrawl", options.Livecrawl}, {"livecrawlTimeout", options.LivecrawlTimeout}, {"maxAgeHours", options.MaxAgeHours},
	}
	for _, field := range fields {
		if rawValueSet(field.raw) {
			return field.name
		}
	}
	return ""
}

func rawValueSet(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null"))
}

func exaFreshnessControlsConflict(livecrawl, maxAgeHours json.RawMessage) bool {
	return rawValueSet(livecrawl) && rawValueSet(maxAgeHours)
}

func mapExaSearchResult(result model.SearchResult, contents *exaContentsOptions) exaSearchResult {
	mapped := exaSearchResult{
		Title: titleOf(result), URL: result.URL, ID: result.URL, Author: authorOf(result), PublishedDate: exaPublishedDate(result),
	}
	if contents == nil {
		return mapped
	}
	if contents.Text.Enabled {
		if extracted := resultExtractedText(result); extracted != "" {
			text := truncateCompatText(extracted, contents.Text.MaxCharacters)
			mapped.Text = &text
		}
	}
	if contents.Highlights.Enabled {
		mapped.Highlights, mapped.HighlightScores = exaHighlights(result, contents.Highlights.MaxCharacters)
	}
	return mapped
}

func exaContentsHandler(d Deps) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		var vendorRequest exaContentsRequest
		if err := decodeExaJSON(request, &vendorRequest); err != nil {
			writeExaError(writer, request, http.StatusBadRequest, "invalid request body", "INVALID_REQUEST_BODY")
			return
		}
		urls, err := validateExaContentsRequest(vendorRequest, d.Config.ResultsCap)
		if err != nil {
			writeExaError(writer, request, http.StatusBadRequest, err.Error(), exaRequestErrorTag(err))
			return
		}
		if d.Pipeline == nil {
			writeExaError(writer, request, http.StatusServiceUnavailable, "pipeline not ready", "INTERNAL_ERROR")
			return
		}
		query := strings.TrimSpace(vendorRequest.Highlights.Query)
		formats := []string{"json", "markdown", "html"}
		chunksPerSource := 0
		if vendorRequest.Highlights.Enabled {
			chunksPerSource = 3
		}
		options, optionErr := buildOptions(request, d.Config.RespectRobots, "", "", nil, 0, 0, formats, nil, query, urls, nil, nil, "", "", nil, nil, nil, false, "", chunksPerSource)
		if optionErr != nil {
			writeExaError(writer, request, http.StatusForbidden, optionErr.Error(), "INVALID_REQUEST")
			return
		}
		options.PreserveURLResults = true
		parsed := d.Pipeline.Parse(request.Context(), options)
		response := mapExaContentsResponse(middleware.IDFrom(request.Context()), urls, parsed, vendorRequest)
		writeJSONBoundedCustom(writer, http.StatusOK, response, d.Config.MaxRequestOutputBytes,
			http.StatusInsufficientStorage, exaErrorPayload(request, "response byte budget exceeded", "RESPONSE_TOO_LARGE"))
	}
}

func validateExaContentsRequest(request exaContentsRequest, cap int) ([]string, error) {
	if request.IDs == nil && request.URLs == nil {
		return nil, newExaRequestError("INVALID_REQUEST_BODY", "one of ids or urls is required")
	}
	if request.IDs != nil && request.URLs != nil {
		return nil, newExaRequestError("INVALID_REQUEST", "provide exactly one of ids or urls")
	}
	if exaFreshnessControlsConflict(request.Livecrawl, request.MaxAgeHours) {
		return nil, newExaRequestError("INVALID_REQUEST", "livecrawl and maxAgeHours cannot be used together")
	}
	if rawValueSet(request.Summary) || rawValueSet(request.Extras) || rawValueSet(request.Subpages) || rawValueSet(request.Livecrawl) || rawValueSet(request.MaxAgeHours) {
		return nil, newExaRequestError("INVALID_REQUEST_BODY", "summary, extras, subpages, livecrawl, and maxAgeHours are not supported")
	}
	if request.Highlights.Enabled && strings.TrimSpace(request.Highlights.Query) == "" {
		return nil, newExaRequestError("INVALID_REQUEST_BODY", "highlights.query is required for contents highlights")
	}
	if err := validateQueryLength(strings.TrimSpace(request.Highlights.Query)); err != nil {
		return nil, newExaRequestError("INVALID_REQUEST_BODY", err.Error())
	}
	var urls []string
	if request.URLs != nil {
		urls = *request.URLs
	} else {
		urls = *request.IDs
	}
	if len(urls) == 0 {
		return nil, newExaRequestError("INVALID_REQUEST_BODY", "ids or urls must contain at least one value")
	}
	if len(urls) > 100 || (cap > 0 && len(urls) > cap) {
		limit := 100
		if cap > 0 && cap < limit {
			limit = cap
		}
		return nil, newExaRequestError("INVALID_REQUEST_BODY", fmt.Sprintf("too many ids or urls; this instance limit is %d", limit))
	}
	for _, raw := range urls {
		parsed, err := url.Parse(raw)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return nil, newExaRequestError("INVALID_URLS", "ids and urls must contain absolute HTTP(S) URLs")
		}
	}
	return urls, nil
}

func mapExaContentsResponse(requestID string, urls []string, parsed []model.SearchResult, request exaContentsRequest) exaContentsResponse {
	response := exaContentsResponse{RequestID: requestID, Results: []exaContentResult{}, Statuses: []exaContentStatus{}}
	byURL := make(map[string]model.SearchResult, len(parsed))
	for _, result := range parsed {
		canonical, err := cache.CanonicalURL(result.URL)
		if err == nil {
			byURL[canonical] = result
		}
	}
	for _, rawURL := range urls {
		canonical, _ := cache.CanonicalURL(rawURL)
		result, found := byURL[canonical]
		if !found {
			response.Statuses = append(response.Statuses, exaContentStatus{ID: rawURL, Status: "error", Error: &exaStatusError{Tag: "CRAWL_UNKNOWN_ERROR"}})
			continue
		}
		if result.Unsupported != "" {
			response.Statuses = append(response.Statuses, exaContentStatus{ID: rawURL, Status: "error", Error: exaUnsupportedStatus(result.Unsupported)})
			continue
		}
		if strings.TrimSpace(resultExtractedText(result)) == "" {
			response.Statuses = append(response.Statuses, exaContentStatus{ID: rawURL, Status: "error", Error: &exaStatusError{Tag: "CRAWL_UNKNOWN_ERROR"}})
			continue
		}
		mapped := exaContentResult{ID: rawURL, URL: result.URL, Title: titleOf(result), Author: authorOf(result), PublishedDate: exaPublishedDate(result)}
		if request.Text.Enabled {
			text := truncateCompatText(resultExtractedText(result), request.Text.MaxCharacters)
			mapped.Text = &text
		}
		if request.Highlights.Enabled {
			mapped.Highlights, mapped.HighlightScores = exaHighlights(result, request.Highlights.MaxCharacters)
		}
		response.Results = append(response.Results, mapped)
		source := "crawled"
		if result.FromCache {
			source = "cached"
		}
		response.Statuses = append(response.Statuses, exaContentStatus{ID: rawURL, Status: "success", Source: source})
	}
	return response
}

func exaUnsupportedStatus(reason string) *exaStatusError {
	switch reason {
	case fetcher.ReasonRobots, fetcher.ReasonEgress:
		status := http.StatusForbidden
		return &exaStatusError{Tag: "SOURCE_NOT_AVAILABLE", HTTPStatusCode: &status}
	case fetcher.ReasonNonHTML:
		return &exaStatusError{Tag: "UNSUPPORTED_URL"}
	default:
		return &exaStatusError{Tag: "CRAWL_UNKNOWN_ERROR"}
	}
}

func exaHighlights(result model.SearchResult, maxCharacters int) ([]string, []float64) {
	if len(result.Chunks) == 0 {
		text := resultExtractedText(result)
		if text == "" {
			return []string{}, []float64{}
		}
		return []string{normalizeCompatText(text, maxCharacters)}, nil
	}
	highlights := make([]string, 0, len(result.Chunks))
	remaining := maxCharacters
	for _, chunk := range result.Chunks {
		limit := 0
		if maxCharacters > 0 {
			if remaining <= 0 {
				break
			}
			limit = remaining
		}
		text := normalizeCompatText(chunk.Text, limit)
		if text == "" {
			continue
		}
		highlights = append(highlights, text)
		if maxCharacters > 0 {
			remaining -= len([]rune(strings.TrimSuffix(text, "…")))
		}
	}
	return highlights, nil
}

func exaPublishedDate(result model.SearchResult) string {
	published := result.PublishedAt
	if published == nil && result.Content != nil {
		published = result.Content.PublishedAt
	}
	if published == nil {
		return ""
	}
	return published.UTC().Format(time.RFC3339)
}

func exaAPIKey(request *http.Request) string {
	if key := strings.TrimSpace(request.Header.Get("x-api-key")); key != "" {
		return key
	}
	return bearerKey(request)
}

func decodeExaJSON(request *http.Request, value any) error {
	request.Body = http.MaxBytesReader(nil, request.Body, 1<<20)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errBadRequest
	}
	return nil
}

func writeExaError(writer http.ResponseWriter, request *http.Request, status int, message, tag string) {
	if status == http.StatusTooManyRequests {
		writeJSON(writer, status, map[string]string{"error": message})
		return
	}
	writeJSON(writer, status, exaErrorPayload(request, message, tag))
}

func exaErrorPayload(request *http.Request, message, tag string) map[string]any {
	return map[string]any{"requestId": middleware.IDFrom(request.Context()), "error": message, "tag": tag}
}

func exaAuthError(writer http.ResponseWriter, request *http.Request, status int, code string) {
	message := "missing or invalid API key"
	if code == "missing_api_key" {
		message = "missing API key"
	}
	writeJSON(writer, status, exaErrorPayload(request, message, "INVALID_API_KEY"))
}

func exaErrorTag(status int) string {
	if status == http.StatusBadRequest {
		return "INVALID_REQUEST_BODY"
	}
	return "INTERNAL_ERROR"
}

func exaExecutionErrorMessage(err *searchExecutionError) string {
	switch err.Code {
	case "query_required":
		return "query is required"
	case "invalid_request":
		return "invalid request"
	case "forbidden":
		return "request forbidden"
	case "pipeline_not_ready":
		return "service unavailable"
	case "search_failed":
		return "search failed"
	default:
		return "request failed"
	}
}
