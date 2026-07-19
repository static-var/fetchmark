package api

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/staticvar/fetchmark/internal/api/middleware"
	"github.com/staticvar/fetchmark/internal/core/model"
)

type tavilySearchRequest struct {
	Query                    string       `json:"query"`
	SearchDepth              string       `json:"search_depth,omitempty"`
	ChunksPerSource          *int         `json:"chunks_per_source,omitempty"`
	MaxResults               *int         `json:"max_results,omitempty"`
	Topic                    string       `json:"topic,omitempty"`
	TimeRange                string       `json:"time_range,omitempty"`
	StartDate                string       `json:"start_date,omitempty"`
	EndDate                  string       `json:"end_date,omitempty"`
	Days                     int          `json:"days,omitempty"`
	IncludeAnswer            boolOrString `json:"include_answer,omitempty"`
	IncludeRawContent        boolOrString `json:"include_raw_content,omitempty"`
	IncludeImages            bool         `json:"include_images,omitempty"`
	IncludeImageDescriptions bool         `json:"include_image_descriptions,omitempty"`
	IncludeFavicon           bool         `json:"include_favicon,omitempty"`
	IncludeDomains           []string     `json:"include_domains,omitempty"`
	ExcludeDomains           []string     `json:"exclude_domains,omitempty"`
	Country                  string       `json:"country,omitempty"`
	AutoParameters           bool         `json:"auto_parameters,omitempty"`
	ExactMatch               bool         `json:"exact_match,omitempty"`
	IncludeUsage             bool         `json:"include_usage,omitempty"`
	SafeSearch               bool         `json:"safe_search,omitempty"`
}

type tavilySearchResponse struct {
	Query             string         `json:"query"`
	FollowUpQuestions any            `json:"follow_up_questions"`
	Answer            any            `json:"answer"`
	Images            []any          `json:"images"`
	Results           []tavilyResult `json:"results"`
	ResponseTime      float64        `json:"response_time"`
	RequestID         string         `json:"request_id"`
	Usage             map[string]int `json:"usage,omitempty"`
}

type tavilyResult struct {
	Title         string  `json:"title"`
	URL           string  `json:"url"`
	Content       string  `json:"content"`
	Score         float64 `json:"score"`
	RawContent    any     `json:"raw_content"`
	PublishedDate string  `json:"published_date,omitempty"`
}

func tavilySearchHandler(d Deps) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		started := time.Now()
		var vendorRequest tavilySearchRequest
		if err := decodeJSON(request, &vendorRequest); err != nil {
			writeTavilyError(writer, http.StatusBadRequest, "invalid request body")
			return
		}
		canonical, answerMode, rawMode, err := translateTavilyRequest(vendorRequest)
		if err != nil {
			writeTavilyError(writer, http.StatusBadRequest, err.Error())
			return
		}
		executed, executionErr := executeCanonicalSearch(d, request, canonical)
		if executionErr != nil {
			writeTavilyError(writer, executionErr.Status, executionErr.Error())
			return
		}
		response := tavilySearchResponse{
			Query: vendorRequest.Query, FollowUpQuestions: nil, Answer: nil,
			Images:       []any{},
			Results:      tavilyResults(executed.Results, rawMode, canonical.SearchDepth),
			ResponseTime: time.Since(started).Seconds(), RequestID: middleware.IDFrom(request.Context()),
		}
		if answerMode == "extractive" {
			response.Answer = deterministicExtractiveAnswer(executed.Results)
		} else if answerMode == "local_llm" {
			answer, status, answerErr := compatibilityLocalAnswer(request.Context(), d, vendorRequest.Query, executed.Results)
			if answerErr != nil {
				writeTavilyError(writer, status, answerErr.Error())
				return
			}
			response.Answer = answer
		}
		if vendorRequest.IncludeUsage {
			response.Usage = map[string]int{"credits": 0}
		}
		writeJSONBoundedCustomWithSuccessHeaders(writer, http.StatusOK, response, d.Config.MaxRequestOutputBytes,
			http.StatusInsufficientStorage, map[string]any{"detail": map[string]string{"error": "response byte budget exceeded"}}, func(header http.Header) {
				addDiscoveryAttributionHeaders(header, executed.Results)
			})
	}
}

func translateTavilyRequest(request tavilySearchRequest) (searchRequest, string, string, error) {
	if strings.TrimSpace(request.Query) == "" {
		return searchRequest{}, "", "", errors.New("query is required")
	}
	depth := strings.ToLower(strings.TrimSpace(request.SearchDepth))
	if depth == "" {
		depth = "basic"
	}
	switch depth {
	case "basic", "advanced", "fast", "ultra-fast":
	default:
		return searchRequest{}, "", "", errors.New("search_depth must be basic, advanced, fast, or ultra-fast")
	}
	maxResults := 5
	if request.MaxResults != nil {
		maxResults = *request.MaxResults
	}
	if maxResults < 1 || maxResults > 20 {
		return searchRequest{}, "", "", errors.New("max_results must be between 1 and 20")
	}
	if request.ChunksPerSource != nil && (*request.ChunksPerSource < 1 || *request.ChunksPerSource > 3) {
		return searchRequest{}, "", "", errors.New("chunks_per_source must be between 1 and 3")
	}
	chunks := 0
	if request.ChunksPerSource != nil {
		chunks = *request.ChunksPerSource
	}
	if chunks == 0 && (depth == "advanced" || depth == "fast") {
		chunks = 3
	}
	topic := strings.ToLower(strings.TrimSpace(request.Topic))
	var categories []string
	switch topic {
	case "", "general":
	case "news":
		categories = []string{"news"}
	case "finance":
		return searchRequest{}, "", "", errors.New("topic=finance is not supported")
	default:
		return searchRequest{}, "", "", errors.New("topic must be general, news, or finance")
	}
	timeRange := strings.ToLower(strings.TrimSpace(request.TimeRange))
	switch timeRange {
	case "d":
		timeRange = "day"
	case "m":
		timeRange = "month"
	case "y":
		timeRange = "year"
	case "w", "week":
		return searchRequest{}, "", "", errors.New("time_range=week is not supported")
	case "", "day", "month", "year":
	default:
		return searchRequest{}, "", "", errors.New("invalid time_range")
	}
	if request.StartDate != "" || request.EndDate != "" || request.Days != 0 {
		return searchRequest{}, "", "", errors.New("start_date, end_date, and days are not supported")
	}
	if request.IncludeImages || request.IncludeImageDescriptions {
		return searchRequest{}, "", "", errors.New("include_images and include_image_descriptions are not supported")
	}
	if request.IncludeFavicon {
		return searchRequest{}, "", "", errors.New("include_favicon is not supported")
	}
	if strings.TrimSpace(request.Country) != "" {
		return searchRequest{}, "", "", errors.New("country is not supported")
	}
	if request.AutoParameters {
		return searchRequest{}, "", "", errors.New("auto_parameters is not supported")
	}
	if len(request.IncludeDomains) > 300 {
		return searchRequest{}, "", "", errors.New("include_domains cannot contain more than 300 entries")
	}
	if len(request.ExcludeDomains) > 150 {
		return searchRequest{}, "", "", errors.New("exclude_domains cannot contain more than 150 entries")
	}
	answerMode, err := tavilyAnswerMode(request.IncludeAnswer)
	if err != nil {
		return searchRequest{}, "", "", err
	}
	rawMode, err := tavilyRawMode(request.IncludeRawContent)
	if err != nil {
		return searchRequest{}, "", "", err
	}
	var safeSearch *int
	if request.SafeSearch {
		strict := 2
		safeSearch = &strict
	}
	return searchRequest{
		Query: request.Query, SearchDepth: depth, ChunksPerSource: chunks,
		MaxResults: maxResults, Categories: categories, TimeRange: timeRange,
		SafeSearch: safeSearch, IncludeDomains: request.IncludeDomains,
		ExcludeDomains: request.ExcludeDomains, ExactMatch: request.ExactMatch,
		Formats: []string{"json", "markdown", "html"},
	}, answerMode, rawMode, nil
}

func tavilyAnswerMode(value boolOrString) (string, error) {
	if !value.Set || (!value.Bool && value.String == "") {
		return "", nil
	}
	if value.Bool || value.String == "basic" || value.String == "true" {
		return "extractive", nil
	}
	if value.String == "advanced" {
		return "local_llm", nil
	}
	if value.String == "false" {
		return "", nil
	}
	return "", errors.New("include_answer must be false, true, basic, or advanced")
}

func tavilyRawMode(value boolOrString) (string, error) {
	if !value.Set || (!value.Bool && value.String == "") || value.String == "false" {
		return "", nil
	}
	if value.Bool || value.String == "true" || value.String == "markdown" {
		return "markdown", nil
	}
	if value.String == "text" {
		return "text", nil
	}
	return "", errors.New("include_raw_content must be false, true, markdown, or text")
}

func tavilyResults(results []model.SearchResult, rawMode, depth string) []tavilyResult {
	out := make([]tavilyResult, 0, len(results))
	for _, result := range results {
		var raw any
		switch rawMode {
		case "markdown":
			value := strings.TrimSpace(result.Markdown)
			if value == "" && result.Content != nil {
				value = strings.TrimSpace(result.Content.Markdown)
			}
			if value != "" {
				raw = value
			}
		case "text":
			if value := resultExtractedText(result); value != "" {
				raw = value
			}
		}
		published := ""
		if result.PublishedAt != nil {
			published = result.PublishedAt.UTC().Format(time.RFC3339)
		}
		out = append(out, tavilyResult{
			Title: titleOf(result), URL: result.URL, Content: tavilyResultContent(result, depth),
			Score: result.Score, RawContent: raw, PublishedDate: published,
		})
	}
	return out
}

func tavilyResultContent(result model.SearchResult, depth string) string {
	if (depth == "advanced" || depth == "fast") && len(result.Chunks) > 0 {
		parts := make([]string, 0, len(result.Chunks))
		for _, chunk := range result.Chunks {
			if text := normalizeCompatText(chunk.Text, 500); text != "" {
				parts = append(parts, text)
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, " [...] ")
		}
	}
	return resultSummary(result)
}

func writeTavilyError(writer http.ResponseWriter, status int, message string) {
	writeJSON(writer, status, map[string]any{"detail": map[string]string{"error": message}})
}

func tavilyAuthError(writer http.ResponseWriter, _ *http.Request, status int, code string) {
	message := "missing or invalid API key"
	if code == "missing_api_key" {
		message = "missing API key"
	}
	writeTavilyError(writer, status, message)
}
