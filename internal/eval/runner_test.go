package eval

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/search"
	"github.com/staticvar/fetchmark/internal/evaluationmanifest"
)

func TestRunnerRecordsOrderedObservableResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/search" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("Authorization = %q", got)
		}
		var request searchRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if request.Query == "slow query" {
			time.Sleep(20 * time.Millisecond)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"query": request.Query,
			"count": 1,
			"results": []map[string]any{{
				"url":          "https://example.com/" + request.Query,
				"title":        request.Query,
				"published_at": "2026-07-18T00:00:00Z",
				"content":      map[string]string{"main_text": "extracted text"},
			}},
		})
	}))
	t.Cleanup(server.Close)

	suite := Suite{Cases: []Case{
		{ID: "general-001", Intent: IntentGeneral, Query: "slow query", Tags: []string{"general"}, MaxResults: 10, SearchDepth: "basic", ExpectedDomains: []string{"example.com"}, FreshnessSensitive: true},
		{ID: "developer-001", Intent: IntentDeveloper, Query: "fast query", Tags: []string{"developer"}, MaxResults: 10, SearchDepth: "advanced"},
	}}
	runner := Runner{
		Endpoint:        server.URL + "/v1/search",
		APIKey:          "test-key",
		Client:          server.Client(),
		Concurrency:     2,
		RunID:           "test-run",
		Revision:        "0123456789abcdef",
		ConfigurationID: "general-open-v1",
		Now:             func() time.Time { return time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC) },
	}

	records, summary, err := runner.Run(context.Background(), suite)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(records) != 2 || records[0].CaseID != "general-001" || records[1].CaseID != "developer-001" {
		t.Fatalf("record order = %+v", records)
	}
	if records[0].CaseSHA256 != CaseSHA256(suite.Cases[0]) || records[1].CaseSHA256 != CaseSHA256(suite.Cases[1]) {
		t.Fatalf("record case identity = %q / %q", records[0].CaseSHA256, records[1].CaseSHA256)
	}
	if !records[0].NonEmpty || records[0].UniqueDomains != 1 || records[0].ExtractionSuccesses != 1 || records[0].PublishedResults != 1 {
		t.Fatalf("first record = %+v", records[0])
	}
	if records[0].SearchDepth != "basic" || len(records[0].ExpectedDomains) != 1 || records[0].ExpectedDomains[0] != "example.com" || !records[0].FreshnessSensitive {
		t.Fatalf("first record case metadata = %+v", records[0])
	}
	if records[0].ExpectedDomainHits != 1 {
		t.Fatalf("first record expected-domain hits = %+v", records[0])
	}
	if records[0].Revision != "0123456789abcdef" || records[0].ConfigurationID != "general-open-v1" {
		t.Fatalf("first record baseline identity = %+v", records[0])
	}
	if summary.Total != 2 || summary.Succeeded != 2 || summary.NonEmpty != 2 || summary.ExtractionSuccesses != 2 {
		t.Fatalf("summary = %+v", summary)
	}
	if summary.Revision != "0123456789abcdef" || summary.ConfigurationID != "general-open-v1" {
		t.Fatalf("summary baseline identity = %+v", summary)
	}
}

func TestRunnerCapturesConsistentBuildSHA256(t *testing.T) {
	const buildSHA256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return evaluationResponse(http.StatusOK, strings.ToUpper(buildSHA256)), nil
	})}

	suite := Suite{Cases: []Case{
		{ID: "general-001", Intent: IntentGeneral, Query: "one", Tags: []string{"general"}, MaxResults: 10, SearchDepth: "basic"},
		{ID: "developer-001", Intent: IntentDeveloper, Query: "two", Tags: []string{"developer"}, MaxResults: 10, SearchDepth: "basic"},
	}}
	records, summary, err := (Runner{
		Endpoint: "http://example.invalid/v1/search", Client: client, Concurrency: 2, RunID: "build-evidence", RequireBuildSHA256: true,
	}).Run(context.Background(), suite)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(records) != 2 || records[0].BuildSHA256 != buildSHA256 || records[1].BuildSHA256 != buildSHA256 {
		t.Fatalf("records = %+v", records)
	}
	if summary.BuildSHA256 != buildSHA256 {
		t.Fatalf("summary build_sha256 = %q", summary.BuildSHA256)
	}
}

func TestRunnerCapturesAndRequiresExpectedConfigurationSHA256(t *testing.T) {
	const configurationSHA256 = "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		response := evaluationResponse(http.StatusOK, "")
		response.Header.Set(evaluationmanifest.HeaderName, strings.ToUpper(configurationSHA256))
		return response, nil
	})}
	suite := Suite{Cases: []Case{{ID: "general-001", Intent: IntentGeneral, Query: "one", Tags: []string{"general"}, MaxResults: 10, SearchDepth: "basic"}}}
	records, summary, err := (Runner{
		Endpoint: "http://example.invalid/v1/search", Client: client, RunID: "configuration-evidence",
		RequireConfigurationSHA256: true, ExpectedConfigurationSHA256: configurationSHA256,
	}).Run(context.Background(), suite)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if records[0].ConfigurationSHA256 != configurationSHA256 || summary.ConfigurationSHA256 != configurationSHA256 {
		t.Fatalf("records=%+v summary=%+v", records, summary)
	}
}

func TestRunnerRejectsUnexpectedConfigurationSHA256(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		response := evaluationResponse(http.StatusOK, "")
		response.Header.Set(evaluationmanifest.HeaderName, strings.Repeat("b", 64))
		return response, nil
	})}
	suite := Suite{Cases: []Case{{ID: "general-001", Intent: IntentGeneral, Query: "one", Tags: []string{"general"}, MaxResults: 10, SearchDepth: "basic"}}}
	_, summary, err := (Runner{
		Endpoint: "http://example.invalid/v1/search", Client: client, RunID: "configuration-mismatch",
		RequireConfigurationSHA256: true, ExpectedConfigurationSHA256: strings.Repeat("a", 64),
	}).Run(context.Background(), suite)
	if err == nil || !strings.Contains(err.Error(), "unexpected configuration_sha256") || summary.Complete {
		t.Fatalf("Run error=%v summary=%+v", err, summary)
	}
}

func TestRunnerRejectsMixedOrPartialConfigurationSHA256(t *testing.T) {
	for name, second := range map[string]string{
		"mixed":   strings.Repeat("b", 64),
		"partial": "",
	} {
		t.Run(name, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				var payload searchRequest
				if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
					t.Errorf("decode request: %v", err)
				}
				digest := strings.Repeat("a", 64)
				if payload.Query == "two" {
					digest = second
				}
				response := evaluationResponse(http.StatusOK, "")
				if digest != "" {
					response.Header.Set(evaluationmanifest.HeaderName, digest)
				}
				return response, nil
			})}
			suite := Suite{Cases: []Case{
				{ID: "general-001", Intent: IntentGeneral, Query: "one", Tags: []string{"general"}, MaxResults: 10, SearchDepth: "basic"},
				{ID: "developer-001", Intent: IntentDeveloper, Query: "two", Tags: []string{"developer"}, MaxResults: 10, SearchDepth: "basic"},
			}}
			_, summary, err := (Runner{Endpoint: "http://example.invalid/v1/search", Client: client, Concurrency: 2, RunID: "mixed-configuration"}).Run(context.Background(), suite)
			if err == nil || !strings.Contains(err.Error(), "mixed configuration_sha256") || summary.Complete || summary.ConfigurationSHA256 != "" {
				t.Fatalf("Run error=%v summary=%+v", err, summary)
			}
		})
	}
}

func TestRunnerRejectsDuplicateConfigurationSHA256Headers(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		response := evaluationResponse(http.StatusOK, "")
		response.Header[http.CanonicalHeaderKey(evaluationmanifest.HeaderName)] = []string{strings.Repeat("a", 64), strings.Repeat("a", 64)}
		return response, nil
	})}
	suite := Suite{Cases: []Case{{ID: "general-001", Intent: IntentGeneral, Query: "one", Tags: []string{"general"}, MaxResults: 10, SearchDepth: "basic"}}}
	records, summary, err := (Runner{Endpoint: "http://example.invalid/v1/search", Client: client, RunID: "duplicate-configuration"}).Run(context.Background(), suite)
	if err == nil || !strings.Contains(err.Error(), "invalid configuration_sha256") || records[0].Error != "invalid_configuration_sha256" || summary.Complete {
		t.Fatalf("Run error=%v records=%+v summary=%+v", err, records, summary)
	}
}

func TestRunnerRejectsMixedBuildSHA256(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var request searchRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		digit := "a"
		if request.Query == "two" {
			digit = "b"
		}
		return evaluationResponse(http.StatusOK, strings.Repeat(digit, 64)), nil
	})}
	suite := Suite{Cases: []Case{
		{ID: "general-001", Intent: IntentGeneral, Query: "one", Tags: []string{"general"}, MaxResults: 10, SearchDepth: "basic"},
		{ID: "developer-001", Intent: IntentDeveloper, Query: "two", Tags: []string{"developer"}, MaxResults: 10, SearchDepth: "basic"},
	}}
	_, summary, err := (Runner{Endpoint: "http://example.invalid/v1/search", Client: client, Concurrency: 2, RunID: "mixed-builds"}).Run(context.Background(), suite)
	if err == nil || !strings.Contains(err.Error(), "mixed build_sha256") {
		t.Fatalf("Run error = %v, want mixed build_sha256", err)
	}
	if summary.Complete {
		t.Fatalf("summary should be incomplete: %+v", summary)
	}
	if summary.BuildSHA256 != "" {
		t.Fatalf("mixed run summary build_sha256 = %q, want omitted", summary.BuildSHA256)
	}
}

func TestRunnerKeepsBuildIdentityWhenAnotherRequestHasNoResponse(t *testing.T) {
	const buildSHA256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		var payload searchRequest
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if payload.Query == "transport failure" {
			return nil, errors.New("connection refused")
		}
		return evaluationResponse(http.StatusOK, buildSHA256), nil
	})}
	suite := Suite{Cases: []Case{
		{ID: "general-001", Intent: IntentGeneral, Query: "response", Tags: []string{"general"}, MaxResults: 10, SearchDepth: "basic"},
		{ID: "developer-001", Intent: IntentDeveloper, Query: "transport failure", Tags: []string{"developer"}, MaxResults: 10, SearchDepth: "basic"},
	}}
	records, summary, err := (Runner{
		Endpoint: "http://example.invalid/v1/search", Client: client, Concurrency: 2, RunID: "partial-transport", RequireBuildSHA256: true,
	}).Run(context.Background(), suite)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.BuildSHA256 != buildSHA256 || records[1].HTTPStatus != 0 || records[1].BuildSHA256 != "" {
		t.Fatalf("records = %+v summary = %+v", records, summary)
	}
	var artifact strings.Builder
	if err := WriteRecords(&artifact, records); err != nil {
		t.Fatalf("WriteRecords: %v", err)
	}
	if _, err := LoadRecords(strings.NewReader(artifact.String())); err != nil {
		t.Fatalf("LoadRecords written run: %v", err)
	}
}

func TestRunnerCanRequireBuildSHA256(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return evaluationResponse(http.StatusOK, ""), nil
	})}
	suite := Suite{Cases: []Case{{ID: "general-001", Intent: IntentGeneral, Query: "one", Tags: []string{"general"}, MaxResults: 10, SearchDepth: "basic"}}}
	_, summary, err := (Runner{
		Endpoint: "http://example.invalid/v1/search", Client: client, RunID: "missing-build", RequireBuildSHA256: true,
	}).Run(context.Background(), suite)
	if err == nil || !strings.Contains(err.Error(), "missing build_sha256") {
		t.Fatalf("Run error = %v, want missing build_sha256", err)
	}
	if summary.Complete {
		t.Fatalf("summary should be incomplete: %+v", summary)
	}
}

func TestRunnerRejectsInvalidBuildSHA256(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return evaluationResponse(http.StatusOK, "not-a-digest"), nil
	})}
	suite := Suite{Cases: []Case{{ID: "general-001", Intent: IntentGeneral, Query: "one", Tags: []string{"general"}, MaxResults: 10, SearchDepth: "basic"}}}
	records, summary, err := (Runner{
		Endpoint: "http://example.invalid/v1/search", Client: client, RunID: "invalid-build",
	}).Run(context.Background(), suite)
	if err == nil || !strings.Contains(err.Error(), "invalid build_sha256") {
		t.Fatalf("Run error = %v, want invalid build_sha256", err)
	}
	if len(records) != 1 || records[0].Error != "invalid_build_sha256" || records[0].BuildSHA256 != "" || summary.BuildSHA256 != "" || summary.Complete {
		t.Fatalf("records = %+v summary = %+v", records, summary)
	}
}

func TestRunnerRejectsDuplicateBuildSHA256Headers(t *testing.T) {
	for name, values := range map[string][]string{
		"equal":       {strings.Repeat("a", 64), strings.Repeat("a", 64)},
		"conflicting": {strings.Repeat("a", 64), strings.Repeat("b", 64)},
	} {
		t.Run(name, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				response := evaluationResponse(http.StatusOK, "")
				response.Header[http.CanonicalHeaderKey("X-Fetchmark-Build-SHA256")] = values
				return response, nil
			})}
			suite := Suite{Cases: []Case{{ID: "general-001", Intent: IntentGeneral, Query: "one", Tags: []string{"general"}, MaxResults: 10, SearchDepth: "basic"}}}
			records, summary, err := (Runner{Endpoint: "http://example.invalid/v1/search", Client: client, RunID: "duplicate-build"}).Run(context.Background(), suite)
			if err == nil || !strings.Contains(err.Error(), "invalid build_sha256") {
				t.Fatalf("Run error = %v, want invalid build_sha256", err)
			}
			if records[0].Error != "invalid_build_sha256" || records[0].BuildSHA256 != "" || summary.BuildSHA256 != "" {
				t.Fatalf("untrusted header retained: record=%+v summary=%+v", records[0], summary)
			}
		})
	}
}

func evaluationResponse(status int, buildSHA256 string) *http.Response {
	header := make(http.Header)
	header.Set("Content-Type", "application/json")
	if buildSHA256 != "" {
		header.Set("X-Fetchmark-Build-SHA256", buildSHA256)
	}
	return &http.Response{
		StatusCode: status,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(`{"query":"q","count":0,"results":[]}`)),
	}
}

func TestRunnerPreservesDiscoveryEvidenceOnSuccessAndFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var payload searchRequest
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		if payload.Query == "failed" {
			writer.WriteHeader(http.StatusBadGateway)
			_, _ = writer.Write([]byte(`{"error":"search_failed","discovery":{"status":"failed","lanes":[{"provider":"mwmbl","lane":"mwmbl-general","variant":"original","status":"failed","candidate_count":0,"duration_ms":8000,"diagnostics":[{"reason":"timeout","retryable":true}]}]}}`))
			return
		}
		_, _ = writer.Write([]byte(`{"query":"partial","count":0,"results":[],"discovery":{"status":"partial","lanes":[{"provider":"wiby","lane":"wiby-general","variant":"original","status":"partial","candidate_count":2,"duration_ms":25,"diagnostics":[{"source":"official_api","reason":"malformed_results"}]}]}}`))
	}))
	t.Cleanup(server.Close)

	suite := Suite{Cases: []Case{
		{ID: "general-001", Intent: IntentGeneral, Query: "partial", Tags: []string{"general"}, MaxResults: 10, SearchDepth: "basic"},
		{ID: "developer-001", Intent: IntentDeveloper, Query: "failed", Tags: []string{"developer"}, MaxResults: 10, SearchDepth: "basic"},
	}}
	records, summary, err := (Runner{
		Endpoint: server.URL, Client: server.Client(), Concurrency: 1, RunID: "discovery-evidence",
	}).Run(context.Background(), suite)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(records) != 2 || records[0].Discovery == nil || records[0].Discovery.Status != search.BatchPartial {
		t.Fatalf("success record = %+v", records[0])
	}
	if records[1].Error != "http_502" || records[1].Discovery == nil || records[1].Discovery.Status != search.BatchFailed {
		t.Fatalf("failure record = %+v", records[1])
	}
	if summary.DiscoveryReports != 2 || summary.DiscoveryReportCoverageRate != 1 || summary.DiscoveryStatusCounts["partial"] != 1 || summary.DiscoveryStatusCounts["failed"] != 1 {
		t.Fatalf("discovery summary = %+v", summary)
	}
	if summary.DiscoveryLaneStatusCounts["wiby/wiby-general:original/partial"] != 1 || summary.DiscoveryLaneStatusCounts["mwmbl/mwmbl-general:original/failed"] != 1 {
		t.Fatalf("lane status summary = %+v", summary.DiscoveryLaneStatusCounts)
	}
	if summary.DiscoveryDiagnosticCounts["wiby/malformed_results"] != 1 || summary.DiscoveryDiagnosticCounts["mwmbl/timeout"] != 1 {
		t.Fatalf("diagnostic summary = %+v", summary.DiscoveryDiagnosticCounts)
	}
}

func TestRunnerAppliesAsymmetricDiscoveryHTTPConsistency(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var payload searchRequest
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		if payload.Query == "failed-http" {
			writer.WriteHeader(http.StatusBadGateway)
			_, _ = writer.Write([]byte(`{"error":"search_failed","discovery":{"status":"healthy","lanes":[{"provider":"wiby","lane":"wiby-general","variant":"original","status":"healthy","candidate_count":1,"duration_ms":1}]}}`))
			return
		}
		_, _ = writer.Write([]byte(`{"query":"successful-http","count":0,"results":[],"discovery":{"status":"failed","lanes":[{"provider":"wiby","lane":"wiby-general","variant":"original","status":"failed","candidate_count":0,"duration_ms":1}]}}`))
	}))
	t.Cleanup(server.Close)

	suite := Suite{Cases: []Case{
		{ID: "general-001", Intent: IntentGeneral, Query: "successful-http", Tags: []string{"general"}, MaxResults: 10, SearchDepth: "basic"},
		{ID: "developer-001", Intent: IntentDeveloper, Query: "failed-http", Tags: []string{"developer"}, MaxResults: 10, SearchDepth: "basic"},
	}}
	records, summary, err := (Runner{Endpoint: server.URL, Client: server.Client(), Concurrency: 1, RunID: "http-report-consistency"}).Run(context.Background(), suite)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if records[0].Error != "invalid_discovery_report" || records[0].Discovery != nil {
		t.Fatalf("success record = %+v", records[0])
	}
	if records[1].Error != "http_502" || records[1].Discovery == nil || records[1].Discovery.Status != search.BatchHealthy {
		t.Fatalf("failure record = %+v", records[1])
	}
	if summary.DiscoveryReports != 1 || summary.DiscoveryReportCoverageRate != 0.5 || summary.DiscoveryStatusCounts["healthy"] != 1 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestRunnerRetainsPartialDiscoveryEvidenceOnHTTPFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusBadGateway)
		_, _ = writer.Write([]byte(`{"error":"search_failed","discovery":{"status":"partial","lanes":[{"provider":"wiby","lane":"wiby-general","variant":"original","status":"healthy","candidate_count":1,"duration_ms":1},{"provider":"mwmbl","lane":"mwmbl-general","variant":"original","status":"failed","candidate_count":0,"duration_ms":2,"diagnostics":[{"reason":"canceled"}]}]}}`))
	}))
	t.Cleanup(server.Close)

	suite := Suite{Cases: []Case{{ID: "general-001", Intent: IntentGeneral, Query: "partial failure", Tags: []string{"general"}, MaxResults: 10, SearchDepth: "basic"}}}
	records, summary, err := (Runner{Endpoint: server.URL, Client: server.Client(), Concurrency: 1, RunID: "partial-http-failure"}).Run(context.Background(), suite)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if records[0].Error != "http_502" || records[0].Discovery == nil || records[0].Discovery.Status != search.BatchPartial || len(records[0].Discovery.Lanes) != 2 {
		t.Fatalf("record = %+v", records[0])
	}
	if summary.DiscoveryReports != 1 || summary.DiscoveryStatusCounts["partial"] != 1 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestDecodeSearchResponseRejectsTrailingJSON(t *testing.T) {
	var response searchResponse
	err := decodeSearchResponse(strings.NewReader(`{"results":[]} {"results":[]}`), 1024, &response)
	if !errors.Is(err, errInvalidEvaluationResponse) {
		t.Fatalf("decodeSearchResponse error = %v", err)
	}
}

func TestDecodeSearchResponseRejectsOversizedBody(t *testing.T) {
	var response searchResponse
	err := decodeSearchResponse(strings.NewReader(`{"results":[]}`), 4, &response)
	if !errors.Is(err, errEvaluationResponseTooLarge) {
		t.Fatalf("decodeSearchResponse error = %v", err)
	}
}

func TestRunnerRejectsInconsistentSearchResponseEnvelope(t *testing.T) {
	tests := map[string]struct {
		body       string
		maxResults int
	}{
		"mismatched query echo": {body: `{"query":"different","count":0,"results":[]}`, maxResults: 1},
		"count mismatch":        {body: `{"query":"expected","count":1,"results":[]}`, maxResults: 1},
		"over requested limit":  {body: `{"query":"expected","count":2,"results":[{"url":"https://one.example/"},{"url":"https://two.example/"}]}`, maxResults: 1},
		"malformed URL":         {body: `{"query":"expected","count":1,"results":[{"url":"http://[::1"}]}`, maxResults: 1},
		"non-http URL":          {body: `{"query":"expected","count":1,"results":[{"url":"ftp://example.com/file"}]}`, maxResults: 1},
		"userinfo URL":          {body: `{"query":"expected","count":1,"results":[{"url":"https://user:secret@example.com/"}]}`, maxResults: 1},
		"empty URL":             {body: `{"query":"expected","count":1,"results":[{"url":""}]}`, maxResults: 1},
		"empty URL host":        {body: `{"query":"expected","count":1,"results":[{"url":"https:///path"}]}`, maxResults: 1},
		"duplicate exact URL":   {body: `{"query":"expected","count":2,"results":[{"url":"https://one.example/"},{"url":"https://one.example/"}]}`, maxResults: 2},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(test.body)),
				}, nil
			})}
			suite := Suite{Cases: []Case{{
				ID: "general-001", Intent: IntentGeneral, Query: "expected", Tags: []string{"general"}, MaxResults: test.maxResults, SearchDepth: "basic",
			}}}
			records, _, err := (Runner{
				Endpoint: "http://example.invalid/v1/search", Client: client, RunID: "invalid-envelope",
			}).Run(context.Background(), suite)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if len(records) != 1 || records[0].Error != "invalid_response" || records[0].ResultCount != 0 || len(records[0].Results) != 0 {
				t.Fatalf("inconsistent response was accepted: %+v", records)
			}
		})
	}
}

func TestSummarizeComputesRatesLatencyAndIntentBreakdown(t *testing.T) {
	records := []Record{
		{Intent: IntentGeneral, Attempted: true, DurationMS: 10, HTTPStatus: 200, NonEmpty: true, ResultCount: 2, ExtractionSuccesses: 1, UniqueDomains: 2},
		{Intent: IntentGeneral, Attempted: true, DurationMS: 20, HTTPStatus: 200},
		{Intent: IntentDeveloper, Attempted: true, DurationMS: 100, Error: "timeout"},
		{Intent: IntentDeveloper, Attempted: true, DurationMS: 40, HTTPStatus: 200, NonEmpty: true, ResultCount: 1, ExtractionSuccesses: 1, UniqueDomains: 1},
	}

	summary := Summarize(records)
	if summary.Total != 4 || summary.Succeeded != 3 || summary.NonEmpty != 2 {
		t.Fatalf("summary counts = %+v", summary)
	}
	if summary.NonEmptyRate != 0.5 || summary.ExtractionSuccessRate != 2.0/3.0 {
		t.Fatalf("summary rates = %+v", summary)
	}
	if summary.P50LatencyMS != 20 || summary.P95LatencyMS != 100 {
		t.Fatalf("latency summary = %+v", summary)
	}
	if summary.Attempted != 4 || summary.LatencySamples != 4 || !summary.Complete {
		t.Fatalf("completion summary = %+v", summary)
	}
	if summary.ByIntent[IntentDeveloper].Errors != 1 {
		t.Fatalf("intent summary = %+v", summary.ByIntent)
	}
}

func TestSummarizeAggregatesTypedBrokerProvenance(t *testing.T) {
	observed := observeResults([]model.SearchResult{
		{
			URL: "https://one.example/doc",
			Provenance: []model.DiscoveryProvenance{
				{Provider: "searxng", Lane: "searxng-open", Variant: "original"},
				{Provider: "crossref", Lane: "crossref-research", Variant: "exact"},
				{Provider: "crossref", Lane: "crossref-research", Variant: "freshness"},
			},
		},
		{
			URL:        "https://two.example/doc",
			Provenance: []model.DiscoveryProvenance{{Provider: "crossref", Lane: "crossref-research", Variant: "exact"}},
		},
		{
			URL: "https://three.example/doc",
			Metadata: map[string]string{
				"rrf_sources": "not valid provenance",
			},
		},
	})
	if len(observed[0].Sources) != 3 || observed[0].Sources[0] != (SourceObservation{Provider: "crossref", Lane: "crossref-research", Variant: "exact"}) {
		t.Fatalf("typed sources = %#v", observed[0].Sources)
	}
	if !observed[2].ProvenanceMalformed {
		t.Fatalf("malformed provenance was not retained as an explicit signal: %#v", observed[2])
	}

	summary := Summarize([]Record{{
		Intent: IntentResearch, Attempted: true, HTTPStatus: 200,
		ResultCount: len(observed), Results: observed,
	}})
	if summary.ProvenanceResults != 2 || summary.ProvenanceMalformedResults != 1 || summary.MultiSourceResults != 1 {
		t.Fatalf("provenance counts = %+v", summary)
	}
	if summary.ProvenanceCoverageRate != 2.0/3.0 {
		t.Fatalf("provenance coverage = %v", summary.ProvenanceCoverageRate)
	}
	if summary.SourceContributions["crossref"] != 2 || summary.SourceContributions["searxng"] != 1 {
		t.Fatalf("source contributions = %#v", summary.SourceContributions)
	}
	if summary.LaneContributions["crossref/crossref-research:exact"] != 2 || summary.LaneContributions["crossref/crossref-research:freshness"] != 1 {
		t.Fatalf("lane contributions = %#v", summary.LaneContributions)
	}
	if summary.SourcePairOverlap["crossref|searxng"] != 1 {
		t.Fatalf("source overlap = %#v", summary.SourcePairOverlap)
	}
	if summary.SourceUniqueDomains["crossref"] != 2 || summary.SourceUniqueDomains["searxng"] != 1 {
		t.Fatalf("source domains = %#v", summary.SourceUniqueDomains)
	}
}

func TestEvaluatorDoesNotCountTwoSearXNGLanesAsIndependentProviders(t *testing.T) {
	observed := observeResults([]model.SearchResult{{
		URL: "https://one.example/doc",
		Provenance: []model.DiscoveryProvenance{
			{Provider: "searxng", Lane: "searxng-default", Variant: "original"},
			{Provider: "searxng", Lane: "searxng-open", Variant: "original"},
		},
	}})
	summary := Summarize([]Record{{Intent: IntentGeneral, Attempted: true, HTTPStatus: 200, ResultCount: 1, Results: observed}})
	if summary.MultiSourceResults != 0 || len(summary.SourcePairOverlap) != 0 || summary.SourceContributions["searxng"] != 1 {
		t.Fatalf("summary invented independent providers: %+v", summary)
	}
}

func TestEvaluatorMarksLegacyLaneProviderUnavailable(t *testing.T) {
	observed := observeResults([]model.SearchResult{{
		URL:      "https://legacy.example/doc",
		Metadata: map[string]string{"rrf_sources": "searxng-open:original"},
	}})
	if len(observed) != 1 || len(observed[0].Sources) != 1 || !observed[0].Sources[0].ProviderUnavailable {
		t.Fatalf("legacy observations = %#v", observed)
	}
	summary := Summarize([]Record{{Intent: IntentGeneral, Attempted: true, HTTPStatus: 200, ResultCount: 1, Results: observed}})
	if len(summary.SourceContributions) != 0 || summary.LaneContributions["unavailable/searxng-open:original"] != 1 {
		t.Fatalf("legacy summary invented provider identity: %+v", summary)
	}
}

func TestEvaluatorAcceptsTypedAndLegacyConceptProvenance(t *testing.T) {
	observed := observeResults([]model.SearchResult{
		{
			URL: "https://typed.example/doc",
			Provenance: []model.DiscoveryProvenance{{
				Provider: "mwmbl", Lane: "mwmbl-general", Variant: "concept",
			}},
		},
		{
			URL: "https://legacy.example/doc",
			Metadata: map[string]string{
				"rrf_sources": "mwmbl-general:concept",
			},
		},
	})
	for index, result := range observed {
		if result.ProvenanceMalformed || len(result.Sources) != 1 || result.Sources[0].Variant != "concept" {
			t.Fatalf("concept observation %d = %#v", index, result)
		}
	}
	if observed[0].Sources[0].Provider != "mwmbl" || observed[0].Sources[0].ProviderUnavailable {
		t.Fatalf("typed concept provider = %#v", observed[0].Sources[0])
	}
	if !observed[1].Sources[0].ProviderUnavailable {
		t.Fatalf("legacy concept provider availability = %#v", observed[1].Sources[0])
	}
}

func TestSummarizeExcludesNeverStartedCasesFromLatency(t *testing.T) {
	records := []Record{
		{Intent: IntentGeneral, Attempted: true, DurationMS: 75, HTTPStatus: 200},
		{Intent: IntentGeneral, Error: "cancelled"},
		{Intent: IntentDeveloper, Error: "cancelled"},
	}

	summary := Summarize(records)
	if summary.Attempted != 1 || summary.LatencySamples != 1 || summary.Complete {
		t.Fatalf("completion summary = %+v", summary)
	}
	if summary.P50LatencyMS != 75 || summary.P95LatencyMS != 75 {
		t.Fatalf("latency summary = %+v", summary)
	}
}

func TestRunnerMarksWholeRunCancellationIncompleteAfterRequestStarts(t *testing.T) {
	started := make(chan struct{})
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		close(started)
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}
	suite := Suite{Cases: []Case{{
		ID: "general-001", Intent: IntentGeneral, Query: "query",
		Tags: []string{"general"}, MaxResults: 10, SearchDepth: "basic",
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	type outcome struct {
		records []Record
		summary Summary
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		records, summary, err := (Runner{Endpoint: "http://example.invalid/v1/search", Client: client}).Run(ctx, suite)
		done <- outcome{records: records, summary: summary, err: err}
	}()
	<-started
	cancel()
	got := <-done
	if !errors.Is(got.err, context.Canceled) {
		t.Fatalf("Run error = %v", got.err)
	}
	if len(got.records) != 1 || !got.records[0].Attempted {
		t.Fatalf("records = %+v", got.records)
	}
	if got.summary.Complete || got.summary.Attempted != 1 || got.summary.LatencySamples != 1 {
		t.Fatalf("summary = %+v", got.summary)
	}
}

func TestRunnerCapturesHTTPFailureWithoutAbortingSuite(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":"search_failed"}`, http.StatusBadGateway)
	}))
	t.Cleanup(server.Close)

	suite := Suite{Cases: []Case{{ID: "general-001", Intent: IntentGeneral, Query: "query", Tags: []string{"general"}, MaxResults: 10, SearchDepth: "basic"}}}
	runner := Runner{Endpoint: server.URL, Client: server.Client(), Concurrency: 1, RunID: "failed-run"}
	records, summary, err := runner.Run(context.Background(), suite)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(records) != 1 || records[0].HTTPStatus != http.StatusBadGateway || records[0].Error != "http_502" {
		t.Fatalf("records = %+v", records)
	}
	if summary.Errors != 1 {
		t.Fatalf("summary = %+v", summary)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
