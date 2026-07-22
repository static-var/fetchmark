package eval

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/staticvar/fetchmark/internal/buildidentity"
	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/search"
	"github.com/staticvar/fetchmark/internal/evaluationmanifest"
)

const maxEvaluationResponseBytes = 32 << 20

var (
	errEvaluationResponseTooLarge = errors.New("evaluation response too large")
	errInvalidEvaluationResponse  = errors.New("invalid evaluation response")
)

type searchRequest struct {
	Query         string   `json:"query"`
	Engines       []string `json:"engines,omitempty"`
	Categories    []string `json:"categories,omitempty"`
	Language      string   `json:"language,omitempty"`
	TimeRange     string   `json:"time_range,omitempty"`
	SearchDepth   string   `json:"search_depth"`
	MaxResults    int      `json:"max_results"`
	Formats       []string `json:"formats"`
	RespectRobots *bool    `json:"respect_robots"`
}

type searchResponse struct {
	Query     string                  `json:"query"`
	Count     int                     `json:"count"`
	Results   []model.SearchResult    `json:"results"`
	Discovery *search.DiscoveryReport `json:"discovery,omitempty"`
}

// SourceObservation is one exact broker provider/lane/variant tuple attached
// to a returned result. Legacy rrf_sources metadata has no provider identity;
// ProviderUnavailable keeps the evaluator from inventing one from lane names.
type SourceObservation struct {
	Provider            string `json:"provider,omitempty"`
	Lane                string `json:"lane"`
	Variant             string `json:"variant"`
	ProviderUnavailable bool   `json:"provider_unavailable,omitempty"`
	// Source is accepted only when loading schema-v1 legacy artifacts. New
	// records always write Lane plus ProviderUnavailable instead.
	Source string `json:"source,omitempty"`
}

// ResultObservation is the compact evidence retained for later relevance,
// overlap, and contribution analysis.
type ResultObservation struct {
	URL                 string              `json:"url"`
	Domain              string              `json:"domain,omitempty"`
	Title               string              `json:"title,omitempty"`
	Engines             []string            `json:"engines,omitempty"`
	PublishedAt         *time.Time          `json:"published_at,omitempty"`
	Extracted           bool                `json:"extracted"`
	Unsupported         string              `json:"unsupported_reason,omitempty"`
	Score               float64             `json:"score,omitempty"`
	Sources             []SourceObservation `json:"sources,omitempty"`
	ProvenanceMalformed bool                `json:"provenance_malformed,omitempty"`
}

// Record is one raw, append-friendly JSONL evaluation observation.
type Record struct {
	SchemaVersion              int                     `json:"schema_version"`
	RunID                      string                  `json:"run_id"`
	Revision                   string                  `json:"revision,omitempty"`
	ConfigurationID            string                  `json:"configuration_id,omitempty"`
	BuildSHA256                string                  `json:"build_sha256,omitempty"`
	ConfigurationSHA256        string                  `json:"configuration_sha256,omitempty"`
	CaseSHA256                 string                  `json:"case_sha256,omitempty"`
	CaseID                     string                  `json:"case_id"`
	Intent                     Intent                  `json:"intent"`
	Query                      string                  `json:"query"`
	Tags                       []string                `json:"tags,omitempty"`
	SearchDepth                string                  `json:"search_depth"`
	ExpectedDomains            []string                `json:"expected_domains,omitempty"`
	FreshnessSensitive         bool                    `json:"freshness_sensitive"`
	StartedAt                  time.Time               `json:"started_at"`
	Attempted                  bool                    `json:"attempted"`
	DurationMS                 int64                   `json:"duration_ms"`
	HTTPStatus                 int                     `json:"http_status,omitempty"`
	Error                      string                  `json:"error,omitempty"`
	NonEmpty                   bool                    `json:"non_empty"`
	ResultCount                int                     `json:"result_count"`
	UniqueDomains              int                     `json:"unique_domains"`
	ExtractionSuccesses        int                     `json:"extraction_successes"`
	PublishedResults           int                     `json:"published_results"`
	ExpectedDomainHits         int                     `json:"expected_domain_hits"`
	ProvenanceResults          int                     `json:"provenance_results"`
	ProvenanceMalformedResults int                     `json:"provenance_malformed_results"`
	MultiSourceResults         int                     `json:"multi_source_results"`
	Discovery                  *search.DiscoveryReport `json:"discovery,omitempty"`
	Results                    []ResultObservation     `json:"results,omitempty"`
}

// IntentSummary keeps per-intent comparisons meaningful as the suite grows.
type IntentSummary struct {
	Total     int `json:"total"`
	Succeeded int `json:"succeeded"`
	NonEmpty  int `json:"non_empty"`
	Errors    int `json:"errors"`
}

// Summary is deterministic aggregate output for one run.
type Summary struct {
	SchemaVersion               int                      `json:"schema_version"`
	RunID                       string                   `json:"run_id,omitempty"`
	Revision                    string                   `json:"revision,omitempty"`
	ConfigurationID             string                   `json:"configuration_id,omitempty"`
	BuildSHA256                 string                   `json:"build_sha256,omitempty"`
	ConfigurationSHA256         string                   `json:"configuration_sha256,omitempty"`
	Total                       int                      `json:"total"`
	Attempted                   int                      `json:"attempted"`
	Complete                    bool                     `json:"complete"`
	Succeeded                   int                      `json:"succeeded"`
	Errors                      int                      `json:"errors"`
	NonEmpty                    int                      `json:"non_empty"`
	NonEmptyRate                float64                  `json:"non_empty_rate"`
	ResultCount                 int                      `json:"result_count"`
	UniqueDomains               int                      `json:"unique_domains"`
	ExtractionSuccesses         int                      `json:"extraction_successes"`
	ExtractionSuccessRate       float64                  `json:"extraction_success_rate"`
	PublishedResults            int                      `json:"published_results"`
	ExpectedDomainHits          int                      `json:"expected_domain_hits"`
	ProvenanceResults           int                      `json:"provenance_results"`
	ProvenanceMalformedResults  int                      `json:"provenance_malformed_results"`
	MultiSourceResults          int                      `json:"multi_source_results"`
	ProvenanceCoverageRate      float64                  `json:"provenance_coverage_rate"`
	SourceContributions         map[string]int           `json:"source_contributions,omitempty"`
	LaneContributions           map[string]int           `json:"lane_contributions,omitempty"`
	SourcePairOverlap           map[string]int           `json:"source_pair_overlap,omitempty"`
	SourceUniqueDomains         map[string]int           `json:"source_unique_domains,omitempty"`
	P50LatencyMS                int64                    `json:"p50_latency_ms"`
	P95LatencyMS                int64                    `json:"p95_latency_ms"`
	LatencySamples              int                      `json:"latency_samples"`
	ByIntent                    map[Intent]IntentSummary `json:"by_intent"`
	ErrorCounts                 map[string]int           `json:"error_counts,omitempty"`
	DiscoveryReports            int                      `json:"discovery_reports"`
	DiscoveryReportCoverageRate float64                  `json:"discovery_report_coverage_rate"`
	DiscoveryStatusCounts       map[string]int           `json:"discovery_status_counts,omitempty"`
	DiscoveryLaneStatusCounts   map[string]int           `json:"discovery_lane_status_counts,omitempty"`
	DiscoveryDiagnosticCounts   map[string]int           `json:"discovery_diagnostic_counts,omitempty"`
}

// Runner executes an explicitly configured live evaluation.
type Runner struct {
	Endpoint        string
	APIKey          string
	Client          *http.Client
	Concurrency     int
	RunID           string
	Revision        string
	ConfigurationID string
	// RequireBuildSHA256 makes the runtime executable identity a completion
	// gate for every HTTP response in the run.
	RequireBuildSHA256 bool
	// RequireConfigurationSHA256 gates completion on one consistent resolved
	// configuration digest. ExpectedConfigurationSHA256 additionally binds the
	// run to a separately fetched manifest.
	RequireConfigurationSHA256  bool
	ExpectedConfigurationSHA256 string
	Now                         func() time.Time
}

// Run evaluates every case concurrently while returning records in suite order.
func (r Runner) Run(ctx context.Context, suite Suite) ([]Record, Summary, error) {
	if err := suite.Validate(); err != nil {
		return nil, Summary{}, err
	}
	endpoint, err := validateEndpoint(r.Endpoint)
	if err != nil {
		return nil, Summary{}, err
	}
	concurrency := r.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > 32 {
		return nil, Summary{}, errors.New("eval: concurrency must be at most 32")
	}
	now := r.Now
	if now == nil {
		now = time.Now
	}
	runID := strings.TrimSpace(r.RunID)
	if runID == "" {
		runID = now().UTC().Format("20060102T150405.000000000Z")
	}
	if !validBaselineIdentity(runID) {
		return nil, Summary{}, errors.New("eval: run_id must be a bounded identifier")
	}
	revision := strings.TrimSpace(r.Revision)
	configurationID := strings.TrimSpace(r.ConfigurationID)
	if err := ValidateBaselineIdentity(revision, configurationID); err != nil {
		return nil, Summary{}, err
	}
	expectedConfigurationSHA256 := strings.TrimSpace(r.ExpectedConfigurationSHA256)
	if expectedConfigurationSHA256 != "" {
		canonical, ok := buildidentity.Parse(expectedConfigurationSHA256)
		if !ok || canonical != expectedConfigurationSHA256 {
			return nil, Summary{}, errors.New("eval: expected configuration_sha256 must be a lowercase SHA-256")
		}
	}
	client := r.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}

	records := make([]Record, len(suite.Cases))
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i, c := range suite.Cases {
		i, c := i, c
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				records[i] = baseRecord(c, runID, revision, configurationID, now(), "cancelled")
				return
			}
			records[i] = r.runCase(ctx, client, endpoint, c, runID, revision, configurationID, now)
		}()
	}
	wg.Wait()
	summary := Summarize(records)
	buildSHA256, buildIdentityErr := validateRunBuildIdentity(records, r.RequireBuildSHA256)
	configurationSHA256, configurationIdentityErr := validateRunConfigurationIdentity(records, r.RequireConfigurationSHA256, expectedConfigurationSHA256)
	if ctx.Err() != nil {
		summary.Complete = false
		if buildIdentityErr != nil || configurationIdentityErr != nil {
			return records, summary, errors.Join(ctx.Err(), buildIdentityErr, configurationIdentityErr)
		}
		summary.BuildSHA256 = buildSHA256
		summary.ConfigurationSHA256 = configurationSHA256
		return records, summary, ctx.Err()
	}
	if buildIdentityErr != nil || configurationIdentityErr != nil {
		summary.Complete = false
		return records, summary, errors.Join(buildIdentityErr, configurationIdentityErr)
	}
	summary.BuildSHA256 = buildSHA256
	summary.ConfigurationSHA256 = configurationSHA256
	return records, summary, nil
}

func validateRunConfigurationIdentity(records []Record, required bool, expected string) (string, error) {
	var observed string
	responses := 0
	missing := 0
	for _, record := range records {
		if record.Error == "invalid_configuration_sha256" {
			return "", errors.New("eval: invalid configuration_sha256 evidence")
		}
		if record.HTTPStatus == 0 {
			if record.ConfigurationSHA256 != "" {
				return "", errors.New("eval: configuration_sha256 without an HTTP response")
			}
			continue
		}
		responses++
		if record.ConfigurationSHA256 == "" {
			missing++
			continue
		}
		canonical, ok := buildidentity.Parse(record.ConfigurationSHA256)
		if !ok || canonical != record.ConfigurationSHA256 {
			return "", errors.New("eval: invalid configuration_sha256 evidence")
		}
		if observed == "" {
			observed = canonical
		} else if observed != canonical {
			return "", errors.New("eval: mixed configuration_sha256 values in one run")
		}
	}
	if observed != "" && missing > 0 {
		return "", errors.New("eval: mixed configuration_sha256 presence in one run")
	}
	if required && (responses == 0 || missing > 0 || observed == "") {
		return "", errors.New("eval: missing configuration_sha256 from live response")
	}
	if expected != "" && observed != expected {
		return "", errors.New("eval: unexpected configuration_sha256 from live response")
	}
	return observed, nil
}

func validateRunBuildIdentity(records []Record, required bool) (string, error) {
	var observed string
	responses := 0
	missing := 0
	for _, record := range records {
		if record.Error == "invalid_build_sha256" {
			return "", errors.New("eval: invalid build_sha256 evidence")
		}
		if record.HTTPStatus == 0 {
			if record.BuildSHA256 != "" {
				return "", errors.New("eval: build_sha256 without an HTTP response")
			}
			continue
		}
		responses++
		if record.BuildSHA256 == "" {
			missing++
			continue
		}
		canonical, ok := buildidentity.Parse(record.BuildSHA256)
		if !ok || canonical != record.BuildSHA256 {
			return "", errors.New("eval: invalid build_sha256 evidence")
		}
		if observed == "" {
			observed = canonical
		} else if observed != canonical {
			return "", errors.New("eval: mixed build_sha256 values in one run")
		}
	}
	if observed != "" && missing > 0 {
		return "", errors.New("eval: mixed build_sha256 presence in one run")
	}
	if required && (responses == 0 || missing > 0 || observed == "") {
		return "", errors.New("eval: missing build_sha256 from live response")
	}
	return observed, nil
}

func validBaselineIdentity(value string) bool {
	if len(value) > 128 {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || strings.ContainsRune("._/-:@+", character) {
			continue
		}
		return false
	}
	return true
}

// ValidateBaselineIdentity checks optional comparison metadata without
// executing a run. Commands use it before reserving an output artifact.
func ValidateBaselineIdentity(revision, configurationID string) error {
	if !validBaselineIdentity(strings.TrimSpace(revision)) || !validBaselineIdentity(strings.TrimSpace(configurationID)) {
		return errors.New("eval: revision and configuration_id must be bounded identifiers")
	}
	return nil
}

// ValidateRunID checks an optional operator-supplied run identifier. An empty
// value is valid because Runner will generate a timestamp identifier.
func ValidateRunID(runID string) error {
	runID = strings.TrimSpace(runID)
	if runID != "" && !validBaselineIdentity(runID) {
		return errors.New("eval: run_id must be a bounded identifier")
	}
	return nil
}

func validateEndpoint(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return "", errors.New("eval: endpoint must be an absolute http(s) URL")
	}
	if u.User != nil {
		return "", errors.New("eval: endpoint must not contain credentials")
	}
	return u.String(), nil
}

// ValidateEndpoint checks the live evaluator destination without making a
// request. Commands use this before reserving an exclusive output artifact.
func ValidateEndpoint(raw string) error {
	_, err := validateEndpoint(raw)
	return err
}

func (r Runner) runCase(ctx context.Context, client *http.Client, endpoint string, c Case, runID, revision, configurationID string, now func() time.Time) Record {
	started := now().UTC()
	record := baseRecord(c, runID, revision, configurationID, started, "")
	respectRobots := true
	payload, err := json.Marshal(searchRequest{
		Query: c.Query, Engines: c.Engines, Categories: c.Categories,
		Language: c.Language, TimeRange: c.TimeRange, SearchDepth: c.SearchDepth,
		MaxResults: c.MaxResults, Formats: []string{"json"}, RespectRobots: &respectRobots,
	})
	if err != nil {
		record.Error = "encode_request"
		return record
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		record.Error = "build_request"
		return record
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Fetchmark-Eval/1")
	if strings.TrimSpace(r.APIKey) != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(r.APIKey))
	}

	record.Attempted = true
	wallStart := time.Now()
	resp, err := client.Do(req)
	record.DurationMS = time.Since(wallStart).Milliseconds()
	if err != nil {
		record.Error = classifyRequestError(ctx, err)
		return record
	}
	defer resp.Body.Close()
	record.HTTPStatus = resp.StatusCode
	if buildSHA256Values := resp.Header.Values(buildidentity.HeaderName); len(buildSHA256Values) > 0 {
		if len(buildSHA256Values) != 1 {
			record.Error = "invalid_build_sha256"
			return record
		}
		canonical, ok := buildidentity.Parse(buildSHA256Values[0])
		if !ok {
			record.Error = "invalid_build_sha256"
			return record
		}
		record.BuildSHA256 = canonical
	}
	if configurationSHA256Values := resp.Header.Values(evaluationmanifest.HeaderName); len(configurationSHA256Values) > 0 {
		if len(configurationSHA256Values) != 1 {
			record.Error = "invalid_configuration_sha256"
			return record
		}
		canonical, ok := buildidentity.Parse(configurationSHA256Values[0])
		if !ok {
			record.Error = "invalid_configuration_sha256"
			return record
		}
		record.ConfigurationSHA256 = canonical
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var response searchResponse
		if decodeErr := decodeSearchResponse(resp.Body, maxEvaluationResponseBytes, &response); decodeErr == nil && response.Discovery != nil {
			if validateDiscoveryHTTPOutcome(response.Discovery, false) == nil {
				record.Discovery = response.Discovery
			}
		}
		record.Error = fmt.Sprintf("http_%d", resp.StatusCode)
		return record
	}
	var response searchResponse
	if err := decodeSearchResponse(resp.Body, maxEvaluationResponseBytes, &response); err != nil {
		if errors.Is(err, errEvaluationResponseTooLarge) {
			record.Error = "response_too_large"
		} else {
			record.Error = "decode_response"
		}
		return record
	}
	if err := validateSearchResponseEnvelope(c, response); err != nil {
		record.Error = "invalid_response"
		return record
	}
	if err := validateDiscoveryHTTPOutcome(response.Discovery, true); err != nil {
		record.Error = "invalid_discovery_report"
		return record
	}
	record.Discovery = response.Discovery
	record.Results = observeResults(response.Results)
	provenance := aggregateProvenance(record.Results)
	record.ProvenanceResults = provenance.results
	record.ProvenanceMalformedResults = provenance.malformed
	record.MultiSourceResults = provenance.multiSource
	record.ResultCount = len(record.Results)
	record.NonEmpty = record.ResultCount > 0
	domains := make(map[string]struct{}, record.ResultCount)
	expected := expectedDomainSet(c.ExpectedDomains)
	for _, result := range record.Results {
		if result.Domain != "" {
			domains[result.Domain] = struct{}{}
			if domainExpected(result.Domain, expected) {
				record.ExpectedDomainHits++
			}
		}
		if result.Extracted {
			record.ExtractionSuccesses++
		}
		if result.PublishedAt != nil {
			record.PublishedResults++
		}
	}
	record.UniqueDomains = len(domains)
	return record
}

func validateSearchResponseEnvelope(c Case, response searchResponse) error {
	if response.Query != c.Query || response.Count != len(response.Results) || len(response.Results) > c.MaxResults {
		return errInvalidEvaluationResponse
	}
	seenURLs := make(map[string]struct{}, len(response.Results))
	for _, result := range response.Results {
		parsed, err := url.Parse(result.URL)
		if err != nil || parsed.User != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return errInvalidEvaluationResponse
		}
		if _, duplicate := seenURLs[result.URL]; duplicate {
			return errInvalidEvaluationResponse
		}
		seenURLs[result.URL] = struct{}{}
	}
	return nil
}

func baseRecord(c Case, runID, revision, configurationID string, started time.Time, failure string) Record {
	return Record{
		SchemaVersion: 1, RunID: runID, Revision: revision, ConfigurationID: configurationID,
		CaseSHA256: CaseSHA256(c), CaseID: c.ID, Intent: c.Intent, Query: c.Query,
		Tags: append([]string(nil), c.Tags...), SearchDepth: c.SearchDepth,
		ExpectedDomains: append([]string(nil), c.ExpectedDomains...), FreshnessSensitive: c.FreshnessSensitive,
		StartedAt: started.UTC(), Error: failure,
	}
}

func decodeSearchResponse(body io.Reader, maxBytes int64, response *searchResponse) error {
	limited := &io.LimitedReader{R: body, N: maxBytes + 1}
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(response); err != nil {
		if limited.N == 0 {
			return errEvaluationResponseTooLarge
		}
		return fmt.Errorf("%w: %v", errInvalidEvaluationResponse, err)
	}

	var trailing any
	err := decoder.Decode(&trailing)
	if limited.N == 0 {
		return errEvaluationResponseTooLarge
	}
	if err == nil {
		return errInvalidEvaluationResponse
	}
	if !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: %v", errInvalidEvaluationResponse, err)
	}
	return nil
}

func classifyRequestError(ctx context.Context, err error) string {
	if errors.Is(ctx.Err(), context.Canceled) {
		return "cancelled"
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "network_error"
}

func observeResults(results []model.SearchResult) []ResultObservation {
	out := make([]ResultObservation, 0, len(results))
	for _, result := range results {
		extracted := result.Markdown != "" || result.HTML != ""
		if result.Content != nil {
			extracted = extracted || result.Content.MainText != "" || result.Content.Markdown != "" || result.Content.CleanedHTML != ""
		}
		domain := ""
		if parsed, err := url.Parse(result.URL); err == nil {
			domain = strings.ToLower(parsed.Hostname())
		}
		sources, malformed := sourceObservations(result)
		out = append(out, ResultObservation{
			URL: result.URL, Domain: domain, Title: result.Title,
			Engines: append([]string(nil), result.Engines...), PublishedAt: result.PublishedAt,
			Extracted: extracted, Unsupported: result.Unsupported, Score: result.Score,
			Sources: sources, ProvenanceMalformed: malformed,
		})
	}
	return out
}

func sourceObservations(result model.SearchResult) ([]SourceObservation, bool) {
	if len(result.Provenance) > 0 {
		out := make([]SourceObservation, 0, len(result.Provenance))
		seen := make(map[SourceObservation]struct{}, len(result.Provenance))
		for _, value := range result.Provenance {
			observation := SourceObservation{Provider: value.Provider, Lane: value.Lane, Variant: value.Variant}
			if !validSourceObservation(observation) {
				return nil, true
			}
			if _, duplicate := seen[observation]; duplicate {
				return nil, true
			}
			seen[observation] = struct{}{}
			out = append(out, observation)
		}
		sort.Slice(out, func(i, j int) bool {
			if out[i].Provider != out[j].Provider {
				return out[i].Provider < out[j].Provider
			}
			if out[i].Lane != out[j].Lane {
				return out[i].Lane < out[j].Lane
			}
			return out[i].Variant < out[j].Variant
		})
		return out, false
	}
	return parseLegacySourceObservations(result.Metadata)
}

func parseLegacySourceObservations(metadata map[string]string) ([]SourceObservation, bool) {
	raw := metadata["rrf_sources"]
	if raw == "" {
		return nil, false
	}
	parts := strings.Split(raw, ",")
	out := make([]SourceObservation, 0, len(parts))
	seen := make(map[SourceObservation]struct{}, len(parts))
	for _, part := range parts {
		lane, variant, found := strings.Cut(part, ":")
		observation := SourceObservation{Lane: lane, Variant: variant, ProviderUnavailable: true}
		if !found || strings.Contains(variant, ":") || !validSourceObservation(observation) {
			return nil, true
		}
		if _, duplicate := seen[observation]; duplicate {
			return nil, true
		}
		seen[observation] = struct{}{}
		out = append(out, observation)
	}
	return out, false
}

func validSourceObservation(observation SourceObservation) bool {
	observation, ok := normalizeSourceObservation(observation)
	if !ok {
		return false
	}
	return validNormalizedSourceObservation(observation)
}

func normalizeSourceObservation(observation SourceObservation) (SourceObservation, bool) {
	if observation.Source == "" {
		return observation, true
	}
	if observation.Provider != "" || observation.Lane != "" || observation.ProviderUnavailable {
		return SourceObservation{}, false
	}
	observation.Lane = observation.Source
	observation.Source = ""
	observation.ProviderUnavailable = true
	return observation, true
}

func validNormalizedSourceObservation(observation SourceObservation) bool {
	if !observation.ProviderUnavailable && !validObservationID(observation.Provider) {
		return false
	}
	if observation.ProviderUnavailable && observation.Provider != "" {
		return false
	}
	if !validObservationID(observation.Lane) {
		return false
	}
	switch observation.Variant {
	case "original", "exact", "freshness", "docs", "concept", "other":
		return true
	default:
		return false
	}
}

func validObservationID(value string) bool {
	if value == "" || len(value) > 48 || strings.TrimSpace(value) != value || strings.ToLower(value) != value {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '_' && character != '-' {
			return false
		}
	}
	return true
}

type provenanceAggregate struct {
	results             int
	malformed           int
	multiSource         int
	sourceContributions map[string]int
	laneContributions   map[string]int
	pairOverlap         map[string]int
	sourceDomains       map[string]map[string]struct{}
}

func aggregateProvenance(results []ResultObservation) provenanceAggregate {
	aggregate := provenanceAggregate{
		sourceContributions: make(map[string]int),
		laneContributions:   make(map[string]int),
		pairOverlap:         make(map[string]int),
		sourceDomains:       make(map[string]map[string]struct{}),
	}
	for _, result := range results {
		if result.ProvenanceMalformed {
			aggregate.malformed++
			continue
		}
		if len(result.Sources) == 0 {
			continue
		}
		distinctSources := make(map[string]struct{}, len(result.Sources))
		seenLanes := make(map[SourceObservation]struct{}, len(result.Sources))
		valid := true
		for _, source := range result.Sources {
			normalized, ok := normalizeSourceObservation(source)
			if !ok || !validNormalizedSourceObservation(normalized) {
				valid = false
				break
			}
			source = normalized
			if _, duplicate := seenLanes[source]; duplicate {
				valid = false
				break
			}
			seenLanes[source] = struct{}{}
			if !source.ProviderUnavailable {
				distinctSources[source.Provider] = struct{}{}
			}
		}
		if !valid {
			aggregate.malformed++
			continue
		}
		aggregate.results++
		for source := range seenLanes {
			provider := source.Provider
			if source.ProviderUnavailable {
				provider = "unavailable"
			}
			aggregate.laneContributions[provider+"/"+source.Lane+":"+source.Variant]++
		}
		sources := make([]string, 0, len(distinctSources))
		for source := range distinctSources {
			aggregate.sourceContributions[source]++
			sources = append(sources, source)
			if result.Domain != "" {
				if aggregate.sourceDomains[source] == nil {
					aggregate.sourceDomains[source] = make(map[string]struct{})
				}
				aggregate.sourceDomains[source][result.Domain] = struct{}{}
			}
		}
		sort.Strings(sources)
		if len(sources) > 1 {
			aggregate.multiSource++
		}
		for left := 0; left < len(sources); left++ {
			for right := left + 1; right < len(sources); right++ {
				aggregate.pairOverlap[sources[left]+"|"+sources[right]]++
			}
		}
	}
	return aggregate
}

func expectedDomainSet(domains []string) map[string]struct{} {
	out := make(map[string]struct{}, len(domains))
	for _, domain := range domains {
		out[strings.ToLower(strings.TrimSpace(domain))] = struct{}{}
	}
	return out
}

func domainExpected(domain string, expected map[string]struct{}) bool {
	for candidate := range expected {
		if domain == candidate || strings.HasSuffix(domain, "."+candidate) {
			return true
		}
	}
	return false
}

// Summarize aggregates records without consulting wall-clock or network state.
func Summarize(records []Record) Summary {
	summary := Summary{
		SchemaVersion: 1, Total: len(records), ByIntent: map[Intent]IntentSummary{}, ErrorCounts: map[string]int{},
		SourceContributions: map[string]int{}, LaneContributions: map[string]int{},
		SourcePairOverlap: map[string]int{}, SourceUniqueDomains: map[string]int{},
		DiscoveryStatusCounts: map[string]int{}, DiscoveryLaneStatusCounts: map[string]int{},
		DiscoveryDiagnosticCounts: map[string]int{},
	}
	latencies := make([]int64, 0, len(records))
	domains := map[string]struct{}{}
	sourceDomains := map[string]map[string]struct{}{}
	for _, record := range records {
		if summary.RunID == "" {
			summary.RunID = record.RunID
			summary.Revision = record.Revision
			summary.ConfigurationID = record.ConfigurationID
		}
		intent := summary.ByIntent[record.Intent]
		intent.Total++
		if record.Attempted {
			summary.Attempted++
			latencies = append(latencies, record.DurationMS)
		}
		if record.Error == "" && record.HTTPStatus >= 200 && record.HTTPStatus < 300 {
			summary.Succeeded++
			intent.Succeeded++
		} else {
			summary.Errors++
			intent.Errors++
			if record.Error != "" {
				summary.ErrorCounts[record.Error]++
			}
		}
		if record.NonEmpty {
			summary.NonEmpty++
			intent.NonEmpty++
		}
		if record.Discovery != nil {
			summary.DiscoveryReports++
			summary.DiscoveryStatusCounts[string(record.Discovery.Status)]++
			for _, lane := range record.Discovery.Lanes {
				laneKey := lane.Provider + "/" + lane.Lane + ":" + lane.Variant + "/" + string(lane.Status)
				summary.DiscoveryLaneStatusCounts[laneKey]++
				for _, diagnostic := range lane.Diagnostics {
					summary.DiscoveryDiagnosticCounts[lane.Provider+"/"+diagnostic.Reason]++
				}
			}
		}
		summary.ResultCount += record.ResultCount
		summary.ExtractionSuccesses += record.ExtractionSuccesses
		summary.PublishedResults += record.PublishedResults
		summary.ExpectedDomainHits += record.ExpectedDomainHits
		provenance := aggregateProvenance(record.Results)
		summary.ProvenanceResults += provenance.results
		summary.ProvenanceMalformedResults += provenance.malformed
		summary.MultiSourceResults += provenance.multiSource
		for source, count := range provenance.sourceContributions {
			summary.SourceContributions[source] += count
		}
		for lane, count := range provenance.laneContributions {
			summary.LaneContributions[lane] += count
		}
		for pair, count := range provenance.pairOverlap {
			summary.SourcePairOverlap[pair] += count
		}
		for source, observedDomains := range provenance.sourceDomains {
			if sourceDomains[source] == nil {
				sourceDomains[source] = make(map[string]struct{})
			}
			for domain := range observedDomains {
				sourceDomains[source][domain] = struct{}{}
			}
		}
		for _, result := range record.Results {
			if result.Domain != "" {
				domains[result.Domain] = struct{}{}
			}
		}
		summary.ByIntent[record.Intent] = intent
	}
	if buildSHA256, err := validateRunBuildIdentity(records, false); err == nil {
		summary.BuildSHA256 = buildSHA256
	}
	if configurationSHA256, err := validateRunConfigurationIdentity(records, false, ""); err == nil {
		summary.ConfigurationSHA256 = configurationSHA256
	}
	summary.Complete = summary.Attempted == summary.Total
	summary.LatencySamples = len(latencies)
	summary.UniqueDomains = len(domains)
	for source, observedDomains := range sourceDomains {
		summary.SourceUniqueDomains[source] = len(observedDomains)
	}
	if summary.Total > 0 {
		summary.NonEmptyRate = float64(summary.NonEmpty) / float64(summary.Total)
	}
	if summary.Attempted > 0 {
		summary.DiscoveryReportCoverageRate = float64(summary.DiscoveryReports) / float64(summary.Attempted)
	}
	if summary.ResultCount > 0 {
		summary.ExtractionSuccessRate = float64(summary.ExtractionSuccesses) / float64(summary.ResultCount)
		summary.ProvenanceCoverageRate = float64(summary.ProvenanceResults) / float64(summary.ResultCount)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	summary.P50LatencyMS = percentile(latencies, 0.50)
	summary.P95LatencyMS = percentile(latencies, 0.95)
	return summary
}

func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(p*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	return sorted[index]
}

// WriteRecords emits stable ordered JSONL suitable for later comparison.
func WriteRecords(w io.Writer, records []Record) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for _, record := range records {
		if err := enc.Encode(record); err != nil {
			return fmt.Errorf("eval: write record: %w", err)
		}
	}
	return nil
}
