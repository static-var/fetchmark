package eval

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/staticvar/fetchmark/internal/buildidentity"
	"github.com/staticvar/fetchmark/internal/core/search"
)

const (
	maxEvaluationRecords          = 1_000
	maxEvaluationArtifactLineSize = 40 << 20
	maxEvaluationArtifactBytes    = 256 << 20
)

// LoadRecords strictly decodes one ordered JSONL run artifact. It rejects
// mixed runs and structurally inconsistent rows before offline analysis.
func LoadRecords(reader io.Reader) ([]Record, error) {
	if reader == nil {
		return nil, errors.New("eval: nil record reader")
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), maxEvaluationArtifactLineSize)
	records := make([]Record, 0)
	totalBytes := 0
	for line := 1; scanner.Scan(); line++ {
		totalBytes += len(scanner.Bytes()) + 1
		if totalBytes > maxEvaluationArtifactBytes {
			return nil, fmt.Errorf("eval: record artifact exceeds %d bytes", maxEvaluationArtifactBytes)
		}
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" {
			continue
		}
		if len(records) >= maxEvaluationRecords {
			return nil, fmt.Errorf("eval: record artifact exceeds %d rows", maxEvaluationRecords)
		}
		decoder := json.NewDecoder(strings.NewReader(raw))
		decoder.DisallowUnknownFields()
		var record Record
		if err := decoder.Decode(&record); err != nil {
			return nil, fmt.Errorf("eval: record line %d: %w", line, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			if err == nil {
				return nil, fmt.Errorf("eval: record line %d has multiple JSON values", line)
			}
			return nil, fmt.Errorf("eval: record line %d has trailing JSON: %w", line, err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("eval: read records: %w", err)
	}
	if _, _, err := indexRunResults(records); err != nil {
		return nil, err
	}
	validIntents := make(map[Intent]struct{}, len(AllIntents()))
	for _, intent := range AllIntents() {
		validIntents[intent] = struct{}{}
	}
	for index, record := range records {
		if record.Revision != records[0].Revision || record.ConfigurationID != records[0].ConfigurationID {
			return nil, fmt.Errorf("eval: record %d has mixed baseline identity", index+1)
		}
		if record.BuildSHA256 != "" {
			canonical, ok := buildidentity.Parse(record.BuildSHA256)
			if !ok || canonical != record.BuildSHA256 {
				return nil, fmt.Errorf("eval: record %d has invalid build_sha256", index+1)
			}
		}
		if record.ConfigurationSHA256 != "" {
			canonical, ok := buildidentity.Parse(record.ConfigurationSHA256)
			if !ok || canonical != record.ConfigurationSHA256 {
				return nil, fmt.Errorf("eval: record %d has invalid configuration_sha256", index+1)
			}
		}
		if record.CaseSHA256 != "" {
			canonical, ok := buildidentity.Parse(record.CaseSHA256)
			if !ok || canonical != record.CaseSHA256 {
				return nil, fmt.Errorf("eval: record %d has invalid case_sha256", index+1)
			}
		}
		if !validBaselineIdentity(record.Revision) || !validBaselineIdentity(record.ConfigurationID) {
			return nil, fmt.Errorf("eval: record %d has invalid baseline identity", index+1)
		}
		if _, valid := validIntents[record.Intent]; !valid || !strings.HasPrefix(record.CaseID, string(record.Intent)+"-") {
			return nil, fmt.Errorf("eval: record %d has invalid intent", index+1)
		}
		if strings.TrimSpace(record.Query) == "" || (record.SearchDepth != "basic" && record.SearchDepth != "advanced") {
			return nil, fmt.Errorf("eval: record %d has invalid query metadata", index+1)
		}
		if record.DurationMS < 0 || record.HTTPStatus < 0 || record.HTTPStatus > 599 {
			return nil, fmt.Errorf("eval: record %d has invalid response metadata", index+1)
		}
		if err := validateRecordObservations(record, index+1); err != nil {
			return nil, err
		}
	}
	if _, err := validateRunBuildIdentity(records, false); err != nil {
		return nil, err
	}
	if _, err := validateRunConfigurationIdentity(records, false, ""); err != nil {
		return nil, err
	}
	if err := validateLabelTemplateBudget(records, maxEvaluationArtifactLineSize, maxEvaluationArtifactBytes); err != nil {
		return nil, err
	}
	return records, nil
}

func validateRecordObservations(record Record, position int) error {
	if record.ResultCount > 50 || (!record.Attempted && (record.DurationMS != 0 || record.HTTPStatus != 0 || len(record.Results) != 0 || record.Discovery != nil)) {
		return fmt.Errorf("eval: record %d has invalid attempt state", position)
	}
	succeeded := record.Error == "" && record.HTTPStatus >= 200 && record.HTTPStatus < 300
	if record.Error == "" && !succeeded {
		return fmt.Errorf("eval: record %d has invalid success state", position)
	}
	if len(record.Results) > 0 && !succeeded {
		return fmt.Errorf("eval: record %d retains results for a failed request", position)
	}
	if record.Discovery != nil && record.HTTPStatus == 0 {
		return fmt.Errorf("eval: record %d retains discovery evidence without an HTTP response", position)
	}
	if err := validateDiscoveryHTTPOutcome(record.Discovery, succeeded); err != nil {
		return fmt.Errorf("eval: record %d has invalid discovery evidence: %w", position, err)
	}
	expected := expectedDomainSet(record.ExpectedDomains)
	for _, domain := range record.ExpectedDomains {
		if !validExpectedDomain(domain) {
			return fmt.Errorf("eval: record %d has invalid expected domain", position)
		}
	}
	domains := make(map[string]struct{}, len(record.Results))
	extractions := 0
	published := 0
	expectedHits := 0
	for resultIndex, result := range record.Results {
		parsed, _ := url.Parse(result.URL)
		domain := strings.ToLower(parsed.Hostname())
		if result.Domain != domain {
			return fmt.Errorf("eval: record %d result %d has inconsistent domain", position, resultIndex+1)
		}
		if domain != "" {
			domains[domain] = struct{}{}
			if domainExpected(domain, expected) {
				expectedHits++
			}
		}
		if result.Extracted {
			extractions++
		}
		if result.PublishedAt != nil {
			published++
		}
		if result.ProvenanceMalformed && len(result.Sources) != 0 {
			return fmt.Errorf("eval: record %d result %d mixes malformed and typed provenance", position, resultIndex+1)
		}
		seenSources := make(map[SourceObservation]struct{}, len(result.Sources))
		for _, source := range result.Sources {
			normalized, ok := normalizeSourceObservation(source)
			if !ok || !validNormalizedSourceObservation(normalized) {
				return fmt.Errorf("eval: record %d result %d has invalid source provenance", position, resultIndex+1)
			}
			if _, duplicate := seenSources[normalized]; duplicate {
				return fmt.Errorf("eval: record %d result %d repeats source provenance", position, resultIndex+1)
			}
			seenSources[normalized] = struct{}{}
		}
	}
	provenance := aggregateProvenance(record.Results)
	if record.UniqueDomains != len(domains) || record.ExtractionSuccesses != extractions ||
		record.PublishedResults != published || record.ExpectedDomainHits != expectedHits ||
		record.ProvenanceResults != provenance.results || record.ProvenanceMalformedResults != provenance.malformed ||
		record.MultiSourceResults != provenance.multiSource {
		return fmt.Errorf("eval: record %d has inconsistent derived counts", position)
	}
	return nil
}

func validateDiscoveryReport(report *search.DiscoveryReport) error {
	if report == nil {
		return nil
	}
	return search.ValidateDiscoveryReport(*report)
}

func validateDiscoveryHTTPOutcome(report *search.DiscoveryReport, succeeded bool) error {
	if err := validateDiscoveryReport(report); err != nil {
		return err
	}
	if report != nil && succeeded && report.Status == search.BatchFailed {
		return errors.New("discovery evidence is inconsistent with HTTP outcome")
	}
	return nil
}
