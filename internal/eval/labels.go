package eval

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

const maxRelevanceLabels = 50_000

// RelevanceLabel is one human judgment bound to an exact run, case, and
// returned URL. Relevance uses a deliberately small ordinal scale:
// 0 irrelevant, 1 marginal, 2 relevant, 3 highly relevant.
type RelevanceLabel struct {
	SchemaVersion int    `json:"schema_version"`
	RunID         string `json:"run_id"`
	CaseID        string `json:"case_id"`
	URL           string `json:"url"`
	Relevance     int    `json:"relevance"`
}

type relevanceLabelDocument struct {
	SchemaVersion int     `json:"schema_version"`
	RunID         string  `json:"run_id"`
	CaseID        string  `json:"case_id"`
	Intent        *Intent `json:"intent,omitempty"`
	Query         *string `json:"query,omitempty"`
	URL           string  `json:"url"`
	Title         *string `json:"title,omitempty"`
	Domain        *string `json:"domain,omitempty"`
	Rank          *int    `json:"rank,omitempty"`
	Relevance     *int    `json:"relevance"`
}

// RelevanceIntentSummary keeps label coverage and ranking quality comparable
// across the fixed intent categories.
type RelevanceIntentSummary struct {
	EligibleCases           int     `json:"eligible_cases"`
	TotalResults            int     `json:"total_results"`
	LabeledResults          int     `json:"labeled_results"`
	LabelCoverageRate       float64 `json:"label_coverage_rate"`
	RelevantHitCases        int     `json:"relevant_hit_cases"`
	RelevantHitCoverageRate float64 `json:"relevant_hit_coverage_rate"`
	RelevantResults         int     `json:"relevant_results"`
	MeanRelevance           float64 `json:"mean_relevance"`
	FullyLabeledCases       int     `json:"fully_labeled_cases"`
	RankingSamples          int     `json:"ranking_samples"`
	MeanPrecisionAt5        float64 `json:"mean_precision_at_5"`
	MeanNDCGAt10            float64 `json:"mean_ndcg_at_10"`
	MeanReciprocalRank      float64 `json:"mean_reciprocal_rank"`
}

// RelevanceSummary is a deterministic offline report. Relevant-hit coverage is
// a lower bound over supplied labels, while ranking metrics include only
// successful, non-empty cases whose returned results are all labeled; missing
// labels are reported as missing rather than treated as irrelevant.
type RelevanceSummary struct {
	SchemaVersion           int                               `json:"schema_version"`
	RunID                   string                            `json:"run_id"`
	TotalCases              int                               `json:"total_cases"`
	EligibleCases           int                               `json:"eligible_cases"`
	TotalResults            int                               `json:"total_results"`
	LabeledResults          int                               `json:"labeled_results"`
	LabelCoverageRate       float64                           `json:"label_coverage_rate"`
	RelevantHitCases        int                               `json:"relevant_hit_cases"`
	RelevantHitCoverageRate float64                           `json:"relevant_hit_coverage_rate"`
	RelevantResults         int                               `json:"relevant_results"`
	HighlyRelevantResults   int                               `json:"highly_relevant_results"`
	RelevantRate            float64                           `json:"relevant_rate"`
	MeanRelevance           float64                           `json:"mean_relevance"`
	FullyLabeledCases       int                               `json:"fully_labeled_cases"`
	RankingSamples          int                               `json:"ranking_samples"`
	MeanPrecisionAt5        float64                           `json:"mean_precision_at_5"`
	MeanNDCGAt10            float64                           `json:"mean_ndcg_at_10"`
	MeanReciprocalRank      float64                           `json:"mean_reciprocal_rank"`
	GradeCounts             map[string]int                    `json:"grade_counts"`
	SourceRelevantResults   map[string]int                    `json:"source_relevant_results,omitempty"`
	SourceMeanRelevance     map[string]float64                `json:"source_mean_relevance,omitempty"`
	LaneRelevantResults     map[string]int                    `json:"lane_relevant_results,omitempty"`
	LaneMeanRelevance       map[string]float64                `json:"lane_mean_relevance,omitempty"`
	ByIntent                map[Intent]RelevanceIntentSummary `json:"by_intent"`
}

type resultKey struct {
	caseID string
	url    string
}

type indexedResult struct {
	result ResultObservation
	intent Intent
	query  string
	rank   int
}

// LoadRelevanceLabels strictly decodes human judgments and binds every entry
// to a result that actually appeared in the supplied run.
func LoadRelevanceLabels(reader io.Reader, records []Record) ([]RelevanceLabel, error) {
	if reader == nil {
		return nil, errors.New("eval: nil relevance-label reader")
	}
	runID, results, err := indexRunResults(records)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), maxEvaluationArtifactLineSize)
	labels := make([]RelevanceLabel, 0)
	seen := make(map[resultKey]struct{})
	totalBytes := 0
	for line := 1; scanner.Scan(); line++ {
		totalBytes += len(scanner.Bytes()) + 1
		if totalBytes > maxEvaluationArtifactBytes {
			return nil, fmt.Errorf("eval: relevance-label artifact exceeds %d bytes", maxEvaluationArtifactBytes)
		}
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" {
			continue
		}
		if len(labels) >= maxRelevanceLabels {
			return nil, fmt.Errorf("eval: relevance labels exceed %d entries", maxRelevanceLabels)
		}
		var document relevanceLabelDocument
		decoder := json.NewDecoder(strings.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&document); err != nil {
			return nil, fmt.Errorf("eval: relevance label line %d: %w", line, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			if err == nil {
				return nil, fmt.Errorf("eval: relevance label line %d has multiple JSON values", line)
			}
			return nil, fmt.Errorf("eval: relevance label line %d has trailing JSON: %w", line, err)
		}
		if document.SchemaVersion != 1 || document.RunID != runID || document.Relevance == nil || *document.Relevance < 0 || *document.Relevance > 3 {
			return nil, fmt.Errorf("eval: relevance label line %d has invalid version, run, or relevance", line)
		}
		key := resultKey{caseID: document.CaseID, url: document.URL}
		observed, exists := results[key]
		if !exists {
			return nil, fmt.Errorf("eval: relevance label line %d does not match a run result", line)
		}
		if (document.Intent != nil && *document.Intent != observed.intent) ||
			(document.Query != nil && *document.Query != observed.query) ||
			(document.Title != nil && *document.Title != observed.result.Title) ||
			(document.Domain != nil && *document.Domain != observed.result.Domain) ||
			(document.Rank != nil && *document.Rank != observed.rank) {
			return nil, fmt.Errorf("eval: relevance label line %d has altered template metadata", line)
		}
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("eval: relevance label line %d duplicates a run result", line)
		}
		seen[key] = struct{}{}
		labels = append(labels, RelevanceLabel{
			SchemaVersion: 1, RunID: document.RunID, CaseID: document.CaseID,
			URL: document.URL, Relevance: *document.Relevance,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("eval: read relevance labels: %w", err)
	}
	if len(labels) == 0 {
		return nil, errors.New("eval: relevance-label file is empty or unfilled")
	}
	return labels, nil
}

type labelTemplateEntry struct {
	SchemaVersion int    `json:"schema_version"`
	RunID         string `json:"run_id"`
	CaseID        string `json:"case_id"`
	Intent        Intent `json:"intent"`
	Query         string `json:"query"`
	URL           string `json:"url"`
	Title         string `json:"title,omitempty"`
	Domain        string `json:"domain,omitempty"`
	Rank          int    `json:"rank"`
	Relevance     *int   `json:"relevance"`
}

// WriteLabelTemplate emits one stable, intentionally incomplete JSONL row per
// returned result. Source identity is omitted so relevance judgments can be
// made without provider-name bias.
func WriteLabelTemplate(writer io.Writer, records []Record) error {
	if writer == nil {
		return errors.New("eval: nil label-template writer")
	}
	if _, _, err := indexRunResults(records); err != nil {
		return err
	}
	if err := validateLabelTemplateBudget(records, maxEvaluationArtifactLineSize, maxEvaluationArtifactBytes); err != nil {
		return err
	}
	for _, record := range records {
		for index, result := range record.Results {
			encoded, err := marshalLabelTemplateEntry(record, result, index+1)
			if err != nil {
				return err
			}
			encoded = append(encoded, '\n')
			written, err := writer.Write(encoded)
			if err != nil {
				return fmt.Errorf("eval: write label template: %w", err)
			}
			if written != len(encoded) {
				return fmt.Errorf("eval: write label template: %w", io.ErrShortWrite)
			}
		}
	}
	return nil
}

func validateLabelTemplateBudget(records []Record, maxLineBytes, maxTotalBytes int) error {
	if maxLineBytes <= 0 || maxTotalBytes <= 0 {
		return errors.New("eval: invalid label-template budget")
	}
	totalBytes := 0
	for _, record := range records {
		for index, result := range record.Results {
			encoded, err := marshalLabelTemplateEntry(record, result, index+1)
			if err != nil {
				return err
			}
			lineBytes := len(encoded) + 1
			if lineBytes > maxLineBytes {
				return fmt.Errorf("eval: label-template row exceeds %d bytes", maxLineBytes)
			}
			if lineBytes > maxTotalBytes-totalBytes {
				return fmt.Errorf("eval: label-template artifact exceeds %d bytes", maxTotalBytes)
			}
			totalBytes += lineBytes
		}
	}
	return nil
}

func marshalLabelTemplateEntry(record Record, result ResultObservation, rank int) ([]byte, error) {
	encoded, err := json.Marshal(makeLabelTemplateEntry(record, result, rank))
	if err != nil {
		return nil, fmt.Errorf("eval: encode label template: %w", err)
	}
	return encoded, nil
}

func makeLabelTemplateEntry(record Record, result ResultObservation, rank int) labelTemplateEntry {
	return labelTemplateEntry{
		SchemaVersion: 1, RunID: record.RunID, CaseID: record.CaseID,
		Intent: record.Intent, Query: record.Query, URL: result.URL,
		Title: result.Title, Domain: result.Domain, Rank: rank,
	}
}

// ScoreRelevance combines a run with strict human labels. It never treats an
// absent label as grade zero.
func ScoreRelevance(records []Record, labels []RelevanceLabel) (RelevanceSummary, error) {
	runID, indexed, err := indexRunResults(records)
	if err != nil {
		return RelevanceSummary{}, err
	}
	labelByResult := make(map[resultKey]int, len(labels))
	for index, label := range labels {
		key := resultKey{caseID: label.CaseID, url: label.URL}
		if label.SchemaVersion != 1 || label.RunID != runID || label.Relevance < 0 || label.Relevance > 3 {
			return RelevanceSummary{}, fmt.Errorf("eval: relevance label %d has invalid version, run, or relevance", index+1)
		}
		if _, exists := indexed[key]; !exists {
			return RelevanceSummary{}, fmt.Errorf("eval: relevance label %d does not match a run result", index+1)
		}
		if _, duplicate := labelByResult[key]; duplicate {
			return RelevanceSummary{}, fmt.Errorf("eval: relevance label %d duplicates a run result", index+1)
		}
		labelByResult[key] = label.Relevance
	}

	report := RelevanceSummary{
		SchemaVersion: 1, RunID: runID, TotalCases: len(records),
		GradeCounts:           map[string]int{"0": 0, "1": 0, "2": 0, "3": 0},
		SourceRelevantResults: map[string]int{}, SourceMeanRelevance: map[string]float64{},
		LaneRelevantResults: map[string]int{}, LaneMeanRelevance: map[string]float64{},
		ByIntent: map[Intent]RelevanceIntentSummary{},
	}
	sourceGradeSum := map[string]int{}
	sourceLabelCount := map[string]int{}
	laneGradeSum := map[string]int{}
	laneLabelCount := map[string]int{}
	intentGradeSum := map[Intent]int{}
	intentNDCGSum := map[Intent]float64{}
	intentMRRSum := map[Intent]float64{}
	intentPrecisionAt5Sum := map[Intent]float64{}
	var relevanceSum int
	var precisionAt5Sum, ndcgSum, mrrSum float64

	for _, record := range records {
		intentSummary := report.ByIntent[record.Intent]
		report.TotalResults += len(record.Results)
		intentSummary.TotalResults += len(record.Results)
		eligible := record.Error == "" && record.HTTPStatus >= 200 && record.HTTPStatus < 300
		if eligible {
			report.EligibleCases++
			intentSummary.EligibleCases++
		}
		grades := make([]int, len(record.Results))
		fullyLabeled := eligible && len(record.Results) > 0
		hasRelevantLabel := false
		for index, result := range record.Results {
			grade, labeled := labelByResult[resultKey{caseID: record.CaseID, url: result.URL}]
			if !labeled {
				fullyLabeled = false
				continue
			}
			grades[index] = grade
			report.LabeledResults++
			intentSummary.LabeledResults++
			relevanceSum += grade
			intentGradeSum[record.Intent] += grade
			report.GradeCounts[strconv.Itoa(grade)]++
			if grade >= 2 {
				hasRelevantLabel = true
				report.RelevantResults++
				intentSummary.RelevantResults++
			}
			if grade == 3 {
				report.HighlyRelevantResults++
			}
			distinctSources := map[string]struct{}{}
			for _, source := range result.Sources {
				normalized, ok := normalizeSourceObservation(source)
				if !ok || !validNormalizedSourceObservation(normalized) {
					continue
				}
				source = normalized
				provider := source.Provider
				if source.ProviderUnavailable {
					provider = "unavailable"
				}
				lane := provider + "/" + source.Lane + ":" + source.Variant
				laneGradeSum[lane] += grade
				laneLabelCount[lane]++
				if grade >= 2 {
					report.LaneRelevantResults[lane]++
				}
				if !source.ProviderUnavailable {
					distinctSources[source.Provider] = struct{}{}
				}
			}
			for source := range distinctSources {
				sourceGradeSum[source] += grade
				sourceLabelCount[source]++
				if grade >= 2 {
					report.SourceRelevantResults[source]++
				}
			}
		}
		if eligible && hasRelevantLabel {
			report.RelevantHitCases++
			intentSummary.RelevantHitCases++
		}
		if fullyLabeled {
			precision := precisionAt(grades, 5)
			ndcg := ndcgAt(grades, 10)
			mrr := reciprocalRank(grades)
			report.FullyLabeledCases++
			report.RankingSamples++
			precisionAt5Sum += precision
			ndcgSum += ndcg
			mrrSum += mrr
			intentSummary.FullyLabeledCases++
			intentSummary.RankingSamples++
			intentPrecisionAt5Sum[record.Intent] += precision
			intentNDCGSum[record.Intent] += ndcg
			intentMRRSum[record.Intent] += mrr
		}
		report.ByIntent[record.Intent] = intentSummary
	}

	if report.TotalResults > 0 {
		report.LabelCoverageRate = float64(report.LabeledResults) / float64(report.TotalResults)
	}
	if report.LabeledResults > 0 {
		report.RelevantRate = float64(report.RelevantResults) / float64(report.LabeledResults)
		report.MeanRelevance = float64(relevanceSum) / float64(report.LabeledResults)
	}
	if report.EligibleCases > 0 {
		report.RelevantHitCoverageRate = float64(report.RelevantHitCases) / float64(report.EligibleCases)
	}
	if report.RankingSamples > 0 {
		report.MeanPrecisionAt5 = precisionAt5Sum / float64(report.RankingSamples)
		report.MeanNDCGAt10 = ndcgSum / float64(report.RankingSamples)
		report.MeanReciprocalRank = mrrSum / float64(report.RankingSamples)
	}
	for source, count := range sourceLabelCount {
		report.SourceMeanRelevance[source] = float64(sourceGradeSum[source]) / float64(count)
	}
	for lane, count := range laneLabelCount {
		report.LaneMeanRelevance[lane] = float64(laneGradeSum[lane]) / float64(count)
	}
	for intent, summary := range report.ByIntent {
		if summary.TotalResults > 0 {
			summary.LabelCoverageRate = float64(summary.LabeledResults) / float64(summary.TotalResults)
		}
		if summary.LabeledResults > 0 {
			summary.MeanRelevance = float64(intentGradeSum[intent]) / float64(summary.LabeledResults)
		}
		if summary.EligibleCases > 0 {
			summary.RelevantHitCoverageRate = float64(summary.RelevantHitCases) / float64(summary.EligibleCases)
		}
		if summary.RankingSamples > 0 {
			summary.MeanPrecisionAt5 = intentPrecisionAt5Sum[intent] / float64(summary.RankingSamples)
			summary.MeanNDCGAt10 = intentNDCGSum[intent] / float64(summary.RankingSamples)
			summary.MeanReciprocalRank = intentMRRSum[intent] / float64(summary.RankingSamples)
		}
		report.ByIntent[intent] = summary
	}
	return report, nil
}

func precisionAt(grades []int, limit int) float64 {
	if len(grades) == 0 || limit <= 0 {
		return 0
	}
	if len(grades) < limit {
		limit = len(grades)
	}
	relevant := 0
	for _, grade := range grades[:limit] {
		if grade >= 2 {
			relevant++
		}
	}
	return float64(relevant) / float64(limit)
}

func indexRunResults(records []Record) (string, map[resultKey]indexedResult, error) {
	if len(records) == 0 {
		return "", nil, errors.New("eval: evaluation run is empty")
	}
	runID := records[0].RunID
	if strings.TrimSpace(runID) == "" || !validBaselineIdentity(runID) {
		return "", nil, errors.New("eval: evaluation run has an invalid run_id")
	}
	results := make(map[resultKey]indexedResult)
	seenCases := make(map[string]struct{}, len(records))
	for recordIndex, record := range records {
		if record.SchemaVersion != 1 || record.RunID != runID || !caseIDPattern.MatchString(record.CaseID) {
			return "", nil, fmt.Errorf("eval: record %d has invalid version, run, or case", recordIndex+1)
		}
		if _, duplicate := seenCases[record.CaseID]; duplicate {
			return "", nil, fmt.Errorf("eval: record %d duplicates case %q", recordIndex+1, record.CaseID)
		}
		seenCases[record.CaseID] = struct{}{}
		if record.ResultCount != len(record.Results) || record.NonEmpty != (len(record.Results) > 0) {
			return "", nil, fmt.Errorf("eval: record %d result counts are inconsistent", recordIndex+1)
		}
		for resultIndex, result := range record.Results {
			parsed, err := url.Parse(result.URL)
			if err != nil || parsed.User != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
				return "", nil, fmt.Errorf("eval: record %d result %d has invalid URL", recordIndex+1, resultIndex+1)
			}
			key := resultKey{caseID: record.CaseID, url: result.URL}
			if _, duplicate := results[key]; duplicate {
				return "", nil, fmt.Errorf("eval: record %d repeats result URL", recordIndex+1)
			}
			results[key] = indexedResult{result: result, intent: record.Intent, query: record.Query, rank: resultIndex + 1}
		}
	}
	return runID, results, nil
}

func ndcgAt(grades []int, limit int) float64 {
	if len(grades) == 0 || limit <= 0 {
		return 0
	}
	if len(grades) < limit {
		limit = len(grades)
	}
	dcg := discountedGain(grades[:limit])
	ideal := append([]int(nil), grades...)
	sort.Sort(sort.Reverse(sort.IntSlice(ideal)))
	idcg := discountedGain(ideal[:limit])
	if idcg == 0 {
		return 0
	}
	return dcg / idcg
}

func discountedGain(grades []int) float64 {
	var total float64
	for index, grade := range grades {
		gain := math.Pow(2, float64(grade)) - 1
		total += gain / math.Log2(float64(index)+2)
	}
	return total
}

func reciprocalRank(grades []int) float64 {
	for index, grade := range grades {
		if grade >= 2 {
			return 1 / float64(index+1)
		}
	}
	return 0
}
