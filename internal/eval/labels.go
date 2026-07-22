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

	"github.com/staticvar/fetchmark/internal/buildidentity"
)

const (
	maxRelevanceLabels = 50_000
	// Bound cross-run provenance separately from one maximum-size run.
	maxPooledRelevanceLabels = maxRelevanceLabels * 5
)

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

// JudgmentOrigin records whether a pooled judgment is provisional assistant
// evidence or independent human ground truth. The distinction is mandatory so
// provisional tuning evidence cannot silently satisfy a human-label gate.
type JudgmentOrigin string

const (
	JudgmentOriginAssistantProvisional JudgmentOrigin = "assistant_provisional"
	JudgmentOriginIndependentHuman     JudgmentOrigin = "independent_human"
)

// PooledRelevanceLabel is a provenance-bound relevance judgment that can be
// reused across runs for the same fixed evaluation case and exact URL. The
// complete case digest plus intent/query metadata reject pools from another
// suite revision or request-control configuration.
type PooledRelevanceLabel struct {
	SchemaVersion  int
	SourceRunID    string
	JudgmentOrigin JudgmentOrigin
	CaseID         string
	Intent         Intent
	Query          string
	CaseSHA256     string
	URL            string
	Relevance      int
}

type relevanceLabelDocument struct {
	SchemaVersion  int             `json:"schema_version"`
	RunID          string          `json:"run_id"`
	JudgmentOrigin *JudgmentOrigin `json:"judgment_origin,omitempty"`
	CaseID         string          `json:"case_id"`
	Intent         *Intent         `json:"intent,omitempty"`
	Query          *string         `json:"query,omitempty"`
	CaseSHA256     *string         `json:"case_sha256,omitempty"`
	URL            string          `json:"url"`
	Title          *string         `json:"title,omitempty"`
	Domain         *string         `json:"domain,omitempty"`
	Rank           *int            `json:"rank,omitempty"`
	Relevance      *int            `json:"relevance"`
}

// RelevanceIntentSummary keeps label coverage and ranking quality comparable
// across the fixed intent categories.
type RelevanceIntentSummary struct {
	TotalCases                    int      `json:"total_cases"`
	EligibleCases                 int      `json:"eligible_cases"`
	TotalResults                  int      `json:"total_results"`
	LabeledResults                int      `json:"labeled_results"`
	LabelCoverageRate             float64  `json:"label_coverage_rate"`
	RelevantHitCases              int      `json:"relevant_hit_cases"`
	RelevantHitCoverageRate       float64  `json:"relevant_hit_coverage_rate"`
	RelevantResults               int      `json:"relevant_results"`
	MeanRelevance                 float64  `json:"mean_relevance"`
	FullyLabeledCases             int      `json:"fully_labeled_cases"`
	RankingSamples                int      `json:"ranking_samples"`
	MeanPrecisionAt5              float64  `json:"mean_precision_at_5"`
	MeanNDCGAt10                  float64  `json:"mean_ndcg_at_10"`
	MeanReciprocalRank            float64  `json:"mean_reciprocal_rank"`
	AllEligibleRankingCases       int      `json:"all_eligible_ranking_cases"`
	AllEligibleRankingComplete    bool     `json:"all_eligible_ranking_complete"`
	AllEligibleRelevantResultsAt5 *int     `json:"all_eligible_relevant_results_at_5"`
	AllEligibleRelevantYieldAt5   *float64 `json:"all_eligible_relevant_yield_at_5"`
	AllEligibleMeanPrecisionAt5   *float64 `json:"all_eligible_mean_precision_at_5"`
	AllEligibleMeanNDCGAt10       *float64 `json:"all_eligible_mean_ndcg_at_10"`
	AllEligibleMeanReciprocalRank *float64 `json:"all_eligible_mean_reciprocal_rank"`
	AllQueryRankingCases          int      `json:"all_query_ranking_cases"`
	AllQueryRankingComplete       bool     `json:"all_query_ranking_complete"`
	AllQueryRelevantResultsAt5    *int     `json:"all_query_relevant_results_at_5"`
	AllQueryRelevantYieldAt5      *float64 `json:"all_query_relevant_yield_at_5"`
	AllQueryMeanPrecisionAt5      *float64 `json:"all_query_mean_precision_at_5"`
	AllQueryMeanNDCGAt10          *float64 `json:"all_query_mean_ndcg_at_10"`
	AllQueryMeanReciprocalRank    *float64 `json:"all_query_mean_reciprocal_rank"`
}

// RelevanceSummary is a deterministic offline report. Relevant-hit coverage is
// a lower bound over supplied labels, while ranking metrics include only
// successful, non-empty cases whose returned results are all labeled; missing
// labels are reported as missing rather than treated as irrelevant.
type RelevanceSummary struct {
	SchemaVersion                 int                               `json:"schema_version"`
	RunID                         string                            `json:"run_id"`
	TotalCases                    int                               `json:"total_cases"`
	EligibleCases                 int                               `json:"eligible_cases"`
	TotalResults                  int                               `json:"total_results"`
	LabeledResults                int                               `json:"labeled_results"`
	LabelCoverageRate             float64                           `json:"label_coverage_rate"`
	RelevantHitCases              int                               `json:"relevant_hit_cases"`
	RelevantHitCoverageRate       float64                           `json:"relevant_hit_coverage_rate"`
	RelevantResults               int                               `json:"relevant_results"`
	HighlyRelevantResults         int                               `json:"highly_relevant_results"`
	RelevantRate                  float64                           `json:"relevant_rate"`
	MeanRelevance                 float64                           `json:"mean_relevance"`
	FullyLabeledCases             int                               `json:"fully_labeled_cases"`
	RankingSamples                int                               `json:"ranking_samples"`
	MeanPrecisionAt5              float64                           `json:"mean_precision_at_5"`
	MeanNDCGAt10                  float64                           `json:"mean_ndcg_at_10"`
	MeanReciprocalRank            float64                           `json:"mean_reciprocal_rank"`
	NDCGIdealScope                string                            `json:"ndcg_ideal_scope"`
	AllEligibleRankingCases       int                               `json:"all_eligible_ranking_cases"`
	AllEligibleRankingComplete    bool                              `json:"all_eligible_ranking_complete"`
	AllEligibleRelevantResultsAt5 *int                              `json:"all_eligible_relevant_results_at_5"`
	AllEligibleRelevantYieldAt5   *float64                          `json:"all_eligible_relevant_yield_at_5"`
	AllEligibleMeanPrecisionAt5   *float64                          `json:"all_eligible_mean_precision_at_5"`
	AllEligibleMeanNDCGAt10       *float64                          `json:"all_eligible_mean_ndcg_at_10"`
	AllEligibleMeanReciprocalRank *float64                          `json:"all_eligible_mean_reciprocal_rank"`
	AllQueryRankingCases          int                               `json:"all_query_ranking_cases"`
	AllQueryRankingComplete       bool                              `json:"all_query_ranking_complete"`
	AllQueryRelevantResultsAt5    *int                              `json:"all_query_relevant_results_at_5"`
	AllQueryRelevantYieldAt5      *float64                          `json:"all_query_relevant_yield_at_5"`
	AllQueryMeanPrecisionAt5      *float64                          `json:"all_query_mean_precision_at_5"`
	AllQueryMeanNDCGAt10          *float64                          `json:"all_query_mean_ndcg_at_10"`
	AllQueryMeanReciprocalRank    *float64                          `json:"all_query_mean_reciprocal_rank"`
	GradeCounts                   map[string]int                    `json:"grade_counts"`
	SourceRelevantResults         map[string]int                    `json:"source_relevant_results,omitempty"`
	SourceMeanRelevance           map[string]float64                `json:"source_mean_relevance,omitempty"`
	LaneRelevantResults           map[string]int                    `json:"lane_relevant_results,omitempty"`
	LaneMeanRelevance             map[string]float64                `json:"lane_mean_relevance,omitempty"`
	ByIntent                      map[Intent]RelevanceIntentSummary `json:"by_intent"`
}

type resultKey struct {
	caseID string
	url    string
}

type pooledJudgmentSourceKey struct {
	resultKey
	runID string
}

type pooledCaseMetadata struct {
	intent     Intent
	query      string
	caseSHA256 string
}

type indexedResult struct {
	result     ResultObservation
	intent     Intent
	query      string
	caseSHA256 string
	rank       int
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
			(document.CaseSHA256 != nil && *document.CaseSHA256 != observed.caseSHA256) ||
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

// LoadPooledRelevanceLabels decodes judgments that may have been concatenated
// from multiple run-bound label artifacts. Every row must retain the exact
// source run, complete fixed-case digest, intent/query, and judgment origin.
// Repeated URLs from distinct source runs remain separate provenance records,
// while conflicting grades for one case and URL are rejected.
func LoadPooledRelevanceLabels(reader io.Reader) ([]PooledRelevanceLabel, error) {
	if reader == nil {
		return nil, errors.New("eval: nil pooled relevance-label reader")
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), maxEvaluationArtifactLineSize)
	labels := make([]PooledRelevanceLabel, 0)
	judgmentByResult := make(map[resultKey]PooledRelevanceLabel)
	originBySource := make(map[pooledJudgmentSourceKey]JudgmentOrigin)
	metadataByCase := make(map[string]pooledCaseMetadata)
	totalBytes := 0
	for line := 1; scanner.Scan(); line++ {
		totalBytes += len(scanner.Bytes()) + 1
		if totalBytes > maxEvaluationArtifactBytes {
			return nil, fmt.Errorf("eval: pooled relevance-label artifact exceeds %d bytes", maxEvaluationArtifactBytes)
		}
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" {
			continue
		}
		if len(labels) >= maxPooledRelevanceLabels {
			return nil, fmt.Errorf("eval: pooled relevance labels exceed %d entries", maxPooledRelevanceLabels)
		}
		var document relevanceLabelDocument
		decoder := json.NewDecoder(strings.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&document); err != nil {
			return nil, fmt.Errorf("eval: pooled relevance label line %d: %w", line, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			if err == nil {
				return nil, fmt.Errorf("eval: pooled relevance label line %d has multiple JSON values", line)
			}
			return nil, fmt.Errorf("eval: pooled relevance label line %d has trailing JSON: %w", line, err)
		}
		if document.SchemaVersion != 1 || strings.TrimSpace(document.RunID) == "" || document.RunID != strings.TrimSpace(document.RunID) ||
			!validBaselineIdentity(document.RunID) || document.JudgmentOrigin == nil || !validJudgmentOrigin(*document.JudgmentOrigin) ||
			document.Intent == nil || document.Query == nil || document.CaseSHA256 == nil || !caseIDPattern.MatchString(document.CaseID) ||
			document.Relevance == nil || *document.Relevance < 0 || *document.Relevance > 3 {
			return nil, fmt.Errorf("eval: pooled relevance label line %d has invalid version, provenance, case metadata, or relevance", line)
		}
		parsed, err := url.Parse(document.URL)
		if err != nil || parsed.User != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return nil, fmt.Errorf("eval: pooled relevance label line %d has invalid URL", line)
		}
		if !validIntent(*document.Intent) || !strings.HasPrefix(document.CaseID, string(*document.Intent)+"-") {
			return nil, fmt.Errorf("eval: pooled relevance label line %d has invalid intent", line)
		}
		if strings.TrimSpace(*document.Query) == "" {
			return nil, fmt.Errorf("eval: pooled relevance label line %d has empty query metadata", line)
		}
		caseSHA256, ok := buildidentity.Parse(*document.CaseSHA256)
		if !ok || caseSHA256 != *document.CaseSHA256 {
			return nil, fmt.Errorf("eval: pooled relevance label line %d has invalid case_sha256", line)
		}
		metadata := pooledCaseMetadata{intent: *document.Intent, query: *document.Query, caseSHA256: caseSHA256}
		if existing, present := metadataByCase[document.CaseID]; present && existing != metadata {
			return nil, fmt.Errorf("eval: pooled relevance label line %d conflicts with case metadata", line)
		}
		metadataByCase[document.CaseID] = metadata

		key := resultKey{caseID: document.CaseID, url: document.URL}
		if existing, duplicate := judgmentByResult[key]; duplicate {
			if existing.Relevance != *document.Relevance || existing.Intent != *document.Intent || existing.Query != *document.Query || existing.CaseSHA256 != caseSHA256 {
				return nil, fmt.Errorf("eval: pooled relevance label line %d conflicts with an earlier judgment", line)
			}
		}
		sourceKey := pooledJudgmentSourceKey{resultKey: key, runID: document.RunID}
		if existingOrigin, duplicate := originBySource[sourceKey]; duplicate {
			if existingOrigin != *document.JudgmentOrigin {
				return nil, fmt.Errorf("eval: pooled relevance label line %d changes a source judgment origin", line)
			}
			return nil, fmt.Errorf("eval: pooled relevance label line %d duplicates a source judgment", line)
		}
		originBySource[sourceKey] = *document.JudgmentOrigin
		label := PooledRelevanceLabel{
			SchemaVersion:  1,
			SourceRunID:    document.RunID,
			JudgmentOrigin: *document.JudgmentOrigin,
			CaseID:         document.CaseID,
			Intent:         *document.Intent,
			Query:          *document.Query,
			CaseSHA256:     caseSHA256,
			URL:            document.URL,
			Relevance:      *document.Relevance,
		}
		judgmentByResult[key] = label
		labels = append(labels, label)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("eval: read pooled relevance labels: %w", err)
	}
	if len(labels) == 0 {
		return nil, errors.New("eval: pooled relevance-label file is empty or unfilled")
	}
	return labels, nil
}

func validJudgmentOrigin(origin JudgmentOrigin) bool {
	return origin == JudgmentOriginAssistantProvisional || origin == JudgmentOriginIndependentHuman
}

func validIntent(candidate Intent) bool {
	for _, intent := range AllIntents() {
		if candidate == intent {
			return true
		}
	}
	return false
}

type labelTemplateEntry struct {
	SchemaVersion int    `json:"schema_version"`
	RunID         string `json:"run_id"`
	CaseID        string `json:"case_id"`
	Intent        Intent `json:"intent"`
	Query         string `json:"query"`
	CaseSHA256    string `json:"case_sha256,omitempty"`
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
		Intent: record.Intent, Query: record.Query, CaseSHA256: record.CaseSHA256, URL: result.URL,
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
	return scoreRelevance(records, runID, labelByResult, nil, "run")
}

// ScoreRelevancePooled scores a run against a cross-run judgment pool. Every
// returned result still needs an exact judgment before all-query ranking
// metrics are emitted, while the NDCG ideal ranking includes all judged URLs
// for the case, including stronger candidates returned by another run.
func ScoreRelevancePooled(records []Record, labels []PooledRelevanceLabel) (RelevanceSummary, error) {
	runID, indexed, err := indexRunResults(records)
	if err != nil {
		return RelevanceSummary{}, err
	}
	if len(labels) == 0 {
		return RelevanceSummary{}, errors.New("eval: pooled relevance labels are empty")
	}
	if len(labels) > maxPooledRelevanceLabels {
		return RelevanceSummary{}, fmt.Errorf("eval: pooled relevance labels exceed %d entries", maxPooledRelevanceLabels)
	}
	recordByCase := make(map[string]Record, len(records))
	for index, record := range records {
		caseSHA256, ok := buildidentity.Parse(record.CaseSHA256)
		if !ok || caseSHA256 != record.CaseSHA256 {
			return RelevanceSummary{}, fmt.Errorf("eval: record %d is missing a valid case_sha256 required for pooled scoring", index+1)
		}
		recordByCase[record.CaseID] = record
	}
	pooledByResult := make(map[resultKey]int, len(labels))
	originBySource := make(map[pooledJudgmentSourceKey]JudgmentOrigin, len(labels))
	metadataByCase := make(map[string]pooledCaseMetadata)
	idealByCase := make(map[string][]int)
	for index, label := range labels {
		if label.SchemaVersion != 1 || strings.TrimSpace(label.SourceRunID) == "" || label.SourceRunID != strings.TrimSpace(label.SourceRunID) ||
			!validBaselineIdentity(label.SourceRunID) || !validJudgmentOrigin(label.JudgmentOrigin) ||
			!validIntent(label.Intent) || !strings.HasPrefix(label.CaseID, string(label.Intent)+"-") ||
			strings.TrimSpace(label.Query) == "" || !caseIDPattern.MatchString(label.CaseID) || label.Relevance < 0 || label.Relevance > 3 {
			return RelevanceSummary{}, fmt.Errorf("eval: pooled relevance label %d has invalid version, provenance, case metadata, or relevance", index+1)
		}
		caseSHA256, ok := buildidentity.Parse(label.CaseSHA256)
		if !ok || caseSHA256 != label.CaseSHA256 {
			return RelevanceSummary{}, fmt.Errorf("eval: pooled relevance label %d has invalid case_sha256", index+1)
		}
		parsed, parseErr := url.Parse(label.URL)
		if parseErr != nil || parsed.User != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return RelevanceSummary{}, fmt.Errorf("eval: pooled relevance label %d has invalid URL", index+1)
		}
		if record, exists := recordByCase[label.CaseID]; exists {
			if label.Intent != record.Intent || label.Query != record.Query || label.CaseSHA256 != record.CaseSHA256 {
				return RelevanceSummary{}, fmt.Errorf("eval: pooled relevance label %d does not match run case metadata", index+1)
			}
		}
		metadata := pooledCaseMetadata{intent: label.Intent, query: label.Query, caseSHA256: label.CaseSHA256}
		if existing, present := metadataByCase[label.CaseID]; present && existing != metadata {
			return RelevanceSummary{}, fmt.Errorf("eval: pooled relevance label %d conflicts with case metadata", index+1)
		}
		metadataByCase[label.CaseID] = metadata
		key := resultKey{caseID: label.CaseID, url: label.URL}
		if existing, duplicate := pooledByResult[key]; duplicate {
			if existing != label.Relevance {
				return RelevanceSummary{}, fmt.Errorf("eval: pooled relevance label %d conflicts with an earlier judgment", index+1)
			}
		} else {
			pooledByResult[key] = label.Relevance
			idealByCase[label.CaseID] = append(idealByCase[label.CaseID], label.Relevance)
		}
		sourceKey := pooledJudgmentSourceKey{resultKey: key, runID: label.SourceRunID}
		if existingOrigin, duplicate := originBySource[sourceKey]; duplicate {
			if existingOrigin != label.JudgmentOrigin {
				return RelevanceSummary{}, fmt.Errorf("eval: pooled relevance label %d changes a source judgment origin", index+1)
			}
			return RelevanceSummary{}, fmt.Errorf("eval: pooled relevance label %d duplicates a source judgment", index+1)
		}
		originBySource[sourceKey] = label.JudgmentOrigin
	}
	labelByResult := make(map[resultKey]int)
	for key, grade := range pooledByResult {
		if _, returned := indexed[key]; returned {
			labelByResult[key] = grade
		}
	}
	return scoreRelevance(records, runID, labelByResult, idealByCase, "pooled")
}

func scoreRelevance(records []Record, runID string, labelByResult map[resultKey]int, idealByCase map[string][]int, idealScope string) (RelevanceSummary, error) {

	report := RelevanceSummary{
		SchemaVersion: 1, RunID: runID, TotalCases: len(records), NDCGIdealScope: idealScope,
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
	intentAllNDCGSum := map[Intent]float64{}
	intentAllMRRSum := map[Intent]float64{}
	intentAllPrecisionAt5Sum := map[Intent]float64{}
	intentAllRelevantAt5 := map[Intent]int{}
	intentAllQueryNDCGSum := map[Intent]float64{}
	intentAllQueryMRRSum := map[Intent]float64{}
	intentAllQueryPrecisionAt5Sum := map[Intent]float64{}
	intentAllQueryRelevantAt5 := map[Intent]int{}
	var relevanceSum int
	var allRelevantAt5 int
	var allQueryRelevantAt5 int
	var precisionAt5Sum, ndcgSum, mrrSum float64
	var allPrecisionAt5Sum, allNDCGSum, allMRRSum float64
	var allQueryPrecisionAt5Sum, allQueryNDCGSum, allQueryMRRSum float64

	for _, record := range records {
		intentSummary := report.ByIntent[record.Intent]
		intentSummary.TotalCases++
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
		caseIdeal := grades
		if idealScope == "pooled" {
			caseIdeal = idealByCase[record.CaseID]
		}
		if fullyLabeled {
			precision := precisionAt(grades, 5)
			ndcg := ndcgAtWithIdeal(grades, caseIdeal, 10)
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
		allEligibleScoreable := eligible && (len(record.Results) == 0 || fullyLabeled)
		if allEligibleScoreable {
			relevantAt5 := relevantAt(grades, 5)
			fixedPrecisionAt5 := float64(relevantAt5) / 5
			ndcg := ndcgAtWithIdeal(grades, caseIdeal, 10)
			mrr := reciprocalRank(grades)
			report.AllEligibleRankingCases++
			allRelevantAt5 += relevantAt5
			allPrecisionAt5Sum += fixedPrecisionAt5
			allNDCGSum += ndcg
			allMRRSum += mrr
			intentSummary.AllEligibleRankingCases++
			intentAllRelevantAt5[record.Intent] += relevantAt5
			intentAllPrecisionAt5Sum[record.Intent] += fixedPrecisionAt5
			intentAllNDCGSum[record.Intent] += ndcg
			intentAllMRRSum[record.Intent] += mrr
		}
		allQueryScoreable := !eligible || len(record.Results) == 0 || fullyLabeled
		if allQueryScoreable {
			allQueryGrades := grades
			if !eligible {
				allQueryGrades = nil
			}
			relevantAt5 := relevantAt(allQueryGrades, 5)
			fixedPrecisionAt5 := float64(relevantAt5) / 5
			ndcg := ndcgAtWithIdeal(allQueryGrades, caseIdeal, 10)
			mrr := reciprocalRank(allQueryGrades)
			report.AllQueryRankingCases++
			allQueryRelevantAt5 += relevantAt5
			allQueryPrecisionAt5Sum += fixedPrecisionAt5
			allQueryNDCGSum += ndcg
			allQueryMRRSum += mrr
			intentSummary.AllQueryRankingCases++
			intentAllQueryRelevantAt5[record.Intent] += relevantAt5
			intentAllQueryPrecisionAt5Sum[record.Intent] += fixedPrecisionAt5
			intentAllQueryNDCGSum[record.Intent] += ndcg
			intentAllQueryMRRSum[record.Intent] += mrr
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
	if report.TotalCases > 0 {
		report.RelevantHitCoverageRate = float64(report.RelevantHitCases) / float64(report.TotalCases)
	}
	if report.RankingSamples > 0 {
		report.MeanPrecisionAt5 = precisionAt5Sum / float64(report.RankingSamples)
		report.MeanNDCGAt10 = ndcgSum / float64(report.RankingSamples)
		report.MeanReciprocalRank = mrrSum / float64(report.RankingSamples)
	}
	if report.EligibleCases > 0 && report.AllEligibleRankingCases == report.EligibleCases {
		report.AllEligibleRankingComplete = true
		relevantAt5 := allRelevantAt5
		yield := float64(relevantAt5) / float64(report.EligibleCases)
		precision := allPrecisionAt5Sum / float64(report.EligibleCases)
		ndcg := allNDCGSum / float64(report.EligibleCases)
		mrr := allMRRSum / float64(report.EligibleCases)
		report.AllEligibleRelevantResultsAt5 = &relevantAt5
		report.AllEligibleRelevantYieldAt5 = &yield
		report.AllEligibleMeanPrecisionAt5 = &precision
		report.AllEligibleMeanNDCGAt10 = &ndcg
		report.AllEligibleMeanReciprocalRank = &mrr
	}
	if report.TotalCases > 0 && report.AllQueryRankingCases == report.TotalCases {
		report.AllQueryRankingComplete = true
		relevantAt5 := allQueryRelevantAt5
		yield := float64(relevantAt5) / float64(report.TotalCases)
		precision := allQueryPrecisionAt5Sum / float64(report.TotalCases)
		ndcg := allQueryNDCGSum / float64(report.TotalCases)
		mrr := allQueryMRRSum / float64(report.TotalCases)
		report.AllQueryRelevantResultsAt5 = &relevantAt5
		report.AllQueryRelevantYieldAt5 = &yield
		report.AllQueryMeanPrecisionAt5 = &precision
		report.AllQueryMeanNDCGAt10 = &ndcg
		report.AllQueryMeanReciprocalRank = &mrr
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
		if summary.TotalCases > 0 {
			summary.RelevantHitCoverageRate = float64(summary.RelevantHitCases) / float64(summary.TotalCases)
		}
		if summary.RankingSamples > 0 {
			summary.MeanPrecisionAt5 = intentPrecisionAt5Sum[intent] / float64(summary.RankingSamples)
			summary.MeanNDCGAt10 = intentNDCGSum[intent] / float64(summary.RankingSamples)
			summary.MeanReciprocalRank = intentMRRSum[intent] / float64(summary.RankingSamples)
		}
		if summary.EligibleCases > 0 && summary.AllEligibleRankingCases == summary.EligibleCases {
			summary.AllEligibleRankingComplete = true
			relevantAt5 := intentAllRelevantAt5[intent]
			yield := float64(relevantAt5) / float64(summary.EligibleCases)
			precision := intentAllPrecisionAt5Sum[intent] / float64(summary.EligibleCases)
			ndcg := intentAllNDCGSum[intent] / float64(summary.EligibleCases)
			mrr := intentAllMRRSum[intent] / float64(summary.EligibleCases)
			summary.AllEligibleRelevantResultsAt5 = &relevantAt5
			summary.AllEligibleRelevantYieldAt5 = &yield
			summary.AllEligibleMeanPrecisionAt5 = &precision
			summary.AllEligibleMeanNDCGAt10 = &ndcg
			summary.AllEligibleMeanReciprocalRank = &mrr
		}
		if summary.TotalCases > 0 && summary.AllQueryRankingCases == summary.TotalCases {
			summary.AllQueryRankingComplete = true
			relevantAt5 := intentAllQueryRelevantAt5[intent]
			yield := float64(relevantAt5) / float64(summary.TotalCases)
			precision := intentAllQueryPrecisionAt5Sum[intent] / float64(summary.TotalCases)
			ndcg := intentAllQueryNDCGSum[intent] / float64(summary.TotalCases)
			mrr := intentAllQueryMRRSum[intent] / float64(summary.TotalCases)
			summary.AllQueryRelevantResultsAt5 = &relevantAt5
			summary.AllQueryRelevantYieldAt5 = &yield
			summary.AllQueryMeanPrecisionAt5 = &precision
			summary.AllQueryMeanNDCGAt10 = &ndcg
			summary.AllQueryMeanReciprocalRank = &mrr
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
	return float64(relevantAt(grades, limit)) / float64(limit)
}

func relevantAt(grades []int, limit int) int {
	if limit <= 0 {
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
	return relevant
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
			results[key] = indexedResult{result: result, intent: record.Intent, query: record.Query, caseSHA256: record.CaseSHA256, rank: resultIndex + 1}
		}
	}
	return runID, results, nil
}

func ndcgAt(grades []int, limit int) float64 {
	return ndcgAtWithIdeal(grades, grades, limit)
}

func ndcgAtWithIdeal(grades, idealGrades []int, limit int) float64 {
	if limit <= 0 {
		return 0
	}
	resultLimit := limit
	if len(grades) < resultLimit {
		resultLimit = len(grades)
	}
	dcg := discountedGain(grades[:resultLimit])
	ideal := append([]int(nil), idealGrades...)
	sort.Sort(sort.Reverse(sort.IntSlice(ideal)))
	idealLimit := limit
	if len(ideal) < idealLimit {
		idealLimit = len(ideal)
	}
	idcg := discountedGain(ideal[:idealLimit])
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
