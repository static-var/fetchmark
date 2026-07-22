package eval

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

func TestRelevanceLabelsAreRunBoundAndScoreRankingAndSources(t *testing.T) {
	records := labeledRecordsFixture()
	labels, err := LoadRelevanceLabels(strings.NewReader(`
{"schema_version":1,"run_id":"run-a","case_id":"general-001","url":"https://one.example/","relevance":3}
{"schema_version":1,"run_id":"run-a","case_id":"general-001","url":"https://two.example/","relevance":0}
{"schema_version":1,"run_id":"run-a","case_id":"developer-001","url":"https://three.example/","relevance":2}
`), records)
	if err != nil {
		t.Fatalf("LoadRelevanceLabels: %v", err)
	}
	report, err := ScoreRelevance(records, labels)
	if err != nil {
		t.Fatalf("ScoreRelevance: %v", err)
	}
	if report.RunID != "run-a" || report.TotalResults != 3 || report.LabeledResults != 3 || report.LabelCoverageRate != 1 {
		t.Fatalf("coverage report = %+v", report)
	}
	if report.RelevantResults != 2 || report.HighlyRelevantResults != 1 || report.MeanRelevance != 5.0/3.0 {
		t.Fatalf("relevance report = %+v", report)
	}
	if report.FullyLabeledCases != 2 || report.RankingSamples != 2 || report.MeanNDCGAt10 != 1 || report.MeanReciprocalRank != 1 {
		t.Fatalf("ranking report = %+v", report)
	}
	if report.RelevantHitCases != 2 || report.RelevantHitCoverageRate != 1 || report.MeanPrecisionAt5 != 0.75 {
		t.Fatalf("precision-first report = %+v", report)
	}
	if report.SourceRelevantResults["searxng"] != 1 || report.SourceRelevantResults["crossref"] != 1 || report.SourceRelevantResults["wikipedia"] != 1 {
		t.Fatalf("source relevant results = %#v", report.SourceRelevantResults)
	}
	if report.SourceMeanRelevance["searxng"] != 1.5 || report.SourceMeanRelevance["crossref"] != 3 || report.SourceMeanRelevance["wikipedia"] != 2 {
		t.Fatalf("source mean relevance = %#v", report.SourceMeanRelevance)
	}
}

func TestRelevanceLabelsRejectUnknownDuplicateAndMissingGrades(t *testing.T) {
	records := labeledRecordsFixture()
	tests := map[string]string{
		"unknown URL":    `{"schema_version":1,"run_id":"run-a","case_id":"general-001","url":"https://unknown.example/","relevance":2}`,
		"wrong run":      `{"schema_version":1,"run_id":"other","case_id":"general-001","url":"https://one.example/","relevance":2}`,
		"missing grade":  `{"schema_version":1,"run_id":"run-a","case_id":"general-001","url":"https://one.example/"}`,
		"grade too high": `{"schema_version":1,"run_id":"run-a","case_id":"general-001","url":"https://one.example/","relevance":4}`,
		"duplicate": `{"schema_version":1,"run_id":"run-a","case_id":"general-001","url":"https://one.example/","relevance":2}
{"schema_version":1,"run_id":"run-a","case_id":"general-001","url":"https://one.example/","relevance":3}`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadRelevanceLabels(strings.NewReader(raw), records); err == nil {
				t.Fatal("expected label validation error")
			}
		})
	}
}

func TestPartialLabelsReportCoverageWithoutInventingRankingGrades(t *testing.T) {
	records := labeledRecordsFixture()
	labels, err := LoadRelevanceLabels(strings.NewReader(
		`{"schema_version":1,"run_id":"run-a","case_id":"general-001","url":"https://one.example/","relevance":3}`,
	), records)
	if err != nil {
		t.Fatal(err)
	}
	report, err := ScoreRelevance(records, labels)
	if err != nil {
		t.Fatal(err)
	}
	if report.LabeledResults != 1 || report.LabelCoverageRate != 1.0/3.0 || report.FullyLabeledCases != 0 || report.RankingSamples != 0 {
		t.Fatalf("partial report = %+v", report)
	}
	if report.RelevantHitCases != 1 || report.RelevantHitCoverageRate != 0.5 || report.MeanPrecisionAt5 != 0 {
		t.Fatalf("partial precision-first report = %+v", report)
	}
	if report.AllEligibleRankingComplete || report.AllEligibleRankingCases != 0 ||
		report.AllEligibleRelevantResultsAt5 != nil || report.AllEligibleRelevantYieldAt5 != nil ||
		report.AllEligibleMeanPrecisionAt5 != nil || report.AllEligibleMeanNDCGAt10 != nil ||
		report.AllEligibleMeanReciprocalRank != nil {
		t.Fatalf("partial labels produced all-eligible ranking metrics = %+v", report)
	}
	if report.AllQueryRankingComplete || report.AllQueryRankingCases != 0 ||
		report.AllQueryRelevantResultsAt5 != nil || report.AllQueryRelevantYieldAt5 != nil ||
		report.AllQueryMeanPrecisionAt5 != nil || report.AllQueryMeanNDCGAt10 != nil ||
		report.AllQueryMeanReciprocalRank != nil {
		t.Fatalf("partial labels produced all-query ranking metrics = %+v", report)
	}
}

func TestAllEligibleMetricsCountEmptyCasesAndUseFiveResultPrecisionDenominator(t *testing.T) {
	records := append(labeledRecordsFixture(), Record{
		SchemaVersion: 1, RunID: "run-a", CaseID: "fresh-001", Intent: IntentFresh,
		Query: "fresh query", SearchDepth: "advanced", Attempted: true, HTTPStatus: 200,
		NonEmpty: false, ResultCount: 0,
	})
	labels, err := LoadRelevanceLabels(strings.NewReader(`
{"schema_version":1,"run_id":"run-a","case_id":"general-001","url":"https://one.example/","relevance":3}
{"schema_version":1,"run_id":"run-a","case_id":"general-001","url":"https://two.example/","relevance":0}
{"schema_version":1,"run_id":"run-a","case_id":"developer-001","url":"https://three.example/","relevance":2}
`), records)
	if err != nil {
		t.Fatal(err)
	}
	report, err := ScoreRelevance(records, labels)
	if err != nil {
		t.Fatal(err)
	}
	if !report.AllEligibleRankingComplete || report.AllEligibleRankingCases != 3 {
		t.Fatalf("all-eligible completeness = %+v", report)
	}
	if report.AllEligibleRelevantResultsAt5 == nil || *report.AllEligibleRelevantResultsAt5 != 2 || report.AllEligibleRelevantYieldAt5 == nil ||
		!closeEnough(*report.AllEligibleRelevantYieldAt5, 2.0/3.0) {
		t.Fatalf("all-eligible relevant yield = %+v", report)
	}
	if report.AllEligibleMeanPrecisionAt5 == nil || !closeEnough(*report.AllEligibleMeanPrecisionAt5, 2.0/15.0) {
		t.Fatalf("all-eligible fixed-denominator precision = %+v", report)
	}
	if report.AllEligibleMeanNDCGAt10 == nil || !closeEnough(*report.AllEligibleMeanNDCGAt10, 2.0/3.0) ||
		report.AllEligibleMeanReciprocalRank == nil || !closeEnough(*report.AllEligibleMeanReciprocalRank, 2.0/3.0) {
		t.Fatalf("all-eligible ranking = %+v", report)
	}
	fresh := report.ByIntent[IntentFresh]
	if !fresh.AllEligibleRankingComplete || fresh.AllEligibleRankingCases != 1 ||
		fresh.AllEligibleMeanPrecisionAt5 == nil || *fresh.AllEligibleMeanPrecisionAt5 != 0 {
		t.Fatalf("empty fresh intent was not scored as an abstention: %+v", fresh)
	}
}

func TestPooledQrelsUseCrossRunIdealRankingAndAcceptConsistentDuplicates(t *testing.T) {
	records := labeledRecordsFixture()[:1]
	records[0].Results = records[0].Results[:1]
	records[0].ResultCount = 1
	records[0].UniqueDomains = 1
	qrels, err := LoadPooledRelevanceLabels(strings.NewReader(`
{"schema_version":1,"run_id":"old-run","judgment_origin":"assistant_provisional","case_id":"general-001","intent":"general","query":"general query","url":"https://one.example/","relevance":2}
{"schema_version":1,"run_id":"other-run","judgment_origin":"independent_human","case_id":"general-001","intent":"general","query":"general query","url":"https://best.example/","relevance":3}
{"schema_version":1,"run_id":"third-run","judgment_origin":"assistant_provisional","case_id":"general-001","intent":"general","query":"general query","url":"https://best.example/","relevance":3}
`))
	if err != nil {
		t.Fatalf("LoadPooledRelevanceLabels: %v", err)
	}
	if len(qrels) != 3 || qrels[0].SourceRunID != "old-run" || qrels[0].JudgmentOrigin != JudgmentOriginAssistantProvisional {
		t.Fatalf("pooled qrels provenance = %#v", qrels)
	}
	report, err := ScoreRelevancePooled(records, qrels)
	if err != nil {
		t.Fatalf("ScoreRelevancePooled: %v", err)
	}
	wantNDCG := discountedGain([]int{2}) / discountedGain([]int{3, 2})
	if report.NDCGIdealScope != "pooled" || report.MeanNDCGAt10 != wantNDCG {
		t.Fatalf("pooled ranking report = %+v", report)
	}
	if report.AllEligibleMeanNDCGAt10 == nil || *report.AllEligibleMeanNDCGAt10 != wantNDCG {
		t.Fatalf("all-eligible pooled NDCG = %+v", report)
	}

	_, err = LoadPooledRelevanceLabels(strings.NewReader(`
{"schema_version":1,"run_id":"run-one","judgment_origin":"independent_human","case_id":"general-001","intent":"general","query":"general query","url":"https://one.example/","relevance":2}
{"schema_version":1,"run_id":"run-two","judgment_origin":"independent_human","case_id":"general-001","intent":"general","query":"general query","url":"https://one.example/","relevance":3}
`))
	if err == nil {
		t.Fatal("conflicting duplicate pooled judgments were accepted")
	}
}

func TestPooledQrelsRequireExactCaseAndJudgmentProvenance(t *testing.T) {
	valid := `{"schema_version":1,"run_id":"run-a","judgment_origin":"independent_human","case_id":"general-001","intent":"general","query":"general query","url":"https://one.example/","relevance":2}`
	tests := map[string]string{
		"missing source run": strings.Replace(valid, `"run_id":"run-a",`, "", 1),
		"missing origin":     strings.Replace(valid, `"judgment_origin":"independent_human",`, "", 1),
		"unknown origin":     strings.Replace(valid, "independent_human", "model_generated", 1),
		"missing intent":     strings.Replace(valid, `"intent":"general",`, "", 1),
		"missing query":      strings.Replace(valid, `"query":"general query",`, "", 1),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadPooledRelevanceLabels(strings.NewReader(raw)); err == nil {
				t.Fatal("invalid pooled qrels metadata was accepted")
			}
		})
	}
	qrels, err := LoadPooledRelevanceLabels(strings.NewReader(strings.Replace(valid, "general query", "different query", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ScoreRelevancePooled(labeledRecordsFixture(), qrels); err == nil {
		t.Fatal("pooled qrels with a mismatched fixed-suite query were accepted")
	}
}

func TestPooledQrelsCountFailuresAndAbstentionsInAllQueryDenominator(t *testing.T) {
	records := []Record{
		{
			SchemaVersion: 1, RunID: "run-a", CaseID: "general-001", Intent: IntentGeneral,
			Query: "general query", SearchDepth: "advanced", Attempted: true, HTTPStatus: 200,
			NonEmpty: true, ResultCount: 1, UniqueDomains: 1,
			Results: []ResultObservation{{URL: "https://one.example/", Domain: "one.example", Title: "One"}},
		},
		{SchemaVersion: 1, RunID: "run-a", CaseID: "fresh-001", Intent: IntentFresh, Query: "fresh query", SearchDepth: "advanced", Attempted: true, HTTPStatus: 200},
		{SchemaVersion: 1, RunID: "run-a", CaseID: "developer-001", Intent: IntentDeveloper, Query: "developer query", SearchDepth: "advanced", Attempted: true, Error: "timeout"},
		{SchemaVersion: 1, RunID: "run-a", CaseID: "research-001", Intent: IntentResearch, Query: "research query", SearchDepth: "advanced", Attempted: false, Error: "cancelled"},
	}
	qrels, err := LoadPooledRelevanceLabels(strings.NewReader(
		`{"schema_version":1,"run_id":"source-run","judgment_origin":"assistant_provisional","case_id":"general-001","intent":"general","query":"general query","url":"https://one.example/","relevance":3}`,
	))
	if err != nil {
		t.Fatal(err)
	}
	report, err := ScoreRelevancePooled(records, qrels)
	if err != nil {
		t.Fatal(err)
	}
	if report.RelevantHitCases != 1 || report.RelevantHitCoverageRate != 0.25 {
		t.Fatalf("relevant-hit gate excluded failures or abstentions: %+v", report)
	}
	if !report.AllQueryRankingComplete || report.AllQueryRankingCases != 4 ||
		report.AllQueryMeanPrecisionAt5 == nil || *report.AllQueryMeanPrecisionAt5 != 0.05 ||
		report.AllQueryMeanNDCGAt10 == nil || *report.AllQueryMeanNDCGAt10 != 0.25 ||
		report.AllQueryMeanReciprocalRank == nil || *report.AllQueryMeanReciprocalRank != 0.25 {
		t.Fatalf("all-query abstention metrics = %+v", report)
	}
}

func closeEnough(left, right float64) bool {
	return math.Abs(left-right) < 1e-12
}

func TestWriteLabelTemplateIsOrderedAndIntentionallyIncomplete(t *testing.T) {
	var output bytes.Buffer
	if err := WriteLabelTemplate(&output, labeledRecordsFixture()); err != nil {
		t.Fatalf("WriteLabelTemplate: %v", err)
	}
	want := "\"case_id\":\"general-001\""
	if !strings.Contains(output.String(), want) || !strings.Contains(output.String(), `"rank":1`) || !strings.Contains(output.String(), `"relevance":null`) {
		t.Fatalf("template = %s", output.String())
	}
	if strings.Index(output.String(), "https://one.example/") > strings.Index(output.String(), "https://three.example/") {
		t.Fatalf("template order changed: %s", output.String())
	}
	if _, err := LoadRelevanceLabels(strings.NewReader(output.String()), labeledRecordsFixture()); err == nil {
		t.Fatal("an unfilled template was accepted as completed human judgment")
	}
	completed := strings.ReplaceAll(output.String(), `"relevance":null`, `"relevance":2`)
	labels, err := LoadRelevanceLabels(strings.NewReader(completed), labeledRecordsFixture())
	if err != nil {
		t.Fatalf("completed generated template was not accepted: %v", err)
	}
	if len(labels) != 3 {
		t.Fatalf("completed template labels = %#v", labels)
	}
	altered := strings.Replace(completed, `"rank":1`, `"rank":2`, 1)
	if _, err := LoadRelevanceLabels(strings.NewReader(altered), labeledRecordsFixture()); err == nil {
		t.Fatal("altered generated template metadata was accepted")
	}
}

func TestLargeAcceptedRecordTemplateRoundTripsThroughLabelLoader(t *testing.T) {
	records := labeledRecordsFixture()[:1]
	records[0].Results = records[0].Results[:1]
	records[0].Results[0].Title = strings.Repeat("x", (1<<20)+1)
	records[0].ResultCount = 1
	records[0].UniqueDomains = 1
	records[0].ProvenanceResults = 1
	records[0].MultiSourceResults = 1

	var artifact bytes.Buffer
	if err := WriteRecords(&artifact, records); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadRecords(&artifact)
	if err != nil {
		t.Fatalf("LoadRecords: %v", err)
	}
	var template bytes.Buffer
	if err := WriteLabelTemplate(&template, loaded); err != nil {
		t.Fatalf("WriteLabelTemplate: %v", err)
	}
	completed := strings.ReplaceAll(template.String(), `"relevance":null`, `"relevance":2`)
	if _, err := LoadRelevanceLabels(strings.NewReader(completed), loaded); err != nil {
		t.Fatalf("LoadRelevanceLabels: %v", err)
	}
}

func TestLabelTemplateBudgetRejectsAggregateExpansion(t *testing.T) {
	if err := validateLabelTemplateBudget(labeledRecordsFixture(), 1<<20, 1); err == nil {
		t.Fatal("expected aggregate template budget rejection")
	}
}

func labeledRecordsFixture() []Record {
	return []Record{
		{
			SchemaVersion: 1, RunID: "run-a", CaseID: "general-001", Intent: IntentGeneral,
			Query: "general query", SearchDepth: "advanced", Attempted: true, HTTPStatus: 200,
			NonEmpty: true, ResultCount: 2, UniqueDomains: 2, ProvenanceResults: 2, MultiSourceResults: 1,
			Results: []ResultObservation{
				{URL: "https://one.example/", Domain: "one.example", Title: "One", Sources: []SourceObservation{{Provider: "searxng", Lane: "searxng-default", Variant: "original"}, {Provider: "crossref", Lane: "crossref-research", Variant: "exact"}}},
				{URL: "https://two.example/", Domain: "two.example", Title: "Two", Sources: []SourceObservation{{Provider: "searxng", Lane: "searxng-default", Variant: "original"}}},
			},
		},
		{
			SchemaVersion: 1, RunID: "run-a", CaseID: "developer-001", Intent: IntentDeveloper,
			Query: "developer query", SearchDepth: "advanced", Attempted: true, HTTPStatus: 200,
			NonEmpty: true, ResultCount: 1, UniqueDomains: 1, ProvenanceResults: 1,
			Results: []ResultObservation{
				{URL: "https://three.example/", Domain: "three.example", Title: "Three", Sources: []SourceObservation{{Provider: "wikipedia", Lane: "wikipedia-knowledge", Variant: "original"}}},
			},
		},
	}
}
