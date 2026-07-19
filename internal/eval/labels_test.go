package eval

import (
	"bytes"
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
