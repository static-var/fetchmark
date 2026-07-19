package eval

import (
	"os"
	"strings"
	"testing"
)

func TestLoadSuiteValidatesAndPreservesCases(t *testing.T) {
	raw := strings.NewReader(`
{"id":"general-001","intent":"general","query":"how do tidal bores form","tags":["science"],"max_results":10,"search_depth":"basic"}
{"id":"developer-001","intent":"developer","query":"Go context cancellation official documentation","tags":["go","docs"],"max_results":10,"search_depth":"advanced","expected_domains":["go.dev"]}
`)

	suite, err := LoadSuite(raw)
	if err != nil {
		t.Fatalf("LoadSuite: %v", err)
	}
	if len(suite.Cases) != 2 || suite.Cases[1].ID != "developer-001" {
		t.Fatalf("suite = %+v", suite)
	}
	if got := suite.IntentCounts()[IntentDeveloper]; got != 1 {
		t.Fatalf("developer count = %d, want 1", got)
	}
}

func TestLoadSuiteRejectsDuplicateAndInvalidCases(t *testing.T) {
	tests := map[string]string{
		"duplicate id": `{"id":"general-001","intent":"general","query":"a","max_results":10,"search_depth":"basic"}
{"id":"general-001","intent":"general","query":"b","max_results":10,"search_depth":"basic"}`,
		"unknown intent": `{"id":"other-001","intent":"other","query":"a","max_results":10,"search_depth":"basic"}`,
		"empty query":    `{"id":"general-001","intent":"general","query":" ","max_results":10,"search_depth":"basic"}`,
		"bad depth":      `{"id":"general-001","intent":"general","query":"a","max_results":10,"search_depth":"deep"}`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadSuite(strings.NewReader(raw)); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestRepositorySuiteHasBalancedRepresentativeBaseline(t *testing.T) {
	f, err := os.Open("../../eval/queries.jsonl")
	if err != nil {
		t.Fatalf("open repository suite: %v", err)
	}
	defer f.Close()

	suite, err := LoadSuite(f)
	if err != nil {
		t.Fatalf("LoadSuite: %v", err)
	}
	if len(suite.Cases) != 120 {
		t.Fatalf("cases = %d, want 120", len(suite.Cases))
	}
	for _, intent := range AllIntents() {
		if got := suite.IntentCounts()[intent]; got != 20 {
			t.Fatalf("%s cases = %d, want 20", intent, got)
		}
	}
}
