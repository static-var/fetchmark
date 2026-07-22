package eval

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestCaseSHA256BindsEveryFixedCaseField(t *testing.T) {
	base := Case{
		ID: "developer-001", Intent: IntentDeveloper, Query: "Kotlin coroutine cancellation",
		Tags: []string{"kotlin", "docs"}, MaxResults: 10, SearchDepth: "advanced",
		Engines: []string{"github"}, Categories: []string{"it"}, Language: "en", TimeRange: "month",
		ExpectedDomains: []string{"kotlinlang.org"}, FreshnessSensitive: true,
	}
	baseDigest := CaseSHA256(base)
	mutations := map[string]func(*Case){
		"id":                  func(c *Case) { c.ID = "developer-002" },
		"intent":              func(c *Case) { c.Intent = IntentGeneral },
		"query":               func(c *Case) { c.Query += " guide" },
		"tags":                func(c *Case) { c.Tags = []string{"docs"} },
		"max results":         func(c *Case) { c.MaxResults++ },
		"search depth":        func(c *Case) { c.SearchDepth = "basic" },
		"engines":             func(c *Case) { c.Engines = []string{"gitlab"} },
		"categories":          func(c *Case) { c.Categories = []string{"general"} },
		"language":            func(c *Case) { c.Language = "de" },
		"time range":          func(c *Case) { c.TimeRange = "day" },
		"expected domains":    func(c *Case) { c.ExpectedDomains = []string{"go.dev"} },
		"freshness sensitive": func(c *Case) { c.FreshnessSensitive = false },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := base
			changed.Tags = append([]string(nil), base.Tags...)
			changed.Engines = append([]string(nil), base.Engines...)
			changed.Categories = append([]string(nil), base.Categories...)
			changed.ExpectedDomains = append([]string(nil), base.ExpectedDomains...)
			mutate(&changed)
			if reflect.DeepEqual(changed, base) || CaseSHA256(changed) == baseDigest {
				t.Fatalf("%s did not change complete case identity", name)
			}
		})
	}
}

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
