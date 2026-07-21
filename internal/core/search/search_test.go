package search

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/model"
)

func TestHitJSONNeverIncludesProviderDocument(t *testing.T) {
	raw, err := json.Marshal(Hit{URL: "https://example.com", ProviderDocument: &model.ProviderDocument{HTML: []byte("secret provider html")}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "secret provider html") || strings.Contains(string(raw), "ProviderDocument") {
		t.Fatalf("provider document leaked through hit JSON: %s", raw)
	}
}

func TestValidateDiscoveryReportAggregateMatrix(t *testing.T) {
	healthy := DiscoveryLaneReport{Provider: "wiby", Lane: "wiby-general", Variant: "original", Status: BatchHealthy, CandidateCount: 1}
	partial := DiscoveryLaneReport{Provider: "wiby", Lane: "wiby-general", Variant: "original", Status: BatchPartial, CandidateCount: 1}
	authoritative := DiscoveryLaneReport{Provider: "mwmbl", Lane: "mwmbl-general", Variant: "original", Status: BatchAuthoritativeEmpty}
	degraded := DiscoveryLaneReport{Provider: "mwmbl", Lane: "mwmbl-general", Variant: "original", Status: BatchDegradedEmpty}
	failed := DiscoveryLaneReport{Provider: "crossref", Lane: "crossref-research", Variant: "original", Status: BatchFailed}

	tests := []DiscoveryReport{
		{Status: BatchHealthy, Lanes: []DiscoveryLaneReport{healthy, authoritative}},
		{Status: BatchPartial, Lanes: []DiscoveryLaneReport{healthy, failed}},
		{Status: BatchPartial, Lanes: []DiscoveryLaneReport{partial}},
		{Status: BatchDegradedEmpty, Lanes: []DiscoveryLaneReport{degraded}},
		{Status: BatchDegradedEmpty, Lanes: []DiscoveryLaneReport{authoritative, failed}},
		{Status: BatchAuthoritativeEmpty, Lanes: []DiscoveryLaneReport{authoritative}},
		{Status: BatchFailed, Lanes: []DiscoveryLaneReport{failed}},
		{Status: BatchFailed},
	}
	for index, report := range tests {
		if err := ValidateDiscoveryReport(report); err != nil {
			t.Fatalf("case %d: %v", index, err)
		}
	}
}

func TestValidateDiscoveryReportRejectsInvalidBoundsAndSemantics(t *testing.T) {
	valid := DiscoveryLaneReport{Provider: "wiby", Lane: "wiby-general", Variant: "original", Status: BatchHealthy, CandidateCount: 1}
	tests := map[string]DiscoveryReport{
		"aggregate mismatch": {Status: BatchFailed, Lanes: []DiscoveryLaneReport{valid}},
		"healthy empty":      {Status: BatchAuthoritativeEmpty, Lanes: []DiscoveryLaneReport{{Provider: "wiby", Lane: "wiby-general", Variant: "original", Status: BatchHealthy}}},
		"partial empty":      {Status: BatchDegradedEmpty, Lanes: []DiscoveryLaneReport{{Provider: "wiby", Lane: "wiby-general", Variant: "original", Status: BatchPartial}}},
		"raw identity":       {Status: BatchFailed, Lanes: []DiscoveryLaneReport{{Provider: "http://private", Lane: "wiby-general", Variant: "original", Status: BatchFailed}}},
		"candidate overflow": {Status: BatchHealthy, Lanes: []DiscoveryLaneReport{{Provider: "wiby", Lane: "wiby-general", Variant: "original", Status: BatchHealthy, CandidateCount: MaxDiscoveryCandidateCount + 1}}},
		"duration overflow":  {Status: BatchHealthy, Lanes: []DiscoveryLaneReport{{Provider: "wiby", Lane: "wiby-general", Variant: "original", Status: BatchHealthy, CandidateCount: 1, DurationMS: (MaxDiscoveryDuration + time.Millisecond).Milliseconds()}}},
		"retry overflow": {Status: BatchDegradedEmpty, Lanes: []DiscoveryLaneReport{{
			Provider: "wiby", Lane: "wiby-general", Variant: "original", Status: BatchDegradedEmpty,
			Diagnostics: []DiscoveryDiagnostic{{Reason: "timeout", RetryAfterMS: (MaxDiscoveryRetryAfter + time.Millisecond).Milliseconds()}},
		}}},
		"too many diagnostics": {Status: BatchDegradedEmpty, Lanes: []DiscoveryLaneReport{{
			Provider: "wiby", Lane: "wiby-general", Variant: "original", Status: BatchDegradedEmpty,
			Diagnostics: make([]DiscoveryDiagnostic, MaxDiscoveryDiagnosticsPerLane+1),
		}}},
		"oversized fallback": {Status: BatchDegradedEmpty, Lanes: []DiscoveryLaneReport{{
			Provider: "wiby", Lane: "wiby-general", Variant: "original", Status: BatchDegradedEmpty,
			Diagnostics: []DiscoveryDiagnostic{{
				Reason:   "parser_failed",
				Fallback: &ParseFallback{Format: "cleaned_dom", Content: strings.Repeat("x", MaxDiscoveryFallbackBytes+1)},
			}},
		}}},
	}
	tooManyLanes := make([]DiscoveryLaneReport, MaxDiscoveryLanes+1)
	for index := range tooManyLanes {
		tooManyLanes[index] = DiscoveryLaneReport{Provider: "wiby", Lane: "lane-" + string(rune('a'+index%26)), Variant: "original", Status: BatchFailed}
	}
	tests["too many lanes"] = DiscoveryReport{Status: BatchFailed, Lanes: tooManyLanes}

	for name, report := range tests {
		t.Run(name, func(t *testing.T) {
			if err := ValidateDiscoveryReport(report); err == nil {
				t.Fatal("accepted invalid report")
			}
		})
	}
}

func TestValidatedDiscoveryReportDeepCopiesDiagnostics(t *testing.T) {
	report := DiscoveryReport{Status: BatchDegradedEmpty, Lanes: []DiscoveryLaneReport{{
		Provider: "wiby", Lane: "wiby-general", Variant: "original", Status: BatchDegradedEmpty,
		Diagnostics: []DiscoveryDiagnostic{{
			Reason: "timeout", Fallback: &ParseFallback{Format: "cleaned_dom", Content: "<main>fallback</main>"},
		}},
	}}}
	cloned, err := ValidatedDiscoveryReport(report)
	if err != nil {
		t.Fatal(err)
	}
	report.Lanes[0].Diagnostics[0].Reason = "changed"
	report.Lanes[0].Diagnostics[0].Fallback.Content = "changed"
	if cloned.Lanes[0].Diagnostics[0].Reason != "timeout" {
		t.Fatalf("clone mutated: %+v", cloned)
	}
	if cloned.Lanes[0].Diagnostics[0].Fallback.Content != "<main>fallback</main>" {
		t.Fatalf("fallback clone mutated: %+v", cloned)
	}
}
