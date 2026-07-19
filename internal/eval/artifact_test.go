package eval

import (
	"bytes"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/core/search"
)

func TestLoadRecordsRoundTripsOneStrictRun(t *testing.T) {
	records := labeledRecordsFixture()
	var artifact bytes.Buffer
	if err := WriteRecords(&artifact, records); err != nil {
		t.Fatalf("WriteRecords: %v", err)
	}
	loaded, err := LoadRecords(&artifact)
	if err != nil {
		t.Fatalf("LoadRecords: %v", err)
	}
	if len(loaded) != len(records) || loaded[0].RunID != "run-a" || loaded[1].CaseID != "developer-001" {
		t.Fatalf("loaded records = %#v", loaded)
	}
}

func TestLoadRecordsPreservesBuildSHA256(t *testing.T) {
	const buildSHA256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	records := labeledRecordsFixture()
	for index := range records {
		records[index].BuildSHA256 = buildSHA256
	}
	var artifact bytes.Buffer
	if err := WriteRecords(&artifact, records); err != nil {
		t.Fatalf("WriteRecords: %v", err)
	}
	loaded, err := LoadRecords(&artifact)
	if err != nil {
		t.Fatalf("LoadRecords: %v", err)
	}
	if got := Summarize(loaded).BuildSHA256; got != buildSHA256 {
		t.Fatalf("summary build_sha256 = %q, want %q", got, buildSHA256)
	}
}

func TestLoadRecordsPreservesConfigurationSHA256(t *testing.T) {
	const digest = "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
	records := labeledRecordsFixture()
	for index := range records {
		records[index].ConfigurationSHA256 = digest
	}
	var artifact bytes.Buffer
	if err := WriteRecords(&artifact, records); err != nil {
		t.Fatalf("WriteRecords: %v", err)
	}
	loaded, err := LoadRecords(&artifact)
	if err != nil {
		t.Fatalf("LoadRecords: %v", err)
	}
	if got := Summarize(loaded).ConfigurationSHA256; got != digest {
		t.Fatalf("summary configuration_sha256 = %q", got)
	}
}

func TestLoadRecordsRejectsInvalidOrMixedConfigurationSHA256(t *testing.T) {
	tests := map[string][]string{
		"invalid": {"not-a-digest", "not-a-digest"},
		"mixed":   {strings.Repeat("a", 64), strings.Repeat("b", 64)},
		"partial": {strings.Repeat("a", 64), ""},
	}
	for name, digests := range tests {
		t.Run(name, func(t *testing.T) {
			records := labeledRecordsFixture()
			for index := range records {
				records[index].ConfigurationSHA256 = digests[index]
			}
			var artifact bytes.Buffer
			if err := WriteRecords(&artifact, records); err != nil {
				t.Fatalf("WriteRecords: %v", err)
			}
			if _, err := LoadRecords(&artifact); err == nil {
				t.Fatal("LoadRecords accepted invalid configuration identity")
			}
		})
	}
}

func TestLoadRecordsRejectsInvalidOrMixedBuildSHA256(t *testing.T) {
	tests := map[string][]string{
		"invalid": {"not-a-digest", "not-a-digest"},
		"mixed":   {strings.Repeat("a", 64), strings.Repeat("b", 64)},
		"partial": {strings.Repeat("a", 64), ""},
	}
	for name, builds := range tests {
		t.Run(name, func(t *testing.T) {
			records := labeledRecordsFixture()
			for index := range records {
				records[index].BuildSHA256 = builds[index]
			}
			var artifact bytes.Buffer
			if err := WriteRecords(&artifact, records); err != nil {
				t.Fatalf("WriteRecords: %v", err)
			}
			if _, err := LoadRecords(&artifact); err == nil {
				t.Fatal("LoadRecords accepted invalid build identity")
			}
		})
	}
}

func TestLoadRecordsAllowsBuildlessRowsWithoutHTTPResponse(t *testing.T) {
	raw := `{"schema_version":1,"run_id":"run-a","build_sha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","case_id":"general-001","intent":"general","query":"q","search_depth":"basic","started_at":"2026-07-18T00:00:00Z","attempted":true,"http_status":200,"non_empty":false,"result_count":0,"unique_domains":0}
{"schema_version":1,"run_id":"run-a","case_id":"developer-001","intent":"developer","query":"q","search_depth":"basic","started_at":"2026-07-18T00:00:00Z","attempted":true,"error":"network_error","non_empty":false,"result_count":0,"unique_domains":0}`
	records, err := LoadRecords(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("LoadRecords: %v", err)
	}
	if got := Summarize(records).BuildSHA256; got != "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" {
		t.Fatalf("summary build_sha256 = %q", got)
	}
}

func TestLoadRecordsAcceptsLegacySourceVariantProvenanceWithoutInventingProvider(t *testing.T) {
	raw := `{"schema_version":1,"run_id":"run-a","case_id":"general-001","intent":"general","query":"q","search_depth":"basic","started_at":"2026-07-18T00:00:00Z","attempted":true,"http_status":200,"non_empty":true,"result_count":1,"unique_domains":1,"provenance_results":1,"results":[{"url":"https://one.example/","domain":"one.example","extracted":false,"sources":[{"source":"searxng-open","variant":"original"}]}]}`
	records, err := LoadRecords(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("LoadRecords legacy artifact: %v", err)
	}
	summary := Summarize(records)
	if len(summary.SourceContributions) != 0 || summary.LaneContributions["unavailable/searxng-open:original"] != 1 {
		t.Fatalf("legacy summary invented provider identity: %+v", summary)
	}
}

func TestLoadRecordsRejectsUnknownMixedAndInconsistentRows(t *testing.T) {
	tests := map[string]string{
		"unknown field": `{"schema_version":1,"run_id":"run-a","case_id":"general-001","intent":"general","query":"q","search_depth":"basic","started_at":"2026-07-18T00:00:00Z","result_count":0,"non_empty":false,"surprise":true}`,
		"mixed run": `{"schema_version":1,"run_id":"run-a","case_id":"general-001","intent":"general","query":"q","search_depth":"basic","started_at":"2026-07-18T00:00:00Z","result_count":0,"non_empty":false}
{"schema_version":1,"run_id":"run-b","case_id":"developer-001","intent":"developer","query":"q","search_depth":"basic","started_at":"2026-07-18T00:00:00Z","result_count":0,"non_empty":false}`,
		"inconsistent count":          `{"schema_version":1,"run_id":"run-a","case_id":"general-001","intent":"general","query":"q","search_depth":"basic","started_at":"2026-07-18T00:00:00Z","result_count":1,"non_empty":true}`,
		"inconsistent derived counts": `{"schema_version":1,"run_id":"run-a","case_id":"general-001","intent":"general","query":"q","search_depth":"basic","started_at":"2026-07-18T00:00:00Z","attempted":true,"http_status":200,"result_count":1,"non_empty":true,"unique_domains":0,"results":[{"url":"https://one.example/","domain":"one.example","extracted":true}]}`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadRecords(strings.NewReader(raw)); err == nil {
				t.Fatal("expected strict artifact validation error")
			}
		})
	}
}

func TestLoadRecordsRejectsEmptyArtifact(t *testing.T) {
	if _, err := LoadRecords(strings.NewReader("\n")); err == nil {
		t.Fatal("expected empty artifact error")
	}
}

func TestLoadRecordsPreservesStrictDiscoveryEvidenceOnHTTPFailure(t *testing.T) {
	raw := `{"schema_version":1,"run_id":"run-a","case_id":"general-001","intent":"general","query":"q","tags":["general"],"search_depth":"basic","started_at":"2026-07-18T00:00:00Z","attempted":true,"duration_ms":8000,"http_status":502,"error":"http_502","non_empty":false,"result_count":0,"unique_domains":0,"extraction_successes":0,"published_results":0,"expected_domain_hits":0,"provenance_results":0,"provenance_malformed_results":0,"multi_source_results":0,"discovery":{"status":"failed","lanes":[{"provider":"mwmbl","lane":"mwmbl-general","variant":"original","status":"failed","candidate_count":0,"duration_ms":8000,"diagnostics":[{"reason":"timeout","retryable":true}]}]}}`
	records, err := LoadRecords(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("LoadRecords: %v", err)
	}
	if len(records) != 1 || records[0].Discovery == nil || records[0].Discovery.Lanes[0].Diagnostics[0].Reason != "timeout" {
		t.Fatalf("records = %+v", records)
	}
	summary := Summarize(records)
	if summary.DiscoveryReports != 1 || summary.DiscoveryStatusCounts["failed"] != 1 || summary.DiscoveryDiagnosticCounts["mwmbl/timeout"] != 1 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestLoadRecordsPreservesPartialDiscoveryEvidenceOnHTTPFailure(t *testing.T) {
	raw := `{"schema_version":1,"run_id":"run-a","case_id":"general-001","intent":"general","query":"q","tags":["general"],"search_depth":"basic","started_at":"2026-07-18T00:00:00Z","attempted":true,"duration_ms":100,"http_status":502,"error":"http_502","non_empty":false,"result_count":0,"unique_domains":0,"extraction_successes":0,"published_results":0,"expected_domain_hits":0,"provenance_results":0,"provenance_malformed_results":0,"multi_source_results":0,"discovery":{"status":"partial","lanes":[{"provider":"wiby","lane":"wiby-general","variant":"original","status":"healthy","candidate_count":1,"duration_ms":1},{"provider":"mwmbl","lane":"mwmbl-general","variant":"original","status":"failed","candidate_count":0,"duration_ms":2,"diagnostics":[{"reason":"canceled"}]}]}}`
	records, err := LoadRecords(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("LoadRecords: %v", err)
	}
	if records[0].Discovery == nil || records[0].Discovery.Status != search.BatchPartial || len(records[0].Discovery.Lanes) != 2 {
		t.Fatalf("records = %+v", records)
	}
}

func TestLoadRecordsRejectsUnsafeOrInconsistentDiscoveryEvidence(t *testing.T) {
	base := `{"schema_version":1,"run_id":"run-a","case_id":"general-001","intent":"general","query":"q","tags":["general"],"search_depth":"basic","started_at":"2026-07-18T00:00:00Z","attempted":true,"http_status":200,"non_empty":false,"result_count":0,"unique_domains":0,"extraction_successes":0,"published_results":0,"expected_domain_hits":0,"provenance_results":0,"provenance_malformed_results":0,"multi_source_results":0,"discovery":DISCOVERY}`
	tests := map[string]string{
		"unknown status":      `{"status":"mystery"}`,
		"internal instance":   `{"status":"failed","lanes":[{"provider":"wiby","lane":"wiby-general","variant":"original","status":"failed","candidate_count":0,"duration_ms":1,"instance":"http://private.internal"}]}`,
		"duplicate lane":      `{"status":"partial","lanes":[{"provider":"wiby","lane":"wiby-general","variant":"original","status":"healthy","candidate_count":1,"duration_ms":1},{"provider":"wiby","lane":"wiby-general","variant":"original","status":"partial","candidate_count":1,"duration_ms":2}]}`,
		"failed candidates":   `{"status":"failed","lanes":[{"provider":"wiby","lane":"wiby-general","variant":"original","status":"failed","candidate_count":1,"duration_ms":1}]}`,
		"raw diagnostic":      `{"status":"partial","lanes":[{"provider":"wiby","lane":"wiby-general","variant":"original","status":"partial","candidate_count":1,"duration_ms":1,"diagnostics":[{"reason":"timeout!"}]}]}`,
		"healthy only failed": `{"status":"healthy","lanes":[{"provider":"wiby","lane":"wiby-general","variant":"original","status":"failed","candidate_count":0,"duration_ms":1}]}`,
		"failed with usable":  `{"status":"failed","lanes":[{"provider":"wiby","lane":"wiby-general","variant":"original","status":"healthy","candidate_count":1,"duration_ms":1}]}`,
		"healthy empty lane":  `{"status":"authoritative_empty","lanes":[{"provider":"wiby","lane":"wiby-general","variant":"original","status":"healthy","candidate_count":0,"duration_ms":1}]}`,
		"partial empty lane":  `{"status":"degraded_empty","lanes":[{"provider":"wiby","lane":"wiby-general","variant":"original","status":"partial","candidate_count":0,"duration_ms":1}]}`,
		"http success failed": `{"status":"failed","lanes":[{"provider":"wiby","lane":"wiby-general","variant":"original","status":"failed","candidate_count":0,"duration_ms":1}]}`,
	}
	for name, discovery := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadRecords(strings.NewReader(strings.Replace(base, "DISCOVERY", discovery, 1))); err == nil {
				t.Fatal("LoadRecords accepted invalid discovery evidence")
			}
		})
	}
}
