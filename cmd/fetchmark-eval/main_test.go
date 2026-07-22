package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	feval "github.com/staticvar/fetchmark/internal/eval"
	"github.com/staticvar/fetchmark/internal/evaluationmanifest"
)

func TestRunValidatesSuiteWithoutLiveRequests(t *testing.T) {
	suitePath := writeSuite(t)
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"-suite", suitePath}, &stdout, &stderr, func(string) string { return "" })
	if code != 0 {
		t.Fatalf("code = %d stderr=%s", code, stderr.String())
	}
	var got struct {
		CaseCount int `json:"case_count"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode stdout: %v (%s)", err, stdout.String())
	}
	if got.CaseCount != 1 {
		t.Fatalf("case_count = %d", got.CaseCount)
	}
}

func TestRunSelectsBalancedCasesPerIntent(t *testing.T) {
	suitePath := writeBalancedSuite(t, 2)
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"-suite", suitePath, "-per-intent", "1"}, &stdout, &stderr, func(string) string { return "" })
	if code != 0 {
		t.Fatalf("code = %d stderr=%s", code, stderr.String())
	}
	var got struct {
		CaseCount    int                  `json:"case_count"`
		IntentCounts map[feval.Intent]int `json:"intent_counts"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode stdout: %v (%s)", err, stdout.String())
	}
	if got.CaseCount != len(feval.AllIntents()) {
		t.Fatalf("case_count = %d", got.CaseCount)
	}
	for _, intent := range feval.AllIntents() {
		if got.IntentCounts[intent] != 1 {
			t.Fatalf("intent_counts[%q] = %d", intent, got.IntentCounts[intent])
		}
	}
}

func TestRunSelectsOneIntent(t *testing.T) {
	suitePath := writeBalancedSuite(t, 2)
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"-suite", suitePath, "-intent", "developer"}, &stdout, &stderr, func(string) string { return "" })
	if code != 0 {
		t.Fatalf("code = %d stderr=%s", code, stderr.String())
	}
	var got struct {
		CaseCount    int                  `json:"case_count"`
		IntentCounts map[feval.Intent]int `json:"intent_counts"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode stdout: %v (%s)", err, stdout.String())
	}
	if got.CaseCount != 2 {
		t.Fatalf("case_count = %d, want 2", got.CaseCount)
	}
	if len(got.IntentCounts) != 1 || got.IntentCounts[feval.IntentDeveloper] != 2 {
		t.Fatalf("intent_counts = %+v", got.IntentCounts)
	}
}

func TestRunRejectsInvalidOrAmbiguousIntentSelection(t *testing.T) {
	suitePath := writeBalancedSuite(t, 2)
	for name, args := range map[string][]string{
		"unknown":         {"-intent", "other"},
		"with limit":      {"-intent", "developer", "-limit", "1"},
		"with per-intent": {"-intent", "developer", "-per-intent", "1"},
	} {
		t.Run(name, func(t *testing.T) {
			fullArgs := append([]string{"-suite", suitePath}, args...)
			var stdout, stderr bytes.Buffer
			if code := run(context.Background(), fullArgs, &stdout, &stderr, func(string) string { return "" }); code == 0 {
				t.Fatalf("args %v should fail", args)
			}
		})
	}
}

func TestRunRejectsIntentMissingFromSuiteBeforeCreatingLiveOutput(t *testing.T) {
	suitePath := writeSuite(t)
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"-suite", suitePath, "-intent", "developer"}, &stdout, &stderr, func(string) string { return "" }); code == 0 {
		t.Fatal("missing selected intent should fail in validation mode")
	}

	output := filepath.Join(t.TempDir(), "run.jsonl")
	stdout.Reset()
	stderr.Reset()
	code := run(context.Background(), []string{
		"-suite", suitePath,
		"-intent", "developer",
		"-live",
		"-output", output,
	}, &stdout, &stderr, func(string) string { return "" })
	if code == 0 {
		t.Fatal("missing selected intent should fail in live mode")
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("output path exists after selection failure: %v", err)
	}
}

func TestRunRejectsInvalidOrAmbiguousPerIntentSelection(t *testing.T) {
	suitePath := writeBalancedSuite(t, 2)
	for name, args := range map[string][]string{
		"negative":   {"-per-intent", "-1"},
		"too many":   {"-per-intent", "3"},
		"with limit": {"-per-intent", "1", "-limit", "1"},
	} {
		t.Run(name, func(t *testing.T) {
			fullArgs := append([]string{"-suite", suitePath}, args...)
			var stdout, stderr bytes.Buffer
			if code := run(context.Background(), fullArgs, &stdout, &stderr, func(string) string { return "" }); code == 0 {
				t.Fatalf("args %v should fail", args)
			}
		})
	}
}

func TestRunRequiresNewOutputPathForLiveMode(t *testing.T) {
	suitePath := writeSuite(t)
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"-suite", suitePath, "-live"}, &stdout, &stderr, func(string) string { return "" }); code == 0 {
		t.Fatal("live mode without output should fail")
	}

	existing := filepath.Join(t.TempDir(), "existing.jsonl")
	if err := os.WriteFile(existing, []byte("user data"), 0o600); err != nil {
		t.Fatalf("write existing output: %v", err)
	}
	stdout.Reset()
	stderr.Reset()
	code := run(context.Background(), []string{"-suite", suitePath, "-live", "-output", existing}, &stdout, &stderr, func(string) string { return "" })
	if code == 0 {
		t.Fatal("live mode should refuse an existing output path")
	}
	data, err := os.ReadFile(existing)
	if err != nil {
		t.Fatalf("read existing output: %v", err)
	}
	if string(data) != "user data" {
		t.Fatalf("existing output was overwritten: %q", data)
	}
}

func TestRunRejectsInvalidEndpointBeforeCreatingOutput(t *testing.T) {
	suitePath := writeSuite(t)
	output := filepath.Join(t.TempDir(), "run.jsonl")
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{
		"-suite", suitePath,
		"-live",
		"-endpoint", "not-an-endpoint",
		"-output", output,
	}, &stdout, &stderr, func(string) string { return "" })
	if code == 0 {
		t.Fatal("invalid endpoint should fail")
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("output path exists after validation failure: %v", err)
	}
}

func TestRunRejectsInvalidBaselineIdentityBeforeCreatingOutput(t *testing.T) {
	suitePath := writeSuite(t)
	for name, invalidArgs := range map[string][]string{
		"revision": {"-revision", "not a stable revision"},
		"run id":   {"-run-id", "not a stable run"},
	} {
		t.Run(name, func(t *testing.T) {
			output := filepath.Join(t.TempDir(), "run.jsonl")
			args := []string{"-suite", suitePath, "-live", "-output", output}
			args = append(args, invalidArgs...)
			var stdout, stderr bytes.Buffer
			code := run(context.Background(), args, &stdout, &stderr, func(string) string { return "" })
			if code == 0 {
				t.Fatal("invalid run identity should fail")
			}
			if _, err := os.Stat(output); !os.IsNotExist(err) {
				t.Fatalf("output path exists after identity validation failure: %v", err)
			}
		})
	}
}

func TestRunCapturesExactConfigurationManifestAndBindsLiveRecords(t *testing.T) {
	manifest := []byte(`{"schema_version":1,"discovery":{"primary_source":"mwmbl"}}`)
	sum := sha256.Sum256(manifest)
	digest := fmt.Sprintf("%x", sum)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer eval-key" {
			t.Errorf("authorization = %q", request.Header.Get("Authorization"))
		}
		switch request.URL.Path {
		case "/v1/evaluation/configuration":
			writer.Header().Set(evaluationmanifest.HeaderName, digest)
			_, _ = writer.Write(manifest)
		case "/v1/search":
			writer.Header().Set(evaluationmanifest.HeaderName, digest)
			_, _ = writer.Write([]byte(`{"query":"q","count":0,"results":[]}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)

	directory := t.TempDir()
	output := filepath.Join(directory, "run.jsonl")
	manifestOutput := filepath.Join(directory, "configuration.json")
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{
		"-suite", writeSuite(t), "-live", "-endpoint", server.URL + "/v1/search",
		"-output", output, "-configuration-manifest-output", manifestOutput,
		"-require-configuration-sha256",
	}, &stdout, &stderr, func(name string) string {
		if name == "FM_EVAL_API_KEY" {
			return "eval-key"
		}
		return ""
	})
	if code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	writtenManifest, err := os.ReadFile(manifestOutput)
	if err != nil || !bytes.Equal(writtenManifest, manifest) {
		t.Fatalf("manifest=%q err=%v", writtenManifest, err)
	}
	recordFile, err := os.Open(output)
	if err != nil {
		t.Fatal(err)
	}
	records, err := feval.LoadRecords(recordFile)
	_ = recordFile.Close()
	if err != nil || len(records) != 1 || records[0].ConfigurationSHA256 != digest {
		t.Fatalf("records=%+v err=%v", records, err)
	}
	var report struct {
		ConfigurationManifest string        `json:"configuration_manifest"`
		Summary               feval.Summary `json:"summary"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode report: %v (%s)", err, stdout.String())
	}
	if report.ConfigurationManifest != manifestOutput || report.Summary.ConfigurationSHA256 != digest {
		t.Fatalf("report=%+v", report)
	}
}

func TestRunRefusesExistingConfigurationManifestBeforeCreatingRunOutput(t *testing.T) {
	directory := t.TempDir()
	output := filepath.Join(directory, "run.jsonl")
	manifestOutput := filepath.Join(directory, "configuration.json")
	if err := os.WriteFile(manifestOutput, []byte("user data"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{
		"-suite", writeSuite(t), "-live", "-output", output,
		"-configuration-manifest-output", manifestOutput,
	}, &stdout, &stderr, func(string) string { return "" })
	if code == 0 {
		t.Fatal("existing manifest output should be refused")
	}
	if data, err := os.ReadFile(manifestOutput); err != nil || string(data) != "user data" {
		t.Fatalf("manifest=%q err=%v", data, err)
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("run output exists: %v", err)
	}
}

func TestRunRemovesNewOutputsWhenConfigurationCaptureFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set(evaluationmanifest.HeaderName, strings.Repeat("0", 64))
		_, _ = writer.Write([]byte(`{"schema_version":1}`))
	}))
	t.Cleanup(server.Close)
	directory := t.TempDir()
	output := filepath.Join(directory, "run.jsonl")
	manifestOutput := filepath.Join(directory, "configuration.json")
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{
		"-suite", writeSuite(t), "-live", "-endpoint", server.URL + "/v1/search",
		"-output", output, "-configuration-manifest-output", manifestOutput,
	}, &stdout, &stderr, func(string) string { return "" })
	if code == 0 || !strings.Contains(stderr.String(), "digest mismatch") {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	for _, path := range []string{output, manifestOutput} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("failed capture left %s: %v", path, err)
		}
	}
}

func TestRunAnalyzesRecordsAndHumanLabelsOffline(t *testing.T) {
	recordsPath := writeRecords(t)
	labelsPath := filepath.Join(t.TempDir(), "labels.jsonl")
	labels := `{"schema_version":1,"run_id":"run-a","case_id":"general-001","url":"https://one.example/","relevance":3}` + "\n"
	if err := os.WriteFile(labelsPath, []byte(labels), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"-records", recordsPath, "-labels", labelsPath}, &stdout, &stderr, func(string) string { return "" })
	if code != 0 {
		t.Fatalf("code = %d stderr=%s", code, stderr.String())
	}
	var got struct {
		Summary   feval.Summary          `json:"summary"`
		Relevance feval.RelevanceSummary `json:"relevance"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode stdout: %v (%s)", err, stdout.String())
	}
	if got.Summary.RunID != "run-a" || got.Summary.ResultCount != 1 {
		t.Fatalf("summary = %+v", got.Summary)
	}
	if got.Relevance.LabeledResults != 1 || got.Relevance.MeanRelevance != 3 {
		t.Fatalf("relevance = %+v", got.Relevance)
	}
}

func TestRunAnalyzesRecordsAgainstPooledCrossRunQrels(t *testing.T) {
	recordsPath := writeRecords(t)
	qrelsPath := filepath.Join(t.TempDir(), "qrels.jsonl")
	qrels := strings.Join([]string{
		`{"schema_version":1,"run_id":"older-run","judgment_origin":"assistant_provisional","case_id":"general-001","intent":"general","query":"query","url":"https://one.example/","relevance":2}`,
		`{"schema_version":1,"run_id":"other-run","judgment_origin":"independent_human","case_id":"general-001","intent":"general","query":"query","url":"https://better.example/","relevance":3}`,
	}, "\n") + "\n"
	if err := os.WriteFile(qrelsPath, []byte(qrels), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"-records", recordsPath, "-qrels", qrelsPath}, &stdout, &stderr, func(string) string { return "" })
	if code != 0 {
		t.Fatalf("code = %d stderr=%s", code, stderr.String())
	}
	var got struct {
		Qrels                string                 `json:"qrels"`
		QrelsSHA256          string                 `json:"qrels_sha256"`
		QrelsJudgmentOrigins []string               `json:"qrels_judgment_origins"`
		QrelsSourceRunIDs    []string               `json:"qrels_source_run_ids"`
		Relevance            feval.RelevanceSummary `json:"relevance"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode stdout: %v (%s)", err, stdout.String())
	}
	if got.Qrels != qrelsPath || got.Relevance.NDCGIdealScope != "pooled" || !got.Relevance.AllQueryRankingComplete {
		t.Fatalf("pooled relevance = %+v", got)
	}
	wantSHA256 := fmt.Sprintf("%x", sha256.Sum256([]byte(qrels)))
	if got.QrelsSHA256 != wantSHA256 ||
		!slices.Equal(got.QrelsJudgmentOrigins, []string{"assistant_provisional", "independent_human"}) ||
		!slices.Equal(got.QrelsSourceRunIDs, []string{"older-run", "other-run"}) {
		t.Fatalf("qrels provenance = %+v, want digest %s", got, wantSHA256)
	}
	if got.Relevance.MeanNDCGAt10 >= 1 {
		t.Fatalf("cross-run ideal did not penalize the omitted stronger candidate: %+v", got.Relevance)
	}
}

func TestRunCreatesNewBlindLabelTemplateAndRefusesOverwrite(t *testing.T) {
	recordsPath := writeRecords(t)
	templatePath := filepath.Join(t.TempDir(), "labels-template.jsonl")
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"-records", recordsPath, "-label-template", templatePath}, &stdout, &stderr, func(string) string { return "" })
	if code != 0 {
		t.Fatalf("code = %d stderr=%s", code, stderr.String())
	}
	template, err := os.ReadFile(templatePath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(template), `"relevance":null`) || strings.Contains(string(template), `"sources"`) {
		t.Fatalf("template = %s", template)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), []string{"-records", recordsPath, "-label-template", templatePath}, &stdout, &stderr, func(string) string { return "" }); code == 0 {
		t.Fatal("existing template path should be refused")
	}
}

func TestRunCreatesNewBlindLabelUIAndRefusesOverwrite(t *testing.T) {
	recordsPath := writeRecords(t)
	uiPath := filepath.Join(t.TempDir(), "labels.html")
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"-records", recordsPath, "-label-ui", uiPath}, &stdout, &stderr, func(string) string { return "" })
	if code != 0 {
		t.Fatalf("code = %d stderr=%s", code, stderr.String())
	}
	ui, err := os.ReadFile(uiPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(ui, []byte("Export completed labels")) || bytes.Contains(ui, []byte(`"sources"`)) {
		t.Fatalf("label UI is incomplete or not blind: %s", ui)
	}
	var report struct {
		LabelUI string `json:"label_ui"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode stdout: %v (%s)", err, stdout.String())
	}
	if report.LabelUI != uiPath {
		t.Fatalf("label_ui = %q", report.LabelUI)
	}

	before := append([]byte(nil), ui...)
	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), []string{"-records", recordsPath, "-label-ui", uiPath}, &stdout, &stderr, func(string) string { return "" }); code == 0 {
		t.Fatal("existing label UI path should be refused")
	}
	after, err := os.ReadFile(uiPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("existing label UI was overwritten")
	}
}

func TestRunRejectsAmbiguousOfflineModes(t *testing.T) {
	recordsPath := writeRecords(t)
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"-records", recordsPath, "-live"}, &stdout, &stderr, func(string) string { return "" }); code == 0 {
		t.Fatal("records and live modes should conflict")
	}
	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), []string{"-labels", "labels.jsonl"}, &stdout, &stderr, func(string) string { return "" }); code == 0 {
		t.Fatal("labels without records should fail")
	}
	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), []string{"-qrels", "qrels.jsonl"}, &stdout, &stderr, func(string) string { return "" }); code == 0 {
		t.Fatal("qrels without records should fail")
	}
	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), []string{"-records", recordsPath, "-labels", "labels.jsonl", "-qrels", "qrels.jsonl"}, &stdout, &stderr, func(string) string { return "" }); code == 0 {
		t.Fatal("run-bound labels and pooled qrels should conflict")
	}
	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), []string{"-label-ui", "labels.html"}, &stdout, &stderr, func(string) string { return "" }); code == 0 {
		t.Fatal("label UI without records should fail")
	}
	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), []string{"-records", recordsPath, "-require-build-sha256"}, &stdout, &stderr, func(string) string { return "" }); code == 0 || !strings.Contains(stderr.String(), "-records cannot be combined with live-run controls") {
		t.Fatalf("records and build requirement should conflict: code=%d stderr=%s", code, stderr.String())
	}
}

func writeSuite(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "queries.jsonl")
	line := `{"id":"general-001","intent":"general","query":"how tides work","tags":["science"],"max_results":10,"search_depth":"basic"}` + "\n"
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		t.Fatalf("write suite: %v", err)
	}
	return path
}

func writeBalancedSuite(t *testing.T, perIntent int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "queries.jsonl")
	var lines strings.Builder
	for _, intent := range feval.AllIntents() {
		for i := 1; i <= perIntent; i++ {
			fmt.Fprintf(&lines, `{"id":"%s-%03d","intent":"%s","query":"query %s %d","tags":["test"],"max_results":10,"search_depth":"basic"}`+"\n", intent, i, intent, intent, i)
		}
	}
	if err := os.WriteFile(path, []byte(lines.String()), 0o600); err != nil {
		t.Fatalf("write suite: %v", err)
	}
	return path
}

func writeRecords(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "run.jsonl")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	records := []feval.Record{{
		SchemaVersion: 1, RunID: "run-a", CaseID: "general-001", Intent: feval.IntentGeneral,
		Query: "query", SearchDepth: "basic", Attempted: true, HTTPStatus: 200,
		NonEmpty: true, ResultCount: 1, UniqueDomains: 1,
		Results: []feval.ResultObservation{{URL: "https://one.example/", Domain: "one.example", Title: "One"}},
	}}
	if err := feval.WriteRecords(file, records); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}
