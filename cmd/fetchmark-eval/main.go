// Command fetchmark-eval validates Fetchmark's fixed query suite and, only
// with -live, executes it against an operator-supplied Fetchmark endpoint.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	feval "github.com/staticvar/fetchmark/internal/eval"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr, os.Getenv))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	fs := flag.NewFlagSet("fetchmark-eval", flag.ContinueOnError)
	fs.SetOutput(stderr)
	suitePath := fs.String("suite", "eval/queries.jsonl", "path to the versioned JSONL query suite")
	live := fs.Bool("live", false, "execute live HTTP queries (disabled by default)")
	recordsPath := fs.String("records", "", "existing JSONL run artifact to analyze offline")
	labelsPath := fs.String("labels", "", "completed relevance-label JSONL for -records")
	labelTemplatePath := fs.String("label-template", "", "new blind relevance-label template path for -records")
	labelUIPath := fs.String("label-ui", "", "new self-contained offline relevance-labeling HTML path for -records")
	endpoint := fs.String("endpoint", "http://127.0.0.1:8080/v1/search", "Fetchmark /v1/search endpoint")
	outputPath := fs.String("output", "", "new JSONL output path required with -live")
	concurrency := fs.Int("concurrency", 2, "maximum concurrent live queries (1..32)")
	requestTimeout := fs.Duration("request-timeout", 30*time.Second, "timeout for each HTTP request")
	runTimeout := fs.Duration("run-timeout", 15*time.Minute, "deadline for the complete live run")
	runID := fs.String("run-id", "", "stable run identifier (timestamp when omitted)")
	revision := fs.String("revision", "", "Fetchmark revision recorded with each live observation")
	configurationID := fs.String("configuration-id", "", "deployment configuration identifier recorded with the live run")
	requireBuildSHA256 := fs.Bool("require-build-sha256", false, "fail a live run unless every HTTP response identifies one executable artifact")
	configurationManifestOutput := fs.String("configuration-manifest-output", "", "new exact resolved-configuration JSON path fetched before a live run")
	requireConfigurationSHA256 := fs.Bool("require-configuration-sha256", false, "fail a live run unless every HTTP response identifies one resolved configuration")
	limit := fs.Int("limit", 0, "run only the first N validated cases (0 means all)")
	perIntent := fs.Int("per-intent", 0, "run the first N cases from each represented intent (0 means all)")
	intentName := fs.String("intent", "", "run only cases for one intent")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr, "fetchmark-eval: unexpected positional arguments")
		return 2
	}
	if strings.TrimSpace(*recordsPath) == "" {
		if strings.TrimSpace(*labelsPath) != "" || strings.TrimSpace(*labelTemplatePath) != "" || strings.TrimSpace(*labelUIPath) != "" {
			fmt.Fprintln(stderr, "fetchmark-eval: -labels, -label-template, and -label-ui require -records")
			return 2
		}
	} else {
		if *live || strings.TrimSpace(*outputPath) != "" || *limit != 0 || *perIntent != 0 || strings.TrimSpace(*intentName) != "" || strings.TrimSpace(*revision) != "" || strings.TrimSpace(*configurationID) != "" || *requireBuildSHA256 || strings.TrimSpace(*configurationManifestOutput) != "" || *requireConfigurationSHA256 {
			fmt.Fprintln(stderr, "fetchmark-eval: -records cannot be combined with live-run controls")
			return 2
		}
		return analyzeRecords(*recordsPath, *labelsPath, *labelTemplatePath, *labelUIPath, stdout, stderr)
	}
	if !*live && (strings.TrimSpace(*configurationManifestOutput) != "" || *requireConfigurationSHA256) {
		fmt.Fprintln(stderr, "fetchmark-eval: configuration manifest controls require -live")
		return 2
	}

	suiteFile, err := os.Open(*suitePath)
	if err != nil {
		fmt.Fprintf(stderr, "fetchmark-eval: open suite: %v\n", err)
		return 1
	}
	suite, loadErr := feval.LoadSuite(suiteFile)
	closeErr := suiteFile.Close()
	if loadErr != nil {
		fmt.Fprintf(stderr, "fetchmark-eval: %v\n", loadErr)
		return 1
	}
	if closeErr != nil {
		fmt.Fprintf(stderr, "fetchmark-eval: close suite: %v\n", closeErr)
		return 1
	}
	if *limit < 0 || *limit > len(suite.Cases) {
		fmt.Fprintf(stderr, "fetchmark-eval: limit must be 0..%d\n", len(suite.Cases))
		return 2
	}
	if *perIntent < 0 {
		fmt.Fprintln(stderr, "fetchmark-eval: per-intent must be non-negative")
		return 2
	}
	if *limit > 0 && *perIntent > 0 {
		fmt.Fprintln(stderr, "fetchmark-eval: -limit and -per-intent cannot be combined")
		return 2
	}
	intent, err := parseIntent(*intentName)
	if err != nil {
		fmt.Fprintf(stderr, "fetchmark-eval: %v\n", err)
		return 2
	}
	if intent != "" && (*limit > 0 || *perIntent > 0) {
		fmt.Fprintln(stderr, "fetchmark-eval: -intent cannot be combined with -limit or -per-intent")
		return 2
	}
	if *limit > 0 {
		suite.Cases = suite.Cases[:*limit]
	}
	if *perIntent > 0 {
		var err error
		suite, err = samplePerIntent(suite, *perIntent)
		if err != nil {
			fmt.Fprintf(stderr, "fetchmark-eval: %v\n", err)
			return 2
		}
	}
	if intent != "" {
		suite, err = selectIntent(suite, intent)
		if err != nil {
			fmt.Fprintf(stderr, "fetchmark-eval: %v\n", err)
			return 2
		}
	}

	if !*live {
		return writeJSON(stdout, stderr, map[string]any{
			"schema_version": 1,
			"suite":          *suitePath,
			"case_count":     len(suite.Cases),
			"intent_counts":  suite.IntentCounts(),
			"live":           false,
		})
	}
	if strings.TrimSpace(*outputPath) == "" {
		fmt.Fprintln(stderr, "fetchmark-eval: -output is required with -live")
		return 2
	}
	if *concurrency < 1 || *concurrency > 32 {
		fmt.Fprintln(stderr, "fetchmark-eval: concurrency must be 1..32")
		return 2
	}
	if *requestTimeout <= 0 || *runTimeout <= 0 {
		fmt.Fprintln(stderr, "fetchmark-eval: timeouts must be positive")
		return 2
	}
	if err := feval.ValidateEndpoint(*endpoint); err != nil {
		fmt.Fprintf(stderr, "fetchmark-eval: %v\n", err)
		return 2
	}
	if err := feval.ValidateBaselineIdentity(*revision, *configurationID); err != nil {
		fmt.Fprintf(stderr, "fetchmark-eval: %v\n", err)
		return 2
	}
	if err := feval.ValidateRunID(*runID); err != nil {
		fmt.Fprintf(stderr, "fetchmark-eval: %v\n", err)
		return 2
	}

	output, err := os.OpenFile(*outputPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		fmt.Fprintf(stderr, "fetchmark-eval: create output: %v\n", err)
		return 1
	}
	removeNewOutput := func() {
		_ = output.Close()
		_ = os.Remove(*outputPath)
	}

	apiKey := getenv("FM_EVAL_API_KEY")
	liveClient := &http.Client{Timeout: *requestTimeout}
	configurationManifestPath := strings.TrimSpace(*configurationManifestOutput)
	configurationSHA256 := ""
	if configurationManifestPath != "" {
		manifestOutput, createErr := os.OpenFile(configurationManifestPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if createErr != nil {
			removeNewOutput()
			fmt.Fprintf(stderr, "fetchmark-eval: create configuration manifest: %v\n", createErr)
			return 1
		}
		manifestCtx, manifestCancel := context.WithTimeout(ctx, *requestTimeout)
		manifest, digest, fetchErr := feval.FetchConfigurationManifest(manifestCtx, liveClient, *endpoint, apiKey)
		manifestCancel()
		writeErr := fetchErr
		if writeErr == nil {
			_, writeErr = manifestOutput.Write(manifest)
		}
		if syncErr := manifestOutput.Sync(); writeErr == nil {
			writeErr = syncErr
		}
		if closeErr := manifestOutput.Close(); writeErr == nil {
			writeErr = closeErr
		}
		if writeErr != nil {
			_ = os.Remove(configurationManifestPath)
			removeNewOutput()
			fmt.Fprintf(stderr, "fetchmark-eval: capture configuration manifest: %v\n", writeErr)
			return 1
		}
		configurationSHA256 = digest
	}

	liveCtx, cancel := context.WithTimeout(ctx, *runTimeout)
	records, summary, runErr := (feval.Runner{
		Endpoint:                    *endpoint,
		APIKey:                      apiKey,
		Client:                      liveClient,
		Concurrency:                 *concurrency,
		RunID:                       *runID,
		Revision:                    *revision,
		ConfigurationID:             *configurationID,
		RequireBuildSHA256:          *requireBuildSHA256,
		RequireConfigurationSHA256:  *requireConfigurationSHA256 || configurationManifestPath != "",
		ExpectedConfigurationSHA256: configurationSHA256,
	}).Run(liveCtx, suite)
	cancel()
	writeErr := feval.WriteRecords(output, records)
	if syncErr := output.Sync(); writeErr == nil {
		writeErr = syncErr
	}
	if closeOutputErr := output.Close(); writeErr == nil {
		writeErr = closeOutputErr
	}
	if writeErr != nil {
		fmt.Fprintf(stderr, "fetchmark-eval: write output: %v\n", writeErr)
		return 1
	}
	report := map[string]any{"output": *outputPath, "summary": summary}
	if configurationManifestPath != "" {
		report["configuration_manifest"] = configurationManifestPath
	}
	if code := writeJSON(stdout, stderr, report); code != 0 {
		return code
	}
	if runErr != nil {
		fmt.Fprintf(stderr, "fetchmark-eval: run incomplete: %v\n", runErr)
		return 1
	}
	return 0
}

func parseIntent(raw string) (feval.Intent, error) {
	name := strings.TrimSpace(raw)
	if name == "" {
		return "", nil
	}
	for _, intent := range feval.AllIntents() {
		if name == string(intent) {
			return intent, nil
		}
	}
	return "", fmt.Errorf("unknown intent %q", name)
}

func selectIntent(suite feval.Suite, selected feval.Intent) (feval.Suite, error) {
	cases := make([]feval.Case, 0, suite.IntentCounts()[selected])
	for _, c := range suite.Cases {
		if c.Intent == selected {
			cases = append(cases, c)
		}
	}
	if len(cases) == 0 {
		return feval.Suite{}, fmt.Errorf("intent %q has no cases in the suite", selected)
	}
	return feval.Suite{Cases: cases}, nil
}

func samplePerIntent(suite feval.Suite, perIntent int) (feval.Suite, error) {
	if perIntent < 1 {
		return feval.Suite{}, fmt.Errorf("per-intent must be positive")
	}
	available := suite.IntentCounts()
	for _, intent := range feval.AllIntents() {
		if available[intent] > 0 && available[intent] < perIntent {
			return feval.Suite{}, fmt.Errorf("per-intent %d exceeds the %d available %s cases", perIntent, available[intent], intent)
		}
	}
	selected := make([]feval.Case, 0, perIntent*len(feval.AllIntents()))
	counts := make(map[feval.Intent]int, len(feval.AllIntents()))
	for _, c := range suite.Cases {
		if counts[c.Intent] >= perIntent {
			continue
		}
		selected = append(selected, c)
		counts[c.Intent]++
	}
	return feval.Suite{Cases: selected}, nil
}

func analyzeRecords(recordsPath, labelsPath, labelTemplatePath, labelUIPath string, stdout, stderr io.Writer) int {
	recordsFile, err := os.Open(recordsPath)
	if err != nil {
		fmt.Fprintf(stderr, "fetchmark-eval: open records: %v\n", err)
		return 1
	}
	records, loadErr := feval.LoadRecords(recordsFile)
	closeErr := recordsFile.Close()
	if loadErr != nil {
		fmt.Fprintf(stderr, "fetchmark-eval: %v\n", loadErr)
		return 1
	}
	if closeErr != nil {
		fmt.Fprintf(stderr, "fetchmark-eval: close records: %v\n", closeErr)
		return 1
	}

	report := map[string]any{
		"records": recordsPath,
		"summary": feval.Summarize(records),
	}
	if strings.TrimSpace(labelsPath) != "" {
		labelsFile, err := os.Open(labelsPath)
		if err != nil {
			fmt.Fprintf(stderr, "fetchmark-eval: open labels: %v\n", err)
			return 1
		}
		labels, loadErr := feval.LoadRelevanceLabels(labelsFile, records)
		closeErr := labelsFile.Close()
		if loadErr != nil {
			fmt.Fprintf(stderr, "fetchmark-eval: %v\n", loadErr)
			return 1
		}
		if closeErr != nil {
			fmt.Fprintf(stderr, "fetchmark-eval: close labels: %v\n", closeErr)
			return 1
		}
		relevance, err := feval.ScoreRelevance(records, labels)
		if err != nil {
			fmt.Fprintf(stderr, "fetchmark-eval: score labels: %v\n", err)
			return 1
		}
		report["labels"] = labelsPath
		report["relevance"] = relevance
	}
	if strings.TrimSpace(labelTemplatePath) != "" {
		if err := createLabelTemplate(labelTemplatePath, records); err != nil {
			fmt.Fprintf(stderr, "fetchmark-eval: create label template: %v\n", err)
			return 1
		}
		report["label_template"] = labelTemplatePath
	}
	if strings.TrimSpace(labelUIPath) != "" {
		if err := createLabelUI(labelUIPath, records); err != nil {
			fmt.Fprintf(stderr, "fetchmark-eval: create label UI: %v\n", err)
			return 1
		}
		report["label_ui"] = labelUIPath
	}
	return writeJSON(stdout, stderr, report)
}

func createLabelTemplate(path string, records []feval.Record) error {
	output, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	writeErr := feval.WriteLabelTemplate(output, records)
	if syncErr := output.Sync(); writeErr == nil {
		writeErr = syncErr
	}
	if closeErr := output.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		_ = os.Remove(path)
	}
	return writeErr
}

func createLabelUI(path string, records []feval.Record) error {
	output, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	writeErr := feval.WriteLabelUI(output, records)
	if syncErr := output.Sync(); writeErr == nil {
		writeErr = syncErr
	}
	if closeErr := output.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		_ = os.Remove(path)
	}
	return writeErr
}

func writeJSON(stdout, stderr io.Writer, value any) int {
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		fmt.Fprintf(stderr, "fetchmark-eval: write summary: %v\n", err)
		return 1
	}
	return 0
}
