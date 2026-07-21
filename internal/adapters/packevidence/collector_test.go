package packevidence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/ccindex"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/indexpackadmission"
	"github.com/staticvar/fetchmark/internal/core/indexpackselection"
)

const collectorConfig = `{"version":1,"product_token":"FetchmarkPackEvidence","contact_uri":"mailto:operator@example.org","evidence_validity_hours":12,"min_host_interval_ms":1000,"global_concurrency":3,"request_timeout_seconds":20,"rights":{"allowed_fields":["url_metadata"],"basis":"url_metadata_policy","evidence_uri":"https://commoncrawl.org/terms-of-use","rights_notice":"URL metadata only under the publisher policy"}}`

func TestCollectPublishesOrderedAuditableBundle(t *testing.T) {
	candidates := []ccindex.Candidate{
		normalizedCandidate("https://one.example.org/page"),
		normalizedCandidate("https://two.example.org/page"),
		normalizedCandidate("https://three.example.org/page"),
	}
	input := encodeNormalized(t, candidates)
	parent := t.TempDir()
	output := filepath.Join(parent, "evidence-r1")
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	result, err := collectWithHooks(context.Background(), Options{
		ConfigRaw: []byte(collectorConfig), RightsEvidence: []byte("reviewed rights evidence\n"),
		Candidates: bytes.NewReader(input), Observer: fakeRowObserver{}, CollectorVersion: "1.0.0", UserAgentVersion: "1.0.0", OutputDir: output,
	}, collectHooksForTime(now))
	if err != nil {
		t.Fatal(err)
	}
	if result.Path != output || result.InputRecords != 3 || result.AdmittedRecords != 3 || result.ObservationRecords != 3 || result.RobotsObjects != 1 {
		t.Fatalf("result = %#v", result)
	}
	candidateRaw := readFile(t, filepath.Join(output, CandidatesFilename))
	observationRaw := readFile(t, filepath.Join(output, ObservationsFilename))
	reportRaw := readFile(t, filepath.Join(output, ReportFilename))
	if result.ReportSHA256 != digest(reportRaw) {
		t.Fatalf("report digest = %q", result.ReportSHA256)
	}
	var report Report
	if err := json.Unmarshal(reportRaw, &report); err != nil {
		t.Fatal(err)
	}
	if report.Input.SHA256 != digest(input) || report.Candidates.SHA256 != digest(candidateRaw) || report.Observations.SHA256 != digest(observationRaw) ||
		report.Candidates.Records != 3 || report.Observations.Records != 3 || report.Outcomes["admitted"] != 3 {
		t.Fatalf("report = %#v", report)
	}
	lines := bytes.Split(bytes.TrimSpace(observationRaw), []byte{'\n'})
	for index, line := range lines {
		var observation Observation
		if err := json.Unmarshal(line, &observation); err != nil {
			t.Fatal(err)
		}
		if observation.Row != uint64(index+1) || observation.InputLineSHA256 == "" {
			t.Fatalf("observation %d = %#v", index, observation)
		}
	}
	if bytes.Contains(candidateRaw, []byte(`"version":1,"urlkey"`)) {
		t.Fatal("normalized-row version leaked into builder candidate schema")
	}
	if bytes.Contains(candidateRaw, []byte("reviewed rights evidence")) || bytes.Contains(observationRaw, []byte("reviewed rights evidence")) || bytes.Contains(reportRaw, []byte("reviewed rights evidence")) {
		t.Fatal("bundle retained exact rights evidence bytes")
	}
	policyPath := filepath.Join(output, report.Robots[0].Path)
	if string(readFile(t, policyPath)) != "User-agent: *\nAllow: /\n" {
		t.Fatal("robots policy object differs")
	}
	candidateDigest := sha256.Sum256(candidateRaw)
	specRaw, err := json.Marshal(indexpackselection.Spec{
		Version: indexpackselection.SpecVersion, Profile: indexpackselection.ProfileLightweight, PackID: "collector-e2e", Revision: 1,
		CreatedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(30 * 24 * time.Hour).Format(time.RFC3339),
		Publisher: indexpack.Publisher{
			Name: "Evidence collector test", ContactURI: "mailto:publisher@example.org",
			TakedownURI: "https://publisher.example.org/takedown", RightsNotice: "URL metadata only",
		},
		Languages: []string{"en"}, MaxRecords: 10, MaxRecordsPerHost: 10, MaxPermissionAgeHours: 24,
		RobotsUserAgent: "FetchmarkPackEvidence/1.0.0 (+mailto:operator@example.org)",
		CandidateSHA256: hex.EncodeToString(candidateDigest[:]),
		Inputs: []indexpack.BuildInput{{
			Name: "normalized Common Crawl URL metadata", URI: "https://index.commoncrawl.org/CC-MAIN-2026-30-index",
			RetrievedAt: now.Add(-time.Hour).Format(time.RFC3339), SHA256: strings.Repeat("5", 64), RightsNotice: "URL metadata only",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	accepted := 0
	selection, err := indexpackselection.Select(context.Background(), now, specRaw, []byte(`{"version":1,"urls":[],"host_suffixes":[]}`), bytes.NewReader(candidateRaw), func(indexpack.Record) error {
		accepted++
		return nil
	})
	if err != nil || accepted != 3 || selection.Accepted != 3 {
		t.Fatalf("builder selection accepted=%d report=%#v error=%v", accepted, selection, err)
	}
	assertNoStaging(t, parent)
}

func TestCollectLateInvalidInputPublishesNothing(t *testing.T) {
	valid := encodeNormalized(t, []ccindex.Candidate{normalizedCandidate("https://example.org/page")})
	input := append(valid, []byte("{}\n")...)
	parent := t.TempDir()
	output := filepath.Join(parent, "evidence-r1")
	_, err := collectWithHooks(context.Background(), Options{
		ConfigRaw: []byte(collectorConfig), RightsEvidence: []byte("evidence"), Candidates: bytes.NewReader(input),
		Observer: fakeRowObserver{userAgentVersion: "1"}, CollectorVersion: "1", UserAgentVersion: "1", OutputDir: output,
	}, collectHooksForTime(time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)))
	if err == nil || !strings.Contains(err.Error(), "row 2") {
		t.Fatalf("error = %v", err)
	}
	if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("partial output exists: %v", statErr)
	}
	assertNoStaging(t, parent)
}

func TestCollectKeepsTenThousandRowSafetyCeiling(t *testing.T) {
	candidates := make([]ccindex.Candidate, indexpackadmission.MaxCollectorRecords+1)
	for index := range candidates {
		candidates[index] = normalizedCandidate(fmt.Sprintf("https://host-%05d.example.org/page", index))
	}
	parent := t.TempDir()
	output := filepath.Join(parent, "evidence-r1")
	_, err := collectWithHooks(context.Background(), Options{
		ConfigRaw: []byte(collectorConfig), RightsEvidence: []byte("evidence"), Candidates: bytes.NewReader(encodeNormalized(t, candidates)),
		Observer: fakeRowObserver{}, CollectorVersion: "1.0.0", UserAgentVersion: "1.0.0", OutputDir: output,
	}, collectHooksForTime(time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)))
	if err == nil || !strings.Contains(err.Error(), "input exceeds 10000 rows") {
		t.Fatalf("error = %v", err)
	}
	if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("partial output exists: %v", statErr)
	}
	assertNoStaging(t, parent)
}

func TestCollectRefusesObserverAdmissionsThatFailBuilderPreflight(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		mutate func(*ccindex.Candidate)
	}{
		{name: "status", mutate: func(value *ccindex.Candidate) { value.Status = "204" }},
		{name: "capture MIME", mutate: func(value *ccindex.Candidate) { value.MIME = "application/pdf" }},
		{name: "detected MIME", mutate: func(value *ccindex.Candidate) { value.MIMEDetected = "text/plain" }},
		{name: "zero WARC length", mutate: func(value *ccindex.Candidate) { value.Length = "0" }},
		{name: "overflowing WARC location", mutate: func(value *ccindex.Candidate) { value.Offset = "18446744073709551615" }},
		{name: "future capture", mutate: func(value *ccindex.Candidate) { value.Timestamp = now.Add(time.Second).Format("20060102150405") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := normalizedCandidate("https://example.org/page")
			test.mutate(&value)
			parent := t.TempDir()
			output := filepath.Join(parent, "evidence-r1")
			_, err := collectWithHooks(context.Background(), Options{
				ConfigRaw: []byte(collectorConfig), RightsEvidence: []byte("evidence"), Candidates: bytes.NewReader(encodeNormalized(t, []ccindex.Candidate{value})),
				Observer: fakeRowObserver{}, CollectorVersion: "1.0.0", UserAgentVersion: "1.0.0", OutputDir: output,
			}, collectHooksForTime(now))
			if err == nil || !strings.Contains(err.Error(), "non-buildable admitted row") {
				t.Fatalf("error = %v", err)
			}
			if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("partial output exists: %v", statErr)
			}
			assertNoStaging(t, parent)
		})
	}
}

func TestCollectRejectsOutputOverwriteAndExpiredHandoff(t *testing.T) {
	input := encodeNormalized(t, []ccindex.Candidate{normalizedCandidate("https://example.org/page")})
	parent := t.TempDir()
	output := filepath.Join(parent, "existing")
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	_, err := collectWithHooks(context.Background(), Options{
		ConfigRaw: []byte(collectorConfig), RightsEvidence: []byte("evidence"), Candidates: bytes.NewReader(input),
		Observer: fakeRowObserver{userAgentVersion: "1"}, CollectorVersion: "1", UserAgentVersion: "1", OutputDir: output,
	}, collectHooksForTime(time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)))
	if err == nil || !strings.Contains(err.Error(), "output already exists") {
		t.Fatalf("overwrite error = %v", err)
	}

	expiredOutput := filepath.Join(parent, "expired")
	_, err = collectWithHooks(context.Background(), Options{
		ConfigRaw: []byte(collectorConfig), RightsEvidence: []byte("evidence"), Candidates: bytes.NewReader(input),
		Observer: fakeRowObserver{validUntil: "2026-07-19T12:00:30Z", userAgentVersion: "1"}, CollectorVersion: "1", UserAgentVersion: "1", OutputDir: expiredOutput,
	}, collectHooksForTime(time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)))
	if err == nil || !strings.Contains(err.Error(), "safe builder handoff") {
		t.Fatalf("expiry error = %v", err)
	}
	if _, statErr := os.Lstat(expiredOutput); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expired bundle exists: %v", statErr)
	}
}

func TestCollectReportsPostCommitFailure(t *testing.T) {
	input := encodeNormalized(t, []ccindex.Candidate{normalizedCandidate("https://example.org/page")})
	parent := t.TempDir()
	output := filepath.Join(parent, "evidence-r1")
	hooks := collectHooksForTime(time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC))
	baseSync := hooks.syncParent
	syncCalls := 0
	hooks.syncParent = func(root *os.Root) error {
		syncCalls++
		if syncCalls == 1 {
			return errors.New("injected post-commit sync failure")
		}
		return baseSync(root)
	}
	result, err := collectWithHooks(context.Background(), Options{
		ConfigRaw: []byte(collectorConfig), RightsEvidence: []byte("evidence"), Candidates: bytes.NewReader(input),
		Observer: fakeRowObserver{userAgentVersion: "1"}, CollectorVersion: "1", UserAgentVersion: "1", OutputDir: output,
	}, hooks)
	if !errors.Is(err, ErrBundleCommitted) || result.Path != output || result.ReportSHA256 == "" {
		t.Fatalf("result=%#v error=%v", result, err)
	}
	if _, statErr := os.Stat(filepath.Join(output, ReportFilename)); statErr != nil {
		t.Fatalf("committed report missing: %v", statErr)
	}
}

type fakeRowObserver struct {
	validUntil       string
	userAgentVersion string
}

func (observer fakeRowObserver) Observe(ctx context.Context, row uint64, candidate ccindex.Candidate, rights indexpackselection.RightsDecision) (Result, error) {
	select {
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-time.After(time.Duration(4-row) * time.Millisecond):
	}
	validUntil := observer.validUntil
	if validUntil == "" {
		validUntil = "2026-07-20T00:00:00Z"
	}
	userAgentVersion := observer.userAgentVersion
	if userAgentVersion == "" {
		userAgentVersion = "1.0.0"
	}
	policy := []byte("User-agent: *\nAllow: /\n")
	policyDigest := sha256.Sum256(policy)
	indexing, exactIndexing, err := indexpackadmission.EvaluateIndexing(indexpackadmission.IndexingInput{
		Status: 200, FinalURL: candidate.URL, ContentType: "text/html; charset=utf-8",
		Representation: []byte("<html><head><meta name=\"robots\" content=\"index\"></head><body>fixture</body></html>"),
		Complete:       true, UserAgent: "FetchmarkPackEvidence/" + userAgentVersion + " (+mailto:operator@example.org)",
		ObservedAt: time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC), Validity: 12 * time.Hour,
	})
	if err != nil {
		return Result{}, err
	}
	indexing.ValidUntil = validUntil
	admission := indexpackselection.Admission{
		Version: indexpackselection.AdmissionVersion,
		Robots: indexpackselection.RobotsObservation{
			UserAgent: "FetchmarkPackEvidence/" + userAgentVersion + " (+mailto:operator@example.org)",
			RobotsURI: strings.TrimSuffix(candidate.URL, "/page") + "/robots.txt", CheckedAt: "2026-07-19T12:00:00Z", ValidUntil: validUntil,
			Outcome: "allowed", BodySHA256: hex.EncodeToString(policyDigest[:]),
		},
		Indexing: indexing,
		Rights:   rights,
	}
	return Result{
		Candidate: candidateWithAdmission(candidate, admission), PolicyBody: policy, PolicyBodyComplete: true,
		Observation: Observation{
			Version: ObservationVersion, Row: row, URL: candidate.URL, Outcome: "admitted",
			Robots: RobotsEvidence{Status: 200, RobotsURI: admission.Robots.RobotsURI, FinalURI: admission.Robots.RobotsURI, CheckedAt: admission.Robots.CheckedAt, ValidUntil: admission.Robots.ValidUntil, Outcome: "allowed", BodySHA256: admission.Robots.BodySHA256, BodyComplete: true},
			Indexing: ResponseEvidence{Status: 200, FinalURL: candidate.URL, CheckedAt: admission.Indexing.CheckedAt, ValidUntil: admission.Indexing.ValidUntil, Outcome: "indexable",
				ContentType: exactIndexing.ContentType, XRobotsTag: exactIndexing.XRobotsTag, MetadataRobots: exactIndexing.MetadataRobots,
				HeadersSHA256: admission.Indexing.HeadersSHA256, RepresentationSHA256: admission.Indexing.RepresentationSHA256, ParserVersion: admission.Indexing.ParserVersion},
		},
	}, nil
}

func encodeNormalized(t *testing.T, candidates []ccindex.Candidate) []byte {
	t.Helper()
	var output bytes.Buffer
	for index, candidate := range candidates {
		candidate.URLKey = fmt.Sprintf("org,example,%05d)/page", index)
		raw, err := json.Marshal(candidate)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ccindex.DecodeCandidate(raw); err != nil {
			t.Fatalf("invalid fixture: %v", err)
		}
		output.Write(raw)
		output.WriteByte('\n')
	}
	return output.Bytes()
}

func collectHooksForTime(now time.Time) collectHooks {
	hooks := defaultCollectHooks()
	hooks.now = func() time.Time { return now }
	return hooks
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func assertNoStaging(t *testing.T, parent string) {
	t.Helper()
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".fetchmark-pack-evidence-") {
			t.Fatalf("staging artifact remains: %s", entry.Name())
		}
	}
}
