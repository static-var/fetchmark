package packevidence

import (
	"bytes"
	"context"
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

func TestMergedEvidenceRejectsRobotsDisallowAndDerivedNoIndex(t *testing.T) {
	userAgent := "FetchmarkPackEvidence/1.0.0 (+mailto:operator@example.org)"
	disallow := []byte("User-agent: *\nDisallow: /\n")
	robotsEvidence := RobotsEvidence{
		Status: 200, RobotsURI: "https://one.example.org/robots.txt", FinalURI: "https://one.example.org/robots.txt",
		Outcome: "allowed", BodySHA256: digest(disallow), BodyComplete: true,
	}
	if err := validateMergedRobotsEvidence(robotsEvidence, userAgent, "https://one.example.org/page", disallow); err == nil || !strings.Contains(err.Error(), "does not allow") {
		t.Fatal("robots-disallowed archived policy was accepted")
	}

	indexing := ResponseEvidence{
		Status: 200, FinalURL: "https://one.example.org/page", CheckedAt: "2026-07-19T12:00:00Z", ValidUntil: "2026-07-20T00:00:00Z",
		Outcome: "indexable", ContentType: "text/html", XRobotsTag: []string{"noindex"},
		RepresentationSHA256: strings.Repeat("c", 64), ParserVersion: indexpackadmission.NoIndexParserVersion,
	}
	if _, err := validateMergedIndexingEvidence(indexing, userAgent); err == nil || !strings.Contains(err.Error(), "not indexable") {
		t.Fatal("derived noindex evidence was accepted as indexable")
	}
}

func TestValidateMergeReportRejectsMalformedPortableAccounting(t *testing.T) {
	fixture := newMergeFixture(t)
	output := filepath.Join(fixture.root, "merged")
	if _, err := mergeWithHooks(context.Background(), MergeOptions{
		PartitionsDir: fixture.partitions, BundleDirs: fixture.bundles, MergerVersion: "test-merger", OutputDir: output,
	}, mergeHooksForTime(fixture.now)); err != nil {
		t.Fatal(err)
	}
	var report MergeReport
	if err := json.Unmarshal(readFile(t, filepath.Join(output, MergeReportFilename)), &report); err != nil {
		t.Fatal(err)
	}
	report.Outcomes["invented"] = 1
	report.Outcomes["admitted"]--
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := DecodeMergeReport(raw); err == nil {
		t.Fatal("portable report accepted an unknown outcome key")
	}
	report.Outcomes = map[string]uint64{"admitted": report.Input.Records}
	report.Sources[0].Input.Path = "../part.jsonl"
	raw, err = json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := DecodeMergeReport(raw); err == nil {
		t.Fatal("portable report accepted an unsafe source path")
	}
}

func TestValidateMergeReportReappliesCollectorAndAggregateBounds(t *testing.T) {
	fixture := newMergeFixture(t)
	output := filepath.Join(fixture.root, "merged")
	if _, err := mergeWithHooks(context.Background(), MergeOptions{
		PartitionsDir: fixture.partitions, BundleDirs: fixture.bundles, MergerVersion: "test-merger", OutputDir: output,
	}, mergeHooksForTime(fixture.now)); err != nil {
		t.Fatal(err)
	}
	var valid MergeReport
	if err := json.Unmarshal(readFile(t, filepath.Join(output, MergeReportFilename)), &valid); err != nil {
		t.Fatal(err)
	}

	oversizedSource := valid
	source := valid.Sources[0]
	source.Input.Records = indexpackadmission.MaxCollectorRecords + 1
	source.Observations.Records = source.Input.Records
	source.LastRow = source.Input.Records
	source.Outcomes = map[string]uint64{"admitted": source.Candidates.Records, "rejected": source.Input.Records - source.Candidates.Records}
	source.Rejections = map[string]uint64{"invalid_capture_metadata": source.Outcomes["rejected"]}
	oversizedSource.Sources = []MergeSource{source}
	oversizedSource.Input = source.Input
	oversizedSource.Input.Path = ""
	oversizedSource.Candidates = source.Candidates
	oversizedSource.Outcomes = cloneMergeCounts(source.Outcomes)
	oversizedSource.Rejections = cloneMergeCounts(source.Rejections)
	if raw, err := json.Marshal(oversizedSource); err != nil {
		t.Fatal(err)
	} else if _, _, err := DecodeMergeReport(raw); err == nil {
		t.Fatal("portable report accepted a source above the collector row limit")
	}

	zeroObservationBytes := valid
	zeroObservationBytes.Sources = append([]MergeSource(nil), valid.Sources...)
	zeroObservationBytes.Sources[0].Observations.Bytes = 0
	if raw, err := json.Marshal(zeroObservationBytes); err != nil {
		t.Fatal(err)
	} else if _, _, err := DecodeMergeReport(raw); err == nil {
		t.Fatal("portable report accepted nonempty observations with zero bytes")
	}

	aggregate := oversizedPortableMergeReport()
	if raw, err := json.Marshal(aggregate); err != nil {
		t.Fatal(err)
	} else if _, _, err := DecodeMergeReport(raw); err == nil {
		t.Fatal("portable report accepted an aggregate above the input byte limit")
	}
}

func TestMergeRejectsBalancedPerSourceCandidateCountDrift(t *testing.T) {
	fixture := newCustomMergeFixture(t, []ccindex.Candidate{
		normalizedCandidate("https://one.example.org/page"), normalizedCandidate("https://two.example.org/page"),
		normalizedCandidate("https://three.example.org/page"), normalizedCandidate("https://four.example.org/page"),
	}, []int{2, 2}, rejectSecondObserver{}, 0)
	for index, records := range []uint64{0, 2} {
		path := filepath.Join(fixture.bundles[index], ReportFilename)
		var report Report
		if err := json.Unmarshal(readFile(t, path), &report); err != nil {
			t.Fatal(err)
		}
		report.Candidates.Records = records
		raw, err := json.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	output := filepath.Join(fixture.root, "merged")
	if _, err := mergeWithHooks(context.Background(), MergeOptions{
		PartitionsDir: fixture.partitions, BundleDirs: fixture.bundles, MergerVersion: "test-merger", OutputDir: output,
	}, mergeHooksForTime(fixture.now)); err == nil || !strings.Contains(err.Error(), "candidate record count") {
		t.Fatalf("error = %v", err)
	}
}

func TestMergeRejectsAdmittedEvidenceOutsideCollectorInterval(t *testing.T) {
	fixture := newCustomMergeFixture(t, []ccindex.Candidate{
		normalizedCandidate("https://one.example.org/page"), normalizedCandidate("https://two.example.org/page"),
		normalizedCandidate("https://three.example.org/page"),
	}, []int{2, 1}, fakeRowObserver{userAgentVersion: "1.0.0"}, 10*time.Minute)
	bundle := fixture.bundles[0]
	candidateRaw := rewriteCandidateLines(t, readFile(t, filepath.Join(bundle, CandidatesFilename)), func(candidate *indexpackselection.Candidate) {
		candidate.Admission.Robots.CheckedAt = "2026-07-19T12:05:00Z"
		candidate.Admission.Indexing.CheckedAt = "2026-07-19T12:05:00Z"
	})
	observationRaw := rewriteObservationLines(t, readFile(t, filepath.Join(bundle, ObservationsFilename)), func(observation *Observation) {
		observation.Robots.CheckedAt = "2026-07-19T12:05:00Z"
		observation.Indexing.CheckedAt = "2026-07-19T12:05:00Z"
	})
	if err := os.WriteFile(filepath.Join(bundle, CandidatesFilename), candidateRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, ObservationsFilename), observationRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	var report Report
	path := filepath.Join(bundle, ReportFilename)
	if err := json.Unmarshal(readFile(t, path), &report); err != nil {
		t.Fatal(err)
	}
	report.Candidates.SHA256, report.Candidates.Bytes = digest(candidateRaw), uint64(len(candidateRaw))
	report.Observations.SHA256, report.Observations.Bytes = digest(observationRaw), uint64(len(observationRaw))
	reportRaw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, reportRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(fixture.root, "merged")
	if _, err := mergeWithHooks(context.Background(), MergeOptions{
		PartitionsDir: fixture.partitions, BundleDirs: fixture.bundles, MergerVersion: "test-merger", OutputDir: output,
	}, mergeHooksForTime(fixture.now)); err == nil || !strings.Contains(err.Error(), "outside the collector interval") {
		t.Fatalf("error = %v", err)
	}
}

func oversizedPortableMergeReport() MergeReport {
	started := "2026-07-19T12:00:00Z"
	digestA := strings.Repeat("a", 64)
	inputBytesPerSource := uint64(MaxPartitionInputBytes)/uint64(maxPartitionCount) + 1
	sources := make([]MergeSource, 0, int(maxPartitionCount))
	var firstRow uint64 = 1
	for index := uint64(0); index < maxPartitionCount; index++ {
		input := ArtifactDescriptor{
			Path:   filepath.ToSlash(filepath.Join("shards", fmt.Sprintf("part-%06d.jsonl", index+1))),
			SHA256: digestA, Bytes: inputBytesPerSource, Records: indexpackadmission.MaxCollectorRecords,
		}
		sources = append(sources, MergeSource{
			Partition: index + 1, FirstRow: firstRow, LastRow: firstRow + input.Records - 1,
			Input: input, ReportSHA256: digestA,
			Candidates:   ArtifactDescriptor{Path: CandidatesFilename, SHA256: digest(nil)},
			Observations: ArtifactDescriptor{Path: ObservationsFilename, SHA256: digestA, Bytes: 1, Records: input.Records},
			Outcomes:     map[string]uint64{"rejected": input.Records}, Rejections: map[string]uint64{"invalid_capture_metadata": input.Records},
		})
		firstRow += input.Records
	}
	totalBytes := inputBytesPerSource * uint64(maxPartitionCount)
	return MergeReport{
		Version: MergeReportVersion, Merger: "fetchmark-pack-evidence", MergerVersion: "test", Algorithm: MergeAlgorithm,
		PartitionManifestSHA256: digestA, StartedAt: started, CompletedAt: started, ConfigSHA256: digestA,
		RightsEvidenceSHA256: digestA, CollectorVersion: "test-collector",
		Input:      ArtifactDescriptor{SHA256: digestA, Bytes: totalBytes, Records: MaxPartitionInputRecords},
		Candidates: ArtifactDescriptor{Path: CandidatesFilename, SHA256: digest(nil)}, Sources: sources,
		Outcomes: map[string]uint64{"rejected": MaxPartitionInputRecords}, Rejections: map[string]uint64{"invalid_capture_metadata": MaxPartitionInputRecords},
	}
}

func TestMergeVerifiesBundlesAndRestoresGlobalCandidateOrder(t *testing.T) {
	fixture := newMergeFixture(t)
	output := filepath.Join(fixture.root, "merged")

	result, err := mergeWithHooks(context.Background(), MergeOptions{
		PartitionsDir: fixture.partitions,
		BundleDirs:    []string{fixture.bundles[1], fixture.bundles[0]},
		MergerVersion: "test-merger",
		OutputDir:     output,
	}, mergeHooksForTime(fixture.now))
	if err != nil {
		t.Fatal(err)
	}
	if result.Path != output || result.InputRecords != 3 || result.AdmittedRecords != 3 || result.Bundles != 2 ||
		result.ReportSHA256 == "" || result.CandidateSHA256 == "" {
		t.Fatalf("result = %#v", result)
	}

	mergedCandidates := readFile(t, filepath.Join(output, CandidatesFilename))
	wantCandidates := append(readFile(t, filepath.Join(fixture.bundles[0], CandidatesFilename)), readFile(t, filepath.Join(fixture.bundles[1], CandidatesFilename))...)
	if !bytes.Equal(mergedCandidates, wantCandidates) || result.CandidateSHA256 != digest(mergedCandidates) {
		t.Fatal("merged candidate stream did not preserve global partition order")
	}
	reportRaw := readFile(t, filepath.Join(output, MergeReportFilename))
	if result.ReportSHA256 != digest(reportRaw) {
		t.Fatal("merge result does not bind the exact report")
	}
	var report MergeReport
	if err := json.Unmarshal(reportRaw, &report); err != nil {
		t.Fatal(err)
	}
	if report.Version != MergeReportVersion || report.Algorithm != MergeAlgorithm ||
		report.PartitionManifestSHA256 != fixture.manifestSHA256 || report.Input.Records != 3 ||
		report.Candidates.SHA256 != result.CandidateSHA256 || report.Candidates.Records != 3 ||
		len(report.Sources) != 2 || report.Sources[0].FirstRow != 1 || report.Sources[0].LastRow != 2 ||
		report.Sources[1].FirstRow != 3 || report.Sources[1].LastRow != 3 ||
		report.Sources[0].ReportSHA256 != digest(readFile(t, filepath.Join(fixture.bundles[0], ReportFilename))) ||
		report.Sources[1].ReportSHA256 != digest(readFile(t, filepath.Join(fixture.bundles[1], ReportFilename))) {
		t.Fatalf("merge report = %#v", report)
	}

	repeatedOutput := filepath.Join(fixture.root, "merged-repeat")
	repeated, err := mergeWithHooks(context.Background(), MergeOptions{
		PartitionsDir: fixture.partitions, BundleDirs: fixture.bundles, MergerVersion: "test-merger", OutputDir: repeatedOutput,
	}, mergeHooksForTime(fixture.now))
	if err != nil {
		t.Fatal(err)
	}
	if repeated.ReportSHA256 != result.ReportSHA256 ||
		!bytes.Equal(readFile(t, filepath.Join(repeatedOutput, MergeReportFilename)), reportRaw) ||
		!bytes.Equal(readFile(t, filepath.Join(repeatedOutput, CandidatesFilename)), mergedCandidates) {
		t.Fatal("repeat merge was not byte deterministic")
	}
}

func TestMergeRejectsDriftMissingDuplicateAndExpiredEvidence(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *mergeFixture)
		want   string
	}{
		{
			name: "observation drift",
			mutate: func(t *testing.T, fixture *mergeFixture) {
				path := filepath.Join(fixture.bundles[0], ObservationsFilename)
				raw := readFile(t, path)
				raw[len(raw)-2] ^= 1
				if err := os.WriteFile(path, raw, 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: "observations",
		},
		{
			name: "missing bundle",
			mutate: func(t *testing.T, fixture *mergeFixture) {
				fixture.bundles = fixture.bundles[:1]
			},
			want: "exactly one bundle",
		},
		{
			name: "duplicate bundle",
			mutate: func(t *testing.T, fixture *mergeFixture) {
				fixture.bundles[1] = fixture.bundles[0]
			},
			want: "duplicate",
		},
		{
			name: "mixed collector identity",
			mutate: func(t *testing.T, fixture *mergeFixture) {
				path := filepath.Join(fixture.bundles[1], ReportFilename)
				var report Report
				if err := json.Unmarshal(readFile(t, path), &report); err != nil {
					t.Fatal(err)
				}
				report.CollectorVersion = "different-collector"
				raw, err := json.Marshal(report)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, raw, 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: "collector identity",
		},
		{
			name: "missing robots object",
			mutate: func(t *testing.T, fixture *mergeFixture) {
				var report Report
				if err := json.Unmarshal(readFile(t, filepath.Join(fixture.bundles[0], ReportFilename)), &report); err != nil {
					t.Fatal(err)
				}
				if len(report.Robots) == 0 {
					t.Fatal("fixture has no robots object")
				}
				if err := os.Remove(filepath.Join(fixture.bundles[0], filepath.FromSlash(report.Robots[0].Path))); err != nil {
					t.Fatal(err)
				}
			},
			want: "regular file",
		},
		{
			name: "expired evidence",
			mutate: func(_ *testing.T, fixture *mergeFixture) {
				fixture.now = fixture.now.Add(24 * time.Hour)
			},
			want: "expired",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newMergeFixture(t)
			test.mutate(t, fixture)
			output := filepath.Join(fixture.root, "merged")
			_, err := mergeWithHooks(context.Background(), MergeOptions{
				PartitionsDir: fixture.partitions, BundleDirs: fixture.bundles, MergerVersion: "test-merger", OutputDir: output,
			}, mergeHooksForTime(fixture.now))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
			if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("partial merge output exists: %v", statErr)
			}
		})
	}
}

func TestMergeReportsPostCommitFailure(t *testing.T) {
	fixture := newMergeFixture(t)
	output := filepath.Join(fixture.root, "merged")
	hooks := mergeHooksForTime(fixture.now)
	baseSync := hooks.syncParent
	calls := 0
	hooks.syncParent = func(root *os.Root) error {
		calls++
		if calls == 1 {
			return errors.New("injected post-commit sync failure")
		}
		return baseSync(root)
	}
	result, err := mergeWithHooks(context.Background(), MergeOptions{
		PartitionsDir: fixture.partitions, BundleDirs: fixture.bundles, MergerVersion: "test-merger", OutputDir: output,
	}, hooks)
	if !errors.Is(err, ErrMergeCommitted) || result.Path != output || result.ReportSHA256 == "" {
		t.Fatalf("result=%#v error=%v", result, err)
	}
	if _, statErr := os.Stat(filepath.Join(output, MergeReportFilename)); statErr != nil {
		t.Fatalf("committed report missing: %v", statErr)
	}
}

type mergeFixture struct {
	root           string
	partitions     string
	bundles        []string
	manifestSHA256 string
	now            time.Time
}

func newMergeFixture(t *testing.T) *mergeFixture {
	t.Helper()
	return newCustomMergeFixture(t, []ccindex.Candidate{
		normalizedCandidate("https://one.example.org/page"),
		normalizedCandidate("https://two.example.org/page"),
		normalizedCandidate("https://three.example.org/page"),
	}, []int{2, 1}, fakeRowObserver{userAgentVersion: "1.0.0"}, 0)
}

func newCustomMergeFixture(t *testing.T, candidates []ccindex.Candidate, partSizes []int, observer RowObserver, mergeDelay time.Duration) *mergeFixture {
	t.Helper()
	root := t.TempDir()
	partitions := filepath.Join(root, "partitions")
	if err := os.MkdirAll(filepath.Join(partitions, "shards"), 0o700); err != nil {
		t.Fatal(err)
	}
	normalized := encodeNormalized(t, candidates)
	lines := bytes.SplitAfter(normalized, []byte{'\n'})
	parts := make([][]byte, 0, len(partSizes))
	cursor := 0
	for _, size := range partSizes {
		if size <= 0 || cursor+size > len(candidates) {
			t.Fatal("invalid custom partition sizes")
		}
		parts = append(parts, bytes.Join(lines[cursor:cursor+size], nil))
		cursor += size
	}
	if cursor != len(candidates) {
		t.Fatal("custom partition sizes do not cover candidates")
	}
	descriptors := make([]PartitionDescriptor, 0, len(parts))
	var first uint64 = 1
	for index, raw := range parts {
		path := filepath.ToSlash(filepath.Join("shards", fmt.Sprintf("part-%06d.jsonl", index+1)))
		if err := os.WriteFile(filepath.Join(partitions, filepath.FromSlash(path)), raw, 0o600); err != nil {
			t.Fatal(err)
		}
		records := uint64(bytes.Count(raw, []byte{'\n'}))
		descriptors = append(descriptors, PartitionDescriptor{
			ArtifactDescriptor: ArtifactDescriptor{Path: path, SHA256: digest(raw), Bytes: uint64(len(raw)), Records: records},
			FirstRow:           first, LastRow: first + records - 1,
		})
		first += records
	}
	manifest := PartitionManifest{
		Version: PartitionManifestVersion, Format: PartitionFormat, Algorithm: PartitionAlgorithm,
		MaxRecordsPerPartition: indexpackadmission.MaxCollectorRecords,
		Input:                  ArtifactDescriptor{SHA256: digest(normalized), Bytes: uint64(len(normalized)), Records: uint64(len(candidates))},
		Partitions:             descriptors,
	}
	manifestRaw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(partitions, PartitionManifestFilename), manifestRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	collectionTime := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	bundles := make([]string, len(parts))
	for index, raw := range parts {
		bundles[index] = filepath.Join(root, "bundle-"+string(rune('1'+index)))
		if _, err := collectWithHooks(context.Background(), Options{
			ConfigRaw: []byte(collectorConfig), RightsEvidence: []byte("evidence"), Candidates: bytes.NewReader(raw),
			Observer: observer, CollectorVersion: "test-collector", UserAgentVersion: "1.0.0", OutputDir: bundles[index],
		}, collectHooksForTime(collectionTime)); err != nil {
			t.Fatal(err)
		}
	}
	return &mergeFixture{root: root, partitions: partitions, bundles: bundles, manifestSHA256: digest(manifestRaw), now: collectionTime.Add(mergeDelay)}
}

type rejectSecondObserver struct{}

func (rejectSecondObserver) Observe(ctx context.Context, row uint64, candidate ccindex.Candidate, rights indexpackselection.RightsDecision) (Result, error) {
	result, err := (fakeRowObserver{userAgentVersion: "1.0.0"}).Observe(ctx, row, candidate, rights)
	if err == nil && row == 2 {
		result.Observation.Outcome = "rejected"
		result.Observation.Reason = "indexing_not_permitted"
	}
	return result, err
}

func rewriteCandidateLines(t *testing.T, raw []byte, mutate func(*indexpackselection.Candidate)) []byte {
	t.Helper()
	var output bytes.Buffer
	for _, line := range bytes.Split(bytes.TrimSuffix(raw, []byte{'\n'}), []byte{'\n'}) {
		var candidate indexpackselection.Candidate
		if err := indexpack.DecodeStrictJSON(line, &candidate); err != nil {
			t.Fatal(err)
		}
		mutate(&candidate)
		encoded, err := json.Marshal(candidate)
		if err != nil {
			t.Fatal(err)
		}
		output.Write(encoded)
		output.WriteByte('\n')
	}
	return output.Bytes()
}

func rewriteObservationLines(t *testing.T, raw []byte, mutate func(*Observation)) []byte {
	t.Helper()
	var output bytes.Buffer
	for _, line := range bytes.Split(bytes.TrimSuffix(raw, []byte{'\n'}), []byte{'\n'}) {
		var observation Observation
		if err := indexpack.DecodeStrictJSON(line, &observation); err != nil {
			t.Fatal(err)
		}
		mutate(&observation)
		encoded, err := json.Marshal(observation)
		if err != nil {
			t.Fatal(err)
		}
		output.Write(encoded)
		output.WriteByte('\n')
	}
	return output.Bytes()
}

func mergeHooksForTime(now time.Time) mergeHooks {
	hooks := defaultMergeHooks()
	hooks.now = func() time.Time { return now }
	return hooks
}
