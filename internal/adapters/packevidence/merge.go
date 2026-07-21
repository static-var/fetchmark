package packevidence

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/ccindex"
	"github.com/staticvar/fetchmark/internal/adapters/robots"
	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/canonicalurl"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/indexpackadmission"
	"github.com/staticvar/fetchmark/internal/core/indexpackselection"
)

const (
	MergeReportVersion            = 1
	MergeAlgorithm                = "verified-partition-bundles-v1"
	MergeReportFilename           = "merge-report.json"
	MaxPartitionManifestBytes     = 1 << 20
	MaxCollectorReportBytes       = 4 << 20
	MaxMergeReportBytes           = 1 << 20
	MaxMergedObservationLineBytes = 512 << 10
	maxPartitionCount             = (MaxPartitionInputRecords + indexpackadmission.MaxCollectorRecords - 1) / indexpackadmission.MaxCollectorRecords
)

var ErrMergeCommitted = errors.New("pack evidence merge committed but operation completion failed")

type MergeOptions struct {
	PartitionsDir string
	BundleDirs    []string
	MergerVersion string
	OutputDir     string
}

type MergeSource struct {
	Partition     uint64             `json:"partition"`
	FirstRow      uint64             `json:"first_row"`
	LastRow       uint64             `json:"last_row"`
	Input         ArtifactDescriptor `json:"input"`
	ReportSHA256  string             `json:"report_sha256"`
	Candidates    ArtifactDescriptor `json:"candidates"`
	Observations  ArtifactDescriptor `json:"observations"`
	RobotsObjects uint64             `json:"robots_objects"`
	Outcomes      map[string]uint64  `json:"outcomes"`
	Rejections    map[string]uint64  `json:"rejections"`
}

type MergeRights struct {
	AllowedFields  []string `json:"allowed_fields"`
	Basis          string   `json:"basis"`
	EvidenceURI    string   `json:"evidence_uri"`
	EvidenceSHA256 string   `json:"evidence_sha256"`
	RightsNotice   string   `json:"rights_notice"`
}

type MergeReport struct {
	Version                 int                `json:"version"`
	Merger                  string             `json:"merger"`
	MergerVersion           string             `json:"merger_version"`
	Algorithm               string             `json:"algorithm"`
	PartitionManifestSHA256 string             `json:"partition_manifest_sha256"`
	StartedAt               string             `json:"started_at"`
	CompletedAt             string             `json:"completed_at"`
	ConfigSHA256            string             `json:"config_sha256"`
	RightsEvidenceSHA256    string             `json:"rights_evidence_sha256"`
	CollectorVersion        string             `json:"collector_version"`
	RobotsUserAgent         string             `json:"robots_user_agent,omitempty"`
	Rights                  *MergeRights       `json:"rights,omitempty"`
	PermissionValidUntil    string             `json:"permission_valid_until,omitempty"`
	Input                   ArtifactDescriptor `json:"input"`
	Candidates              ArtifactDescriptor `json:"candidates"`
	Sources                 []MergeSource      `json:"sources"`
	Outcomes                map[string]uint64  `json:"outcomes"`
	Rejections              map[string]uint64  `json:"rejections"`
}

type MergeResult struct {
	Path            string `json:"path"`
	ReportSHA256    string `json:"report_sha256"`
	CandidateSHA256 string `json:"candidate_sha256"`
	InputRecords    uint64 `json:"input_records"`
	AdmittedRecords uint64 `json:"admitted_records"`
	Bundles         uint64 `json:"bundles"`
}

type mergeHooks struct {
	now           func() time.Time
	activate      func(*os.Root, string, string) error
	checkParent   func(*os.Root, string) error
	syncParent    func(*os.Root) error
	removeStaging func(*os.Root, string) error
}

func defaultMergeHooks() mergeHooks {
	return mergeHooks{
		now: time.Now, activate: activateNoReplace, checkParent: rootStillAtPath, syncParent: syncRoot,
		removeStaging: func(root *os.Root, name string) error { return root.RemoveAll(name) },
	}
}

func Merge(ctx context.Context, options MergeOptions) (MergeResult, error) {
	return mergeWithHooks(ctx, options, defaultMergeHooks())
}

// DecodeMergeReport strictly verifies the portable aggregate evidence report
// carried by an evidence-bound open-pack build. The report binds exact source
// collector reports; it does not authenticate the workers that produced them.
func DecodeMergeReport(raw []byte) (MergeReport, string, error) {
	if len(raw) == 0 || len(raw) > MaxMergeReportBytes {
		return MergeReport{}, "", fmt.Errorf("pack evidence merger: report size must be 1..%d bytes", MaxMergeReportBytes)
	}
	var report MergeReport
	if err := indexpack.DecodeStrictJSON(raw, &report); err != nil {
		return MergeReport{}, "", fmt.Errorf("pack evidence merger: decode aggregate report: %w", err)
	}
	if err := validateMergeReport(report); err != nil {
		return MergeReport{}, "", err
	}
	return report, digest(raw), nil
}

func MergeCommittedError(result MergeResult, cause error) error {
	if cause == nil {
		return nil
	}
	return fmt.Errorf("%w: output=%q report_sha256=%s candidate_sha256=%s: %w; inspect the existing merge before retrying or removing it",
		ErrMergeCommitted, result.Path, result.ReportSHA256, result.CandidateSHA256, cause)
}

func mergeWithHooks(ctx context.Context, options MergeOptions, hooks mergeHooks) (result MergeResult, returnErr error) {
	if err := validateMergeOptions(ctx, options, hooks); err != nil {
		return MergeResult{}, err
	}
	parentPath, err := secureconfigfile.ValidateDirectory(filepath.Dir(options.OutputDir))
	if err != nil {
		return MergeResult{}, fmt.Errorf("pack evidence merger: validate output parent: %w", err)
	}
	parentRoot, err := os.OpenRoot(parentPath)
	if err != nil {
		return MergeResult{}, fmt.Errorf("pack evidence merger: anchor output parent: %w", err)
	}
	committed := false
	defer func() { returnErr = joinMergeError(returnErr, parentRoot.Close(), committed, result) }()
	outputBase := filepath.Base(options.OutputDir)
	nameDigest := sha256.Sum256([]byte(outputBase))
	suffix := hex.EncodeToString(nameDigest[:])
	lockName := ".fetchmark-pack-evidence-merge-lock-" + suffix
	lock, err := parentRoot.OpenFile(lockName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return MergeResult{}, fmt.Errorf("pack evidence merger: claim output lock: %w", err)
	}
	if err := errors.Join(lock.Sync(), lock.Close()); err != nil {
		_ = parentRoot.Remove(lockName)
		return MergeResult{}, fmt.Errorf("pack evidence merger: persist output lock: %w", err)
	}
	defer func() {
		removeErr := parentRoot.Remove(lockName)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		returnErr = joinMergeError(returnErr, errors.Join(removeErr, hooks.syncParent(parentRoot)), committed, result)
	}()
	if _, err := parentRoot.Lstat(outputBase); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return MergeResult{}, errors.New("pack evidence merger: output already exists")
		}
		return MergeResult{}, fmt.Errorf("pack evidence merger: inspect output: %w", err)
	}
	stagingName := ".fetchmark-pack-evidence-merge-" + suffix
	if err := parentRoot.Mkdir(stagingName, 0o700); err != nil {
		return MergeResult{}, fmt.Errorf("pack evidence merger: create staging directory: %w", err)
	}
	stagingCreated := true
	defer func() {
		if stagingCreated {
			returnErr = joinMergeError(returnErr, hooks.removeStaging(parentRoot, stagingName), committed, result)
		}
	}()
	stagingRoot, err := parentRoot.OpenRoot(stagingName)
	if err != nil {
		return MergeResult{}, fmt.Errorf("pack evidence merger: anchor staging directory: %w", err)
	}
	defer func() { returnErr = joinMergeError(returnErr, stagingRoot.Close(), committed, result) }()
	candidateFile, err := newArtifactWriter(stagingRoot, CandidatesFilename)
	if err != nil {
		return MergeResult{}, err
	}
	report, err := verifyMergeSources(ctx, options, hooks.now().UTC(), candidateFile)
	if err != nil {
		_ = candidateFile.abort()
		return MergeResult{}, err
	}
	if err := candidateFile.finish(); err != nil {
		return MergeResult{}, err
	}
	report.Candidates = candidateFile.descriptor()
	if err := validateMergeReport(report); err != nil {
		return MergeResult{}, err
	}
	reportRaw, err := json.Marshal(report)
	if err != nil {
		return MergeResult{}, fmt.Errorf("pack evidence merger: encode aggregate report: %w", err)
	}
	if len(reportRaw) == 0 || len(reportRaw) > MaxMergeReportBytes {
		return MergeResult{}, fmt.Errorf("pack evidence merger: aggregate report exceeds %d bytes", MaxMergeReportBytes)
	}
	if err := writeExclusive(stagingRoot, MergeReportFilename, reportRaw); err != nil {
		return MergeResult{}, err
	}
	if err := syncDirectory(stagingRoot, "."); err != nil {
		return MergeResult{}, err
	}

	// Re-open and re-hash every source artifact immediately before activation.
	// The second pass intentionally emits nowhere; any source drift invalidates
	// the already-staged candidate stream.
	revalidated, err := verifyMergeSources(ctx, options, hooks.now().UTC(), nil)
	if err != nil {
		return MergeResult{}, fmt.Errorf("pack evidence merger: final source revalidation: %w", err)
	}
	if !sameMergeReportEvidence(report, revalidated) {
		return MergeResult{}, errors.New("pack evidence merger: source evidence changed before activation")
	}
	if err := hooks.checkParent(parentRoot, parentPath); err != nil {
		return MergeResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return MergeResult{}, err
	}
	if err := hooks.activate(parentRoot, stagingName, outputBase); err != nil {
		if errors.Is(err, os.ErrExist) {
			return MergeResult{}, errors.New("pack evidence merger: output appeared during merge")
		}
		return MergeResult{}, fmt.Errorf("pack evidence merger: activate output: %w", err)
	}
	stagingCreated = false
	committed = true
	result = MergeResult{
		Path: options.OutputDir, ReportSHA256: digest(reportRaw), CandidateSHA256: report.Candidates.SHA256,
		InputRecords: report.Input.Records, AdmittedRecords: report.Candidates.Records, Bundles: uint64(len(report.Sources)),
	}
	if err := errors.Join(hooks.syncParent(parentRoot), hooks.checkParent(parentRoot, parentPath)); err != nil {
		return result, MergeCommittedError(result, err)
	}
	return result, nil
}

func joinMergeError(existing, next error, committed bool, result MergeResult) error {
	if next == nil {
		return existing
	}
	if committed && !errors.Is(next, ErrMergeCommitted) {
		next = MergeCommittedError(result, next)
	}
	return errors.Join(existing, next)
}

func validateMergeOptions(ctx context.Context, options MergeOptions, hooks mergeHooks) error {
	if ctx == nil || !cleanAbsolute(options.PartitionsDir) || !cleanAbsolute(options.OutputDir) ||
		strings.TrimSpace(options.MergerVersion) != options.MergerVersion || options.MergerVersion == "" || len(options.MergerVersion) > 128 ||
		len(options.BundleDirs) == 0 || len(options.BundleDirs) > int(maxPartitionCount) || hooks.now == nil || hooks.activate == nil ||
		hooks.checkParent == nil || hooks.syncParent == nil || hooks.removeStaging == nil {
		return errors.New("pack evidence merger: valid context, version, partition directory, bundles, output, and hooks are required")
	}
	seen := map[string]struct{}{options.PartitionsDir: {}}
	for _, bundle := range options.BundleDirs {
		if !cleanAbsolute(bundle) {
			return errors.New("pack evidence merger: bundle paths must be clean and absolute")
		}
		if _, duplicate := seen[bundle]; duplicate {
			return errors.New("pack evidence merger: duplicate input directory")
		}
		seen[bundle] = struct{}{}
	}
	for source := range seen {
		if sameOrDescendant(options.OutputDir, source) {
			return errors.New("pack evidence merger: output must not be inside an input directory")
		}
	}
	return nil
}

func sameOrDescendant(candidate, parent string) bool {
	relative, err := filepath.Rel(parent, candidate)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

type mergeBundle struct {
	path         string
	input        ArtifactDescriptor
	reportDigest string
}

type mergeCandidateSink interface {
	writeRaw([]byte) error
	descriptor() ArtifactDescriptor
}

type mergeCounter struct {
	hasher  hash.Hash
	bytes   uint64
	records uint64
}

func newMergeCounter() *mergeCounter { return &mergeCounter{hasher: sha256.New()} }
func (counter *mergeCounter) writeRaw(raw []byte) error {
	_, _ = counter.hasher.Write(raw)
	counter.bytes += uint64(len(raw))
	counter.records++
	return nil
}
func (counter *mergeCounter) descriptor() ArtifactDescriptor {
	return ArtifactDescriptor{Path: CandidatesFilename, SHA256: hex.EncodeToString(counter.hasher.Sum(nil)), Bytes: counter.bytes, Records: counter.records}
}

func verifyMergeSources(ctx context.Context, options MergeOptions, now time.Time, output mergeCandidateSink) (MergeReport, error) {
	if output == nil {
		output = newMergeCounter()
	}
	partitionPath, err := secureconfigfile.ValidateDirectory(options.PartitionsDir)
	if err != nil {
		return MergeReport{}, fmt.Errorf("pack evidence merger: validate partition directory: %w", err)
	}
	partitionRoot, err := os.OpenRoot(partitionPath)
	if err != nil {
		return MergeReport{}, err
	}
	defer partitionRoot.Close()
	manifestRaw, err := readMergeFile(partitionRoot, PartitionManifestFilename, MaxPartitionManifestBytes)
	if err != nil {
		return MergeReport{}, err
	}
	var manifest PartitionManifest
	if err := indexpack.DecodeStrictJSON(manifestRaw, &manifest); err != nil {
		return MergeReport{}, fmt.Errorf("pack evidence merger: decode partition manifest: %w", err)
	}
	if err := validatePartitionManifest(manifest); err != nil {
		return MergeReport{}, err
	}
	if len(options.BundleDirs) != len(manifest.Partitions) {
		return MergeReport{}, errors.New("pack evidence merger: exactly one bundle is required for every partition")
	}
	bundles := make(map[string]mergeBundle, len(options.BundleDirs))
	for _, bundlePath := range options.BundleDirs {
		validated, err := secureconfigfile.ValidateDirectory(bundlePath)
		if err != nil {
			return MergeReport{}, fmt.Errorf("pack evidence merger: validate bundle: %w", err)
		}
		root, err := os.OpenRoot(validated)
		if err != nil {
			return MergeReport{}, err
		}
		raw, readErr := readMergeFile(root, ReportFilename, MaxCollectorReportBytes)
		closeErr := root.Close()
		if readErr != nil || closeErr != nil {
			return MergeReport{}, errors.Join(readErr, closeErr)
		}
		var report Report
		if err := indexpack.DecodeStrictJSON(raw, &report); err != nil {
			return MergeReport{}, fmt.Errorf("pack evidence merger: decode collector report: %w", err)
		}
		if err := validateCollectorReport(report); err != nil {
			return MergeReport{}, err
		}
		key := artifactKey(report.Input)
		if _, duplicate := bundles[key]; duplicate {
			return MergeReport{}, errors.New("pack evidence merger: duplicate bundle input")
		}
		bundles[key] = mergeBundle{path: validated, input: report.Input, reportDigest: digest(raw)}
	}

	combinedInput := sha256.New()
	outcomes := make(map[string]uint64)
	rejections := make(map[string]uint64)
	sources := make([]MergeSource, 0, len(manifest.Partitions))
	var sharedConfig, sharedRights, sharedCollector, sharedUA string
	var rights *MergeRights
	var earliestPermission time.Time
	var started, completed time.Time
	seenHosts := make(map[string]struct{}, manifest.Input.Records)
	for index, partition := range manifest.Partitions {
		if err := ctx.Err(); err != nil {
			return MergeReport{}, err
		}
		bundle, found := bundles[artifactKey(partition.ArtifactDescriptor)]
		if !found {
			return MergeReport{}, fmt.Errorf("pack evidence merger: missing bundle for partition %d", index+1)
		}
		delete(bundles, artifactKey(partition.ArtifactDescriptor))
		bundleRoot, collectorReport, err := openMergeBundle(bundle)
		if err != nil {
			return MergeReport{}, err
		}
		if index == 0 {
			sharedConfig, sharedRights, sharedCollector = collectorReport.ConfigSHA256, collectorReport.RightsEvidenceSHA256, collectorReport.CollectorVersion
			started, _ = time.Parse(time.RFC3339, collectorReport.StartedAt)
			completed, _ = time.Parse(time.RFC3339, collectorReport.CompletedAt)
		} else if collectorReport.ConfigSHA256 != sharedConfig || collectorReport.RightsEvidenceSHA256 != sharedRights || collectorReport.CollectorVersion != sharedCollector {
			_ = bundleRoot.Close()
			return MergeReport{}, errors.New("pack evidence merger: collector identity, config, or rights evidence differs across bundles")
		}
		bundleStarted, _ := time.Parse(time.RFC3339, collectorReport.StartedAt)
		bundleCompleted, _ := time.Parse(time.RFC3339, collectorReport.CompletedAt)
		if bundleCompleted.After(now.UTC()) {
			_ = bundleRoot.Close()
			return MergeReport{}, errors.New("pack evidence merger: collector completion is in the future")
		}
		if bundleStarted.Before(started) {
			started = bundleStarted
		}
		if bundleCompleted.After(completed) {
			completed = bundleCompleted
		}
		partitionInput, err := openMergeExact(partitionRoot, partition.Path, partition.Bytes)
		if err != nil {
			_ = bundleRoot.Close()
			return MergeReport{}, err
		}
		verification, err := verifyCollectorBundle(ctx, bundleRoot, partitionInput, partition, collectorReport, output, combinedInput, now, seenHosts, &sharedUA, &rights, &earliestPermission)
		identityErr := rootStillAtPath(bundleRoot, bundle.path)
		closeErr := errors.Join(identityErr, partitionInput.Close(), bundleRoot.Close())
		if err != nil || closeErr != nil {
			return MergeReport{}, errors.Join(err, closeErr)
		}
		for key, value := range verification.outcomes {
			outcomes[key] += value
		}
		for key, value := range verification.rejections {
			rejections[key] += value
		}
		sources = append(sources, MergeSource{
			Partition: uint64(index + 1), FirstRow: partition.FirstRow, LastRow: partition.LastRow,
			Input: partition.ArtifactDescriptor, ReportSHA256: bundle.reportDigest,
			Candidates: collectorReport.Candidates, Observations: collectorReport.Observations, RobotsObjects: uint64(len(collectorReport.Robots)),
			Outcomes: cloneMergeCounts(verification.outcomes), Rejections: cloneMergeCounts(verification.rejections),
		})
	}
	if len(bundles) != 0 {
		return MergeReport{}, errors.New("pack evidence merger: extra bundle does not match a partition")
	}
	if hex.EncodeToString(combinedInput.Sum(nil)) != manifest.Input.SHA256 {
		return MergeReport{}, errors.New("pack evidence merger: ordered partitions do not reproduce the original input")
	}
	if !earliestPermission.IsZero() && !earliestPermission.After(now.UTC().Add(indexpackselection.MinimumCompletionValidity)) {
		return MergeReport{}, errors.New("pack evidence merger: admitted evidence expired before safe builder handoff")
	}
	permission := ""
	if !earliestPermission.IsZero() {
		permission = earliestPermission.UTC().Format(time.RFC3339)
	}
	report := MergeReport{
		Version: MergeReportVersion, Merger: "fetchmark-pack-evidence", MergerVersion: options.MergerVersion, Algorithm: MergeAlgorithm,
		PartitionManifestSHA256: digest(manifestRaw), StartedAt: started.UTC().Format(time.RFC3339), CompletedAt: completed.UTC().Format(time.RFC3339),
		ConfigSHA256: sharedConfig, RightsEvidenceSHA256: sharedRights, CollectorVersion: sharedCollector, RobotsUserAgent: sharedUA, Rights: rights,
		PermissionValidUntil: permission, Input: manifest.Input, Candidates: output.descriptor(), Sources: sources,
		Outcomes: outcomes, Rejections: rejections,
	}
	if err := rootStillAtPath(partitionRoot, partitionPath); err != nil {
		return MergeReport{}, err
	}
	return report, validateMergeReport(report)
}

func openMergeBundle(bundle mergeBundle) (*os.Root, Report, error) {
	root, err := os.OpenRoot(bundle.path)
	if err != nil {
		return nil, Report{}, err
	}
	raw, err := readMergeFile(root, ReportFilename, MaxCollectorReportBytes)
	if err != nil {
		_ = root.Close()
		return nil, Report{}, err
	}
	var report Report
	if err := indexpack.DecodeStrictJSON(raw, &report); err != nil {
		_ = root.Close()
		return nil, Report{}, fmt.Errorf("pack evidence merger: decode collector report: %w", err)
	}
	if err := validateCollectorReport(report); err != nil {
		_ = root.Close()
		return nil, Report{}, err
	}
	if digest(raw) != bundle.reportDigest || artifactKey(report.Input) != artifactKey(bundle.input) {
		_ = root.Close()
		return nil, Report{}, errors.New("pack evidence merger: collector report changed after bundle matching")
	}
	return root, report, nil
}

type verifiedCollector struct {
	outcomes   map[string]uint64
	rejections map[string]uint64
}

func verifyCollectorBundle(
	ctx context.Context,
	root *os.Root,
	input *os.File,
	partition PartitionDescriptor,
	report Report,
	output mergeCandidateSink,
	combinedInput io.Writer,
	now time.Time,
	seenHosts map[string]struct{},
	sharedUA *string,
	sharedRights **MergeRights,
	earliest *time.Time,
) (verifiedCollector, error) {
	candidates, err := openMergeExact(root, report.Candidates.Path, report.Candidates.Bytes)
	if err != nil {
		return verifiedCollector{}, err
	}
	defer candidates.Close()
	observations, err := openMergeExact(root, report.Observations.Path, report.Observations.Bytes)
	if err != nil {
		return verifiedCollector{}, err
	}
	defer observations.Close()
	inputCounter := &hashCounter{hasher: sha256.New()}
	candidateCounter := &hashCounter{hasher: sha256.New()}
	observationCounter := &hashCounter{hasher: sha256.New()}
	inputReader := bufio.NewReaderSize(io.TeeReader(io.TeeReader(input, inputCounter), combinedInput), ccindex.MaxNormalizedLineBytes+2)
	candidateReader := bufio.NewReaderSize(io.TeeReader(candidates, candidateCounter), indexpackselection.MaxCandidateBytes+2)
	observationReader := bufio.NewReaderSize(io.TeeReader(observations, observationCounter), MaxMergedObservationLineBytes+2)
	outcomes := make(map[string]uint64)
	rejections := make(map[string]uint64)
	referencedRobots := make(map[string]struct{})
	robotsByDigest, err := indexRobotsObjects(report.Robots)
	if err != nil {
		return verifiedCollector{}, err
	}
	startedAt, _ := time.Parse(time.RFC3339, report.StartedAt)
	completedAt, _ := time.Parse(time.RFC3339, report.CompletedAt)
	var candidateRecords uint64
	for row := uint64(1); row <= partition.Records; row++ {
		if err := ctx.Err(); err != nil {
			return verifiedCollector{}, err
		}
		_, inputLine, err := readMergeLine(inputReader, ccindex.MaxNormalizedLineBytes)
		if err != nil {
			return verifiedCollector{}, fmt.Errorf("pack evidence merger: partition row %d: %w", row, err)
		}
		normalized, err := ccindex.DecodeCandidate(inputLine)
		if err != nil {
			return verifiedCollector{}, fmt.Errorf("pack evidence merger: partition row %d: %w", row, err)
		}
		canonical, canonicalErr := canonicalurl.V1(normalized.URL)
		parsed, parseErr := url.Parse(normalized.URL)
		if canonicalErr != nil || canonical != normalized.URL || parseErr != nil || parsed.Hostname() == "" {
			return verifiedCollector{}, fmt.Errorf("pack evidence merger: partition row %d URL is not canonical", row)
		}
		host := strings.ToLower(parsed.Hostname())
		if _, duplicate := seenHosts[host]; duplicate {
			return verifiedCollector{}, fmt.Errorf("pack evidence merger: partition row %d repeats host %q", row, host)
		}
		seenHosts[host] = struct{}{}
		_, observationLine, err := readMergeLine(observationReader, MaxMergedObservationLineBytes)
		if err != nil {
			return verifiedCollector{}, fmt.Errorf("pack evidence merger: observations row %d: %w", row, err)
		}
		var observation Observation
		if err := indexpack.DecodeStrictJSON(observationLine, &observation); err != nil {
			return verifiedCollector{}, fmt.Errorf("pack evidence merger: observations row %d: %w", row, err)
		}
		lineDigest := sha256.Sum256(inputLine)
		if err := validateMergedObservation(observation, row, normalized.URL, hex.EncodeToString(lineDigest[:])); err != nil {
			return verifiedCollector{}, err
		}
		outcomes[observation.Outcome]++
		if observation.Outcome == "rejected" {
			rejections[observation.Reason]++
		} else {
			candidateRaw, candidateLine, err := readMergeLine(candidateReader, indexpackselection.MaxCandidateBytes)
			if err != nil {
				return verifiedCollector{}, fmt.Errorf("pack evidence merger: admitted row %d has no candidate: %w", row, err)
			}
			var candidate indexpackselection.Candidate
			if err := indexpack.DecodeStrictJSON(candidateLine, &candidate); err != nil {
				return verifiedCollector{}, fmt.Errorf("pack evidence merger: candidate for row %d: %w", row, err)
			}
			if !sameCandidateMetadata(candidate, normalized) {
				return verifiedCollector{}, fmt.Errorf("pack evidence merger: candidate metadata differs at row %d", row)
			}
			robotDescriptor, found := robotsByDigest[observation.Robots.BodySHA256]
			if !found {
				return verifiedCollector{}, fmt.Errorf("pack evidence merger: candidate admission row %d references an unknown robots object", row)
			}
			policyBody, err := readMergedRobotsPolicy(root, robotDescriptor)
			if err != nil {
				return verifiedCollector{}, fmt.Errorf("pack evidence merger: candidate admission row %d: %w", row, err)
			}
			if err := validateMergedRobotsEvidence(observation.Robots, candidate.Admission.Robots.UserAgent, observation.URL, policyBody); err != nil {
				return verifiedCollector{}, fmt.Errorf("pack evidence merger: candidate admission row %d: %w", row, err)
			}
			headersDigest, err := validateMergedIndexingEvidence(observation.Indexing, candidate.Admission.Robots.UserAgent)
			if err != nil || headersDigest != candidate.Admission.Indexing.HeadersSHA256 {
				return verifiedCollector{}, fmt.Errorf("pack evidence merger: candidate admission row %d has contradictory indexing evidence: %w", row, err)
			}
			preflight, reason, err := indexpackselection.PreflightCandidate(candidate, startedAt)
			if err != nil || reason != "" {
				return verifiedCollector{}, fmt.Errorf("pack evidence merger: candidate preflight failed at row %d: %s: %w", row, reason, err)
			}
			validUntil, err := validateMergedAdmission(candidate.Admission, observation, report.RightsEvidenceSHA256, preflight.CapturedAt, now.UTC(), sharedUA, sharedRights)
			if err != nil {
				return verifiedCollector{}, fmt.Errorf("pack evidence merger: candidate admission row %d: %w", row, err)
			}
			if !validUntil.After(now.UTC().Add(indexpackselection.MinimumCompletionValidity)) {
				return verifiedCollector{}, fmt.Errorf("pack evidence merger: admitted evidence expired at row %d", row)
			}
			if earliest.IsZero() || validUntil.Before(*earliest) {
				*earliest = validUntil
			}
			if !mergedObservationWithinCollection(candidate.Admission, startedAt, completedAt) {
				return verifiedCollector{}, fmt.Errorf("pack evidence merger: candidate admission row %d falls outside the collector interval", row)
			}
			if err := output.writeRaw(candidateRaw); err != nil {
				return verifiedCollector{}, err
			}
			candidateRecords++
		}
		if observation.Robots.Status != 0 && observation.Robots.BodyComplete {
			referencedRobots[observation.Robots.BodySHA256] = struct{}{}
		}
	}
	if _, _, err := readMergeLine(inputReader, ccindex.MaxNormalizedLineBytes); !errors.Is(err, io.EOF) {
		return verifiedCollector{}, errors.New("pack evidence merger: partition contains trailing rows")
	}
	if _, _, err := readMergeLine(observationReader, MaxMergedObservationLineBytes); !errors.Is(err, io.EOF) {
		return verifiedCollector{}, errors.New("pack evidence merger: observations contain trailing rows")
	}
	if _, _, err := readMergeLine(candidateReader, indexpackselection.MaxCandidateBytes); !errors.Is(err, io.EOF) {
		return verifiedCollector{}, errors.New("pack evidence merger: candidates contain trailing rows")
	}
	if err := verifyCounter(inputCounter, partition.ArtifactDescriptor); err != nil {
		return verifiedCollector{}, err
	}
	if err := verifyCounter(candidateCounter, report.Candidates); err != nil {
		return verifiedCollector{}, err
	}
	if candidateRecords != report.Candidates.Records {
		return verifiedCollector{}, errors.New("pack evidence merger: candidate record count differs from descriptor")
	}
	if err := verifyCounter(observationCounter, report.Observations); err != nil {
		return verifiedCollector{}, err
	}
	if !equalCounts(outcomes, report.Outcomes) || !equalCounts(rejections, report.Rejections) {
		return verifiedCollector{}, errors.New("pack evidence merger: collector outcome accounting differs from observations")
	}
	if err := verifyRobotsObjects(root, report.Robots, referencedRobots); err != nil {
		return verifiedCollector{}, err
	}
	return verifiedCollector{outcomes: outcomes, rejections: rejections}, nil
}

func validatePartitionManifest(manifest PartitionManifest) error {
	if manifest.Version != PartitionManifestVersion || manifest.Format != PartitionFormat || manifest.Algorithm != PartitionAlgorithm ||
		manifest.MaxRecordsPerPartition != indexpackadmission.MaxCollectorRecords || len(manifest.Partitions) == 0 || len(manifest.Partitions) > int(maxPartitionCount) ||
		manifest.Input.Path != "" || !validMergeDigest(manifest.Input.SHA256) || manifest.Input.Bytes == 0 || manifest.Input.Bytes > uint64(MaxPartitionInputBytes) ||
		manifest.Input.Records == 0 || manifest.Input.Records > MaxPartitionInputRecords {
		return errors.New("pack evidence merger: partition manifest header is invalid")
	}
	var rows, bytes uint64
	for index, partition := range manifest.Partitions {
		expectedPath := filepath.ToSlash(filepath.Join("shards", fmt.Sprintf("part-%06d.jsonl", index+1)))
		if partition.Path != expectedPath || !validMergeDigest(partition.SHA256) || partition.Bytes == 0 || partition.Records == 0 ||
			partition.Records > indexpackadmission.MaxCollectorRecords || partition.FirstRow != rows+1 ||
			partition.LastRow != partition.FirstRow+partition.Records-1 {
			return fmt.Errorf("pack evidence merger: partition descriptor %d is invalid", index+1)
		}
		if bytes > ^uint64(0)-partition.Bytes {
			return errors.New("pack evidence merger: partition byte count overflow")
		}
		rows += partition.Records
		bytes += partition.Bytes
	}
	if rows != manifest.Input.Records || bytes != manifest.Input.Bytes {
		return errors.New("pack evidence merger: partition totals differ from input descriptor")
	}
	return nil
}

func validateCollectorReport(report Report) error {
	started, startErr := time.Parse(time.RFC3339, report.StartedAt)
	completed, completeErr := time.Parse(time.RFC3339, report.CompletedAt)
	if report.Version != ReportVersion || report.Collector != "fetchmark-pack-evidence" ||
		strings.TrimSpace(report.CollectorVersion) != report.CollectorVersion || report.CollectorVersion == "" || len(report.CollectorVersion) > 128 ||
		startErr != nil || completeErr != nil || started.UTC().Format(time.RFC3339) != report.StartedAt || completed.UTC().Format(time.RFC3339) != report.CompletedAt || completed.Before(started) ||
		!validMergeDigest(report.ConfigSHA256) || !validMergeDigest(report.RightsEvidenceSHA256) ||
		report.Input.Path != "" || !validMergeDescriptor(report.Input, true) || report.Input.Records == 0 || report.Input.Records > indexpackadmission.MaxCollectorRecords ||
		report.Candidates.Path != CandidatesFilename || !validMergeDescriptor(report.Candidates, false) || report.Candidates.Records > report.Input.Records ||
		report.Observations.Path != ObservationsFilename || !validMergeDescriptor(report.Observations, true) || report.Observations.Records != report.Input.Records ||
		report.Outcomes == nil || report.Rejections == nil {
		return errors.New("pack evidence merger: collector report is invalid")
	}
	return nil
}

func validateMergedObservation(observation Observation, row uint64, rawURL, lineDigest string) error {
	if observation.Version != ObservationVersion || observation.Row != row || observation.InputLineSHA256 != lineDigest || observation.URL != rawURL ||
		(observation.Outcome != "admitted" && observation.Outcome != "rejected") ||
		(observation.Outcome == "admitted" && observation.Reason != "") ||
		(observation.Outcome == "rejected" && (observation.Reason == "" || len(observation.Reason) > 128)) {
		return fmt.Errorf("pack evidence merger: observation row %d is inconsistent with its input", row)
	}
	return nil
}

func validateMergedAdmission(admission indexpackselection.Admission, observation Observation, rightsDigest string, capturedAt, mergeTime time.Time, sharedUA *string, sharedRights **MergeRights) (time.Time, error) {
	page, pageErr := url.Parse(observation.URL)
	expectedRobots := ""
	if pageErr == nil {
		expectedRobots = page.Scheme + "://" + page.Host + "/robots.txt"
	}
	robotsWindow := validMergeWindow(admission.Robots.CheckedAt, admission.Robots.ValidUntil)
	indexingWindow := validMergeWindow(admission.Indexing.CheckedAt, admission.Indexing.ValidUntil)
	rightsWindow := validMergeWindow(admission.Rights.ObservedAt, admission.Rights.ValidUntil)
	robotsChecked, _ := time.Parse(time.RFC3339, admission.Robots.CheckedAt)
	indexingChecked, _ := time.Parse(time.RFC3339, admission.Indexing.CheckedAt)
	rightsObserved, _ := time.Parse(time.RFC3339, admission.Rights.ObservedAt)
	if admission.Version != indexpackselection.AdmissionVersion || strings.TrimSpace(admission.Robots.UserAgent) != admission.Robots.UserAgent || admission.Robots.UserAgent == "" || len(admission.Robots.UserAgent) > 256 ||
		pageErr != nil || admission.Robots.RobotsURI != expectedRobots || admission.Robots.RobotsURI != observation.Robots.RobotsURI ||
		admission.Robots.CheckedAt != observation.Robots.CheckedAt || admission.Robots.ValidUntil != observation.Robots.ValidUntil || admission.Robots.Outcome != observation.Robots.Outcome ||
		admission.Robots.BodySHA256 != observation.Robots.BodySHA256 || !validMergeDigest(admission.Robots.BodySHA256) || admission.Robots.Outcome != "allowed" || !observation.Robots.BodyComplete || !robotsWindow || robotsChecked.Before(capturedAt) || robotsChecked.After(mergeTime) ||
		admission.Indexing.CheckedAt != observation.Indexing.CheckedAt || admission.Indexing.ValidUntil != observation.Indexing.ValidUntil ||
		admission.Indexing.FinalURL != observation.Indexing.FinalURL || admission.Indexing.Outcome != observation.Indexing.Outcome ||
		admission.Indexing.HeadersSHA256 != observation.Indexing.HeadersSHA256 || admission.Indexing.RepresentationSHA256 != observation.Indexing.RepresentationSHA256 ||
		admission.Indexing.ParserVersion != observation.Indexing.ParserVersion || admission.Indexing.FinalURL != observation.URL || admission.Indexing.Outcome != "indexable" ||
		observation.Indexing.Status != 200 || !validMergeDigest(admission.Indexing.HeadersSHA256) || !validMergeDigest(admission.Indexing.RepresentationSHA256) ||
		admission.Indexing.ParserVersion != indexpackadmission.NoIndexParserVersion || !indexingWindow || indexingChecked.Before(capturedAt) || indexingChecked.After(mergeTime) ||
		admission.Rights.Outcome != "permitted" || len(admission.Rights.AllowedFields) != 1 || admission.Rights.AllowedFields[0] != "url_metadata" ||
		admission.Rights.EvidenceSHA256 != rightsDigest || !validMergeDigest(admission.Rights.EvidenceSHA256) || !validMergeRightsBasis(admission.Rights.Basis) ||
		!validMergeHTTPURL(admission.Rights.EvidenceURI) || strings.TrimSpace(admission.Rights.RightsNotice) != admission.Rights.RightsNotice || admission.Rights.RightsNotice == "" || len(admission.Rights.RightsNotice) > 2048 || !rightsWindow || rightsObserved.Before(capturedAt) || rightsObserved.After(mergeTime) {
		return time.Time{}, errors.New("admission does not match admitted observation evidence")
	}
	if *sharedUA == "" {
		*sharedUA = admission.Robots.UserAgent
	} else if *sharedUA != admission.Robots.UserAgent {
		return time.Time{}, errors.New("robots user agent differs across admitted rows")
	}
	semantic := &MergeRights{
		AllowedFields: append([]string(nil), admission.Rights.AllowedFields...), Basis: admission.Rights.Basis,
		EvidenceURI: admission.Rights.EvidenceURI, EvidenceSHA256: admission.Rights.EvidenceSHA256, RightsNotice: admission.Rights.RightsNotice,
	}
	if *sharedRights == nil {
		*sharedRights = semantic
	} else if !sameMergeRights(**sharedRights, *semantic) {
		return time.Time{}, errors.New("rights decision differs across admitted rows")
	}
	return earliestAdmissionExpiry(admission)
}

func validateMergedRobotsEvidence(evidence RobotsEvidence, userAgent, rawURL string, body []byte) error {
	bodyDigest := sha256.Sum256(body)
	finalURI, finalErr := canonicalurl.V1(evidence.FinalURI)
	if evidence.Outcome != "allowed" || !evidence.BodyComplete || evidence.FailureReason != "" || evidence.RetryAfterSeconds != 0 ||
		!validMergeDigest(evidence.BodySHA256) || evidence.BodySHA256 != hex.EncodeToString(bodyDigest[:]) ||
		finalErr != nil || finalURI != evidence.FinalURI {
		return errors.New("robots evidence is not a complete affirmative observation")
	}
	switch {
	case evidence.Status >= 200 && evidence.Status < 300:
		allowed, err := robots.EvaluatePolicy(body, userAgent, rawURL)
		if err != nil || !allowed {
			return errors.Join(errors.New("archived robots policy does not allow the candidate"), err)
		}
	case evidence.Status >= 400 && evidence.Status < 500 && evidence.Status != 429:
		if len(body) != 0 {
			return errors.New("robots unavailable response must bind the empty policy body")
		}
	default:
		return errors.New("robots status cannot authorize admission")
	}
	return nil
}

func validateMergedIndexingEvidence(evidence ResponseEvidence, userAgent string) (string, error) {
	if evidence.Outcome != "indexable" || evidence.FailureReason != "" || evidence.RedirectLocation != "" || evidence.RetryAfterSeconds != 0 ||
		evidence.CheckedAt == "" || evidence.ValidUntil == "" || !validMergeDigest(evidence.RepresentationSHA256) {
		return "", errors.New("indexing evidence is not a complete affirmative observation")
	}
	return indexpackadmission.ValidateDerivedIndexableEvidence(indexpackadmission.ResponseEvidence{
		Version: indexpackadmission.ResponseEvidenceVersion, Status: evidence.Status, FinalURL: evidence.FinalURL,
		ContentType: evidence.ContentType, XRobotsTag: append([]string(nil), evidence.XRobotsTag...),
		MetadataRobots: append([]string(nil), evidence.MetadataRobots...),
		Disposition:    "permitted", ParserVersion: evidence.ParserVersion,
	}, userAgent)
}

func mergedObservationWithinCollection(admission indexpackselection.Admission, startedAt, completedAt time.Time) bool {
	for _, raw := range []string{admission.Robots.CheckedAt, admission.Indexing.CheckedAt, admission.Rights.ObservedAt} {
		observed, err := time.Parse(time.RFC3339, raw)
		if err != nil || observed.Before(startedAt) || observed.After(completedAt) {
			return false
		}
	}
	return true
}

func validMergeWindow(observedRaw, validRaw string) bool {
	observed, observedErr := time.Parse(time.RFC3339, observedRaw)
	valid, validErr := time.Parse(time.RFC3339, validRaw)
	return observedErr == nil && validErr == nil && observed.UTC().Format(time.RFC3339) == observedRaw && valid.UTC().Format(time.RFC3339) == validRaw &&
		valid.After(observed) && valid.Sub(observed) <= indexpackadmission.MaxValidityHours*time.Hour
}

func validMergeRightsBasis(value string) bool {
	switch value {
	case "explicit_license", "publisher_permission", "public_domain", "url_metadata_policy":
		return true
	default:
		return false
	}
}

func validMergeHTTPURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.User == nil && parsed.Fragment == "" && parsed.RawPath == "" && parsed.RawQuery == "" && !parsed.ForceQuery
}

func indexRobotsObjects(descriptors []ArtifactDescriptor) (map[string]ArtifactDescriptor, error) {
	indexed := make(map[string]ArtifactDescriptor, len(descriptors))
	previous := ""
	for _, descriptor := range descriptors {
		expectedPath := filepath.ToSlash(filepath.Join("robots", "sha256", descriptor.SHA256))
		if descriptor.Path != expectedPath || descriptor.Path <= previous || !validMergeDescriptor(descriptor, false) || descriptor.Records != 0 || descriptor.Bytes > robotsPolicyBound() {
			return nil, errors.New("pack evidence merger: robots descriptor is invalid")
		}
		indexed[descriptor.SHA256] = descriptor
		previous = descriptor.Path
	}
	return indexed, nil
}

func readMergedRobotsPolicy(root *os.Root, descriptor ArtifactDescriptor) ([]byte, error) {
	file, err := openMergeExact(root, descriptor.Path, descriptor.Bytes)
	if err != nil {
		return nil, err
	}
	body, readErr := io.ReadAll(io.LimitReader(file, int64(robotsPolicyBound())+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || uint64(len(body)) != descriptor.Bytes || digest(body) != descriptor.SHA256 {
		return nil, errors.Join(errors.New("pack evidence merger: robots object digest mismatch"), readErr, closeErr)
	}
	return body, nil
}

func verifyRobotsObjects(root *os.Root, descriptors []ArtifactDescriptor, referenced map[string]struct{}) error {
	if len(descriptors) != len(referenced) {
		return errors.New("pack evidence merger: robots object set differs from observations")
	}
	indexed, err := indexRobotsObjects(descriptors)
	if err != nil {
		return err
	}
	for _, descriptor := range descriptors {
		if _, found := referenced[descriptor.SHA256]; !found {
			return errors.New("pack evidence merger: unreferenced robots object")
		}
		if _, err := readMergedRobotsPolicy(root, indexed[descriptor.SHA256]); err != nil {
			return err
		}
	}
	return nil
}

func robotsPolicyBound() uint64 { return 512 << 10 }

func validateMergeReport(report MergeReport) error {
	if report.Version != MergeReportVersion || report.Merger != "fetchmark-pack-evidence" || report.Algorithm != MergeAlgorithm ||
		strings.TrimSpace(report.MergerVersion) != report.MergerVersion || report.MergerVersion == "" || len(report.MergerVersion) > 128 ||
		!validMergeDigest(report.PartitionManifestSHA256) || !validMergeDigest(report.ConfigSHA256) || !validMergeDigest(report.RightsEvidenceSHA256) ||
		strings.TrimSpace(report.CollectorVersion) != report.CollectorVersion || report.CollectorVersion == "" || len(report.CollectorVersion) > 128 ||
		report.Input.Path != "" || !validMergeDescriptor(report.Input, true) || report.Input.Records > MaxPartitionInputRecords || report.Input.Bytes == 0 || report.Input.Bytes > uint64(MaxPartitionInputBytes) ||
		!validMergeDescriptor(report.Candidates, false) || report.Candidates.Path != CandidatesFilename || !validMergeStreamBytes(report.Candidates, indexpackselection.MaxCandidateBytes) ||
		len(report.Sources) == 0 || len(report.Sources) > int(maxPartitionCount) || report.Outcomes == nil || report.Rejections == nil {
		return errors.New("pack evidence merger: aggregate report is invalid")
	}
	started, startErr := time.Parse(time.RFC3339, report.StartedAt)
	completed, completeErr := time.Parse(time.RFC3339, report.CompletedAt)
	if startErr != nil || completeErr != nil || started.UTC().Format(time.RFC3339) != report.StartedAt || completed.UTC().Format(time.RFC3339) != report.CompletedAt || completed.Before(started) {
		return errors.New("pack evidence merger: aggregate report timestamps are invalid")
	}
	if report.Candidates.Records != report.Outcomes["admitted"] {
		return errors.New("pack evidence merger: aggregate admitted count differs from candidate stream")
	}
	if sumMergeCounts(report.Outcomes) != report.Input.Records || safeMergeSum(report.Outcomes["admitted"], report.Outcomes["rejected"]) != report.Input.Records ||
		sumMergeCounts(report.Rejections) != report.Outcomes["rejected"] || !validMergeOutcomes(report.Outcomes) || !validMergeRejections(report.Rejections) {
		return errors.New("pack evidence merger: aggregate outcome accounting is invalid")
	}
	if report.Candidates.Records > 0 {
		permission, permissionErr := time.Parse(time.RFC3339, report.PermissionValidUntil)
		if report.RobotsUserAgent == "" || len(report.RobotsUserAgent) > 256 || report.Rights == nil || permissionErr != nil ||
			permission.UTC().Format(time.RFC3339) != report.PermissionValidUntil || !permission.After(completed) ||
			len(report.Rights.AllowedFields) != 1 || report.Rights.AllowedFields[0] != "url_metadata" ||
			report.Rights.EvidenceSHA256 != report.RightsEvidenceSHA256 || !validMergeDigest(report.Rights.EvidenceSHA256) ||
			!validMergeRightsBasis(report.Rights.Basis) || !validMergeHTTPURL(report.Rights.EvidenceURI) ||
			strings.TrimSpace(report.Rights.RightsNotice) != report.Rights.RightsNotice || report.Rights.RightsNotice == "" || len(report.Rights.RightsNotice) > 2048 {
			return errors.New("pack evidence merger: aggregate permission identity is invalid")
		}
	} else if report.RobotsUserAgent != "" || report.Rights != nil || report.PermissionValidUntil != "" {
		return errors.New("pack evidence merger: empty candidate stream must not claim aggregate permission identity")
	}
	var inputs, admitted uint64
	var inputBytes, candidateBytes uint64
	sourceOutcomes := make(map[string]uint64)
	sourceRejections := make(map[string]uint64)
	for index, source := range report.Sources {
		expectedInputPath := filepath.ToSlash(filepath.Join("shards", fmt.Sprintf("part-%06d.jsonl", index+1)))
		expectedFirst, firstOK := addMergeCount(inputs, 1)
		expectedLast, lastOK := addMergeCount(source.FirstRow, source.Input.Records-1)
		if !firstOK || !lastOK || source.Partition != uint64(index+1) || source.FirstRow != expectedFirst || source.LastRow != expectedLast ||
			source.Input.Path != expectedInputPath || !validMergeDescriptor(source.Input, true) || source.Input.Records > indexpackadmission.MaxCollectorRecords ||
			source.Input.Bytes == 0 || !validMergeStreamBytes(source.Input, ccindex.MaxNormalizedLineBytes) || !validMergeDigest(source.ReportSHA256) ||
			source.Candidates.Path != CandidatesFilename || !validMergeDescriptor(source.Candidates, false) || source.Candidates.Records > source.Input.Records || !validMergeStreamBytes(source.Candidates, indexpackselection.MaxCandidateBytes) ||
			source.Observations.Path != ObservationsFilename || !validMergeDescriptor(source.Observations, true) || source.Observations.Records != source.Input.Records ||
			source.Observations.Bytes == 0 || !validMergeStreamBytes(source.Observations, MaxMergedObservationLineBytes) || source.RobotsObjects > source.Input.Records ||
			!validMergeOutcomes(source.Outcomes) || !validMergeRejections(source.Rejections) || sumMergeCounts(source.Outcomes) != source.Input.Records ||
			source.Outcomes["admitted"] != source.Candidates.Records || sumMergeCounts(source.Rejections) != source.Outcomes["rejected"] {
			return errors.New("pack evidence merger: aggregate source descriptor is invalid")
		}
		nextInputs, okInputs := addMergeCount(inputs, source.Input.Records)
		nextInputBytes, okInputBytes := addMergeCount(inputBytes, source.Input.Bytes)
		nextAdmitted, okAdmitted := addMergeCount(admitted, source.Candidates.Records)
		nextCandidateBytes, okCandidateBytes := addMergeCount(candidateBytes, source.Candidates.Bytes)
		if !okInputs || !okInputBytes || !okAdmitted || !okCandidateBytes {
			return errors.New("pack evidence merger: aggregate source descriptor overflows")
		}
		inputs, inputBytes, admitted, candidateBytes = nextInputs, nextInputBytes, nextAdmitted, nextCandidateBytes
		for key, value := range source.Outcomes {
			sourceOutcomes[key] += value
		}
		for key, value := range source.Rejections {
			sourceRejections[key] += value
		}
	}
	if inputs != report.Input.Records || inputBytes != report.Input.Bytes || admitted != report.Candidates.Records || candidateBytes != report.Candidates.Bytes ||
		!equalCounts(sourceOutcomes, report.Outcomes) || !equalCounts(sourceRejections, report.Rejections) {
		return errors.New("pack evidence merger: aggregate source rows differ from input")
	}
	return nil
}

func readMergeFile(root *os.Root, relative string, maximum int) ([]byte, error) {
	file, err := openMergeBounded(root, relative, uint64(maximum))
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, int64(maximum)+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}
	if len(raw) == 0 || len(raw) > maximum {
		return nil, fmt.Errorf("pack evidence merger: %s has invalid size", relative)
	}
	return raw, nil
}

func openMergeBounded(root *os.Root, relative string, maximum uint64) (*os.File, error) {
	if !cleanMergeRelative(relative) {
		return nil, errors.New("pack evidence merger: artifact path is unsafe")
	}
	before, err := root.Lstat(relative)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || uint64(before.Size()) > maximum {
		return nil, errors.Join(fmt.Errorf("pack evidence merger: %s is not a bounded regular file", relative), err)
	}
	file, err := root.Open(relative)
	if err != nil {
		return nil, err
	}
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) || uint64(opened.Size()) > maximum {
		_ = file.Close()
		return nil, errors.New("pack evidence merger: artifact changed while opening")
	}
	return file, nil
}

func openMergeExact(root *os.Root, relative string, exact uint64) (*os.File, error) {
	file, err := openMergeBounded(root, relative, exact)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || uint64(info.Size()) != exact {
		_ = file.Close()
		return nil, errors.New("pack evidence merger: artifact size differs from descriptor")
	}
	return file, nil
}

func cleanMergeRelative(value string) bool {
	return value != "" && value == filepath.ToSlash(filepath.Clean(filepath.FromSlash(value))) && !filepath.IsAbs(filepath.FromSlash(value)) && value != "." && !strings.HasPrefix(value, "../")
}

func readMergeLine(reader *bufio.Reader, maximum int) ([]byte, []byte, error) {
	raw, err := reader.ReadSlice('\n')
	if errors.Is(err, io.EOF) && len(raw) == 0 {
		return nil, nil, io.EOF
	}
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return nil, nil, fmt.Errorf("line exceeds %d bytes", maximum)
		}
		if errors.Is(err, io.EOF) {
			return nil, nil, errors.New("line is not newline terminated")
		}
		return nil, nil, err
	}
	line := raw[:len(raw)-1]
	if len(line) == 0 || len(line) > maximum || line[len(line)-1] == '\r' {
		return nil, nil, errors.New("line framing is invalid")
	}
	return raw, line, nil
}

func verifyCounter(counter *hashCounter, descriptor ArtifactDescriptor) error {
	if counter.bytes != descriptor.Bytes || hex.EncodeToString(counter.hasher.Sum(nil)) != descriptor.SHA256 {
		return fmt.Errorf("pack evidence merger: %s digest or byte count differs from descriptor", descriptor.Path)
	}
	return nil
}

func validMergeDescriptor(descriptor ArtifactDescriptor, recordsRequired bool) bool {
	return validMergeDigest(descriptor.SHA256) && (!recordsRequired || descriptor.Records > 0)
}

func validMergeStreamBytes(descriptor ArtifactDescriptor, maximumLineBytes int) bool {
	if maximumLineBytes <= 0 {
		return false
	}
	if descriptor.Records == 0 {
		return descriptor.Bytes == 0
	}
	maximum, ok := multiplyMergeCount(descriptor.Records, uint64(maximumLineBytes)+1)
	return ok && descriptor.Bytes > 0 && descriptor.Bytes <= maximum
}

func validMergeDigest(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func artifactKey(descriptor ArtifactDescriptor) string {
	return fmt.Sprintf("%s/%d/%d", descriptor.SHA256, descriptor.Bytes, descriptor.Records)
}

func equalCounts(left, right map[string]uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func cloneMergeCounts(values map[string]uint64) map[string]uint64 {
	cloned := make(map[string]uint64, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func sumMergeCounts(values map[string]uint64) uint64 {
	var total uint64
	for _, value := range values {
		if total > ^uint64(0)-value {
			return ^uint64(0)
		}
		total += value
	}
	return total
}

func validMergeCounts(values map[string]uint64) bool {
	for key, value := range values {
		if strings.TrimSpace(key) != key || key == "" || len(key) > 128 || value == 0 {
			return false
		}
	}
	return true
}

func validMergeOutcomes(values map[string]uint64) bool {
	if !validMergeCounts(values) {
		return false
	}
	for key := range values {
		if key != "admitted" && key != "rejected" {
			return false
		}
	}
	return true
}

func validMergeRejections(values map[string]uint64) bool {
	if !validMergeCounts(values) {
		return false
	}
	for key := range values {
		switch key {
		case "invalid_url", "non_public_url", "noncanonical_url", "query_not_allowed", "invalid_capture_metadata",
			"status_not_200", "mime_not_html", "capture_after_build", "language_invalid", "robots_not_allowed",
			"indexing_fetch_failed", "indexing_evaluation_failed", "indexing_redirect_mismatch", "indexing_not_permitted":
		default:
			return false
		}
	}
	return true
}

func addMergeCount(left, right uint64) (uint64, bool) {
	if left > ^uint64(0)-right {
		return 0, false
	}
	return left + right, true
}

func multiplyMergeCount(left, right uint64) (uint64, bool) {
	if left != 0 && right > ^uint64(0)/left {
		return 0, false
	}
	return left * right, true
}

func safeMergeSum(left, right uint64) uint64 {
	total, ok := addMergeCount(left, right)
	if !ok {
		return ^uint64(0)
	}
	return total
}

func sameMergeRights(left, right MergeRights) bool {
	return len(left.AllowedFields) == 1 && len(right.AllowedFields) == 1 && left.AllowedFields[0] == right.AllowedFields[0] &&
		left.Basis == right.Basis && left.EvidenceURI == right.EvidenceURI && left.EvidenceSHA256 == right.EvidenceSHA256 && left.RightsNotice == right.RightsNotice
}

func sameMergeReportEvidence(left, right MergeReport) bool {
	leftRaw, leftErr := json.Marshal(left)
	rightRaw, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftRaw) == string(rightRaw)
}
