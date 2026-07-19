package packevidence

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/ccindex"
	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/indexpackadmission"
	"github.com/staticvar/fetchmark/internal/core/indexpackselection"
)

const (
	ReportVersion        = 1
	CandidatesFilename   = "candidates.jsonl"
	ObservationsFilename = "observations.jsonl"
	ReportFilename       = "report.json"
)

var ErrBundleCommitted = errors.New("pack evidence bundle committed but operation completion failed")

// CommittedError preserves recovery evidence when a caller encounters a
// failure after Collect has atomically activated the bundle, such as closing
// the input descriptor or serializing the command result.
func CommittedError(result CollectionResult, cause error) error {
	if cause == nil {
		return nil
	}
	return committedCollectError(result, cause)
}

type RowObserver interface {
	Observe(context.Context, uint64, ccindex.Candidate, indexpackselection.RightsDecision) (Result, error)
}

type Options struct {
	ConfigRaw        []byte
	RightsEvidence   []byte
	Candidates       io.Reader
	Observer         RowObserver
	CollectorVersion string
	UserAgentVersion string
	OutputDir        string
}

type ArtifactDescriptor struct {
	Path    string `json:"path,omitempty"`
	SHA256  string `json:"sha256"`
	Bytes   uint64 `json:"bytes"`
	Records uint64 `json:"records,omitempty"`
}

type Report struct {
	Version              int                  `json:"version"`
	Collector            string               `json:"collector"`
	CollectorVersion     string               `json:"collector_version"`
	StartedAt            string               `json:"started_at"`
	CompletedAt          string               `json:"completed_at"`
	ConfigSHA256         string               `json:"config_sha256"`
	RightsEvidenceSHA256 string               `json:"rights_evidence_sha256"`
	Input                ArtifactDescriptor   `json:"input"`
	Candidates           ArtifactDescriptor   `json:"candidates"`
	Observations         ArtifactDescriptor   `json:"observations"`
	Robots               []ArtifactDescriptor `json:"robots"`
	Outcomes             map[string]uint64    `json:"outcomes"`
	Rejections           map[string]uint64    `json:"rejections"`
}

type CollectionResult struct {
	Path               string `json:"path"`
	ReportSHA256       string `json:"report_sha256"`
	InputRecords       uint64 `json:"input_records"`
	AdmittedRecords    uint64 `json:"admitted_records"`
	ObservationRecords uint64 `json:"observation_records"`
	RobotsObjects      uint64 `json:"robots_objects"`
}

type collectHooks struct {
	now           func() time.Time
	activate      func(*os.Root, string, string) error
	checkParent   func(*os.Root, string) error
	syncParent    func(*os.Root) error
	removeStaging func(*os.Root, string) error
}

func defaultCollectHooks() collectHooks {
	return collectHooks{
		now: time.Now, activate: activateNoReplace, checkParent: rootStillAtPath,
		syncParent: syncRoot, removeStaging: func(root *os.Root, name string) error { return root.RemoveAll(name) },
	}
}

func Collect(ctx context.Context, options Options) (CollectionResult, error) {
	return collectWithHooks(ctx, options, defaultCollectHooks())
}

func collectWithHooks(ctx context.Context, options Options, hooks collectHooks) (result CollectionResult, returnErr error) {
	if ctx == nil || options.Candidates == nil || options.Observer == nil || !cleanAbsolute(options.OutputDir) || hooks.now == nil || hooks.activate == nil || hooks.checkParent == nil || hooks.syncParent == nil || hooks.removeStaging == nil {
		return CollectionResult{}, errors.New("pack evidence collector: context, inputs, observer, output, and hooks are required")
	}
	config, configDigest, err := indexpackadmission.DecodeConfig(options.ConfigRaw)
	if err != nil {
		return CollectionResult{}, err
	}
	if strings.TrimSpace(options.CollectorVersion) != options.CollectorVersion || options.CollectorVersion == "" || len(options.CollectorVersion) > 128 {
		return CollectionResult{}, errors.New("pack evidence collector: collector version is invalid")
	}
	expectedUserAgent, err := config.UserAgent(options.UserAgentVersion)
	if err != nil {
		return CollectionResult{}, fmt.Errorf("pack evidence collector: user-agent version: %w", err)
	}
	startedAt := hooks.now().UTC().Truncate(time.Second)
	rights, err := config.RightsDecision(options.RightsEvidence, startedAt)
	if err != nil {
		return CollectionResult{}, err
	}
	parentPath, err := secureconfigfile.ValidateDirectory(filepath.Dir(options.OutputDir))
	if err != nil {
		return CollectionResult{}, fmt.Errorf("pack evidence collector: validate output parent: %w", err)
	}
	outputBase := filepath.Base(options.OutputDir)
	parentRoot, err := os.OpenRoot(parentPath)
	if err != nil {
		return CollectionResult{}, fmt.Errorf("pack evidence collector: anchor output parent: %w", err)
	}
	committed := false
	defer func() {
		returnErr = joinCollectError(returnErr, parentRoot.Close(), committed, result)
	}()
	nameDigest := sha256.Sum256([]byte(outputBase))
	suffix := hex.EncodeToString(nameDigest[:])
	lockName := ".fetchmark-pack-evidence-lock-" + suffix
	lock, err := parentRoot.OpenFile(lockName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return CollectionResult{}, fmt.Errorf("pack evidence collector: claim output lock: %w", err)
	}
	if err := errors.Join(lock.Sync(), lock.Close()); err != nil {
		_ = parentRoot.Remove(lockName)
		return CollectionResult{}, fmt.Errorf("pack evidence collector: persist output lock: %w", err)
	}
	defer func() {
		removeErr := parentRoot.Remove(lockName)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		returnErr = joinCollectError(returnErr, errors.Join(removeErr, hooks.syncParent(parentRoot)), committed, result)
	}()
	if _, err := parentRoot.Lstat(outputBase); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return CollectionResult{}, errors.New("pack evidence collector: output already exists")
		}
		return CollectionResult{}, fmt.Errorf("pack evidence collector: inspect output: %w", err)
	}
	stagingName := ".fetchmark-pack-evidence-" + suffix
	if err := parentRoot.Mkdir(stagingName, 0o700); err != nil {
		return CollectionResult{}, fmt.Errorf("pack evidence collector: create private staging directory: %w", err)
	}
	stagingRoot, err := parentRoot.OpenRoot(stagingName)
	if err != nil {
		_ = parentRoot.RemoveAll(stagingName)
		return CollectionResult{}, fmt.Errorf("pack evidence collector: anchor staging directory: %w", err)
	}
	defer func() {
		returnErr = joinCollectError(returnErr, stagingRoot.Close(), committed, result)
	}()
	stagingCreated := true
	defer func() {
		if stagingCreated {
			returnErr = joinCollectError(returnErr, hooks.removeStaging(parentRoot, stagingName), committed, result)
		}
	}()
	for _, directory := range []string{"robots", filepath.Join("robots", "sha256")} {
		if err := stagingRoot.Mkdir(directory, 0o700); err != nil {
			return CollectionResult{}, fmt.Errorf("pack evidence collector: create %s: %w", directory, err)
		}
	}
	candidateFile, err := newArtifactWriter(stagingRoot, CandidatesFilename)
	if err != nil {
		return CollectionResult{}, err
	}
	observationFile, err := newArtifactWriter(stagingRoot, ObservationsFilename)
	if err != nil {
		_ = candidateFile.finish()
		return CollectionResult{}, err
	}
	artifacts, earliestValidUntil, err := collectRows(ctx, options.Candidates, options.Observer, rights, expectedUserAgent, startedAt, int(config.GlobalConcurrency), stagingRoot, candidateFile, observationFile)
	if err != nil {
		_ = candidateFile.abort()
		_ = observationFile.abort()
		return CollectionResult{}, err
	}
	if err := candidateFile.finish(); err != nil {
		_ = observationFile.abort()
		return CollectionResult{}, err
	}
	if err := observationFile.finish(); err != nil {
		return CollectionResult{}, err
	}
	completedAt := hooks.now().UTC().Truncate(time.Second)
	if !earliestValidUntil.IsZero() && !earliestValidUntil.After(completedAt.Add(indexpackselection.MinimumCompletionValidity)) {
		return CollectionResult{}, errors.New("pack evidence collector: admitted evidence expires before safe builder handoff")
	}
	rightsDigest := sha256.Sum256(options.RightsEvidence)
	report := Report{
		Version: ReportVersion, Collector: "fetchmark-pack-evidence", CollectorVersion: options.CollectorVersion,
		StartedAt: startedAt.Format(time.RFC3339), CompletedAt: completedAt.Format(time.RFC3339),
		ConfigSHA256: configDigest, RightsEvidenceSHA256: hex.EncodeToString(rightsDigest[:]),
		Input: artifacts.input, Candidates: candidateFile.descriptor(), Observations: observationFile.descriptor(),
		Robots: artifacts.robots, Outcomes: artifacts.outcomes, Rejections: artifacts.rejections,
	}
	reportRaw, err := json.Marshal(report)
	if err != nil {
		return CollectionResult{}, err
	}
	if err := writeExclusive(stagingRoot, ReportFilename, reportRaw); err != nil {
		return CollectionResult{}, err
	}
	result = CollectionResult{
		Path: options.OutputDir, ReportSHA256: digest(reportRaw), InputRecords: artifacts.input.Records,
		AdmittedRecords: report.Candidates.Records, ObservationRecords: report.Observations.Records, RobotsObjects: uint64(len(report.Robots)),
	}
	for _, directory := range []string{filepath.Join("robots", "sha256"), "robots", "."} {
		if err := syncDirectory(stagingRoot, directory); err != nil {
			return CollectionResult{}, err
		}
	}
	if err := hooks.checkParent(parentRoot, parentPath); err != nil {
		return CollectionResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return CollectionResult{}, err
	}
	if err := hooks.activate(parentRoot, stagingName, outputBase); err != nil {
		if errors.Is(err, os.ErrExist) {
			return CollectionResult{}, errors.New("pack evidence collector: output appeared during collection")
		}
		return CollectionResult{}, fmt.Errorf("pack evidence collector: activate bundle: %w", err)
	}
	committed = true
	stagingCreated = false
	if err := hooks.syncParent(parentRoot); err != nil {
		return result, committedCollectError(result, err)
	}
	if err := hooks.checkParent(parentRoot, parentPath); err != nil {
		return result, committedCollectError(result, err)
	}
	return result, nil
}

type rowJob struct {
	sequence  uint64
	lineSHA   string
	candidate ccindex.Candidate
}

type rowResult struct {
	job    rowJob
	result Result
	err    error
}

type collectionArtifacts struct {
	input      ArtifactDescriptor
	robots     []ArtifactDescriptor
	outcomes   map[string]uint64
	rejections map[string]uint64
}

func collectRows(ctx context.Context, input io.Reader, observer RowObserver, rights indexpackselection.RightsDecision, expectedUserAgent string, captureCutoff time.Time, concurrency int, root *os.Root, candidates, observations *artifactWriter) (collectionArtifacts, time.Time, error) {
	if concurrency < 1 || concurrency > indexpackadmission.MaxGlobalConcurrency {
		return collectionArtifacts{}, time.Time{}, errors.New("pack evidence collector: invalid worker concurrency")
	}
	workerContext, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan rowJob, concurrency)
	results := make(chan rowResult, concurrency)
	var workers sync.WaitGroup
	for index := 0; index < concurrency; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobs {
				result, err := observer.Observe(workerContext, job.sequence, job.candidate, rights)
				results <- rowResult{job: job, result: result, err: err}
			}
		}()
	}
	defer func() {
		close(jobs)
		workers.Wait()
	}()
	inputCounter := &hashCounter{hasher: sha256.New()}
	scanner := bufio.NewScanner(io.TeeReader(input, inputCounter))
	scanner.Buffer(make([]byte, 64<<10), ccindex.MaxNormalizedLineBytes+1)
	pending := make(map[uint64]rowResult, concurrency)
	objects := make(map[string]ArtifactDescriptor)
	outcomes := make(map[string]uint64)
	rejections := make(map[string]uint64)
	var dispatched, completed, expected uint64
	expected = 1
	scanDone := false
	inflight := 0
	var earliest time.Time
	for !scanDone || inflight > 0 {
		for !scanDone && inflight < concurrency {
			if !scanner.Scan() {
				scanDone = true
				if err := scanner.Err(); err != nil {
					return collectionArtifacts{}, time.Time{}, fmt.Errorf("pack evidence collector: scan normalized input: %w", err)
				}
				break
			}
			dispatched++
			if dispatched > indexpackadmission.MaxCollectorRecords {
				return collectionArtifacts{}, time.Time{}, fmt.Errorf("pack evidence collector: input exceeds %d rows", indexpackadmission.MaxCollectorRecords)
			}
			line := bytes.Clone(scanner.Bytes())
			candidate, err := ccindex.DecodeCandidate(line)
			if err != nil {
				return collectionArtifacts{}, time.Time{}, fmt.Errorf("pack evidence collector: row %d: %w", dispatched, err)
			}
			lineDigest := sha256.Sum256(line)
			select {
			case jobs <- rowJob{sequence: dispatched, lineSHA: hex.EncodeToString(lineDigest[:]), candidate: candidate}:
				inflight++
			case <-workerContext.Done():
				return collectionArtifacts{}, time.Time{}, workerContext.Err()
			}
		}
		if inflight == 0 {
			continue
		}
		var completedResult rowResult
		select {
		case completedResult = <-results:
		case <-workerContext.Done():
			return collectionArtifacts{}, time.Time{}, workerContext.Err()
		}
		inflight--
		if completedResult.err != nil {
			cancel()
			return collectionArtifacts{}, time.Time{}, fmt.Errorf("pack evidence collector: observe row %d: %w", completedResult.job.sequence, completedResult.err)
		}
		pending[completedResult.job.sequence] = completedResult
		for {
			ordered, ok := pending[expected]
			if !ok {
				break
			}
			delete(pending, expected)
			ordered.result.Observation.InputLineSHA256 = ordered.job.lineSHA
			if err := validateRowResult(ordered, expectedUserAgent, captureCutoff); err != nil {
				return collectionArtifacts{}, time.Time{}, err
			}
			if err := observations.writeJSON(ordered.result.Observation); err != nil {
				return collectionArtifacts{}, time.Time{}, err
			}
			outcomes[ordered.result.Observation.Outcome]++
			if ordered.result.Observation.Outcome == "admitted" {
				if err := candidates.writeJSON(ordered.result.Candidate); err != nil {
					return collectionArtifacts{}, time.Time{}, err
				}
				validUntil, err := earliestAdmissionExpiry(ordered.result.Candidate.Admission)
				if err != nil {
					return collectionArtifacts{}, time.Time{}, fmt.Errorf("pack evidence collector: row %d: %w", ordered.job.sequence, err)
				}
				if earliest.IsZero() || validUntil.Before(earliest) {
					earliest = validUntil
				}
			} else {
				rejections[ordered.result.Observation.Reason]++
			}
			if ordered.result.Observation.Robots.Status != 0 && ordered.result.PolicyBodyComplete {
				descriptor, err := writePolicyObject(root, ordered.result.Observation.Robots.BodySHA256, ordered.result.PolicyBody)
				if err != nil {
					return collectionArtifacts{}, time.Time{}, err
				}
				objects[descriptor.SHA256] = descriptor
			}
			completed++
			expected++
		}
	}
	if dispatched == 0 || completed != dispatched {
		return collectionArtifacts{}, time.Time{}, errors.New("pack evidence collector: normalized input is empty or incomplete")
	}
	robotObjects := make([]ArtifactDescriptor, 0, len(objects))
	for _, descriptor := range objects {
		robotObjects = append(robotObjects, descriptor)
	}
	sort.Slice(robotObjects, func(left, right int) bool { return robotObjects[left].Path < robotObjects[right].Path })
	return collectionArtifacts{
		input:  ArtifactDescriptor{SHA256: hex.EncodeToString(inputCounter.hasher.Sum(nil)), Bytes: inputCounter.bytes, Records: dispatched},
		robots: robotObjects, outcomes: outcomes, rejections: rejections,
	}, earliest, nil
}

func validateRowResult(result rowResult, expectedUserAgent string, captureCutoff time.Time) error {
	observation := result.result.Observation
	if observation.Version != ObservationVersion || observation.Row != result.job.sequence || observation.InputLineSHA256 != result.job.lineSHA ||
		(observation.Outcome != "admitted" && observation.Outcome != "rejected") ||
		(observation.Outcome == "rejected" && observation.Reason == "") || len(observation.Reason) > 128 {
		return fmt.Errorf("pack evidence collector: observer returned invalid row %d evidence", result.job.sequence)
	}
	if observation.Outcome == "admitted" && (!sameCandidateMetadata(result.result.Candidate, result.job.candidate) || result.result.Candidate.Admission.Robots.UserAgent != expectedUserAgent || result.result.Candidate.Admission.Robots.Outcome != "allowed" || !observation.Robots.BodyComplete || !result.result.PolicyBodyComplete || result.result.Candidate.Admission.Indexing.Outcome != "indexable" || result.result.Candidate.Admission.Rights.Outcome != "permitted") {
		return fmt.Errorf("pack evidence collector: observer returned non-buildable admitted row %d", result.job.sequence)
	}
	if observation.Outcome == "admitted" {
		preflight, reason, err := indexpackselection.PreflightCandidate(result.result.Candidate, captureCutoff)
		if err != nil {
			return fmt.Errorf("pack evidence collector: observer returned non-buildable admitted row %d: %w", result.job.sequence, err)
		}
		if reason != "" {
			return fmt.Errorf("pack evidence collector: observer returned non-buildable admitted row %d: %s", result.job.sequence, reason)
		}
		if preflight.CanonicalURL != result.result.Candidate.URL {
			return fmt.Errorf("pack evidence collector: observer returned noncanonical admitted row %d", result.job.sequence)
		}
	}
	return nil
}

func sameCandidateMetadata(actual indexpackselection.Candidate, expected ccindex.Candidate) bool {
	return actual.URLKey == expected.URLKey && actual.Timestamp == expected.Timestamp && actual.URL == expected.URL &&
		actual.MIME == expected.MIME && actual.MIMEDetected == expected.MIMEDetected && actual.Status == expected.Status &&
		actual.Digest == expected.Digest && actual.Length == expected.Length && actual.Offset == expected.Offset &&
		actual.Filename == expected.Filename && actual.Languages == expected.Languages && actual.Encoding == expected.Encoding
}

func earliestAdmissionExpiry(admission indexpackselection.Admission) (time.Time, error) {
	values := []string{admission.Robots.ValidUntil, admission.Indexing.ValidUntil, admission.Rights.ValidUntil}
	earliest, err := time.Parse(time.RFC3339, values[0])
	if err != nil || earliest.UTC().Format(time.RFC3339) != values[0] {
		return time.Time{}, errors.New("admission validity is not canonical RFC3339 UTC")
	}
	for _, raw := range values[1:] {
		value, err := time.Parse(time.RFC3339, raw)
		if err != nil || value.UTC().Format(time.RFC3339) != raw {
			return time.Time{}, errors.New("admission validity is not canonical RFC3339 UTC")
		}
		if value.Before(earliest) {
			earliest = value
		}
	}
	return earliest, nil
}

func writePolicyObject(root *os.Root, expectedDigest string, body []byte) (ArtifactDescriptor, error) {
	if len(expectedDigest) != sha256.Size*2 {
		return ArtifactDescriptor{}, errors.New("pack evidence collector: invalid robots object digest")
	}
	actual := digest(body)
	if actual != expectedDigest {
		return ArtifactDescriptor{}, errors.New("pack evidence collector: robots object digest mismatch")
	}
	relative := filepath.Join("robots", "sha256", expectedDigest)
	if _, err := root.Lstat(relative); err == nil {
		return ArtifactDescriptor{Path: filepath.ToSlash(relative), SHA256: actual, Bytes: uint64(len(body))}, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return ArtifactDescriptor{}, err
	}
	if err := writeExclusive(root, relative, body); err != nil {
		return ArtifactDescriptor{}, err
	}
	return ArtifactDescriptor{Path: filepath.ToSlash(relative), SHA256: actual, Bytes: uint64(len(body))}, nil
}

type artifactWriter struct {
	path    string
	file    *os.File
	hasher  hash.Hash
	bytes   uint64
	records uint64
	closed  bool
}

func newArtifactWriter(root *os.Root, path string) (*artifactWriter, error) {
	file, err := root.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("pack evidence collector: create %s: %w", path, err)
	}
	return &artifactWriter{path: path, file: file, hasher: sha256.New()}, nil
}

func (writer *artifactWriter) writeJSON(value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return writer.writeRaw(raw)
}

func (writer *artifactWriter) writeRaw(raw []byte) error {
	written, err := writer.file.Write(raw)
	if written > 0 {
		_, _ = writer.hasher.Write(raw[:written])
		writer.bytes += uint64(written)
	}
	if err == nil && written != len(raw) {
		err = io.ErrShortWrite
	}
	if err == nil {
		writer.records++
	}
	return err
}

func (writer *artifactWriter) finish() error {
	if writer.closed {
		return nil
	}
	writer.closed = true
	return errors.Join(writer.file.Sync(), writer.file.Close())
}

func (writer *artifactWriter) abort() error {
	if writer == nil || writer.closed {
		return nil
	}
	writer.closed = true
	return writer.file.Close()
}

func (writer *artifactWriter) descriptor() ArtifactDescriptor {
	return ArtifactDescriptor{Path: writer.path, SHA256: hex.EncodeToString(writer.hasher.Sum(nil)), Bytes: writer.bytes, Records: writer.records}
}

type hashCounter struct {
	hasher hash.Hash
	bytes  uint64
}

func (counter *hashCounter) Write(raw []byte) (int, error) {
	written, err := counter.hasher.Write(raw)
	counter.bytes += uint64(written)
	return written, err
}

func writeExclusive(root *os.Root, relative string, raw []byte) error {
	file, err := root.OpenFile(relative, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("pack evidence collector: create %s: %w", relative, err)
	}
	written, writeErr := io.Copy(file, bytes.NewReader(raw))
	if writeErr == nil && written != int64(len(raw)) {
		writeErr = io.ErrShortWrite
	}
	return errors.Join(writeErr, file.Sync(), file.Close())
}

func syncDirectory(root *os.Root, relative string) error {
	directory, err := root.Open(relative)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func syncRoot(root *os.Root) error { return syncDirectory(root, ".") }

func rootStillAtPath(root *os.Root, path string) error {
	anchored, err := root.Stat(".")
	if err != nil {
		return err
	}
	current, err := os.Lstat(path)
	if err != nil || !current.IsDir() || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(anchored, current) {
		return errors.New("pack evidence collector: output parent path changed during collection")
	}
	return nil
}

func activateNoReplace(root *os.Root, source, destination string) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	renameErr := renameNoReplaceAt(directory, source, destination)
	closeErr := directory.Close()
	if renameErr != nil {
		return errors.Join(renameErr, closeErr)
	}
	return nil
}

func cleanAbsolute(path string) bool {
	return path != "" && strings.TrimSpace(path) == path && filepath.IsAbs(path) && filepath.Clean(path) == path
}

func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func joinCollectError(existing, next error, committed bool, result CollectionResult) error {
	if next == nil {
		return existing
	}
	if committed && !errors.Is(next, ErrBundleCommitted) {
		next = committedCollectError(result, next)
	}
	return errors.Join(existing, next)
}

func committedCollectError(result CollectionResult, cause error) error {
	return fmt.Errorf("%w: output=%q report_sha256=%s: %w; inspect the existing bundle before retrying or removing it", ErrBundleCommitted, result.Path, result.ReportSHA256, cause)
}
