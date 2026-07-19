// Package openpackbuilder owns the offline publisher-side filesystem and
// compression boundary for deterministic signed open-index packs. Runtime
// Fetchmark binaries do not import this package.
package openpackbuilder

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
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
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/staticvar/fetchmark/internal/adapters/openpackindex"
	"github.com/staticvar/fetchmark/internal/adapters/packevidence"
	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/indexpackselection"
)

const (
	ManifestFilename                          = "manifest.json"
	SignatureFilename                         = "manifest.ed25519"
	BuildReportFilename                       = "build-report.json"
	ReportSignatureFilename                   = "build-report.ed25519"
	EvidenceReportFilename                    = "evidence-report.json"
	BuildReportVersion                        = 2
	BuildReportSignatureDomain                = "fetchmark-open-index-pack-build-report-v2\n"
	EvidenceBuildReportVersion                = 3
	EvidenceBuildReportSignatureDomain        = "fetchmark-open-index-pack-build-report-v3\n"
	maxShardRecords                    uint64 = 100_000
	maxShardUncompressedBytes          uint64 = 64 << 20
)

type Options struct {
	SpecRaw           []byte
	ExclusionsRaw     []byte
	Candidates        io.Reader
	EvidenceReportRaw []byte
	Identity          Identity
	BuilderVersion    string
	OutputDir         string
}

type Result struct {
	Path                string `json:"path"`
	PackID              string `json:"pack_id"`
	Revision            uint64 `json:"revision"`
	ManifestSHA256      string `json:"manifest_sha256"`
	BuildReportSHA256   string `json:"build_report_sha256"`
	RecordCount         uint64 `json:"record_count"`
	ShardCount          int    `json:"shard_count"`
	OrdinaryInstallable bool   `json:"ordinary_installable"`
	ProjectionBytes     uint64 `json:"projection_bytes,omitempty"`
}

type ShardEvidence struct {
	Path                  string `json:"path"`
	RecordCount           uint64 `json:"record_count"`
	CompressedSizeBytes   uint64 `json:"compressed_size_bytes"`
	UncompressedSizeBytes uint64 `json:"uncompressed_size_bytes"`
	SHA256                string `json:"sha256"`
	UncompressedSHA256    string `json:"uncompressed_sha256"`
}

type BuildReport struct {
	Version              int                       `json:"version"`
	Builder              string                    `json:"builder"`
	BuilderVersion       string                    `json:"builder_version"`
	Profile              string                    `json:"profile"`
	OrdinaryInstallable  bool                      `json:"ordinary_installable"`
	PublisherID          string                    `json:"publisher_id"`
	KeyID                string                    `json:"key_id"`
	ManifestSHA256       string                    `json:"manifest_sha256"`
	PackID               string                    `json:"pack_id"`
	Revision             uint64                    `json:"revision"`
	CreatedAt            string                    `json:"created_at"`
	Selection            indexpackselection.Report `json:"selection"`
	Shards               []ShardEvidence           `json:"shards"`
	SelectedStreamSHA256 string                    `json:"selected_stream_sha256"`
	EvidenceReportSHA256 string                    `json:"evidence_report_sha256,omitempty"`
}

func Build(ctx context.Context, options Options) (result Result, returnErr error) {
	return build(ctx, options, time.Now)
}

func build(ctx context.Context, options Options, now func() time.Time) (result Result, returnErr error) {
	if ctx == nil || options.Candidates == nil {
		return Result{}, errors.New("open pack builder: context and candidates are required")
	}
	if now == nil {
		return Result{}, errors.New("open pack builder: clock is required")
	}
	if options.Identity.publisherID == "" || options.Identity.keyID == "" || len(options.Identity.privateKey) != ed25519.PrivateKeySize {
		return Result{}, errors.New("open pack builder: valid publisher identity is required")
	}
	if strings.TrimSpace(options.BuilderVersion) != options.BuilderVersion || options.BuilderVersion == "" || len(options.BuilderVersion) > 128 {
		return Result{}, errors.New("open pack builder: builder version is required")
	}
	if !cleanAbsolute(options.OutputDir) {
		return Result{}, errors.New("open pack builder: output must be a clean absolute path")
	}
	specRaw := bytes.Clone(options.SpecRaw)
	exclusionsRaw := bytes.Clone(options.ExclusionsRaw)
	evidenceRaw := bytes.Clone(options.EvidenceReportRaw)
	spec, _, err := indexpackselection.DecodeSpec(specRaw)
	if err != nil {
		return Result{}, err
	}
	if _, _, err := indexpackselection.DecodeExclusions(exclusionsRaw); err != nil {
		return Result{}, err
	}
	var evidenceReport packevidence.MergeReport
	var evidenceDigest string
	if len(evidenceRaw) != 0 {
		evidenceReport, evidenceDigest, err = packevidence.DecodeMergeReport(evidenceRaw)
		if err != nil {
			return Result{}, fmt.Errorf("open pack builder: evidence report: %w", err)
		}
		if evidenceReport.Candidates.SHA256 != spec.CandidateSHA256 {
			return Result{}, errors.New("open pack builder: evidence report candidate digest differs from spec")
		}
	}
	outputBase := filepath.Base(options.OutputDir)
	if outputBase == "." || outputBase == string(filepath.Separator) {
		return Result{}, errors.New("open pack builder: output must name a new bundle directory")
	}
	parentPath, err := secureconfigfile.ValidateDirectory(filepath.Dir(options.OutputDir))
	if err != nil {
		return Result{}, fmt.Errorf("open pack builder: validate output parent: %w", err)
	}
	outputDir := filepath.Join(parentPath, outputBase)
	parentRoot, err := os.OpenRoot(parentPath)
	if err != nil {
		return Result{}, fmt.Errorf("open pack builder: anchor output parent: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, parentRoot.Close())
	}()
	nameDigest := sha256.Sum256([]byte(outputBase))
	nameSuffix := hex.EncodeToString(nameDigest[:])
	lockName := ".fetchmark-pack-lock-" + nameSuffix
	lock, err := parentRoot.OpenFile(lockName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return Result{}, fmt.Errorf("open pack builder: claim output lock without overwrite: %w", err)
	}
	if err := errors.Join(lock.Sync(), lock.Close()); err != nil {
		_ = parentRoot.Remove(lockName)
		return Result{}, fmt.Errorf("open pack builder: persist output lock: %w", err)
	}
	defer func() {
		if err := parentRoot.Remove(lockName); err != nil && !errors.Is(err, os.ErrNotExist) {
			returnErr = errors.Join(returnErr, fmt.Errorf("open pack builder: remove output lock: %w", err))
		}
		returnErr = errors.Join(returnErr, syncRoot(parentRoot))
	}()
	if _, err := parentRoot.Lstat(outputBase); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return Result{}, errors.New("open pack builder: output already exists")
		}
		return Result{}, fmt.Errorf("open pack builder: inspect output: %w", err)
	}
	stagingName := ".fetchmark-pack-build-" + nameSuffix
	if err := parentRoot.Mkdir(stagingName, 0o700); err != nil {
		return Result{}, fmt.Errorf("open pack builder: create private staging directory: %w", err)
	}
	stagingRoot, err := parentRoot.OpenRoot(stagingName)
	if err != nil {
		_ = parentRoot.RemoveAll(stagingName)
		return Result{}, fmt.Errorf("open pack builder: anchor private staging directory: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, stagingRoot.Close())
	}()
	stagingCreated := true
	defer func() {
		if stagingCreated {
			if err := removeStaging(parentRoot, stagingName); err != nil {
				returnErr = errors.Join(returnErr, fmt.Errorf("open pack builder: remove incomplete staging directory: %w", err))
			}
		}
	}()
	writer := newShardWriter(ctx, stagingRoot, indexpack.KindSnapshot)
	defer writer.abort()
	candidateInput := options.Candidates
	var rightsSummary *candidateRightsSummary
	if len(evidenceRaw) != 0 {
		rightsSummary = &candidateRightsSummary{}
		candidateInput = io.TeeReader(candidateInput, rightsSummary)
	}
	selection, err := indexpackselection.Select(ctx, now().UTC(), specRaw, exclusionsRaw, candidateInput, writer.add)
	if err != nil {
		return Result{}, err
	}
	if rightsSummary != nil {
		if err := rightsSummary.finish(); err != nil {
			return Result{}, err
		}
	}
	if err := writer.finish(); err != nil {
		return Result{}, err
	}
	if err := indexpackselection.ValidateCompletion(selection, now().UTC()); err != nil {
		return Result{}, err
	}
	if len(evidenceRaw) != 0 {
		if err := validateEvidenceSelection(evidenceReport, selection, spec, rightsSummary, now().UTC()); err != nil {
			return Result{}, err
		}
	}
	if spec.Profile == indexpackselection.ProfileLightweight && len(writer.descriptors) != 1 {
		return Result{}, errors.New("open pack builder: lightweight profile must produce exactly one shard")
	}

	languages := make([]string, 0, len(selection.AcceptedByLanguage))
	for language := range selection.AcceptedByLanguage {
		languages = append(languages, language)
	}
	sort.Strings(languages)
	descriptors := append([]indexpack.Shard(nil), writer.descriptors...)
	sort.Slice(descriptors, func(left, right int) bool { return descriptors[left].Path < descriptors[right].Path })
	manifest := indexpack.Manifest{
		Version: indexpack.VersionURLMetadata, Kind: indexpack.KindSnapshot,
		PackID: spec.PackID, Revision: spec.Revision, CreatedAt: spec.CreatedAt, ExpiresAt: spec.ExpiresAt,
		SigningKeyID: options.Identity.keyID, Publisher: spec.Publisher,
		Policy:    indexpack.Policy{Robots: "rfc9309", NoIndex: "exclude", BodyDistribution: "forbidden"},
		Languages: languages, RecordCount: selection.Accepted, Shards: descriptors,
		Build: indexpack.Build{
			Generator: "fetchmark-pack-build", GeneratorVersion: options.BuilderVersion, Analyzer: "url-derived-lexical", AnalyzerVersion: "1",
			PolicySHA256: selection.PolicySHA256, ExclusionsSHA256: selection.ExclusionsSHA256,
			CandidateSHA256: selection.CandidateSHA256, Inputs: spec.Inputs,
		},
	}
	manifestRaw, err := indexpack.EncodeManifest(manifest)
	if err != nil {
		return Result{}, fmt.Errorf("open pack builder: encode manifest: %w", err)
	}
	manifestSignature, err := indexpack.SignManifest(manifestRaw, options.Identity.privateKey)
	if err != nil {
		return Result{}, fmt.Errorf("open pack builder: sign manifest: %w", err)
	}
	manifestSignatureRaw, err := indexpack.EncodeSignature(manifestSignature)
	if err != nil {
		return Result{}, err
	}
	if _, err := indexpack.VerifyManifest(manifestRaw, manifestSignatureRaw, map[string]ed25519.PublicKey{options.Identity.keyID: options.Identity.PublicKey()}); err != nil {
		return Result{}, fmt.Errorf("open pack builder: self-verify manifest signature: %w", err)
	}
	if err := writeExclusive(stagingRoot, ManifestFilename, manifestRaw); err != nil {
		return Result{}, err
	}
	if err := writeExclusive(stagingRoot, SignatureFilename, manifestSignatureRaw); err != nil {
		return Result{}, err
	}
	manifestDigest := indexpack.ManifestDigest(manifestRaw)
	ordinary := false
	var projectionBytes uint64
	if spec.Profile == indexpackselection.ProfileLightweight {
		projectionBytes, err = verifyOrdinaryInstall(ctx, stagingRoot, manifest, manifestDigest, options.Identity.PublicKey())
		if err != nil {
			return Result{}, fmt.Errorf("open pack builder: lightweight consumer gate: %w", err)
		}
		ordinary = true
	}
	if err := indexpackselection.ValidateCompletion(selection, now().UTC()); err != nil {
		return Result{}, err
	}
	reportVersion := BuildReportVersion
	if len(evidenceRaw) != 0 {
		reportVersion = EvidenceBuildReportVersion
		if err := writeExclusive(stagingRoot, EvidenceReportFilename, evidenceRaw); err != nil {
			return Result{}, err
		}
	}
	report := BuildReport{
		Version: reportVersion, Builder: "fetchmark-pack-build", BuilderVersion: options.BuilderVersion,
		Profile: spec.Profile, OrdinaryInstallable: ordinary, PublisherID: options.Identity.publisherID,
		KeyID: options.Identity.keyID, ManifestSHA256: manifestDigest, PackID: spec.PackID,
		Revision: spec.Revision, CreatedAt: spec.CreatedAt, Selection: selection,
		Shards: writer.evidence, SelectedStreamSHA256: hex.EncodeToString(writer.selectedHasher.Sum(nil)),
		EvidenceReportSHA256: evidenceDigest,
	}
	if err := report.validate(); err != nil {
		return Result{}, err
	}
	reportRaw, err := json.Marshal(report)
	if err != nil {
		return Result{}, err
	}
	reportSignatureRaw, err := signReport(reportRaw, options.Identity)
	if err != nil {
		return Result{}, err
	}
	if err := VerifyBuildReport(reportRaw, reportSignatureRaw, options.Identity.PublicKey()); err != nil {
		return Result{}, fmt.Errorf("open pack builder: self-verify build report signature: %w", err)
	}
	if err := writeExclusive(stagingRoot, BuildReportFilename, reportRaw); err != nil {
		return Result{}, err
	}
	if err := writeExclusive(stagingRoot, ReportSignatureFilename, reportSignatureRaw); err != nil {
		return Result{}, err
	}
	if err := syncBundleDirectories(stagingRoot); err != nil {
		return Result{}, err
	}
	if err := indexpackselection.ValidateCompletion(selection, now().UTC()); err != nil {
		return Result{}, err
	}
	if len(evidenceRaw) != 0 {
		if err := validateEvidenceSelection(evidenceReport, selection, spec, rightsSummary, now().UTC()); err != nil {
			return Result{}, err
		}
	}
	if err := rootStillAtPath(parentRoot, parentPath); err != nil {
		return Result{}, err
	}
	if err := activateNoReplace(parentRoot, stagingName, outputBase); err != nil {
		if errors.Is(err, os.ErrExist) {
			return Result{}, errors.New("open pack builder: output appeared during build")
		}
		return Result{}, fmt.Errorf("open pack builder: atomically activate bundle: %w", err)
	}
	stagingCreated = false
	if err := syncRoot(parentRoot); err != nil {
		return Result{}, err
	}
	if err := rootStillAtPath(parentRoot, parentPath); err != nil {
		return Result{}, err
	}
	return Result{
		Path: outputDir, PackID: spec.PackID, Revision: spec.Revision,
		ManifestSHA256: manifestDigest, BuildReportSHA256: indexpack.Digest(reportRaw),
		RecordCount: selection.Accepted, ShardCount: len(descriptors), OrdinaryInstallable: ordinary, ProjectionBytes: projectionBytes,
	}, nil
}

func removeStaging(parentRoot *os.Root, stagingName string) error {
	return parentRoot.RemoveAll(stagingName)
}

func syncBundleDirectories(stagingRoot *os.Root) error {
	for _, relative := range []string{filepath.Join("shards", "sha256"), "shards", "."} {
		if err := syncDirectory(stagingRoot, relative); err != nil {
			return fmt.Errorf("open pack builder: sync bundle directory %s: %w", relative, err)
		}
	}
	return nil
}

func syncRoot(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func rootStillAtPath(root *os.Root, path string) error {
	anchored, err := root.Stat(".")
	if err != nil {
		return fmt.Errorf("open pack builder: inspect anchored output parent: %w", err)
	}
	current, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("open pack builder: output parent path changed during build: %w", err)
	}
	if !current.IsDir() || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(anchored, current) {
		return errors.New("open pack builder: output parent path changed during build")
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
	// The rename result is authoritative. A close error on this temporary
	// descriptor cannot undo activation; durability is checked by syncRoot.
	return nil
}

func verifyOrdinaryInstall(ctx context.Context, bundleRoot *os.Root, manifest indexpack.Manifest, manifestDigest string, publicKey ed25519.PublicKey) (projectionBytes uint64, returnErr error) {
	root, err := os.MkdirTemp("", "fetchmark-pack-build-verify-")
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := os.RemoveAll(root); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("remove verification projection: %w", err))
		}
	}()
	createdAt, err := time.Parse(time.RFC3339, manifest.CreatedAt)
	if err != nil {
		return 0, err
	}
	installed, err := openpackindex.InstallFromRoot(ctx, bundleRoot, openpackindex.InstallOptions{
		Root: root, TrustedKeys: map[string]ed25519.PublicKey{manifest.SigningKeyID: publicKey},
		Acceptance: indexpack.Acceptance{
			Now: createdAt, ExpectedPackID: manifest.PackID, ExpectedManifestSHA256: manifestDigest,
			ExpectedKeyID: manifest.SigningKeyID, MinimumRevision: manifest.Revision, ExpectedRevision: manifest.Revision,
			ExpectedRecordCount: manifest.RecordCount, ExpectedCreatedAt: manifest.CreatedAt, ExpectedExpiresAt: manifest.ExpiresAt,
		},
		MaxProjectionBytes: openpackindex.MaxProjectionBytes,
	})
	if err != nil {
		return 0, err
	}
	return installed.ProjectionBytes, nil
}

func VerifyBuildReport(reportRaw, signatureRaw []byte, publicKey ed25519.PublicKey) error {
	signature, err := indexpack.DecodeSignature(signatureRaw)
	if err != nil {
		return err
	}
	if len(publicKey) != ed25519.PublicKeySize || signature.KeyID != indexpack.KeyID(publicKey) {
		return errors.New("open pack builder: report signing key mismatch")
	}
	decoded, err := decodeSignatureBytes(signature.Signature)
	if err != nil {
		return err
	}
	domain, err := buildReportDomain(reportRaw)
	if err != nil {
		return err
	}
	message := append([]byte(domain), reportRaw...)
	if !ed25519.Verify(publicKey, message, decoded) {
		return errors.New("open pack builder: report signature verification failed")
	}
	var report BuildReport
	if err := indexpack.DecodeStrictJSON(reportRaw, &report); err != nil {
		return fmt.Errorf("open pack builder: invalid report: %w", err)
	}
	if err := validateBuildReportVersionedFields(reportRaw, report.Version); err != nil {
		return err
	}
	if err := report.validate(); err != nil {
		return err
	}
	if report.KeyID != signature.KeyID {
		return errors.New("open pack builder: report identity is invalid")
	}
	return nil
}

func buildReportDomain(reportRaw []byte) (string, error) {
	var envelope struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(reportRaw, &envelope); err != nil {
		return "", fmt.Errorf("open pack builder: decode report version: %w", err)
	}
	switch envelope.Version {
	case BuildReportVersion:
		return BuildReportSignatureDomain, nil
	case EvidenceBuildReportVersion:
		return EvidenceBuildReportSignatureDomain, nil
	default:
		return "", errors.New("open pack builder: unsupported build-report version")
	}
}

func validateBuildReportVersionedFields(reportRaw []byte, version int) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(reportRaw, &fields); err != nil {
		return err
	}
	_, evidencePresent := fields["evidence_report_sha256"]
	if version == BuildReportVersion && evidencePresent {
		return errors.New("open pack builder: evidence_report_sha256 is not supported by build-report version 2")
	}
	if version == EvidenceBuildReportVersion && !evidencePresent {
		return errors.New("open pack builder: evidence_report_sha256 is required by build-report version 3")
	}
	return nil
}

func validateEvidenceSelection(evidence packevidence.MergeReport, selection indexpackselection.Report, spec indexpackselection.Spec, rightsSummary *candidateRightsSummary, buildTime time.Time) error {
	if evidence.Candidates.SHA256 != spec.CandidateSHA256 || evidence.Candidates.SHA256 != selection.CandidateSHA256 ||
		evidence.Candidates.Records != selection.Candidates || evidence.RobotsUserAgent != spec.RobotsUserAgent {
		return errors.New("open pack builder: evidence report does not bind the selected candidate input")
	}
	if rightsSummary == nil || rightsSummary.records != selection.Candidates || rightsSummary.rights == nil || evidence.Rights == nil || !sameEvidenceRights(*evidence.Rights, *rightsSummary.rights) {
		return errors.New("open pack builder: evidence report rights do not bind the candidate stream")
	}
	evidenceUntil, evidenceErr := time.Parse(time.RFC3339, evidence.PermissionValidUntil)
	selectionUntil, selectionErr := time.Parse(time.RFC3339, selection.PermissionValidUntil)
	completed, completedErr := time.Parse(time.RFC3339, evidence.CompletedAt)
	if evidenceErr != nil || selectionErr != nil || completedErr != nil || evidenceUntil.UTC().Format(time.RFC3339) != evidence.PermissionValidUntil ||
		evidenceUntil.After(selectionUntil) || !evidenceUntil.After(buildTime.UTC().Add(indexpackselection.MinimumCompletionValidity)) ||
		completed.After(buildTime.UTC().Add(indexpackselection.MaxBuildTimestampSkew)) {
		return errors.New("open pack builder: evidence report validity does not bind the build window")
	}
	return nil
}

type candidateRightsSummary struct {
	pending []byte
	records uint64
	rights  *packevidence.MergeRights
	err     error
}

func (summary *candidateRightsSummary) Write(chunk []byte) (int, error) {
	if summary == nil || summary.err != nil {
		if summary != nil {
			return 0, summary.err
		}
		return 0, errors.New("open pack builder: candidate rights summarizer is nil")
	}
	summary.pending = append(summary.pending, chunk...)
	for {
		newline := bytes.IndexByte(summary.pending, '\n')
		if newline < 0 {
			if len(summary.pending) > indexpackselection.MaxCandidateBytes {
				summary.err = errors.New("open pack builder: candidate rights line exceeds limit")
				return 0, summary.err
			}
			break
		}
		line := summary.pending[:newline]
		if len(line) == 0 || len(line) > indexpackselection.MaxCandidateBytes || line[len(line)-1] == '\r' {
			summary.err = errors.New("open pack builder: candidate rights line framing is invalid")
			return 0, summary.err
		}
		var candidate indexpackselection.Candidate
		if err := indexpack.DecodeStrictJSON(line, &candidate); err != nil {
			summary.err = fmt.Errorf("open pack builder: decode candidate rights: %w", err)
			return 0, summary.err
		}
		semantic := &packevidence.MergeRights{
			AllowedFields: append([]string(nil), candidate.Admission.Rights.AllowedFields...), Basis: candidate.Admission.Rights.Basis,
			EvidenceURI: candidate.Admission.Rights.EvidenceURI, EvidenceSHA256: candidate.Admission.Rights.EvidenceSHA256,
			RightsNotice: candidate.Admission.Rights.RightsNotice,
		}
		if summary.rights == nil {
			summary.rights = semantic
		} else if !sameEvidenceRights(*summary.rights, *semantic) {
			summary.err = errors.New("open pack builder: candidate rights decisions differ")
			return 0, summary.err
		}
		summary.records++
		summary.pending = summary.pending[newline+1:]
	}
	return len(chunk), nil
}

func (summary *candidateRightsSummary) finish() error {
	if summary == nil || summary.err != nil {
		if summary != nil {
			return summary.err
		}
		return errors.New("open pack builder: candidate rights summary is missing")
	}
	if len(summary.pending) != 0 || summary.records == 0 || summary.rights == nil {
		return errors.New("open pack builder: candidate rights stream is incomplete")
	}
	return nil
}

func sameEvidenceRights(left, right packevidence.MergeRights) bool {
	return len(left.AllowedFields) == 1 && len(right.AllowedFields) == 1 && left.AllowedFields[0] == right.AllowedFields[0] &&
		left.Basis == right.Basis && left.EvidenceURI == right.EvidenceURI && left.EvidenceSHA256 == right.EvidenceSHA256 && left.RightsNotice == right.RightsNotice
}

func (report BuildReport) validate() error {
	if (report.Version != BuildReportVersion && report.Version != EvidenceBuildReportVersion) || report.Builder != "fetchmark-pack-build" ||
		strings.TrimSpace(report.BuilderVersion) != report.BuilderVersion || report.BuilderVersion == "" || len(report.BuilderVersion) > 128 {
		return errors.New("open pack builder: report builder identity is invalid")
	}
	if !publisherIDPattern.MatchString(report.PublisherID) || !publisherIDPattern.MatchString(report.PackID) || report.Revision == 0 ||
		!validDigest(report.KeyID) || !validDigest(report.ManifestSHA256) || !validDigest(report.SelectedStreamSHA256) {
		return errors.New("open pack builder: report identity is invalid")
	}
	if (report.Version == BuildReportVersion && report.EvidenceReportSHA256 != "") ||
		(report.Version == EvidenceBuildReportVersion && !validDigest(report.EvidenceReportSHA256)) {
		return errors.New("open pack builder: report evidence binding is invalid")
	}
	createdAt, err := time.Parse(time.RFC3339, report.CreatedAt)
	if err != nil || createdAt.UTC().Format(time.RFC3339) != report.CreatedAt {
		return errors.New("open pack builder: report created_at is invalid")
	}
	selection := report.Selection
	permissionValidUntil, permissionErr := time.Parse(time.RFC3339, selection.PermissionValidUntil)
	if selection.Version != indexpackselection.ReportVersion || !validDigest(selection.PolicySHA256) || !validDigest(selection.ExclusionsSHA256) || !validDigest(selection.CandidateSHA256) ||
		selection.Accepted == 0 || selection.UniqueHosts == 0 || selection.UniqueHosts > selection.Accepted || selection.PermissionOldestAge > indexpackselection.MaxPermissionAgeHours ||
		permissionErr != nil || permissionValidUntil.UTC().Format(time.RFC3339) != selection.PermissionValidUntil || !permissionValidUntil.After(createdAt) ||
		selection.Candidates < selection.Accepted || selection.Candidates-selection.Accepted != selection.Rejected ||
		sumCounts(selection.RejectedByReason) != selection.Rejected || sumCounts(selection.AcceptedByLanguage) != selection.Accepted {
		return errors.New("open pack builder: report selection accounting is invalid")
	}
	switch report.Profile {
	case indexpackselection.ProfileLightweight:
		if !report.OrdinaryInstallable || selection.Accepted > indexpack.RecommendedInstallRecordLimit || len(report.Shards) != 1 {
			return errors.New("open pack builder: lightweight report exceeds the ordinary consumer profile")
		}
	case indexpackselection.ProfileFormatPrototype:
		if report.OrdinaryInstallable || selection.Accepted > 5_000_000 {
			return errors.New("open pack builder: format prototype report has invalid consumer claims")
		}
	default:
		return errors.New("open pack builder: report profile is invalid")
	}
	if len(report.Shards) == 0 || len(report.Shards) > indexpack.MaxManifestShards {
		return errors.New("open pack builder: report shard count is invalid")
	}
	seen := make(map[string]struct{}, len(report.Shards))
	var records uint64
	for _, shard := range report.Shards {
		if shard.Path != indexpack.ShardPath(shard.SHA256) || !validDigest(shard.SHA256) || !validDigest(shard.UncompressedSHA256) ||
			shard.RecordCount == 0 || shard.RecordCount > indexpack.MaxShardRecords || shard.CompressedSizeBytes == 0 || shard.CompressedSizeBytes > indexpack.MaxCompressedShardBytes ||
			shard.UncompressedSizeBytes == 0 || shard.UncompressedSizeBytes > indexpack.MaxUncompressedShardBytes {
			return errors.New("open pack builder: report shard evidence is invalid")
		}
		if _, duplicate := seen[shard.Path]; duplicate {
			return errors.New("open pack builder: report repeats a shard")
		}
		seen[shard.Path] = struct{}{}
		if records > indexpack.MaxPackRecords-shard.RecordCount {
			return errors.New("open pack builder: report shard record count overflows")
		}
		records += shard.RecordCount
	}
	if records != selection.Accepted {
		return errors.New("open pack builder: report shard records differ from selection")
	}
	return nil
}

func validDigest(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func sumCounts(counts map[string]uint64) uint64 {
	var total uint64
	for label, count := range counts {
		if strings.TrimSpace(label) != label || label == "" || total > ^uint64(0)-count {
			return ^uint64(0)
		}
		total += count
	}
	return total
}

type shardWriter struct {
	ctx            context.Context
	root           *os.Root
	kind           indexpack.Kind
	file           *os.File
	temporary      string
	encoder        *zstd.Encoder
	compressed     *countingHash
	uncompressed   *countingHash
	count          uint64
	descriptors    []indexpack.Shard
	evidence       []ShardEvidence
	selectedHasher hash.Hash
}

func newShardWriter(ctx context.Context, root *os.Root, kind indexpack.Kind) *shardWriter {
	return &shardWriter{ctx: ctx, root: root, kind: kind, selectedHasher: sha256.New()}
}

func (writer *shardWriter) add(record indexpack.Record) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if writer.count > 0 && (writer.count >= maxShardRecords || writer.uncompressed.bytes+uint64(len(raw)) > maxShardUncompressedBytes) {
		if err := writer.closeShard(); err != nil {
			return err
		}
	}
	if writer.encoder == nil {
		if err := writer.openShard(); err != nil {
			return err
		}
	}
	if _, err := writer.uncompressed.Write(raw); err != nil {
		return err
	}
	if _, err := writer.selectedHasher.Write(raw); err != nil {
		return err
	}
	if _, err := writer.encoder.Write(raw); err != nil {
		return fmt.Errorf("open pack builder: compress shard: %w", err)
	}
	writer.count++
	return nil
}

func (writer *shardWriter) openShard() error {
	temporary := fmt.Sprintf(".shard-%04d.tmp", len(writer.descriptors))
	file, err := writer.root.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	compressed := &countingHash{hash: sha256.New()}
	encoder, err := zstd.NewWriter(io.MultiWriter(file, compressed), zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(true))
	if err != nil {
		_ = file.Close()
		return err
	}
	writer.file = file
	writer.temporary = temporary
	writer.encoder = encoder
	writer.compressed = compressed
	writer.uncompressed = &countingHash{hash: sha256.New()}
	writer.count = 0
	return nil
}

func (writer *shardWriter) finish() error {
	if writer.encoder == nil {
		return errors.New("open pack builder: selection produced no shard")
	}
	return writer.closeShard()
}

func (writer *shardWriter) closeShard() error {
	temporary := writer.temporary
	if err := writer.encoder.Close(); err != nil {
		return err
	}
	writer.encoder = nil
	if err := writer.file.Sync(); err != nil {
		return err
	}
	if err := writer.file.Close(); err != nil {
		return err
	}
	writer.file = nil
	writer.temporary = ""
	compressedDigest := writer.compressed.digest()
	descriptor := indexpack.Shard{
		Path: indexpack.ShardPath(compressedDigest), Compression: "zstd", SHA256: compressedDigest,
		CompressedSizeBytes: writer.compressed.bytes, UncompressedSHA256: writer.uncompressed.digest(),
		UncompressedSizeBytes: writer.uncompressed.bytes, RecordCount: writer.count,
	}
	if descriptor.CompressedSizeBytes > indexpack.MaxCompressedShardBytes || descriptor.UncompressedSizeBytes > indexpack.MaxUncompressedShardBytes || descriptor.RecordCount > indexpack.MaxShardRecords {
		return errors.New("open pack builder: shard exceeds neutral format limits")
	}
	destination := filepath.FromSlash(descriptor.Path)
	if err := writer.root.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return err
	}
	if _, err := writer.root.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return errors.New("open pack builder: duplicate content-addressed shard")
		}
		return err
	}
	if err := writer.root.Rename(temporary, destination); err != nil {
		return err
	}
	if err := verifyShard(writer.ctx, writer.root, destination, descriptor, writer.kind); err != nil {
		return err
	}
	writer.descriptors = append(writer.descriptors, descriptor)
	writer.evidence = append(writer.evidence, ShardEvidence{
		Path: descriptor.Path, RecordCount: descriptor.RecordCount,
		CompressedSizeBytes: descriptor.CompressedSizeBytes, UncompressedSizeBytes: descriptor.UncompressedSizeBytes,
		SHA256: descriptor.SHA256, UncompressedSHA256: descriptor.UncompressedSHA256,
	})
	writer.compressed = nil
	writer.uncompressed = nil
	writer.count = 0
	return nil
}

func (writer *shardWriter) abort() {
	if writer.encoder != nil {
		writer.encoder.Close()
		writer.encoder = nil
	}
	if writer.file != nil {
		writer.file.Close()
		writer.file = nil
	}
}

func verifyShard(ctx context.Context, root *os.Root, relative string, descriptor indexpack.Shard, kind indexpack.Kind) error {
	compressed, err := root.Open(relative)
	if err != nil {
		return err
	}
	if err := indexpack.VerifyCompressedShard(compressed, descriptor); err != nil {
		_ = compressed.Close()
		return err
	}
	if _, err := compressed.Seek(0, io.SeekStart); err != nil {
		_ = compressed.Close()
		return err
	}
	decoder, err := zstd.NewReader(compressed, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(indexpack.MaxUncompressedShardBytes))
	if err != nil {
		_ = compressed.Close()
		return err
	}
	err = indexpack.ScanRecordStreamVersion(ctx, decoder, indexpack.VersionURLMetadata, kind, descriptor, func(indexpack.Record) error { return nil })
	decoder.Close()
	closeErr := compressed.Close()
	return errors.Join(err, closeErr)
}

type countingHash struct {
	hash  hash.Hash
	bytes uint64
}

func (counter *countingHash) Write(raw []byte) (int, error) {
	written, err := counter.hash.Write(raw)
	counter.bytes += uint64(written)
	return written, err
}

func (counter *countingHash) digest() string { return hex.EncodeToString(counter.hash.Sum(nil)) }

func signReport(reportRaw []byte, identity Identity) ([]byte, error) {
	domain, err := buildReportDomain(reportRaw)
	if err != nil {
		return nil, err
	}
	message := append([]byte(domain), reportRaw...)
	signature := indexpack.DetachedSignature{
		Version: indexpack.SignatureVersion, Algorithm: indexpack.SignatureAlgorithm,
		KeyID: identity.keyID, Signature: encodeSignatureBytes(ed25519.Sign(identity.privateKey, message)),
	}
	return indexpack.EncodeSignature(signature)
}

func encodeSignatureBytes(raw []byte) string {
	return base64.StdEncoding.EncodeToString(raw)
}

func decodeSignatureBytes(encoded string) ([]byte, error) {
	raw, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(raw) != ed25519.SignatureSize || base64.StdEncoding.EncodeToString(raw) != encoded {
		return nil, errors.New("open pack builder: report signature is invalid")
	}
	return raw, nil
}

func writeExclusive(root *os.Root, relative string, raw []byte) error {
	file, err := root.OpenFile(relative, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open pack builder: create %s: %w", filepath.Base(relative), err)
	}
	written, writeErr := io.Copy(file, bytes.NewReader(raw))
	if writeErr == nil && written != int64(len(raw)) {
		writeErr = io.ErrShortWrite
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	return errors.Join(writeErr, syncErr, closeErr)
}

func syncDirectory(root *os.Root, relative string) error {
	directory, err := root.Open(relative)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func cleanAbsolute(path string) bool {
	return path != "" && strings.TrimSpace(path) == path && filepath.IsAbs(path) && filepath.Clean(path) == path
}
