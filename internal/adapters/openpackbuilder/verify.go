package openpackbuilder

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/staticvar/fetchmark/internal/adapters/packevidence"

	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/indexpackselection"
)

type Verification struct {
	PackID              string `json:"pack_id"`
	Revision            uint64 `json:"revision"`
	ManifestSHA256      string `json:"manifest_sha256"`
	BuildReportSHA256   string `json:"build_report_sha256"`
	RecordCount         uint64 `json:"record_count"`
	ShardCount          int    `json:"shard_count"`
	OrdinaryInstallable bool   `json:"ordinary_installable"`
	ProjectionBytes     uint64 `json:"projection_bytes,omitempty"`
}

// VerifyBundle independently verifies publisher signatures, report/manifest
// bindings, every shard digest and record, and the selected record-stream hash.
func VerifyBundle(ctx context.Context, bundle string, publicIdentity PublicIdentity, publicKey ed25519.PublicKey) (Verification, error) {
	if ctx == nil || !cleanAbsolute(bundle) || len(publicKey) != ed25519.PublicKeySize || publicIdentity.KeyID != indexpack.KeyID(publicKey) {
		return Verification{}, errors.New("open pack builder: valid context, bundle, and public identity are required")
	}
	bundleRoot, err := os.OpenRoot(bundle)
	if err != nil {
		return Verification{}, fmt.Errorf("open pack builder: anchor bundle: %w", err)
	}
	defer bundleRoot.Close()
	verification, _, _, err := verifySnapshotBundleFromRoot(ctx, bundleRoot, publicIdentity, publicKey)
	return verification, err
}

func verifySnapshotBundleFromRoot(
	ctx context.Context,
	bundleRoot *os.Root,
	publicIdentity PublicIdentity,
	publicKey ed25519.PublicKey,
) (Verification, indexpack.VerifiedManifest, BuildReport, error) {
	if ctx == nil || bundleRoot == nil || len(publicKey) != ed25519.PublicKeySize ||
		publicIdentity.KeyID != indexpack.KeyID(publicKey) || publicIdentity.PublisherID == "" {
		return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, errors.New("open pack builder: valid context, bundle root, and public identity are required")
	}
	manifestRaw, err := readBoundedRegular(bundleRoot, ManifestFilename, indexpack.MaxManifestBytes)
	if err != nil {
		return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, err
	}
	signatureRaw, err := readBoundedRegular(bundleRoot, SignatureFilename, indexpack.MaxSignatureBytes)
	if err != nil {
		return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, err
	}
	verified, err := indexpack.VerifyManifest(manifestRaw, signatureRaw, map[string]ed25519.PublicKey{publicIdentity.KeyID: publicKey})
	if err != nil {
		return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, err
	}
	if verified.Manifest.Version != indexpack.VersionURLMetadata || verified.Manifest.Kind != indexpack.KindSnapshot {
		return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, errors.New("open pack builder: build verifier requires a v2 snapshot")
	}
	if err := validateBuilderManifestProvenance(verified.Manifest); err != nil {
		return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, err
	}
	reportRaw, err := readBoundedRegular(bundleRoot, BuildReportFilename, indexpack.MaxManifestBytes)
	if err != nil {
		return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, err
	}
	reportSignatureRaw, err := readBoundedRegular(bundleRoot, ReportSignatureFilename, indexpack.MaxSignatureBytes)
	if err != nil {
		return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, err
	}
	if err := VerifyBuildReport(reportRaw, reportSignatureRaw, publicKey); err != nil {
		return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, err
	}
	var report BuildReport
	if err := indexpack.DecodeStrictJSON(reportRaw, &report); err != nil {
		return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, err
	}
	manifest := verified.Manifest
	if report.Profile != indexpackselection.ProfileLightweight && report.Profile != indexpackselection.ProfileFormatPrototype {
		return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, errors.New("open pack builder: report profile is invalid")
	}
	ordinary := false
	var projectionBytes uint64
	if report.Profile == indexpackselection.ProfileLightweight {
		projectionBytes, err = verifyOrdinaryInstall(ctx, bundleRoot, manifest, verified.Digest, publicKey)
		if err != nil {
			return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, fmt.Errorf("open pack builder: lightweight consumer gate: %w", err)
		}
		ordinary = true
	}
	if report.PublisherID != publicIdentity.PublisherID || report.KeyID != publicIdentity.KeyID || report.ManifestSHA256 != verified.Digest ||
		report.PackID != manifest.PackID || report.Revision != manifest.Revision || report.CreatedAt != manifest.CreatedAt ||
		report.Selection.Accepted != manifest.RecordCount || report.Selection.PolicySHA256 != manifest.Build.PolicySHA256 ||
		report.Selection.ExclusionsSHA256 != manifest.Build.ExclusionsSHA256 || report.Selection.CandidateSHA256 != manifest.Build.CandidateSHA256 ||
		report.OrdinaryInstallable != ordinary || len(report.Shards) != len(manifest.Shards) {
		return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, errors.New("open pack builder: report does not bind the manifest and selection")
	}
	expectedRightsNotice := ""
	if report.Version == EvidenceBuildReportVersion {
		evidenceRaw, err := readBoundedRegular(bundleRoot, EvidenceReportFilename, packevidence.MaxMergeReportBytes)
		if err != nil {
			return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, err
		}
		evidence, evidenceDigest, err := packevidence.DecodeMergeReport(evidenceRaw)
		if err != nil || evidenceDigest != report.EvidenceReportSHA256 {
			return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, errors.Join(errors.New("open pack builder: carried evidence report digest differs from signed build report"), err)
		}
		createdAt, _ := time.Parse(time.RFC3339, report.CreatedAt)
		if err := validateCarriedEvidence(evidence, report.Selection, createdAt); err != nil {
			return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, err
		}
		expectedRightsNotice = evidence.Rights.RightsNotice
	}
	manifestShards := make(map[string]indexpack.Shard, len(manifest.Shards))
	for _, descriptor := range manifest.Shards {
		manifestShards[descriptor.Path] = descriptor
	}
	selectedHasher := sha256.New()
	var selectedCount uint64
	seenReportShards := make(map[string]struct{}, len(report.Shards))
	for _, evidence := range report.Shards {
		if err := ctx.Err(); err != nil {
			return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, err
		}
		descriptor, found := manifestShards[evidence.Path]
		if !found {
			return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, errors.New("open pack builder: report references unknown shard")
		}
		if _, duplicate := seenReportShards[evidence.Path]; duplicate {
			return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, errors.New("open pack builder: report repeats a shard")
		}
		seenReportShards[evidence.Path] = struct{}{}
		if evidence.RecordCount != descriptor.RecordCount || evidence.CompressedSizeBytes != descriptor.CompressedSizeBytes ||
			evidence.UncompressedSizeBytes != descriptor.UncompressedSizeBytes || evidence.SHA256 != descriptor.SHA256 ||
			evidence.UncompressedSHA256 != descriptor.UncompressedSHA256 {
			return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, errors.New("open pack builder: report shard evidence differs from manifest")
		}
		if err := verifyAndHashShard(ctx, bundleRoot, filepath.FromSlash(descriptor.Path), descriptor, manifest.Build.Inputs[0].Name, expectedRightsNotice, selectedHasher, &selectedCount); err != nil {
			return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, err
		}
	}
	if selectedCount != manifest.RecordCount || hex.EncodeToString(selectedHasher.Sum(nil)) != report.SelectedStreamSHA256 {
		return Verification{}, indexpack.VerifiedManifest{}, BuildReport{}, errors.New("open pack builder: selected record stream differs from report")
	}
	verification := Verification{
		PackID: manifest.PackID, Revision: manifest.Revision, ManifestSHA256: verified.Digest,
		BuildReportSHA256: indexpack.Digest(reportRaw), RecordCount: manifest.RecordCount,
		ShardCount: len(manifest.Shards), OrdinaryInstallable: ordinary, ProjectionBytes: projectionBytes,
	}
	return verification, verified, report, nil
}

func validateCarriedEvidence(evidence packevidence.MergeReport, selection indexpackselection.Report, createdAt time.Time) error {
	if evidence.Candidates.SHA256 != selection.CandidateSHA256 || evidence.Candidates.Records != selection.Candidates {
		return errors.New("open pack builder: carried evidence report does not bind selection input")
	}
	evidenceUntil, evidenceErr := time.Parse(time.RFC3339, evidence.PermissionValidUntil)
	selectionUntil, selectionErr := time.Parse(time.RFC3339, selection.PermissionValidUntil)
	completed, completedErr := time.Parse(time.RFC3339, evidence.CompletedAt)
	if evidenceErr != nil || selectionErr != nil || completedErr != nil || evidenceUntil.After(selectionUntil) ||
		!evidenceUntil.After(createdAt) || completed.After(createdAt.Add(indexpackselection.MaxBuildTimestampSkew)) {
		return errors.New("open pack builder: carried evidence report has an invalid build window")
	}
	return nil
}

func validateBuilderManifestProvenance(manifest indexpack.Manifest) error {
	if len(manifest.Build.Inputs) != 1 {
		return errors.New("open pack builder: manifest must declare exactly one source input")
	}
	if manifest.Build.Inputs[0].SHA256 == manifest.Build.CandidateSHA256 {
		return errors.New("open pack builder: source and candidate digests must differ")
	}
	return nil
}

func verifyAndHashShard(ctx context.Context, root *os.Root, relative string, descriptor indexpack.Shard, expectedSource, expectedRightsNotice string, selected io.Writer, count *uint64) error {
	file, err := openRegularExact(root, relative, descriptor.CompressedSizeBytes)
	if err != nil {
		return err
	}
	if err := indexpack.VerifyCompressedShard(file, descriptor); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return err
	}
	decoder, err := zstd.NewReader(file, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(indexpack.MaxUncompressedShardBytes))
	if err != nil {
		_ = file.Close()
		return err
	}
	err = indexpack.ScanRecordStreamVersion(ctx, io.TeeReader(decoder, selected), indexpack.VersionURLMetadata, indexpack.KindSnapshot, descriptor, func(record indexpack.Record) error {
		if err := validateBuilderRecordProvenance(record, expectedSource); err != nil {
			return err
		}
		if expectedRightsNotice != "" && record.Provenance[0].RightsNotice != expectedRightsNotice {
			return errors.New("open pack builder: record rights notice differs from aggregate evidence")
		}
		*count++
		return nil
	})
	decoder.Close()
	return errors.Join(err, file.Close())
}

func validateBuilderRecordProvenance(record indexpack.Record, expectedSource string) error {
	if len(record.Provenance) != 1 || record.Provenance[0].Source != expectedSource {
		return errors.New("open pack builder: record provenance does not match the manifest source input")
	}
	parsed, err := url.Parse(record.Provenance[0].SourceURI)
	if err != nil || parsed.Scheme != "https" || parsed.Host != "data.commoncrawl.org" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.RawPath != "" ||
		!strings.HasPrefix(parsed.Path, "/") || !indexpackselection.ValidCommonCrawlWARCPath(strings.TrimPrefix(parsed.Path, "/")) {
		return errors.New("open pack builder: record provenance source_uri is not a Common Crawl WARC artifact")
	}
	return nil
}

func readBoundedRegular(root *os.Root, relative string, maximum int) ([]byte, error) {
	file, err := openRegularExactBound(root, relative, int64(maximum))
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, int64(maximum)+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}
	if len(raw) == 0 || len(raw) > maximum {
		return nil, fmt.Errorf("open pack builder: %s has invalid size", relative)
	}
	return raw, nil
}

func openRegularExact(root *os.Root, relative string, size uint64) (*os.File, error) {
	if size > uint64(^uint(0)>>1) && strconv.IntSize == 32 {
		return nil, errors.New("open pack builder: file size cannot be represented")
	}
	file, err := openRegularExactBound(root, relative, int64(size))
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || uint64(info.Size()) != size {
		_ = file.Close()
		return nil, errors.New("open pack builder: shard size differs from manifest")
	}
	return file, nil
}

func openRegularExactBound(root *os.Root, relative string, maximum int64) (*os.File, error) {
	if root == nil || relative == "" || filepath.IsAbs(relative) || filepath.Clean(relative) != relative || strings.Contains(relative, "\\") {
		return nil, errors.New("open pack builder: bundle file path is unsafe")
	}
	before, err := root.Lstat(relative)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Size() < 1 || before.Size() > maximum {
		return nil, errors.New("open pack builder: bundle file is not a bounded regular file")
	}
	file, err := root.Open(relative)
	if err != nil {
		return nil, err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) || !opened.Mode().IsRegular() {
		_ = file.Close()
		return nil, errors.New("open pack builder: bundle file changed while opening")
	}
	return file, nil
}
