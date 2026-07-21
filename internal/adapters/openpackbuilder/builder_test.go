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
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/staticvar/fetchmark/internal/adapters/openpackindex"
	"github.com/staticvar/fetchmark/internal/adapters/packevidence"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/indexpackselection"
	"github.com/staticvar/fetchmark/internal/core/search"
)

func TestBuildDeterministicSignedURLMetadataPackEndToEnd(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Second)
	input := candidateStream(t, created,
		"https://docs.example.org/guides/open-search",
		"https://reference.example.net/manual/discovery",
	)
	specRaw := builderSpec(t, created, input)
	exclusionsRaw := []byte(`{"version":1,"urls":[],"host_suffixes":[]}`)
	identityRaw, public, err := GenerateIdentity("fetchmark-open-publisher", bytes.NewReader(bytes.Repeat([]byte{7}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(identityRaw)
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	firstPath := filepath.Join(base, "first")
	secondPath := filepath.Join(base, "second")
	first, err := Build(context.Background(), Options{
		SpecRaw: specRaw, ExclusionsRaw: exclusionsRaw, Candidates: bytes.NewReader(input), Identity: identity, BuilderVersion: "test", OutputDir: firstPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := Build(context.Background(), Options{
		SpecRaw: specRaw, ExclusionsRaw: exclusionsRaw, Candidates: bytes.NewReader(input), Identity: identity, BuilderVersion: "test", OutputDir: secondPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.ManifestSHA256 != second.ManifestSHA256 || first.BuildReportSHA256 != second.BuildReportSHA256 || first.RecordCount != 2 || first.ShardCount != 1 || !first.OrdinaryInstallable || first.ProjectionBytes == 0 || second.ProjectionBytes == 0 {
		t.Fatalf("build results differ: first=%#v second=%#v", first, second)
	}
	for _, relative := range []string{ManifestFilename, SignatureFilename, BuildReportFilename, ReportSignatureFilename} {
		firstRaw, err := os.ReadFile(filepath.Join(firstPath, relative))
		if err != nil {
			t.Fatal(err)
		}
		secondRaw, err := os.ReadFile(filepath.Join(secondPath, relative))
		if err != nil || !bytes.Equal(firstRaw, secondRaw) {
			t.Fatalf("%s is not deterministic: %v", relative, err)
		}
	}
	entries, err := os.ReadDir(filepath.Dir(firstPath))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".fetchmark-pack-") {
			t.Fatalf("successful build left private state %q", entry.Name())
		}
	}
	manifestRaw := readFile(t, filepath.Join(firstPath, ManifestFilename))
	signatureRaw := readFile(t, filepath.Join(firstPath, SignatureFilename))
	verified, err := indexpack.VerifyManifest(manifestRaw, signatureRaw, map[string]ed25519.PublicKey{public.KeyID: identity.PublicKey()})
	if err != nil {
		t.Fatal(err)
	}
	if verified.Manifest.Version != indexpack.VersionURLMetadata || verified.Manifest.RecordCount != 2 || len(verified.Manifest.Shards) != 1 || verified.Manifest.SigningKeyID != public.KeyID {
		t.Fatalf("manifest = %#v", verified.Manifest)
	}
	candidateDigest := sha256.Sum256(input)
	if verified.Manifest.Build.CandidateSHA256 != hex.EncodeToString(candidateDigest[:]) || verified.Manifest.Build.Inputs[0].SHA256 != strings.Repeat("8", 64) {
		t.Fatalf("manifest provenance = %#v", verified.Manifest.Build)
	}
	reportRaw := readFile(t, filepath.Join(firstPath, BuildReportFilename))
	if bytes.Contains(reportRaw, []byte("projection_bytes")) {
		t.Fatal("environment-dependent projection bytes were included in signed report")
	}
	reportSignatureRaw := readFile(t, filepath.Join(firstPath, ReportSignatureFilename))
	if err := VerifyBuildReport(reportRaw, reportSignatureRaw, identity.PublicKey()); err != nil {
		t.Fatal(err)
	}
	verification, err := VerifyBundle(context.Background(), firstPath, public, identity.PublicKey())
	if err != nil || verification.ManifestSHA256 != first.ManifestSHA256 || verification.BuildReportSHA256 != first.BuildReportSHA256 || verification.RecordCount != 2 || !verification.OrdinaryInstallable || verification.ProjectionBytes == 0 {
		t.Fatalf("VerifyBundle = %#v, %v", verification, err)
	}
	var report BuildReport
	if err := indexpack.DecodeStrictJSON(reportRaw, &report); err != nil {
		t.Fatal(err)
	}
	if report.ManifestSHA256 != first.ManifestSHA256 || report.Selection.Accepted != 2 || report.Selection.Rejected != 0 || !report.OrdinaryInstallable || report.SelectedStreamSHA256 == "" {
		t.Fatalf("report = %#v", report)
	}

	acceptance := indexpack.Acceptance{
		Now: created.Add(time.Hour), ExpectedPackID: verified.Manifest.PackID,
		ExpectedManifestSHA256: verified.Digest, ExpectedKeyID: public.KeyID,
		MinimumRevision: 1, ExpectedRevision: 1, ExpectedRecordCount: 2,
		ExpectedCreatedAt: verified.Manifest.CreatedAt, ExpectedExpiresAt: verified.Manifest.ExpiresAt,
	}
	installed, err := openpackindex.Install(context.Background(), openpackindex.InstallOptions{
		BundleDir: firstPath, Root: filepath.Join(base, "installed"),
		TrustedKeys: map[string]ed25519.PublicKey{public.KeyID: identity.PublicKey()}, Acceptance: acceptance,
	})
	if err != nil {
		t.Fatal(err)
	}
	index, err := openpackindex.Open(openpackindex.OpenOptions{
		Path: installed.Path, ExpectedManifestSHA256: installed.ManifestSHA256,
		ExpectedPackID: installed.PackID, ExpectedRevision: installed.Revision, ExpectedRecordCount: 2,
		ExpectedKeyID: public.KeyID, ExpectedCreatedAt: verified.Manifest.CreatedAt,
		ExpectedExpiresAt: verified.Manifest.ExpiresAt, Now: created.Add(time.Hour), ProviderID: "built-pack",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	batch, err := index.SearchBatch(context.Background(), search.Query{Q: "discovery"})
	if err != nil || len(batch.Hits) != 1 || batch.Hits[0].URL != "https://reference.example.net/manual/discovery" || batch.Hits[0].Title != "" {
		t.Fatalf("URL-derived search = %#v, %v", batch, err)
	}
}

func TestBuildBindsAndCarriesVerifiedAggregateEvidence(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Second)
	input := candidateStream(t, created, "https://docs.example.org/a", "https://docs.example.org/b")
	evidenceRaw := builderEvidenceReport(t, created, input)
	identityRaw, public, err := GenerateIdentity("evidence-publisher", bytes.NewReader(bytes.Repeat([]byte{31}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(identityRaw)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "bundle")
	result, err := Build(context.Background(), Options{
		SpecRaw: builderSpec(t, created, input), ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`),
		Candidates: bytes.NewReader(input), EvidenceReportRaw: evidenceRaw,
		Identity: identity, BuilderVersion: "test", OutputDir: output,
	})
	if err != nil {
		t.Fatal(err)
	}
	carriedEvidence := readFile(t, filepath.Join(output, EvidenceReportFilename))
	if !bytes.Equal(carriedEvidence, evidenceRaw) {
		t.Fatal("builder did not carry the exact aggregate evidence report")
	}
	var report BuildReport
	if err := indexpack.DecodeStrictJSON(readFile(t, filepath.Join(output, BuildReportFilename)), &report); err != nil {
		t.Fatal(err)
	}
	if report.Version != EvidenceBuildReportVersion || report.EvidenceReportSHA256 != indexpack.Digest(evidenceRaw) {
		t.Fatalf("build report = %#v", report)
	}
	if _, err := VerifyBundle(context.Background(), output, public, identity.PublicKey()); err != nil {
		t.Fatal(err)
	}
	corrupt := bytes.Clone(carriedEvidence)
	corrupt[len(corrupt)-2] ^= 1
	if err := os.WriteFile(filepath.Join(output, EvidenceReportFilename), corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyBundle(context.Background(), output, public, identity.PublicKey()); err == nil {
		t.Fatal("bundle verifier accepted a changed aggregate evidence report")
	}
	if result.BuildReportSHA256 == "" {
		t.Fatal("build result omitted signed report digest")
	}
}

func TestBuildRejectsAggregateEvidenceForDifferentCandidateStream(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Second)
	input := candidateStream(t, created, "https://docs.example.org/a")
	other := candidateStream(t, created, "https://docs.example.org/other")
	identityRaw, _, err := GenerateIdentity("evidence-publisher", bytes.NewReader(bytes.Repeat([]byte{32}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(identityRaw)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "bundle")
	_, err = Build(context.Background(), Options{
		SpecRaw: builderSpec(t, created, input), ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`),
		Candidates: bytes.NewReader(input), EvidenceReportRaw: builderEvidenceReport(t, created, other),
		Identity: identity, BuilderVersion: "test", OutputDir: output,
	})
	if err == nil || !strings.Contains(err.Error(), "evidence report") {
		t.Fatalf("error = %v", err)
	}
	if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("partial bundle exists: %v", statErr)
	}
}

func TestBuildRejectsAggregateRightsContradictingCandidateStream(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Second)
	input := candidateStream(t, created, "https://docs.example.org/a")
	var evidence packevidence.MergeReport
	if err := indexpack.DecodeStrictJSON(builderEvidenceReport(t, created, input), &evidence); err != nil {
		t.Fatal(err)
	}
	evidence.Rights.RightsNotice = "altered aggregate rights assertion"
	evidenceRaw, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	identityRaw, _, err := GenerateIdentity("evidence-publisher", bytes.NewReader(bytes.Repeat([]byte{33}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(identityRaw)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "bundle")
	_, err = Build(context.Background(), Options{
		SpecRaw: builderSpec(t, created, input), ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`),
		Candidates: bytes.NewReader(input), EvidenceReportRaw: evidenceRaw,
		Identity: identity, BuilderVersion: "test", OutputDir: output,
	})
	if err == nil || !strings.Contains(err.Error(), "rights") {
		t.Fatalf("error = %v", err)
	}
	if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("partial bundle exists: %v", statErr)
	}
}

func TestVerifyBundleRejectsResignedAggregateRightsContradictingRecords(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Second)
	input := candidateStream(t, created, "https://docs.example.org/a")
	identityRaw, public, err := GenerateIdentity("evidence-publisher", bytes.NewReader(bytes.Repeat([]byte{35}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(identityRaw)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "bundle")
	if _, err := Build(context.Background(), Options{
		SpecRaw: builderSpec(t, created, input), ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`),
		Candidates: bytes.NewReader(input), EvidenceReportRaw: builderEvidenceReport(t, created, input),
		Identity: identity, BuilderVersion: "test", OutputDir: output,
	}); err != nil {
		t.Fatal(err)
	}
	var evidence packevidence.MergeReport
	if err := indexpack.DecodeStrictJSON(readFile(t, filepath.Join(output, EvidenceReportFilename)), &evidence); err != nil {
		t.Fatal(err)
	}
	evidence.Rights.RightsNotice = "altered aggregate rights assertion"
	evidenceRaw, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(output, EvidenceReportFilename), evidenceRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	var report BuildReport
	if err := indexpack.DecodeStrictJSON(readFile(t, filepath.Join(output, BuildReportFilename)), &report); err != nil {
		t.Fatal(err)
	}
	report.EvidenceReportSHA256 = indexpack.Digest(evidenceRaw)
	reportRaw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	signatureRaw, err := signReport(reportRaw, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(output, BuildReportFilename), reportRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(output, ReportSignatureFilename), signatureRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyBundle(context.Background(), output, public, identity.PublicKey()); err == nil || !strings.Contains(err.Error(), "rights notice") {
		t.Fatalf("error = %v", err)
	}
}

func TestBuildRechecksAggregateEvidenceExpiryBeforeActivation(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Second)
	input := candidateStream(t, created, "https://docs.example.org/a")
	var evidence packevidence.MergeReport
	if err := indexpack.DecodeStrictJSON(builderEvidenceReport(t, created, input), &evidence); err != nil {
		t.Fatal(err)
	}
	evidence.PermissionValidUntil = created.Add(2 * time.Minute).Format(time.RFC3339)
	evidenceRaw, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	identityRaw, _, err := GenerateIdentity("evidence-publisher", bytes.NewReader(bytes.Repeat([]byte{34}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(identityRaw)
	if err != nil {
		t.Fatal(err)
	}
	clockCalls := 0
	clock := func() time.Time {
		clockCalls++
		if clockCalls >= 5 {
			return created.Add(3 * time.Minute)
		}
		return created
	}
	output := filepath.Join(t.TempDir(), "bundle")
	_, err = build(context.Background(), Options{
		SpecRaw: builderSpec(t, created, input), ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`),
		Candidates: bytes.NewReader(input), EvidenceReportRaw: evidenceRaw,
		Identity: identity, BuilderVersion: "test", OutputDir: output,
	}, clock)
	if err == nil || !strings.Contains(err.Error(), "evidence report validity") {
		t.Fatalf("error = %v (clock calls=%d)", err, clockCalls)
	}
	if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expired evidence bundle exists: %v", statErr)
	}
}

func TestBuildDeltaDeterministicallyPublishesExactSnapshotTransition(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Second)
	parentInput := candidateStream(t, created,
		"https://docs.example.org/a",
		"https://docs.example.org/b",
	)
	identityRaw, public, err := GenerateIdentity("fetchmark-open-publisher", bytes.NewReader(bytes.Repeat([]byte{19}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(identityRaw)
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	parentPath := filepath.Join(base, "parent")
	parent, err := Build(context.Background(), Options{
		SpecRaw: builderSpec(t, created, parentInput), ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`),
		Candidates: bytes.NewReader(parentInput), Identity: identity, BuilderVersion: "test", OutputDir: parentPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	targetCreated := created.Add(5 * time.Minute)
	targetInput := candidateStream(t, targetCreated,
		"https://docs.example.org/b",
		"https://docs.example.org/c",
	)
	targetSpec := deltaBuilderSpec(t, targetCreated, created.Add(30*24*time.Hour), targetInput)
	firstPath := filepath.Join(base, "delta-first")
	secondPath := filepath.Join(base, "delta-second")
	build := func(output string) DeltaResult {
		result, buildErr := BuildDelta(context.Background(), DeltaOptions{
			ParentBundleDir: parentPath, SpecRaw: targetSpec,
			ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`), Candidates: bytes.NewReader(targetInput),
			Identity: identity, BuilderVersion: "test", OutputDir: output,
		})
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		return result
	}
	first := build(firstPath)
	second := build(secondPath)
	if first.ManifestSHA256 != second.ManifestSHA256 || first.BuildReportSHA256 != second.BuildReportSHA256 ||
		first.ParentManifestSHA256 != parent.ManifestSHA256 || first.OperationCount != 3 || first.ProjectionRecordCount != 2 ||
		first.UpsertCount != 2 || first.TombstoneCount != 1 || !first.OrdinaryInstallable || first.ProjectionBytes == 0 {
		t.Fatalf("delta results differ or have invalid accounting: first=%#v second=%#v", first, second)
	}
	for _, relative := range []string{ManifestFilename, SignatureFilename, BuildReportFilename, ReportSignatureFilename} {
		if left, right := readFile(t, filepath.Join(firstPath, relative)), readFile(t, filepath.Join(secondPath, relative)); !bytes.Equal(left, right) {
			t.Fatalf("%s is not deterministic", relative)
		}
	}
	manifestRaw := readFile(t, filepath.Join(firstPath, ManifestFilename))
	signatureRaw := readFile(t, filepath.Join(firstPath, SignatureFilename))
	verified, err := indexpack.VerifyManifest(manifestRaw, signatureRaw, map[string]ed25519.PublicKey{public.KeyID: identity.PublicKey()})
	if err != nil {
		t.Fatal(err)
	}
	if verified.Manifest.Kind != indexpack.KindDelta || verified.Manifest.ParentManifestSHA256 != parent.ManifestSHA256 ||
		verified.Manifest.Revision != 2 || verified.Manifest.RecordCount != 3 {
		t.Fatalf("delta manifest = %#v", verified.Manifest)
	}
	verification, err := VerifyDeltaBundle(context.Background(), parentPath, firstPath, public, identity.PublicKey())
	if err != nil || verification.ManifestSHA256 != first.ManifestSHA256 || verification.ParentManifestSHA256 != parent.ManifestSHA256 ||
		verification.OperationCount != 3 || verification.ProjectionRecordCount != 2 || !verification.OrdinaryInstallable || verification.ProjectionBytes == 0 {
		t.Fatalf("VerifyDeltaBundle = %#v, %v", verification, err)
	}
	if DeltaBuildReportSignatureDomain != "fetchmark-open-index-pack-delta-build-report-v1\n" {
		t.Fatalf("unexpected delta report signature domain %q", DeltaBuildReportSignatureDomain)
	}
	reportRaw := readFile(t, filepath.Join(firstPath, BuildReportFilename))
	reportSignatureRaw := readFile(t, filepath.Join(firstPath, ReportSignatureFilename))
	if err := VerifyDeltaBuildReport(reportRaw, reportSignatureRaw, identity.PublicKey()); err != nil {
		t.Fatalf("VerifyDeltaBuildReport: %v", err)
	}
	if err := VerifyBuildReport(reportRaw, reportSignatureRaw, identity.PublicKey()); err == nil {
		t.Fatal("delta report signature was accepted under the snapshot report domain")
	}
	tampered := append([]byte(nil), reportRaw...)
	tampered[len(tampered)-1] ^= 1
	if err := VerifyDeltaBuildReport(tampered, reportSignatureRaw, identity.PublicKey()); err == nil {
		t.Fatal("tampered delta report was accepted")
	}
	otherParentInput := candidateStream(t, created, "https://docs.example.org/unrelated")
	otherParentPath := filepath.Join(base, "other-parent")
	if _, err := Build(context.Background(), Options{
		SpecRaw: builderSpec(t, created, otherParentInput), ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`),
		Candidates: bytes.NewReader(otherParentInput), Identity: identity, BuilderVersion: "test", OutputDir: otherParentPath,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyDeltaBundle(context.Background(), otherParentPath, firstPath, public, identity.PublicKey()); err == nil || !strings.Contains(err.Error(), "parent") {
		t.Fatalf("delta verifier accepted wrong signed parent: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := VerifyDeltaBundle(canceled, parentPath, firstPath, public, identity.PublicKey()); !errors.Is(err, context.Canceled) {
		t.Fatalf("delta verifier cancellation error = %v", err)
	}
}

func TestBuildDeltaRejectsNoopAndValidityExtensionWithoutActivatingOutput(t *testing.T) {
	created := time.Date(2026, 7, 19, 11, 10, 0, 0, time.UTC)
	targetCreated := created.Add(5 * time.Minute)
	input := candidateStream(t, created, "https://docs.example.org/a")
	identityRaw, _, err := GenerateIdentity("fetchmark-open-publisher", bytes.NewReader(bytes.Repeat([]byte{21}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(identityRaw)
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	parentPath := filepath.Join(base, "parent")
	if _, err := build(context.Background(), Options{
		SpecRaw: builderSpec(t, created, input), ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`),
		Candidates: bytes.NewReader(input), Identity: identity, BuilderVersion: "test", OutputDir: parentPath,
	}, func() time.Time { return created }); err != nil {
		t.Fatal(err)
	}
	if _, err := buildDelta(context.Background(), DeltaOptions{
		ParentBundleDir: parentPath, SpecRaw: deltaBuilderSpec(t, created.Add(5*time.Minute), created.Add(30*24*time.Hour), input),
		ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`), Candidates: bytes.NewReader(input),
		Identity: identity, BuilderVersion: "test", OutputDir: filepath.Join(parentPath, "nested-delta"),
	}, func() time.Time { return targetCreated }); err == nil || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("overlapping output error = %v", err)
	}
	caseAlias := filepath.Join(base, "PARENT")
	if aliasInfo, aliasErr := os.Stat(caseAlias); aliasErr == nil {
		parentInfo, parentErr := os.Stat(parentPath)
		if parentErr != nil {
			t.Fatal(parentErr)
		}
		if os.SameFile(aliasInfo, parentInfo) {
			if _, err := buildDelta(context.Background(), DeltaOptions{
				ParentBundleDir: parentPath, SpecRaw: deltaBuilderSpec(t, created.Add(5*time.Minute), created.Add(30*24*time.Hour), input),
				ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`), Candidates: bytes.NewReader(input),
				Identity: identity, BuilderVersion: "test", OutputDir: filepath.Join(caseAlias, "case-alias-delta"),
			}, func() time.Time { return targetCreated }); err == nil || !strings.Contains(err.Error(), "inside the signed parent") {
				t.Fatalf("case-aliased overlapping output error = %v", err)
			}
		}
	}
	for _, test := range []struct {
		name    string
		expires time.Time
		want    string
	}{
		{name: "no operation", expires: created.Add(30 * 24 * time.Hour), want: "identical"},
		{name: "extends parent validity", expires: created.Add(30*24*time.Hour + time.Second), want: "validity"},
	} {
		t.Run(test.name, func(t *testing.T) {
			output := filepath.Join(base, strings.ReplaceAll(test.name, " ", "-"))
			_, err := buildDelta(context.Background(), DeltaOptions{
				ParentBundleDir: parentPath, SpecRaw: deltaBuilderSpec(t, targetCreated, test.expires, input),
				ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`), Candidates: bytes.NewReader(input),
				Identity: identity, BuilderVersion: "test", OutputDir: output,
			}, func() time.Time { return targetCreated })
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("BuildDelta error = %v, want %q", err, test.want)
			}
			if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("failed delta activated output: %v", statErr)
			}
		})
	}
}

func TestBuildDeltaCancellationImmediatelyBeforeActivationLeavesNoOutput(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Second).Add(-5 * time.Minute)
	parentInput := candidateStream(t, created, "https://docs.example.org/a")
	identityRaw, _, err := GenerateIdentity("fetchmark-open-publisher", bytes.NewReader(bytes.Repeat([]byte{23}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(identityRaw)
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	parentPath := filepath.Join(base, "parent")
	if _, err := Build(context.Background(), Options{
		SpecRaw: builderSpec(t, created, parentInput), ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`),
		Candidates: bytes.NewReader(parentInput), Identity: identity, BuilderVersion: "test", OutputDir: parentPath,
	}); err != nil {
		t.Fatal(err)
	}
	buildTime := created.Add(5 * time.Minute)
	targetInput := candidateStream(t, buildTime, "https://docs.example.org/b")
	output := filepath.Join(base, "canceled-delta")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clockCalls := 0
	clock := func() time.Time {
		clockCalls++
		if clockCalls == 4 {
			cancel()
		}
		return buildTime
	}
	_, err = buildDelta(ctx, DeltaOptions{
		ParentBundleDir: parentPath, SpecRaw: deltaBuilderSpec(t, buildTime, created.Add(30*24*time.Hour), targetInput),
		ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`), Candidates: bytes.NewReader(targetInput),
		Identity: identity, BuilderVersion: "test", OutputDir: output,
	}, clock)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("buildDelta error = %v, want context.Canceled (clock calls=%d)", err, clockCalls)
	}
	if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("canceled delta activated output: %v", statErr)
	}
}

func TestBuildDeltaCommittedErrorCarriesRecoveryEvidence(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Second).Add(-5 * time.Minute)
	parentInput := candidateStream(t, created, "https://docs.example.org/a")
	identityRaw, _, err := GenerateIdentity("fetchmark-open-publisher", bytes.NewReader(bytes.Repeat([]byte{25}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(identityRaw)
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	parentPath := filepath.Join(base, "parent")
	if _, err := Build(context.Background(), Options{
		SpecRaw: builderSpec(t, created, parentInput), ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`),
		Candidates: bytes.NewReader(parentInput), Identity: identity, BuilderVersion: "test", OutputDir: parentPath,
	}); err != nil {
		t.Fatal(err)
	}
	buildTime := created.Add(5 * time.Minute)
	targetInput := candidateStream(t, buildTime, "https://docs.example.org/b")
	output := filepath.Join(base, "committed-delta")
	hooks := defaultDeltaBuildHooks()
	hooks.syncOutputParent = func(*os.Root) error { return errors.New("synthetic parent fsync failure") }
	result, err := buildDeltaWithHooks(context.Background(), DeltaOptions{
		ParentBundleDir: parentPath, SpecRaw: deltaBuilderSpec(t, buildTime, created.Add(30*24*time.Hour), targetInput),
		ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`), Candidates: bytes.NewReader(targetInput),
		Identity: identity, BuilderVersion: "test", OutputDir: output,
	}, func() time.Time { return buildTime }, hooks)
	if !errors.Is(err, ErrDeltaBuildCommitted) || result.Path == "" || result.ManifestSHA256 == "" || result.BuildReportSHA256 == "" ||
		!strings.Contains(err.Error(), result.Path) || !strings.Contains(err.Error(), result.ManifestSHA256) || !strings.Contains(err.Error(), "verify-delta-run") {
		t.Fatalf("committed result=%#v error=%v", result, err)
	}
	if info, statErr := os.Stat(result.Path); statErr != nil || !info.IsDir() {
		t.Fatalf("committed delta missing after post-commit failure: %v", statErr)
	}
}

func TestBuildReportVersionTwoSignatureDomainGolden(t *testing.T) {
	if BuildReportSignatureDomain != "fetchmark-open-index-pack-build-report-v2\n" {
		t.Fatalf("unexpected build-report signature domain %q", BuildReportSignatureDomain)
	}
	seed := make([]byte, ed25519.SeedSize)
	for index := range seed {
		seed[index] = byte(index)
	}
	identityRaw, _, err := GenerateIdentity("golden-publisher", bytes.NewReader(seed))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(identityRaw)
	if err != nil {
		t.Fatal(err)
	}
	shardDigest := strings.Repeat("6", 64)
	reportRaw, err := json.Marshal(BuildReport{
		Version: BuildReportVersion, Builder: "fetchmark-pack-build", BuilderVersion: "golden-v2",
		Profile: indexpackselection.ProfileFormatPrototype, PublisherID: "golden-publisher", KeyID: identity.keyID,
		ManifestSHA256: strings.Repeat("1", 64), PackID: "golden-pack", Revision: 1, CreatedAt: "2026-07-19T00:00:00Z",
		Selection: indexpackselection.Report{
			Version: indexpackselection.ReportVersion, PolicySHA256: strings.Repeat("2", 64), ExclusionsSHA256: strings.Repeat("3", 64), CandidateSHA256: strings.Repeat("4", 64),
			Candidates: 1, Accepted: 1, UniqueHosts: 1, RejectedByReason: map[string]uint64{}, AcceptedByLanguage: map[string]uint64{"eng": 1}, PermissionOldestAge: 1,
			PermissionValidUntil: "2026-07-19T01:00:00Z",
		},
		Shards: []ShardEvidence{{
			Path: indexpack.ShardPath(shardDigest), RecordCount: 1, CompressedSizeBytes: 1, UncompressedSizeBytes: 1,
			SHA256: shardDigest, UncompressedSHA256: strings.Repeat("7", 64),
		}},
		SelectedStreamSHA256: strings.Repeat("5", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	signatureRaw, err := signReport(reportRaw, identity)
	if err != nil {
		t.Fatal(err)
	}
	const expectedSignature = `{"version":1,"algorithm":"ed25519","key_id":"56475aa75463474c0285df5dbf2bcab73da651358839e9b77481b2eab107708c","signature":"XIqYxH5rfPt48UQadw1BiCtJC6WI1mBko3+JXsKrLASBRmU7ntPuIbBNCQM4EtBOBVr6+UYeYt6YbBKH2XIdBA=="}`
	if string(signatureRaw) != expectedSignature {
		t.Fatalf("v2 build-report signature bytes changed:\n%s", signatureRaw)
	}
	if err := VerifyBuildReport(reportRaw, signatureRaw, identity.PublicKey()); err != nil {
		t.Fatal(err)
	}
	wrongDomainMessage := append([]byte("fetchmark-open-index-pack-build-report-v1\n"), reportRaw...)
	wrongDomainRaw, err := indexpack.EncodeSignature(indexpack.DetachedSignature{
		Version: indexpack.SignatureVersion, Algorithm: indexpack.SignatureAlgorithm, KeyID: identity.keyID,
		Signature: encodeSignatureBytes(ed25519.Sign(identity.privateKey, wrongDomainMessage)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyBuildReport(reportRaw, wrongDomainRaw, identity.PublicKey()); err == nil {
		t.Fatal("build report accepted a signature from the prior domain")
	}
}

func TestVerifyBundleRejectsNonCanonicalSourceContract(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Second)
	for name, mutate := range map[string]func(*indexpack.Manifest){
		"multiple source inputs": func(manifest *indexpack.Manifest) {
			manifest.Build.Inputs = append(manifest.Build.Inputs, indexpack.BuildInput{
				Name: "ZZZ secondary URL Index export", URI: "https://index.commoncrawl.org/CC-MAIN-2026-25-index",
				RetrievedAt: created.Add(-time.Hour).Format(time.RFC3339), SHA256: strings.Repeat("9", 64),
				RightsNotice: "Common Crawl terms; URL metadata only",
			})
		},
		"source digest equals candidate digest": func(manifest *indexpack.Manifest) {
			manifest.Build.Inputs[0].SHA256 = manifest.Build.CandidateSHA256
		},
	} {
		t.Run(name, func(t *testing.T) {
			bundle, public, identity := buildTestBundle(t, created)
			rewriteSignedBundleMetadata(t, bundle, identity, func(manifest *indexpack.Manifest, _ *BuildReport) {
				mutate(manifest)
			})
			if _, err := VerifyBundle(context.Background(), bundle, public, identity.PublicKey()); err == nil {
				t.Fatal("VerifyBundle accepted a bundle outside the canonical source contract")
			}
		})
	}
}

func TestVerifyBundleRejectsResignedSemanticCrossMismatches(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Second)
	for name, mutate := range map[string]func(*indexpack.Manifest, *BuildReport){
		"candidate digest": func(_ *indexpack.Manifest, report *BuildReport) {
			report.Selection.CandidateSHA256 = strings.Repeat("a", 64)
		},
		"policy digest": func(_ *indexpack.Manifest, report *BuildReport) {
			report.Selection.PolicySHA256 = strings.Repeat("b", 64)
		},
		"exclusions digest": func(_ *indexpack.Manifest, report *BuildReport) {
			report.Selection.ExclusionsSHA256 = strings.Repeat("c", 64)
		},
		"pack ID": func(_ *indexpack.Manifest, report *BuildReport) {
			report.PackID = "different-pack"
		},
		"revision": func(_ *indexpack.Manifest, report *BuildReport) {
			report.Revision++
		},
		"creation time": func(_ *indexpack.Manifest, report *BuildReport) {
			report.CreatedAt = created.Add(-time.Second).Format(time.RFC3339)
		},
		"shard evidence": func(_ *indexpack.Manifest, report *BuildReport) {
			report.Shards[0].CompressedSizeBytes++
		},
		"record count": func(_ *indexpack.Manifest, report *BuildReport) {
			report.Selection.Candidates = 2
			report.Selection.Accepted = 2
			report.Selection.UniqueHosts = 2
			report.Shards[0].RecordCount = 2
		},
	} {
		t.Run(name, func(t *testing.T) {
			bundle, public, identity := buildTestBundle(t, created)
			rewriteSignedBundleMetadata(t, bundle, identity, mutate)
			if _, err := VerifyBundle(context.Background(), bundle, public, identity.PublicKey()); err == nil {
				t.Fatal("VerifyBundle accepted independently re-signed inconsistent metadata")
			}
		})
	}
	bundle, public, identity := buildTestBundle(t, created)
	public.PublisherID = "different-publisher"
	if _, err := VerifyBundle(context.Background(), bundle, public, identity.PublicKey()); err == nil {
		t.Fatal("VerifyBundle accepted a different public publisher identity")
	}
	bundle, public, identity = buildTestBundle(t, created)
	rewriteSignedBuildReport(t, bundle, identity, func(report *BuildReport) {
		report.ManifestSHA256 = strings.Repeat("d", 64)
	})
	if _, err := VerifyBundle(context.Background(), bundle, public, identity.PublicKey()); err == nil {
		t.Fatal("VerifyBundle accepted an independently re-signed manifest digest mismatch")
	}
}

func TestVerifyBundleRejectsResignedRogueRecordProvenance(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Second)
	for name, mutate := range map[string]func(*indexpack.Record){
		"unrelated source name": func(record *indexpack.Record) {
			record.Provenance[0].Source = "unrelated-source"
		},
		"non Common Crawl source URI": func(record *indexpack.Record) {
			record.Provenance[0].SourceURI = "https://example.org/crawl-data/capture.warc.gz"
		},
		"dot segment": func(record *indexpack.Record) {
			record.Provenance[0].SourceURI = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-25/../forged.warc.gz"
		},
		"encoded dot segment": func(record *indexpack.Record) {
			record.Provenance[0].SourceURI = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-25/%2e%2e/forged.warc.gz"
		},
		"double slash": func(record *indexpack.Record) {
			record.Provenance[0].SourceURI = "https://data.commoncrawl.org/crawl-data//CC-MAIN-2026-25/forged.warc.gz"
		},
		"non CC-MAIN artifact": func(record *indexpack.Record) {
			record.Provenance[0].SourceURI = "https://data.commoncrawl.org/crawl-data/not-a-cc-main-artifact.warc.gz"
		},
	} {
		t.Run(name, func(t *testing.T) {
			bundle, public, identity := buildTestBundle(t, created)
			rewriteSignedFirstShard(t, bundle, identity, mutate)
			if _, err := VerifyBundle(context.Background(), bundle, public, identity.PublicKey()); err == nil {
				t.Fatal("VerifyBundle accepted re-signed record provenance outside the canonical builder contract")
			}
		})
	}
}

func TestBuildFailsClosedWithoutOverwritingOrLeavingLateDigestOutput(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Second)
	input := candidateStream(t, created, "https://docs.example.org/a")
	specRaw := builderSpec(t, created, []byte("different input"))
	exclusionsRaw := []byte(`{"version":1,"urls":[],"host_suffixes":[]}`)
	identityRaw, _, err := GenerateIdentity("publisher", bytes.NewReader(bytes.Repeat([]byte{9}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(identityRaw)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "bundle")
	_, err = Build(context.Background(), Options{
		SpecRaw: specRaw, ExclusionsRaw: exclusionsRaw, Candidates: bytes.NewReader(input), Identity: identity, BuilderVersion: "test", OutputDir: output,
	})
	if !errors.Is(err, indexpackselection.ErrInvalidCandidate) || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("Build error = %v", err)
	}
	if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("late digest failure left output: %v", statErr)
	}
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(output, "owner-data")
	if err := os.WriteFile(marker, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = Build(context.Background(), Options{
		SpecRaw: specRaw, ExclusionsRaw: exclusionsRaw, Candidates: bytes.NewReader(input), Identity: identity, BuilderVersion: "test", OutputDir: output,
	})
	if err == nil || string(readFile(t, marker)) != "preserve" {
		t.Fatalf("existing output handling error=%v", err)
	}
}

func TestBuildClockRejectsBackdatedExpiredAdmission(t *testing.T) {
	created := time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC)
	input := candidateStream(t, created, "https://docs.example.org/a")
	var candidate indexpackselection.Candidate
	if err := json.Unmarshal(bytes.TrimSpace(input), &candidate); err != nil {
		t.Fatal(err)
	}
	expired := created.Add(time.Minute).Format(time.RFC3339)
	candidate.Admission.Robots.ValidUntil = expired
	candidate.Admission.Indexing.ValidUntil = expired
	candidate.Admission.Rights.ValidUntil = expired
	input, err := json.Marshal(candidate)
	if err != nil {
		t.Fatal(err)
	}
	input = append(input, '\n')
	identityRaw, _, err := GenerateIdentity("publisher", bytes.NewReader(bytes.Repeat([]byte{6}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(identityRaw)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "bundle")
	_, err = build(context.Background(), Options{
		SpecRaw: builderSpec(t, created, input), ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`),
		Candidates: bytes.NewReader(input), Identity: identity, BuilderVersion: "test", OutputDir: output,
	}, func() time.Time { return created.Add(10 * time.Minute) })
	if !errors.Is(err, indexpackselection.ErrInvalidCandidate) {
		t.Fatalf("build error = %v", err)
	}
	if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expired-admission build left output: %v", statErr)
	}
}

func TestDescriptorAnchoredCleanupDoesNotFollowReplacedAncestor(t *testing.T) {
	base := t.TempDir()
	parent := filepath.Join(base, "publisher")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(parent)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	const staging = ".fetchmark-pack-build-test"
	if err := root.Mkdir(staging, 0o700); err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(base, "publisher-moved")
	if err := os.Rename(parent, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(parent, staging), 0o700); err != nil {
		t.Fatal(err)
	}
	replacementMarker := filepath.Join(parent, staging, "unrelated")
	if err := os.WriteFile(replacementMarker, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := removeStaging(root, staging); err != nil {
		t.Fatal(err)
	}
	if string(readFile(t, replacementMarker)) != "preserve" {
		t.Fatal("descriptor-anchored cleanup changed replacement path")
	}
	if _, err := os.Lstat(filepath.Join(moved, staging)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("original staging still exists: %v", err)
	}
}

func TestBuildFailsIfAnchoredOutputParentIsReplaced(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Second)
	input := candidateStream(t, created, "https://docs.example.org/a")
	specRaw := builderSpec(t, created, input)
	identityRaw, _, err := GenerateIdentity("publisher", bytes.NewReader(bytes.Repeat([]byte{8}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(identityRaw)
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	parent := filepath.Join(base, "publisher")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	reader := &blockingReader{reader: bytes.NewReader(input), started: make(chan struct{}), release: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, buildErr := Build(context.Background(), Options{
			SpecRaw: specRaw, ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`), Candidates: reader,
			Identity: identity, BuilderVersion: "test", OutputDir: filepath.Join(parent, "bundle"),
		})
		result <- buildErr
	}()
	<-reader.started
	moved := filepath.Join(base, "publisher-moved")
	if err := os.Rename(parent, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(parent, "replacement-owner-data")
	if err := os.WriteFile(marker, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	close(reader.release)
	if err := <-result; err == nil || !strings.Contains(err.Error(), "output parent path changed") {
		t.Fatalf("Build error = %v", err)
	}
	if string(readFile(t, marker)) != "preserve" {
		t.Fatal("build changed replacement output parent")
	}
	if _, err := os.Lstat(filepath.Join(moved, "bundle")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("bundle activated under moved parent: %v", err)
	}
	entries, err := os.ReadDir(moved)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".fetchmark-pack-") {
			t.Fatalf("failed build left private state %q", entry.Name())
		}
	}
}

func TestActivationNeverReplacesDestination(t *testing.T) {
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := root.Mkdir("source", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := root.Mkdir("destination", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := activateNoReplace(root, "source", "destination"); !errors.Is(err, os.ErrExist) {
		t.Fatalf("activateNoReplace error = %v", err)
	}
	if _, err := root.Stat("source"); err != nil {
		t.Fatalf("source was moved despite conflict: %v", err)
	}
	if _, err := root.Stat("destination"); err != nil {
		t.Fatalf("destination was replaced: %v", err)
	}
}

func TestIdentityAndReportSignatureFailClosed(t *testing.T) {
	raw, _, err := GenerateIdentity("publisher", bytes.NewReader(bytes.Repeat([]byte{3}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(raw)
	if err != nil {
		t.Fatal(err)
	}
	var corruptDocument identityDocument
	if err := json.Unmarshal(raw, &corruptDocument); err != nil {
		t.Fatal(err)
	}
	corruptKey, err := base64.StdEncoding.DecodeString(corruptDocument.Ed25519PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	corruptKey[0] ^= 1
	corruptDocument.Ed25519PrivateKey = base64.StdEncoding.EncodeToString(corruptKey)
	corruptSeed, err := json.Marshal(corruptDocument)
	if err != nil {
		t.Fatal(err)
	}
	for name, candidate := range map[string][]byte{
		"unknown":      bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"unknown":true`), 1),
		"duplicate":    bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		"bad key":      bytes.Replace(raw, []byte(identity.keyID), []byte(strings.Repeat("a", 64)), 1),
		"corrupt seed": corruptSeed,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeIdentity(candidate); err == nil {
				t.Fatal("invalid identity accepted")
			}
		})
	}
	shardDigest := strings.Repeat("6", 64)
	report, err := json.Marshal(BuildReport{
		Version: BuildReportVersion, Builder: "fetchmark-pack-build", BuilderVersion: "test",
		Profile: indexpackselection.ProfileFormatPrototype, PublisherID: "publisher", KeyID: identity.keyID,
		ManifestSHA256: strings.Repeat("1", 64), PackID: "pack", Revision: 1, CreatedAt: "2026-07-19T00:00:00Z",
		Selection: indexpackselection.Report{
			Version: indexpackselection.ReportVersion, PolicySHA256: strings.Repeat("2", 64), ExclusionsSHA256: strings.Repeat("3", 64), CandidateSHA256: strings.Repeat("4", 64),
			Candidates: 1, Accepted: 1, UniqueHosts: 1, RejectedByReason: map[string]uint64{}, AcceptedByLanguage: map[string]uint64{"eng": 1}, PermissionOldestAge: 1,
			PermissionValidUntil: "2026-07-19T01:00:00Z",
		},
		Shards: []ShardEvidence{{
			Path: indexpack.ShardPath(shardDigest), RecordCount: 1, CompressedSizeBytes: 1, UncompressedSizeBytes: 1,
			SHA256: shardDigest, UncompressedSHA256: strings.Repeat("7", 64),
		}},
		SelectedStreamSHA256: strings.Repeat("5", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	signature, err := signReport(report, identity)
	if err != nil || VerifyBuildReport(report, signature, identity.PublicKey()) != nil {
		t.Fatalf("valid report signature: %v", err)
	}
	tampered := append([]byte(nil), report...)
	tampered[len(tampered)-1] ^= 1
	if err := VerifyBuildReport(tampered, signature, identity.PublicKey()); err == nil {
		t.Fatal("tampered report accepted")
	}
	invalidAccounting := bytes.Replace(report, []byte(`"candidates":1`), []byte(`"candidates":2`), 1)
	invalidSignature, err := signReport(invalidAccounting, identity)
	if err != nil || VerifyBuildReport(invalidAccounting, invalidSignature, identity.PublicKey()) == nil {
		t.Fatalf("signed invalid accounting accepted: %v", err)
	}
}

func candidateStream(t *testing.T, created time.Time, urls ...string) []byte {
	t.Helper()
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	for index, rawURL := range urls {
		candidate := indexpackselection.Candidate{
			URLKey: "org,example)/", Timestamp: "20260701000000", URL: rawURL,
			MIME: "text/html", MIMEDetected: "text/html", Status: "200",
			Digest: strings.Repeat("A", 32), Length: "12000", Offset: fmt.Sprintf("%d", 1000+index*12000),
			Filename:  fmt.Sprintf("crawl-data/CC-MAIN-2026-26/segments/fixture/warc/CC-MAIN-20260701000000-%05d.warc.gz", index),
			Languages: "eng", Encoding: "UTF-8",
			Admission: admission(rawURL, created.Add(-time.Hour)),
		}
		if err := encoder.Encode(candidate); err != nil {
			t.Fatal(err)
		}
	}
	return output.Bytes()
}

func admission(rawURL string, checked time.Time) indexpackselection.Admission {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}
	return indexpackselection.Admission{
		Version: 1,
		Robots: indexpackselection.RobotsObservation{
			UserAgent: "Fetchmark-PackBuilder/1", RobotsURI: parsed.Scheme + "://" + parsed.Host + "/robots.txt",
			CheckedAt: checked.Format(time.RFC3339), ValidUntil: checked.Add(24 * time.Hour).Format(time.RFC3339),
			Outcome: "allowed", BodySHA256: strings.Repeat("1", 64),
		},
		Indexing: indexpackselection.IndexingObservation{
			CheckedAt: checked.Format(time.RFC3339), ValidUntil: checked.Add(24 * time.Hour).Format(time.RFC3339),
			FinalURL: rawURL, Outcome: "indexable", HeadersSHA256: strings.Repeat("2", 64),
			RepresentationSHA256: strings.Repeat("3", 64), ParserVersion: "fetchmark-noindex-v1",
		},
		Rights: indexpackselection.RightsDecision{
			Outcome: "permitted", AllowedFields: []string{"url_metadata"}, Basis: "url_metadata_policy",
			EvidenceURI: "https://commoncrawl.org/terms-of-use", EvidenceSHA256: strings.Repeat("4", 64),
			ObservedAt: checked.Format(time.RFC3339), ValidUntil: checked.Add(24 * time.Hour).Format(time.RFC3339),
			RightsNotice: "URL metadata only; third-party rights remain applicable",
		},
	}
}

func builderSpec(t *testing.T, created time.Time, input []byte) []byte {
	t.Helper()
	digest := sha256.Sum256(input)
	spec := indexpackselection.Spec{
		Version: indexpackselection.SpecVersion, Profile: indexpackselection.ProfileLightweight,
		PackID: "commoncrawl-url-eng", Revision: 1, CreatedAt: created.Format(time.RFC3339),
		ExpiresAt: created.Add(30 * 24 * time.Hour).Format(time.RFC3339),
		Publisher: indexpack.Publisher{
			Name: "Fetchmark test publisher", ContactURI: "mailto:publisher@example.org",
			TakedownURI: "https://publisher.example.org/takedown", RightsNotice: "URL metadata only; no page bodies",
		},
		Languages: []string{"eng"}, MaxRecords: 10_000, MaxRecordsPerHost: 10,
		MaxPermissionAgeHours: 24, RobotsUserAgent: "Fetchmark-PackBuilder/1", CandidateSHA256: hex.EncodeToString(digest[:]),
		Inputs: []indexpack.BuildInput{{
			Name: "CC-MAIN-2026-26 URL Index export", URI: "https://index.commoncrawl.org/CC-MAIN-2026-26-index",
			RetrievedAt: created.Add(-time.Hour).Format(time.RFC3339), SHA256: strings.Repeat("8", 64),
			RightsNotice: "Common Crawl terms; URL metadata only",
		}},
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func builderEvidenceReport(t *testing.T, created time.Time, input []byte) []byte {
	t.Helper()
	records := uint64(bytes.Count(input, []byte{'\n'}))
	candidates := packevidence.ArtifactDescriptor{
		Path: packevidence.CandidatesFilename, SHA256: indexpack.Digest(input), Bytes: uint64(len(input)), Records: records,
	}
	partitionInput := packevidence.ArtifactDescriptor{
		Path: "shards/part-000001.jsonl", SHA256: strings.Repeat("a", 64), Bytes: 100, Records: records,
	}
	report := packevidence.MergeReport{
		Version: packevidence.MergeReportVersion, Merger: "fetchmark-pack-evidence", MergerVersion: "test-merger",
		Algorithm: packevidence.MergeAlgorithm, PartitionManifestSHA256: strings.Repeat("b", 64),
		StartedAt: created.Add(-10 * time.Minute).Format(time.RFC3339), CompletedAt: created.Add(-5 * time.Minute).Format(time.RFC3339),
		ConfigSHA256: strings.Repeat("c", 64), RightsEvidenceSHA256: strings.Repeat("4", 64), RobotsUserAgent: "Fetchmark-PackBuilder/1",
		CollectorVersion: "test-collector",
		Rights: &packevidence.MergeRights{
			AllowedFields: []string{"url_metadata"}, Basis: "url_metadata_policy", EvidenceURI: "https://commoncrawl.org/terms-of-use",
			EvidenceSHA256: strings.Repeat("4", 64), RightsNotice: "URL metadata only; third-party rights remain applicable",
		},
		PermissionValidUntil: created.Add(23 * time.Hour).Format(time.RFC3339),
		Input:                packevidence.ArtifactDescriptor{SHA256: strings.Repeat("d", 64), Bytes: 100, Records: records},
		Candidates:           candidates,
		Sources: []packevidence.MergeSource{{
			Partition: 1, FirstRow: 1, LastRow: records, Input: partitionInput, ReportSHA256: strings.Repeat("e", 64),
			Candidates:   candidates,
			Observations: packevidence.ArtifactDescriptor{Path: packevidence.ObservationsFilename, SHA256: strings.Repeat("f", 64), Bytes: 100, Records: records},
			Outcomes:     map[string]uint64{"admitted": records}, Rejections: map[string]uint64{},
		}},
		Outcomes: map[string]uint64{"admitted": records}, Rejections: map[string]uint64{},
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func deltaBuilderSpec(t *testing.T, created, expires time.Time, input []byte) []byte {
	t.Helper()
	var spec indexpackselection.Spec
	if err := indexpack.DecodeStrictJSON(builderSpec(t, created, input), &spec); err != nil {
		t.Fatal(err)
	}
	spec.Revision = 2
	spec.ExpiresAt = expires.UTC().Format(time.RFC3339)
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func buildTestBundle(t *testing.T, created time.Time) (string, PublicIdentity, Identity) {
	t.Helper()
	input := candidateStream(t, created, "https://docs.example.org/a")
	identityRaw, public, err := GenerateIdentity("publisher", bytes.NewReader(bytes.Repeat([]byte{11}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(identityRaw)
	if err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(t.TempDir(), "bundle")
	if _, err := Build(context.Background(), Options{
		SpecRaw: builderSpec(t, created, input), ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`),
		Candidates: bytes.NewReader(input), Identity: identity, BuilderVersion: "test", OutputDir: bundle,
	}); err != nil {
		t.Fatal(err)
	}
	return bundle, public, identity
}

func rewriteSignedBundleMetadata(t *testing.T, bundle string, identity Identity, mutate func(*indexpack.Manifest, *BuildReport)) {
	t.Helper()
	manifest, err := indexpack.DecodeManifest(readFile(t, filepath.Join(bundle, ManifestFilename)))
	if err != nil {
		t.Fatal(err)
	}
	var report BuildReport
	if err := indexpack.DecodeStrictJSON(readFile(t, filepath.Join(bundle, BuildReportFilename)), &report); err != nil {
		t.Fatal(err)
	}
	mutate(&manifest, &report)
	writeSignedBundleMetadata(t, bundle, identity, manifest, report)
}

func writeSignedBundleMetadata(t *testing.T, bundle string, identity Identity, manifest indexpack.Manifest, report BuildReport) {
	t.Helper()
	manifestRaw, err := indexpack.EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestSignature, err := indexpack.SignManifest(manifestRaw, identity.privateKey)
	if err != nil {
		t.Fatal(err)
	}
	manifestSignatureRaw, err := indexpack.EncodeSignature(manifestSignature)
	if err != nil {
		t.Fatal(err)
	}
	report.ManifestSHA256 = indexpack.ManifestDigest(manifestRaw)
	reportRaw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	reportSignatureRaw, err := signReport(reportRaw, identity)
	if err != nil {
		t.Fatal(err)
	}
	for relative, raw := range map[string][]byte{
		ManifestFilename: manifestRaw, SignatureFilename: manifestSignatureRaw,
		BuildReportFilename: reportRaw, ReportSignatureFilename: reportSignatureRaw,
	} {
		if err := os.WriteFile(filepath.Join(bundle, relative), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func rewriteSignedBuildReport(t *testing.T, bundle string, identity Identity, mutate func(*BuildReport)) {
	t.Helper()
	var report BuildReport
	if err := indexpack.DecodeStrictJSON(readFile(t, filepath.Join(bundle, BuildReportFilename)), &report); err != nil {
		t.Fatal(err)
	}
	mutate(&report)
	reportRaw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	reportSignatureRaw, err := signReport(reportRaw, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, BuildReportFilename), reportRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, ReportSignatureFilename), reportSignatureRaw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func rewriteSignedFirstShard(t *testing.T, bundle string, identity Identity, mutate func(*indexpack.Record)) {
	t.Helper()
	manifest, err := indexpack.DecodeManifest(readFile(t, filepath.Join(bundle, ManifestFilename)))
	if err != nil {
		t.Fatal(err)
	}
	var report BuildReport
	if err := indexpack.DecodeStrictJSON(readFile(t, filepath.Join(bundle, BuildReportFilename)), &report); err != nil {
		t.Fatal(err)
	}
	descriptor := manifest.Shards[0]
	decoder, err := zstd.NewReader(bytes.NewReader(readFile(t, filepath.Join(bundle, filepath.FromSlash(descriptor.Path)))))
	if err != nil {
		t.Fatal(err)
	}
	uncompressed, err := io.ReadAll(decoder)
	decoder.Close()
	if err != nil {
		t.Fatal(err)
	}
	var record indexpack.Record
	if err := indexpack.DecodeStrictJSON(bytes.TrimSuffix(uncompressed, []byte("\n")), &record); err != nil {
		t.Fatal(err)
	}
	mutate(&record)
	recordRaw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	recordRaw = append(recordRaw, '\n')
	var compressed bytes.Buffer
	encoder, err := zstd.NewWriter(&compressed, zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.Write(recordRaw); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	compressedDigest := indexpack.Digest(compressed.Bytes())
	descriptor.Path = indexpack.ShardPath(compressedDigest)
	descriptor.SHA256 = compressedDigest
	descriptor.CompressedSizeBytes = uint64(compressed.Len())
	descriptor.UncompressedSHA256 = indexpack.Digest(recordRaw)
	descriptor.UncompressedSizeBytes = uint64(len(recordRaw))
	manifest.Shards[0] = descriptor
	report.Shards[0] = ShardEvidence{
		Path: descriptor.Path, RecordCount: descriptor.RecordCount,
		CompressedSizeBytes: descriptor.CompressedSizeBytes, UncompressedSizeBytes: descriptor.UncompressedSizeBytes,
		SHA256: descriptor.SHA256, UncompressedSHA256: descriptor.UncompressedSHA256,
	}
	report.SelectedStreamSHA256 = indexpack.Digest(recordRaw)
	shardPath := filepath.Join(bundle, filepath.FromSlash(descriptor.Path))
	if err := os.MkdirAll(filepath.Dir(shardPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(shardPath, compressed.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	writeSignedBundleMetadata(t, bundle, identity, manifest, report)
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type blockingReader struct {
	reader  *bytes.Reader
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (reader *blockingReader) Read(buffer []byte) (int, error) {
	reader.once.Do(func() { close(reader.started) })
	<-reader.release
	return reader.reader.Read(buffer)
}
