package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/staticvar/fetchmark/internal/adapters/ccindex"
	"github.com/staticvar/fetchmark/internal/adapters/openpackbuilder"
	"github.com/staticvar/fetchmark/internal/adapters/packevidence"
	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/adapters/tufrepository"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/indexpackselection"
)

func TestKeygenCreatesPrivateIdentityWithoutPrintingSecret(t *testing.T) {
	output := filepath.Join(t.TempDir(), "publisher.json")
	deps := defaultDependencies()
	deps.random = bytes.NewReader(bytes.Repeat([]byte{5}, 64))
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"keygen", "-publisher-id", "publisher.one", "-out", output}, &stdout, &stderr, deps)
	if code != 0 || stderr.Len() != 0 || !strings.Contains(stdout.String(), `"publisher_id":"publisher.one"`) || strings.Contains(stdout.String(), "private") {
		t.Fatalf("keygen code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	info, err := os.Stat(output)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("identity mode: %v %#o", err, info.Mode().Perm())
	}
	raw, err := secureconfigfile.Read(output, secureconfigfile.Options{MaxBytes: openpackbuilder.MaxIdentityBytes, Mode: secureconfigfile.PrivateIdentity})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openpackbuilder.DecodeIdentity(raw); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), []string{"keygen", "-publisher-id", "publisher.one", "-out", output}, &stdout, &stderr, deps); code != 1 || string(raw) != string(mustRead(t, output)) {
		t.Fatalf("second keygen code=%d stderr=%q", code, stderr.String())
	}
}

func TestChannelKeygenCreatesPrivateThresholdIdentityWithoutPrintingSecret(t *testing.T) {
	now := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	output := filepath.Join(t.TempDir(), "channel-identity.json")
	randomBytes := make([]byte, 256)
	for index := range randomBytes {
		randomBytes[index] = byte(index)
	}
	deps := defaultDependencies()
	deps.now = func() time.Time { return now }
	deps.random = bytes.NewReader(randomBytes)
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{
		"channel-keygen", "-repository-id", "fetchmark-community",
		"-root-expires", now.Add(365 * 24 * time.Hour).Format(time.RFC3339), "-out", output,
	}, &stdout, &stderr, deps)
	var keygenResult channelIdentityResult
	decodeErr := json.Unmarshal(stdout.Bytes(), &keygenResult)
	if code != 0 || stderr.Len() != 0 || decodeErr != nil || keygenResult.RootThreshold != 2 ||
		strings.Contains(stdout.String(), "ed25519_private_key") || strings.Contains(stdout.String(), "bootstrap_root") {
		t.Fatalf("channel-keygen code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	raw, err := secureconfigfile.Read(output, secureconfigfile.Options{MaxBytes: tufrepository.MaxIdentityBytes, Mode: secureconfigfile.PrivateIdentity})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := tufrepository.DecodeIdentity(raw)
	if err != nil || identity.RepositoryID() != "fetchmark-community" {
		t.Fatalf("channel identity = %#v, %v", identity, err)
	}
	failedOutput := filepath.Join(filepath.Dir(output), "channel-identity-result-failed.json")
	deps.random = deterministicChannelRandom()
	stderr.Reset()
	code = run(context.Background(), []string{
		"channel-keygen", "-repository-id", "fetchmark-community",
		"-root-expires", now.Add(365 * 24 * time.Hour).Format(time.RFC3339), "-out", failedOutput,
	}, failWriter{}, &stderr, deps)
	canonicalFailedParent, err := secureconfigfile.ValidateDirectory(filepath.Dir(failedOutput))
	if err != nil {
		t.Fatal(err)
	}
	canonicalFailedOutput := filepath.Join(canonicalFailedParent, filepath.Base(failedOutput))
	if code != 1 || !strings.Contains(stderr.String(), ErrChannelIdentityCommitted.Error()) ||
		!strings.Contains(stderr.String(), canonicalFailedOutput) || !strings.Contains(stderr.String(), "root_sha256=") ||
		!strings.Contains(stderr.String(), "before retrying or removing") {
		t.Fatalf("channel-keygen result failure code=%d stderr=%q", code, stderr.String())
	}
	if _, err := os.Stat(failedOutput); err != nil {
		t.Fatalf("committed channel identity missing: %v", err)
	}
}

func TestChannelStageFullyVerifiesSnapshotBeforeRepositoryStaging(t *testing.T) {
	now := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	base := t.TempDir()
	identityRaw, _, err := tufrepository.GenerateIdentity("fetchmark-community", now.Add(365*24*time.Hour), deterministicChannelRandom())
	if err != nil {
		t.Fatal(err)
	}
	_, publisherPublic, err := openpackbuilder.GenerateIdentity("publisher", bytes.NewReader(bytes.Repeat([]byte{9}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	publicRaw, err := json.Marshal(publisherPublic)
	if err != nil {
		t.Fatal(err)
	}
	manifestRaw := channelStageManifest(t, now, publisherPublic.KeyID)
	paths := map[string]string{
		"identity": filepath.Join(base, "channel-identity.json"), "public": filepath.Join(base, "publisher-public.json"),
		"bundle": filepath.Join(base, "bundle"), "output": filepath.Join(base, "repository-v1"),
	}
	deps := defaultDependencies()
	deps.now = func() time.Time { return now }
	deps.readSecure = func(path string, options secureconfigfile.Options) ([]byte, error) {
		switch path {
		case paths["identity"]:
			if options.Mode != secureconfigfile.PrivateIdentity {
				t.Fatalf("channel identity mode = %v", options.Mode)
			}
			return identityRaw, nil
		case paths["public"]:
			return publicRaw, nil
		case filepath.Join(paths["bundle"], openpackbuilder.ManifestFilename):
			return manifestRaw, nil
		default:
			return nil, fmt.Errorf("unexpected secure read %s", path)
		}
	}
	verified := false
	deps.verify = func(_ context.Context, bundle string, identity openpackbuilder.PublicIdentity, key ed25519.PublicKey) (openpackbuilder.Verification, error) {
		verified = true
		if bundle != paths["bundle"] || identity.KeyID != publisherPublic.KeyID || len(key) != 32 {
			t.Fatalf("snapshot verifier inputs = %q %#v %d", bundle, identity, len(key))
		}
		return openpackbuilder.Verification{ManifestSHA256: indexpack.ManifestDigest(manifestRaw)}, nil
	}
	staged := false
	deps.stageChannel = func(_ context.Context, options tufrepository.StageOptions) (tufrepository.StageResult, error) {
		staged = true
		if options.OutputDir != paths["output"] || options.TargetPath != "packs/developer/manifest.json" ||
			options.Version != 1 || options.Withdraw || len(options.PublisherKeys) != 1 || options.Now != now ||
			options.ExpectedManifestSHA256 != indexpack.ManifestDigest(manifestRaw) {
			t.Fatalf("stage options = %#v", options)
		}
		return tufrepository.StageResult{Status: tufrepository.StatusStaged, Path: options.OutputDir, Version: options.Version}, nil
	}
	args := []string{
		"channel-stage", "-identity", paths["identity"], "-bundle", paths["bundle"],
		"-public-identity", paths["public"], "-target", "packs/developer/manifest.json",
		"-metadata-version", "1", "-timestamp-expires", now.Add(24 * time.Hour).Format(time.RFC3339),
		"-snapshot-expires", now.Add(48 * time.Hour).Format(time.RFC3339),
		"-targets-expires", now.Add(30 * 24 * time.Hour).Format(time.RFC3339), "-out", paths["output"],
	}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), args, &stdout, &stderr, deps)
	if code != 0 || stderr.Len() != 0 || !verified || !staged || !strings.Contains(stdout.String(), `"status":"staged"`) {
		t.Fatalf("channel-stage code=%d stdout=%q stderr=%q verified=%v staged=%v", code, stdout.String(), stderr.String(), verified, staged)
	}
}

func TestChannelStageRoutesDeltaThroughFullVerifierAndBindsExactDigest(t *testing.T) {
	now := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	base := t.TempDir()
	identityRaw, _, err := tufrepository.GenerateIdentity("fetchmark-community", now.Add(365*24*time.Hour), deterministicChannelRandom())
	if err != nil {
		t.Fatal(err)
	}
	_, publisherPublic, err := openpackbuilder.GenerateIdentity("publisher", bytes.NewReader(bytes.Repeat([]byte{9}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	publicRaw, err := json.Marshal(publisherPublic)
	if err != nil {
		t.Fatal(err)
	}
	manifestRaw := channelStageManifestKind(t, now, publisherPublic.KeyID, indexpack.KindDelta)
	paths := map[string]string{
		"identity": filepath.Join(base, "channel-identity.json"), "public": filepath.Join(base, "publisher-public.json"),
		"parent": filepath.Join(base, "snapshot-bundle"), "bundle": filepath.Join(base, "delta-bundle"),
		"previous": filepath.Join(base, "repository-v1"), "output": filepath.Join(base, "repository-v2"),
	}
	deps := defaultDependencies()
	deps.now = func() time.Time { return now }
	deps.readSecure = func(path string, _ secureconfigfile.Options) ([]byte, error) {
		switch path {
		case paths["identity"]:
			return identityRaw, nil
		case paths["public"]:
			return publicRaw, nil
		case filepath.Join(paths["bundle"], openpackbuilder.ManifestFilename):
			return manifestRaw, nil
		default:
			return nil, fmt.Errorf("unexpected secure read %s", path)
		}
	}
	deps.verify = func(context.Context, string, openpackbuilder.PublicIdentity, ed25519.PublicKey) (openpackbuilder.Verification, error) {
		t.Fatal("delta channel stage called snapshot verifier")
		return openpackbuilder.Verification{}, nil
	}
	verified := false
	manifestDigest := indexpack.ManifestDigest(manifestRaw)
	deps.verifyDelta = func(_ context.Context, parent, bundle string, identity openpackbuilder.PublicIdentity, key ed25519.PublicKey) (openpackbuilder.DeltaVerification, error) {
		verified = true
		if parent != paths["parent"] || bundle != paths["bundle"] || identity.KeyID != publisherPublic.KeyID || len(key) != ed25519.PublicKeySize {
			t.Fatalf("delta verifier inputs = %q %q %#v %d", parent, bundle, identity, len(key))
		}
		return openpackbuilder.DeltaVerification{ManifestSHA256: manifestDigest}, nil
	}
	staged := false
	deps.stageChannel = func(_ context.Context, options tufrepository.StageOptions) (tufrepository.StageResult, error) {
		staged = true
		if options.BundleDir != paths["bundle"] || options.PreviousRepositoryDir != paths["previous"] ||
			options.ExpectedManifestSHA256 != manifestDigest || options.Version != 2 || options.Withdraw {
			t.Fatalf("delta stage options = %#v", options)
		}
		return tufrepository.StageResult{Status: tufrepository.StatusStaged, Path: options.OutputDir, Version: options.Version}, nil
	}
	args := []string{
		"channel-stage", "-identity", paths["identity"], "-bundle", paths["bundle"], "-parent-bundle", paths["parent"],
		"-public-identity", paths["public"], "-previous", paths["previous"], "-target", "packs/developer/manifest.json",
		"-metadata-version", "2", "-timestamp-expires", now.Add(24 * time.Hour).Format(time.RFC3339),
		"-snapshot-expires", now.Add(48 * time.Hour).Format(time.RFC3339),
		"-targets-expires", now.Add(30 * 24 * time.Hour).Format(time.RFC3339), "-out", paths["output"],
	}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), args, &stdout, &stderr, deps)
	if code != 0 || stderr.Len() != 0 || !verified || !staged {
		t.Fatalf("delta channel-stage code=%d stdout=%q stderr=%q verified=%v staged=%v", code, stdout.String(), stderr.String(), verified, staged)
	}
}

func TestChannelStageWithdrawalSkipsBundleAndPreservesCommittedRecoveryEvidence(t *testing.T) {
	now := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	base := t.TempDir()
	identityRaw, _, err := tufrepository.GenerateIdentity("fetchmark-community", now.Add(365*24*time.Hour), deterministicChannelRandom())
	if err != nil {
		t.Fatal(err)
	}
	identityPath := filepath.Join(base, "channel-identity.json")
	previous := filepath.Join(base, "generation-1")
	output := filepath.Join(base, "generation-2")
	deps := defaultDependencies()
	deps.now = func() time.Time { return now }
	deps.readSecure = func(path string, options secureconfigfile.Options) ([]byte, error) {
		if path != identityPath || options.Mode != secureconfigfile.PrivateIdentity {
			t.Fatalf("unexpected withdrawal read %s %#v", path, options)
		}
		return identityRaw, nil
	}
	deps.verify = func(context.Context, string, openpackbuilder.PublicIdentity, ed25519.PublicKey) (openpackbuilder.Verification, error) {
		t.Fatal("withdrawal called snapshot verifier")
		return openpackbuilder.Verification{}, nil
	}
	deps.verifyDelta = func(context.Context, string, string, openpackbuilder.PublicIdentity, ed25519.PublicKey) (openpackbuilder.DeltaVerification, error) {
		t.Fatal("withdrawal called delta verifier")
		return openpackbuilder.DeltaVerification{}, nil
	}
	injected := errors.New("injected committed sync failure")
	deps.stageChannel = func(_ context.Context, options tufrepository.StageOptions) (tufrepository.StageResult, error) {
		if !options.Withdraw || options.BundleDir != "" || len(options.PublisherKeys) != 0 || options.PreviousRepositoryDir != previous {
			t.Fatalf("withdrawal stage options = %#v", options)
		}
		result := tufrepository.StageResult{
			Status: tufrepository.StatusWithdrawn, Path: output, Version: 2,
			RootSHA256: strings.Repeat("a", 64), TargetsSHA256: strings.Repeat("b", 64),
			SnapshotSHA256: strings.Repeat("c", 64), TimestampSHA256: strings.Repeat("d", 64),
		}
		return result, &tufrepository.CommittedStageError{Result: result, Cause: injected}
	}
	args := []string{
		"channel-stage", "-identity", identityPath, "-previous", previous, "-withdraw",
		"-target", "packs/developer/manifest.json", "-metadata-version", "2",
		"-timestamp-expires", now.Add(24 * time.Hour).Format(time.RFC3339),
		"-snapshot-expires", now.Add(48 * time.Hour).Format(time.RFC3339),
		"-targets-expires", now.Add(30 * 24 * time.Hour).Format(time.RFC3339), "-out", output,
	}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), args, &stdout, &stderr, deps)
	if code != 1 || stdout.Len() != 0 {
		t.Fatalf("committed withdrawal code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	for _, evidence := range []string{output, strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64), strings.Repeat("d", 64), injected.Error(), "inspect the committed generation"} {
		if !strings.Contains(stderr.String(), evidence) {
			t.Fatalf("committed withdrawal stderr missing %q: %s", evidence, stderr.String())
		}
	}

	deps.stageChannel = func(_ context.Context, options tufrepository.StageOptions) (tufrepository.StageResult, error) {
		return tufrepository.StageResult{
			Status: tufrepository.StatusWithdrawn, Path: output, Version: options.Version,
			RootSHA256: strings.Repeat("a", 64), TargetsSHA256: strings.Repeat("b", 64),
			SnapshotSHA256: strings.Repeat("c", 64), TimestampSHA256: strings.Repeat("d", 64),
		}, nil
	}
	stderr.Reset()
	code = run(context.Background(), args, failWriter{}, &stderr, deps)
	if code != 1 || !strings.Contains(stderr.String(), output) ||
		!strings.Contains(stderr.String(), strings.Repeat("a", 64)) ||
		!strings.Contains(stderr.String(), strings.Repeat("d", 64)) ||
		!strings.Contains(stderr.String(), "write stage result") ||
		!strings.Contains(stderr.String(), "inspect the committed generation") {
		t.Fatalf("channel-stage result failure code=%d stderr=%q", code, stderr.String())
	}
}

func TestChannelMirrorActivateRoutesExactCASWithoutPrivateTUFIdentity(t *testing.T) {
	base := t.TempDir()
	_, publisherPublic, err := openpackbuilder.GenerateIdentity("publisher", bytes.NewReader(bytes.Repeat([]byte{7}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	publicRaw, err := json.Marshal(publisherPublic)
	if err != nil {
		t.Fatal(err)
	}
	paths := map[string]string{
		"root": filepath.Join(base, "root.json"), "mirror": filepath.Join(base, "mirror"),
		"candidate": filepath.Join(base, "generation-1"), "public": filepath.Join(base, "publisher-public.json"),
	}
	candidateDigest := strings.Repeat("a", 64)
	deps := defaultDependencies()
	deps.readSecure = func(path string, options secureconfigfile.Options) ([]byte, error) {
		if path != paths["public"] || options.Mode != secureconfigfile.PublicConfig {
			t.Fatalf("unexpected mirror identity read %s %#v", path, options)
		}
		return publicRaw, nil
	}
	called := false
	deps.activateMirror = func(_ context.Context, options tufrepository.MirrorActivationOptions) (tufrepository.MirrorActivationResult, error) {
		called = true
		if options.TrustedRootPath != paths["root"] || options.MirrorDir != paths["mirror"] ||
			options.CandidateDir != paths["candidate"] || options.TargetPath != "packs/developer/manifest.json" ||
			!options.InitializeEmpty || options.ExpectedCurrentTimestampSHA256 != "" ||
			options.ExpectedCandidateTimestampSHA256 != candidateDigest || len(options.PublisherKeys) != 1 {
			t.Fatalf("mirror activation options = %#v", options)
		}
		return tufrepository.MirrorActivationResult{
			Status: tufrepository.MirrorStatusInitialized, Path: options.MirrorDir,
			HeadPath:          filepath.Join(options.MirrorDir, "metadata", "timestamp.json"),
			ToTimestampSHA256: candidateDigest, HeadCommitted: true,
		}, nil
	}
	args := []string{
		"channel-mirror-activate", "-trusted-root", paths["root"], "-mirror", paths["mirror"],
		"-candidate", paths["candidate"], "-target", "packs/developer/manifest.json",
		"-public-identity", paths["public"], "-expected-candidate-timestamp-sha256", candidateDigest,
		"-initialize-empty",
	}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), args, &stdout, &stderr, deps)
	if code != 0 || stderr.Len() != 0 || !called || !strings.Contains(stdout.String(), `"status":"initialized"`) ||
		!strings.Contains(stdout.String(), candidateDigest) {
		t.Fatalf("channel-mirror-activate code=%d stdout=%q stderr=%q called=%v", code, stdout.String(), stderr.String(), called)
	}
}

func TestChannelMirrorActivateRejectsAmbiguousInitialHead(t *testing.T) {
	base := t.TempDir()
	digest := strings.Repeat("a", 64)
	args := []string{
		"channel-mirror-activate", "-trusted-root", filepath.Join(base, "root.json"),
		"-mirror", filepath.Join(base, "mirror"), "-candidate", filepath.Join(base, "candidate"),
		"-target", "packs/developer/manifest.json", "-public-identity", filepath.Join(base, "public.json"),
		"-expected-current-timestamp-sha256", strings.Repeat("b", 64),
		"-expected-candidate-timestamp-sha256", digest, "-initialize-empty",
	}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), args, &stdout, &stderr, defaultDependencies())
	if code != 2 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "forbids -expected-current-timestamp-sha256") {
		t.Fatalf("ambiguous initial head code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func deterministicChannelRandom() *bytes.Reader {
	raw := make([]byte, 256)
	for index := range raw {
		raw[index] = byte(index)
	}
	return bytes.NewReader(raw)
}

func channelStageManifest(t *testing.T, now time.Time, keyID string) []byte {
	return channelStageManifestKind(t, now, keyID, indexpack.KindSnapshot)
}

func channelStageManifestKind(t *testing.T, now time.Time, keyID string, kind indexpack.Kind) []byte {
	t.Helper()
	shardDigest := strings.Repeat("1", 64)
	manifest := indexpack.Manifest{
		Version: indexpack.Version, Kind: kind, PackID: "developer-en", Revision: 1,
		CreatedAt: now.Add(-time.Hour).Format(time.RFC3339), ExpiresAt: now.Add(30 * 24 * time.Hour).Format(time.RFC3339),
		SigningKeyID: keyID,
		Publisher:    indexpack.Publisher{Name: "Publisher", ContactURI: "mailto:operator@example.com", TakedownURI: "https://example.com/takedown", RightsNotice: "CC0-1.0"},
		Policy:       indexpack.Policy{Robots: "rfc9309", NoIndex: "exclude", BodyDistribution: "forbidden"},
		Languages:    []string{"en"}, RecordCount: 1,
		Shards: []indexpack.Shard{{Path: indexpack.ShardPath(shardDigest), Compression: "zstd", SHA256: shardDigest, CompressedSizeBytes: 80, UncompressedSHA256: strings.Repeat("5", 64), UncompressedSizeBytes: 100, RecordCount: 1}},
		Build:  indexpack.Build{Generator: "fixture", GeneratorVersion: "1", Analyzer: "url-derived-lexical", AnalyzerVersion: "1", PolicySHA256: strings.Repeat("6", 64), ExclusionsSHA256: strings.Repeat("7", 64), Inputs: []indexpack.BuildInput{{Name: "fixture", URI: "https://example.com/input", RetrievedAt: now.Add(-time.Hour).Format(time.RFC3339), SHA256: strings.Repeat("2", 64), RightsNotice: "CC0-1.0"}}},
	}
	if kind == indexpack.KindDelta {
		manifest.Revision = 2
		manifest.ParentManifestSHA256 = strings.Repeat("9", 64)
	}
	raw, err := indexpack.EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestBuildRoutesSecureInputsAndCandidateStream(t *testing.T) {
	base := t.TempDir()
	request := request{
		command: "build", spec: filepath.Join(base, "spec.json"), candidates: filepath.Join(base, "candidates.jsonl"),
		exclusions: filepath.Join(base, "exclusions.json"), evidenceReport: filepath.Join(base, "merge-report.json"),
		identity: filepath.Join(base, "identity.json"), output: filepath.Join(base, "bundle"),
	}
	identityRaw, _, err := openpackbuilder.GenerateIdentity("publisher", bytes.NewReader(bytes.Repeat([]byte{6}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	reads := make(map[string]secureconfigfile.Options)
	deps := dependencies{
		builderVersion: func() (string, error) { return "test-version", nil },
		readSecure: func(path string, options secureconfigfile.Options) ([]byte, error) {
			reads[path] = options
			switch path {
			case request.spec:
				return []byte(`{"spec":true}`), nil
			case request.exclusions:
				return []byte(`{"exclusions":true}`), nil
			case request.evidenceReport:
				return []byte(`{"evidence":true}`), nil
			case request.identity:
				return identityRaw, nil
			default:
				return nil, errors.New("unexpected path")
			}
		},
		openCandidate: func(path string) (io.ReadCloser, error) {
			if path != request.candidates {
				t.Fatalf("candidate path = %q", path)
			}
			return io.NopCloser(strings.NewReader("candidate input")), nil
		},
		build: func(_ context.Context, options openpackbuilder.Options) (openpackbuilder.Result, error) {
			if string(options.SpecRaw) != `{"spec":true}` || string(options.ExclusionsRaw) != `{"exclusions":true}` || string(options.EvidenceReportRaw) != `{"evidence":true}` || options.OutputDir != request.output || options.Identity.PublisherID() != "publisher" || options.BuilderVersion != "test-version" {
				t.Fatalf("build options = %#v", options)
			}
			raw, err := io.ReadAll(options.Candidates)
			if err != nil || string(raw) != "candidate input" {
				t.Fatalf("candidate stream = %q, %v", raw, err)
			}
			return openpackbuilder.Result{PackID: "pack"}, nil
		},
	}
	result, err := executeBuild(context.Background(), request, deps)
	if err != nil || result.PackID != "pack" {
		t.Fatalf("executeBuild = %#v, %v", result, err)
	}
	if reads[request.identity].Mode != secureconfigfile.PrivateIdentity || reads[request.spec].Mode != secureconfigfile.PublicConfig || reads[request.exclusions].Mode != secureconfigfile.PublicConfig || reads[request.evidenceReport].Mode != secureconfigfile.PublicConfig || reads[request.evidenceReport].MaxBytes != packevidence.MaxMergeReportBytes {
		t.Fatalf("secure read modes = %#v", reads)
	}
}

func TestBuildDeltaRoutesExactParentSecureInputsAndCandidateStream(t *testing.T) {
	base := t.TempDir()
	request := request{
		command: "build-delta", parentBundle: filepath.Join(base, "parent"),
		spec: filepath.Join(base, "spec.json"), candidates: filepath.Join(base, "candidates.jsonl"),
		exclusions: filepath.Join(base, "exclusions.json"), identity: filepath.Join(base, "identity.json"), output: filepath.Join(base, "delta"),
	}
	identityRaw, _, err := openpackbuilder.GenerateIdentity("publisher", bytes.NewReader(bytes.Repeat([]byte{16}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	reads := make(map[string]secureconfigfile.Options)
	deps := dependencies{
		builderVersion: func() (string, error) { return "test-version", nil },
		readSecure: func(path string, options secureconfigfile.Options) ([]byte, error) {
			reads[path] = options
			switch path {
			case request.spec:
				return []byte(`{"spec":true}`), nil
			case request.exclusions:
				return []byte(`{"exclusions":true}`), nil
			case request.identity:
				return identityRaw, nil
			default:
				return nil, errors.New("unexpected path")
			}
		},
		openCandidate: func(path string) (io.ReadCloser, error) {
			if path != request.candidates {
				t.Fatalf("candidate path = %q", path)
			}
			return io.NopCloser(strings.NewReader("candidate input")), nil
		},
		buildDelta: func(_ context.Context, options openpackbuilder.DeltaOptions) (openpackbuilder.DeltaResult, error) {
			if options.ParentBundleDir != request.parentBundle || string(options.SpecRaw) != `{"spec":true}` ||
				string(options.ExclusionsRaw) != `{"exclusions":true}` || options.OutputDir != request.output ||
				options.Identity.PublisherID() != "publisher" || options.BuilderVersion != "test-version" {
				t.Fatalf("delta options = %#v", options)
			}
			raw, err := io.ReadAll(options.Candidates)
			if err != nil || string(raw) != "candidate input" {
				t.Fatalf("candidate stream = %q, %v", raw, err)
			}
			return openpackbuilder.DeltaResult{
				Path: request.output, PackID: "pack", ParentManifestSHA256: strings.Repeat("a", 64),
				ManifestSHA256: strings.Repeat("b", 64), BuildReportSHA256: strings.Repeat("c", 64),
			}, nil
		},
	}
	result, err := executeBuildDelta(context.Background(), request, deps)
	if err != nil || result.PackID != "pack" {
		t.Fatalf("executeBuildDelta = %#v, %v", result, err)
	}
	if reads[request.identity].Mode != secureconfigfile.PrivateIdentity || reads[request.spec].Mode != secureconfigfile.PublicConfig || reads[request.exclusions].Mode != secureconfigfile.PublicConfig {
		t.Fatalf("secure read modes = %#v", reads)
	}
	deps.openCandidate = func(string) (io.ReadCloser, error) {
		return closeErrorReader{Reader: strings.NewReader("candidate input")}, nil
	}
	committed, err := executeBuildDelta(context.Background(), request, deps)
	if !errors.Is(err, openpackbuilder.ErrDeltaBuildCommitted) || committed.Path != request.output ||
		!strings.Contains(err.Error(), committed.ManifestSHA256) || !strings.Contains(err.Error(), "close candidate input") {
		t.Fatalf("committed close result=%#v error=%v", committed, err)
	}
}

func TestNormalizeDoesNotSyncOrActivateFailedOperation(t *testing.T) {
	file := &recordingStagingFile{}
	operationErr := errors.New("normalize failed")
	if err := finishNormalizationFile(file, operationErr); !errors.Is(err, operationErr) || file.syncCalls != 0 || file.closeCalls != 1 {
		t.Fatalf("failed finish error=%v sync=%d close=%d", err, file.syncCalls, file.closeCalls)
	}
	file = &recordingStagingFile{}
	if err := finishNormalizationFile(file, nil); err != nil || file.syncCalls != 1 || file.closeCalls != 1 {
		t.Fatalf("successful finish error=%v sync=%d close=%d", err, file.syncCalls, file.closeCalls)
	}

	base := t.TempDir()
	output := filepath.Join(base, "canceled.jsonl")
	ctx, cancel := context.WithCancel(context.Background())
	normalize := func(_ context.Context, _ io.Reader, writer io.Writer, _ ccindex.Options) (ccindex.Report, error) {
		if _, err := writer.Write([]byte("partial\n")); err != nil {
			return ccindex.Report{}, err
		}
		cancel()
		return ccindex.Report{OutputSHA256: strings.Repeat("a", 64)}, nil
	}
	if _, err := normalizeToFile(ctx, strings.NewReader("input"), output, ccindex.FormatCDXAPIJSON, normalize); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled normalize error = %v", err)
	}
	if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled normalization activated output: %v", err)
	}
}

func TestNormalizeReportsCommittedArtifactAfterPostLinkFailures(t *testing.T) {
	artifact := []byte("normalized artifact\n")
	digest := sha256.Sum256(artifact)
	normalize := func(_ context.Context, _ io.Reader, writer io.Writer, _ ccindex.Options) (ccindex.Report, error) {
		written, err := writer.Write(artifact)
		return ccindex.Report{Records: 1, OutputBytes: uint64(written), OutputSHA256: hex.EncodeToString(digest[:])}, err
	}
	tests := map[string]func(*normalizationHooks){
		"remove staging": func(hooks *normalizationHooks) {
			hooks.removeStaging = func(*os.Root, string) error { return errors.New("remove fault") }
		},
		"sync parent": func(hooks *normalizationHooks) {
			hooks.syncParent = func(*os.Root) error { return errors.New("sync fault") }
		},
		"second parent check": func(hooks *normalizationHooks) {
			checks := 0
			hooks.checkParent = func(root *os.Root, path string) error {
				checks++
				if checks == 2 {
					return errors.New("identity fault")
				}
				return rootStillAtPath(root, path)
			}
		},
	}
	for name, inject := range tests {
		t.Run(name, func(t *testing.T) {
			base := t.TempDir()
			output := filepath.Join(base, "normalized.jsonl")
			hooks := defaultNormalizationHooks()
			inject(&hooks)
			result, err := normalizeToFileWithHooks(context.Background(), strings.NewReader("input"), output, ccindex.FormatCDXAPIJSON, normalize, hooks)
			if !errors.Is(err, ErrNormalizationCommitted) {
				t.Fatalf("post-commit error = %v", err)
			}
			if result.Path != output || result.Report.OutputSHA256 != hex.EncodeToString(digest[:]) {
				t.Fatalf("post-commit result = %#v", result)
			}
			if !bytes.Equal(mustRead(t, output), artifact) || !strings.Contains(err.Error(), output) || !strings.Contains(err.Error(), result.Report.OutputSHA256) || !strings.Contains(err.Error(), "before retrying or removing") {
				t.Fatalf("post-commit recovery evidence missing: %v", err)
			}
			assertNoNormalizationStaging(t, base)
		})
	}
}

func TestNormalizeCancellationDuringFinalParentCheckDoesNotCommit(t *testing.T) {
	base := t.TempDir()
	output := filepath.Join(base, "normalized.jsonl")
	ctx, cancel := context.WithCancel(context.Background())
	hooks := defaultNormalizationHooks()
	hooks.checkParent = func(root *os.Root, path string) error {
		if err := rootStillAtPath(root, path); err != nil {
			return err
		}
		cancel()
		return nil
	}
	normalize := func(_ context.Context, _ io.Reader, writer io.Writer, _ ccindex.Options) (ccindex.Report, error) {
		raw := []byte("normalized artifact\n")
		_, err := writer.Write(raw)
		return ccindex.Report{Records: 1, OutputSHA256: strings.Repeat("a", 64)}, err
	}
	if _, err := normalizeToFileWithHooks(ctx, strings.NewReader("input"), output, ccindex.FormatCDXAPIJSON, normalize, hooks); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
	if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancellation committed output: %v", err)
	}
	assertNoNormalizationStaging(t, base)
}

func TestExecuteNormalizeReportsCommittedArtifactWhenInputCloseFails(t *testing.T) {
	base := t.TempDir()
	output := filepath.Join(base, "normalized.jsonl")
	artifact := []byte("normalized artifact\n")
	digest := sha256.Sum256(artifact)
	deps := dependencies{
		openCandidate: func(string) (io.ReadCloser, error) {
			return closeErrorReader{Reader: strings.NewReader("input")}, nil
		},
		normalize: func(_ context.Context, _ io.Reader, writer io.Writer, _ ccindex.Options) (ccindex.Report, error) {
			written, err := writer.Write(artifact)
			return ccindex.Report{Records: 1, OutputBytes: uint64(written), OutputSHA256: hex.EncodeToString(digest[:])}, err
		},
	}
	result, err := executeNormalize(context.Background(), request{input: filepath.Join(base, "input"), output: output, format: ccindex.FormatCDXAPIJSON}, deps)
	if !errors.Is(err, ErrNormalizationCommitted) || result.Path != output || !bytes.Equal(mustRead(t, output), artifact) {
		t.Fatalf("input-close result=%#v error=%v", result, err)
	}
}

func TestRunReportsCommittedArtifactWhenResultOutputFails(t *testing.T) {
	base := t.TempDir()
	input := filepath.Join(base, "input.jsonl")
	output := filepath.Join(base, "normalized.jsonl")
	artifact := []byte("normalized artifact\n")
	digest := sha256.Sum256(artifact)
	deps := defaultDependencies()
	deps.openCandidate = func(string) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("input")), nil
	}
	deps.normalize = func(_ context.Context, _ io.Reader, writer io.Writer, _ ccindex.Options) (ccindex.Report, error) {
		written, err := writer.Write(artifact)
		return ccindex.Report{Records: 1, OutputBytes: uint64(written), OutputSHA256: hex.EncodeToString(digest[:])}, err
	}
	var stderr bytes.Buffer
	code := run(context.Background(), []string{"normalize", "-format", ccindex.FormatCDXAPIJSON, "-input", input, "-out", output}, failWriter{}, &stderr, deps)
	if code != 1 || !bytes.Equal(mustRead(t, output), artifact) || !strings.Contains(stderr.String(), ErrNormalizationCommitted.Error()) || !strings.Contains(stderr.String(), output) || !strings.Contains(stderr.String(), hex.EncodeToString(digest[:])) {
		t.Fatalf("result-output failure code=%d stderr=%q", code, stderr.String())
	}
	assertNoNormalizationStaging(t, base)
}

func TestResolvedBuilderVersionUsesLinkedRevisionOrExecutableDigest(t *testing.T) {
	original := version
	t.Cleanup(func() { version = original })
	version = "revision-123"
	if got, err := resolvedBuilderVersion(); err != nil || got != version {
		t.Fatalf("linked version = %q, %v", got, err)
	}
	version = "dev"
	got, err := resolvedBuilderVersion()
	if err != nil || !strings.HasPrefix(got, "binary-sha256:") || len(got) != len("binary-sha256:")+64 {
		t.Fatalf("development version = %q, %v", got, err)
	}
	version = " bad "
	if _, err := resolvedBuilderVersion(); err == nil {
		t.Fatal("invalid linked version accepted")
	}
}

func TestCLINormalizeCreatesExactNoOverwriteArtifact(t *testing.T) {
	base := t.TempDir()
	inputPath := filepath.Join(base, "common-crawl.jsonl")
	outputPath := filepath.Join(base, "normalized.jsonl")
	raw := []byte(`{"urlkey":"org,commoncrawl)/get-started","timestamp":"20251014220259","url":"https://www.commoncrawl.org/get-started","mime":"text/html","mime-detected":"text/html","status":"200","digest":"D4IRZZ6NS7QW37BB2ODQPCUGG7ISRFGV","length":"12675","offset":"686242195","filename":"crawl-data/CC-MAIN-2025-43/segments/fixture/warc/CC-MAIN-20251014214924-00000.warc.gz","languages":"eng","encoding":"UTF-8"}` + "\n")
	if err := os.WriteFile(inputPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	args := []string{"normalize", "-format", ccindex.FormatCDXAPIJSON, "-input", inputPath, "-out", outputPath}
	if code := run(context.Background(), args, &stdout, &stderr, defaultDependencies()); code != 0 || stderr.Len() != 0 {
		t.Fatalf("normalize code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	var result normalizeResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	output := mustRead(t, outputPath)
	if result.Path != outputPath || result.Report.Records != 1 || result.Report.InputBytes != uint64(len(raw)) || result.Report.OutputBytes != uint64(len(output)) || !strings.Contains(string(output), `"version":1`) {
		t.Fatalf("normalize result=%#v output=%q", result, output)
	}
	info, err := os.Stat(outputPath)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("normalized output mode: %v %#o", err, info.Mode().Perm())
	}
	before := append([]byte(nil), output...)
	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), args, &stdout, &stderr, defaultDependencies()); code != 1 || !bytes.Equal(before, mustRead(t, outputPath)) {
		t.Fatalf("second normalize code=%d stderr=%q", code, stderr.String())
	}
	malformedPath := filepath.Join(base, "malformed.jsonl")
	malformedOutput := filepath.Join(base, "malformed-output.jsonl")
	if err := os.WriteFile(malformedPath, bytes.Replace(raw, []byte(`"encoding":"UTF-8"`), []byte(`"encoding":"UTF-8","unknown":true`), 1), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), []string{"normalize", "-format", ccindex.FormatCDXAPIJSON, "-input", malformedPath, "-out", malformedOutput}, &stdout, &stderr, defaultDependencies()); code != 1 {
		t.Fatalf("malformed normalize code=%d stderr=%q", code, stderr.String())
	}
	if _, err := os.Lstat(malformedOutput); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("malformed normalization left output: %v", err)
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".fetchmark-normalize-") {
			t.Fatalf("normalization left staging file %q", entry.Name())
		}
	}
}

func TestCLINormalizeURLIndexParquetCreatesExactArtifact(t *testing.T) {
	base := t.TempDir()
	inputPath := filepath.Join(base, "url-index.parquet")
	outputPath := filepath.Join(base, "normalized.jsonl")
	var parquetInput bytes.Buffer
	writer := parquet.NewGenericWriter[cliURLIndexParquetRow](&parquetInput)
	digest := "D4IRZZ6NS7QW37BB2ODQPCUGG7ISRFGV"
	mime := "text/html"
	row := cliURLIndexParquetRow{
		URLKey: "org,commoncrawl)/get-started", URL: "https://www.commoncrawl.org/get-started",
		FetchTime: time.Date(2025, 10, 14, 22, 2, 59, 0, time.UTC).UnixMilli(), FetchStatus: 200,
		ContentDigest: &digest, ContentMIME: &mime,
		WARCFilename: "crawl-data/CC-MAIN-2025-43/segments/fixture/warc/CC-MAIN-20251014214924-00000.warc.gz",
		WARCOffset:   686242195, WARCLength: 12675,
	}
	if _, err := writer.Write([]cliURLIndexParquetRow{row}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(inputPath, parquetInput.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	args := []string{"normalize", "-format", ccindex.FormatURLIndexParquet, "-input", inputPath, "-out", outputPath}
	if code := run(context.Background(), args, &stdout, &stderr, defaultDependencies()); code != 0 || stderr.Len() != 0 {
		t.Fatalf("normalize code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	var result normalizeResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	inputDigest := sha256.Sum256(parquetInput.Bytes())
	if result.Report.Format != ccindex.FormatURLIndexParquet || result.Report.Records != 1 || result.Report.InputSHA256 != hex.EncodeToString(inputDigest[:]) {
		t.Fatalf("normalize result=%#v", result)
	}
	candidate, err := ccindex.DecodeCandidate(bytes.TrimSpace(mustRead(t, outputPath)))
	if err != nil {
		t.Fatal(err)
	}
	if candidate.URL != row.URL || candidate.Timestamp != "20251014220259" || candidate.Digest != digest {
		t.Fatalf("normalized candidate=%#v", candidate)
	}
}

func TestCLISelectParquetCreatesBoundedAuditableArtifact(t *testing.T) {
	base := t.TempDir()
	inputPath := filepath.Join(base, "url-index.parquet")
	outputPath := filepath.Join(base, "selected.jsonl")
	digest := "D4IRZZ6NS7QW37BB2ODQPCUGG7ISRFGV"
	mime := "text/html"
	language := "eng"
	rows := make([]cliURLIndexParquetRow, 0, 3)
	for index, host := range []string{"a.example.org", "b.example.org", "c.example.org"} {
		rows = append(rows, cliURLIndexParquetRow{
			URLKey: host + ")/page", URL: "https://" + host + "/page",
			FetchTime: time.Date(2025, 10, 14, 22, 2, 59, 0, time.UTC).UnixMilli(), FetchStatus: 200,
			ContentDigest: &digest, ContentMIME: &mime, ContentMIMEDetected: &mime, ContentLanguages: &language,
			WARCFilename: "crawl-data/CC-MAIN-2025-43/segments/fixture/warc/CC-MAIN-20251014214924-00000.warc.gz",
			WARCOffset:   int32(686242195 + index), WARCLength: 12675,
		})
	}
	var parquetInput bytes.Buffer
	writer := parquet.NewGenericWriter[cliURLIndexParquetRow](&parquetInput)
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(inputPath, parquetInput.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	args := []string{
		"select-parquet", "-input", inputPath, "-out", outputPath,
		"-max-records", "2", "-languages", "eng", "-not-after", "2026-01-01T00:00:00Z",
	}
	if code := run(context.Background(), args, &stdout, &stderr, defaultDependencies()); code != 0 || stderr.Len() != 0 {
		t.Fatalf("select code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	var result normalizeResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Path != outputPath || result.Report.Format != ccindex.FormatURLIndexParquetSelection || result.Report.Records != 2 || result.Report.Selection == nil {
		t.Fatalf("select result=%#v", result)
	}
	if result.Report.Selection.SourceRecords != 3 || result.Report.Selection.EligibleRecords != 3 || result.Report.Selection.EligibleNotSelected != 1 {
		t.Fatalf("selection report=%#v", result.Report.Selection)
	}
	if lines := bytes.Count(mustRead(t, outputPath), []byte{'\n'}); lines != 2 {
		t.Fatalf("selected rows=%d", lines)
	}
	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), args, &stdout, &stderr, defaultDependencies()); code != 1 || !strings.Contains(stderr.String(), "output already exists") {
		t.Fatalf("second select code=%d stderr=%q", code, stderr.String())
	}
}

func TestCLISelectParquetPartsCreatesDeterministicAuditableArtifact(t *testing.T) {
	base := t.TempDir()
	firstPath := filepath.Join(base, "first.parquet")
	secondPath := filepath.Join(base, "second.parquet")
	writeCLIParquetPart(t, firstPath,
		cliParquetCandidate("https://a.example.org/one", 1),
		cliParquetCandidate("https://b.example.org/page", 2),
	)
	writeCLIParquetPart(t, secondPath,
		cliParquetCandidate("https://a.example.org/two", 3),
		cliParquetCandidate("https://c.example.org/page", 4),
	)
	firstOutput := filepath.Join(base, "selected-first.jsonl")
	secondOutput := filepath.Join(base, "selected-second.jsonl")
	args := func(output string, inputs ...string) []string {
		values := []string{"select-parquet-parts"}
		for _, input := range inputs {
			values = append(values, "-input", input)
		}
		return append(values, "-out", output, "-max-records", "2", "-languages", "eng", "-not-after", "2026-01-01T00:00:00Z")
	}
	var firstStdout, stderr bytes.Buffer
	if code := run(context.Background(), args(firstOutput, firstPath, secondPath), &firstStdout, &stderr, defaultDependencies()); code != 0 || stderr.Len() != 0 {
		t.Fatalf("first selection code=%d stdout=%q stderr=%q", code, firstStdout.String(), stderr.String())
	}
	var firstResult normalizeResult
	if err := json.Unmarshal(firstStdout.Bytes(), &firstResult); err != nil {
		t.Fatal(err)
	}
	var secondStdout bytes.Buffer
	stderr.Reset()
	if code := run(context.Background(), args(secondOutput, secondPath, firstPath), &secondStdout, &stderr, defaultDependencies()); code != 0 || stderr.Len() != 0 {
		t.Fatalf("second selection code=%d stdout=%q stderr=%q", code, secondStdout.String(), stderr.String())
	}
	var secondResult normalizeResult
	if err := json.Unmarshal(secondStdout.Bytes(), &secondResult); err != nil {
		t.Fatal(err)
	}
	if firstResult.Report.Format != ccindex.FormatURLIndexParquetPartsSelection || firstResult.Report.Parts == nil ||
		firstResult.Report.Parts.InputCount != 2 || firstResult.Report.Records != 2 ||
		firstResult.Report.InputSHA256 != secondResult.Report.InputSHA256 ||
		firstResult.Report.OutputSHA256 != secondResult.Report.OutputSHA256 ||
		!bytes.Equal(mustRead(t, firstOutput), mustRead(t, secondOutput)) {
		t.Fatalf("first=%#v second=%#v", firstResult, secondResult)
	}
}

func writeCLIParquetPart(t *testing.T, path string, rows ...cliURLIndexParquetRow) {
	t.Helper()
	var raw bytes.Buffer
	writer := parquet.NewGenericWriter[cliURLIndexParquetRow](&raw)
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
}

func cliParquetCandidate(rawURL string, offset int32) cliURLIndexParquetRow {
	digest := "D4IRZZ6NS7QW37BB2ODQPCUGG7ISRFGV"
	mime := "text/html"
	language := "eng"
	return cliURLIndexParquetRow{
		URLKey: rawURL, URL: rawURL, FetchTime: time.Date(2025, 10, 14, 22, 2, 59, 0, time.UTC).UnixMilli(), FetchStatus: 200,
		ContentDigest: &digest, ContentMIME: &mime, ContentMIMEDetected: &mime, ContentLanguages: &language,
		WARCFilename: "crawl-data/CC-MAIN-2025-43/segments/fixture/warc/CC-MAIN-20251014214924-00000.warc.gz",
		WARCOffset:   offset, WARCLength: 12675,
	}
}

type cliURLIndexParquetRow struct {
	URLKey              string  `parquet:"url_surtkey"`
	URL                 string  `parquet:"url"`
	FetchTime           int64   `parquet:"fetch_time,timestamp(millisecond)"`
	FetchStatus         int16   `parquet:"fetch_status"`
	ContentDigest       *string `parquet:"content_digest"`
	ContentMIME         *string `parquet:"content_mime_type"`
	ContentMIMEDetected *string `parquet:"content_mime_detected"`
	ContentLanguages    *string `parquet:"content_languages"`
	WARCFilename        string  `parquet:"warc_filename"`
	WARCOffset          int32   `parquet:"warc_record_offset"`
	WARCLength          int32   `parquet:"warc_record_length"`
}

func TestCLIEndToEndBuildAndPublicVerification(t *testing.T) {
	base := t.TempDir()
	privatePath := filepath.Join(base, "publisher-private.json")
	publicPath := filepath.Join(base, "publisher-public.json")
	deps := defaultDependencies()
	deps.random = bytes.NewReader(bytes.Repeat([]byte{8}, 64))
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"keygen", "-publisher-id", "test-publisher", "-out", privatePath}, &stdout, &stderr, deps); code != 0 {
		t.Fatalf("keygen code=%d stderr=%q", code, stderr.String())
	}
	if err := os.WriteFile(publicPath, bytes.TrimSpace(stdout.Bytes()), 0o600); err != nil {
		t.Fatal(err)
	}

	created := time.Now().UTC().Truncate(time.Second)
	captured := created.Add(-2 * time.Hour)
	checked := created.Add(-30 * time.Minute)
	rawURL := "https://www.rfc-editor.org/rfc/rfc9309"
	candidate := indexpackselection.Candidate{
		URLKey: "org,rfc-editor)/rfc/rfc9309", Timestamp: captured.Format("20060102150405"), URL: rawURL,
		MIME: "text/html", MIMEDetected: "text/html", Status: "200", Digest: strings.Repeat("A", 32),
		Length: "12000", Offset: "1000", Filename: "crawl-data/CC-MAIN-2026-26/segments/test/warc/CC-MAIN-20260701000000-00000.warc.gz", Languages: "eng", Encoding: "UTF-8",
		Admission: indexpackselection.Admission{
			Version: 1,
			Robots: indexpackselection.RobotsObservation{
				UserAgent: "Fetchmark-PackBuilder/1", RobotsURI: "https://www.rfc-editor.org/robots.txt",
				CheckedAt: checked.Format(time.RFC3339), ValidUntil: checked.Add(2 * time.Hour).Format(time.RFC3339), Outcome: "allowed", BodySHA256: strings.Repeat("1", 64),
			},
			Indexing: indexpackselection.IndexingObservation{
				CheckedAt: checked.Format(time.RFC3339), ValidUntil: checked.Add(2 * time.Hour).Format(time.RFC3339), FinalURL: rawURL,
				Outcome: "indexable", HeadersSHA256: strings.Repeat("2", 64), RepresentationSHA256: strings.Repeat("3", 64), ParserVersion: "test-noindex-v1",
			},
			Rights: indexpackselection.RightsDecision{
				Outcome: "permitted", AllowedFields: []string{"url_metadata"}, Basis: "url_metadata_policy", EvidenceURI: "https://commoncrawl.org/terms-of-use",
				EvidenceSHA256: strings.Repeat("4", 64), ObservedAt: checked.Format(time.RFC3339), ValidUntil: checked.Add(2 * time.Hour).Format(time.RFC3339), RightsNotice: "Test URL metadata only",
			},
		},
	}
	var candidates bytes.Buffer
	if err := json.NewEncoder(&candidates).Encode(candidate); err != nil {
		t.Fatal(err)
	}
	inputDigest := sha256.Sum256(candidates.Bytes())
	spec := indexpackselection.Spec{
		Version: indexpackselection.SpecVersion, Profile: indexpackselection.ProfileLightweight, PackID: "rfc-url-test", Revision: 1,
		CreatedAt: created.Format(time.RFC3339), ExpiresAt: created.Add(24 * time.Hour).Format(time.RFC3339),
		Publisher: indexpack.Publisher{
			Name: "Fetchmark test publisher", ContactURI: "mailto:test@example.org", TakedownURI: "https://www.rfc-editor.org/", RightsNotice: "Test URL metadata only",
		},
		Languages: []string{"eng"}, MaxRecords: 10, MaxRecordsPerHost: 10, MaxPermissionAgeHours: 1, RobotsUserAgent: "Fetchmark-PackBuilder/1",
		CandidateSHA256: hex.EncodeToString(inputDigest[:]),
		Inputs: []indexpack.BuildInput{{
			Name: "test URL Index export", URI: "https://index.commoncrawl.org/CC-MAIN-2026-26-index", RetrievedAt: checked.Format(time.RFC3339),
			SHA256: strings.Repeat("5", 64), RightsNotice: "Test URL metadata only",
		}},
	}
	specRaw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{
		filepath.Join(base, "spec.json"):        specRaw,
		filepath.Join(base, "candidates.jsonl"): candidates.Bytes(),
		filepath.Join(base, "exclusions.json"):  []byte(`{"version":1,"urls":[],"host_suffixes":[]}`),
	}
	for path, raw := range files {
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	bundle := filepath.Join(base, "bundle")
	stdout.Reset()
	stderr.Reset()
	buildArgs := []string{
		"build", "-spec", filepath.Join(base, "spec.json"), "-candidates", filepath.Join(base, "candidates.jsonl"),
		"-exclusions", filepath.Join(base, "exclusions.json"), "-identity", privatePath, "-out", bundle,
	}
	if code := run(context.Background(), buildArgs, &stdout, &stderr, defaultDependencies()); code != 0 || !strings.Contains(stdout.String(), `"ordinary_installable":true`) {
		t.Fatalf("build code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), []string{"verify-run", "-bundle", bundle, "-public-identity", publicPath}, &stdout, &stderr, defaultDependencies()); code != 0 || !strings.Contains(stdout.String(), `"record_count":1`) {
		t.Fatalf("verify-run code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	for _, name := range []string{openpackbuilder.ManifestFilename, openpackbuilder.SignatureFilename, openpackbuilder.BuildReportFilename, openpackbuilder.ReportSignatureFilename} {
		if _, err := os.Stat(filepath.Join(bundle, name)); err != nil {
			t.Fatal(fmt.Errorf("missing %s: %w", name, err))
		}
	}
}

func TestParseRequestRejectsAmbiguousPathsAndFlags(t *testing.T) {
	base := t.TempDir()
	valid := []string{"build", "-spec", filepath.Join(base, "spec"), "-candidates", filepath.Join(base, "candidates"), "-exclusions", filepath.Join(base, "exclusions"), "-identity", filepath.Join(base, "identity"), "-out", filepath.Join(base, "out")}
	if request, err := parseRequest(valid, io.Discard); err != nil || request.command != "build" {
		t.Fatalf("valid request = %#v, %v", request, err)
	}
	delta := []string{"build-delta", "-parent-bundle", filepath.Join(base, "parent"), "-spec", filepath.Join(base, "spec"), "-candidates", filepath.Join(base, "candidates"), "-exclusions", filepath.Join(base, "exclusions"), "-identity", filepath.Join(base, "identity"), "-out", filepath.Join(base, "delta")}
	if request, err := parseRequest(delta, io.Discard); err != nil || request.command != "build-delta" || request.parentBundle != filepath.Join(base, "parent") {
		t.Fatalf("valid delta request = %#v, %v", request, err)
	}
	verifyDelta := []string{"verify-delta-run", "-parent-bundle", filepath.Join(base, "parent"), "-bundle", filepath.Join(base, "delta"), "-public-identity", filepath.Join(base, "public")}
	if request, err := parseRequest(verifyDelta, io.Discard); err != nil || request.command != "verify-delta-run" {
		t.Fatalf("valid delta verification request = %#v, %v", request, err)
	}
	normalize := []string{"normalize", "-format", ccindex.FormatCDXJHeader, "-input", filepath.Join(base, "raw"), "-out", filepath.Join(base, "normalized")}
	if request, err := parseRequest(normalize, io.Discard); err != nil || request.command != "normalize" || request.format != ccindex.FormatCDXJHeader {
		t.Fatalf("valid normalize request = %#v, %v", request, err)
	}
	parquetNormalize := []string{"normalize", "-format", ccindex.FormatURLIndexParquet, "-input", filepath.Join(base, "raw.parquet"), "-out", filepath.Join(base, "normalized")}
	if request, err := parseRequest(parquetNormalize, io.Discard); err != nil || request.format != ccindex.FormatURLIndexParquet {
		t.Fatalf("valid Parquet normalize request = %#v, %v", request, err)
	}
	selectParquet := []string{"select-parquet", "-input", filepath.Join(base, "raw.parquet"), "-out", filepath.Join(base, "selected"), "-max-records", "100", "-languages", "deu,eng", "-not-after", "2026-01-01T00:00:00Z"}
	if request, err := parseRequest(selectParquet, io.Discard); err != nil || request.command != "select-parquet" || request.maxRecords != 100 || !slices.Equal(request.languages, []string{"deu", "eng"}) || !request.notAfter.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("valid Parquet selection request = %#v, %v", request, err)
	}
	selectParts := []string{"select-parquet-parts", "-input", filepath.Join(base, "part-1.parquet"), "-input", filepath.Join(base, "part-2.parquet"), "-out", filepath.Join(base, "selected"), "-profile", ccindex.ParquetSelectionProfilePrototype, "-max-records", "5000000", "-languages", "eng", "-not-after", "2026-01-01T00:00:00Z"}
	if request, err := parseRequest(selectParts, io.Discard); err != nil || request.command != "select-parquet-parts" || request.selectionProfile != ccindex.ParquetSelectionProfilePrototype || request.maxRecords != ccindex.MaxParquetPrototypeSelectionRecords || len(request.inputs) != 2 {
		t.Fatalf("valid multi-part Parquet selection request = %#v, %v", request, err)
	}
	tests := [][]string{
		nil,
		{"unknown"},
		{"keygen", "-publisher-id", "publisher", "-out", "relative"},
		{"build", "-spec", filepath.Join(base, "spec")},
		{"build-delta", "-parent-bundle", filepath.Join(base, "same"), "-spec", filepath.Join(base, "spec"), "-candidates", filepath.Join(base, "candidates"), "-exclusions", filepath.Join(base, "exclusions"), "-identity", filepath.Join(base, "identity"), "-out", filepath.Join(base, "same")},
		{"verify-delta-run", "-parent-bundle", filepath.Join(base, "same"), "-bundle", filepath.Join(base, "same"), "-public-identity", filepath.Join(base, "public")},
		append(append([]string(nil), valid...), "extra"),
		append(append([]string(nil), valid...), "-out", filepath.Join(base, "second")),
		{"normalize", "-format", "unknown", "-input", filepath.Join(base, "input"), "-out", filepath.Join(base, "out")},
		{"normalize", "-format", ccindex.FormatCDXAPIJSON, "-input", filepath.Join(base, "same"), "-out", filepath.Join(base, "same")},
		{"select-parquet", "-input", filepath.Join(base, "input"), "-out", filepath.Join(base, "out"), "-max-records", "0", "-languages", "eng", "-not-after", "2026-01-01T00:00:00Z"},
		{"select-parquet", "-input", filepath.Join(base, "input"), "-out", filepath.Join(base, "out"), "-max-records", "1", "-languages", "eng,eng", "-not-after", "2026-01-01T00:00:00Z"},
		{"select-parquet", "-input", filepath.Join(base, "input"), "-out", filepath.Join(base, "out"), "-max-records", "1", "-languages", "ENG", "-not-after", "2026-01-01T00:00:00Z"},
		{"select-parquet", "-input", filepath.Join(base, "input"), "-out", filepath.Join(base, "out"), "-max-records", "1", "-languages", "eng", "-not-after", "2026-01-01T01:00:00+01:00"},
		{"select-parquet", "-input", filepath.Join(base, "input"), "-out", filepath.Join(base, "out"), "-max-records", "10001", "-languages", "eng", "-not-after", "2026-01-01T00:00:00Z"},
		{"select-parquet", "-input", filepath.Join(base, "input"), "-out", filepath.Join(base, "out"), "-profile", "unknown", "-max-records", "1", "-languages", "eng", "-not-after", "2026-01-01T00:00:00Z"},
		{"select-parquet-parts", "-input", filepath.Join(base, "one"), "-out", filepath.Join(base, "out"), "-max-records", "1", "-languages", "eng", "-not-after", "2026-01-01T00:00:00Z"},
		{"select-parquet-parts", "-input", filepath.Join(base, "same"), "-input", filepath.Join(base, "same"), "-out", filepath.Join(base, "out"), "-max-records", "1", "-languages", "eng", "-not-after", "2026-01-01T00:00:00Z"},
		{"select-parquet-parts", "-input", filepath.Join(base, "one"), "-input", filepath.Join(base, "out"), "-out", filepath.Join(base, "out"), "-max-records", "1", "-languages", "eng", "-not-after", "2026-01-01T00:00:00Z"},
	}
	for _, args := range tests {
		if _, err := parseRequest(args, io.Discard); err == nil {
			t.Fatalf("parseRequest(%q) succeeded", args)
		}
	}
}

func TestOpenStableCandidateRejectsSymlink(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "target")
	if err := os.WriteFile(target, []byte("candidate"), 0o600); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(base, "link")
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := openStableCandidate(symlink); err == nil {
		t.Fatal("symlink candidate accepted")
	}
	reader, err := openStableCandidate(target)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type recordingStagingFile struct {
	syncCalls  int
	closeCalls int
}

type closeErrorReader struct{ io.Reader }

func (closeErrorReader) Close() error { return errors.New("close input fault") }

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("write result fault") }

func (*recordingStagingFile) Write(raw []byte) (int, error) { return len(raw), nil }
func (file *recordingStagingFile) Sync() error {
	file.syncCalls++
	return nil
}
func (file *recordingStagingFile) Close() error {
	file.closeCalls++
	return nil
}

func assertNoNormalizationStaging(t *testing.T, directory string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".fetchmark-normalize-") {
			t.Fatalf("normalization left staging file %q", entry.Name())
		}
	}
}
