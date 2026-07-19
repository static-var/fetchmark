package tufrepository

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/tufchannel"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

const testTargetPath = "packs/developer/manifest.json"

func TestStageCreatesConsumerCompatibleConsistentSnapshotRepository(t *testing.T) {
	fixture := newStageFixture(t)
	result, err := Stage(context.Background(), fixture.stageOptions(1, ""))
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	if result.Status != StatusStaged || result.Path != fixture.output || result.Version != 1 || result.ManifestSHA256 != fixture.manifestDigest {
		t.Fatalf("stage result = %#v", result)
	}
	for _, relative := range []string{
		"root.json", "metadata/1.root.json", "metadata/timestamp.json",
		"metadata/1.snapshot.json", "metadata/1.targets.json",
		"targets/packs/developer/" + fixture.manifestDigest + ".manifest.json",
		"targets/packs/developer/" + fixture.manifestDigest + ".manifest.ed25519",
		"targets/packs/developer/" + fixture.manifest.Shards[0].Path,
	} {
		info, statErr := os.Lstat(filepath.Join(fixture.output, filepath.FromSlash(relative)))
		if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Fatalf("staged file %s = %v, %v", relative, info, statErr)
		}
	}
	for _, forbidden := range []string{"metadata/snapshot.json", "metadata/targets.json", "targets/packs/developer/manifest.json"} {
		if _, statErr := os.Lstat(filepath.Join(fixture.output, filepath.FromSlash(forbidden))); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("unexpected inconsistent-snapshot file %s: %v", forbidden, statErr)
		}
	}

	state := filepath.Join(fixture.root, "consumer-state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	selection, err := tufchannel.Select(context.Background(), tufchannel.Options{
		TrustedRootPath: filepath.Join(fixture.output, "root.json"),
		MetadataDir:     filepath.Join(fixture.output, "metadata"), StateDir: state,
		BundleDir: fixture.bundle, TargetPath: testTargetPath,
	})
	if err != nil {
		t.Fatalf("consumer Select: %v", err)
	}
	if selection.Status != tufchannel.StatusSelected || selection.ManifestSHA256 != fixture.manifestDigest || selection.RootVersion != 1 || selection.TargetsVersion != 1 {
		t.Fatalf("consumer selection = %#v", selection)
	}
	if _, err := Stage(context.Background(), fixture.stageOptions(1, "")); !errors.Is(err, ErrStageExists) {
		t.Fatalf("second Stage = %v, want ErrStageExists", err)
	}
}

func TestStageSuccessorAndWithdrawalAdvanceExistingConsumer(t *testing.T) {
	fixture := newStageFixture(t)
	first := fixture.stageOptions(1, "")
	if _, err := Stage(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(fixture.root, "consumer-state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	consumer := tufchannel.Options{
		TrustedRootPath: filepath.Join(fixture.output, "root.json"), MetadataDir: filepath.Join(fixture.output, "metadata"),
		StateDir: state, BundleDir: fixture.bundle, TargetPath: testTargetPath,
	}
	if selected, err := tufchannel.Select(context.Background(), consumer); err != nil || selected.Status != tufchannel.StatusSelected {
		t.Fatalf("initial selection = %#v, %v", selected, err)
	}

	replay := fixture.stageOptions(2, fixture.output)
	replay.OutputDir = filepath.Join(fixture.root, "repository-v2-replay")
	if _, err := Stage(context.Background(), replay); !errors.Is(err, ErrInvalidStage) ||
		!strings.Contains(err.Error(), "revision and manifest digest advance") {
		t.Fatalf("semantic rollback Stage = %v", err)
	}

	withdrawnOutput := filepath.Join(fixture.root, "repository-v2-withdrawn")
	withdrawal := fixture.stageOptions(2, fixture.output)
	withdrawal.OutputDir = withdrawnOutput
	withdrawal.Withdraw = true
	withdrawal.BundleDir = ""
	withdrawal.PublisherKeys = nil
	withdrawal.ExpectedManifestSHA256 = ""
	result, err := Stage(context.Background(), withdrawal)
	if err != nil {
		t.Fatalf("withdrawal Stage: %v", err)
	}
	if result.Status != StatusWithdrawn || result.Version != 2 || result.ManifestSHA256 != "" {
		t.Fatalf("withdrawal result = %#v", result)
	}
	consumer.MetadataDir = filepath.Join(withdrawnOutput, "metadata")
	consumer.BundleDir = ""
	selected, err := tufchannel.Select(context.Background(), consumer)
	if err != nil {
		t.Fatalf("withdrawn Select: %v", err)
	}
	if selected.Status != tufchannel.StatusAbsent || selected.TargetsVersion != 2 || selected.TimestampVersion != 2 {
		t.Fatalf("withdrawn selection = %#v", selected)
	}
	reintroduce := fixture.stageOptions(3, withdrawnOutput)
	reintroduce.OutputDir = filepath.Join(fixture.root, "repository-v3-reintroduced")
	if _, err := Stage(context.Background(), reintroduce); !errors.Is(err, ErrInvalidStage) ||
		!strings.Contains(err.Error(), "reintroducing a withdrawn target is not supported") {
		t.Fatalf("withdrawn target reintroduction Stage = %v", err)
	}

	bad := fixture.stageOptions(4, fixture.output)
	bad.OutputDir = filepath.Join(fixture.root, "repository-bad-version")
	if _, err := Stage(context.Background(), bad); !errors.Is(err, ErrInvalidStage) {
		t.Fatalf("non-sequential Stage = %v, want ErrInvalidStage", err)
	}
}

func TestStageSuccessorPublishesStrictlyNewerPackToExistingConsumer(t *testing.T) {
	fixture := newStageFixture(t)
	if _, err := Stage(context.Background(), fixture.stageOptions(1, "")); err != nil {
		t.Fatal(err)
	}
	firstOutput := fixture.output
	firstDigest := fixture.manifestDigest
	state := filepath.Join(fixture.root, "consumer-state-successor")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	consumer := tufchannel.Options{
		TrustedRootPath: filepath.Join(fixture.output, "root.json"), MetadataDir: filepath.Join(fixture.output, "metadata"),
		StateDir: state, BundleDir: fixture.bundle, TargetPath: testTargetPath,
	}
	if selected, err := tufchannel.Select(context.Background(), consumer); err != nil || selected.Revision != 1 {
		t.Fatalf("initial selection = %#v, %v", selected, err)
	}
	previous := fixture.output
	fixture.advanceSnapshot(t, 2)
	fixture.output = filepath.Join(fixture.root, "repository-v2")
	result, err := Stage(context.Background(), fixture.stageOptions(2, previous))
	if err != nil {
		t.Fatalf("successor Stage: %v", err)
	}
	if result.Revision != 2 || result.ManifestSHA256 != fixture.manifestDigest {
		t.Fatalf("successor result = %#v", result)
	}
	for output, digest := range map[string]string{firstOutput: firstDigest, fixture.output: fixture.manifestDigest} {
		signaturePath := filepath.Join(output, "targets", "packs", "developer", digest+".manifest.ed25519")
		if info, statErr := os.Stat(signaturePath); statErr != nil || !info.Mode().IsRegular() {
			t.Fatalf("generation signature %s = %v, %v", signaturePath, info, statErr)
		}
	}
	if firstDigest == fixture.manifestDigest {
		t.Fatal("successor reused the initial manifest digest")
	}
	consumer.MetadataDir = filepath.Join(fixture.output, "metadata")
	selected, err := tufchannel.Select(context.Background(), consumer)
	if err != nil {
		t.Fatalf("successor Select: %v", err)
	}
	if selected.Status != tufchannel.StatusSelected || selected.Revision != 2 || selected.ManifestSHA256 != fixture.manifestDigest ||
		selected.TimestampVersion != 2 || selected.TargetsVersion != 2 {
		t.Fatalf("successor selection = %#v", selected)
	}
}

func TestStageCancellationImmediatelyBeforeActivationPublishesNothing(t *testing.T) {
	fixture := newStageFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	dependencies := defaultStageDependencies()
	dependencies.beforeActivate = cancel
	if _, err := stageWithDependencies(ctx, fixture.stageOptions(1, ""), dependencies); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Stage = %v, want context.Canceled", err)
	}
	if _, err := os.Lstat(fixture.output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled stage output exists: %v", err)
	}
	assertNoStageTemporary(t, fixture.root)
}

func TestStageRevalidatesMinimumPublicationHorizonBeforeActivation(t *testing.T) {
	fixture := newStageFixture(t)
	dependencies := defaultStageDependencies()
	dependencies.now = func() time.Time {
		return fixture.now.Add(24*time.Hour - MinimumPublicationHorizon + time.Second)
	}
	if _, err := stageWithDependencies(context.Background(), fixture.stageOptions(1, ""), dependencies); !errors.Is(err, ErrInvalidStage) ||
		!strings.Contains(err.Error(), "after final staging") {
		t.Fatalf("late expiry Stage = %v", err)
	}
	if _, err := os.Lstat(fixture.output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("late expiry stage output exists: %v", err)
	}
	assertNoStageTemporary(t, fixture.root)
}

func TestStageRejectsInsufficientPublicationHorizonBeforeWork(t *testing.T) {
	fixture := newStageFixture(t)
	options := fixture.stageOptions(1, "")
	options.TimestampExpires = fixture.now.Add(MinimumPublicationHorizon - time.Second)
	if _, err := Stage(context.Background(), options); !errors.Is(err, ErrInvalidStage) ||
		!strings.Contains(err.Error(), "after staging starts") {
		t.Fatalf("short horizon Stage = %v", err)
	}
	if _, err := os.Lstat(fixture.output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("short horizon stage output exists: %v", err)
	}
}

func TestStagePostCommitFailureReturnsRecoveryEvidence(t *testing.T) {
	fixture := newStageFixture(t)
	injected := errors.New("injected parent sync failure")
	dependencies := defaultStageDependencies()
	dependencies.syncParent = func(*os.Root) error { return injected }
	result, err := stageWithDependencies(context.Background(), fixture.stageOptions(1, ""), dependencies)
	var committed *CommittedStageError
	if !errors.As(err, &committed) || !errors.Is(err, injected) {
		t.Fatalf("post-commit Stage = %#v, %v, want CommittedStageError", result, err)
	}
	if result.Path != fixture.output || committed.Result.Path != fixture.output {
		t.Fatalf("post-commit paths = %q, %q", result.Path, committed.Result.Path)
	}
	for _, evidence := range []string{result.Path, result.RootSHA256, result.TargetsSHA256, result.SnapshotSHA256, result.TimestampSHA256, result.ManifestSHA256, "inspect the committed generation"} {
		if !strings.Contains(err.Error(), evidence) {
			t.Fatalf("post-commit error missing %q: %v", evidence, err)
		}
	}
	if _, statErr := os.Stat(fixture.output); statErr != nil {
		t.Fatalf("committed stage missing: %v", statErr)
	}
	assertNoStageTemporary(t, fixture.root)
}

func TestStageRejectsTamperedShardBeforeOutput(t *testing.T) {
	fixture := newStageFixture(t)
	shardPath := filepath.Join(fixture.bundle, filepath.FromSlash(fixture.manifest.Shards[0].Path))
	tampered := []byte("portable open pack shard ByteS")
	if len(tampered) != int(fixture.manifest.Shards[0].CompressedSizeBytes) {
		t.Fatalf("tamper fixture length = %d", len(tampered))
	}
	if err := os.WriteFile(shardPath, tampered, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Stage(context.Background(), fixture.stageOptions(1, "")); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("tampered Stage = %v", err)
	}
	if _, err := os.Lstat(fixture.output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tampered stage output exists: %v", err)
	}
	assertNoStageTemporary(t, fixture.root)
}

func TestStageRejectsBundleDifferentFromFullyVerifiedManifest(t *testing.T) {
	fixture := newStageFixture(t)
	options := fixture.stageOptions(1, "")
	options.ExpectedManifestSHA256 = strings.Repeat("f", 64)
	if _, err := Stage(context.Background(), options); !errors.Is(err, ErrInvalidStage) ||
		!strings.Contains(err.Error(), "differs from the exact fully verified manifest") {
		t.Fatalf("mismatched verified digest Stage = %v", err)
	}
	if _, err := os.Lstat(fixture.output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("mismatched verified digest output exists: %v", err)
	}
}

func TestStageRejectsTargetsMetadataThatOutlivesManifest(t *testing.T) {
	fixture := newStageFixture(t)
	options := fixture.stageOptions(1, "")
	manifestExpires, err := time.Parse(time.RFC3339, fixture.manifest.ExpiresAt)
	if err != nil {
		t.Fatal(err)
	}
	options.TargetsExpires = manifestExpires.Add(time.Second)
	if _, err := Stage(context.Background(), options); !errors.Is(err, ErrInvalidStage) ||
		!strings.Contains(err.Error(), "must not outlive") {
		t.Fatalf("outliving targets metadata Stage = %v", err)
	}
	if _, err := os.Lstat(fixture.output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("outliving targets metadata output exists: %v", err)
	}
}

func TestStageRejectsSymlinkedBundlePathComponents(t *testing.T) {
	fixture := newStageFixture(t)
	original := filepath.Join(fixture.bundle, "shards")
	real := filepath.Join(fixture.bundle, "real-shards")
	if err := os.Rename(original, real); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("real-shards", original); err != nil {
		t.Fatal(err)
	}
	if _, err := Stage(context.Background(), fixture.stageOptions(1, "")); !errors.Is(err, ErrInvalidStage) ||
		!strings.Contains(err.Error(), "is a symlink") {
		t.Fatalf("symlinked Stage = %v, want ErrInvalidStage", err)
	}
	if _, err := os.Lstat(fixture.output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("symlinked stage output exists: %v", err)
	}
}

func TestStageProducesByteIdenticalMetadataForSameGeneration(t *testing.T) {
	fixture := newStageFixture(t)
	first := fixture.stageOptions(1, "")
	if _, err := Stage(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	secondOutput := filepath.Join(fixture.root, "repository-v1-repeat")
	second := fixture.stageOptions(1, "")
	second.OutputDir = secondOutput
	if _, err := Stage(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	for _, relative := range []string{"root.json", "metadata/1.root.json", "metadata/1.targets.json", "metadata/1.snapshot.json", "metadata/timestamp.json"} {
		left, err := os.ReadFile(filepath.Join(fixture.output, relative))
		if err != nil {
			t.Fatal(err)
		}
		right, err := os.ReadFile(filepath.Join(secondOutput, relative))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(left, right) {
			t.Fatalf("metadata differs for %s", relative)
		}
	}
}

func TestStageCarriesRootChainAndSignsWithOperationalIdentity(t *testing.T) {
	fixture := newStageFixture(t)
	firstOutput := fixture.output
	if _, err := Stage(context.Background(), fixture.stageOptions(1, "")); err != nil {
		t.Fatal(err)
	}
	bootstrapRaw, err := fixture.identity.BootstrapRoot()
	if err != nil {
		t.Fatal(err)
	}
	newPrivate, newPublic := generateRootSigners(t, fixture.identity.repositoryID)
	candidateRaw, proposal, err := PrepareRootRotation(
		fixture.identity.repositoryID, bootstrapRaw, newPublic, fixture.now.Add(2*365*24*time.Hour), fixture.now,
	)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := SignRootRotationWithLegacyIdentity(
		bootstrapRaw, candidateRaw, indexpack.Digest(bootstrapRaw), proposal.CandidateSHA256, fixture.identity, fixture.now,
	)
	if err != nil {
		t.Fatal(err)
	}
	contributions := append(legacy, signWithRootSignerDocuments(
		t, fixture.identity.repositoryID, bootstrapRaw, candidateRaw, proposal.CandidateSHA256, newPrivate, fixture.now,
	)...)
	signedRootRaw, _, err := AssembleRootRotation(
		fixture.identity.repositoryID, bootstrapRaw, candidateRaw, indexpack.Digest(bootstrapRaw),
		proposal.CandidateSHA256, contributions, fixture.now,
	)
	if err != nil {
		t.Fatal(err)
	}
	operationalRaw, err := ApplyRootRotation(
		fixture.identity, signedRootRaw, indexpack.Digest(bootstrapRaw), indexpack.Digest(signedRootRaw), fixture.now,
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture.identity, err = DecodeIdentity(operationalRaw)
	if err != nil {
		t.Fatal(err)
	}
	fixture.advanceSnapshot(t, 2)
	fixture.output = filepath.Join(fixture.root, "repository-v2-rotated")
	result, err := Stage(context.Background(), fixture.stageOptions(2, firstOutput))
	if err != nil {
		t.Fatalf("Stage rotated generation: %v", err)
	}
	if result.RootVersion != 2 || result.RootSHA256 != indexpack.Digest(signedRootRaw) {
		t.Fatalf("rotated stage result = %#v", result)
	}
	for relative, expected := range map[string][]byte{
		"root.json":            bootstrapRaw,
		"metadata/1.root.json": bootstrapRaw,
		"metadata/2.root.json": signedRootRaw,
	} {
		actual, err := os.ReadFile(filepath.Join(fixture.output, filepath.FromSlash(relative)))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(actual, expected) {
			t.Fatalf("%s differs from the identity chain", relative)
		}
	}
	state := filepath.Join(fixture.root, "rotated-stage-consumer-state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	selected, err := tufchannel.Select(context.Background(), tufchannel.Options{
		TrustedRootPath: filepath.Join(fixture.output, "root.json"), MetadataDir: filepath.Join(fixture.output, "metadata"),
		StateDir: state, BundleDir: fixture.bundle, TargetPath: testTargetPath,
	})
	if err != nil || selected.RootVersion != 2 || selected.TimestampVersion != 2 {
		t.Fatalf("Select rotated stage = %#v, %v", selected, err)
	}
}

type stageFixture struct {
	root             string
	bundle           string
	output           string
	now              time.Time
	identity         Identity
	publisherKey     ed25519.PublicKey
	publisherPrivate ed25519.PrivateKey
	manifest         indexpack.Manifest
	manifestDigest   string
}

func newStageFixture(t *testing.T) stageFixture {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	identityRaw, _, err := GenerateIdentity("fetchmark-community", now.Add(365*24*time.Hour), deterministicIdentityRandom())
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(identityRaw)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	shardRaw := []byte("portable open pack shard bytes")
	shardDigest := indexpack.ShardDigest(shardRaw)
	manifest := indexpack.Manifest{
		Version: indexpack.Version, Kind: indexpack.KindSnapshot, PackID: "developer-en", Revision: 1,
		CreatedAt: now.Add(-time.Hour).Format(time.RFC3339), ExpiresAt: now.Add(30 * 24 * time.Hour).Format(time.RFC3339),
		SigningKeyID: indexpack.KeyID(publicKey),
		Publisher:    indexpack.Publisher{Name: "Example Publisher", ContactURI: "mailto:operator@example.com", TakedownURI: "https://example.com/takedown", RightsNotice: "CC0-1.0"},
		Policy:       indexpack.Policy{Robots: "rfc9309", NoIndex: "exclude", BodyDistribution: "forbidden"},
		Languages:    []string{"en"}, RecordCount: 1,
		Shards: []indexpack.Shard{{Path: indexpack.ShardPath(shardDigest), Compression: "zstd", SHA256: shardDigest, CompressedSizeBytes: uint64(len(shardRaw)), UncompressedSHA256: strings.Repeat("5", 64), UncompressedSizeBytes: 100, RecordCount: 1}},
		Build:  indexpack.Build{Generator: "fixture", GeneratorVersion: "1", Analyzer: "url-derived-lexical", AnalyzerVersion: "1", PolicySHA256: strings.Repeat("6", 64), ExclusionsSHA256: strings.Repeat("7", 64), Inputs: []indexpack.BuildInput{{Name: "fixture", URI: "https://example.com/input", RetrievedAt: now.Add(-2 * time.Hour).Format(time.RFC3339), SHA256: strings.Repeat("2", 64), RightsNotice: "CC0-1.0"}}},
	}
	manifestRaw, err := indexpack.EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	signature, err := indexpack.SignManifest(manifestRaw, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	signatureRaw, err := indexpack.EncodeSignature(signature)
	if err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(root, "bundle")
	if err := os.MkdirAll(filepath.Join(bundle, filepath.FromSlash(filepath.Dir(manifest.Shards[0].Path))), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string][]byte{"manifest.json": manifestRaw, "manifest.ed25519": signatureRaw, manifest.Shards[0].Path: shardRaw} {
		path := filepath.Join(bundle, filepath.FromSlash(name))
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return stageFixture{root: root, bundle: bundle, output: filepath.Join(root, "repository-v1"), now: now, identity: identity, publisherKey: publicKey, publisherPrivate: privateKey, manifest: manifest, manifestDigest: indexpack.ManifestDigest(manifestRaw)}
}

func (fixture stageFixture) stageOptions(version int64, previous string) StageOptions {
	return StageOptions{
		Identity: fixture.identity, BundleDir: fixture.bundle, OutputDir: fixture.output,
		PreviousRepositoryDir: previous, TargetPath: testTargetPath, Version: version, Now: fixture.now,
		TimestampExpires: fixture.now.Add(24 * time.Hour), SnapshotExpires: fixture.now.Add(48 * time.Hour),
		TargetsExpires:         fixture.now.Add(30 * 24 * time.Hour),
		PublisherKeys:          map[string]ed25519.PublicKey{indexpack.KeyID(fixture.publisherKey): fixture.publisherKey},
		ExpectedManifestSHA256: fixture.manifestDigest,
	}
}

func (fixture *stageFixture) advanceSnapshot(t *testing.T, revision uint64) {
	t.Helper()
	fixture.manifest.Revision = revision
	fixture.manifest.CreatedAt = fixture.now.Add(-30 * time.Minute).Format(time.RFC3339)
	manifestRaw, err := indexpack.EncodeManifest(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	signature, err := indexpack.SignManifest(manifestRaw, fixture.publisherPrivate)
	if err != nil {
		t.Fatal(err)
	}
	signatureRaw, err := indexpack.EncodeSignature(signature)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixture.bundle, "manifest.json"), manifestRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixture.bundle, "manifest.ed25519"), signatureRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	fixture.manifestDigest = indexpack.ManifestDigest(manifestRaw)
}

func assertNoStageTemporary(t *testing.T, directory string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".fetchmark-tuf-stage-") {
			t.Fatalf("staging directory remains: %s", entry.Name())
		}
	}
}
