package openpackbuilder

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/tufchannel"
	"github.com/staticvar/fetchmark/internal/adapters/tufrepository"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

func TestRealSnapshotDeltaPublicationAdvancesExistingTUFConsumer(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	created := now.Add(-2 * time.Minute)
	targetCreated := now.Add(-time.Minute)
	root := t.TempDir()

	publisherRaw, publicIdentity, err := GenerateIdentity("publisher", bytes.NewReader(bytes.Repeat([]byte{41}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := DecodeIdentity(publisherRaw)
	if err != nil {
		t.Fatal(err)
	}
	parentInput := candidateStream(t, created, "https://docs.example.org/parent")
	parentBundle := filepath.Join(root, "snapshot-bundle")
	parentResult, err := Build(ctx, Options{
		SpecRaw: builderSpec(t, created, parentInput), ExclusionsRaw: []byte(`{"version":1,"urls":[],"host_suffixes":[]}`),
		Candidates: bytes.NewReader(parentInput), Identity: publisher, BuilderVersion: "integration-test", OutputDir: parentBundle,
	})
	if err != nil {
		t.Fatalf("Build snapshot: %v", err)
	}
	parentVerification, err := VerifyBundle(ctx, parentBundle, publicIdentity, publisher.PublicKey())
	if err != nil {
		t.Fatalf("VerifyBundle: %v", err)
	}
	if parentVerification.ManifestSHA256 != parentResult.ManifestSHA256 {
		t.Fatalf("snapshot verification = %#v, build = %#v", parentVerification, parentResult)
	}

	targetInput := candidateStream(t, targetCreated, "https://docs.example.org/successor")
	deltaBundle := filepath.Join(root, "delta-bundle")
	deltaResult, err := BuildDelta(ctx, DeltaOptions{
		ParentBundleDir: parentBundle,
		SpecRaw:         deltaBuilderSpec(t, targetCreated, created.Add(30*24*time.Hour), targetInput),
		ExclusionsRaw:   []byte(`{"version":1,"urls":[],"host_suffixes":[]}`),
		Candidates:      bytes.NewReader(targetInput), Identity: publisher, BuilderVersion: "integration-test", OutputDir: deltaBundle,
	})
	if err != nil {
		t.Fatalf("BuildDelta: %v", err)
	}
	deltaVerification, err := VerifyDeltaBundle(ctx, parentBundle, deltaBundle, publicIdentity, publisher.PublicKey())
	if err != nil {
		t.Fatalf("VerifyDeltaBundle: %v", err)
	}
	if deltaVerification.ManifestSHA256 != deltaResult.ManifestSHA256 ||
		deltaVerification.ParentManifestSHA256 != parentVerification.ManifestSHA256 ||
		deltaVerification.Revision != 2 {
		t.Fatalf("delta verification = %#v, build = %#v", deltaVerification, deltaResult)
	}

	channelRandom := make([]byte, 256)
	for index := range channelRandom {
		channelRandom[index] = byte(index)
	}
	channelRaw, _, err := tufrepository.GenerateIdentity("fetchmark-community", now.Add(365*24*time.Hour), bytes.NewReader(channelRandom))
	if err != nil {
		t.Fatal(err)
	}
	channelIdentity, err := tufrepository.DecodeIdentity(channelRaw)
	if err != nil {
		t.Fatal(err)
	}
	trustedPublisherKeys := map[string]ed25519.PublicKey{publicIdentity.KeyID: publisher.PublicKey()}
	targetPath := "packs/developer/manifest.json"
	repositoryV1 := filepath.Join(root, "repository-v1")
	stageV1, err := tufrepository.Stage(ctx, tufrepository.StageOptions{
		Identity: channelIdentity, BundleDir: parentBundle, OutputDir: repositoryV1, TargetPath: targetPath, Version: 1,
		Now: now, TimestampExpires: now.Add(24 * time.Hour), SnapshotExpires: now.Add(48 * time.Hour),
		TargetsExpires: now.Add(10 * 24 * time.Hour), PublisherKeys: trustedPublisherKeys,
		ExpectedManifestSHA256: parentVerification.ManifestSHA256,
	})
	if err != nil {
		t.Fatalf("Stage snapshot: %v", err)
	}
	if stageV1.ManifestSHA256 != parentVerification.ManifestSHA256 || stageV1.Kind != indexpack.KindSnapshot {
		t.Fatalf("snapshot stage = %#v", stageV1)
	}

	consumerState := filepath.Join(root, "consumer-state")
	if err := os.Mkdir(consumerState, 0o700); err != nil {
		t.Fatal(err)
	}
	consumer := tufchannel.Options{
		TrustedRootPath: filepath.Join(repositoryV1, "root.json"), MetadataDir: filepath.Join(repositoryV1, "metadata"),
		StateDir: consumerState, BundleDir: parentBundle, TargetPath: targetPath,
	}
	selectedV1, err := tufchannel.Select(ctx, consumer)
	if err != nil {
		t.Fatalf("Select snapshot: %v", err)
	}
	if selectedV1.Revision != 1 || selectedV1.Kind != indexpack.KindSnapshot || selectedV1.ManifestSHA256 != parentVerification.ManifestSHA256 {
		t.Fatalf("snapshot selection = %#v", selectedV1)
	}

	repositoryV2 := filepath.Join(root, "repository-v2")
	stageV2, err := tufrepository.Stage(ctx, tufrepository.StageOptions{
		Identity: channelIdentity, BundleDir: deltaBundle, PreviousRepositoryDir: repositoryV1,
		OutputDir: repositoryV2, TargetPath: targetPath, Version: 2, Now: now.Add(time.Minute),
		TimestampExpires: now.Add(24*time.Hour + time.Minute), SnapshotExpires: now.Add(48*time.Hour + time.Minute),
		TargetsExpires: now.Add(10*24*time.Hour + time.Minute), PublisherKeys: trustedPublisherKeys,
		ExpectedManifestSHA256: deltaVerification.ManifestSHA256,
	})
	if err != nil {
		t.Fatalf("Stage delta: %v", err)
	}
	if stageV2.ManifestSHA256 != deltaVerification.ManifestSHA256 || stageV2.Kind != indexpack.KindDelta || stageV2.Revision != 2 {
		t.Fatalf("delta stage = %#v", stageV2)
	}
	consumer.MetadataDir = filepath.Join(repositoryV2, "metadata")
	consumer.BundleDir = deltaBundle
	selectedV2, err := tufchannel.Select(ctx, consumer)
	if err != nil {
		t.Fatalf("Select delta: %v", err)
	}
	if selectedV2.Status != tufchannel.StatusSelected || selectedV2.Kind != indexpack.KindDelta || selectedV2.Revision != 2 ||
		selectedV2.ManifestSHA256 != deltaVerification.ManifestSHA256 ||
		selectedV2.ParentManifestSHA256 != parentVerification.ManifestSHA256 || selectedV2.TimestampVersion != 2 {
		t.Fatalf("delta selection = %#v", selectedV2)
	}
}
