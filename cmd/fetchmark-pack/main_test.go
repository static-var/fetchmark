package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/staticvar/fetchmark/internal/adapters/openpackindex"
	"github.com/staticvar/fetchmark/internal/adapters/openpackregistryfile"
	"github.com/staticvar/fetchmark/internal/adapters/tufchannel"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/openpackregistry"
	"github.com/staticvar/fetchmark/internal/core/search"
)

type commandFixture struct {
	bundle       string
	registry     string
	installed    string
	root         string
	now          time.Time
	manifest     indexpack.Manifest
	manifestHash string
	privateKey   ed25519.PrivateKey
}

func TestChannelSelectDispatchesOfflineTUFInputsAndReportsSelection(t *testing.T) {
	root := t.TempDir()
	paths := map[string]string{}
	for _, name := range []string{"trusted-root.json", "metadata", "state", "bundle"} {
		paths[name] = filepath.Join(root, name)
	}
	deps := defaultDependencies()
	var captured tufchannel.Options
	deps.selectChannel = func(_ context.Context, options tufchannel.Options) (tufchannel.Selection, error) {
		captured = options
		return tufchannel.Selection{
			Status: tufchannel.StatusSelected, TargetPath: "packs/developer/manifest.json",
			PackID: "developer-en", Kind: indexpack.KindSnapshot, Revision: 3,
			ManifestSHA256: strings.Repeat("a", 64), TargetLength: 123,
			RootVersion: 2, TimestampVersion: 4, SnapshotVersion: 3, TargetsVersion: 3,
		}, nil
	}
	stdout, stderr, code := runCommand(t, deps,
		"channel-select", "-trusted-root", paths["trusted-root.json"],
		"-metadata-dir", paths["metadata"], "-state-dir", paths["state"],
		"-target", "packs/developer/manifest.json", "-bundle", paths["bundle"],
	)
	if code != 0 || stderr != "" {
		t.Fatalf("channel-select code=%d stderr=%q", code, stderr)
	}
	for _, irrelevant := range []string{"source_id", "record_count", "operation_count", "projection_bytes"} {
		if strings.Contains(stdout, `"`+irrelevant+`"`) {
			t.Fatalf("channel-select output contains irrelevant %s: %s", irrelevant, stdout)
		}
	}
	if captured.TrustedRootPath != paths["trusted-root.json"] || captured.MetadataDir != paths["metadata"] ||
		captured.StateDir != paths["state"] || captured.BundleDir != paths["bundle"] ||
		captured.TargetPath != "packs/developer/manifest.json" {
		t.Fatalf("options = %#v", captured)
	}
	var got result
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "selected" || got.PackID != "developer-en" || got.Revision != 3 || got.TargetPath != "packs/developer/manifest.json" ||
		got.RootVersion != 2 || got.TimestampVersion != 4 {
		t.Fatalf("result = %#v", got)
	}
}

func TestChannelFetchRequiresExplicitNetworkConsentAndReportsRetrievedBundle(t *testing.T) {
	const targetBaseURL = "https://packs.example.invalid/targets/"
	root := t.TempDir()
	paths := map[string]string{}
	for _, name := range []string{"trusted-root.json", "metadata", "state", "output"} {
		paths[name] = filepath.Join(root, name)
	}
	deps := defaultDependencies()
	called := false
	var captured tufchannel.RetrieveOptions
	deps.retrieveChannel = func(_ context.Context, options tufchannel.RetrieveOptions) (tufchannel.Retrieval, error) {
		called = true
		captured = options
		return tufchannel.Retrieval{
			Status: tufchannel.StatusRetrieved, Path: paths["output"], TargetPath: "packs/developer/manifest.json",
			PackID: "developer-en", Kind: indexpack.KindSnapshot, Revision: 3,
			ManifestSHA256: strings.Repeat("a", 64), ShardCount: 1, FileCount: 3, DownloadBytes: 12_345,
			RootVersion: 2, TimestampVersion: 4, SnapshotVersion: 3, TargetsVersion: 3,
		}, nil
	}
	stdout, stderr, code := runCommand(t, deps,
		"channel-fetch", "-allow-network", "-trusted-root", paths["trusted-root.json"],
		"-metadata-dir", paths["metadata"], "-state-dir", paths["state"],
		"-target", "packs/developer/manifest.json", "-target-base-url", targetBaseURL,
		"-output", paths["output"], "-max-download-bytes", "123456", "-max-shards", "2",
	)
	if code != 0 || stderr != "" || !called {
		t.Fatalf("channel-fetch code=%d stderr=%q called=%v", code, stderr, called)
	}
	if captured.TargetBaseURL != targetBaseURL || captured.OutputDir != paths["output"] ||
		captured.MaxDownloadBytes != 123456 || captured.MaxShards != 2 || captured.StateDir != paths["state"] {
		t.Fatalf("retrieve options = %#v", captured)
	}
	var got result
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "retrieved" || got.Path != paths["output"] || got.ShardCount != 1 || got.FileCount != 3 || got.DownloadBytes != 12_345 {
		t.Fatalf("channel-fetch result = %#v", got)
	}

	called = false
	stdout, stderr, code = runCommand(t, deps,
		"channel-fetch", "-trusted-root", paths["trusted-root.json"],
		"-metadata-dir", paths["metadata"], "-state-dir", paths["state"],
		"-target", "packs/developer/manifest.json", "-target-base-url", targetBaseURL,
		"-output", paths["output"],
	)
	if code != 2 || stdout != "" || called || !strings.Contains(stderr, "-allow-network is required") {
		t.Fatalf("channel-fetch without consent code=%d stdout=%q stderr=%q called=%v", code, stdout, stderr, called)
	}
}

func TestRegistryCandidateSelectsVerifiesAndWritesSuccessorWithoutInstallingObject(t *testing.T) {
	fixture := newCommandFixture(t)
	currentRegistry, err := openpackregistryfile.Load(fixture.registry)
	if err != nil {
		t.Fatal(err)
	}
	current, found := currentRegistry.Lookup("developer")
	if !found {
		t.Fatal("current binding missing")
	}
	newDigest := strings.Repeat("b", 64)
	newCreated := fixture.now.Add(time.Minute).Format(time.RFC3339)
	newExpires := fixture.now.Add(6 * time.Hour).Format(time.RFC3339)
	output := filepath.Join(filepath.Dir(fixture.registry), "registry-candidate.json")
	paths := map[string]string{
		"trusted-root": filepath.Join(fixture.root, "root.json"),
		"metadata":     filepath.Join(fixture.root, "metadata"),
		"state":        filepath.Join(fixture.root, "state"),
	}
	deps := defaultDependencies()
	deps.now = func() time.Time { return fixture.now.Add(2 * time.Minute) }
	deps.selectChannel = func(_ context.Context, options tufchannel.Options) (tufchannel.Selection, error) {
		if options.BundleDir != fixture.bundle || options.TargetPath != "packs/developer/manifest.json" {
			t.Fatalf("selection options = %#v", options)
		}
		return tufchannel.Selection{
			Status: tufchannel.StatusSelected, TargetPath: options.TargetPath,
			PackID: current.PackID, Kind: indexpack.KindSnapshot, Revision: current.Revision + 1,
			ManifestSHA256: newDigest, SigningKeyID: current.SigningKeyID,
			ManifestRecordCount: 2, CreatedAt: newCreated, ExpiresAt: newExpires,
		}, nil
	}
	verified := false
	deps.install = func(_ context.Context, options openpackindex.InstallOptions) (openpackindex.Installed, error) {
		verified = true
		if options.Acceptance.ExpectedManifestSHA256 != newDigest || options.Acceptance.ExpectedKeyID != current.SigningKeyID ||
			options.Acceptance.ExpectedRecordCount != 2 || options.Root == filepath.Dir(filepath.Dir(current.InstalledPath)) {
			t.Fatalf("candidate verification options = %#v", options)
		}
		return openpackindex.Installed{
			Path: filepath.Join(options.Root, "objects", newDigest), ManifestSHA256: newDigest,
			PackID: current.PackID, Revision: current.Revision + 1, RecordCount: 2, OperationCount: 2,
			ProjectionBytes: 1234,
		}, nil
	}
	var writtenRegistry openpackregistry.Registry
	deps.writeRegistryCandidate = func(_ context.Context, path string, candidate openpackregistry.Registry) (string, error) {
		if path != output {
			t.Fatalf("candidate output = %q", path)
		}
		writtenRegistry = candidate
		return path, nil
	}

	stdout, stderr, code := runCommand(t, deps,
		"registry-candidate", "-registry", fixture.registry, "-source", "developer",
		"-trusted-root", paths["trusted-root"], "-metadata-dir", paths["metadata"],
		"-state-dir", paths["state"], "-target", "packs/developer/manifest.json",
		"-bundle", fixture.bundle, "-out", output,
	)
	if code != 0 || stderr != "" || !verified {
		t.Fatalf("registry-candidate code=%d stderr=%q verified=%v", code, stderr, verified)
	}
	updated, found := writtenRegistry.Lookup("developer")
	if !found || updated.ManifestSHA256 != newDigest || updated.Revision != current.Revision+1 ||
		updated.InstalledPath != filepath.Join(filepath.Dir(current.InstalledPath), newDigest) {
		t.Fatalf("candidate binding = %#v", updated)
	}
	var got result
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "candidate" || got.Path != output || got.ManifestSHA256 != newDigest || got.RecordCount != 2 || got.OperationCount != 2 {
		t.Fatalf("candidate result = %#v", got)
	}
	if len(got.BaseRegistrySHA256) != 64 || len(got.CandidateRegistrySHA256) != 64 || got.BaseRegistrySHA256 == got.CandidateRegistrySHA256 {
		t.Fatalf("candidate registry digests = %#v", got)
	}
	if _, err := os.Lstat(updated.InstalledPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("candidate command installed object: %v", err)
	}
}

func TestRegistryCandidateCommittedWriteFailureReportsRecoveryEvidence(t *testing.T) {
	fixture := newCommandFixture(t)
	registry, baseDigest, err := openpackregistryfile.LoadWithDigest(fixture.registry)
	if err != nil {
		t.Fatal(err)
	}
	current, _ := registry.Lookup("developer")
	newDigest := strings.Repeat("b", 64)
	output := filepath.Join(filepath.Dir(fixture.registry), "committed-candidate.json")
	deps := defaultDependencies()
	deps.now = func() time.Time { return fixture.now.Add(2 * time.Minute) }
	deps.selectChannel = func(context.Context, tufchannel.Options) (tufchannel.Selection, error) {
		return tufchannel.Selection{
			Status: tufchannel.StatusSelected, TargetPath: "packs/developer/manifest.json",
			PackID: current.PackID, Kind: indexpack.KindSnapshot, Revision: current.Revision + 1,
			ManifestSHA256: newDigest, SigningKeyID: current.SigningKeyID,
			ManifestRecordCount: 2, CreatedAt: fixture.now.Add(time.Minute).Format(time.RFC3339),
			ExpiresAt: fixture.now.Add(6 * time.Hour).Format(time.RFC3339),
		}, nil
	}
	deps.install = func(_ context.Context, options openpackindex.InstallOptions) (openpackindex.Installed, error) {
		return openpackindex.Installed{
			Path: filepath.Join(options.Root, "objects", newDigest), ManifestSHA256: newDigest,
			PackID: current.PackID, Revision: current.Revision + 1, RecordCount: 2,
			OperationCount: 2, ProjectionBytes: 1234,
		}, nil
	}
	var candidateDigest string
	injected := errors.New("injected post-commit failure")
	deps.writeRegistryCandidate = func(_ context.Context, _ string, candidate openpackregistry.Registry) (string, error) {
		raw, encodeErr := candidate.Encode()
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		candidateDigest = indexpack.Digest(raw)
		return output, &openpackregistryfile.CommittedCandidateError{Path: output, Cause: injected}
	}

	stdout, stderr, code := runCommand(t, deps,
		"registry-candidate", "-registry", fixture.registry, "-source", "developer",
		"-trusted-root", filepath.Join(fixture.root, "root.json"),
		"-metadata-dir", filepath.Join(fixture.root, "metadata"),
		"-state-dir", filepath.Join(fixture.root, "state"),
		"-target", "packs/developer/manifest.json", "-bundle", fixture.bundle, "-out", output,
	)
	if code != 1 || stdout != "" || candidateDigest == "" {
		t.Fatalf("committed write code=%d stdout=%q stderr=%q digest=%q", code, stdout, stderr, candidateDigest)
	}
	for _, evidence := range []string{output, "base_registry_sha256=" + baseDigest, "candidate_registry_sha256=" + candidateDigest, "verify the candidate before retrying", injected.Error()} {
		if !strings.Contains(stderr, evidence) {
			t.Fatalf("committed write stderr missing %q: %s", evidence, stderr)
		}
	}
}

func TestRegistryCandidateSignedAbsenceWritesNothing(t *testing.T) {
	fixture := newCommandFixture(t)
	output := filepath.Join(filepath.Dir(fixture.registry), "absent-candidate.json")
	deps := defaultDependencies()
	deps.selectChannel = func(_ context.Context, options tufchannel.Options) (tufchannel.Selection, error) {
		if options.BundleDir != "" {
			t.Fatalf("signed absence bundle = %q, want empty", options.BundleDir)
		}
		return tufchannel.Selection{Status: tufchannel.StatusAbsent, TargetPath: options.TargetPath}, nil
	}
	deps.install = func(context.Context, openpackindex.InstallOptions) (openpackindex.Installed, error) {
		t.Fatal("signed absence called snapshot materializer")
		return openpackindex.Installed{}, nil
	}
	deps.applyDelta = func(context.Context, openpackindex.DeltaInstallOptions) (openpackindex.Installed, error) {
		t.Fatal("signed absence called delta materializer")
		return openpackindex.Installed{}, nil
	}
	deps.writeRegistryCandidate = func(context.Context, string, openpackregistry.Registry) (string, error) {
		t.Fatal("signed absence wrote a registry candidate")
		return "", nil
	}

	stdout, stderr, code := runCommand(t, deps,
		"registry-candidate", "-registry", fixture.registry, "-source", "developer",
		"-trusted-root", filepath.Join(fixture.root, "root.json"),
		"-metadata-dir", filepath.Join(fixture.root, "metadata"),
		"-state-dir", filepath.Join(fixture.root, "state"),
		"-target", "packs/developer/manifest.json", "-out", output,
	)
	if code != 0 || stderr != "" {
		t.Fatalf("signed absence code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	var got result
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != string(tufchannel.StatusAbsent) || got.TargetPath != "packs/developer/manifest.json" {
		t.Fatalf("signed absence result = %#v", got)
	}
	if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("signed absence output exists: %v", err)
	}
}

func TestRegistryCandidateRequiresDeltaProjectionCountAndNeverWritesBeforeVerification(t *testing.T) {
	fixture := newCommandFixture(t)
	baseArgs := []string{
		"registry-candidate", "-registry", fixture.registry, "-source", "developer",
		"-trusted-root", filepath.Join(fixture.root, "root.json"),
		"-metadata-dir", filepath.Join(fixture.root, "metadata"),
		"-state-dir", filepath.Join(fixture.root, "state"),
		"-target", "packs/developer/manifest.json", "-bundle", fixture.bundle,
		"-parent-bundle", fixture.bundle, "-out", filepath.Join(fixture.root, "candidate.json"),
	}
	deps := defaultDependencies()
	deps.selectChannel = func(context.Context, tufchannel.Options) (tufchannel.Selection, error) {
		return tufchannel.Selection{Status: tufchannel.StatusSelected, Kind: indexpack.KindDelta}, nil
	}
	written := false
	deps.writeRegistryCandidate = func(context.Context, string, openpackregistry.Registry) (string, error) {
		written = true
		return "", nil
	}
	stdout, stderr, code := runCommand(t, deps, baseArgs...)
	if code != 1 || stdout != "" || written || !strings.Contains(stderr, "delta requires -projection-record-count") {
		t.Fatalf("delta missing count code=%d stdout=%q stderr=%q written=%v", code, stdout, stderr, written)
	}

	deps.selectChannel = func(context.Context, tufchannel.Options) (tufchannel.Selection, error) {
		return tufchannel.Selection{}, errors.New("verification failed")
	}
	args := append(append([]string(nil), baseArgs...), "-projection-record-count", "1")
	stdout, stderr, code = runCommand(t, deps, args...)
	if code != 1 || stdout != "" || written || !strings.Contains(stderr, "verification failed") {
		t.Fatalf("failed selection code=%d stdout=%q stderr=%q written=%v", code, stdout, stderr, written)
	}
}

func TestRegistryCandidateRejectsOutputInsideProtectedInputBeforeSelection(t *testing.T) {
	fixture := newCommandFixture(t)
	output := filepath.Join(fixture.bundle, "candidate.json")
	metadataDir := t.TempDir()
	stateDir := t.TempDir()
	deps := defaultDependencies()
	called := false
	deps.selectChannel = func(context.Context, tufchannel.Options) (tufchannel.Selection, error) {
		called = true
		return tufchannel.Selection{}, nil
	}
	stdout, stderr, code := runCommand(t, deps,
		"registry-candidate", "-registry", fixture.registry, "-source", "developer",
		"-trusted-root", fixture.registry, "-metadata-dir", metadataDir,
		"-state-dir", stateDir, "-target", "packs/developer/manifest.json",
		"-bundle", fixture.bundle, "-out", output,
	)
	if code != 1 || stdout != "" || called || !strings.Contains(stderr, "candidate output must not be inside bundle") {
		t.Fatalf("protected output code=%d stdout=%q stderr=%q selected=%v", code, stdout, stderr, called)
	}
	if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("protected output exists: %v", err)
	}
}

func TestRegistryCandidateValidatesCurrentTrustBeforeAdvancingChannelState(t *testing.T) {
	fixture := newCommandFixture(t)
	called := false
	deps := defaultDependencies()
	deps.selectChannel = func(context.Context, tufchannel.Options) (tufchannel.Selection, error) {
		called = true
		return tufchannel.Selection{}, nil
	}
	stdout, stderr, code := runCommand(t, deps,
		"registry-candidate", "-registry", fixture.registry, "-source", "missing",
		"-trusted-root", fixture.registry, "-metadata-dir", t.TempDir(),
		"-state-dir", t.TempDir(), "-target", "packs/developer/manifest.json",
		"-bundle", fixture.bundle, "-out", filepath.Join(filepath.Dir(fixture.registry), "candidate.json"),
	)
	if code != 1 || stdout != "" || called || !strings.Contains(stderr, "not defined in current registry") {
		t.Fatalf("invalid current trust code=%d stdout=%q stderr=%q selected=%v", code, stdout, stderr, called)
	}
}

func TestRegistryCandidateDeltaUsesExactCurrentSnapshotAndExplicitProjectionCount(t *testing.T) {
	fixture := newCommandFixture(t)
	registry, err := openpackregistryfile.Load(fixture.registry)
	if err != nil {
		t.Fatal(err)
	}
	current, _ := registry.Lookup("developer")
	deltaDigest := strings.Repeat("b", 64)
	output := filepath.Join(filepath.Dir(fixture.registry), "delta-candidate.json")
	deps := defaultDependencies()
	deps.now = func() time.Time { return fixture.now.Add(2 * time.Minute) }
	deps.selectChannel = func(context.Context, tufchannel.Options) (tufchannel.Selection, error) {
		return tufchannel.Selection{
			Status: tufchannel.StatusSelected, TargetPath: "packs/developer/manifest.json",
			PackID: current.PackID, Kind: indexpack.KindDelta, Revision: current.Revision + 1,
			ManifestSHA256: deltaDigest, ParentManifestSHA256: current.ManifestSHA256,
			SigningKeyID: current.SigningKeyID, ManifestRecordCount: 3,
			CreatedAt: fixture.now.Add(time.Minute).Format(time.RFC3339),
			ExpiresAt: fixture.now.Add(6 * time.Hour).Format(time.RFC3339),
		}, nil
	}
	deps.applyDelta = func(_ context.Context, options openpackindex.DeltaInstallOptions) (openpackindex.Installed, error) {
		if options.ExpectedParentManifestSHA256 != current.ManifestSHA256 || options.ExpectedProjectionRecordCount != 9 ||
			options.DeltaAcceptance.ExpectedRecordCount != 3 || options.ParentBundleDir != fixture.bundle {
			t.Fatalf("delta candidate options = %#v", options)
		}
		return openpackindex.Installed{
			Path: filepath.Join(options.Root, "objects", deltaDigest), ManifestSHA256: deltaDigest,
			PackID: current.PackID, Revision: current.Revision + 1, RecordCount: 9,
			OperationCount: 3, ProjectionBytes: 2345,
		}, nil
	}
	var candidate openpackregistry.Registry
	deps.writeRegistryCandidate = func(_ context.Context, _ string, value openpackregistry.Registry) (string, error) {
		candidate = value
		return output, nil
	}
	stdout, stderr, code := runCommand(t, deps,
		"registry-candidate", "-registry", fixture.registry, "-source", "developer",
		"-trusted-root", filepath.Join(fixture.root, "root.json"),
		"-metadata-dir", filepath.Join(fixture.root, "metadata"),
		"-state-dir", filepath.Join(fixture.root, "state"),
		"-target", "packs/developer/manifest.json", "-bundle", fixture.bundle,
		"-parent-bundle", fixture.bundle, "-projection-record-count", "9", "-out", output,
	)
	if code != 0 || stderr != "" {
		t.Fatalf("delta candidate code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	binding, found := candidate.Lookup("developer")
	if !found || binding.Kind != indexpack.KindDelta || binding.ParentManifestSHA256 != current.ManifestSHA256 ||
		binding.ManifestRecordCount != 3 || binding.RecordCount != 9 {
		t.Fatalf("delta candidate binding = %#v", binding)
	}
}

func TestRegistryCandidateThenExistingVerifyAndInstallEndToEnd(t *testing.T) {
	fixture := newCommandFixture(t)
	bundle, manifest, manifestRaw := writeSnapshotRevisionBundle(t, fixture, fixture.manifest.Revision+1)
	manifestDigest := indexpack.ManifestDigest(manifestRaw)
	candidatePath := filepath.Join(filepath.Dir(fixture.registry), "registry-r2.json")
	deps := defaultDependencies()
	deps.now = func() time.Time { return fixture.now.Add(2 * time.Minute) }
	deps.selectChannel = func(_ context.Context, options tufchannel.Options) (tufchannel.Selection, error) {
		return tufchannel.Selection{
			Status: tufchannel.StatusSelected, TargetPath: options.TargetPath,
			PackID: manifest.PackID, Kind: manifest.Kind, Revision: manifest.Revision,
			ManifestSHA256: manifestDigest, SigningKeyID: manifest.SigningKeyID,
			ManifestRecordCount: manifest.RecordCount, CreatedAt: manifest.CreatedAt, ExpiresAt: manifest.ExpiresAt,
		}, nil
	}
	common := []string{
		"-registry", fixture.registry, "-source", "developer",
		"-trusted-root", filepath.Join(fixture.root, "root.json"),
		"-metadata-dir", filepath.Join(fixture.root, "metadata"),
		"-state-dir", filepath.Join(fixture.root, "state"),
		"-target", "packs/developer/manifest.json", "-bundle", bundle, "-out", candidatePath,
	}
	stdout, stderr, code := runCommand(t, deps, append([]string{"registry-candidate"}, common...)...)
	if code != 0 || stderr != "" {
		t.Fatalf("candidate code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	candidate, err := openpackregistryfile.Load(candidatePath)
	if err != nil {
		t.Fatalf("load candidate: %v", err)
	}
	binding, found := candidate.Lookup("developer")
	if !found || binding.ManifestSHA256 != manifestDigest {
		t.Fatalf("candidate binding = %#v", binding)
	}
	current, err := openpackregistryfile.Load(fixture.registry)
	if err != nil {
		t.Fatal(err)
	}
	currentBinding, _ := current.Lookup("developer")
	if currentBinding.ManifestSHA256 != fixture.manifestHash {
		t.Fatal("candidate generation changed current registry")
	}

	stdout, stderr, code = runCommand(t, deps, "verify", "-registry", candidatePath, "-source", "developer", "-bundle", bundle)
	if code != 0 || stderr != "" {
		t.Fatalf("verify candidate code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	stdout, stderr, code = runCommand(t, deps, "install", "-registry", candidatePath, "-source", "developer", "-bundle", bundle)
	if code != 0 || stderr != "" {
		t.Fatalf("install candidate code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if _, err := os.Stat(binding.InstalledPath); err != nil {
		t.Fatalf("installed candidate object: %v", err)
	}
}

func TestRegistryInstallActivateRestartRollbackEndToEnd(t *testing.T) {
	fixture := newCommandFixture(t)
	activationTime := fixture.now.Add(2 * time.Minute)
	deps := defaultDependencies()
	deps.now = func() time.Time { return activationTime }

	stdout, stderr, code := runCommand(t, deps,
		"install", "-registry", fixture.registry, "-source", "developer", "-bundle", fixture.bundle,
	)
	if code != 0 || stderr != "" {
		t.Fatalf("install current code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	oldIndex, oldBinding := openRegistryBindingIndex(t, fixture.registry, "developer", activationTime)
	oldIndexClosed := false
	t.Cleanup(func() {
		if !oldIndexClosed {
			if err := oldIndex.Close(); err != nil {
				t.Errorf("close original open index: %v", err)
			}
		}
	})

	closeOldIndex := func() {
		t.Helper()
		if err := oldIndex.Close(); err != nil {
			t.Errorf("close original open index: %v", err)
		}
		oldIndexClosed = true
	}

	bundle, manifest, manifestRaw := writeSnapshotRevisionBundle(t, fixture, fixture.manifest.Revision+1)
	manifestDigest := indexpack.ManifestDigest(manifestRaw)
	candidatePath := filepath.Join(filepath.Dir(fixture.registry), "registry-r2.json")
	deps.selectChannel = func(_ context.Context, options tufchannel.Options) (tufchannel.Selection, error) {
		return tufchannel.Selection{
			Status: tufchannel.StatusSelected, TargetPath: options.TargetPath,
			PackID: manifest.PackID, Kind: manifest.Kind, Revision: manifest.Revision,
			ManifestSHA256: manifestDigest, SigningKeyID: manifest.SigningKeyID,
			ManifestRecordCount: manifest.RecordCount, CreatedAt: manifest.CreatedAt, ExpiresAt: manifest.ExpiresAt,
		}, nil
	}
	stdout, stderr, code = runCommand(t, deps,
		"registry-candidate", "-registry", fixture.registry, "-source", "developer",
		"-trusted-root", filepath.Join(fixture.root, "root.json"),
		"-metadata-dir", filepath.Join(fixture.root, "metadata"),
		"-state-dir", filepath.Join(fixture.root, "state"),
		"-target", "packs/developer/manifest.json", "-bundle", bundle, "-out", candidatePath,
	)
	if code != 0 || stderr != "" {
		t.Fatalf("candidate code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	stdout, stderr, code = runCommand(t, deps,
		"install", "-registry", candidatePath, "-source", "developer", "-bundle", bundle,
	)
	if code != 0 || stderr != "" {
		t.Fatalf("install candidate code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}

	_, currentDigest, err := openpackregistryfile.LoadWithDigest(fixture.registry)
	if err != nil {
		t.Fatalf("load current registry digest: %v", err)
	}
	_, candidateDigest, err := openpackregistryfile.LoadWithDigest(candidatePath)
	if err != nil {
		t.Fatalf("load candidate registry digest: %v", err)
	}
	rollbackPath := filepath.Join(filepath.Dir(fixture.registry), "registry-r1.rollback.json")
	stdout, stderr, code = runCommand(t, deps,
		"registry-activate", "-registry", fixture.registry, "-source", "developer", "-candidate", candidatePath,
		"-expected-current-sha256", currentDigest, "-expected-candidate-sha256", candidateDigest,
		"-rollback-out", rollbackPath,
	)
	if code != 0 || stderr != "" {
		t.Fatalf("activate code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	assertFileDigest(t, fixture.registry, candidateDigest)
	assertFileDigest(t, rollbackPath, currentDigest)

	oldBatch, err := oldIndex.SearchBatch(context.Background(), search.Query{Q: "open pack fixture", MaxResults: 5})
	if err != nil || len(oldBatch.Hits) != 1 {
		t.Fatalf("already-open original projection after activation: hits=%d err=%v", len(oldBatch.Hits), err)
	}
	if oldBinding.Revision != fixture.manifest.Revision {
		t.Fatalf("already-open binding revision=%d, want %d", oldBinding.Revision, fixture.manifest.Revision)
	}
	closeOldIndex()
	newIndex, newBinding := openRegistryBindingIndex(t, fixture.registry, "developer", activationTime)
	if newBinding.Revision != manifest.Revision || newBinding.ManifestSHA256 != manifestDigest {
		t.Fatalf("restart binding = %#v", newBinding)
	}
	if err := newIndex.Close(); err != nil {
		t.Fatalf("close activated projection: %v", err)
	}

	rollbackArchive := filepath.Join(filepath.Dir(fixture.registry), "registry-r2.rollback.json")
	stdout, stderr, code = runCommand(t, deps,
		"registry-rollback", "-registry", fixture.registry, "-source", "developer", "-rollback", rollbackPath,
		"-expected-current-sha256", candidateDigest, "-expected-rollback-sha256", currentDigest,
		"-rollback-out", rollbackArchive,
	)
	if code != 0 || stderr != "" {
		t.Fatalf("rollback code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	assertFileDigest(t, fixture.registry, currentDigest)
	assertFileDigest(t, rollbackArchive, candidateDigest)
	restoredIndex, restoredBinding := openRegistryBindingIndex(t, fixture.registry, "developer", activationTime)
	if restoredBinding.Revision != fixture.manifest.Revision || restoredBinding.ManifestSHA256 != fixture.manifestHash {
		t.Fatalf("restored binding = %#v", restoredBinding)
	}
	if err := restoredIndex.Close(); err != nil {
		t.Fatalf("close restored projection: %v", err)
	}
	for _, path := range []string{oldBinding.InstalledPath, newBinding.InstalledPath, rollbackPath, rollbackArchive} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("preserved lifecycle artifact %q: %v", path, err)
		}
	}
}

func TestRegistryActivateAndRollbackRouteExplicitCASContracts(t *testing.T) {
	root := t.TempDir()
	registry := filepath.Join(root, "registry.json")
	candidate := filepath.Join(root, "candidate.json")
	archive := filepath.Join(root, "archive.json")
	currentDigest := strings.Repeat("a", 64)
	candidateDigest := strings.Repeat("b", 64)

	tests := []struct {
		name          string
		command       string
		candidateFlag string
		digestFlag    string
		mode          openpackregistry.TransitionMode
		status        openpackregistryfile.ActivationStatus
	}{
		{name: "activate", command: "registry-activate", candidateFlag: "-candidate", digestFlag: "-expected-candidate-sha256", mode: openpackregistry.TransitionActivate, status: openpackregistryfile.ActivationStatusActivated},
		{name: "rollback", command: "registry-rollback", candidateFlag: "-rollback", digestFlag: "-expected-rollback-sha256", mode: openpackregistry.TransitionRollback, status: openpackregistryfile.ActivationStatusRolledBack},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			deps := defaultDependencies()
			called := false
			deps.activateRegistry = func(_ context.Context, options openpackregistryfile.ActivationOptions) (openpackregistryfile.ActivationResult, error) {
				called = true
				if options.Mode != test.mode || options.SourceID != "developer" || options.RegistryPath != registry ||
					options.CandidatePath != candidate || options.RollbackPath != archive ||
					options.ExpectedCurrentSHA256 != currentDigest || options.ExpectedCandidateSHA256 != candidateDigest {
					t.Fatalf("activation options = %#v", options)
				}
				return openpackregistryfile.ActivationResult{
					Status: test.status, Mode: test.mode, SourceID: "developer", Path: registry, RollbackPath: archive,
					FromRegistrySHA256: currentDigest, ToRegistrySHA256: candidateDigest,
					RollbackRegistrySHA256: currentDigest, FromRevision: 1, ToRevision: 2, RestartRequired: true,
				}, nil
			}
			stdout, stderr, code := runCommand(t, deps,
				test.command, "-registry", registry, "-source", "developer", test.candidateFlag, candidate,
				"-expected-current-sha256", currentDigest, test.digestFlag, candidateDigest,
				"-rollback-out", archive,
			)
			if code != 0 || stderr != "" || !called || !strings.Contains(stdout, `"restart_required":true`) ||
				!strings.Contains(stdout, `"status":"`+string(test.status)+`"`) {
				t.Fatalf("%s code=%d stdout=%q stderr=%q called=%v", test.command, code, stdout, stderr, called)
			}
		})
	}
}

func TestRegistryActivationResultWriteFailurePreservesCommittedEvidence(t *testing.T) {
	root := t.TempDir()
	registry := filepath.Join(root, "registry.json")
	candidate := filepath.Join(root, "candidate.json")
	rollback := filepath.Join(root, "rollback.json")
	currentDigest := strings.Repeat("a", 64)
	candidateDigest := strings.Repeat("b", 64)
	deps := defaultDependencies()
	deps.activateRegistry = func(_ context.Context, options openpackregistryfile.ActivationOptions) (openpackregistryfile.ActivationResult, error) {
		return openpackregistryfile.ActivationResult{
			Status: openpackregistryfile.ActivationStatusActivated, Mode: options.Mode, SourceID: options.SourceID,
			Path: registry, RollbackPath: rollback, FromRegistrySHA256: currentDigest,
			ToRegistrySHA256: candidateDigest, RollbackRegistrySHA256: currentDigest, RestartRequired: true,
		}, nil
	}
	var stderr bytes.Buffer
	code := runWithDependencies(context.Background(), []string{
		"registry-activate", "-registry", registry, "-source", "developer", "-candidate", candidate,
		"-expected-current-sha256", currentDigest, "-expected-candidate-sha256", candidateDigest,
		"-rollback-out", rollback,
	}, activationFailWriter{}, &stderr, deps)
	if code != 1 {
		t.Fatalf("result failure code=%d stderr=%q", code, stderr.String())
	}
	for _, evidence := range []string{registry, rollback, currentDigest, candidateDigest, "restart_required=true", "write result"} {
		if !strings.Contains(stderr.String(), evidence) {
			t.Fatalf("activation result error missing %q: %s", evidence, stderr.String())
		}
	}
}

type activationFailWriter struct{}

func (activationFailWriter) Write([]byte) (int, error) {
	return 0, errors.New("injected result writer failure")
}

func TestVerifyValidatesFullBundleAndCleansPrivateProjection(t *testing.T) {
	fixture := newCommandFixture(t)
	verificationRoot := filepath.Join(t.TempDir(), "verify-root")
	deps := defaultDependencies()
	deps.now = func() time.Time { return fixture.now }
	deps.makeVerifyRoot = func() (string, error) {
		if err := os.Mkdir(verificationRoot, 0o700); err != nil {
			return "", err
		}
		return verificationRoot, nil
	}
	var removed string
	deps.removeVerifyRoot = func(path string) error {
		removed = path
		return os.RemoveAll(path)
	}

	stdout, stderr, code := runCommand(t, deps, "verify", "-registry", fixture.registry, "-source", "developer", "-bundle", fixture.bundle)
	if code != 0 || stderr != "" {
		t.Fatalf("verify code=%d stderr=%q", code, stderr)
	}
	if removed != verificationRoot {
		t.Fatalf("removed %q, want %q", removed, verificationRoot)
	}
	if _, err := os.Lstat(verificationRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("verification root remains: %v", err)
	}
	var got result
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if got.Status != "verified" || got.SourceID != "developer" || got.PackID != fixture.manifest.PackID ||
		got.Revision != fixture.manifest.Revision || got.ManifestSHA256 != fixture.manifestHash || got.RecordCount != 1 ||
		got.OperationCount != 1 ||
		got.ProjectionBytes == 0 || got.ProjectionBytes > openpackindex.MaxProjectionBytes || got.Path != "" {
		t.Fatalf("verify result = %#v", got)
	}
	if _, err := os.Lstat(fixture.installed); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("verify activated registry destination: %v", err)
	}
}

func TestInstallUsesRegistryPathAndRefusesExistingDestination(t *testing.T) {
	fixture := newCommandFixture(t)
	deps := defaultDependencies()
	deps.now = func() time.Time { return fixture.now }
	args := []string{"install", "-registry", fixture.registry, "-source", "developer", "-bundle", fixture.bundle}

	stdout, stderr, code := runCommand(t, deps, args...)
	if code != 0 || stderr != "" {
		t.Fatalf("install code=%d stderr=%q", code, stderr)
	}
	var got result
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "installed" || got.Path != fixture.installed || got.ManifestSHA256 != fixture.manifestHash || got.RecordCount != 1 || got.OperationCount != 1 ||
		got.ProjectionBytes == 0 || got.ProjectionBytes > openpackindex.MaxProjectionBytes {
		t.Fatalf("install result = %#v", got)
	}
	info, err := os.Stat(fixture.installed)
	if err != nil || !info.IsDir() {
		t.Fatalf("installed path: %v", err)
	}

	stdout, stderr, code = runCommand(t, deps, args...)
	if code != 1 || stdout != "" || !strings.Contains(stderr, "projection already exists") {
		t.Fatalf("second install code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
}

func TestVerifyPassesProjectionLimitAndReportsMeasuredBytes(t *testing.T) {
	fixture := newCommandFixture(t)
	deps := defaultDependencies()
	deps.now = func() time.Time { return fixture.now }
	var gotLimit uint64
	deps.install = func(_ context.Context, options openpackindex.InstallOptions) (openpackindex.Installed, error) {
		gotLimit = options.MaxProjectionBytes
		return openpackindex.Installed{
			Path: options.Root, ManifestSHA256: fixture.manifestHash, PackID: fixture.manifest.PackID,
			Revision: fixture.manifest.Revision, RecordCount: fixture.manifest.RecordCount,
			OperationCount: fixture.manifest.RecordCount, ProjectionBytes: 12_345,
		}, nil
	}

	stdout, stderr, code := runCommand(t, deps, "verify", "-registry", fixture.registry, "-source", "developer", "-bundle", fixture.bundle, "-max-projection-bytes", "23456")
	if code != 0 || stderr != "" {
		t.Fatalf("verify code=%d stderr=%q", code, stderr)
	}
	if gotLimit != 23_456 {
		t.Fatalf("MaxProjectionBytes = %d, want 23456", gotLimit)
	}
	var got result
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatal(err)
	}
	if got.ProjectionBytes != 12_345 {
		t.Fatalf("ProjectionBytes = %d, want 12345", got.ProjectionBytes)
	}
}

func TestDeltaVerifyAndInstallRequireParentBundleAndDispatch(t *testing.T) {
	for _, command := range []string{"verify", "install"} {
		t.Run(command, func(t *testing.T) {
			fixture := newCommandFixture(t)
			parentBundle := filepath.Join(t.TempDir(), "parent-bundle")
			if err := os.Mkdir(parentBundle, 0o700); err != nil {
				t.Fatal(err)
			}
			deltaDigest := strings.Repeat("d", 64)
			const operationCount = uint64(3)
			const projectionCount = uint64(2)
			mutateRegistryToDelta(t, fixture.registry, fixture.manifestHash, deltaDigest, operationCount, projectionCount)

			deps := defaultDependencies()
			deps.now = func() time.Time { return fixture.now }
			deps.install = func(context.Context, openpackindex.InstallOptions) (openpackindex.Installed, error) {
				t.Fatal("snapshot installer called for delta binding")
				return openpackindex.Installed{}, nil
			}
			var captured openpackindex.DeltaInstallOptions
			deps.applyDelta = func(_ context.Context, options openpackindex.DeltaInstallOptions) (openpackindex.Installed, error) {
				captured = options
				path := filepath.Join(options.Root, "objects", deltaDigest)
				if err := os.MkdirAll(path, 0o700); err != nil {
					return openpackindex.Installed{}, err
				}
				return openpackindex.Installed{
					Path: path, ManifestSHA256: deltaDigest, PackID: fixture.manifest.PackID,
					Revision: 2, RecordCount: projectionCount, OperationCount: operationCount, ProjectionBytes: 12_345,
				}, nil
			}

			stdout, stderr, code := runCommand(t, deps, command, "-registry", fixture.registry, "-source", "developer", "-bundle", fixture.bundle, "-parent-bundle", parentBundle)
			if code != 0 || stderr != "" {
				t.Fatalf("%s code=%d stderr=%q", command, code, stderr)
			}
			if captured.ParentBundleDir != parentBundle || captured.BundleDir != fixture.bundle ||
				captured.ExpectedParentManifestSHA256 != fixture.manifestHash ||
				captured.ExpectedProjectionRecordCount != projectionCount ||
				captured.DeltaAcceptance.ExpectedManifestSHA256 != deltaDigest ||
				captured.DeltaAcceptance.ExpectedRecordCount != operationCount {
				t.Fatalf("delta options = %#v", captured)
			}
			var got result
			if err := json.Unmarshal([]byte(stdout), &got); err != nil {
				t.Fatal(err)
			}
			if got.RecordCount != projectionCount || got.OperationCount != operationCount || got.ManifestSHA256 != deltaDigest {
				t.Fatalf("%s result = %#v", command, got)
			}
		})
	}
}

func TestParentBundleMustMatchRegistryBindingKind(t *testing.T) {
	fixture := newCommandFixture(t)
	parentBundle := filepath.Join(t.TempDir(), "parent-bundle")
	if err := os.Mkdir(parentBundle, 0o700); err != nil {
		t.Fatal(err)
	}
	deps := defaultDependencies()
	deps.now = func() time.Time { return fixture.now }

	stdout, stderr, code := runCommand(t, deps, "verify", "-registry", fixture.registry, "-source", "developer", "-bundle", fixture.bundle, "-parent-bundle", parentBundle)
	if code != 1 || stdout != "" || !strings.Contains(stderr, "snapshot binding forbids -parent-bundle") {
		t.Fatalf("snapshot parent code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}

	mutateRegistryToDelta(t, fixture.registry, fixture.manifestHash, strings.Repeat("d", 64), 1, 1)
	stdout, stderr, code = runCommand(t, deps, "verify", "-registry", fixture.registry, "-source", "developer", "-bundle", fixture.bundle)
	if code != 1 || stdout != "" || !strings.Contains(stderr, "delta binding requires -parent-bundle") {
		t.Fatalf("delta missing parent code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
}

func TestVerifyAndInstallRejectRegistryPinsThatDifferFromSignedManifest(t *testing.T) {
	tests := []struct {
		name   string
		field  string
		value  func(commandFixture) any
		needle string
	}{
		{name: "record count", field: "record_count", value: func(commandFixture) any { return uint64(2) }, needle: "record_count"},
		{name: "created at", field: "created_at", value: func(fixture commandFixture) any {
			return fixture.now.Add(-2 * time.Hour).Format(time.RFC3339)
		}, needle: "created_at"},
		{name: "expires at", field: "expires_at", value: func(fixture commandFixture) any {
			return fixture.now.Add(48 * time.Hour).Format(time.RFC3339)
		}, needle: "expires_at"},
	}
	for _, test := range tests {
		for _, command := range []string{"verify", "install"} {
			t.Run(test.name+"/"+command, func(t *testing.T) {
				fixture := newCommandFixture(t)
				mutateRegistryBinding(t, fixture.registry, test.field, test.value(fixture))
				deps := defaultDependencies()
				deps.now = func() time.Time { return fixture.now }
				stdout, stderr, code := runCommand(t, deps, command, "-registry", fixture.registry, "-source", "developer", "-bundle", fixture.bundle)
				if code != 1 || stdout != "" || !strings.Contains(stderr, test.needle) {
					t.Fatalf("%s code=%d stdout=%q stderr=%q", command, code, stdout, stderr)
				}
				if _, err := os.Lstat(fixture.installed); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("failed %s activated projection: %v", command, err)
				}
			})
		}
	}
}

func TestVerifyCleanupErrorIsFatalAfterCleanup(t *testing.T) {
	fixture := newCommandFixture(t)
	verificationRoot := filepath.Join(t.TempDir(), "verify-root")
	deps := defaultDependencies()
	deps.now = func() time.Time { return fixture.now }
	deps.makeVerifyRoot = func() (string, error) {
		if err := os.Mkdir(verificationRoot, 0o700); err != nil {
			return "", err
		}
		return verificationRoot, nil
	}
	deps.removeVerifyRoot = func(path string) error {
		if err := os.RemoveAll(path); err != nil {
			return err
		}
		return errors.New("synthetic cleanup failure")
	}
	stdout, stderr, code := runCommand(t, deps, "verify", "-registry", fixture.registry, "-source", "developer", "-bundle", fixture.bundle)
	if code != 1 || stdout != "" || !strings.Contains(stderr, "remove private verification root") {
		t.Fatalf("cleanup failure code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if _, err := os.Lstat(verificationRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("verification root remains: %v", err)
	}
}

func TestRejectsBadRegistrySourceAndFlags(t *testing.T) {
	fixture := newCommandFixture(t)
	badRegistry := filepath.Join(t.TempDir(), "registry.json")
	if err := os.WriteFile(badRegistry, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	insecureRegistry := filepath.Join(t.TempDir(), "registry.json")
	registryRaw, err := os.ReadFile(fixture.registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(insecureRegistry, registryRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(insecureRegistry, 0o666); err != nil {
		t.Fatal(err)
	}
	deps := defaultDependencies()
	deps.now = func() time.Time { return fixture.now }
	tests := []struct {
		name string
		args []string
		code int
		part string
	}{
		{name: "no subcommand", code: 2, part: "expected exactly one subcommand"},
		{name: "unknown subcommand", args: []string{"download"}, code: 2, part: "unknown subcommand"},
		{name: "unknown flag", args: []string{"verify", "-wat"}, code: 2, part: "flag provided but not defined"},
		{name: "missing flag", args: []string{"verify", "-registry", fixture.registry, "-source", "developer"}, code: 2, part: "-bundle is required"},
		{name: "relative registry", args: []string{"verify", "-registry", "registry.json", "-source", "developer", "-bundle", fixture.bundle}, code: 2, part: "clean absolute path"},
		{name: "relative bundle", args: []string{"verify", "-registry", fixture.registry, "-source", "developer", "-bundle", "bundle"}, code: 2, part: "clean absolute path"},
		{name: "relative parent bundle", args: []string{"verify", "-registry", fixture.registry, "-source", "developer", "-bundle", fixture.bundle, "-parent-bundle", "parent"}, code: 2, part: "clean absolute path"},
		{name: "positionals", args: []string{"verify", "-registry", fixture.registry, "-source", "developer", "-bundle", fixture.bundle, "extra"}, code: 2, part: "unexpected positional"},
		{name: "duplicate", args: []string{"verify", "-registry", fixture.registry, "-registry", fixture.registry, "-source", "developer", "-bundle", fixture.bundle}, code: 2, part: "may only be specified once"},
		{name: "duplicate parent bundle", args: []string{"verify", "-registry", fixture.registry, "-source", "developer", "-bundle", fixture.bundle, "-parent-bundle", fixture.bundle, "-parent-bundle", fixture.bundle}, code: 2, part: "may only be specified once"},
		{name: "zero projection limit", args: []string{"verify", "-registry", fixture.registry, "-source", "developer", "-bundle", fixture.bundle, "-max-projection-bytes", "0"}, code: 2, part: "must be an integer"},
		{name: "excessive projection limit", args: []string{"verify", "-registry", fixture.registry, "-source", "developer", "-bundle", fixture.bundle, "-max-projection-bytes", "536870913"}, code: 2, part: "must be an integer"},
		{name: "duplicate projection limit", args: []string{"verify", "-registry", fixture.registry, "-source", "developer", "-bundle", fixture.bundle, "-max-projection-bytes", "1000", "-max-projection-bytes", "2000"}, code: 2, part: "may only be specified once"},
		{name: "channel missing metadata", args: []string{"channel-select", "-trusted-root", fixture.registry, "-state-dir", fixture.root, "-target", "packs/developer/manifest.json"}, code: 2, part: "-metadata-dir is required"},
		{name: "channel relative root", args: []string{"channel-select", "-trusted-root", "root.json", "-metadata-dir", fixture.root, "-state-dir", fixture.root, "-target", "packs/developer/manifest.json"}, code: 2, part: "-trusted-root must be a clean absolute path"},
		{name: "channel relative metadata", args: []string{"channel-select", "-trusted-root", fixture.registry, "-metadata-dir", "metadata", "-state-dir", fixture.root, "-target", "packs/developer/manifest.json"}, code: 2, part: "-metadata-dir must be a clean absolute path"},
		{name: "channel relative state", args: []string{"channel-select", "-trusted-root", fixture.registry, "-metadata-dir", fixture.root, "-state-dir", "state", "-target", "packs/developer/manifest.json"}, code: 2, part: "-state-dir must be a clean absolute path"},
		{name: "channel relative bundle", args: []string{"channel-select", "-trusted-root", fixture.registry, "-metadata-dir", fixture.root, "-state-dir", fixture.root, "-target", "packs/developer/manifest.json", "-bundle", "bundle"}, code: 2, part: "-bundle must be a clean absolute path"},
		{name: "channel pack-only flag", args: []string{"channel-select", "-registry", fixture.registry}, code: 2, part: "flag provided but not defined"},
		{name: "bad source", args: []string{"verify", "-registry", fixture.registry, "-source", "missing", "-bundle", fixture.bundle}, code: 1, part: "not defined in registry"},
		{name: "bad registry", args: []string{"verify", "-registry", badRegistry, "-source", "developer", "-bundle", fixture.bundle}, code: 1, part: "invalid registry"},
		{name: "writable registry", args: []string{"verify", "-registry", insecureRegistry, "-source", "developer", "-bundle", fixture.bundle}, code: 1, part: "must not be writable by group or world"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stdout, stderr, code := runCommand(t, deps, test.args...)
			if code != test.code || stdout != "" || !strings.Contains(stderr, test.part) {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout, stderr)
			}
		})
	}
}

func runCommand(t *testing.T, deps dependencies, args ...string) (string, string, int) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := runWithDependencies(context.Background(), args, &stdout, &stderr, deps)
	return stdout.String(), stderr.String(), code
}

func openRegistryBindingIndex(t *testing.T, registryPath, sourceID string, now time.Time) (*openpackindex.Index, openpackregistry.Binding) {
	t.Helper()
	registry, err := openpackregistryfile.Load(registryPath)
	if err != nil {
		t.Fatalf("load registry %q: %v", registryPath, err)
	}
	binding, found := registry.Lookup(sourceID)
	if !found {
		t.Fatalf("source %q missing from registry %q", sourceID, registryPath)
	}
	index, err := openpackindex.Open(openpackindex.OpenOptions{
		Path: binding.InstalledPath, ExpectedManifestSHA256: binding.ManifestSHA256,
		ExpectedPackID: binding.PackID, ExpectedRevision: binding.Revision,
		ExpectedRecordCount: binding.RecordCount, ExpectedKeyID: binding.SigningKeyID,
		ExpectedCreatedAt: binding.CreatedAt, ExpectedExpiresAt: binding.ExpiresAt,
		Now: now, ProviderID: binding.SourceID,
	})
	if err != nil {
		t.Fatalf("open projection for %q: %v", sourceID, err)
	}
	return index, binding
}

func assertFileDigest(t *testing.T, path, expected string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %q: %v", path, err)
	}
	if got := indexpack.Digest(raw); got != expected {
		t.Fatalf("digest %q = %s, want %s", path, got, expected)
	}
}

func mutateRegistryBinding(t *testing.T, path, field string, value any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	bindings, ok := document["bindings"].([]any)
	if !ok || len(bindings) != 1 {
		t.Fatalf("unexpected bindings: %#v", document["bindings"])
	}
	binding, ok := bindings[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected binding: %#v", bindings[0])
	}
	binding[field] = value
	updated, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, updated, 0o600); err != nil {
		t.Fatal(err)
	}
}

func mutateRegistryToDelta(t *testing.T, path, parentDigest, deltaDigest string, operationCount, projectionCount uint64) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	document["version"] = float64(2)
	bindings, ok := document["bindings"].([]any)
	if !ok || len(bindings) != 1 {
		t.Fatalf("unexpected bindings: %#v", document["bindings"])
	}
	binding, ok := bindings[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected binding: %#v", bindings[0])
	}
	delete(binding, "record_count")
	binding["kind"] = string(indexpack.KindDelta)
	binding["parent_manifest_sha256"] = parentDigest
	binding["manifest_sha256"] = deltaDigest
	binding["revision"] = float64(2)
	binding["manifest_record_count"] = operationCount
	binding["projection_record_count"] = projectionCount
	binding["installed_path"] = filepath.Join(filepath.Dir(filepath.Dir(binding["installed_path"].(string))), "objects", deltaDigest)
	updated, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, updated, 0o600); err != nil {
		t.Fatal(err)
	}
}

func newCommandFixture(t *testing.T) commandFixture {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	record := indexpack.Record{
		Operation: indexpack.OperationUpsert, URL: "https://docs.example.com/open-pack", Title: "Open pack fixture",
		SalientSketch: "local open discovery fixture", Language: "en", FetchedAt: now.Add(-time.Hour).Format(time.RFC3339),
		ContentSHA256: strings.Repeat("a", 64), AuthorityScore: 100, FreshnessScore: 100,
		Provenance: []indexpack.Provenance{{
			Source: "fixture", SourceURI: "https://example.com/source", RetrievedAt: now.Add(-2 * time.Hour).Format(time.RFC3339), RightsNotice: "fixture rights",
		}},
	}
	var raw bytes.Buffer
	encoder := json.NewEncoder(&raw)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(record); err != nil {
		t.Fatal(err)
	}
	var compressed bytes.Buffer
	zstdEncoder, err := zstd.NewWriter(&compressed, zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := zstdEncoder.Write(raw.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := zstdEncoder.Close(); err != nil {
		t.Fatal(err)
	}
	compressedDigest := indexpack.ShardDigest(compressed.Bytes())
	manifest := indexpack.Manifest{
		Version: indexpack.Version, Kind: indexpack.KindSnapshot, PackID: "developer-en", Revision: 1,
		CreatedAt: now.Add(-time.Hour).Format(time.RFC3339), ExpiresAt: now.Add(24 * time.Hour).Format(time.RFC3339),
		SigningKeyID: indexpack.KeyID(publicKey),
		Publisher:    indexpack.Publisher{Name: "Fixture Publisher", ContactURI: "mailto:operator@example.com", TakedownURI: "https://example.com/takedown", RightsNotice: "fixture rights"},
		Policy:       indexpack.Policy{Robots: "rfc9309", NoIndex: "exclude", BodyDistribution: "forbidden"}, Languages: []string{"en"}, RecordCount: 1,
		Shards: []indexpack.Shard{{
			Path: indexpack.ShardPath(compressedDigest), Compression: "zstd", SHA256: compressedDigest,
			CompressedSizeBytes: uint64(compressed.Len()), UncompressedSHA256: indexpack.ShardDigest(raw.Bytes()),
			UncompressedSizeBytes: uint64(raw.Len()), RecordCount: 1,
		}},
		Build: indexpack.Build{
			Generator: "fixture", GeneratorVersion: "1", Analyzer: "unicode-lexical", AnalyzerVersion: "1",
			PolicySHA256: strings.Repeat("1", 64), ExclusionsSHA256: strings.Repeat("2", 64),
			Inputs: []indexpack.BuildInput{{Name: "fixture-input", URI: "https://example.com/input", RetrievedAt: now.Add(-2 * time.Hour).Format(time.RFC3339), SHA256: strings.Repeat("3", 64), RightsNotice: "fixture rights"}},
		},
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
	base := t.TempDir()
	bundle := filepath.Join(base, "bundle")
	root := filepath.Join(base, "installed")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(bundle, filepath.Dir(manifest.Shards[0].Path)), 0o700); err != nil {
		t.Fatal(err)
	}
	for path, data := range map[string][]byte{
		filepath.Join(bundle, openpackindex.ManifestFilename):  manifestRaw,
		filepath.Join(bundle, openpackindex.SignatureFilename): signatureRaw,
		filepath.Join(bundle, manifest.Shards[0].Path):         compressed.Bytes(),
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	manifestHash := indexpack.ManifestDigest(manifestRaw)
	installedPath := filepath.Join(root, "objects", manifestHash)
	registry := filepath.Join(base, "registry.json")
	registryDocument := map[string]any{
		"version": 1,
		"publishers": []map[string]any{{
			"id": "fixture-publisher", "key_id": indexpack.KeyID(publicKey),
			"ed25519_public_key": base64.StdEncoding.EncodeToString(publicKey),
		}},
		"bindings": []map[string]any{{
			"source_id": "developer", "pack_id": manifest.PackID, "publisher_id": "fixture-publisher",
			"manifest_sha256": manifestHash, "revision": manifest.Revision, "record_count": manifest.RecordCount,
			"created_at": manifest.CreatedAt, "expires_at": manifest.ExpiresAt, "installed_path": installedPath,
		}},
	}
	registryRaw, err := json.Marshal(registryDocument)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(registry, registryRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	return commandFixture{
		bundle: bundle, registry: registry, installed: installedPath, root: root,
		now: now, manifest: manifest, manifestHash: manifestHash, privateKey: privateKey,
	}
}

func writeSnapshotRevisionBundle(t *testing.T, fixture commandFixture, revision uint64) (string, indexpack.Manifest, []byte) {
	t.Helper()
	manifest := fixture.manifest
	manifest.Revision = revision
	manifest.CreatedAt = fixture.now.Format(time.RFC3339)
	manifest.ExpiresAt = fixture.now.Add(12 * time.Hour).Format(time.RFC3339)
	manifestRaw, err := indexpack.EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	signature, err := indexpack.SignManifest(manifestRaw, fixture.privateKey)
	if err != nil {
		t.Fatal(err)
	}
	signatureRaw, err := indexpack.EncodeSignature(signature)
	if err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(filepath.Dir(fixture.bundle), "bundle-r"+strconv.FormatUint(revision, 10))
	if err := os.MkdirAll(filepath.Join(bundle, filepath.Dir(manifest.Shards[0].Path)), 0o700); err != nil {
		t.Fatal(err)
	}
	shardRaw, err := os.ReadFile(filepath.Join(fixture.bundle, filepath.FromSlash(manifest.Shards[0].Path)))
	if err != nil {
		t.Fatal(err)
	}
	for path, raw := range map[string][]byte{
		filepath.Join(bundle, openpackindex.ManifestFilename):              manifestRaw,
		filepath.Join(bundle, openpackindex.SignatureFilename):             signatureRaw,
		filepath.Join(bundle, filepath.FromSlash(manifest.Shards[0].Path)): shardRaw,
	} {
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return bundle, manifest, manifestRaw
}
