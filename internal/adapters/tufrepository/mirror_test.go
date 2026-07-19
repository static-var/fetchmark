package tufrepository

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/tufchannel"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

func TestActivateMirrorInitializesConsumerCompatibleRepository(t *testing.T) {
	fixture := newStageFixture(t)
	staged, err := Stage(context.Background(), fixture.stageOptions(1, ""))
	if err != nil {
		t.Fatal(err)
	}
	mirror := t.TempDir()
	options := fixture.mirrorOptions(mirror, fixture.output, "", staged.TimestampSHA256, true)
	result, err := ActivateMirror(context.Background(), options)
	if err != nil {
		t.Fatalf("ActivateMirror: %v", err)
	}
	if result.Status != MirrorStatusInitialized || !result.InitializedFromEmpty || !result.HeadCommitted ||
		!result.ImmutableArtifactsReady || result.ToTimestampSHA256 != staged.TimestampSHA256 ||
		result.ManifestSHA256 != fixture.manifestDigest || result.Version != 1 {
		t.Fatalf("initial activation = %#v", result)
	}
	for _, relative := range []string{
		"root.json", "metadata/1.root.json", "metadata/1.snapshot.json", "metadata/1.targets.json", "metadata/timestamp.json",
		"targets/packs/developer/" + fixture.manifestDigest + ".manifest.json",
		"targets/packs/developer/" + fixture.manifestDigest + ".manifest.ed25519",
		"targets/packs/developer/" + fixture.manifest.Shards[0].Path,
	} {
		if info, statErr := os.Stat(filepath.Join(mirror, filepath.FromSlash(relative))); statErr != nil || !info.Mode().IsRegular() {
			t.Fatalf("mirror file %s = %v, %v", relative, info, statErr)
		}
	}
	state := filepath.Join(fixture.root, "mirror-consumer-state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	selected, err := tufchannel.Select(context.Background(), tufchannel.Options{
		TrustedRootPath: filepath.Join(fixture.output, "root.json"),
		MetadataDir:     filepath.Join(mirror, "metadata"),
		StateDir:        state,
		BundleDir:       fixture.bundle,
		TargetPath:      testTargetPath,
	})
	if err != nil || selected.Status != tufchannel.StatusSelected || selected.ManifestSHA256 != fixture.manifestDigest {
		t.Fatalf("Select mirror = %#v, %v", selected, err)
	}
	retry, err := ActivateMirror(context.Background(), options)
	if err != nil || retry.Status != MirrorStatusAlreadyActive || !retry.HeadCommitted {
		t.Fatalf("already-applied activation = %#v, %v", retry, err)
	}
}

func TestActivateMirrorCarriesAndConsumesSequentialRootRotation(t *testing.T) {
	fixture := newStageFixture(t)
	firstOutput := fixture.output
	first, err := Stage(context.Background(), fixture.stageOptions(1, ""))
	if err != nil {
		t.Fatal(err)
	}
	mirror := t.TempDir()
	if _, err := ActivateMirror(context.Background(), fixture.mirrorOptions(mirror, firstOutput, "", first.TimestampSHA256, true)); err != nil {
		t.Fatal(err)
	}
	operationalIdentity, signedRootRaw := rotateIdentityForTest(t, fixture.identity, fixture.now)
	fixture.identity = operationalIdentity
	fixture.advanceSnapshot(t, 2)
	fixture.output = filepath.Join(fixture.root, "repository-v2-rotated-mirror")
	second, err := Stage(context.Background(), fixture.stageOptions(2, firstOutput))
	if err != nil {
		t.Fatal(err)
	}
	result, err := ActivateMirror(
		context.Background(), fixture.mirrorOptions(mirror, fixture.output, first.TimestampSHA256, second.TimestampSHA256, false),
	)
	if err != nil {
		t.Fatalf("ActivateMirror rotated successor: %v", err)
	}
	if result.Status != MirrorStatusActivated || result.RootVersion != 2 || result.Version != 2 {
		t.Fatalf("rotated activation = %#v", result)
	}
	rootUpdate, err := os.ReadFile(filepath.Join(mirror, "metadata", "2.root.json"))
	if err != nil || !bytes.Equal(rootUpdate, signedRootRaw) {
		t.Fatalf("mirrored root update differs: %v", err)
	}
	state := filepath.Join(fixture.root, "rotated-mirror-consumer-state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	selected, err := tufchannel.Select(context.Background(), tufchannel.Options{
		TrustedRootPath: filepath.Join(fixture.output, "root.json"), MetadataDir: filepath.Join(mirror, "metadata"),
		StateDir: state, BundleDir: fixture.bundle, TargetPath: testTargetPath,
	})
	if err != nil || selected.RootVersion != 2 || selected.TimestampVersion != 2 {
		t.Fatalf("Select rotated mirror = %#v, %v", selected, err)
	}
}

func TestActivateMirrorDoesNotReportSameTimestampDifferentRootChainAsApplied(t *testing.T) {
	fixture := newStageFixture(t)
	first, err := Stage(context.Background(), fixture.stageOptions(1, ""))
	if err != nil {
		t.Fatal(err)
	}
	mirror := t.TempDir()
	if _, err := ActivateMirror(context.Background(), fixture.mirrorOptions(mirror, fixture.output, "", first.TimestampSHA256, true)); err != nil {
		t.Fatal(err)
	}
	operationalIdentity, _ := rotateIdentityForTest(t, fixture.identity, fixture.now)
	fixture.identity = operationalIdentity
	fixture.output = filepath.Join(fixture.root, "repository-v1-same-head-rotated-root")
	rotated, err := Stage(context.Background(), fixture.stageOptions(1, ""))
	if err != nil {
		t.Fatal(err)
	}
	if rotated.TimestampSHA256 != first.TimestampSHA256 {
		t.Fatalf("fixture timestamp differs: %s != %s", rotated.TimestampSHA256, first.TimestampSHA256)
	}
	result, err := ActivateMirror(
		context.Background(), fixture.mirrorOptions(mirror, fixture.output, "", rotated.TimestampSHA256, true),
	)
	if !errors.Is(err, ErrMirrorCASMismatch) || result.Status != "" || !strings.Contains(err.Error(), "root chain differs") {
		t.Fatalf("same timestamp, different root chain = %#v, %v", result, err)
	}
}

func TestActivateMirrorRechecksExactPreparedRootChainBeforeCommit(t *testing.T) {
	fixture := newStageFixture(t)
	operationalIdentity, _ := rotateIdentityForTest(t, fixture.identity, fixture.now)
	fixture.identity = operationalIdentity
	staged, err := Stage(context.Background(), fixture.stageOptions(1, ""))
	if err != nil {
		t.Fatal(err)
	}
	mirror := t.TempDir()
	dependencies := defaultMirrorActivationDependencies()
	dependencies.afterPreparedVerification = func() {
		if err := os.WriteFile(filepath.Join(mirror, "metadata", "2.root.json"), []byte("changed"), 0o600); err != nil {
			panic(err)
		}
	}
	result, err := activateMirrorWithDependencies(
		context.Background(), fixture.mirrorOptions(mirror, fixture.output, "", staged.TimestampSHA256, true), dependencies,
	)
	var prepared *PreparedMirrorActivationError
	if !errors.As(err, &prepared) || result.Status != MirrorStatusPrepared || result.HeadCommitted ||
		!strings.Contains(err.Error(), "root chain changed before commit") {
		t.Fatalf("changed prepared root chain = %#v, %v", result, err)
	}
	if _, statErr := os.Stat(filepath.Join(mirror, "metadata", "timestamp.json")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("changed root chain published timestamp: %v", statErr)
	}
}

func TestActivateMirrorConcurrentInitialRetryHasOneCommit(t *testing.T) {
	fixture := newStageFixture(t)
	staged, err := Stage(context.Background(), fixture.stageOptions(1, ""))
	if err != nil {
		t.Fatal(err)
	}
	mirror := t.TempDir()
	options := fixture.mirrorOptions(mirror, fixture.output, "", staged.TimestampSHA256, true)
	results := make(chan MirrorActivationResult, 2)
	errorsChannel := make(chan error, 2)
	var group sync.WaitGroup
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := ActivateMirror(context.Background(), options)
			results <- result
			errorsChannel <- err
		}()
	}
	group.Wait()
	close(results)
	close(errorsChannel)
	for err := range errorsChannel {
		if err != nil {
			t.Fatalf("concurrent activation: %v", err)
		}
	}
	statuses := map[MirrorActivationStatus]int{}
	for result := range results {
		statuses[result.Status]++
	}
	if statuses[MirrorStatusInitialized] != 1 || statuses[MirrorStatusAlreadyActive] != 1 {
		t.Fatalf("concurrent statuses = %v", statuses)
	}
}

func TestActivateMirrorConcurrentCompetingCandidatesHaveOneWinner(t *testing.T) {
	fixture := newStageFixture(t)
	firstOutput := fixture.output
	first, err := Stage(context.Background(), fixture.stageOptions(1, ""))
	if err != nil {
		t.Fatal(err)
	}
	fixture.advanceSnapshot(t, 2)
	fixture.output = filepath.Join(fixture.root, "repository-v1-sibling")
	sibling, err := Stage(context.Background(), fixture.stageOptions(1, ""))
	if err != nil {
		t.Fatal(err)
	}
	mirror := t.TempDir()
	options := []MirrorActivationOptions{
		fixture.mirrorOptions(mirror, firstOutput, "", first.TimestampSHA256, true),
		fixture.mirrorOptions(mirror, fixture.output, "", sibling.TimestampSHA256, true),
	}
	type outcome struct {
		result MirrorActivationResult
		err    error
	}
	outcomes := make(chan outcome, 2)
	var group sync.WaitGroup
	for _, activation := range options {
		group.Add(1)
		go func(options MirrorActivationOptions) {
			defer group.Done()
			result, err := ActivateMirror(context.Background(), options)
			outcomes <- outcome{result: result, err: err}
		}(activation)
	}
	group.Wait()
	close(outcomes)
	winners, losers := 0, 0
	for outcome := range outcomes {
		switch {
		case outcome.err == nil && outcome.result.Status == MirrorStatusInitialized:
			winners++
		case errors.Is(outcome.err, ErrMirrorCASMismatch):
			losers++
		default:
			t.Fatalf("unexpected competing outcome = %#v, %v", outcome.result, outcome.err)
		}
	}
	if winners != 1 || losers != 1 {
		t.Fatalf("competing outcomes winners=%d losers=%d", winners, losers)
	}
}

func TestActivateMirrorCASRejectsSiblingBranchAndRetainsOldObjects(t *testing.T) {
	fixture := newStageFixture(t)
	first, err := Stage(context.Background(), fixture.stageOptions(1, ""))
	if err != nil {
		t.Fatal(err)
	}
	mirror := t.TempDir()
	if _, err := ActivateMirror(context.Background(), fixture.mirrorOptions(mirror, fixture.output, "", first.TimestampSHA256, true)); err != nil {
		t.Fatal(err)
	}
	firstOutput := fixture.output
	firstManifestDigest := fixture.manifestDigest

	fixture.advanceSnapshot(t, 2)
	fixture.output = filepath.Join(fixture.root, "repository-v2-a")
	second, err := Stage(context.Background(), fixture.stageOptions(2, firstOutput))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ActivateMirror(context.Background(), fixture.mirrorOptions(mirror, fixture.output, first.TimestampSHA256, second.TimestampSHA256, false)); err != nil {
		t.Fatalf("activate successor: %v", err)
	}

	fixture.advanceSnapshot(t, 3)
	fixture.output = filepath.Join(fixture.root, "repository-v2-b")
	sibling, err := Stage(context.Background(), fixture.stageOptions(2, firstOutput))
	if err != nil {
		t.Fatal(err)
	}
	_, err = ActivateMirror(context.Background(), fixture.mirrorOptions(mirror, fixture.output, first.TimestampSHA256, sibling.TimestampSHA256, false))
	if !errors.Is(err, ErrMirrorCASMismatch) {
		t.Fatalf("sibling activation = %v, want ErrMirrorCASMismatch", err)
	}
	headRaw, readErr := os.ReadFile(filepath.Join(mirror, "metadata", "timestamp.json"))
	if readErr != nil || indexpack.Digest(headRaw) != second.TimestampSHA256 {
		t.Fatalf("head after sibling rejection = %v, %v", indexpack.Digest(headRaw), readErr)
	}
	for _, relative := range []string{
		"metadata/1.targets.json", "metadata/1.snapshot.json",
		"targets/packs/developer/" + firstManifestDigest + ".manifest.json",
		"targets/packs/developer/" + firstManifestDigest + ".manifest.ed25519",
	} {
		if _, statErr := os.Stat(filepath.Join(mirror, filepath.FromSlash(relative))); statErr != nil {
			t.Fatalf("old immutable object %s removed: %v", relative, statErr)
		}
	}
}

func TestActivateMirrorPublishesAuthenticatedWithdrawalWithoutDeletingArtifacts(t *testing.T) {
	fixture := newStageFixture(t)
	first, err := Stage(context.Background(), fixture.stageOptions(1, ""))
	if err != nil {
		t.Fatal(err)
	}
	mirror := t.TempDir()
	if _, err := ActivateMirror(context.Background(), fixture.mirrorOptions(mirror, fixture.output, "", first.TimestampSHA256, true)); err != nil {
		t.Fatal(err)
	}
	firstOutput := fixture.output
	withdrawalOutput := filepath.Join(fixture.root, "repository-v2-withdrawn-mirror")
	withdrawal := fixture.stageOptions(2, firstOutput)
	withdrawal.OutputDir = withdrawalOutput
	withdrawal.Withdraw = true
	withdrawal.BundleDir = ""
	withdrawal.PublisherKeys = nil
	withdrawal.ExpectedManifestSHA256 = ""
	second, err := Stage(context.Background(), withdrawal)
	if err != nil {
		t.Fatal(err)
	}
	options := fixture.mirrorOptions(mirror, withdrawalOutput, first.TimestampSHA256, second.TimestampSHA256, false)
	result, err := ActivateMirror(context.Background(), options)
	if err != nil || result.Status != MirrorStatusActivated || result.Version != 2 || result.ManifestSHA256 != "" {
		t.Fatalf("withdrawal activation = %#v, %v", result, err)
	}
	for _, relative := range []string{
		"targets/packs/developer/" + fixture.manifestDigest + ".manifest.json",
		"targets/packs/developer/" + fixture.manifestDigest + ".manifest.ed25519",
		"targets/packs/developer/" + fixture.manifest.Shards[0].Path,
	} {
		if _, statErr := os.Stat(filepath.Join(mirror, filepath.FromSlash(relative))); statErr != nil {
			t.Fatalf("withdrawal removed %s: %v", relative, statErr)
		}
	}
	state := filepath.Join(fixture.root, "withdrawn-mirror-state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	selected, err := tufchannel.Select(context.Background(), tufchannel.Options{
		TrustedRootPath: filepath.Join(withdrawalOutput, "root.json"), MetadataDir: filepath.Join(mirror, "metadata"),
		StateDir: state, TargetPath: testTargetPath,
	})
	if err != nil || selected.Status != tufchannel.StatusAbsent || selected.TargetsVersion != 2 {
		t.Fatalf("withdrawn mirror selection = %#v, %v", selected, err)
	}
}

func TestActivateMirrorCancellationAfterPreparationLeavesHeadUnchangedAndRetryable(t *testing.T) {
	fixture := newStageFixture(t)
	staged, err := Stage(context.Background(), fixture.stageOptions(1, ""))
	if err != nil {
		t.Fatal(err)
	}
	mirror := t.TempDir()
	options := fixture.mirrorOptions(mirror, fixture.output, "", staged.TimestampSHA256, true)
	ctx, cancel := context.WithCancel(context.Background())
	dependencies := defaultMirrorActivationDependencies()
	dependencies.beforeFinalRead = cancel
	result, err := activateMirrorWithDependencies(ctx, options, dependencies)
	var prepared *PreparedMirrorActivationError
	if !errors.As(err, &prepared) || !errors.Is(err, context.Canceled) || result.Status != MirrorStatusPrepared || result.HeadCommitted {
		t.Fatalf("canceled activation = %#v, %v", result, err)
	}
	if _, statErr := os.Stat(filepath.Join(mirror, "metadata", "timestamp.json")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("canceled activation published head: %v", statErr)
	}
	retried, err := ActivateMirror(context.Background(), options)
	if err != nil || retried.Status != MirrorStatusInitialized {
		t.Fatalf("retry prepared activation = %#v, %v", retried, err)
	}
}

func TestActivateMirrorRejectsTamperedExistingImmutableFile(t *testing.T) {
	fixture := newStageFixture(t)
	staged, err := Stage(context.Background(), fixture.stageOptions(1, ""))
	if err != nil {
		t.Fatal(err)
	}
	mirror := t.TempDir()
	if err := os.MkdirAll(filepath.Join(mirror, "metadata"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mirror, "metadata", "1.targets.json"), []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = ActivateMirror(context.Background(), fixture.mirrorOptions(mirror, fixture.output, "", staged.TimestampSHA256, true))
	if !errors.Is(err, ErrInvalidMirrorActivation) || !strings.Contains(err.Error(), "existing immutable file") {
		t.Fatalf("tampered immutable activation = %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(mirror, "metadata", "timestamp.json")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("tampered immutable activation published head: %v", statErr)
	}
}

func TestActivateMirrorPostRenameCloseFailureReturnsCommittedEvidence(t *testing.T) {
	fixture := newStageFixture(t)
	staged, err := Stage(context.Background(), fixture.stageOptions(1, ""))
	if err != nil {
		t.Fatal(err)
	}
	mirror := t.TempDir()
	options := fixture.mirrorOptions(mirror, fixture.output, "", staged.TimestampSHA256, true)
	injected := errors.New("injected timestamp-directory close failure")
	dependencies := defaultMirrorActivationDependencies()
	dependencies.closeDirectory = func(directory *os.File) error {
		return errors.Join(directory.Close(), injected)
	}
	result, err := activateMirrorWithDependencies(context.Background(), options, dependencies)
	var committed *CommittedMirrorActivationError
	if !errors.As(err, &committed) || !errors.Is(err, injected) || !result.HeadCommitted || result.Status != MirrorStatusInitialized {
		t.Fatalf("post-rename activation = %#v, %v", result, err)
	}
	headRaw, readErr := os.ReadFile(filepath.Join(mirror, "metadata", "timestamp.json"))
	if readErr != nil || indexpack.Digest(headRaw) != staged.TimestampSHA256 {
		t.Fatalf("committed head = %s, %v", indexpack.Digest(headRaw), readErr)
	}
}

func TestActivateMirrorCanRecoverFromExpiredLiveTimestamp(t *testing.T) {
	fixture := newStageFixture(t)
	first, err := Stage(context.Background(), fixture.stageOptions(1, ""))
	if err != nil {
		t.Fatal(err)
	}
	mirror := t.TempDir()
	if _, err := ActivateMirror(context.Background(), fixture.mirrorOptions(mirror, fixture.output, "", first.TimestampSHA256, true)); err != nil {
		t.Fatal(err)
	}
	firstOutput := fixture.output
	fixture.advanceSnapshot(t, 2)
	fixture.output = filepath.Join(fixture.root, "repository-v2-after-expiry")
	stageOptions := fixture.stageOptions(2, firstOutput)
	stageOptions.TimestampExpires = fixture.now.Add(10 * 24 * time.Hour)
	stageOptions.SnapshotExpires = fixture.now.Add(20 * 24 * time.Hour)
	stageOptions.TargetsExpires = fixture.now.Add(29 * 24 * time.Hour)
	second, err := Stage(context.Background(), stageOptions)
	if err != nil {
		t.Fatal(err)
	}
	dependencies := defaultMirrorActivationDependencies()
	dependencies.now = func() time.Time { return fixture.now.Add(2 * 24 * time.Hour) }
	result, err := activateMirrorWithDependencies(context.Background(), fixture.mirrorOptions(mirror, fixture.output, first.TimestampSHA256, second.TimestampSHA256, false), dependencies)
	if err != nil || result.Status != MirrorStatusActivated {
		t.Fatalf("expired-live recovery = %#v, %v", result, err)
	}
}

func TestActivateMirrorDetectsMirrorPathSwapBeforeCommit(t *testing.T) {
	fixture := newStageFixture(t)
	staged, err := Stage(context.Background(), fixture.stageOptions(1, ""))
	if err != nil {
		t.Fatal(err)
	}
	mirror := t.TempDir()
	moved := mirror + "-moved"
	dependencies := defaultMirrorActivationDependencies()
	dependencies.afterFinalRead = func() {
		if err := os.Rename(mirror, moved); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(mirror, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	result, err := activateMirrorWithDependencies(context.Background(), fixture.mirrorOptions(mirror, fixture.output, "", staged.TimestampSHA256, true), dependencies)
	var prepared *PreparedMirrorActivationError
	if !errors.As(err, &prepared) || result.HeadCommitted || !strings.Contains(err.Error(), "output parent path changed") {
		t.Fatalf("path-swapped activation = %#v, %v", result, err)
	}
	for _, directory := range []string{mirror, moved} {
		if _, statErr := os.Stat(filepath.Join(directory, "metadata", "timestamp.json")); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("path-swapped activation published head in %s: %v", directory, statErr)
		}
	}
}

func TestActivateMirrorRechecksCancellationAfterPreparedVerification(t *testing.T) {
	fixture := newStageFixture(t)
	staged, err := Stage(context.Background(), fixture.stageOptions(1, ""))
	if err != nil {
		t.Fatal(err)
	}
	mirror := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	dependencies := defaultMirrorActivationDependencies()
	dependencies.afterPreparedVerification = cancel
	result, err := activateMirrorWithDependencies(ctx, fixture.mirrorOptions(mirror, fixture.output, "", staged.TimestampSHA256, true), dependencies)
	var prepared *PreparedMirrorActivationError
	if !errors.As(err, &prepared) || !errors.Is(err, context.Canceled) || result.HeadCommitted {
		t.Fatalf("post-verification cancellation = %#v, %v", result, err)
	}
	if _, statErr := os.Stat(filepath.Join(mirror, "metadata", "timestamp.json")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("post-verification cancellation published head: %v", statErr)
	}
}

func TestActivateMirrorRechecksFreshClockImmediatelyBeforeCommit(t *testing.T) {
	fixture := newStageFixture(t)
	staged, err := Stage(context.Background(), fixture.stageOptions(1, ""))
	if err != nil {
		t.Fatal(err)
	}
	mirror := t.TempDir()
	clockCalls := 0
	dependencies := defaultMirrorActivationDependencies()
	dependencies.now = func() time.Time {
		clockCalls++
		if clockCalls >= 3 {
			return fixture.now.Add(24 * time.Hour)
		}
		return fixture.now
	}
	result, err := activateMirrorWithDependencies(context.Background(), fixture.mirrorOptions(mirror, fixture.output, "", staged.TimestampSHA256, true), dependencies)
	var prepared *PreparedMirrorActivationError
	if !errors.As(err, &prepared) || result.HeadCommitted || !strings.Contains(err.Error(), "no longer fresh at mirror commit") || clockCalls != 3 {
		t.Fatalf("late-clock activation = %#v, %v, clock calls=%d", result, err, clockCalls)
	}
	if _, statErr := os.Stat(filepath.Join(mirror, "metadata", "timestamp.json")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("late-clock activation published head: %v", statErr)
	}
}

func TestActivateMirrorAlreadyAppliedRechecksFreshClock(t *testing.T) {
	fixture := newStageFixture(t)
	staged, err := Stage(context.Background(), fixture.stageOptions(1, ""))
	if err != nil {
		t.Fatal(err)
	}
	mirror := t.TempDir()
	options := fixture.mirrorOptions(mirror, fixture.output, "", staged.TimestampSHA256, true)
	if _, err := ActivateMirror(context.Background(), options); err != nil {
		t.Fatal(err)
	}
	clockCalls := 0
	dependencies := defaultMirrorActivationDependencies()
	dependencies.now = func() time.Time {
		clockCalls++
		if clockCalls >= 2 {
			return fixture.now.Add(24 * time.Hour)
		}
		return fixture.now
	}
	result, err := activateMirrorWithDependencies(context.Background(), options, dependencies)
	if !errors.Is(err, ErrInvalidMirrorActivation) || result.Status != "" || !strings.Contains(err.Error(), "no longer fresh at mirror commit") || clockCalls != 2 {
		t.Fatalf("stale already-applied = %#v, %v, clock calls=%d", result, err, clockCalls)
	}
}

func TestActivateMirrorIndependentlyRejectsSignedInvalidExpiryProfile(t *testing.T) {
	fixture := newStageFixture(t)
	if _, err := Stage(context.Background(), fixture.stageOptions(1, "")); err != nil {
		t.Fatal(err)
	}
	rootRaw, err := os.ReadFile(filepath.Join(fixture.output, "root.json"))
	if err != nil {
		t.Fatal(err)
	}
	manifestRaw, err := os.ReadFile(filepath.Join(fixture.bundle, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	options := fixture.stageOptions(1, "")
	options.TimestampExpires = fixture.now.Add(48 * time.Hour)
	options.SnapshotExpires = fixture.now.Add(24 * time.Hour)
	targetsRaw, snapshotRaw, timestampRaw, err := buildMetadata(options, rootRaw, fixture.manifest, manifestRaw)
	if err != nil {
		t.Fatal(err)
	}
	for relative, raw := range map[string][]byte{
		"metadata/1.targets.json": targetsRaw, "metadata/1.snapshot.json": snapshotRaw, "metadata/timestamp.json": timestampRaw,
	} {
		if err := os.WriteFile(filepath.Join(fixture.output, filepath.FromSlash(relative)), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	mirror := t.TempDir()
	_, err = ActivateMirror(context.Background(), fixture.mirrorOptions(mirror, fixture.output, "", indexpack.Digest(timestampRaw), true))
	if !errors.Is(err, ErrInvalidMirrorActivation) || !strings.Contains(err.Error(), "timestamp <= snapshot <= targets < root") {
		t.Fatalf("invalid expiry profile activation = %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(mirror, "metadata", "timestamp.json")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("invalid expiry profile published head: %v", statErr)
	}
}

func TestCopyMirrorFileHonorsCancellationBetweenChunks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &cancelingReader{cancel: cancel}
	written, err := copyMirrorFile(ctx, io.Discard, reader)
	if !errors.Is(err, context.Canceled) || written == 0 || reader.calls != 1 {
		t.Fatalf("copy cancellation = written=%d calls=%d err=%v", written, reader.calls, err)
	}
}

type cancelingReader struct {
	cancel context.CancelFunc
	calls  int
}

func (reader *cancelingReader) Read(buffer []byte) (int, error) {
	reader.calls++
	for index := range buffer {
		buffer[index] = 'x'
	}
	reader.cancel()
	return len(buffer), nil
}

func (fixture stageFixture) mirrorOptions(mirror, candidate, currentDigest, candidateDigest string, initialize bool) MirrorActivationOptions {
	return MirrorActivationOptions{
		TrustedRootPath:                  filepath.Join(candidate, "root.json"),
		MirrorDir:                        mirror,
		CandidateDir:                     candidate,
		TargetPath:                       testTargetPath,
		PublisherKeys:                    map[string]ed25519.PublicKey{indexpack.KeyID(fixture.publisherKey): fixture.publisherKey},
		ExpectedCurrentTimestampSHA256:   currentDigest,
		ExpectedCandidateTimestampSHA256: candidateDigest,
		InitializeEmpty:                  initialize,
	}
}
