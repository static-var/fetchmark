package openpackregistryfile

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/openpackregistry"
)

func TestActivateCASReplacesRegistryAndRetainsExactRollbackBytes(t *testing.T) {
	fixture := newActivationFixture(t)
	result, err := activateFixture(context.Background(), fixture.options(openpackregistry.TransitionActivate))
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if result.Status != ActivationStatusActivated || result.FromRegistrySHA256 != fixture.currentDigest ||
		result.ToRegistrySHA256 != fixture.candidateDigest || result.RestartRequired != true {
		t.Fatalf("result = %#v", result)
	}
	if got := mustReadRegistryFile(t, fixture.currentPath); !bytes.Equal(got, fixture.candidateRaw) {
		t.Fatal("active registry is not the exact candidate bytes")
	}
	if got := mustReadRegistryFile(t, fixture.rollbackPath); !bytes.Equal(got, fixture.currentRaw) {
		t.Fatal("rollback file is not the exact previous registry bytes")
	}
	if info, err := os.Lstat(fixture.rollbackPath); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("rollback file mode = %v, %v", info, err)
	}

	recovered, err := activateFixture(context.Background(), fixture.options(openpackregistry.TransitionActivate))
	if err != nil || recovered.Status != ActivationStatusAlreadyApplied || !recovered.RestartRequired {
		t.Fatalf("idempotent recovery = %#v, %v", recovered, err)
	}
}

func TestActivateCASRejectsStaleCurrentWithoutMutation(t *testing.T) {
	fixture := newActivationFixture(t)
	options := fixture.options(openpackregistry.TransitionActivate)
	options.ExpectedCurrentSHA256 = strings.Repeat("f", 64)
	if _, err := activateFixture(context.Background(), options); !errors.Is(err, ErrCASMismatch) {
		t.Fatalf("Activate stale current = %v", err)
	}
	if got := mustReadRegistryFile(t, fixture.currentPath); !bytes.Equal(got, fixture.currentRaw) {
		t.Fatal("stale activation changed current registry")
	}
	if _, err := os.Lstat(fixture.rollbackPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale activation wrote rollback file: %v", err)
	}
}

func TestActivateCASRejectsCandidateDigestMutationBeforeLocking(t *testing.T) {
	fixture := newActivationFixture(t)
	if err := os.WriteFile(fixture.candidatePath, append(fixture.candidateRaw, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := activateFixture(context.Background(), fixture.options(openpackregistry.TransitionActivate)); !errors.Is(err, ErrCASMismatch) {
		t.Fatalf("Activate mutated candidate = %v", err)
	}
	if got := mustReadRegistryFile(t, fixture.currentPath); !bytes.Equal(got, fixture.currentRaw) {
		t.Fatal("candidate mutation changed current registry")
	}
	if _, err := os.Lstat(fixture.rollbackPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("candidate mutation wrote rollback file: %v", err)
	}
}

func TestActivateCASRejectsTamperedExistingRollbackWithoutReplacingCurrent(t *testing.T) {
	fixture := newActivationFixture(t)
	if err := os.WriteFile(fixture.rollbackPath, fixture.candidateRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := activateFixture(context.Background(), fixture.options(openpackregistry.TransitionActivate)); err == nil ||
		!strings.Contains(err.Error(), "existing rollback output differs") {
		t.Fatalf("Activate tampered rollback = %v", err)
	}
	if got := mustReadRegistryFile(t, fixture.currentPath); !bytes.Equal(got, fixture.currentRaw) {
		t.Fatal("tampered rollback changed current registry")
	}
	if got := mustReadRegistryFile(t, fixture.rollbackPath); !bytes.Equal(got, fixture.candidateRaw) {
		t.Fatal("tampered rollback was overwritten")
	}
}

func TestActivateCASWaitingForCooperatingWriterHonorsCancellation(t *testing.T) {
	fixture := newActivationFixture(t)
	lockPath := filepath.Join(filepath.Dir(fixture.currentPath), activationLockName(filepath.Base(fixture.currentPath)))
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lockFile.Close() })
	locked, err := tryLockRegistryFile(lockFile)
	if err != nil || !locked {
		t.Fatalf("hold activation lock: locked=%v err=%v", locked, err)
	}
	t.Cleanup(func() { _ = unlockRegistryFile(lockFile) })

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, err := activateFixture(ctx, fixture.options(openpackregistry.TransitionActivate)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Activate waiting on lock = %v", err)
	}
	if got := mustReadRegistryFile(t, fixture.currentPath); !bytes.Equal(got, fixture.currentRaw) {
		t.Fatal("canceled lock wait changed current registry")
	}
	if _, err := os.Lstat(fixture.rollbackPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled lock wait wrote rollback file: %v", err)
	}
}

func TestActivateCASCancellationAfterRollbackPreparationDoesNotReplaceCurrent(t *testing.T) {
	fixture := newActivationFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	dependencies := defaultActivationDependencies()
	dependencies.validateProjection = acceptProjectionFixture
	dependencies.beforeReplace = cancel
	result, err := activateWithDependencies(ctx, fixture.options(openpackregistry.TransitionActivate), dependencies)
	var prepared *PreparedActivationError
	if !errors.As(err, &prepared) || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled activation = %#v, %v", result, err)
	}
	if prepared.Result.RollbackPath != fixture.rollbackPath || prepared.Result.FromRegistrySHA256 != fixture.currentDigest {
		t.Fatalf("prepared recovery evidence = %#v", prepared.Result)
	}
	if prepared.Result.RestartRequired {
		t.Fatal("prepared activation unexpectedly requires restart")
	}
	if got := mustReadRegistryFile(t, fixture.currentPath); !bytes.Equal(got, fixture.currentRaw) {
		t.Fatal("canceled activation replaced current registry")
	}
	if got := mustReadRegistryFile(t, fixture.rollbackPath); !bytes.Equal(got, fixture.currentRaw) {
		t.Fatal("canceled activation did not retain exact rollback bytes")
	}
}

func TestActivateCASCancellationAfterFinalReadDoesNotReplaceCurrent(t *testing.T) {
	fixture := newActivationFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	dependencies := defaultActivationDependencies()
	dependencies.validateProjection = acceptProjectionFixture
	dependencies.afterFinalRead = cancel
	result, err := activateWithDependencies(ctx, fixture.options(openpackregistry.TransitionActivate), dependencies)
	var prepared *PreparedActivationError
	if !errors.As(err, &prepared) || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled final read activation = %#v, %v", result, err)
	}
	if prepared.Result.Status != ActivationStatusPrepared || prepared.Result.RollbackPath != fixture.rollbackPath || prepared.Result.RestartRequired {
		t.Fatalf("final-read cancellation evidence = %#v", prepared.Result)
	}
	if got := mustReadRegistryFile(t, fixture.currentPath); !bytes.Equal(got, fixture.currentRaw) {
		t.Fatal("final-read cancellation replaced current registry")
	}
	if got := mustReadRegistryFile(t, fixture.rollbackPath); !bytes.Equal(got, fixture.currentRaw) {
		t.Fatal("final-read cancellation lost rollback bytes")
	}
}

func TestActivateCASRevalidatesProjectionWithFreshClockBeforeReplace(t *testing.T) {
	fixture := newActivationFixture(t)
	candidate, err := Load(fixture.candidatePath)
	if err != nil {
		t.Fatal(err)
	}
	binding, found := candidate.Lookup("developer")
	if !found {
		t.Fatal("candidate binding missing")
	}
	expiresAt, err := time.Parse(time.RFC3339, binding.ExpiresAt)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("projection expired")
	clockCalls := 0
	dependencies := defaultActivationDependencies()
	dependencies.now = func() time.Time {
		clockCalls++
		if clockCalls == 1 {
			return expiresAt.Add(-time.Minute)
		}
		return expiresAt
	}
	dependencies.validateProjection = func(_ context.Context, _ openpackregistry.Binding, now time.Time) error {
		if !now.Before(expiresAt) {
			return injected
		}
		return nil
	}
	result, err := activateWithDependencies(context.Background(), fixture.options(openpackregistry.TransitionActivate), dependencies)
	var prepared *PreparedActivationError
	if !errors.As(err, &prepared) || !errors.Is(err, injected) {
		t.Fatalf("expiry before replace = %#v, %v", result, err)
	}
	if clockCalls != 2 || prepared.Result.Status != ActivationStatusPrepared || prepared.Result.RestartRequired {
		t.Fatalf("expiry clock calls=%d evidence=%#v", clockCalls, prepared.Result)
	}
	if got := mustReadRegistryFile(t, fixture.currentPath); !bytes.Equal(got, fixture.currentRaw) {
		t.Fatal("expired projection replaced current registry")
	}
	if got := mustReadRegistryFile(t, fixture.rollbackPath); !bytes.Equal(got, fixture.currentRaw) {
		t.Fatal("expired projection lost rollback bytes")
	}
}

func TestActivateCASRollbackArchiveCloseFailureReturnsPreparedEvidence(t *testing.T) {
	fixture := newActivationFixture(t)
	injected := errors.New("injected rollback directory close failure")
	dependencies := defaultActivationDependencies()
	dependencies.validateProjection = acceptProjectionFixture
	dependencies.closeRollbackDirectory = func(directory *os.File) error {
		return errors.Join(directory.Close(), injected)
	}
	result, err := activateWithDependencies(context.Background(), fixture.options(openpackregistry.TransitionActivate), dependencies)
	var prepared *PreparedActivationError
	if !errors.As(err, &prepared) || !errors.Is(err, injected) {
		t.Fatalf("rollback close failure = %#v, %v", result, err)
	}
	if prepared.Result.Status != ActivationStatusPrepared || prepared.Result.RollbackPath != fixture.rollbackPath ||
		prepared.Result.RollbackRegistrySHA256 != fixture.currentDigest || prepared.Result.RestartRequired {
		t.Fatalf("prepared rollback evidence = %#v", prepared.Result)
	}
	if got := mustReadRegistryFile(t, fixture.currentPath); !bytes.Equal(got, fixture.currentRaw) {
		t.Fatal("rollback close failure replaced current registry")
	}
	if got := mustReadRegistryFile(t, fixture.rollbackPath); !bytes.Equal(got, fixture.currentRaw) {
		t.Fatal("rollback close failure lost committed rollback bytes")
	}
}

func TestActivateCASPostCommitFailureReturnsExactRecoveryEvidence(t *testing.T) {
	fixture := newActivationFixture(t)
	injected := errors.New("injected parent sync failure")
	dependencies := defaultActivationDependencies()
	dependencies.validateProjection = acceptProjectionFixture
	dependencies.syncParentAfterReplace = func(*os.Root) error { return injected }
	result, err := activateWithDependencies(context.Background(), fixture.options(openpackregistry.TransitionActivate), dependencies)
	var committed *CommittedActivationError
	if !errors.As(err, &committed) || !errors.Is(err, injected) {
		t.Fatalf("post-commit activation = %#v, %v", result, err)
	}
	if committed.Result.Path != fixture.currentPath || committed.Result.RollbackPath != fixture.rollbackPath ||
		committed.Result.FromRegistrySHA256 != fixture.currentDigest || committed.Result.ToRegistrySHA256 != fixture.candidateDigest ||
		!committed.Result.RestartRequired {
		t.Fatalf("committed evidence = %#v", committed.Result)
	}
	if got := mustReadRegistryFile(t, fixture.currentPath); !bytes.Equal(got, fixture.candidateRaw) {
		t.Fatal("post-commit registry does not contain candidate bytes")
	}
}

func TestActivateCASExplicitRollbackUsesSameAuditedPrimitive(t *testing.T) {
	fixture := newActivationFixture(t)
	first, err := activateFixture(context.Background(), fixture.options(openpackregistry.TransitionActivate))
	if err != nil {
		t.Fatal(err)
	}
	rollbackCandidate := fixture.rollbackPath
	rollbackArchive := filepath.Join(filepath.Dir(fixture.currentPath), "rollback-newer.json")
	result, err := activateFixture(context.Background(), ActivationOptions{
		Mode: openpackregistry.TransitionRollback, SourceID: "developer", RegistryPath: fixture.currentPath,
		CandidatePath: rollbackCandidate, RollbackPath: rollbackArchive,
		ExpectedCurrentSHA256: first.ToRegistrySHA256, ExpectedCandidateSHA256: first.FromRegistrySHA256,
	})
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if result.Status != ActivationStatusRolledBack || result.Mode != openpackregistry.TransitionRollback {
		t.Fatalf("rollback result = %#v", result)
	}
	if got := mustReadRegistryFile(t, fixture.currentPath); !bytes.Equal(got, fixture.currentRaw) {
		t.Fatal("rollback did not restore exact prior registry")
	}
	if got := mustReadRegistryFile(t, rollbackArchive); !bytes.Equal(got, fixture.candidateRaw) {
		t.Fatal("rollback did not retain the replaced newer registry")
	}
}

func TestActivateCASConcurrentRetryHasOneCommitAndOneRecovery(t *testing.T) {
	fixture := newActivationFixture(t)
	results := make(chan ActivationResult, 2)
	errorsFound := make(chan error, 2)
	var start sync.WaitGroup
	start.Add(1)
	for range 2 {
		go func() {
			start.Wait()
			result, err := activateFixture(context.Background(), fixture.options(openpackregistry.TransitionActivate))
			results <- result
			errorsFound <- err
		}()
	}
	start.Done()
	statuses := map[ActivationStatus]int{}
	for range 2 {
		result := <-results
		if err := <-errorsFound; err != nil {
			t.Fatalf("concurrent activation: %v", err)
		}
		statuses[result.Status]++
	}
	if statuses[ActivationStatusActivated] != 1 || statuses[ActivationStatusAlreadyApplied] != 1 {
		t.Fatalf("concurrent statuses = %#v", statuses)
	}
}

func TestActivateCASCompetingCandidatesHaveOneWinner(t *testing.T) {
	fixture := newActivationFixture(t)
	current, _, err := LoadWithDigest(fixture.currentPath)
	if err != nil {
		t.Fatal(err)
	}
	binding, found := current.Lookup("developer")
	if !found {
		t.Fatal("developer binding missing")
	}
	created, err := time.Parse(time.RFC3339, binding.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	secondManifestDigest := strings.Repeat("c", 64)
	second, err := current.Promote("developer", openpackregistry.Promotion{
		PackID: binding.PackID, SigningKeyID: binding.SigningKeyID, Kind: indexpack.KindSnapshot,
		ManifestSHA256: secondManifestDigest, Revision: binding.Revision + 1,
		ManifestRecordCount: binding.RecordCount + 2, ProjectionRecordCount: binding.RecordCount + 2,
		CreatedAt: created.Add(2 * time.Minute).Format(time.RFC3339), ExpiresAt: created.Add(3 * time.Hour).Format(time.RFC3339),
		InstalledPath: filepath.Join(filepath.Dir(binding.InstalledPath), secondManifestDigest),
	})
	if err != nil {
		t.Fatal(err)
	}
	secondPath := filepath.Join(filepath.Dir(fixture.currentPath), "candidate-second.json")
	if _, err := WriteNew(context.Background(), secondPath, second); err != nil {
		t.Fatal(err)
	}
	secondRaw := mustReadRegistryFile(t, secondPath)
	secondDigest := indexpack.Digest(secondRaw)

	type outcome struct {
		name     string
		result   ActivationResult
		err      error
		rollback string
		digest   string
	}
	requests := []struct {
		name     string
		path     string
		raw      []byte
		digest   string
		rollback string
	}{
		{name: "first", path: fixture.candidatePath, raw: fixture.candidateRaw, digest: fixture.candidateDigest, rollback: fixture.rollbackPath},
		{name: "second", path: secondPath, raw: secondRaw, digest: secondDigest, rollback: filepath.Join(filepath.Dir(fixture.currentPath), "rollback-second.json")},
	}
	results := make(chan outcome, len(requests))
	var start sync.WaitGroup
	start.Add(1)
	for _, request := range requests {
		go func() {
			start.Wait()
			options := fixture.options(openpackregistry.TransitionActivate)
			options.CandidatePath = request.path
			options.ExpectedCandidateSHA256 = request.digest
			options.RollbackPath = request.rollback
			result, err := activateFixture(context.Background(), options)
			results <- outcome{name: request.name, result: result, err: err, rollback: request.rollback, digest: request.digest}
		}()
	}
	start.Done()

	var winner outcome
	losers := 0
	for range requests {
		found := <-results
		if found.err == nil {
			if winner.name != "" {
				t.Fatalf("multiple competing winners: %s and %s", winner.name, found.name)
			}
			winner = found
			continue
		}
		if !errors.Is(found.err, ErrCASMismatch) {
			t.Fatalf("competing candidate %s error = %v", found.name, found.err)
		}
		losers++
		if _, err := os.Lstat(found.rollback); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("losing candidate %s wrote rollback file: %v", found.name, err)
		}
	}
	if winner.name == "" || losers != 1 || winner.result.Status != ActivationStatusActivated {
		t.Fatalf("winner=%#v losers=%d", winner, losers)
	}
	if got := indexpack.Digest(mustReadRegistryFile(t, fixture.currentPath)); got != winner.digest {
		t.Fatalf("active digest=%s, winning digest=%s", got, winner.digest)
	}
	if got := mustReadRegistryFile(t, winner.rollback); !bytes.Equal(got, fixture.currentRaw) {
		t.Fatal("winning candidate did not preserve exact previous registry")
	}
}

type activationFixture struct {
	currentPath     string
	candidatePath   string
	rollbackPath    string
	currentRaw      []byte
	candidateRaw    []byte
	currentDigest   string
	candidateDigest string
}

func newActivationFixture(t *testing.T) activationFixture {
	t.Helper()
	root := t.TempDir()
	currentPath := writeRegistry(t, root)
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	currentPath = filepath.Join(canonicalRoot, filepath.Base(currentPath))
	current, currentDigest, err := LoadWithDigest(currentPath)
	if err != nil {
		t.Fatal(err)
	}
	binding, found := current.Lookup("developer")
	if !found {
		t.Fatal("developer binding missing")
	}
	digest := strings.Repeat("b", 64)
	created, err := time.Parse(time.RFC3339, binding.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := current.Promote("developer", openpackregistry.Promotion{
		PackID: binding.PackID, SigningKeyID: binding.SigningKeyID, Kind: indexpack.KindSnapshot,
		ManifestSHA256: digest, Revision: binding.Revision + 1,
		ManifestRecordCount: binding.RecordCount + 1, ProjectionRecordCount: binding.RecordCount + 1,
		CreatedAt: created.Add(time.Minute).Format(time.RFC3339), ExpiresAt: created.Add(2 * time.Hour).Format(time.RFC3339),
		InstalledPath: filepath.Join(filepath.Dir(binding.InstalledPath), digest),
	})
	if err != nil {
		t.Fatal(err)
	}
	candidatePath := filepath.Join(canonicalRoot, "candidate.json")
	if _, err := WriteNew(context.Background(), candidatePath, candidate); err != nil {
		t.Fatal(err)
	}
	candidateRaw := mustReadRegistryFile(t, candidatePath)
	return activationFixture{
		currentPath: currentPath, candidatePath: candidatePath, rollbackPath: filepath.Join(canonicalRoot, "rollback-previous.json"),
		currentRaw: mustReadRegistryFile(t, currentPath), candidateRaw: candidateRaw,
		currentDigest: currentDigest, candidateDigest: indexpack.Digest(candidateRaw),
	}
}

func (fixture activationFixture) options(mode openpackregistry.TransitionMode) ActivationOptions {
	return ActivationOptions{
		Mode: mode, SourceID: "developer", RegistryPath: fixture.currentPath, CandidatePath: fixture.candidatePath,
		RollbackPath: fixture.rollbackPath, ExpectedCurrentSHA256: fixture.currentDigest,
		ExpectedCandidateSHA256: fixture.candidateDigest,
	}
}

func activateFixture(ctx context.Context, options ActivationOptions) (ActivationResult, error) {
	dependencies := defaultActivationDependencies()
	dependencies.validateProjection = acceptProjectionFixture
	return activateWithDependencies(ctx, options, dependencies)
}

func acceptProjectionFixture(context.Context, openpackregistry.Binding, time.Time) error { return nil }

func TestActivateRequiresDestinationProjectionToOpenBeforeMutation(t *testing.T) {
	fixture := newActivationFixture(t)
	if _, err := Activate(context.Background(), fixture.options(openpackregistry.TransitionActivate)); err == nil ||
		!strings.Contains(err.Error(), "verify destination projection") {
		t.Fatalf("Activate missing projection = %v", err)
	}
	if got := mustReadRegistryFile(t, fixture.currentPath); !bytes.Equal(got, fixture.currentRaw) {
		t.Fatal("missing projection changed current registry")
	}
	if _, err := os.Lstat(fixture.rollbackPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing projection wrote rollback file: %v", err)
	}
}
