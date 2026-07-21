package tufrepository

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

func TestWriteIdentityAtomicallyCreatesPrivateStableFile(t *testing.T) {
	raw, rootRaw := generatedIdentityFixture(t)
	directory := t.TempDir()
	output := filepath.Join(directory, "channel-identity.json")
	canonicalDirectory, err := secureconfigfile.ValidateDirectory(directory)
	if err != nil {
		t.Fatal(err)
	}
	expectedOutput := filepath.Join(canonicalDirectory, filepath.Base(output))
	result, err := WriteIdentity(context.Background(), output, raw)
	if err != nil {
		t.Fatalf("WriteIdentity: %v", err)
	}
	if result.Path != expectedOutput || result.RepositoryID != "fetchmark-community" ||
		result.RootSHA256 != digestBytes(rootRaw) || result.RootThreshold != 2 {
		t.Fatalf("result = %#v", result)
	}
	info, err := os.Lstat(output)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("identity mode = %v", info.Mode())
	}
	if got, err := os.ReadFile(output); err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("identity bytes differ: %v", err)
	}
	if _, err := WriteIdentity(context.Background(), output, raw); !errors.Is(err, ErrIdentityExists) {
		t.Fatalf("second WriteIdentity error = %v", err)
	}
	if got, err := os.ReadFile(output); err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("existing identity changed: %v", err)
	}
}

func TestWriteIdentityCancellationLeavesNoOutputOrStagingFile(t *testing.T) {
	raw, _ := generatedIdentityFixture(t)
	directory := t.TempDir()
	output := filepath.Join(directory, "channel-identity.json")
	ctx, cancel := context.WithCancel(context.Background())
	dependencies := defaultIdentityWriteDependencies()
	dependencies.beforeActivate = cancel
	if _, err := writeIdentityWithDependencies(ctx, output, raw, dependencies); !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteIdentity error = %v", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("canceled identity artifacts = %v", entries)
	}
}

func TestWriteIdentityPostCommitFailureReturnsRecoveryEvidence(t *testing.T) {
	raw, rootRaw := generatedIdentityFixture(t)
	directory := t.TempDir()
	output := filepath.Join(directory, "channel-identity.json")
	canonicalDirectory, err := secureconfigfile.ValidateDirectory(directory)
	if err != nil {
		t.Fatal(err)
	}
	expectedOutput := filepath.Join(canonicalDirectory, filepath.Base(output))
	injected := errors.New("injected parent sync failure")
	dependencies := defaultIdentityWriteDependencies()
	dependencies.syncParent = func(*os.Root) error { return injected }
	result, err := writeIdentityWithDependencies(context.Background(), output, raw, dependencies)
	var committed *CommittedIdentityError
	if !errors.As(err, &committed) || !errors.Is(err, injected) {
		t.Fatalf("WriteIdentity error = %v", err)
	}
	if result.Path != expectedOutput || committed.Result.Path != expectedOutput ||
		committed.Result.RootSHA256 != digestBytes(rootRaw) || !strings.Contains(err.Error(), "inspect the committed identity") {
		t.Fatalf("post-commit result=%#v error=%v", result, err)
	}
	if _, statErr := os.Stat(output); statErr != nil {
		t.Fatalf("committed identity missing: %v", statErr)
	}
}

func TestWriteIdentityRejectsInvalidBytesBeforeMutation(t *testing.T) {
	directory := t.TempDir()
	output := filepath.Join(directory, "channel-identity.json")
	if _, err := WriteIdentity(context.Background(), output, []byte(`{"version":1}`)); err == nil {
		t.Fatal("WriteIdentity accepted invalid identity")
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("invalid identity artifacts = %v", entries)
	}
}

func generatedIdentityFixture(t *testing.T) ([]byte, []byte) {
	t.Helper()
	raw, rootRaw, err := GenerateIdentity(
		"fetchmark-community",
		time.Date(2027, 7, 1, 0, 0, 0, 0, time.UTC),
		deterministicIdentityRandom(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return raw, rootRaw
}

func digestBytes(raw []byte) string {
	return indexpack.Digest(raw)
}
