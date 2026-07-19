package tufrepository

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
)

func TestWriteCeremonyArtifactAtomicallyCreatesDurableNoOverwriteFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "candidate.json")
	expectedPath := canonicalCeremonyPath(t, path)
	raw := []byte(`{"candidate":true}`)
	result, err := WriteCeremonyArtifact(context.Background(), path, raw, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if result.Path != expectedPath || result.SHA256 != digestBytes(raw) || result.Bytes != uint64(len(raw)) {
		t.Fatalf("write result = %#v", result)
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || !bytes.Equal(mustReadCeremonyArtifact(t, path), raw) {
		t.Fatalf("artifact = %v, %v", info, err)
	}
	if _, err := WriteCeremonyArtifact(context.Background(), path, []byte("replacement"), 1024); !errors.Is(err, ErrCeremonyArtifactExists) {
		t.Fatalf("overwrite = %v", err)
	}
	if !bytes.Equal(mustReadCeremonyArtifact(t, path), raw) {
		t.Fatal("existing artifact changed after rejected overwrite")
	}
}

func TestWriteCeremonyArtifactCancellationLeavesNoFinalOrStagingPath(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "candidate.json")
	ctx, cancel := context.WithCancel(context.Background())
	dependencies := defaultCeremonyArtifactWriteDependencies()
	dependencies.beforeActivate = cancel
	if _, err := writeCeremonyArtifactWithDependencies(ctx, path, []byte("candidate"), 1024, dependencies); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled write = %v", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("canceled artifacts = %v", entries)
	}
}

func TestWriteCeremonyArtifactPostCommitFailureCarriesRecoveryEvidence(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "candidate.json")
	expectedPath := canonicalCeremonyPath(t, path)
	raw := []byte("candidate")
	injected := errors.New("injected parent sync failure")
	dependencies := defaultCeremonyArtifactWriteDependencies()
	dependencies.syncParent = func(*os.Root) error { return injected }
	result, err := writeCeremonyArtifactWithDependencies(context.Background(), path, raw, 1024, dependencies)
	var committed *CommittedCeremonyArtifactError
	if !errors.As(err, &committed) || !errors.Is(err, injected) || result.Path != expectedPath ||
		committed.Result.SHA256 != digestBytes(raw) || !strings.Contains(err.Error(), "inspect and hash") {
		t.Fatalf("post-commit result=%#v error=%v", result, err)
	}
	if !bytes.Equal(mustReadCeremonyArtifact(t, path), raw) {
		t.Fatal("committed artifact is missing or changed")
	}
}

func canonicalCeremonyPath(t *testing.T, path string) string {
	t.Helper()
	parent, err := secureconfigfile.ValidateDirectory(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(parent, filepath.Base(path))
}

func mustReadCeremonyArtifact(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
