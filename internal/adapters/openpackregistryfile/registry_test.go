package openpackregistryfile

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
	"strings"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/openpackregistry"
)

func TestLoadAcceptsPrivateOperatorRegistry(t *testing.T) {
	path := writeRegistry(t, t.TempDir())
	registry, digest, err := LoadWithDigest(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	expectedDigest := indexpack.Digest(mustReadRegistryFile(t, path))
	if digest != expectedDigest {
		t.Fatalf("registry digest = %q, want %q", digest, expectedDigest)
	}
	if _, found := registry.Lookup("developer"); !found {
		t.Fatal("developer binding missing")
	}
}

func TestLoadRejectsWritableRegistrySymlinkAndWritableParent(t *testing.T) {
	t.Run("group or world writable registry", func(t *testing.T) {
		path := writeRegistry(t, t.TempDir())
		if err := os.Chmod(path, 0o666); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Load = %v, want ErrUnsafePath", err)
		}
	})

	t.Run("symlink", func(t *testing.T) {
		root := t.TempDir()
		path := writeRegistry(t, root)
		link := filepath.Join(root, "registry-link.json")
		if err := os.Symlink(path, link); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(link); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Load = %v, want ErrUnsafePath", err)
		}
	})

	t.Run("writable parent", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), "operator")
		if err := os.Mkdir(parent, 0o700); err != nil {
			t.Fatal(err)
		}
		path := writeRegistry(t, parent)
		if err := os.Chmod(parent, 0o777); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Load = %v, want ErrUnsafePath", err)
		}
	})
}

func TestLoadRejectsNonCanonicalPath(t *testing.T) {
	if _, err := Load("registry.json"); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("Load = %v, want ErrUnsafePath", err)
	}
}

func TestWriteNewCreatesPrivateValidatedRegistryWithoutOverwrite(t *testing.T) {
	root := t.TempDir()
	currentPath := writeRegistry(t, root)
	registry, err := Load(currentPath)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := registry.Encode()
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := openpackregistry.Load(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "registry-candidate.json")
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	canonicalOutput := filepath.Join(canonicalRoot, filepath.Base(output))

	written, err := WriteNew(context.Background(), output, candidate)
	if err != nil {
		t.Fatalf("WriteNew: %v", err)
	}
	if written != canonicalOutput {
		t.Fatalf("WriteNew path = %q, want %q", written, canonicalOutput)
	}
	info, err := os.Lstat(output)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("candidate mode = %v, %v", info, err)
	}
	if _, err := Load(output); err != nil {
		t.Fatalf("Load candidate: %v", err)
	}
	original := mustReadRegistryFile(t, output)
	if _, err := WriteNew(context.Background(), output, candidate); !errors.Is(err, ErrCandidateExists) {
		t.Fatalf("second WriteNew = %v, want ErrCandidateExists", err)
	}
	if after := mustReadRegistryFile(t, output); !bytes.Equal(after, original) {
		t.Fatal("second WriteNew changed candidate")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".fetchmark-registry-candidate-") {
			t.Fatalf("staging file remains: %q", entry.Name())
		}
	}
}

func TestWriteNewRejectsUnsafeOutputBeforeMutation(t *testing.T) {
	root := t.TempDir()
	registry, err := Load(writeRegistry(t, root))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteNew(context.Background(), "registry-candidate.json", registry); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("relative WriteNew = %v, want ErrUnsafePath", err)
	}
	unsafeParent := filepath.Join(root, "unsafe")
	if err := os.Mkdir(unsafeParent, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(unsafeParent, 0o777); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteNew(context.Background(), filepath.Join(unsafeParent, "registry.json"), registry); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("unsafe parent WriteNew = %v, want ErrUnsafePath", err)
	}
	entries, err := os.ReadDir(unsafeParent)
	if err != nil || len(entries) != 0 {
		t.Fatalf("unsafe parent mutated: %v, %v", entries, err)
	}
}

func TestWriteNewCancellationImmediatelyBeforeActivationPublishesNothing(t *testing.T) {
	root := t.TempDir()
	registry, err := Load(writeRegistry(t, root))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	output := filepath.Join(root, "cancelled-candidate.json")
	dependencies := defaultCandidateWriteDependencies()
	dependencies.beforeActivate = cancel
	_, err = writeNew(ctx, output, registry, dependencies)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteNew cancelled before activation = %v, want context.Canceled", err)
	}
	if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancelled candidate output exists: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), candidateStagingPrefix) {
			t.Fatalf("cancelled candidate left staging file %q", entry.Name())
		}
	}
}

func TestWriteNewPostCommitFailureReturnsRecoveryEvidence(t *testing.T) {
	root := t.TempDir()
	registry, err := Load(writeRegistry(t, root))
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "committed-candidate.json")
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	canonicalOutput := filepath.Join(canonicalRoot, filepath.Base(output))
	injected := errors.New("injected parent sync failure")
	dependencies := defaultCandidateWriteDependencies()
	dependencies.syncParent = func(*os.Root) error { return injected }

	written, err := writeNew(context.Background(), output, registry, dependencies)
	if written != canonicalOutput {
		t.Fatalf("written path = %q, want %q", written, canonicalOutput)
	}
	var committed *CommittedCandidateError
	if !errors.As(err, &committed) {
		t.Fatalf("WriteNew error = %v, want CommittedCandidateError", err)
	}
	if committed.Path != canonicalOutput || !errors.Is(err, injected) {
		t.Fatalf("committed error = %#v, %v", committed, err)
	}
	if _, statErr := os.Lstat(output); statErr != nil {
		t.Fatalf("committed candidate missing: %v", statErr)
	}
	entries, readErr := os.ReadDir(root)
	if readErr != nil {
		t.Fatal(readErr)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), candidateStagingPrefix) {
			t.Fatalf("committed candidate left staging file %q", entry.Name())
		}
	}
}

func mustReadRegistryFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func writeRegistry(t *testing.T, root string) string {
	t.Helper()
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	digest := strings.Repeat("a", 64)
	document := map[string]any{
		"version": 1,
		"publishers": []map[string]any{{
			"id": "fixture", "key_id": indexpack.KeyID(publicKey),
			"ed25519_public_key": base64.StdEncoding.EncodeToString(publicKey),
		}},
		"bindings": []map[string]any{{
			"source_id": "developer", "pack_id": "developer-en", "publisher_id": "fixture",
			"manifest_sha256": digest, "revision": 1, "record_count": 1,
			"created_at":     now.Add(-time.Hour).Format(time.RFC3339),
			"expires_at":     now.Add(time.Hour).Format(time.RFC3339),
			"installed_path": filepath.Join(root, "objects", digest),
		}},
	}
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "registry.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
