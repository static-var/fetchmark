package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/adapters/federationconfigfile"
)

func TestKeygenCreatesPrivateIdentityAndPrintsOnlyPublicTrustEntry(t *testing.T) {
	output := filepath.Join(t.TempDir(), "identity.json")
	random := bytes.NewReader(bytes.Repeat([]byte{7}, 64))
	stdout, stderr, code := runCommand(t, dependencies{random: random, createPrivate: defaultDependencies().createPrivate},
		"keygen", "-identity-id", "node.alpha", "-out", output)
	if code != 0 || stderr != "" {
		t.Fatalf("keygen code=%d stderr=%q", code, stderr)
	}
	info, err := os.Lstat(output)
	if err != nil || info.Mode().Perm() != 0o600 || !info.Mode().IsRegular() {
		t.Fatalf("private identity mode = %v, err=%v", info.Mode(), err)
	}
	identity, err := federationconfigfile.LoadIdentity(output)
	if err != nil {
		t.Fatalf("LoadIdentity: %v", err)
	}
	var public publicIdentity
	if err := json.Unmarshal([]byte(stdout), &public); err != nil {
		t.Fatalf("decode public output: %v", err)
	}
	if public.ID != identity.IdentityID() || public.KeyID != identity.KeyID() ||
		public.Ed25519PublicKey != base64.StdEncoding.EncodeToString(identity.PublicKey()) {
		t.Fatalf("public output = %#v", public)
	}
	privateRaw, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stdout, base64.StdEncoding.EncodeToString(identity.PrivateKey())) || strings.Contains(stderr, string(privateRaw)) {
		t.Fatal("command output exposed private identity material")
	}
}

func TestKeygenNeverOverwritesExistingIdentity(t *testing.T) {
	output := filepath.Join(t.TempDir(), "identity.json")
	if err := os.WriteFile(output, []byte("preserve me"), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, code := runCommand(t, dependencies{
		random: bytes.NewReader(bytes.Repeat([]byte{9}, 64)), createPrivate: defaultDependencies().createPrivate,
	}, "keygen", "-identity-id", "node.alpha", "-out", output)
	if code != 1 || stdout != "" || !strings.Contains(stderr, "without overwrite") {
		t.Fatalf("keygen code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	raw, err := os.ReadFile(output)
	if err != nil || string(raw) != "preserve me" {
		t.Fatalf("existing identity changed = %q, %v", raw, err)
	}
}

func TestKeygenRejectsInvalidArgumentsBeforeWriting(t *testing.T) {
	root := t.TempDir()
	tests := []struct {
		name string
		args []string
		part string
	}{
		{name: "missing command", part: "expected exactly one subcommand"},
		{name: "unknown command", args: []string{"trust"}, part: "unknown subcommand"},
		{name: "missing identity", args: []string{"keygen", "-out", filepath.Join(root, "one")}, part: "-identity-id is required"},
		{name: "relative output", args: []string{"keygen", "-identity-id", "node.alpha", "-out", "identity.json"}, part: "clean absolute"},
		{name: "positionals", args: []string{"keygen", "-identity-id", "node.alpha", "-out", filepath.Join(root, "two"), "extra"}, part: "unexpected positional"},
		{name: "duplicate", args: []string{"keygen", "-identity-id", "node.alpha", "-identity-id", "node.beta", "-out", filepath.Join(root, "three")}, part: "may only be specified once"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stdout, stderr, code := runCommand(t, defaultDependencies(), test.args...)
			if code != 2 || stdout != "" || !strings.Contains(stderr, test.part) {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout, stderr)
			}
		})
	}
}

func TestKeygenRejectsInvalidIdentityAndRandomFailureWithoutCreatingFile(t *testing.T) {
	root := t.TempDir()
	invalidPath := filepath.Join(root, "invalid.json")
	stdout, stderr, code := runCommand(t, dependencies{
		random: bytes.NewReader(bytes.Repeat([]byte{3}, 64)), createPrivate: defaultDependencies().createPrivate,
	}, "keygen", "-identity-id", "Node Alpha", "-out", invalidPath)
	if code != 1 || stdout != "" || !strings.Contains(stderr, "invalid identity") {
		t.Fatalf("invalid ID code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if _, err := os.Lstat(invalidPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid identity created a file: %v", err)
	}

	failingPath := filepath.Join(root, "random.json")
	stdout, stderr, code = runCommand(t, dependencies{random: errorReader{}, createPrivate: defaultDependencies().createPrivate},
		"keygen", "-identity-id", "node.alpha", "-out", failingPath)
	if code != 1 || stdout != "" || !strings.Contains(stderr, "generate Ed25519 identity") {
		t.Fatalf("random failure code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if _, err := os.Lstat(failingPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("random failure created a file: %v", err)
	}
}

func runCommand(t *testing.T, deps dependencies, args ...string) (string, string, int) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	returnValues := run(args, &stdout, &stderr, deps)
	return stdout.String(), stderr.String(), returnValues
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("synthetic entropy failure") }
