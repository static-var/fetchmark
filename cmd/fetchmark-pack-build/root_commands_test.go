package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/adapters/tufrepository"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

func TestChannelRootCommandsCompleteSplitCustodyMigrationWithoutPrintingSecrets(t *testing.T) {
	now := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	base := t.TempDir()
	legacyRaw, currentRootRaw, err := tufrepository.GenerateIdentity(
		"fetchmark-community", now.Add(365*24*time.Hour), deterministicChannelRandom(),
	)
	if err != nil {
		t.Fatal(err)
	}
	legacyPath := writeRootCommandFixture(t, base, "legacy-identity.json", legacyRaw, 0o600)
	currentRootPath := writeRootCommandFixture(t, base, "1.root.json", currentRootRaw, 0o600)
	deps := defaultDependencies()
	deps.now = func() time.Time { return now }
	randomBytes := make([]byte, 256)
	for index := range randomBytes {
		randomBytes[index] = byte(255 - index)
	}
	deps.random = bytes.NewReader(randomBytes)

	privatePaths := []string{filepath.Join(base, "custodian-a.private.json"), filepath.Join(base, "custodian-b.private.json")}
	publicPaths := []string{filepath.Join(base, "custodian-a.public.json"), filepath.Join(base, "custodian-b.public.json")}
	for index := range privatePaths {
		stdout := runRootCommand(t, deps,
			"channel-root-keygen", "-repository-id", "fetchmark-community", "-signer-id", string(rune('a'+index))+"-custodian", "-out", privatePaths[index],
		)
		if bytes.Contains(stdout, []byte("ed25519_private_key")) || bytes.Contains(stdout, []byte("ed25519_public_key")) {
			t.Fatal("root keygen printed private material")
		}
		var result rootKeygenResult
		if err := json.Unmarshal(stdout, &result); err != nil || result.PublicKeyID == "" {
			t.Fatal(err)
		}
		runRootCommand(t, deps, "channel-root-public", "-signer", privatePaths[index], "-out", publicPaths[index])
	}
	originalPrivate := mustRead(t, privatePaths[0])
	var overwriteStdout, overwriteStderr bytes.Buffer
	if code := run(context.Background(), []string{
		"channel-root-keygen", "-repository-id", "fetchmark-community", "-signer-id", "replacement", "-out", privatePaths[0],
	}, &overwriteStdout, &overwriteStderr, deps); code != 1 || !bytes.Equal(originalPrivate, mustRead(t, privatePaths[0])) {
		t.Fatalf("root keygen overwrite code=%d stderr=%q", code, overwriteStderr.String())
	}
	recoveryPrivate := filepath.Join(base, "custodian-recovery.private.json")
	recoveryPublic := filepath.Join(base, "custodian-recovery.public.json")
	var recoveryStderr bytes.Buffer
	if code := run(context.Background(), []string{
		"channel-root-keygen", "-repository-id", "fetchmark-community", "-signer-id", "recovery-custodian", "-out", recoveryPrivate,
	}, failWriter{}, &recoveryStderr, deps); code != 1 || !strings.Contains(recoveryStderr.String(), ErrRootArtifactCommitted.Error()) {
		t.Fatalf("root keygen output failure code=%d stderr=%q", code, recoveryStderr.String())
	}
	if _, err := os.Stat(recoveryPrivate); err != nil {
		t.Fatalf("committed recovery signer missing: %v", err)
	}
	runRootCommand(t, deps, "channel-root-public", "-signer", recoveryPrivate, "-out", recoveryPublic)
	publicRaw, err := secureconfigfile.Read(
		recoveryPublic, secureconfigfile.Options{MaxBytes: tufrepository.MaxRootSignerBytes, Mode: secureconfigfile.PublicConfig},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tufrepository.DecodeRootSignerPublic(publicRaw); err != nil {
		t.Fatalf("recovered public signer: %v", err)
	}

	candidatePath := filepath.Join(base, "2.root.unsigned.json")
	prepareStdout := runRootCommand(t, deps,
		"channel-root-prepare", "-repository-id", "fetchmark-community", "-current-root", currentRootPath,
		"-new-signer", publicPaths[0], "-new-signer", publicPaths[1],
		"-root-expires", now.Add(2*365*24*time.Hour).Format(time.RFC3339), "-out", candidatePath,
	)
	var prepared rootArtifactResult
	if err := json.Unmarshal(prepareStdout, &prepared); err != nil || prepared.RotationProposal == nil {
		t.Fatalf("prepare result = %s, %v", prepareStdout, err)
	}
	currentDigest := indexpack.Digest(currentRootRaw)
	candidateDigest := prepared.SHA256
	committedCandidatePath := filepath.Join(base, "2.root.output-failed.json")
	var committedStderr bytes.Buffer
	if code := run(context.Background(), []string{
		"channel-root-prepare", "-repository-id", "fetchmark-community", "-current-root", currentRootPath,
		"-new-signer", publicPaths[0], "-new-signer", publicPaths[1],
		"-root-expires", now.Add(2 * 365 * 24 * time.Hour).Format(time.RFC3339), "-out", committedCandidatePath,
	}, failWriter{}, &committedStderr, deps); code != 1 || !strings.Contains(committedStderr.String(), ErrRootArtifactCommitted.Error()) ||
		!strings.Contains(committedStderr.String(), committedCandidatePath) {
		t.Fatalf("committed root artifact code=%d stderr=%q", code, committedStderr.String())
	}
	if _, err := os.Stat(committedCandidatePath); err != nil {
		t.Fatalf("committed root candidate missing: %v", err)
	}

	legacyBundlePath := filepath.Join(base, "legacy-contributions.json")
	runRootCommand(t, deps,
		"channel-root-sign-legacy", "-identity", legacyPath, "-current-root", currentRootPath, "-candidate", candidatePath,
		"-expected-current-root-sha256", currentDigest, "-expected-candidate-root-sha256", candidateDigest, "-out", legacyBundlePath,
	)
	contributionPaths := []string{filepath.Join(base, "custodian-a.contribution.json"), filepath.Join(base, "custodian-b.contribution.json")}
	for index := range privatePaths {
		runRootCommand(t, deps,
			"channel-root-sign", "-repository-id", "fetchmark-community", "-current-root", currentRootPath,
			"-candidate", candidatePath, "-signer", privatePaths[index],
			"-expected-current-root-sha256", currentDigest, "-expected-candidate-root-sha256", candidateDigest,
			"-out", contributionPaths[index],
		)
	}
	signedRootPath := filepath.Join(base, "2.root.json")
	assembleStdout := runRootCommand(t, deps,
		"channel-root-assemble", "-repository-id", "fetchmark-community", "-current-root", currentRootPath,
		"-candidate", candidatePath, "-legacy-contributions", legacyBundlePath,
		"-contribution", contributionPaths[0], "-contribution", contributionPaths[1],
		"-expected-current-root-sha256", currentDigest, "-expected-candidate-root-sha256", candidateDigest,
		"-out", signedRootPath,
	)
	var assembled rootArtifactResult
	if err := json.Unmarshal(assembleStdout, &assembled); err != nil || assembled.RotationResult == nil || assembled.RotationResult.ToVersion != 2 {
		t.Fatalf("assemble result = %s, %v", assembleStdout, err)
	}
	operationalPath := filepath.Join(base, "operational-identity.json")
	applyStdout := runRootCommand(t, deps,
		"channel-root-apply", "-identity", legacyPath, "-signed-root", signedRootPath,
		"-expected-current-root-sha256", currentDigest, "-expected-signed-root-sha256", assembled.SHA256,
		"-out", operationalPath,
	)
	var applied tufrepository.IdentityWriteResult
	if err := json.Unmarshal(applyStdout, &applied); err != nil || applied.RootVersion != 2 || applied.RootSHA256 != assembled.SHA256 {
		t.Fatalf("apply result = %s, %v", applyStdout, err)
	}
	operationalRaw, err := secureconfigfile.Read(
		operationalPath, secureconfigfile.Options{MaxBytes: tufrepository.MaxIdentityBytes, Mode: secureconfigfile.PrivateIdentity},
	)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(operationalRaw, []byte(`"root_keys"`)) || bytes.Contains(operationalRaw, []byte("legacy-root")) {
		t.Fatal("operational identity retained root custody material")
	}
	operational, err := tufrepository.DecodeIdentity(operationalRaw)
	if err != nil {
		t.Fatal(err)
	}
	active, err := operational.ActiveRoot()
	if err != nil || indexpack.Digest(active) != assembled.SHA256 {
		t.Fatalf("operational active root digest = %s, %v", indexpack.Digest(active), err)
	}
}

func TestChannelRootCommandParserRequiresExplicitUnambiguousArtifacts(t *testing.T) {
	for _, args := range [][]string{
		{"channel-root-prepare", "-repository-id", "repo", "-current-root", "/tmp/root", "-new-signer", "/tmp/a", "-root-expires", "2027-01-01T00:00:00Z", "-out", "/tmp/out"},
		{"channel-root-assemble", "-repository-id", "repo", "-current-root", "/tmp/root", "-candidate", "/tmp/candidate", "-legacy-contributions", "/tmp/legacy", "-contribution", "/tmp/one", "-expected-current-root-sha256", strings.Repeat("a", 64), "-expected-candidate-root-sha256", strings.Repeat("b", 64), "-out", "/tmp/out"},
	} {
		if _, err := parseRequest(args, ioDiscard{}); err == nil {
			t.Fatalf("parseRequest accepted incomplete explicit ceremony: %v", args)
		}
	}
	if _, err := parseRequest([]string{
		"channel-root-public", "-signer", "/tmp/signer", "-out", "/tmp/public", "-repository-id", "irrelevant",
	}, ioDiscard{}); err == nil || !strings.Contains(err.Error(), "not valid") {
		t.Fatalf("parser accepted irrelevant ceremony flag: %v", err)
	}
}

type ioDiscard struct{}

func (ioDiscard) Write(raw []byte) (int, error) { return len(raw), nil }

func runRootCommand(t *testing.T, deps dependencies, args ...string) []byte {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), args, &stdout, &stderr, deps); code != 0 {
		t.Fatalf("%v code=%d stdout=%q stderr=%q", args, code, stdout.String(), stderr.String())
	}
	return append([]byte(nil), stdout.Bytes()...)
}

func writeRootCommandFixture(t *testing.T, base, name string, raw []byte, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(base, name)
	if err := os.WriteFile(path, raw, mode); err != nil {
		t.Fatal(err)
	}
	return path
}
