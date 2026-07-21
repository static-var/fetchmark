package tufrepository

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/tufchannel"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/theupdateframework/go-tuf/v2/metadata"
)

func TestRootRotationRequiresOldAndSeparatelyHeldNewThresholds(t *testing.T) {
	fixture := newRootRotationFixture(t)
	signedRaw, result, err := fixture.assemble(t)
	if err != nil {
		t.Fatalf("AssembleRootRotation: %v", err)
	}
	if result.FromVersion != 1 || result.ToVersion != 2 || result.OldThreshold != 2 || result.NewThreshold != 2 ||
		len(result.OldRootKeyIDs) != 2 || len(result.NewRootKeyIDs) != 2 || len(result.SignatureKeyIDs) != 4 ||
		result.CandidateSHA256 != fixture.proposal.CandidateSHA256 || result.SignedRootSHA256 != indexpack.Digest(signedRaw) {
		t.Fatalf("rotation result = %#v", result)
	}
	current, err := metadata.Root().FromBytes(fixture.currentRootRaw)
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := metadata.Root().FromBytes(signedRaw)
	if err != nil {
		t.Fatal(err)
	}
	if len(rotated.Signatures) != 4 || rotated.Signed.Version != 2 || !rotated.Signed.ConsistentSnapshot {
		t.Fatalf("rotated root = version=%d signatures=%d", rotated.Signed.Version, len(rotated.Signatures))
	}
	if err := current.VerifyDelegate(metadata.ROOT, rotated); err != nil {
		t.Fatalf("old threshold: %v", err)
	}
	if err := rotated.VerifyDelegate(metadata.ROOT, rotated); err != nil {
		t.Fatalf("new threshold: %v", err)
	}
	for _, roleName := range []string{metadata.TARGETS, metadata.SNAPSHOT, metadata.TIMESTAMP} {
		if !equalStrings(sortedStrings(current.Signed.Roles[roleName].KeyIDs), sortedStrings(rotated.Signed.Roles[roleName].KeyIDs)) {
			t.Fatalf("%s role changed", roleName)
		}
	}
}

func TestRootRotationConsumerAcceptsSequentialDualThresholdRoot(t *testing.T) {
	fixture := newStageFixture(t)
	if _, err := Stage(context.Background(), fixture.stageOptions(1, "")); err != nil {
		t.Fatal(err)
	}
	currentRootRaw, err := fixture.identity.BootstrapRoot()
	if err != nil {
		t.Fatal(err)
	}
	newPrivate, newPublic := generateRootSigners(t, fixture.identity.repositoryID)
	candidateRaw, proposal, err := PrepareRootRotation(
		fixture.identity.repositoryID, currentRootRaw, newPublic, fixture.now.Add(180*24*time.Hour), fixture.now,
	)
	if err != nil {
		t.Fatal(err)
	}
	contributions, err := SignRootRotationWithLegacyIdentity(
		currentRootRaw, candidateRaw, indexpack.Digest(currentRootRaw), proposal.CandidateSHA256, fixture.identity, fixture.now,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, signerRaw := range newPrivate {
		signer, err := DecodeRootSigner(signerRaw)
		if err != nil {
			t.Fatal(err)
		}
		contribution, err := SignRootRotation(
			fixture.identity.repositoryID, currentRootRaw, candidateRaw, indexpack.Digest(currentRootRaw),
			proposal.CandidateSHA256, signer, fixture.now,
		)
		if err != nil {
			t.Fatal(err)
		}
		contributions = append(contributions, contribution)
	}
	signedRaw, _, err := AssembleRootRotation(
		fixture.identity.repositoryID, currentRootRaw, candidateRaw, indexpack.Digest(currentRootRaw),
		proposal.CandidateSHA256, contributions, fixture.now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixture.output, "metadata", "2.root.json"), signedRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(fixture.root, "rotated-consumer-state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	selected, err := tufchannel.Select(context.Background(), tufchannel.Options{
		TrustedRootPath: filepath.Join(fixture.output, "root.json"), MetadataDir: filepath.Join(fixture.output, "metadata"),
		StateDir: state, BundleDir: fixture.bundle, TargetPath: testTargetPath,
	})
	if err != nil || selected.RootVersion != 2 || selected.Status != tufchannel.StatusSelected {
		t.Fatalf("Select rotated repository = %#v, %v", selected, err)
	}
}

func TestRootRotationRejectsMissingDuplicateAndMisbindingContributions(t *testing.T) {
	fixture := newRootRotationFixture(t)
	all := fixture.contributions(t)
	_, _, err := AssembleRootRotation(
		fixture.repositoryID, fixture.currentRootRaw, fixture.candidateRaw, indexpack.Digest(fixture.currentRootRaw),
		fixture.proposal.CandidateSHA256, all[:3], fixture.now,
	)
	if !errors.Is(err, ErrRootRotationThreshold) {
		t.Fatalf("missing threshold = %v", err)
	}
	duplicate := append(append([][]byte(nil), all[:3]...), all[0])
	_, _, err = AssembleRootRotation(
		fixture.repositoryID, fixture.currentRootRaw, fixture.candidateRaw, indexpack.Digest(fixture.currentRootRaw),
		fixture.proposal.CandidateSHA256, duplicate, fixture.now,
	)
	if !errors.Is(err, ErrInvalidRootRotation) || !strings.Contains(err.Error(), "unique") {
		t.Fatalf("duplicate contribution = %v", err)
	}
	_, _, err = AssembleRootRotation(
		fixture.repositoryID, fixture.currentRootRaw, fixture.candidateRaw, strings.Repeat("f", 64),
		fixture.proposal.CandidateSHA256, all, fixture.now,
	)
	if !errors.Is(err, ErrInvalidRootRotation) {
		t.Fatalf("wrong current digest = %v", err)
	}
	var contribution map[string]any
	if err := json.Unmarshal(all[0], &contribution); err != nil {
		t.Fatal(err)
	}
	contribution["candidate_sha256"] = strings.Repeat("e", 64)
	tampered, _ := json.Marshal(contribution)
	all[0] = tampered
	_, _, err = AssembleRootRotation(
		fixture.repositoryID, fixture.currentRootRaw, fixture.candidateRaw, indexpack.Digest(fixture.currentRootRaw),
		fixture.proposal.CandidateSHA256, all, fixture.now,
	)
	if !errors.Is(err, ErrInvalidRootRotation) || !strings.Contains(err.Error(), "candidate digest differs") {
		t.Fatalf("misbound contribution = %v", err)
	}
}

func TestLegacyContributionBundleRoundTripsExactValidatedContributions(t *testing.T) {
	fixture := newRootRotationFixture(t)
	legacy, err := SignRootRotationWithLegacyIdentity(
		fixture.currentRootRaw, fixture.candidateRaw, indexpack.Digest(fixture.currentRootRaw),
		fixture.proposal.CandidateSHA256, fixture.identity, fixture.now,
	)
	if err != nil {
		t.Fatal(err)
	}
	bundleRaw, err := EncodeRootContributionBundle(legacy)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRootContributionBundle(bundleRaw)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 2 {
		t.Fatalf("decoded contribution count = %d", len(decoded))
	}
	for _, raw := range decoded {
		contribution, err := decodeRootContribution(raw)
		if err != nil || contribution.RepositoryID != fixture.repositoryID ||
			contribution.CandidateSHA256 != fixture.proposal.CandidateSHA256 {
			t.Fatalf("decoded contribution = %#v, %v", contribution, err)
		}
	}
	var bundle RootContributionBundle
	if err := json.Unmarshal(bundleRaw, &bundle); err != nil {
		t.Fatal(err)
	}
	bundle.CandidateSHA256 = strings.Repeat("d", 64)
	tampered, _ := json.Marshal(bundle)
	if _, err := DecodeRootContributionBundle(tampered); !errors.Is(err, ErrInvalidRootRotation) {
		t.Fatalf("tampered bundle = %v", err)
	}
}

func TestRootSignerPublicDocumentContainsNoPrivateKeyAndCandidateDigestIsRequired(t *testing.T) {
	privateRaw, publicRaw, err := GenerateRootSigner("fetchmark-community", "root-custodian-a", bytes.NewReader(bytes.Repeat([]byte{4}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(publicRaw, []byte("private")) || bytes.Contains(publicRaw, privateRaw) {
		t.Fatalf("public signer contains private material: %s", publicRaw)
	}
	signer, err := DecodeRootSigner(privateRaw)
	if err != nil {
		t.Fatal(err)
	}
	public, err := DecodeRootSignerPublic(publicRaw)
	if err != nil || public.KeyID != signer.keyID || public.SignerID != signer.signerID {
		t.Fatalf("public signer = %#v, %v", public, err)
	}
	fixture := newRootRotationFixture(t)
	if _, err := SignRootRotation(
		fixture.repositoryID, fixture.currentRootRaw, fixture.candidateRaw, indexpack.Digest(fixture.currentRootRaw),
		strings.Repeat("0", 64), signer, fixture.now,
	); !errors.Is(err, ErrInvalidRootSigner) {
		t.Fatalf("wrong expected candidate digest = %v", err)
	}
}

func TestPrepareRootRotationIsDeterministicAcrossSignerOrder(t *testing.T) {
	fixture := newRootRotationFixture(t)
	_, public := generateRootSigners(t, fixture.repositoryID)
	forward, forwardProposal, err := PrepareRootRotation(
		fixture.repositoryID, fixture.currentRootRaw, public, fixture.now.Add(400*24*time.Hour), fixture.now,
	)
	if err != nil {
		t.Fatal(err)
	}
	reverse := []RootSignerPublic{public[1], public[0]}
	backward, backwardProposal, err := PrepareRootRotation(
		fixture.repositoryID, fixture.currentRootRaw, reverse, fixture.now.Add(400*24*time.Hour), fixture.now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(forward, backward) || forwardProposal.CandidateSHA256 != backwardProposal.CandidateSHA256 {
		t.Fatal("root proposal changed when the same custodians were supplied in reverse order")
	}
}

func TestRootCustodianRejectsOnlineKeyChangeAndUnrelatedSigner(t *testing.T) {
	fixture := newRootRotationFixture(t)
	candidate, err := metadata.Root().FromBytes(fixture.candidateRaw)
	if err != nil {
		t.Fatal(err)
	}
	oldTimestampID := candidate.Signed.Roles[metadata.TIMESTAMP].KeyIDs[0]
	delete(candidate.Signed.Keys, oldTimestampID)
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{31}, ed25519.SeedSize))
	publicKey, _ := privateKey.Public().(ed25519.PublicKey)
	replacement, err := metadata.KeyFromPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	replacementID, err := replacement.ID()
	if err != nil {
		t.Fatal(err)
	}
	candidate.Signed.Keys[replacementID] = replacement
	candidate.Signed.Roles[metadata.TIMESTAMP].KeyIDs = []string{replacementID}
	maliciousRaw, err := candidate.ToBytes(false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SignRootRotationWithLegacyIdentity(
		fixture.currentRootRaw, maliciousRaw, indexpack.Digest(fixture.currentRootRaw), indexpack.Digest(maliciousRaw),
		fixture.identity, fixture.now,
	); !errors.Is(err, ErrInvalidRootRotation) || !strings.Contains(err.Error(), "preserve every non-root role") {
		t.Fatalf("legacy custodian signed an online-key change: %v", err)
	}

	unrelatedPrivate, _, err := GenerateRootSigner(
		fixture.repositoryID, "unrelated-custodian", bytes.NewReader(bytes.Repeat([]byte{22}, 64)),
	)
	if err != nil {
		t.Fatal(err)
	}
	unrelated, err := DecodeRootSigner(unrelatedPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SignRootRotation(
		fixture.repositoryID, fixture.currentRootRaw, fixture.candidateRaw, indexpack.Digest(fixture.currentRootRaw),
		fixture.proposal.CandidateSHA256, unrelated, fixture.now,
	); !errors.Is(err, ErrInvalidRootSigner) || !strings.Contains(err.Error(), "exactly one side") {
		t.Fatalf("unrelated custodian signed the rotation: %v", err)
	}
}

func TestRootCustodianRejectsWrongCurrentBindingAndNearExpiry(t *testing.T) {
	fixture := newRootRotationFixture(t)
	signer, err := DecodeRootSigner(fixture.newPrivate[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SignRootRotation(
		fixture.repositoryID, fixture.currentRootRaw, fixture.candidateRaw, strings.Repeat("a", 64),
		fixture.proposal.CandidateSHA256, signer, fixture.now,
	); !errors.Is(err, ErrInvalidRootSigner) {
		t.Fatalf("wrong current digest = %v", err)
	}
	if _, err := SignRootRotation(
		"another-repository", fixture.currentRootRaw, fixture.candidateRaw, indexpack.Digest(fixture.currentRootRaw),
		fixture.proposal.CandidateSHA256, signer, fixture.now,
	); !errors.Is(err, ErrInvalidRootSigner) {
		t.Fatalf("wrong repository = %v", err)
	}
	_, public := generateRootSigners(t, fixture.repositoryID)
	if _, _, err := PrepareRootRotation(
		fixture.repositoryID, fixture.currentRootRaw, public, fixture.now.Add(MinimumPublicationHorizon-time.Second), fixture.now,
	); !errors.Is(err, ErrInvalidRootRotation) {
		t.Fatalf("near-expiry proposal = %v", err)
	}
}

func TestRootProfileRejectsMissingNullAndUnreferencedKeysWithoutPanic(t *testing.T) {
	fixture := newRootRotationFixture(t)
	for name, mutate := range map[string]func(*metadata.Metadata[metadata.RootType]){
		"missing": func(root *metadata.Metadata[metadata.RootType]) {
			delete(root.Signed.Keys, root.Signed.Roles[metadata.TIMESTAMP].KeyIDs[0])
		},
		"null": func(root *metadata.Metadata[metadata.RootType]) {
			root.Signed.Keys[root.Signed.Roles[metadata.TIMESTAMP].KeyIDs[0]] = nil
		},
		"unreferenced": func(root *metadata.Metadata[metadata.RootType]) {
			keyID := root.Signed.Roles[metadata.TIMESTAMP].KeyIDs[0]
			root.Signed.Roles[metadata.TIMESTAMP].KeyIDs[0] = root.Signed.Roles[metadata.TARGETS].KeyIDs[0]
			_ = keyID
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate, err := metadata.Root().FromBytes(fixture.candidateRaw)
			if err != nil {
				t.Fatal(err)
			}
			mutate(candidate)
			raw, err := candidate.ToBytes(false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := validateUnsignedRootRotation(mustRoot(t, fixture.currentRootRaw), raw, fixture.now); !errors.Is(err, ErrInvalidRootRotation) {
				t.Fatalf("malformed candidate accepted: %v", err)
			}
		})
	}
}

func mustRoot(t *testing.T, raw []byte) *metadata.Metadata[metadata.RootType] {
	t.Helper()
	root, err := metadata.Root().FromBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

type rootRotationFixture struct {
	repositoryID   string
	now            time.Time
	identity       Identity
	currentRootRaw []byte
	candidateRaw   []byte
	proposal       RootRotationProposal
	newPrivate     [][]byte
}

func newRootRotationFixture(t *testing.T) rootRotationFixture {
	t.Helper()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	repositoryID := "fetchmark-community"
	identityRaw, currentRootRaw, err := GenerateIdentity(repositoryID, now.Add(365*24*time.Hour), deterministicIdentityRandom())
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(identityRaw)
	if err != nil {
		t.Fatal(err)
	}
	newPrivate, newPublic := generateRootSigners(t, repositoryID)
	candidateRaw, proposal, err := PrepareRootRotation(repositoryID, currentRootRaw, newPublic, now.Add(2*365*24*time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	return rootRotationFixture{
		repositoryID: repositoryID, now: now, identity: identity, currentRootRaw: currentRootRaw,
		candidateRaw: candidateRaw, proposal: proposal, newPrivate: newPrivate,
	}
}

func generateRootSigners(t *testing.T, repositoryID string) ([][]byte, []RootSignerPublic) {
	t.Helper()
	privateDocuments := make([][]byte, 0, 2)
	publicDocuments := make([]RootSignerPublic, 0, 2)
	for index, fill := range []byte{9, 10} {
		privateRaw, publicRaw, err := GenerateRootSigner(
			repositoryID, string(rune('a'+index))+"-root-custodian", bytes.NewReader(bytes.Repeat([]byte{fill}, 64)),
		)
		if err != nil {
			t.Fatal(err)
		}
		public, err := DecodeRootSignerPublic(publicRaw)
		if err != nil {
			t.Fatal(err)
		}
		privateDocuments = append(privateDocuments, privateRaw)
		publicDocuments = append(publicDocuments, public)
	}
	return privateDocuments, publicDocuments
}

func (fixture rootRotationFixture) contributions(t *testing.T) [][]byte {
	t.Helper()
	contributions, err := SignRootRotationWithLegacyIdentity(
		fixture.currentRootRaw, fixture.candidateRaw, indexpack.Digest(fixture.currentRootRaw),
		fixture.proposal.CandidateSHA256, fixture.identity, fixture.now,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, privateRaw := range fixture.newPrivate {
		signer, err := DecodeRootSigner(privateRaw)
		if err != nil {
			t.Fatal(err)
		}
		contribution, err := SignRootRotation(
			fixture.repositoryID, fixture.currentRootRaw, fixture.candidateRaw, indexpack.Digest(fixture.currentRootRaw),
			fixture.proposal.CandidateSHA256, signer, fixture.now,
		)
		if err != nil {
			t.Fatal(err)
		}
		contributions = append(contributions, contribution)
	}
	return contributions
}

func (fixture rootRotationFixture) assemble(t *testing.T) ([]byte, RootRotationResult, error) {
	t.Helper()
	return AssembleRootRotation(
		fixture.repositoryID, fixture.currentRootRaw, fixture.candidateRaw, indexpack.Digest(fixture.currentRootRaw),
		fixture.proposal.CandidateSHA256, fixture.contributions(t), fixture.now,
	)
}
