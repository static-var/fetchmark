package tufrepository

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

func TestApplyRootRotationProducesRootlessOperationalIdentity(t *testing.T) {
	fixture := newRootRotationFixture(t)
	signedRootRaw, _, err := fixture.assemble(t)
	if err != nil {
		t.Fatal(err)
	}
	operationalRaw, err := ApplyRootRotation(
		fixture.identity, signedRootRaw, indexpack.Digest(fixture.currentRootRaw), indexpack.Digest(signedRootRaw), fixture.now,
	)
	if err != nil {
		t.Fatalf("ApplyRootRotation: %v", err)
	}
	for _, privateKey := range fixture.identity.rootKeys {
		if bytes.Contains(operationalRaw, []byte(base64.StdEncoding.EncodeToString(privateKey))) {
			t.Fatal("operational identity retained a legacy root private key")
		}
	}
	if bytes.Contains(operationalRaw, []byte(`"root_keys"`)) {
		t.Fatal("operational identity serialized a root_keys field")
	}
	operational, err := DecodeIdentity(operationalRaw)
	if err != nil {
		t.Fatal(err)
	}
	if operational.version != OperationalIdentityVersion || len(operational.rootKeys) != 0 || len(operational.rootUpdates) != 1 {
		t.Fatalf("operational identity = version %d, root keys %d, updates %d", operational.version, len(operational.rootKeys), len(operational.rootUpdates))
	}
	active, err := operational.ActiveRoot()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(active, signedRootRaw) {
		t.Fatal("operational identity active root differs from the assembled successor")
	}
	bootstrap, err := operational.BootstrapRoot()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bootstrap, fixture.currentRootRaw) {
		t.Fatal("operational identity changed the out-of-band bootstrap root")
	}
	if _, err := SignRootRotationWithLegacyIdentity(
		active, fixture.candidateRaw, indexpack.Digest(active), fixture.proposal.CandidateSHA256, operational, fixture.now,
	); !errors.Is(err, ErrInvalidRootRotation) || !strings.Contains(err.Error(), "version-1 identity") {
		t.Fatalf("operational identity exposed legacy signing: %v", err)
	}
}

func TestApplyRootRotationAppendsSequentialPublicChain(t *testing.T) {
	fixture := newRootRotationFixture(t)
	firstSigned, _, err := fixture.assemble(t)
	if err != nil {
		t.Fatal(err)
	}
	firstRaw, err := ApplyRootRotation(
		fixture.identity, firstSigned, indexpack.Digest(fixture.currentRootRaw), indexpack.Digest(firstSigned), fixture.now,
	)
	if err != nil {
		t.Fatal(err)
	}
	first, err := DecodeIdentity(firstRaw)
	if err != nil {
		t.Fatal(err)
	}
	secondPrivate, secondPublic := generateRootSignersWithFill(t, fixture.repositoryID, 41)
	secondCandidate, secondProposal, err := PrepareRootRotation(
		fixture.repositoryID, firstSigned, secondPublic, fixture.now.Add(3*365*24*time.Hour), fixture.now,
	)
	if err != nil {
		t.Fatal(err)
	}
	contributions := signWithRootSignerDocuments(
		t, fixture.repositoryID, firstSigned, secondCandidate, secondProposal.CandidateSHA256,
		append(append([][]byte(nil), fixture.newPrivate...), secondPrivate...), fixture.now,
	)
	secondSigned, _, err := AssembleRootRotation(
		fixture.repositoryID, firstSigned, secondCandidate, indexpack.Digest(firstSigned),
		secondProposal.CandidateSHA256, contributions, fixture.now,
	)
	if err != nil {
		t.Fatal(err)
	}
	secondRaw, err := ApplyRootRotation(
		first, secondSigned, indexpack.Digest(firstSigned), indexpack.Digest(secondSigned), fixture.now,
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := DecodeIdentity(secondRaw)
	if err != nil {
		t.Fatal(err)
	}
	chain, err := second.RootChain()
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 3 || !bytes.Equal(chain[0], fixture.currentRootRaw) ||
		!bytes.Equal(chain[1], firstSigned) || !bytes.Equal(chain[2], secondSigned) {
		t.Fatal("operational identity did not preserve the exact sequential root chain")
	}
}

func TestDecodeOperationalIdentityRejectsMissingAndMisorderedRootChain(t *testing.T) {
	fixture := newRootRotationFixture(t)
	signedRootRaw, _, err := fixture.assemble(t)
	if err != nil {
		t.Fatal(err)
	}
	operationalRaw, err := ApplyRootRotation(
		fixture.identity, signedRootRaw, indexpack.Digest(fixture.currentRootRaw), indexpack.Digest(signedRootRaw), fixture.now,
	)
	if err != nil {
		t.Fatal(err)
	}
	var document identityDocument
	if err := json.Unmarshal(operationalRaw, &document); err != nil {
		t.Fatal(err)
	}
	document.RootUpdates = nil
	missing, _ := json.Marshal(document)
	if _, err := DecodeIdentity(missing); err == nil {
		t.Fatal("operational identity accepted an empty root chain")
	}
	document.RootUpdates = []string{
		base64.StdEncoding.EncodeToString(signedRootRaw),
		base64.StdEncoding.EncodeToString(signedRootRaw),
	}
	misordered, _ := json.Marshal(document)
	if _, err := DecodeIdentity(misordered); !errors.Is(err, ErrInvalidRootRotation) {
		t.Fatalf("operational identity accepted a non-sequential root chain: %v", err)
	}
}

func TestRootRotationAllowsExpiredCurrentRootWithFreshSuccessor(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	identityRaw, expiredRootRaw, err := GenerateIdentity(
		"fetchmark-community", now.Add(-time.Hour), deterministicIdentityRandom(),
	)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(identityRaw)
	if err != nil {
		t.Fatal(err)
	}
	newPrivate, newPublic := generateRootSigners(t, identity.repositoryID)
	candidateRaw, proposal, err := PrepareRootRotation(
		identity.repositoryID, expiredRootRaw, newPublic, now.Add(365*24*time.Hour), now,
	)
	if err != nil {
		t.Fatalf("PrepareRootRotation with expired current root: %v", err)
	}
	legacy, err := SignRootRotationWithLegacyIdentity(
		expiredRootRaw, candidateRaw, indexpack.Digest(expiredRootRaw), proposal.CandidateSHA256, identity, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	contributions := append(legacy, signWithRootSignerDocuments(
		t, identity.repositoryID, expiredRootRaw, candidateRaw, proposal.CandidateSHA256, newPrivate, now,
	)...)
	if _, _, err := AssembleRootRotation(
		identity.repositoryID, expiredRootRaw, candidateRaw, indexpack.Digest(expiredRootRaw),
		proposal.CandidateSHA256, contributions, now,
	); err != nil {
		t.Fatalf("AssembleRootRotation with expired current root: %v", err)
	}
}

func generateRootSignersWithFill(t *testing.T, repositoryID string, firstFill byte) ([][]byte, []RootSignerPublic) {
	t.Helper()
	privateDocuments := make([][]byte, 0, 2)
	publicDocuments := make([]RootSignerPublic, 0, 2)
	for index := range 2 {
		privateRaw, publicRaw, err := GenerateRootSigner(
			repositoryID, string(rune('c'+index))+"-root-custodian",
			bytes.NewReader(bytes.Repeat([]byte{firstFill + byte(index)}, 64)),
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

func signWithRootSignerDocuments(
	t *testing.T,
	repositoryID string,
	currentRootRaw, candidateRaw []byte,
	expectedCandidateSHA256 string,
	privateDocuments [][]byte,
	now time.Time,
) [][]byte {
	t.Helper()
	contributions := make([][]byte, 0, len(privateDocuments))
	for _, raw := range privateDocuments {
		signer, err := DecodeRootSigner(raw)
		if err != nil {
			t.Fatal(err)
		}
		contribution, err := SignRootRotation(
			repositoryID, currentRootRaw, candidateRaw, indexpack.Digest(currentRootRaw),
			expectedCandidateSHA256, signer, now,
		)
		if err != nil {
			t.Fatal(err)
		}
		contributions = append(contributions, contribution)
	}
	return contributions
}

func rotateIdentityForTest(t *testing.T, identity Identity, now time.Time) (Identity, []byte) {
	t.Helper()
	currentRootRaw, err := identity.ActiveRoot()
	if err != nil {
		t.Fatal(err)
	}
	newPrivate, newPublic := generateRootSignersWithFill(t, identity.repositoryID, 51)
	candidateRaw, proposal, err := PrepareRootRotation(
		identity.repositoryID, currentRootRaw, newPublic, now.Add(2*365*24*time.Hour), now,
	)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := SignRootRotationWithLegacyIdentity(
		currentRootRaw, candidateRaw, indexpack.Digest(currentRootRaw), proposal.CandidateSHA256, identity, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	contributions := append(legacy, signWithRootSignerDocuments(
		t, identity.repositoryID, currentRootRaw, candidateRaw, proposal.CandidateSHA256, newPrivate, now,
	)...)
	signedRootRaw, _, err := AssembleRootRotation(
		identity.repositoryID, currentRootRaw, candidateRaw, indexpack.Digest(currentRootRaw),
		proposal.CandidateSHA256, contributions, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	operationalRaw, err := ApplyRootRotation(
		identity, signedRootRaw, indexpack.Digest(currentRootRaw), indexpack.Digest(signedRootRaw), now,
	)
	if err != nil {
		t.Fatal(err)
	}
	operational, err := DecodeIdentity(operationalRaw)
	if err != nil {
		t.Fatal(err)
	}
	return operational, signedRootRaw
}
