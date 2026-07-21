package tufrepository

import (
	"bytes"
	"crypto/ed25519"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/theupdateframework/go-tuf/v2/metadata"
)

const (
	RootSignerVersion              = 1
	RootContributionVersion        = 1
	RootContributionBundleVersion  = 1
	MaxRootSignerBytes             = 8 << 10
	MaxRootContributionBytes       = 4 << 10
	MaxRootContributionBundleBytes = 16 << 10
)

var (
	ErrInvalidRootSigner     = errors.New("TUF repository: invalid root signer")
	ErrInvalidRootRotation   = errors.New("TUF repository: invalid root rotation")
	ErrRootRotationThreshold = errors.New("TUF repository: root rotation threshold not met")
)

type rootSignerDocument struct {
	Version           int    `json:"version"`
	RepositoryID      string `json:"repository_id"`
	SignerID          string `json:"signer_id"`
	KeyID             string `json:"key_id"`
	Ed25519PrivateKey string `json:"ed25519_private_key"`
}

type RootSignerPublic struct {
	Version          int    `json:"version"`
	RepositoryID     string `json:"repository_id"`
	SignerID         string `json:"signer_id"`
	KeyID            string `json:"key_id"`
	Ed25519PublicKey string `json:"ed25519_public_key"`
}

type RootSigner struct {
	repositoryID string
	signerID     string
	keyID        string
	privateKey   ed25519.PrivateKey
}

type RootSignatureContribution struct {
	Version         int    `json:"version"`
	RepositoryID    string `json:"repository_id"`
	SignerID        string `json:"signer_id"`
	CandidateSHA256 string `json:"candidate_sha256"`
	KeyID           string `json:"key_id"`
	Signature       string `json:"signature"`
}

type RootContributionBundle struct {
	Version         int               `json:"version"`
	RepositoryID    string            `json:"repository_id"`
	CandidateSHA256 string            `json:"candidate_sha256"`
	Contributions   []json.RawMessage `json:"contributions"`
}

type RootRotationProposal struct {
	RepositoryID      string   `json:"repository_id"`
	FromVersion       int64    `json:"from_version"`
	ToVersion         int64    `json:"to_version"`
	CurrentRootSHA256 string   `json:"current_root_sha256"`
	CandidateSHA256   string   `json:"candidate_sha256"`
	ExpiresAt         string   `json:"expires_at"`
	OldRootKeyIDs     []string `json:"old_root_key_ids"`
	NewRootKeyIDs     []string `json:"new_root_key_ids"`
	OldThreshold      int      `json:"old_threshold"`
	NewThreshold      int      `json:"new_threshold"`
}

type RootRotationResult struct {
	RepositoryID      string   `json:"repository_id"`
	FromVersion       int64    `json:"from_version"`
	ToVersion         int64    `json:"to_version"`
	CurrentRootSHA256 string   `json:"current_root_sha256"`
	CandidateSHA256   string   `json:"candidate_sha256"`
	SignedRootSHA256  string   `json:"signed_root_sha256"`
	ExpiresAt         string   `json:"expires_at"`
	OldRootKeyIDs     []string `json:"old_root_key_ids"`
	NewRootKeyIDs     []string `json:"new_root_key_ids"`
	SignatureKeyIDs   []string `json:"signature_key_ids"`
	OldThreshold      int      `json:"old_threshold"`
	NewThreshold      int      `json:"new_threshold"`
}

func GenerateRootSigner(repositoryID, signerID string, random io.Reader) ([]byte, []byte, error) {
	if !repositoryIDPattern.MatchString(repositoryID) || !repositoryIDPattern.MatchString(signerID) || random == nil {
		return nil, nil, invalidRootSigner(errors.New("repository ID, signer ID, and randomness are required"))
	}
	publicKey, privateKey, err := ed25519.GenerateKey(random)
	if err != nil {
		return nil, nil, invalidRootSigner(errors.New("generate Ed25519 key"))
	}
	keyID, err := rootPublicKeyID(publicKey)
	if err != nil {
		return nil, nil, err
	}
	privateRaw, err := json.Marshal(rootSignerDocument{
		Version: RootSignerVersion, RepositoryID: repositoryID, SignerID: signerID, KeyID: keyID,
		Ed25519PrivateKey: base64.StdEncoding.EncodeToString(privateKey),
	})
	if err != nil {
		return nil, nil, err
	}
	publicRaw, err := json.Marshal(RootSignerPublic{
		Version: RootSignerVersion, RepositoryID: repositoryID, SignerID: signerID, KeyID: keyID,
		Ed25519PublicKey: base64.StdEncoding.EncodeToString(publicKey),
	})
	if err != nil {
		return nil, nil, err
	}
	if _, err := DecodeRootSigner(privateRaw); err != nil {
		return nil, nil, err
	}
	if _, err := DecodeRootSignerPublic(publicRaw); err != nil {
		return nil, nil, err
	}
	return privateRaw, publicRaw, nil
}

func DecodeRootSigner(raw []byte) (RootSigner, error) {
	if len(raw) == 0 || len(raw) > MaxRootSignerBytes {
		return RootSigner{}, invalidRootSigner(fmt.Errorf("private signer size must be 1..%d bytes", MaxRootSignerBytes))
	}
	var document rootSignerDocument
	if err := indexpack.DecodeStrictJSON(raw, &document); err != nil {
		return RootSigner{}, invalidRootSigner(err)
	}
	if document.Version != RootSignerVersion || !repositoryIDPattern.MatchString(document.RepositoryID) ||
		!repositoryIDPattern.MatchString(document.SignerID) {
		return RootSigner{}, invalidRootSigner(errors.New("invalid signer metadata"))
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(document.Ed25519PrivateKey)
	if err != nil || len(decoded) != ed25519.PrivateKeySize || base64.StdEncoding.EncodeToString(decoded) != document.Ed25519PrivateKey {
		return RootSigner{}, invalidRootSigner(errors.New("private key must be canonical base64 for 64 bytes"))
	}
	privateKey := ed25519.PrivateKey(append([]byte(nil), decoded...))
	derived := ed25519.NewKeyFromSeed(privateKey[:ed25519.SeedSize])
	if subtle.ConstantTimeCompare(privateKey, derived) != 1 {
		return RootSigner{}, invalidRootSigner(errors.New("private key seed and public key are inconsistent"))
	}
	publicKey, _ := privateKey.Public().(ed25519.PublicKey)
	keyID, err := rootPublicKeyID(publicKey)
	if err != nil || subtle.ConstantTimeCompare([]byte(keyID), []byte(document.KeyID)) != 1 {
		return RootSigner{}, invalidRootSigner(errors.New("private key ID is inconsistent"))
	}
	return RootSigner{repositoryID: document.RepositoryID, signerID: document.SignerID, keyID: keyID, privateKey: privateKey}, nil
}

func DecodeRootSignerPublic(raw []byte) (RootSignerPublic, error) {
	if len(raw) == 0 || len(raw) > MaxRootSignerBytes {
		return RootSignerPublic{}, invalidRootSigner(fmt.Errorf("public signer size must be 1..%d bytes", MaxRootSignerBytes))
	}
	var document RootSignerPublic
	if err := indexpack.DecodeStrictJSON(raw, &document); err != nil {
		return RootSignerPublic{}, invalidRootSigner(err)
	}
	if document.Version != RootSignerVersion || !repositoryIDPattern.MatchString(document.RepositoryID) ||
		!repositoryIDPattern.MatchString(document.SignerID) {
		return RootSignerPublic{}, invalidRootSigner(errors.New("invalid public signer metadata"))
	}
	publicKey, err := document.publicKey()
	if err != nil {
		return RootSignerPublic{}, err
	}
	keyID, err := rootPublicKeyID(publicKey)
	if err != nil || subtle.ConstantTimeCompare([]byte(keyID), []byte(document.KeyID)) != 1 {
		return RootSignerPublic{}, invalidRootSigner(errors.New("public key ID is inconsistent"))
	}
	return document, nil
}

func (signer RootSigner) Public() RootSignerPublic {
	publicKey, _ := signer.privateKey.Public().(ed25519.PublicKey)
	return RootSignerPublic{
		Version: RootSignerVersion, RepositoryID: signer.repositoryID, SignerID: signer.signerID,
		KeyID: signer.keyID, Ed25519PublicKey: base64.StdEncoding.EncodeToString(publicKey),
	}
}

func (document RootSignerPublic) publicKey() (ed25519.PublicKey, error) {
	decoded, err := base64.StdEncoding.Strict().DecodeString(document.Ed25519PublicKey)
	if err != nil || len(decoded) != ed25519.PublicKeySize || base64.StdEncoding.EncodeToString(decoded) != document.Ed25519PublicKey {
		return nil, invalidRootSigner(errors.New("public key must be canonical base64 for 32 bytes"))
	}
	return ed25519.PublicKey(append([]byte(nil), decoded...)), nil
}

func PrepareRootRotation(repositoryID string, currentRootRaw []byte, newSigners []RootSignerPublic, expiresAt, now time.Time) ([]byte, RootRotationProposal, error) {
	if !repositoryIDPattern.MatchString(repositoryID) || !exactRotationTime(now) ||
		!exactUTCFutureSecond(expiresAt, now) || expiresAt.Sub(now) < MinimumPublicationHorizon || len(newSigners) != 2 {
		return nil, RootRotationProposal{}, invalidRootRotation(errors.New("repository ID, two new signers, and exact future expiry are required"))
	}
	current, err := decodeCurrentRoot(currentRootRaw)
	if err != nil {
		return nil, RootRotationProposal{}, err
	}
	currentRootRole := current.Signed.Roles[metadata.ROOT]
	oldKeyIDs := sortedStrings(currentRootRole.KeyIDs)
	type validatedSigner struct {
		public RootSignerPublic
		key    *metadata.Key
	}
	validatedSigners := make([]validatedSigner, 0, 2)
	seen := make(map[string]struct{}, len(current.Signed.Keys)+2)
	for keyID := range current.Signed.Keys {
		seen[keyID] = struct{}{}
	}
	seenSigners := make(map[string]struct{}, 2)
	for _, signer := range newSigners {
		validatedRaw, err := json.Marshal(signer)
		if err != nil {
			return nil, RootRotationProposal{}, err
		}
		validated, err := DecodeRootSignerPublic(validatedRaw)
		if err != nil {
			return nil, RootRotationProposal{}, err
		}
		if validated.RepositoryID != repositoryID {
			return nil, RootRotationProposal{}, invalidRootRotation(errors.New("new signer repository ID differs"))
		}
		if _, duplicate := seenSigners[validated.SignerID]; duplicate {
			return nil, RootRotationProposal{}, invalidRootRotation(errors.New("new signer IDs must be distinct"))
		}
		seenSigners[validated.SignerID] = struct{}{}
		if _, duplicate := seen[validated.KeyID]; duplicate {
			return nil, RootRotationProposal{}, invalidRootRotation(errors.New("new root keys must be distinct from every current role key"))
		}
		seen[validated.KeyID] = struct{}{}
		publicKey, _ := validated.publicKey()
		key, err := metadata.KeyFromPublicKey(publicKey)
		if err != nil {
			return nil, RootRotationProposal{}, err
		}
		validatedSigners = append(validatedSigners, validatedSigner{public: validated, key: key})
	}
	sort.Slice(validatedSigners, func(left, right int) bool {
		return validatedSigners[left].public.KeyID < validatedSigners[right].public.KeyID
	})
	newKeyIDs := make([]string, 0, len(validatedSigners))

	candidate := metadata.Root(expiresAt)
	candidate.Signed.Version = current.Signed.Version + 1
	candidate.Signed.Type = current.Signed.Type
	candidate.Signed.SpecVersion = current.Signed.SpecVersion
	candidate.Signed.ConsistentSnapshot = current.Signed.ConsistentSnapshot
	for _, signer := range validatedSigners {
		if err := candidate.Signed.AddKey(signer.key, metadata.ROOT); err != nil {
			return nil, RootRotationProposal{}, err
		}
		newKeyIDs = append(newKeyIDs, signer.public.KeyID)
	}
	candidate.Signed.Roles[metadata.ROOT].Threshold = 2
	for _, roleName := range []string{metadata.TARGETS, metadata.SNAPSHOT, metadata.TIMESTAMP} {
		role := current.Signed.Roles[roleName]
		for _, keyID := range role.KeyIDs {
			key := current.Signed.Keys[keyID]
			if err := candidate.Signed.AddKey(key, roleName); err != nil {
				return nil, RootRotationProposal{}, err
			}
		}
		candidate.Signed.Roles[roleName].Threshold = role.Threshold
	}
	candidate.ClearSignatures()
	candidateRaw, err := candidate.ToBytes(false)
	if err != nil {
		return nil, RootRotationProposal{}, err
	}
	if _, err := validateUnsignedRootRotation(current, candidateRaw, now.UTC()); err != nil {
		return nil, RootRotationProposal{}, err
	}
	proposal := RootRotationProposal{
		RepositoryID: repositoryID, FromVersion: current.Signed.Version, ToVersion: candidate.Signed.Version,
		CurrentRootSHA256: indexpack.Digest(currentRootRaw), CandidateSHA256: indexpack.Digest(candidateRaw),
		ExpiresAt: expiresAt.Format(time.RFC3339), OldRootKeyIDs: oldKeyIDs, NewRootKeyIDs: sortedStrings(newKeyIDs),
		OldThreshold: currentRootRole.Threshold, NewThreshold: candidate.Signed.Roles[metadata.ROOT].Threshold,
	}
	return candidateRaw, proposal, nil
}

func SignRootRotation(
	repositoryID string,
	currentRootRaw, candidateRaw []byte,
	expectedCurrentRootSHA256, expectedCandidateSHA256 string,
	signer RootSigner,
	now time.Time,
) ([]byte, error) {
	if !repositoryIDPattern.MatchString(repositoryID) || len(signer.privateKey) != ed25519.PrivateKeySize ||
		!repositoryIDPattern.MatchString(signer.repositoryID) || !repositoryIDPattern.MatchString(signer.signerID) ||
		signer.repositoryID != repositoryID || !validDigest(expectedCurrentRootSHA256) ||
		!validDigest(expectedCandidateSHA256) || indexpack.Digest(currentRootRaw) != expectedCurrentRootSHA256 ||
		indexpack.Digest(candidateRaw) != expectedCandidateSHA256 || !exactRotationTime(now) {
		return nil, invalidRootSigner(errors.New("valid repository-bound signer, exact roots, digests, and signing time are required"))
	}
	current, err := decodeCurrentRoot(currentRootRaw)
	if err != nil {
		return nil, err
	}
	candidate, err := validateUnsignedRootRotation(current, candidateRaw, now)
	if err != nil {
		return nil, err
	}
	if candidate.Signed.Expires.Sub(now) < MinimumPublicationHorizon {
		return nil, invalidRootRotation(errors.New("candidate root must remain fresh through the minimum rotation horizon"))
	}
	inOld := roleContainsKey(current.Signed.Roles[metadata.ROOT], signer.keyID)
	inNew := roleContainsKey(candidate.Signed.Roles[metadata.ROOT], signer.keyID)
	if inOld == inNew {
		return nil, invalidRootSigner(errors.New("signer key must be authorized by exactly one side of the root rotation"))
	}
	if err := signMetadata(candidate, signer.privateKey); err != nil {
		return nil, err
	}
	if len(candidate.Signatures) != 1 || candidate.Signatures[0].KeyID != signer.keyID {
		return nil, invalidRootRotation(errors.New("signer produced unexpected root signature"))
	}
	contribution := RootSignatureContribution{
		Version: RootContributionVersion, RepositoryID: signer.repositoryID, SignerID: signer.signerID,
		CandidateSHA256: indexpack.Digest(candidateRaw), KeyID: signer.keyID,
		Signature: hex.EncodeToString(candidate.Signatures[0].Signature),
	}
	return json.Marshal(contribution)
}

func SignRootRotationWithLegacyIdentity(
	currentRootRaw, candidateRaw []byte,
	expectedCurrentRootSHA256, expectedCandidateSHA256 string,
	identity Identity,
	now time.Time,
) ([][]byte, error) {
	if err := identity.validate(); err != nil {
		return nil, err
	}
	if identity.version != LegacyIdentityVersion {
		return nil, invalidRootRotation(errors.New("legacy root signing requires a version-1 identity with embedded root keys"))
	}
	bootstrapRoot, err := identity.BootstrapRoot()
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(currentRootRaw, bootstrapRoot) || indexpack.Digest(currentRootRaw) != expectedCurrentRootSHA256 {
		return nil, invalidRootRotation(errors.New("legacy identity is not bound to the exact current root"))
	}
	contributions := make([][]byte, 0, len(identity.rootKeys))
	for index, privateKey := range identity.rootKeys {
		keyID, err := tufKeyID(privateKey)
		if err != nil {
			return nil, err
		}
		signer := RootSigner{
			repositoryID: identity.repositoryID, signerID: fmt.Sprintf("legacy-root-%d", index+1),
			keyID: keyID, privateKey: append(ed25519.PrivateKey(nil), privateKey...),
		}
		raw, err := SignRootRotation(
			identity.repositoryID, currentRootRaw, candidateRaw, expectedCurrentRootSHA256,
			expectedCandidateSHA256, signer, now,
		)
		if err != nil {
			return nil, err
		}
		contributions = append(contributions, raw)
	}
	return contributions, nil
}

func EncodeRootContributionBundle(contributionRaw [][]byte) ([]byte, error) {
	if len(contributionRaw) != 2 {
		return nil, invalidRootRotation(errors.New("legacy contribution bundle requires exactly two contributions"))
	}
	bundle := RootContributionBundle{Version: RootContributionBundleVersion}
	seen := make(map[string]struct{}, 2)
	for _, raw := range contributionRaw {
		contribution, err := decodeRootContribution(raw)
		if err != nil {
			return nil, err
		}
		if bundle.RepositoryID == "" {
			bundle.RepositoryID = contribution.RepositoryID
			bundle.CandidateSHA256 = contribution.CandidateSHA256
		}
		if contribution.RepositoryID != bundle.RepositoryID || contribution.CandidateSHA256 != bundle.CandidateSHA256 {
			return nil, invalidRootRotation(errors.New("legacy contributions have different repository or candidate bindings"))
		}
		if _, duplicate := seen[contribution.KeyID]; duplicate {
			return nil, invalidRootRotation(errors.New("legacy contribution keys must be unique"))
		}
		seen[contribution.KeyID] = struct{}{}
		bundle.Contributions = append(bundle.Contributions, append(json.RawMessage(nil), raw...))
	}
	sort.Slice(bundle.Contributions, func(left, right int) bool {
		var leftContribution, rightContribution RootSignatureContribution
		_ = json.Unmarshal(bundle.Contributions[left], &leftContribution)
		_ = json.Unmarshal(bundle.Contributions[right], &rightContribution)
		return leftContribution.KeyID < rightContribution.KeyID
	})
	return json.Marshal(bundle)
}

func DecodeRootContributionBundle(raw []byte) ([][]byte, error) {
	if len(raw) == 0 || len(raw) > MaxRootContributionBundleBytes {
		return nil, invalidRootRotation(fmt.Errorf("contribution bundle size must be 1..%d bytes", MaxRootContributionBundleBytes))
	}
	var bundle RootContributionBundle
	if err := indexpack.DecodeStrictJSON(raw, &bundle); err != nil {
		return nil, invalidRootRotation(err)
	}
	if bundle.Version != RootContributionBundleVersion || !repositoryIDPattern.MatchString(bundle.RepositoryID) ||
		!validDigest(bundle.CandidateSHA256) || len(bundle.Contributions) != 2 {
		return nil, invalidRootRotation(errors.New("invalid legacy contribution bundle"))
	}
	result := make([][]byte, 0, 2)
	seen := make(map[string]struct{}, 2)
	for _, rawContribution := range bundle.Contributions {
		contribution, err := decodeRootContribution(rawContribution)
		if err != nil {
			return nil, err
		}
		if contribution.RepositoryID != bundle.RepositoryID || contribution.CandidateSHA256 != bundle.CandidateSHA256 {
			return nil, invalidRootRotation(errors.New("bundled contribution binding differs"))
		}
		if _, duplicate := seen[contribution.KeyID]; duplicate {
			return nil, invalidRootRotation(errors.New("bundled contribution keys must be unique"))
		}
		seen[contribution.KeyID] = struct{}{}
		result = append(result, append([]byte(nil), rawContribution...))
	}
	return result, nil
}

func AssembleRootRotation(repositoryID string, currentRootRaw, candidateRaw []byte, expectedCurrentRootSHA256, expectedCandidateSHA256 string, contributionRaw [][]byte, now time.Time) ([]byte, RootRotationResult, error) {
	if !repositoryIDPattern.MatchString(repositoryID) || !validDigest(expectedCurrentRootSHA256) || !validDigest(expectedCandidateSHA256) ||
		indexpack.Digest(currentRootRaw) != expectedCurrentRootSHA256 || indexpack.Digest(candidateRaw) != expectedCandidateSHA256 ||
		!exactRotationTime(now) {
		return nil, RootRotationResult{}, invalidRootRotation(errors.New("repository ID, exact roots, digests, and assembly time are required"))
	}
	current, err := decodeCurrentRoot(currentRootRaw)
	if err != nil {
		return nil, RootRotationResult{}, err
	}
	candidate, err := validateUnsignedRootRotation(current, candidateRaw, now.UTC())
	if err != nil {
		return nil, RootRotationResult{}, err
	}
	if candidate.Signed.Expires.Sub(now) < MinimumPublicationHorizon {
		return nil, RootRotationResult{}, invalidRootRotation(errors.New("candidate root must remain fresh through the minimum rotation horizon"))
	}
	if len(contributionRaw) != 4 {
		return nil, RootRotationResult{}, fmt.Errorf("%w: exactly four disjoint old/new contributions are required", ErrRootRotationThreshold)
	}
	allowed := make(map[string]struct{}, 4)
	for _, role := range []*metadata.Role{current.Signed.Roles[metadata.ROOT], candidate.Signed.Roles[metadata.ROOT]} {
		for _, keyID := range role.KeyIDs {
			allowed[keyID] = struct{}{}
		}
	}
	seenKeys := make(map[string]struct{}, 4)
	contributions := make([]RootSignatureContribution, 0, 4)
	for _, raw := range contributionRaw {
		contribution, err := decodeRootContribution(raw)
		if err != nil {
			return nil, RootRotationResult{}, err
		}
		if contribution.RepositoryID != repositoryID || contribution.CandidateSHA256 != indexpack.Digest(candidateRaw) {
			return nil, RootRotationResult{}, invalidRootRotation(errors.New("contribution repository or candidate digest differs"))
		}
		if _, found := allowed[contribution.KeyID]; !found {
			return nil, RootRotationResult{}, invalidRootRotation(errors.New("contribution key is not authorized by old or new root"))
		}
		if _, duplicate := seenKeys[contribution.KeyID]; duplicate {
			return nil, RootRotationResult{}, invalidRootRotation(errors.New("contribution key IDs must be unique"))
		}
		seenKeys[contribution.KeyID] = struct{}{}
		contributions = append(contributions, contribution)
	}
	sort.Slice(contributions, func(left, right int) bool { return contributions[left].KeyID < contributions[right].KeyID })
	for _, contribution := range contributions {
		signature, _ := hex.DecodeString(contribution.Signature)
		candidate.Signatures = append(candidate.Signatures, metadata.Signature{KeyID: contribution.KeyID, Signature: metadata.HexBytes(signature)})
	}
	if err := current.VerifyDelegate(metadata.ROOT, candidate); err != nil {
		return nil, RootRotationResult{}, fmt.Errorf("%w: old root threshold: %v", ErrRootRotationThreshold, err)
	}
	if err := candidate.VerifyDelegate(metadata.ROOT, candidate); err != nil {
		return nil, RootRotationResult{}, fmt.Errorf("%w: new root threshold: %v", ErrRootRotationThreshold, err)
	}
	signedRaw, err := candidate.ToBytes(false)
	if err != nil {
		return nil, RootRotationResult{}, err
	}
	oldRole, newRole := current.Signed.Roles[metadata.ROOT], candidate.Signed.Roles[metadata.ROOT]
	result := RootRotationResult{
		RepositoryID: repositoryID, FromVersion: current.Signed.Version, ToVersion: candidate.Signed.Version,
		CurrentRootSHA256: indexpack.Digest(currentRootRaw), CandidateSHA256: indexpack.Digest(candidateRaw),
		SignedRootSHA256: indexpack.Digest(signedRaw), ExpiresAt: candidate.Signed.Expires.Format(time.RFC3339),
		OldRootKeyIDs: sortedStrings(oldRole.KeyIDs), NewRootKeyIDs: sortedStrings(newRole.KeyIDs),
		SignatureKeyIDs: sortedStrings(mapKeys(seenKeys)), OldThreshold: oldRole.Threshold, NewThreshold: newRole.Threshold,
	}
	return signedRaw, result, nil
}

func decodeCurrentRoot(raw []byte) (*metadata.Metadata[metadata.RootType], error) {
	if len(raw) == 0 || len(raw) > 512<<10 {
		return nil, invalidRootRotation(errors.New("current root must be bounded"))
	}
	root, err := metadata.Root().FromBytes(raw)
	if err != nil {
		return nil, invalidRootRotation(err)
	}
	if err := validateFixedRootProfile(root); err != nil {
		return nil, err
	}
	if err := root.VerifyDelegate(metadata.ROOT, root); err != nil {
		return nil, invalidRootRotation(fmt.Errorf("current root self-verification: %w", err))
	}
	return root, nil
}

func validateUnsignedRootRotation(current *metadata.Metadata[metadata.RootType], raw []byte, now time.Time) (*metadata.Metadata[metadata.RootType], error) {
	if len(raw) == 0 || len(raw) > 512<<10 {
		return nil, invalidRootRotation(errors.New("unsigned candidate root must be bounded"))
	}
	candidate, err := metadata.Root().FromBytes(raw)
	if err != nil {
		return nil, invalidRootRotation(err)
	}
	if err := validateRootOnlyTransition(current, candidate); err != nil {
		return nil, err
	}
	if len(candidate.Signatures) != 0 ||
		candidate.Signed.Expires.Location() != time.UTC || candidate.Signed.Expires.Nanosecond() != 0 || !candidate.Signed.Expires.After(now) ||
		candidate.Signed.Type != current.Signed.Type || candidate.Signed.SpecVersion != current.Signed.SpecVersion {
		return nil, invalidRootRotation(errors.New("candidate must be unsigned, sequential, future, and preserve the repository profile"))
	}
	return candidate, nil
}

func validateFixedRootProfile(root *metadata.Metadata[metadata.RootType]) error {
	if root == nil || root.Signed.Type != metadata.ROOT || !supportedTUFSpecVersion(root.Signed.SpecVersion) ||
		root.Signed.Version < 1 || !root.Signed.ConsistentSnapshot || len(root.Signed.Keys) != 5 ||
		len(root.Signed.Roles) != 4 || len(root.Signed.UnrecognizedFields) != 0 || len(root.UnrecognizedFields) != 0 {
		return invalidRootRotation(errors.New("root is outside the fixed repository profile"))
	}
	expectedRoles := map[string]struct {
		threshold int
		keyCount  int
	}{
		metadata.ROOT:      {threshold: 2, keyCount: 2},
		metadata.TARGETS:   {threshold: 1, keyCount: 1},
		metadata.SNAPSHOT:  {threshold: 1, keyCount: 1},
		metadata.TIMESTAMP: {threshold: 1, keyCount: 1},
	}
	referenced := make(map[string]struct{}, len(root.Signed.Keys))
	for roleName, expected := range expectedRoles {
		role, found := root.Signed.Roles[roleName]
		if !found || role == nil || role.Threshold != expected.threshold || len(role.KeyIDs) != expected.keyCount ||
			len(role.UnrecognizedFields) != 0 {
			return invalidRootRotation(fmt.Errorf("root role %s is outside the fixed repository profile", roleName))
		}
		for _, keyID := range role.KeyIDs {
			if _, duplicate := referenced[keyID]; duplicate {
				return invalidRootRotation(errors.New("every root role binding must use a distinct key"))
			}
			key, found := root.Signed.Keys[keyID]
			if !found || key == nil || key.Type != metadata.KeyTypeEd25519 || key.Scheme != metadata.KeySchemeEd25519 ||
				len(key.UnrecognizedFields) != 0 || len(key.Value.UnrecognizedFields) != 0 {
				return invalidRootRotation(errors.New("every root role binding must resolve to a strict Ed25519 key"))
			}
			publicKey, err := hex.DecodeString(key.Value.PublicKey)
			if err != nil || len(publicKey) != ed25519.PublicKeySize || hex.EncodeToString(publicKey) != key.Value.PublicKey {
				return invalidRootRotation(errors.New("root public keys must be canonical Ed25519 hex"))
			}
			derivedKeyID, err := key.ID()
			if err != nil || derivedKeyID != keyID {
				return invalidRootRotation(errors.New("root key map ID differs from its key bytes"))
			}
			referenced[keyID] = struct{}{}
		}
	}
	if len(referenced) != len(root.Signed.Keys) {
		return invalidRootRotation(errors.New("root contains an unreferenced key"))
	}
	for _, signature := range root.Signatures {
		if !validDigest(signature.KeyID) || len(signature.Signature) != ed25519.SignatureSize || len(signature.UnrecognizedFields) != 0 {
			return invalidRootRotation(errors.New("root contains a malformed signature"))
		}
	}
	return nil
}

func supportedTUFSpecVersion(value string) bool {
	parts := strings.Split(value, ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if part == "" || (len(part) > 1 && part[0] == '0') {
			return false
		}
		if _, err := strconv.ParseUint(part, 10, 31); err != nil {
			return false
		}
	}
	return parts[0] == "1"
}

func roleContainsKey(role *metadata.Role, keyID string) bool {
	if role == nil {
		return false
	}
	for _, candidate := range role.KeyIDs {
		if candidate == keyID {
			return true
		}
	}
	return false
}

func exactRotationTime(now time.Time) bool {
	return !now.IsZero() && now.Location() == time.UTC && now.Nanosecond() == 0
}

func decodeRootContribution(raw []byte) (RootSignatureContribution, error) {
	if len(raw) == 0 || len(raw) > MaxRootContributionBytes {
		return RootSignatureContribution{}, invalidRootRotation(fmt.Errorf("contribution size must be 1..%d bytes", MaxRootContributionBytes))
	}
	var contribution RootSignatureContribution
	if err := indexpack.DecodeStrictJSON(raw, &contribution); err != nil {
		return RootSignatureContribution{}, invalidRootRotation(err)
	}
	signature, err := hex.DecodeString(contribution.Signature)
	if contribution.Version != RootContributionVersion || !repositoryIDPattern.MatchString(contribution.RepositoryID) ||
		!repositoryIDPattern.MatchString(contribution.SignerID) || !validDigest(contribution.CandidateSHA256) ||
		!validDigest(contribution.KeyID) || err != nil || len(signature) != ed25519.SignatureSize ||
		hex.EncodeToString(signature) != contribution.Signature {
		return RootSignatureContribution{}, invalidRootRotation(errors.New("invalid root signature contribution"))
	}
	return contribution, nil
}

func rootPublicKeyID(publicKey ed25519.PublicKey) (string, error) {
	key, err := metadata.KeyFromPublicKey(publicKey)
	if err != nil {
		return "", err
	}
	return key.ID()
}

func equalMetadataKey(left, right *metadata.Key) bool {
	return left != nil && right != nil && left.Type == right.Type && left.Scheme == right.Scheme &&
		left.Value.PublicKey == right.Value.PublicKey && len(left.UnrecognizedFields) == 0 && len(right.UnrecognizedFields) == 0 &&
		len(left.Value.UnrecognizedFields) == 0 && len(right.Value.UnrecognizedFields) == 0
}

func sortedStrings(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func mapKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	return result
}

func invalidRootSigner(cause error) error {
	return fmt.Errorf("%w: %v", ErrInvalidRootSigner, cause)
}

func invalidRootRotation(cause error) error {
	return fmt.Errorf("%w: %v", ErrInvalidRootRotation, cause)
}
