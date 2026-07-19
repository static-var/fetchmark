// Package tufrepository creates offline, publisher-owned TUF repository
// generations for Fetchmark open-index packs. It performs no network access.
package tufrepository

import (
	"crypto"
	"crypto/ed25519"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/sigstore/sigstore/pkg/signature"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/theupdateframework/go-tuf/v2/metadata"
)

const (
	LegacyIdentityVersion      = 1
	OperationalIdentityVersion = 2
	IdentityVersion            = OperationalIdentityVersion
	MaxIdentityBytes           = 512 << 10
)

var repositoryIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

type privateKeyDocument struct {
	KeyID             string `json:"key_id"`
	Ed25519PrivateKey string `json:"ed25519_private_key"`
}

type identityDocument struct {
	Version       int                  `json:"version"`
	RepositoryID  string               `json:"repository_id"`
	RootExpiresAt string               `json:"root_expires_at"`
	BootstrapRoot string               `json:"bootstrap_root"`
	RootUpdates   []string             `json:"root_updates,omitempty"`
	RootKeys      []privateKeyDocument `json:"root_keys,omitempty"`
	TargetsKey    privateKeyDocument   `json:"targets_key"`
	SnapshotKey   privateKeyDocument   `json:"snapshot_key"`
	TimestampKey  privateKeyDocument   `json:"timestamp_key"`
}

// Identity contains private offline TUF role keys. Root uses a two-of-two
// threshold; the online metadata roles each use one distinct key.
type Identity struct {
	version       int
	repositoryID  string
	rootExpires   time.Time
	bootstrapRoot []byte
	rootUpdates   [][]byte
	rootKeys      []ed25519.PrivateKey
	targetsKey    ed25519.PrivateKey
	snapshotKey   ed25519.PrivateKey
	timestampKey  ed25519.PrivateKey
}

func (identity Identity) RepositoryID() string { return identity.repositoryID }

func GenerateIdentity(repositoryID string, rootExpires time.Time, random io.Reader) ([]byte, []byte, error) {
	if !repositoryIDPattern.MatchString(repositoryID) || random == nil || !exactUTCFutureSecond(rootExpires, time.Time{}) {
		return nil, nil, errors.New("TUF repository: repository ID, exact UTC root expiry, and randomness are required")
	}
	keys := make([]ed25519.PrivateKey, 0, 5)
	for range 5 {
		_, privateKey, err := ed25519.GenerateKey(random)
		if err != nil {
			return nil, nil, errors.New("TUF repository: generate Ed25519 role key")
		}
		keys = append(keys, privateKey)
	}
	identity := Identity{
		version: LegacyIdentityVersion, repositoryID: repositoryID, rootExpires: rootExpires, rootKeys: keys[:2],
		targetsKey: keys[2], snapshotKey: keys[3], timestampKey: keys[4],
	}
	rootRaw, err := buildBootstrapRoot(identity)
	if err != nil {
		return nil, nil, err
	}
	document := identityDocument{
		Version: LegacyIdentityVersion, RepositoryID: repositoryID, RootExpiresAt: rootExpires.Format(time.RFC3339),
		BootstrapRoot: base64.StdEncoding.EncodeToString(rootRaw),
		RootKeys:      []privateKeyDocument{encodePrivateKey(keys[0]), encodePrivateKey(keys[1])},
		TargetsKey:    encodePrivateKey(keys[2]), SnapshotKey: encodePrivateKey(keys[3]), TimestampKey: encodePrivateKey(keys[4]),
	}
	raw, err := json.Marshal(document)
	if err != nil {
		return nil, nil, err
	}
	decoded, err := DecodeIdentity(raw)
	if err != nil {
		return nil, nil, err
	}
	preservedRoot, err := decoded.BootstrapRoot()
	if err != nil {
		return nil, nil, err
	}
	return raw, preservedRoot, nil
}

func DecodeIdentity(raw []byte) (Identity, error) {
	if len(raw) == 0 || len(raw) > MaxIdentityBytes {
		return Identity{}, fmt.Errorf("TUF repository: identity size must be 1..%d bytes", MaxIdentityBytes)
	}
	var document identityDocument
	if err := indexpack.DecodeStrictJSON(raw, &document); err != nil {
		return Identity{}, fmt.Errorf("TUF repository: invalid identity: %w", err)
	}
	rootExpires, err := time.Parse(time.RFC3339, document.RootExpiresAt)
	if err != nil || rootExpires.Location() != time.UTC || rootExpires.Nanosecond() != 0 || rootExpires.Format(time.RFC3339) != document.RootExpiresAt {
		return Identity{}, errors.New("TUF repository: root expiry must be an exact UTC RFC 3339 second")
	}
	if (document.Version != LegacyIdentityVersion && document.Version != OperationalIdentityVersion) ||
		!repositoryIDPattern.MatchString(document.RepositoryID) {
		return Identity{}, errors.New("TUF repository: invalid identity metadata")
	}
	if (document.Version == LegacyIdentityVersion && (len(document.RootKeys) != 2 || len(document.RootUpdates) != 0)) ||
		(document.Version == OperationalIdentityVersion && (len(document.RootKeys) != 0 || len(document.RootUpdates) == 0 || len(document.RootUpdates) > MaxRootRotations)) {
		return Identity{}, errors.New("TUF repository: invalid identity root custody metadata")
	}
	rootKeys := make([]ed25519.PrivateKey, 0, len(document.RootKeys))
	seen := make(map[string]struct{}, 5)
	for _, encoded := range document.RootKeys {
		key, err := decodePrivateKey(encoded, seen)
		if err != nil {
			return Identity{}, err
		}
		rootKeys = append(rootKeys, key)
	}
	targetsKey, err := decodePrivateKey(document.TargetsKey, seen)
	if err != nil {
		return Identity{}, err
	}
	snapshotKey, err := decodePrivateKey(document.SnapshotKey, seen)
	if err != nil {
		return Identity{}, err
	}
	timestampKey, err := decodePrivateKey(document.TimestampKey, seen)
	if err != nil {
		return Identity{}, err
	}
	bootstrapRoot, err := base64.StdEncoding.Strict().DecodeString(document.BootstrapRoot)
	if err != nil || len(bootstrapRoot) == 0 || base64.StdEncoding.EncodeToString(bootstrapRoot) != document.BootstrapRoot {
		return Identity{}, errors.New("TUF repository: bootstrap root must be canonical base64")
	}
	rootUpdates := make([][]byte, 0, len(document.RootUpdates))
	for _, encoded := range document.RootUpdates {
		update, err := base64.StdEncoding.Strict().DecodeString(encoded)
		if err != nil || len(update) == 0 || len(update) > 512<<10 || base64.StdEncoding.EncodeToString(update) != encoded {
			return Identity{}, errors.New("TUF repository: root update must be bounded canonical base64")
		}
		rootUpdates = append(rootUpdates, append([]byte(nil), update...))
	}
	identity := Identity{
		version: document.Version, repositoryID: document.RepositoryID, rootExpires: rootExpires, rootKeys: rootKeys,
		bootstrapRoot: append([]byte(nil), bootstrapRoot...),
		rootUpdates:   rootUpdates,
		targetsKey:    targetsKey, snapshotKey: snapshotKey, timestampKey: timestampKey,
	}
	if err := identity.validate(); err != nil {
		return Identity{}, err
	}
	return identity, nil
}

func (identity Identity) BootstrapRoot() ([]byte, error) {
	if err := identity.validate(); err != nil {
		return nil, err
	}
	return append([]byte(nil), identity.bootstrapRoot...), nil
}

// ActiveRoot returns the latest trusted root carried by the identity. The
// bootstrap bytes remain separately available for out-of-band consumer trust.
func (identity Identity) ActiveRoot() ([]byte, error) {
	if err := identity.validate(); err != nil {
		return nil, err
	}
	if len(identity.rootUpdates) == 0 {
		return append([]byte(nil), identity.bootstrapRoot...), nil
	}
	return append([]byte(nil), identity.rootUpdates[len(identity.rootUpdates)-1]...), nil
}

// RootChain returns the pinned bootstrap followed by every sequential signed
// update. Callers receive copies so the validated identity cannot be mutated.
func (identity Identity) RootChain() ([][]byte, error) {
	if err := identity.validate(); err != nil {
		return nil, err
	}
	chain := make([][]byte, 0, 1+len(identity.rootUpdates))
	chain = append(chain, append([]byte(nil), identity.bootstrapRoot...))
	for _, update := range identity.rootUpdates {
		chain = append(chain, append([]byte(nil), update...))
	}
	return chain, nil
}

// ApplyRootRotation appends one fully dual-threshold signed successor and
// returns a rootless operational identity. The returned document retains only
// public root metadata plus the three online role keys needed for staging.
// Existing identity bytes are never modified or overwritten by this function.
func ApplyRootRotation(
	identity Identity,
	signedRootRaw []byte,
	expectedCurrentRootSHA256, expectedSignedRootSHA256 string,
	now time.Time,
) ([]byte, error) {
	if err := identity.validate(); err != nil {
		return nil, err
	}
	if !validDigest(expectedCurrentRootSHA256) || !validDigest(expectedSignedRootSHA256) ||
		indexpack.Digest(signedRootRaw) != expectedSignedRootSHA256 || !exactRotationTime(now) {
		return nil, invalidRootRotation(errors.New("exact current and successor digests and migration time are required"))
	}
	currentRaw, err := identity.ActiveRoot()
	if err != nil {
		return nil, err
	}
	if indexpack.Digest(currentRaw) != expectedCurrentRootSHA256 {
		return nil, invalidRootRotation(errors.New("identity active root differs from the expected current root"))
	}
	current, err := decodeCurrentRoot(currentRaw)
	if err != nil {
		return nil, err
	}
	successor, err := validateSignedRootTransition(current, signedRootRaw, now)
	if err != nil {
		return nil, err
	}
	updates := make([][]byte, 0, len(identity.rootUpdates)+1)
	for _, update := range identity.rootUpdates {
		updates = append(updates, append([]byte(nil), update...))
	}
	updates = append(updates, append([]byte(nil), signedRootRaw...))
	operational := Identity{
		version: OperationalIdentityVersion, repositoryID: identity.repositoryID,
		rootExpires: successor.Signed.Expires, bootstrapRoot: append([]byte(nil), identity.bootstrapRoot...),
		rootUpdates: updates, targetsKey: append(ed25519.PrivateKey(nil), identity.targetsKey...),
		snapshotKey:  append(ed25519.PrivateKey(nil), identity.snapshotKey...),
		timestampKey: append(ed25519.PrivateKey(nil), identity.timestampKey...),
	}
	if err := operational.validate(); err != nil {
		return nil, err
	}
	document := identityDocument{
		Version: OperationalIdentityVersion, RepositoryID: operational.repositoryID,
		RootExpiresAt: operational.rootExpires.Format(time.RFC3339),
		BootstrapRoot: base64.StdEncoding.EncodeToString(operational.bootstrapRoot),
		TargetsKey:    encodePrivateKey(operational.targetsKey), SnapshotKey: encodePrivateKey(operational.snapshotKey),
		TimestampKey: encodePrivateKey(operational.timestampKey),
	}
	for _, update := range operational.rootUpdates {
		document.RootUpdates = append(document.RootUpdates, base64.StdEncoding.EncodeToString(update))
	}
	raw, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	if _, err := DecodeIdentity(raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func buildBootstrapRoot(identity Identity) ([]byte, error) {
	if err := identity.validateKeys(); err != nil {
		return nil, err
	}
	root := metadata.Root(identity.rootExpires)
	root.Signed.Version = 1
	root.Signed.ConsistentSnapshot = true
	for _, privateKey := range identity.rootKeys {
		if err := addRoleKey(root, privateKey, metadata.ROOT); err != nil {
			return nil, err
		}
	}
	root.Signed.Roles[metadata.ROOT].Threshold = 2
	for roleName, privateKey := range map[string]ed25519.PrivateKey{
		metadata.TARGETS: identity.targetsKey, metadata.SNAPSHOT: identity.snapshotKey, metadata.TIMESTAMP: identity.timestampKey,
	} {
		if err := addRoleKey(root, privateKey, roleName); err != nil {
			return nil, err
		}
	}
	for _, privateKey := range identity.rootKeys {
		if err := signMetadata(root, privateKey); err != nil {
			return nil, err
		}
	}
	if err := root.VerifyDelegate(metadata.ROOT, root); err != nil {
		return nil, fmt.Errorf("TUF repository: self-verify bootstrap root: %w", err)
	}
	return root.ToBytes(false)
}

func (identity Identity) validate() error {
	if err := identity.validateKeys(); err != nil {
		return err
	}
	if err := identity.validateBootstrapRoot(); err != nil {
		return err
	}
	active, err := validateRootChain(identity.bootstrapRoot, identity.rootUpdates, time.Time{})
	if err != nil {
		return err
	}
	if !active.Signed.Expires.Equal(identity.rootExpires) {
		return errors.New("TUF repository: active root expiry differs from the identity")
	}
	return nil
}

func (identity Identity) validateKeys() error {
	if !repositoryIDPattern.MatchString(identity.repositoryID) || !exactUTCFutureSecond(identity.rootExpires, time.Time{}) ||
		(identity.version != LegacyIdentityVersion && identity.version != OperationalIdentityVersion) ||
		len(identity.targetsKey) != ed25519.PrivateKeySize ||
		len(identity.snapshotKey) != ed25519.PrivateKeySize || len(identity.timestampKey) != ed25519.PrivateKeySize {
		return errors.New("TUF repository: valid repository identity is required")
	}
	if (identity.version == LegacyIdentityVersion && (len(identity.rootKeys) != 2 || len(identity.rootUpdates) != 0)) ||
		(identity.version == OperationalIdentityVersion && (len(identity.rootKeys) != 0 || len(identity.rootUpdates) == 0 || len(identity.rootUpdates) > MaxRootRotations)) {
		return errors.New("TUF repository: identity root custody does not match its version")
	}
	seen := make(map[string]struct{}, 5)
	for _, privateKey := range append(append([]ed25519.PrivateKey(nil), identity.rootKeys...), identity.targetsKey, identity.snapshotKey, identity.timestampKey) {
		keyID, err := tufKeyID(privateKey)
		if err != nil {
			return err
		}
		if _, duplicate := seen[keyID]; duplicate {
			return errors.New("TUF repository: role keys must be distinct")
		}
		seen[keyID] = struct{}{}
	}
	return nil
}

func (identity Identity) validateBootstrapRoot() error {
	if len(identity.bootstrapRoot) == 0 || len(identity.bootstrapRoot) > MaxIdentityBytes {
		return errors.New("TUF repository: bounded bootstrap root bytes are required")
	}
	root, err := metadata.Root().FromBytes(identity.bootstrapRoot)
	if err != nil {
		return fmt.Errorf("TUF repository: decode bootstrap root: %w", err)
	}
	if err := validateFixedRootProfile(root); err != nil {
		return err
	}
	if root.Signed.Version != 1 || len(root.Signatures) != 2 ||
		(identity.version == LegacyIdentityVersion && !root.Signed.Expires.Equal(identity.rootExpires)) {
		return errors.New("TUF repository: bootstrap root is outside the fixed repository profile")
	}
	expectedRoles := map[string]struct {
		keys      []ed25519.PrivateKey
		threshold int
	}{
		metadata.TARGETS:   {keys: []ed25519.PrivateKey{identity.targetsKey}, threshold: 1},
		metadata.SNAPSHOT:  {keys: []ed25519.PrivateKey{identity.snapshotKey}, threshold: 1},
		metadata.TIMESTAMP: {keys: []ed25519.PrivateKey{identity.timestampKey}, threshold: 1},
	}
	if identity.version == LegacyIdentityVersion {
		expectedRoles[metadata.ROOT] = struct {
			keys      []ed25519.PrivateKey
			threshold int
		}{keys: identity.rootKeys, threshold: 2}
	}
	for roleName, expected := range expectedRoles {
		role, found := root.Signed.Roles[roleName]
		if !found || role == nil || role.Threshold != expected.threshold || len(role.KeyIDs) != len(expected.keys) ||
			len(role.UnrecognizedFields) != 0 {
			return fmt.Errorf("TUF repository: bootstrap root role %s differs from the private identity", roleName)
		}
		seenRoleKeys := make(map[string]struct{}, len(expected.keys))
		for _, keyID := range role.KeyIDs {
			seenRoleKeys[keyID] = struct{}{}
		}
		for _, privateKey := range expected.keys {
			publicKey, _ := privateKey.Public().(ed25519.PublicKey)
			expectedKey, err := metadata.KeyFromPublicKey(publicKey)
			if err != nil {
				return err
			}
			keyID, err := expectedKey.ID()
			if err != nil {
				return err
			}
			actualKey, found := root.Signed.Keys[keyID]
			if !found || actualKey == nil || actualKey.Type != expectedKey.Type || actualKey.Scheme != expectedKey.Scheme ||
				actualKey.Value.PublicKey != expectedKey.Value.PublicKey || len(actualKey.UnrecognizedFields) != 0 ||
				len(actualKey.Value.UnrecognizedFields) != 0 {
				return errors.New("TUF repository: bootstrap root key differs from the private identity")
			}
			if _, found := seenRoleKeys[keyID]; !found {
				return fmt.Errorf("TUF repository: bootstrap root role %s omits its private key", roleName)
			}
		}
	}
	if err := root.VerifyDelegate(metadata.ROOT, root); err != nil {
		return fmt.Errorf("TUF repository: self-verify bootstrap root: %w", err)
	}
	return nil
}

func encodePrivateKey(privateKey ed25519.PrivateKey) privateKeyDocument {
	keyID, _ := tufKeyID(privateKey)
	return privateKeyDocument{KeyID: keyID, Ed25519PrivateKey: base64.StdEncoding.EncodeToString(privateKey)}
}

func decodePrivateKey(document privateKeyDocument, seen map[string]struct{}) (ed25519.PrivateKey, error) {
	decoded, err := base64.StdEncoding.Strict().DecodeString(document.Ed25519PrivateKey)
	if err != nil || len(decoded) != ed25519.PrivateKeySize || base64.StdEncoding.EncodeToString(decoded) != document.Ed25519PrivateKey {
		return nil, errors.New("TUF repository: private key must be canonical base64 for 64 bytes")
	}
	privateKey := ed25519.PrivateKey(append([]byte(nil), decoded...))
	derived := ed25519.NewKeyFromSeed(privateKey[:ed25519.SeedSize])
	if subtle.ConstantTimeCompare(privateKey, derived) != 1 {
		return nil, errors.New("TUF repository: private key seed and public key are inconsistent")
	}
	keyID, err := tufKeyID(privateKey)
	if err != nil || subtle.ConstantTimeCompare([]byte(keyID), []byte(document.KeyID)) != 1 {
		return nil, errors.New("TUF repository: private key ID is inconsistent")
	}
	if _, duplicate := seen[keyID]; duplicate {
		return nil, errors.New("TUF repository: role keys must be distinct")
	}
	seen[keyID] = struct{}{}
	return privateKey, nil
}

func tufKeyID(privateKey ed25519.PrivateKey) (string, error) {
	publicKey, ok := privateKey.Public().(ed25519.PublicKey)
	if !ok {
		return "", errors.New("TUF repository: derive Ed25519 public key")
	}
	key, err := metadata.KeyFromPublicKey(publicKey)
	if err != nil {
		return "", err
	}
	return key.ID()
}

func addRoleKey(root *metadata.Metadata[metadata.RootType], privateKey ed25519.PrivateKey, roleName string) error {
	publicKey, _ := privateKey.Public().(ed25519.PublicKey)
	key, err := metadata.KeyFromPublicKey(publicKey)
	if err != nil {
		return err
	}
	return root.Signed.AddKey(key, roleName)
}

func signMetadata[T metadata.Roles](document *metadata.Metadata[T], privateKey ed25519.PrivateKey) error {
	signer, err := signature.LoadSigner(privateKey, crypto.Hash(0))
	if err != nil {
		return err
	}
	_, err = document.Sign(signer)
	return err
}

func exactUTCFutureSecond(value, now time.Time) bool {
	if value.IsZero() || value.Location() != time.UTC || value.Nanosecond() != 0 {
		return false
	}
	return now.IsZero() || value.After(now)
}
