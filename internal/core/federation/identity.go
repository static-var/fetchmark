package federation

import (
	"crypto/ed25519"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
)

const (
	IdentityVersion  = 1
	MaxIdentityBytes = 8 << 10
)

var ErrInvalidIdentity = errors.New("federation: invalid identity")

// Identity is the local signing identity for one federation node. Key bytes
// remain private and accessors always return defensive copies.
type Identity struct {
	identityID string
	keyID      string
	publicKey  ed25519.PublicKey
	privateKey ed25519.PrivateKey
}

func (identity Identity) IdentityID() string { return identity.identityID }
func (identity Identity) KeyID() string      { return identity.keyID }
func (identity Identity) PublicKey() ed25519.PublicKey {
	return ed25519.PublicKey(cloneBytes(identity.publicKey))
}
func (identity Identity) PrivateKey() ed25519.PrivateKey {
	return ed25519.PrivateKey(cloneBytes(identity.privateKey))
}

type identityDocument struct {
	Version           int    `json:"version"`
	IdentityID        string `json:"identity_id"`
	KeyID             string `json:"key_id"`
	Ed25519PrivateKey string `json:"ed25519_private_key"`
}

// DecodeIdentity parses a bounded, operator-owned private identity document.
// It accepts only the exact v1 schema and verifies both halves of the Ed25519
// private key before deriving and matching its public key identifier.
func DecodeIdentity(raw []byte) (Identity, error) {
	if len(raw) == 0 || len(raw) > MaxIdentityBytes {
		return Identity{}, invalidIdentity(fmt.Errorf("size must be 1..%d bytes", MaxIdentityBytes))
	}
	var document identityDocument
	if err := decodeStrictJSON(raw, &document); err != nil {
		return Identity{}, invalidIdentity(err)
	}
	if document.Version != IdentityVersion {
		return Identity{}, invalidIdentity(fmt.Errorf("version must be %d", IdentityVersion))
	}
	if err := validateID("identity_id", document.IdentityID); err != nil {
		return Identity{}, invalidIdentity(err)
	}
	if err := validateDigest("key_id", document.KeyID); err != nil {
		return Identity{}, invalidIdentity(err)
	}
	privateKey, err := decodePrivateKey(document.Ed25519PrivateKey)
	if err != nil {
		return Identity{}, invalidIdentity(err)
	}
	derived := ed25519.NewKeyFromSeed(privateKey[:ed25519.SeedSize])
	if subtle.ConstantTimeCompare(privateKey, derived) != 1 {
		return Identity{}, invalidIdentity(errors.New("ed25519_private_key is internally inconsistent"))
	}
	publicKey := ed25519.PublicKey(derived[ed25519.SeedSize:])
	if subtle.ConstantTimeCompare([]byte(KeyID(publicKey)), []byte(document.KeyID)) != 1 {
		return Identity{}, invalidIdentity(errors.New("key_id does not match the derived public key"))
	}
	return Identity{
		identityID: document.IdentityID,
		keyID:      document.KeyID,
		publicKey:  ed25519.PublicKey(cloneBytes(publicKey)),
		privateKey: ed25519.PrivateKey(cloneBytes(derived)),
	}, nil
}

func decodePrivateKey(encoded string) (ed25519.PrivateKey, error) {
	decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(decoded) != ed25519.PrivateKeySize || base64.StdEncoding.EncodeToString(decoded) != encoded {
		return nil, errors.New("ed25519_private_key must be canonical padded base64 for 64 bytes")
	}
	return ed25519.PrivateKey(cloneBytes(decoded)), nil
}

func invalidIdentity(err error) error { return fmt.Errorf("%w: %v", ErrInvalidIdentity, err) }
