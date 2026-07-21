package openpackbuilder

import (
	"crypto/ed25519"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

const (
	IdentityVersion  = 1
	MaxIdentityBytes = 4 << 10
)

var publisherIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

type identityDocument struct {
	Version           int    `json:"version"`
	PublisherID       string `json:"publisher_id"`
	KeyID             string `json:"key_id"`
	Ed25519PrivateKey string `json:"ed25519_private_key"`
}

type Identity struct {
	publisherID string
	keyID       string
	privateKey  ed25519.PrivateKey
}

type PublicIdentity struct {
	PublisherID      string `json:"publisher_id"`
	KeyID            string `json:"key_id"`
	Ed25519PublicKey string `json:"ed25519_public_key"`
}

func DecodePublicIdentity(raw []byte) (PublicIdentity, ed25519.PublicKey, error) {
	if len(raw) == 0 || len(raw) > MaxIdentityBytes {
		return PublicIdentity{}, nil, fmt.Errorf("open pack builder: public identity size must be 1..%d bytes", MaxIdentityBytes)
	}
	var identity PublicIdentity
	if err := indexpack.DecodeStrictJSON(raw, &identity); err != nil {
		return PublicIdentity{}, nil, fmt.Errorf("open pack builder: invalid public identity: %w", err)
	}
	if !publisherIDPattern.MatchString(identity.PublisherID) || len(identity.KeyID) != 64 || strings.ToLower(identity.KeyID) != identity.KeyID {
		return PublicIdentity{}, nil, errors.New("open pack builder: invalid public identity metadata")
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(identity.Ed25519PublicKey)
	if err != nil || len(decoded) != ed25519.PublicKeySize || base64.StdEncoding.EncodeToString(decoded) != identity.Ed25519PublicKey {
		return PublicIdentity{}, nil, errors.New("open pack builder: public key must be canonical base64 for 32 bytes")
	}
	publicKey := ed25519.PublicKey(append([]byte(nil), decoded...))
	if subtle.ConstantTimeCompare([]byte(indexpack.KeyID(publicKey)), []byte(identity.KeyID)) != 1 {
		return PublicIdentity{}, nil, errors.New("open pack builder: public identity key metadata is inconsistent")
	}
	return identity, publicKey, nil
}

func (identity Identity) PublisherID() string { return identity.publisherID }
func (identity Identity) KeyID() string       { return identity.keyID }
func (identity Identity) PublicKey() ed25519.PublicKey {
	public, _ := identity.privateKey.Public().(ed25519.PublicKey)
	return append(ed25519.PublicKey(nil), public...)
}

func GenerateIdentity(publisherID string, random io.Reader) ([]byte, PublicIdentity, error) {
	if !publisherIDPattern.MatchString(publisherID) || random == nil {
		return nil, PublicIdentity{}, errors.New("open pack builder: publisher ID and randomness are required")
	}
	publicKey, privateKey, err := ed25519.GenerateKey(random)
	if err != nil {
		return nil, PublicIdentity{}, errors.New("open pack builder: generate Ed25519 publisher identity")
	}
	keyID := indexpack.KeyID(publicKey)
	document := identityDocument{
		Version: IdentityVersion, PublisherID: publisherID, KeyID: keyID,
		Ed25519PrivateKey: base64.StdEncoding.EncodeToString(privateKey),
	}
	raw, err := json.Marshal(document)
	if err != nil {
		return nil, PublicIdentity{}, err
	}
	if _, err := DecodeIdentity(raw); err != nil {
		return nil, PublicIdentity{}, err
	}
	return raw, PublicIdentity{
		PublisherID: publisherID, KeyID: keyID,
		Ed25519PublicKey: base64.StdEncoding.EncodeToString(publicKey),
	}, nil
}

func DecodeIdentity(raw []byte) (Identity, error) {
	if len(raw) == 0 || len(raw) > MaxIdentityBytes {
		return Identity{}, fmt.Errorf("open pack builder: identity size must be 1..%d bytes", MaxIdentityBytes)
	}
	var document identityDocument
	if err := indexpack.DecodeStrictJSON(raw, &document); err != nil {
		return Identity{}, fmt.Errorf("open pack builder: invalid identity: %w", err)
	}
	if document.Version != IdentityVersion || !publisherIDPattern.MatchString(document.PublisherID) || len(document.KeyID) != 64 || strings.ToLower(document.KeyID) != document.KeyID {
		return Identity{}, errors.New("open pack builder: invalid identity metadata")
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(document.Ed25519PrivateKey)
	if err != nil || len(decoded) != ed25519.PrivateKeySize || base64.StdEncoding.EncodeToString(decoded) != document.Ed25519PrivateKey {
		return Identity{}, errors.New("open pack builder: private key must be canonical base64 for 64 bytes")
	}
	privateKey := ed25519.PrivateKey(append([]byte(nil), decoded...))
	derived := ed25519.NewKeyFromSeed(privateKey[:ed25519.SeedSize])
	if subtle.ConstantTimeCompare(privateKey, derived) != 1 {
		return Identity{}, errors.New("open pack builder: private key seed and public key are inconsistent")
	}
	publicKey, ok := privateKey.Public().(ed25519.PublicKey)
	if !ok || len(publicKey) != ed25519.PublicKeySize || subtle.ConstantTimeCompare([]byte(indexpack.KeyID(publicKey)), []byte(document.KeyID)) != 1 {
		return Identity{}, errors.New("open pack builder: identity key metadata is inconsistent")
	}
	return Identity{publisherID: document.PublisherID, keyID: document.KeyID, privateKey: privateKey}, nil
}
