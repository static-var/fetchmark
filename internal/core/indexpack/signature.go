package indexpack

import (
	"crypto/ed25519"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	SignatureVersion   = 1
	SignatureAlgorithm = "ed25519"
	MaxSignatureBytes  = 1 << 10

	// ManifestSignatureDomain is prepended verbatim before signing the exact
	// manifest bytes. Its trailing newline prevents prefix ambiguity.
	ManifestSignatureDomain = "fetchmark-open-index-pack-manifest-v1\n"
)

var ErrInvalidSignature = errors.New("index pack: invalid signature")

type DetachedSignature struct {
	Version   int    `json:"version"`
	Algorithm string `json:"algorithm"`
	KeyID     string `json:"key_id"`
	Signature string `json:"signature"`
}

func KeyID(publicKey ed25519.PublicKey) string {
	return Digest(publicKey)
}

func SignManifest(manifestBytes []byte, privateKey ed25519.PrivateKey) (DetachedSignature, error) {
	manifest, err := DecodeManifest(manifestBytes)
	if err != nil {
		return DetachedSignature{}, err
	}
	if len(privateKey) != ed25519.PrivateKeySize {
		return DetachedSignature{}, invalidSignature(errors.New("private key has invalid length"))
	}
	publicKey, ok := privateKey.Public().(ed25519.PublicKey)
	if !ok || len(publicKey) != ed25519.PublicKeySize {
		return DetachedSignature{}, invalidSignature(errors.New("private key has invalid public key"))
	}
	if manifest.SigningKeyID != KeyID(publicKey) {
		return DetachedSignature{}, invalidSignature(errors.New("manifest signing_key_id does not match private key"))
	}
	signed := signatureMessage(manifestBytes)
	return DetachedSignature{
		Version:   SignatureVersion,
		Algorithm: SignatureAlgorithm,
		KeyID:     KeyID(publicKey),
		Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, signed)),
	}, nil
}

func EncodeSignature(signature DetachedSignature) ([]byte, error) {
	if _, err := signature.decoded(); err != nil {
		return nil, invalidSignature(err)
	}
	raw, err := json.Marshal(signature)
	if err != nil {
		return nil, invalidSignature(err)
	}
	if len(raw) > MaxSignatureBytes {
		return nil, invalidSignature(errors.New("encoded signature is too large"))
	}
	return raw, nil
}

func DecodeSignature(raw []byte) (DetachedSignature, error) {
	if len(raw) == 0 || len(raw) > MaxSignatureBytes {
		return DetachedSignature{}, invalidSignature(fmt.Errorf("signature size must be 1..%d bytes", MaxSignatureBytes))
	}
	var signature DetachedSignature
	if err := DecodeStrictJSON(raw, &signature); err != nil {
		return DetachedSignature{}, invalidSignature(err)
	}
	if _, err := signature.decoded(); err != nil {
		return DetachedSignature{}, invalidSignature(err)
	}
	return signature, nil
}

func VerifyManifest(manifestBytes, signatureBytes []byte, trustedKeys map[string]ed25519.PublicKey) (VerifiedManifest, error) {
	if len(manifestBytes) == 0 || len(manifestBytes) > MaxManifestBytes {
		return VerifiedManifest{}, invalidManifest(fmt.Errorf("manifest size must be 1..%d bytes", MaxManifestBytes))
	}
	signature, err := DecodeSignature(signatureBytes)
	if err != nil {
		return VerifiedManifest{}, err
	}
	publicKey, trusted := trustedKeys[signature.KeyID]
	if !trusted {
		return VerifiedManifest{}, invalidSignature(errors.New("signing key is not trusted"))
	}
	if len(publicKey) != ed25519.PublicKeySize {
		return VerifiedManifest{}, invalidSignature(errors.New("trusted public key has invalid length"))
	}
	expectedKeyID := KeyID(publicKey)
	if subtle.ConstantTimeCompare([]byte(expectedKeyID), []byte(signature.KeyID)) != 1 {
		return VerifiedManifest{}, invalidSignature(errors.New("trusted key ID does not match key material"))
	}
	signatureRaw, _ := signature.decoded()
	if !ed25519.Verify(publicKey, signatureMessage(manifestBytes), signatureRaw) {
		return VerifiedManifest{}, invalidSignature(errors.New("signature verification failed"))
	}
	manifest, err := DecodeManifest(manifestBytes)
	if err != nil {
		return VerifiedManifest{}, err
	}
	if manifest.SigningKeyID != signature.KeyID {
		return VerifiedManifest{}, invalidSignature(errors.New("manifest signing_key_id does not match detached signature"))
	}
	return VerifiedManifest{Manifest: manifest, Digest: ManifestDigest(manifestBytes), KeyID: signature.KeyID}, nil
}

func (signature DetachedSignature) decoded() ([]byte, error) {
	if signature.Version != SignatureVersion {
		return nil, fmt.Errorf("version must be %d", SignatureVersion)
	}
	if signature.Algorithm != SignatureAlgorithm {
		return nil, fmt.Errorf("algorithm must be %q", SignatureAlgorithm)
	}
	if !hexDigestPattern.MatchString(signature.KeyID) {
		return nil, errors.New("key_id must be a lowercase SHA-256 digest")
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(signature.Signature)
	if err != nil || len(decoded) != ed25519.SignatureSize {
		return nil, errors.New("signature must be canonical base64 for 64 bytes")
	}
	if base64.StdEncoding.EncodeToString(decoded) != signature.Signature {
		return nil, errors.New("signature must use canonical base64")
	}
	return decoded, nil
}

func signatureMessage(manifestBytes []byte) []byte {
	domain := []byte(ManifestSignatureDomain)
	message := make([]byte, len(domain)+len(manifestBytes))
	copy(message, domain)
	copy(message[len(domain):], manifestBytes)
	return message
}

func invalidSignature(err error) error {
	return fmt.Errorf("%w: %v", ErrInvalidSignature, err)
}
