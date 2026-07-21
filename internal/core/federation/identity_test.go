package federation

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestDecodeIdentityStrictAndDefensive(t *testing.T) {
	publicKey, privateKey := testKey(7)
	raw := identityFixture("node.alpha", KeyID(publicKey), privateKey)
	identity, err := DecodeIdentity(raw)
	if err != nil {
		t.Fatalf("DecodeIdentity: %v", err)
	}
	if identity.IdentityID() != "node.alpha" || identity.KeyID() != KeyID(publicKey) {
		t.Fatalf("unexpected identity metadata: %q %q", identity.IdentityID(), identity.KeyID())
	}
	returnedPublic := identity.PublicKey()
	returnedPrivate := identity.PrivateKey()
	if !bytes.Equal(returnedPublic, publicKey) || !bytes.Equal(returnedPrivate, privateKey) {
		t.Fatal("decoded key material does not match fixture")
	}
	returnedPublic[0] ^= 0xff
	returnedPrivate[0] ^= 0xff
	if bytes.Equal(returnedPublic, identity.PublicKey()) || bytes.Equal(returnedPrivate, identity.PrivateKey()) {
		t.Fatal("identity key material mutated through an accessor")
	}
}

func TestDecodeIdentityRejectsMalformedDocuments(t *testing.T) {
	publicKey, privateKey := testKey(9)
	valid := string(identityFixture("node.alpha", KeyID(publicKey), privateKey))
	inconsistent := append(ed25519.PrivateKey(nil), privateKey...)
	inconsistent[ed25519.SeedSize] ^= 0xff

	cases := map[string][]byte{
		"empty":               nil,
		"oversized":           bytes.Repeat([]byte{'x'}, MaxIdentityBytes+1),
		"invalid UTF-8":       append([]byte(valid[:len(valid)-1]), 0xff, '}'),
		"unknown field":       []byte(strings.Replace(valid, `"version":1`, `"version":1,"extra":true`, 1)),
		"duplicate field":     []byte(strings.Replace(valid, `"version":1`, `"version":1,"version":1`, 1)),
		"null":                []byte(strings.Replace(valid, `"identity_id":"node.alpha"`, `"identity_id":null`, 1)),
		"trailing value":      []byte(valid + `{}`),
		"excessive depth":     []byte(strings.Replace(valid, `"version":1`, `"version":1,"extra":`+strings.Repeat("[", maxJSONDepth+2)+`0`+strings.Repeat("]", maxJSONDepth+2), 1)),
		"wrong version":       []byte(strings.Replace(valid, `"version":1`, `"version":2`, 1)),
		"invalid identity ID": []byte(strings.Replace(valid, `"node.alpha"`, `"Node Alpha"`, 1)),
		"invalid key ID":      []byte(strings.Replace(valid, KeyID(publicKey), strings.Repeat("A", 64), 1)),
		"unpadded key":        []byte(strings.Replace(valid, base64.StdEncoding.EncodeToString(privateKey), base64.RawStdEncoding.EncodeToString(privateKey), 1)),
		"wrong key length":    identityFixture("node.alpha", KeyID(publicKey), privateKey[:ed25519.SeedSize]),
		"inconsistent key":    identityFixture("node.alpha", KeyID(publicKey), inconsistent),
		"mismatched key ID":   identityFixture("node.alpha", strings.Repeat("a", 64), privateKey),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeIdentity(raw); !errors.Is(err, ErrInvalidIdentity) {
				t.Fatalf("DecodeIdentity error = %v, want ErrInvalidIdentity", err)
			}
		})
	}
}

func TestDecodeIdentityErrorsDoNotExposePrivateKey(t *testing.T) {
	publicKey, privateKey := testKey(11)
	secret := base64.StdEncoding.EncodeToString(privateKey)
	raw := identityFixture("node.alpha", strings.Repeat("a", 64), privateKey)
	_, err := DecodeIdentity(raw)
	if err == nil {
		t.Fatal("DecodeIdentity succeeded")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error exposed private key material: %v", err)
	}
	if KeyID(publicKey) == strings.Repeat("a", 64) {
		t.Fatal("invalid test fixture")
	}
}

func identityFixture(identityID, keyID string, privateKey []byte) []byte {
	return []byte(fmt.Sprintf(`{"version":1,"identity_id":%q,"key_id":%q,"ed25519_private_key":%q}`,
		identityID, keyID, base64.StdEncoding.EncodeToString(privateKey)))
}
