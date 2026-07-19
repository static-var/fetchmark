package federationconfigfile

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/core/federation"
)

func TestLoadTrustRegistryAndPrivateIdentity(t *testing.T) {
	root := t.TempDir()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityPath := writeFile(t, root, "identity.json", identityDocument("node.alpha", publicKey, privateKey), 0o600)
	registryPath := writeFile(t, root, "trust.json", trustDocument(publicKey), 0o644)

	identity, err := LoadIdentity(identityPath)
	if err != nil {
		t.Fatalf("LoadIdentity: %v", err)
	}
	if identity.IdentityID() != "node.alpha" || identity.KeyID() != federation.KeyID(publicKey) {
		t.Fatalf("unexpected identity: %q %q", identity.IdentityID(), identity.KeyID())
	}
	registry, err := LoadTrustRegistry(registryPath)
	if err != nil {
		t.Fatalf("LoadTrustRegistry: %v", err)
	}
	peer, found := registry.Lookup("peer-one")
	if !found || peer.IdentityID != "node.alpha" {
		t.Fatalf("unexpected peer: %+v, found=%t", peer, found)
	}
}

func TestLoadIdentityRequiresPrivateModeAndTrustRejectsWritableMode(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	identityPath := writeFile(t, root, "identity.json", identityDocument("node.alpha", publicKey, privateKey), 0o644)
	if _, err := LoadIdentity(identityPath); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("LoadIdentity error = %v, want ErrUnsafePath", err)
	}
	registryPath := writeFile(t, root, "trust.json", trustDocument(publicKey), 0o666)
	if _, err := LoadTrustRegistry(registryPath); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("LoadTrustRegistry error = %v, want ErrUnsafePath", err)
	}
}

func TestLoadParsingErrorsPreserveCoreSentinelsWithoutSecrets(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	secret := base64.StdEncoding.EncodeToString(privateKey)
	identityPath := writeFile(t, root, "identity.json", []byte(fmt.Sprintf(
		`{"version":1,"identity_id":"node.alpha","key_id":%q,"ed25519_private_key":%q}`,
		strings.Repeat("a", 64), secret)), 0o600)
	if _, err := LoadIdentity(identityPath); !errors.Is(err, federation.ErrInvalidIdentity) {
		t.Fatalf("LoadIdentity error = %v, want ErrInvalidIdentity", err)
	} else if strings.Contains(err.Error(), secret) {
		t.Fatalf("LoadIdentity error exposed private key material: %v", err)
	}

	registryPath := writeFile(t, root, "trust.json", append(trustDocument(publicKey), []byte(`{}`)...), 0o600)
	if _, err := LoadTrustRegistry(registryPath); !errors.Is(err, federation.ErrInvalidTrustRegistry) {
		t.Fatalf("LoadTrustRegistry error = %v, want ErrInvalidTrustRegistry", err)
	}
}

func TestLoadRejectsNonCanonicalPaths(t *testing.T) {
	if _, err := LoadIdentity("identity.json"); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("LoadIdentity error = %v, want ErrUnsafePath", err)
	}
	if _, err := LoadTrustRegistry("trust.json"); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("LoadTrustRegistry error = %v, want ErrUnsafePath", err)
	}
}

func identityDocument(identityID string, publicKey ed25519.PublicKey, privateKey ed25519.PrivateKey) []byte {
	return []byte(fmt.Sprintf(`{"version":1,"identity_id":%q,"key_id":%q,"ed25519_private_key":%q}`,
		identityID, federation.KeyID(publicKey), base64.StdEncoding.EncodeToString(privateKey)))
}

func trustDocument(publicKey ed25519.PublicKey) []byte {
	return []byte(fmt.Sprintf(`{"version":1,"identities":[{"id":"node.alpha","key_id":%q,"ed25519_public_key":%q}],"peers":[{"id":"peer-one","identity_id":"node.alpha","base_origin":"https://peer.example","allow_private_network":false,"allow_inbound":true}]}`,
		federation.KeyID(publicKey), base64.StdEncoding.EncodeToString(publicKey)))
}

func writeFile(t *testing.T, root, name string, contents []byte, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}
