package tufrepository

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/theupdateframework/go-tuf/v2/metadata"
)

func TestGenerateIdentityProducesDistinctRoleKeysAndStableBootstrapRoot(t *testing.T) {
	expires := time.Date(2027, 7, 1, 0, 0, 0, 0, time.UTC)
	raw, rootRaw, err := GenerateIdentity("fetchmark-community", expires, deterministicIdentityRandom())
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	identity, err := DecodeIdentity(raw)
	if err != nil {
		t.Fatalf("DecodeIdentity: %v", err)
	}
	if identity.RepositoryID() != "fetchmark-community" {
		t.Fatalf("repository ID = %q", identity.RepositoryID())
	}
	regenerated, err := identity.BootstrapRoot()
	if err != nil {
		t.Fatalf("BootstrapRoot: %v", err)
	}
	if !bytes.Equal(rootRaw, regenerated) {
		t.Fatal("bootstrap root is not stable")
	}
	root, err := metadata.Root().FromBytes(rootRaw)
	if err != nil {
		t.Fatalf("parse bootstrap root: %v", err)
	}
	if root.Signed.Version != 1 || !root.Signed.ConsistentSnapshot || !root.Signed.Expires.Equal(expires) {
		t.Fatalf("root metadata = %#v", root.Signed)
	}
	seen := map[string]string{}
	for _, roleName := range []string{metadata.ROOT, metadata.TARGETS, metadata.SNAPSHOT, metadata.TIMESTAMP} {
		role, found := root.Signed.Roles[roleName]
		wantThreshold, wantKeys := 1, 1
		if roleName == metadata.ROOT {
			wantThreshold, wantKeys = 2, 2
		}
		if !found || role.Threshold != wantThreshold || len(role.KeyIDs) != wantKeys {
			t.Fatalf("role %s = %#v", roleName, role)
		}
		for _, keyID := range role.KeyIDs {
			if previous, duplicate := seen[keyID]; duplicate {
				t.Fatalf("roles %s and %s reuse key %s", previous, roleName, keyID)
			}
			seen[keyID] = roleName
		}
	}
	if err := root.VerifyDelegate(metadata.ROOT, root); err != nil {
		t.Fatalf("self-verify root: %v", err)
	}
}

func TestDecodeIdentityPreservesExactSignedBootstrapRootBytes(t *testing.T) {
	expires := time.Date(2027, 7, 1, 0, 0, 0, 0, time.UTC)
	raw, rootRaw, err := GenerateIdentity("fetchmark-community", expires, deterministicIdentityRandom())
	if err != nil {
		t.Fatal(err)
	}
	var indented bytes.Buffer
	if err := json.Indent(&indented, rootRaw, "", "  "); err != nil {
		t.Fatal(err)
	}
	indented.WriteByte('\n')
	var document identityDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	document.BootstrapRoot = base64.StdEncoding.EncodeToString(indented.Bytes())
	fixture, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeIdentity(fixture)
	if err != nil {
		t.Fatalf("DecodeIdentity cross-serialization fixture: %v", err)
	}
	preserved, err := identity.BootstrapRoot()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(preserved, indented.Bytes()) {
		t.Fatal("bootstrap root bytes were regenerated instead of preserved exactly")
	}
}

func TestDecodeIdentityRejectsUnknownAndInconsistentFields(t *testing.T) {
	expires := time.Date(2027, 7, 1, 0, 0, 0, 0, time.UTC)
	raw, _, err := GenerateIdentity("fetchmark-community", expires, deterministicIdentityRandom())
	if err != nil {
		t.Fatal(err)
	}
	unknown := append(append([]byte(nil), raw[:len(raw)-1]...), []byte(",\"unknown\":true}")...)
	if _, err := DecodeIdentity(unknown); err == nil {
		t.Fatal("DecodeIdentity accepted unknown field")
	}
	tampered := bytes.Replace(raw, []byte("\"repository_id\":\"fetchmark-community\""), []byte("\"repository_id\":\"INVALID\""), 1)
	if _, err := DecodeIdentity(tampered); err == nil {
		t.Fatal("DecodeIdentity accepted invalid repository ID")
	}
}

func deterministicIdentityRandom() *bytes.Reader {
	raw := make([]byte, 256)
	for index := range raw {
		raw[index] = byte(index)
	}
	return bytes.NewReader(raw)
}
