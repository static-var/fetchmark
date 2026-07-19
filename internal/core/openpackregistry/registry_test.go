package openpackregistry

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

func TestLoadResolvesExactAcceptanceAndCopiesKeys(t *testing.T) {
	document, publicKey := validRegistryDocument(t)
	registry, err := Load(bytes.NewReader(marshalDocument(t, document)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	binding, exists := registry.Lookup("openpack-developer")
	if !exists {
		t.Fatal("Lookup() did not return configured binding")
	}
	if binding.PublisherID != "fetchmark-community" || binding.InstalledPath != document.Bindings[0].InstalledPath || binding.RecordCount != 10 ||
		binding.CreatedAt != "2026-07-18T00:00:00Z" || binding.ExpiresAt != "2026-08-18T00:00:00Z" {
		t.Fatalf("Lookup() = %#v", binding)
	}
	now := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	acceptance := binding.Acceptance(now)
	if acceptance.Now != now || acceptance.ExpectedPackID != document.Bindings[0].PackID ||
		acceptance.ExpectedManifestSHA256 != document.Bindings[0].ManifestSHA256 ||
		acceptance.ExpectedKeyID != document.Publishers[0].KeyID ||
		acceptance.ExpectedRevision != 7 || acceptance.MinimumRevision != 7 {
		t.Fatalf("Acceptance() = %#v", acceptance)
	}

	keys := registry.TrustedKeys()
	key := keys[document.Publishers[0].KeyID]
	if !bytes.Equal(key, publicKey) {
		t.Fatalf("TrustedKeys() key differs from configured key")
	}
	key[0] ^= 0xff
	delete(keys, document.Publishers[0].KeyID)
	if got := registry.TrustedKeys()[document.Publishers[0].KeyID]; !bytes.Equal(got, publicKey) {
		t.Fatal("TrustedKeys() exposed mutable registry key material")
	}
	if _, exists := registry.Lookup("missing"); exists {
		t.Fatal("Lookup() unexpectedly found missing source")
	}
}

func TestLoadVersionTwoDeltaPinsManifestAndProjectionCounts(t *testing.T) {
	document, _ := validRegistryDocument(t)
	raw := string(marshalDocument(t, document))
	parentDigest := strings.Repeat("b", 64)
	raw = strings.Replace(raw, `"version":1`, `"version":2`, 1)
	raw = strings.Replace(raw, `"record_count":10`, `"kind":"delta","parent_manifest_sha256":"`+parentDigest+`","manifest_record_count":3,"projection_record_count":9`, 1)
	registry, err := Load(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("Load(v2 delta): %v", err)
	}
	binding, found := registry.Lookup("openpack-developer")
	if !found || binding.Kind != indexpack.KindDelta || binding.ParentManifestSHA256 != parentDigest ||
		binding.ManifestRecordCount != 3 || binding.RecordCount != 9 {
		t.Fatalf("delta binding = %#v", binding)
	}
	acceptance := binding.Acceptance(time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC))
	if acceptance.ExpectedRecordCount != 3 {
		t.Fatalf("delta manifest acceptance count = %d, want 3", acceptance.ExpectedRecordCount)
	}
}

func TestLoadVersionTwoSnapshotPinsEqualCounts(t *testing.T) {
	document, _ := validRegistryDocument(t)
	raw := string(marshalDocument(t, document))
	raw = strings.Replace(raw, `"version":1`, `"version":2`, 1)
	raw = strings.Replace(raw, `"record_count":10`, `"kind":"snapshot","manifest_record_count":10,"projection_record_count":10`, 1)
	registry, err := Load(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("Load(v2 snapshot): %v", err)
	}
	binding, found := registry.Lookup("openpack-developer")
	if !found || binding.Kind != indexpack.KindSnapshot || binding.ParentManifestSHA256 != "" ||
		binding.ManifestRecordCount != 10 || binding.RecordCount != 10 {
		t.Fatalf("snapshot binding = %#v", binding)
	}
}

func TestLoadRejectsInvalidVersionTwoBindingFields(t *testing.T) {
	document, _ := validRegistryDocument(t)
	valid := string(marshalDocument(t, document))
	valid = strings.Replace(valid, `"version":1`, `"version":2`, 1)
	valid = strings.Replace(valid, `"record_count":10`, `"kind":"delta","parent_manifest_sha256":"`+strings.Repeat("b", 64)+`","manifest_record_count":3,"projection_record_count":9`, 1)
	tests := map[string]func(string) string{
		"legacy count": func(raw string) string {
			return strings.Replace(raw, `"manifest_record_count":3`, `"record_count":9,"manifest_record_count":3`, 1)
		},
		"missing kind": func(raw string) string {
			return strings.Replace(raw, `"kind":"delta",`, ``, 1)
		},
		"missing parent": func(raw string) string {
			return strings.Replace(raw, `"parent_manifest_sha256":"`+strings.Repeat("b", 64)+`",`, ``, 1)
		},
		"missing manifest count": func(raw string) string {
			return strings.Replace(raw, `"manifest_record_count":3,`, ``, 1)
		},
		"missing projection count": func(raw string) string {
			return strings.Replace(raw, `,"projection_record_count":9`, ``, 1)
		},
		"zero manifest count": func(raw string) string {
			return strings.Replace(raw, `"manifest_record_count":3`, `"manifest_record_count":0`, 1)
		},
		"zero projection count": func(raw string) string {
			return strings.Replace(raw, `"projection_record_count":9`, `"projection_record_count":0`, 1)
		},
		"excess manifest count": func(raw string) string {
			return strings.Replace(raw, `"manifest_record_count":3`, fmt.Sprintf(`"manifest_record_count":%d`, indexpack.RecommendedInstallRecordLimit+1), 1)
		},
		"excess projection count": func(raw string) string {
			return strings.Replace(raw, `"projection_record_count":9`, fmt.Sprintf(`"projection_record_count":%d`, indexpack.RecommendedInstallRecordLimit+1), 1)
		},
		"delta revision one": func(raw string) string {
			return strings.Replace(raw, `"revision":7`, `"revision":1`, 1)
		},
		"snapshot with parent": func(raw string) string {
			return strings.Replace(raw, `"kind":"delta"`, `"kind":"snapshot"`, 1)
		},
		"snapshot mismatched counts": func(raw string) string {
			raw = strings.Replace(raw, `"kind":"delta"`, `"kind":"snapshot"`, 1)
			return strings.Replace(raw, `"parent_manifest_sha256":"`+strings.Repeat("b", 64)+`",`, ``, 1)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Load(strings.NewReader(mutate(valid)))
			if !errors.Is(err, ErrInvalidRegistry) {
				t.Fatalf("Load() error = %v, want ErrInvalidRegistry", err)
			}
		})
	}
}

func TestLoadSupportsMultipleBindingsForReferencedPublisher(t *testing.T) {
	document, _ := validRegistryDocument(t)
	second := document.Bindings[0]
	second.SourceID = "openpack-knowledge"
	second.PackID = "knowledge-en"
	document.Bindings = append(document.Bindings, second)

	registry, err := Load(bytes.NewReader(marshalDocument(t, document)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(registry.TrustedKeys()) != 1 {
		t.Fatalf("TrustedKeys() length = %d, want 1", len(registry.TrustedKeys()))
	}
	if _, exists := registry.Lookup(second.SourceID); !exists {
		t.Fatal("second binding was not loaded")
	}
}

func TestPromoteProducesDeterministicVersionTwoCandidateWithoutMutatingCurrentRegistry(t *testing.T) {
	document, publicKey := validRegistryDocument(t)
	second := document.Bindings[0]
	second.SourceID = "openpack-knowledge"
	second.PackID = "knowledge-en"
	document.Bindings = append(document.Bindings, second)
	registry, err := Load(bytes.NewReader(marshalDocument(t, document)))
	if err != nil {
		t.Fatal(err)
	}
	current, _ := registry.Lookup("openpack-developer")
	digest := strings.Repeat("b", 64)
	promotion := Promotion{
		PackID: "developer-en", SigningKeyID: indexpack.KeyID(publicKey), Kind: indexpack.KindSnapshot,
		ManifestSHA256: digest, Revision: 8, ManifestRecordCount: 12, ProjectionRecordCount: 12,
		CreatedAt: "2026-07-19T00:00:00Z", ExpiresAt: "2026-08-19T00:00:00Z",
		InstalledPath: "/var/lib/fetchmark/packs/objects/" + digest,
	}

	candidate, err := registry.Promote("openpack-developer", promotion)
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	first, err := candidate.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	secondEncoding, err := candidate.Encode()
	if err != nil || !bytes.Equal(first, secondEncoding) {
		t.Fatalf("candidate encoding is not deterministic: %v", err)
	}
	reloaded, err := Load(bytes.NewReader(first))
	if err != nil {
		t.Fatalf("Load candidate: %v", err)
	}
	updated, found := reloaded.Lookup("openpack-developer")
	if !found || updated.ManifestSHA256 != digest || updated.Revision != 8 || updated.Kind != indexpack.KindSnapshot ||
		updated.ManifestRecordCount != 12 || updated.RecordCount != 12 || updated.SigningKeyID != promotion.SigningKeyID {
		t.Fatalf("promoted binding = %#v", updated)
	}
	if knowledge, found := reloaded.Lookup("openpack-knowledge"); !found || knowledge.PackID != "knowledge-en" || knowledge.Revision != 7 {
		t.Fatalf("preserved binding = %#v, found=%v", knowledge, found)
	}
	if unchanged, _ := registry.Lookup("openpack-developer"); unchanged != current {
		t.Fatalf("current registry mutated: before=%#v after=%#v", current, unchanged)
	}
}

func TestPromoteAcceptsExactSnapshotParentDelta(t *testing.T) {
	document, publicKey := validRegistryDocument(t)
	registry, err := Load(bytes.NewReader(marshalDocument(t, document)))
	if err != nil {
		t.Fatal(err)
	}
	parent := document.Bindings[0].ManifestSHA256
	digest := strings.Repeat("b", 64)
	candidate, err := registry.Promote("openpack-developer", Promotion{
		PackID: "developer-en", SigningKeyID: indexpack.KeyID(publicKey), Kind: indexpack.KindDelta,
		ManifestSHA256: digest, ParentManifestSHA256: parent, Revision: 8,
		ManifestRecordCount: 3, ProjectionRecordCount: 9,
		CreatedAt: "2026-07-19T00:00:00Z", ExpiresAt: "2026-08-19T00:00:00Z",
		InstalledPath: "/var/lib/fetchmark/packs/objects/" + digest,
	})
	if err != nil {
		t.Fatalf("Promote delta: %v", err)
	}
	binding, found := candidate.Lookup("openpack-developer")
	if !found || binding.Kind != indexpack.KindDelta || binding.ParentManifestSHA256 != parent ||
		binding.ManifestRecordCount != 3 || binding.RecordCount != 9 {
		t.Fatalf("delta binding = %#v", binding)
	}
}

func TestPromoteRejectsTrustRollbackAndPathChanges(t *testing.T) {
	document, publicKey := validRegistryDocument(t)
	registry, err := Load(bytes.NewReader(marshalDocument(t, document)))
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("b", 64)
	valid := Promotion{
		PackID: "developer-en", SigningKeyID: indexpack.KeyID(publicKey), Kind: indexpack.KindSnapshot,
		ManifestSHA256: digest, Revision: 8, ManifestRecordCount: 12, ProjectionRecordCount: 12,
		CreatedAt: "2026-07-19T00:00:00Z", ExpiresAt: "2026-08-19T00:00:00Z",
		InstalledPath: "/var/lib/fetchmark/packs/objects/" + digest,
	}
	tests := map[string]func(*Promotion){
		"missing source":       func(candidate *Promotion) {},
		"publisher key change": func(candidate *Promotion) { candidate.SigningKeyID = strings.Repeat("c", 64) },
		"pack change":          func(candidate *Promotion) { candidate.PackID = "other-pack" },
		"same revision":        func(candidate *Promotion) { candidate.Revision = 7 },
		"older creation":       func(candidate *Promotion) { candidate.CreatedAt = "2026-07-17T00:00:00Z" },
		"object root change": func(candidate *Promotion) {
			candidate.InstalledPath = "/var/lib/other/objects/" + digest
		},
		"delta wrong parent": func(candidate *Promotion) {
			candidate.Kind = indexpack.KindDelta
			candidate.ParentManifestSHA256 = strings.Repeat("c", 64)
			candidate.ManifestRecordCount = 3
			candidate.ProjectionRecordCount = 9
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			promotion := valid
			mutate(&promotion)
			source := "openpack-developer"
			if name == "missing source" {
				source = "missing"
			}
			if _, err := registry.Promote(source, promotion); !errors.Is(err, ErrInvalidPromotion) {
				t.Fatalf("Promote = %v, want ErrInvalidPromotion", err)
			}
		})
	}
}

func TestLoadRejectsInvalidRegistryShapes(t *testing.T) {
	document, _ := validRegistryDocument(t)
	valid := string(marshalDocument(t, document))
	tests := map[string][]byte{
		"empty":              {},
		"invalid UTF-8":      {0xff},
		"root null":          []byte(`null`),
		"nested null":        []byte(strings.Replace(valid, `"revision":7`, `"revision":null`, 1)),
		"unknown field":      []byte(strings.Replace(valid, `"version":1`, `"version":1,"extra":true`, 1)),
		"duplicate root":     []byte(strings.Replace(valid, `"version":1`, `"version":1,"version":1`, 1)),
		"duplicate nested":   []byte(strings.Replace(valid, `"id":"fetchmark-community"`, `"id":"fetchmark-community","id":"alias"`, 1)),
		"trailing JSON":      []byte(valid + `{}`),
		"uint64 overflow":    []byte(strings.Replace(valid, `"revision":7`, `"revision":18446744073709551616`, 1)),
		"non-object root":    []byte(`[]`),
		"forbidden trailing": []byte(valid + ` true`),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Load(bytes.NewReader(raw))
			if !errors.Is(err, ErrInvalidRegistry) {
				t.Fatalf("Load() error = %v, want ErrInvalidRegistry", err)
			}
		})
	}
}

func TestLoadRejectsOversizedRegistry(t *testing.T) {
	_, err := Load(strings.NewReader(strings.Repeat(" ", MaxRegistryBytes+1)))
	if !errors.Is(err, ErrInvalidRegistry) {
		t.Fatalf("Load() error = %v, want ErrInvalidRegistry", err)
	}
}

func TestLoadRejectsPublisherViolations(t *testing.T) {
	document, _ := validRegistryDocument(t)
	secondKey := deterministicPublicKey(2)
	tests := map[string]func(*registryJSON){
		"version":       func(candidate *registryJSON) { candidate.Version = 2 },
		"no publishers": func(candidate *registryJSON) { candidate.Publishers = nil },
		"too many publishers": func(candidate *registryJSON) {
			candidate.Publishers = make([]publisherJSON, MaxPublishers+1)
		},
		"uppercase publisher id": func(candidate *registryJSON) { candidate.Publishers[0].ID = "Community" },
		"oversized publisher id": func(candidate *registryJSON) { candidate.Publishers[0].ID = strings.Repeat("a", maxIDBytes+1) },
		"bad key digest":         func(candidate *registryJSON) { candidate.Publishers[0].KeyID = strings.Repeat("A", 64) },
		"key mismatch": func(candidate *registryJSON) {
			candidate.Publishers[0].Ed25519PublicKey = base64.StdEncoding.EncodeToString(secondKey)
		},
		"short public key": func(candidate *registryJSON) {
			candidate.Publishers[0].Ed25519PublicKey = base64.StdEncoding.EncodeToString([]byte("short"))
		},
		"noncanonical public key": func(candidate *registryJSON) {
			candidate.Publishers[0].Ed25519PublicKey = strings.TrimRight(candidate.Publishers[0].Ed25519PublicKey, "=")
		},
		"duplicate publisher": func(candidate *registryJSON) {
			candidate.Publishers = append(candidate.Publishers, candidate.Publishers[0])
		},
		"duplicate signing key": func(candidate *registryJSON) {
			alias := candidate.Publishers[0]
			alias.ID = "community-alias"
			candidate.Publishers = append(candidate.Publishers, alias)
		},
		"unreferenced publisher": func(candidate *registryJSON) {
			candidate.Publishers = append(candidate.Publishers, publisherJSON{
				ID:               "unused",
				KeyID:            indexpack.KeyID(secondKey),
				Ed25519PublicKey: base64.StdEncoding.EncodeToString(secondKey),
			})
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := cloneDocument(t, document)
			mutate(&candidate)
			_, err := Load(bytes.NewReader(marshalDocument(t, candidate)))
			if !errors.Is(err, ErrInvalidRegistry) {
				t.Fatalf("Load() error = %v, want ErrInvalidRegistry", err)
			}
			if len(candidate.Publishers) > 0 && candidate.Publishers[0].Ed25519PublicKey != "" && strings.Contains(fmt.Sprint(err), candidate.Publishers[0].Ed25519PublicKey) {
				t.Fatal("error leaked encoded public key")
			}
		})
	}
}

func TestLoadRejectsBindingViolations(t *testing.T) {
	document, _ := validRegistryDocument(t)
	digest := document.Bindings[0].ManifestSHA256
	tests := map[string]func(*registryJSON){
		"no bindings":       func(candidate *registryJSON) { candidate.Bindings = nil },
		"too many bindings": func(candidate *registryJSON) { candidate.Bindings = make([]bindingJSON, MaxBindings+1) },
		"uppercase source":  func(candidate *registryJSON) { candidate.Bindings[0].SourceID = "Openpack" },
		"bad pack":          func(candidate *registryJSON) { candidate.Bindings[0].PackID = "bad/pack" },
		"bad publisher":     func(candidate *registryJSON) { candidate.Bindings[0].PublisherID = "unknown" },
		"bad digest":        func(candidate *registryJSON) { candidate.Bindings[0].ManifestSHA256 = strings.Repeat("z", 64) },
		"zero revision":     func(candidate *registryJSON) { candidate.Bindings[0].Revision = 0 },
		"zero record count": func(candidate *registryJSON) { candidate.Bindings[0].RecordCount = 0 },
		"excess record count": func(candidate *registryJSON) {
			candidate.Bindings[0].RecordCount = indexpack.RecommendedInstallRecordLimit + 1
		},
		"invalid created at": func(candidate *registryJSON) { candidate.Bindings[0].CreatedAt = "2026-07-18T00:00:00+00:00" },
		"invalid expires at": func(candidate *registryJSON) { candidate.Bindings[0].ExpiresAt = "later" },
		"reversed validity":  func(candidate *registryJSON) { candidate.Bindings[0].ExpiresAt = candidate.Bindings[0].CreatedAt },
		"relative path":      func(candidate *registryJSON) { candidate.Bindings[0].InstalledPath = "objects/" + digest },
		"unclean path": func(candidate *registryJSON) {
			candidate.Bindings[0].InstalledPath = "/packs/objects/../objects/" + digest
		},
		"wrong parent": func(candidate *registryJSON) { candidate.Bindings[0].InstalledPath = "/packs/artifacts/" + digest },
		"wrong basename": func(candidate *registryJSON) {
			candidate.Bindings[0].InstalledPath = "/packs/objects/" + strings.Repeat("b", 64)
		},
		"control path":    func(candidate *registryJSON) { candidate.Bindings[0].InstalledPath = "/packs/objects/\n" + digest },
		"format path":     func(candidate *registryJSON) { candidate.Bindings[0].InstalledPath = "/packs/objects/\u202e" + digest },
		"whitespace path": func(candidate *registryJSON) { candidate.Bindings[0].InstalledPath += " " },
		"oversized path": func(candidate *registryJSON) {
			candidate.Bindings[0].InstalledPath = "/" + strings.Repeat("a", maxPathBytes)
		},
		"duplicate source": func(candidate *registryJSON) { candidate.Bindings = append(candidate.Bindings, candidate.Bindings[0]) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := cloneDocument(t, document)
			mutate(&candidate)
			_, err := Load(bytes.NewReader(marshalDocument(t, candidate)))
			if !errors.Is(err, ErrInvalidRegistry) {
				t.Fatalf("Load() error = %v, want ErrInvalidRegistry", err)
			}
		})
	}
}

func validRegistryDocument(t *testing.T) (registryJSON, ed25519.PublicKey) {
	t.Helper()
	publicKey := deterministicPublicKey(1)
	keyID := indexpack.KeyID(publicKey)
	digest := strings.Repeat("a", 64)
	return registryJSON{
		Version: Version,
		Publishers: []publisherJSON{{
			ID:               "fetchmark-community",
			KeyID:            keyID,
			Ed25519PublicKey: base64.StdEncoding.EncodeToString(publicKey),
		}},
		Bindings: []bindingJSON{{
			SourceID:       "openpack-developer",
			PackID:         "developer-en",
			PublisherID:    "fetchmark-community",
			ManifestSHA256: digest,
			Revision:       7,
			RecordCount:    10,
			CreatedAt:      "2026-07-18T00:00:00Z",
			ExpiresAt:      "2026-08-18T00:00:00Z",
			InstalledPath:  "/var/lib/fetchmark/packs/objects/" + digest,
		}},
	}, publicKey
}

func deterministicPublicKey(fill byte) ed25519.PublicKey {
	seed := bytes.Repeat([]byte{fill}, ed25519.SeedSize)
	privateKey := ed25519.NewKeyFromSeed(seed)
	return append(ed25519.PublicKey(nil), privateKey.Public().(ed25519.PublicKey)...)
}

func marshalDocument(t *testing.T, document registryJSON) []byte {
	t.Helper()
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return raw
}

func cloneDocument(t *testing.T, document registryJSON) registryJSON {
	t.Helper()
	var clone registryJSON
	if err := json.Unmarshal(marshalDocument(t, document), &clone); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	return clone
}
