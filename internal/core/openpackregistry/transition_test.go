package openpackregistry

import (
	"bytes"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

func TestValidateTransitionAcceptsExactPromotionAndExplicitReverseRollback(t *testing.T) {
	document, publicKey := validRegistryDocument(t)
	current, err := Load(bytes.NewReader(marshalDocument(t, document)))
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("b", 64)
	candidate, err := current.Promote("openpack-developer", Promotion{
		PackID: "developer-en", SigningKeyID: indexpack.KeyID(publicKey), Kind: indexpack.KindSnapshot,
		ManifestSHA256: digest, Revision: 8, ManifestRecordCount: 12, ProjectionRecordCount: 12,
		CreatedAt: "2026-07-19T00:00:00Z", ExpiresAt: "2026-08-19T00:00:00Z",
		InstalledPath: filepath.Join(filepath.Dir(document.Bindings[0].InstalledPath), digest),
	})
	if err != nil {
		t.Fatal(err)
	}
	forward, err := ValidateTransition(current, candidate, "openpack-developer", TransitionActivate)
	if err != nil {
		t.Fatalf("ValidateTransition activate: %v", err)
	}
	if forward.From.Revision != 7 || forward.To.Revision != 8 || forward.Mode != TransitionActivate {
		t.Fatalf("forward transition = %#v", forward)
	}
	rollback, err := ValidateTransition(candidate, current, "openpack-developer", TransitionRollback)
	if err != nil {
		t.Fatalf("ValidateTransition rollback: %v", err)
	}
	if rollback.From.Revision != 8 || rollback.To.Revision != 7 || rollback.Mode != TransitionRollback {
		t.Fatalf("rollback transition = %#v", rollback)
	}
}

func TestValidateTransitionRejectsWrongDirectionTrustAndUnrelatedChanges(t *testing.T) {
	document, publicKey := validRegistryDocument(t)
	second := document.Bindings[0]
	second.SourceID = "openpack-knowledge"
	second.PackID = "knowledge-en"
	document.Bindings = append(document.Bindings, second)
	current, err := Load(bytes.NewReader(marshalDocument(t, document)))
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("b", 64)
	candidate, err := current.Promote("openpack-developer", Promotion{
		PackID: "developer-en", SigningKeyID: indexpack.KeyID(publicKey), Kind: indexpack.KindSnapshot,
		ManifestSHA256: digest, Revision: 8, ManifestRecordCount: 12, ProjectionRecordCount: 12,
		CreatedAt: "2026-07-19T00:00:00Z", ExpiresAt: "2026-08-19T00:00:00Z",
		InstalledPath: filepath.Join(filepath.Dir(document.Bindings[0].InstalledPath), digest),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateTransition(current, candidate, "openpack-developer", TransitionRollback); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("forward accepted as rollback: %v", err)
	}
	if _, err := ValidateTransition(candidate, current, "openpack-developer", TransitionActivate); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("rollback accepted as activation: %v", err)
	}
	if _, err := ValidateTransition(current, candidate, "openpack-knowledge", TransitionActivate); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("wrong source accepted: %v", err)
	}

	candidateRaw, err := candidate.Encode()
	if err != nil {
		t.Fatal(err)
	}
	tamperedRaw := bytes.Replace(candidateRaw, []byte(`"pack_id":"knowledge-en"`), []byte(`"pack_id":"knowledge-new"`), 1)
	tampered, err := Load(bytes.NewReader(tamperedRaw))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateTransition(current, tampered, "openpack-developer", TransitionActivate); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("unrelated binding change accepted: %v", err)
	}
}

func TestValidateTransitionRollbackRejectsArbitraryOlderSnapshotThatIsNotDeltaParent(t *testing.T) {
	document, publicKey := validRegistryDocument(t)
	parent, err := Load(bytes.NewReader(marshalDocument(t, document)))
	if err != nil {
		t.Fatal(err)
	}
	deltaDigest := strings.Repeat("b", 64)
	current, err := parent.Promote("openpack-developer", Promotion{
		PackID: "developer-en", SigningKeyID: indexpack.KeyID(publicKey), Kind: indexpack.KindDelta,
		ManifestSHA256: deltaDigest, ParentManifestSHA256: document.Bindings[0].ManifestSHA256,
		Revision: 8, ManifestRecordCount: 3, ProjectionRecordCount: 9,
		CreatedAt: "2026-07-19T00:00:00Z", ExpiresAt: "2026-08-19T00:00:00Z",
		InstalledPath: filepath.Join(filepath.Dir(document.Bindings[0].InstalledPath), deltaDigest),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateTransition(current, parent, "openpack-developer", TransitionRollback); err != nil {
		t.Fatalf("exact delta parent rollback: %v", err)
	}

	arbitraryDocument := cloneRegistryDocument(document)
	arbitraryDigest := strings.Repeat("c", 64)
	arbitraryDocument.Bindings[0].ManifestSHA256 = arbitraryDigest
	arbitraryDocument.Bindings[0].Revision = 6
	arbitraryDocument.Bindings[0].CreatedAt = "2026-07-17T00:00:00Z"
	arbitraryDocument.Bindings[0].InstalledPath = filepath.Join(filepath.Dir(document.Bindings[0].InstalledPath), arbitraryDigest)
	arbitrary, err := build(arbitraryDocument)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateTransition(current, arbitrary, "openpack-developer", TransitionRollback); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("arbitrary older snapshot accepted as rollback: %v", err)
	}
}
