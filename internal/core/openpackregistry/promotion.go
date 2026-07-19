package openpackregistry

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

var ErrInvalidPromotion = errors.New("open pack registry: invalid promotion")

// Promotion describes one already channel-selected and independently verified
// revision for an existing operator-trusted source. It deliberately cannot
// add a publisher, rotate a key, rename a pack, or move the object store.
type Promotion struct {
	PackID                string
	SigningKeyID          string
	Kind                  indexpack.Kind
	ManifestSHA256        string
	ParentManifestSHA256  string
	Revision              uint64
	ManifestRecordCount   uint64
	ProjectionRecordCount uint64
	CreatedAt             string
	ExpiresAt             string
	InstalledPath         string
}

// Promote returns a new version-2 registry with one existing source advanced.
// The receiver remains unchanged. Rollback is intentionally not expressed by
// this operation; operators retain and explicitly restore an older registry.
func (registry Registry) Promote(sourceID string, promotion Promotion) (Registry, error) {
	current, exists := registry.bindings[sourceID]
	if !exists {
		return Registry{}, invalidPromotion(errors.New("source is not defined"))
	}
	if promotion.PackID != current.PackID {
		return Registry{}, invalidPromotion(errors.New("pack ID must match the current binding"))
	}
	if promotion.SigningKeyID != current.SigningKeyID {
		return Registry{}, invalidPromotion(errors.New("signing key must match the current publisher binding"))
	}
	if promotion.ManifestSHA256 == current.ManifestSHA256 || promotion.Revision <= current.Revision {
		return Registry{}, invalidPromotion(errors.New("revision and manifest must advance"))
	}
	currentCreated, err := parseTimestamp(current.CreatedAt)
	if err != nil {
		return Registry{}, invalidPromotion(err)
	}
	promotionCreated, err := parseTimestamp(promotion.CreatedAt)
	if err != nil || !promotionCreated.After(currentCreated) {
		return Registry{}, invalidPromotion(errors.New("created_at must advance"))
	}
	if filepath.Dir(promotion.InstalledPath) != filepath.Dir(current.InstalledPath) {
		return Registry{}, invalidPromotion(errors.New("installed object directory must not change"))
	}
	switch promotion.Kind {
	case indexpack.KindSnapshot:
		if promotion.ParentManifestSHA256 != "" {
			return Registry{}, invalidPromotion(errors.New("snapshot must not have a parent"))
		}
	case indexpack.KindDelta:
		if current.Kind != indexpack.KindSnapshot || promotion.ParentManifestSHA256 != current.ManifestSHA256 {
			return Registry{}, invalidPromotion(errors.New("delta must name the exact current snapshot parent"))
		}
	default:
		return Registry{}, invalidPromotion(errors.New("kind must be snapshot or delta"))
	}

	candidateDocument := registryJSON{
		Version:    VersionWithDeltas,
		Publishers: append([]publisherJSON(nil), registry.document.Publishers...),
		Bindings:   make([]bindingJSON, 0, len(registry.document.Bindings)),
	}
	for _, existingDocument := range registry.document.Bindings {
		existing := registry.bindings[existingDocument.SourceID]
		candidate := bindingDocument(existing)
		if existing.SourceID == sourceID {
			candidate = bindingJSON{
				SourceID: sourceID, PackID: current.PackID, PublisherID: current.PublisherID,
				ManifestSHA256: promotion.ManifestSHA256, Kind: promotion.Kind,
				ParentManifestSHA256: promotion.ParentManifestSHA256, Revision: promotion.Revision,
				ManifestRecordCount:   promotion.ManifestRecordCount,
				ProjectionRecordCount: promotion.ProjectionRecordCount,
				CreatedAt:             promotion.CreatedAt, ExpiresAt: promotion.ExpiresAt,
				InstalledPath: promotion.InstalledPath,
			}
		}
		candidateDocument.Bindings = append(candidateDocument.Bindings, candidate)
	}
	candidate, err := build(candidateDocument)
	if err != nil {
		return Registry{}, invalidPromotion(err)
	}
	return candidate, nil
}

// Encode returns the deterministic strict JSON representation of a validated
// registry candidate. The trailing newline is part of the operator artifact.
func (registry Registry) Encode() ([]byte, error) {
	if registry.document.Version != Version && registry.document.Version != VersionWithDeltas {
		return nil, invalid(errors.New("registry is not initialized"))
	}
	raw, err := json.Marshal(registry.document)
	if err != nil {
		return nil, invalid(err)
	}
	raw = append(raw, '\n')
	if len(raw) > MaxRegistryBytes {
		return nil, invalid(fmt.Errorf("encoded size exceeds %d bytes", MaxRegistryBytes))
	}
	return raw, nil
}

func bindingDocument(binding Binding) bindingJSON {
	return bindingJSON{
		SourceID: binding.SourceID, PackID: binding.PackID, PublisherID: binding.PublisherID,
		ManifestSHA256: binding.ManifestSHA256, Kind: binding.Kind,
		ParentManifestSHA256: binding.ParentManifestSHA256, Revision: binding.Revision,
		ManifestRecordCount: binding.ManifestRecordCount, ProjectionRecordCount: binding.RecordCount,
		CreatedAt: binding.CreatedAt, ExpiresAt: binding.ExpiresAt, InstalledPath: binding.InstalledPath,
	}
}

func cloneRegistryDocument(document registryJSON) registryJSON {
	return registryJSON{
		Version:    document.Version,
		Publishers: append([]publisherJSON(nil), document.Publishers...),
		Bindings:   append([]bindingJSON(nil), document.Bindings...),
	}
}

func invalidPromotion(err error) error {
	return fmt.Errorf("%w: %v", ErrInvalidPromotion, err)
}
