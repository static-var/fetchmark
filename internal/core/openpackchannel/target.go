// Package openpackchannel defines the Fetchmark-specific target metadata used
// inside a TUF repository. TUF authenticates and orders channel metadata; this
// package binds one selected TUF target to one exact open-pack manifest.
package openpackchannel

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

const Schema = 1

var (
	ErrInvalidTarget = errors.New("open pack channel: invalid target")
	digestPattern    = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// Target is the application metadata carried in a TUF target's custom field.
// The digest pins the exact manifest bytes; the other fields make selection
// inspectable without weakening the manifest binding.
type Target struct {
	Schema               int            `json:"schema"`
	PackID               string         `json:"pack_id"`
	Kind                 indexpack.Kind `json:"kind"`
	Revision             uint64         `json:"revision"`
	ManifestSHA256       string         `json:"manifest_sha256"`
	ParentManifestSHA256 string         `json:"parent_manifest_sha256,omitempty"`
}

type customEnvelope struct {
	FetchmarkOpenPack Target `json:"fetchmark_open_pack"`
}

// EncodeCustom returns deterministic application metadata for a TUF target.
// The result passes through the same strict decoder used by consumers so the
// publisher cannot emit a shape the selector would interpret differently.
func EncodeCustom(target Target) (json.RawMessage, error) {
	raw, err := json.Marshal(customEnvelope{FetchmarkOpenPack: target})
	if err != nil {
		return nil, invalid(err)
	}
	message := json.RawMessage(raw)
	if _, err := DecodeCustom(&message); err != nil {
		return nil, err
	}
	return message, nil
}

// DecodeCustom parses a strict Fetchmark TUF custom field. Unknown and
// duplicate fields fail closed so publishers cannot create ambiguous views.
func DecodeCustom(raw *json.RawMessage) (Target, error) {
	if raw == nil || len(*raw) == 0 {
		return Target{}, invalid(errors.New("fetchmark_open_pack custom metadata is required"))
	}
	var envelope customEnvelope
	if err := indexpack.DecodeStrictJSON(*raw, &envelope); err != nil {
		return Target{}, invalid(err)
	}
	target := envelope.FetchmarkOpenPack
	if target.Schema != Schema {
		return Target{}, invalid(fmt.Errorf("schema must be %d", Schema))
	}
	if !digestPattern.MatchString(target.ManifestSHA256) {
		return Target{}, invalid(errors.New("manifest_sha256 must be a lowercase SHA-256 digest"))
	}
	if target.Kind == indexpack.KindSnapshot {
		if target.ParentManifestSHA256 != "" {
			return Target{}, invalid(errors.New("snapshot target must not declare parent_manifest_sha256"))
		}
	} else if target.Kind == indexpack.KindDelta {
		if !digestPattern.MatchString(target.ParentManifestSHA256) {
			return Target{}, invalid(errors.New("delta target requires parent_manifest_sha256"))
		}
	} else {
		return Target{}, invalid(errors.New("kind must be snapshot or delta"))
	}
	if target.PackID == "" || target.Revision == 0 {
		return Target{}, invalid(errors.New("pack_id and revision are required"))
	}
	return target, nil
}

// VerifyManifest checks that raw is the exact valid manifest selected by the
// channel and that it is currently usable. Pack signature and shard checks are
// deliberately retained by fetchmark-pack verify/install.
func (target Target) VerifyManifest(raw []byte, now time.Time) (indexpack.Manifest, error) {
	manifest, err := indexpack.DecodeManifest(raw)
	if err != nil {
		return indexpack.Manifest{}, invalid(err)
	}
	if indexpack.ManifestDigest(raw) != target.ManifestSHA256 {
		return indexpack.Manifest{}, invalid(errors.New("manifest digest does not match custom metadata"))
	}
	if manifest.PackID != target.PackID || manifest.Kind != target.Kind || manifest.Revision != target.Revision ||
		manifest.ParentManifestSHA256 != target.ParentManifestSHA256 {
		return indexpack.Manifest{}, invalid(errors.New("manifest identity does not match custom metadata"))
	}
	created, _ := time.Parse(time.RFC3339, manifest.CreatedAt)
	expires, _ := time.Parse(time.RFC3339, manifest.ExpiresAt)
	now = now.UTC()
	if now.Before(created) {
		return indexpack.Manifest{}, invalid(errors.New("manifest is not yet valid"))
	}
	if !now.Before(expires) {
		return indexpack.Manifest{}, invalid(errors.New("manifest is expired"))
	}
	return manifest, nil
}

func invalid(err error) error {
	return fmt.Errorf("%w: %v", ErrInvalidTarget, err)
}
