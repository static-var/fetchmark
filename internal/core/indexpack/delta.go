package indexpack

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"time"
)

var ErrInvalidDeltaTransition = errors.New("index pack: invalid delta transition")

// ValidateDeltaTransition binds one verified delta to one exact verified
// snapshot. The first consumer deliberately materializes a fresh projection
// from that snapshot and does not accept delta chains.
func ValidateDeltaTransition(parent, delta VerifiedManifest) error {
	reject := func(reason string) error {
		return fmt.Errorf("%w: %s", ErrInvalidDeltaTransition, reason)
	}
	if err := parent.Manifest.validate(); err != nil {
		return reject("parent manifest is invalid")
	}
	if err := delta.Manifest.validate(); err != nil {
		return reject("delta manifest is invalid")
	}
	if err := validateDigest(parent.Digest); err != nil || parent.KeyID != parent.Manifest.SigningKeyID {
		return reject("parent verification identity is invalid")
	}
	if err := validateDigest(delta.Digest); err != nil || delta.KeyID != delta.Manifest.SigningKeyID {
		return reject("delta verification identity is invalid")
	}
	if parent.Manifest.Kind != KindSnapshot {
		return reject("parent must be a snapshot")
	}
	if delta.Manifest.Kind != KindDelta {
		return reject("target must be a delta")
	}
	if delta.Manifest.ParentManifestSHA256 != parent.Digest {
		return reject("parent manifest digest does not match")
	}
	if parent.Manifest.Revision == math.MaxUint64 || delta.Manifest.Revision != parent.Manifest.Revision+1 {
		return reject("delta revision must immediately follow the parent")
	}
	if delta.Manifest.Version != parent.Manifest.Version {
		return reject("manifest version changed")
	}
	if delta.Manifest.PackID != parent.Manifest.PackID {
		return reject("pack ID changed")
	}
	if delta.Manifest.Publisher != parent.Manifest.Publisher {
		return reject("publisher metadata changed")
	}
	if delta.KeyID != parent.KeyID {
		return reject("signing key changed")
	}
	if !slices.Equal(delta.Manifest.Languages, parent.Manifest.Languages) {
		return reject("languages changed")
	}
	if delta.Manifest.Policy != parent.Manifest.Policy {
		return reject("pack policy changed")
	}
	parentCreated, _ := time.Parse(time.RFC3339, parent.Manifest.CreatedAt)
	parentExpires, _ := time.Parse(time.RFC3339, parent.Manifest.ExpiresAt)
	deltaCreated, _ := time.Parse(time.RFC3339, delta.Manifest.CreatedAt)
	deltaExpires, _ := time.Parse(time.RFC3339, delta.Manifest.ExpiresAt)
	if deltaCreated.Before(parentCreated) || !deltaCreated.Before(parentExpires) {
		return reject("delta creation is outside the parent validity window")
	}
	if deltaExpires.After(parentExpires) {
		return reject("delta expiry extends the parent validity window")
	}
	return nil
}
