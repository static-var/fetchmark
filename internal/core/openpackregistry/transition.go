package openpackregistry

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
)

var ErrInvalidTransition = errors.New("open pack registry: invalid transition")

type TransitionMode string

const (
	TransitionActivate TransitionMode = "activate"
	TransitionRollback TransitionMode = "rollback"
)

// Transition describes the one source binding changed by an audited registry
// activation. It contains no filesystem behavior.
type Transition struct {
	Mode     TransitionMode
	SourceID string
	From     Binding
	To       Binding
}

// ValidateTransition proves that publisher trust and every unrelated source
// remain unchanged. Forward activation must be the exact deterministic result
// of Promote. Explicit rollback may only select an older retained binding for
// the same publisher, pack, signing key, and object-store root.
func ValidateTransition(current, candidate Registry, sourceID string, mode TransitionMode) (Transition, error) {
	if mode != TransitionActivate && mode != TransitionRollback {
		return Transition{}, invalidTransition(errors.New("mode must be activate or rollback"))
	}
	from, fromFound := current.bindings[sourceID]
	to, toFound := candidate.bindings[sourceID]
	if !fromFound || !toFound {
		return Transition{}, invalidTransition(errors.New("source must exist in both registries"))
	}
	if len(current.bindings) != len(candidate.bindings) || len(current.trustedKey) != len(candidate.trustedKey) {
		return Transition{}, invalidTransition(errors.New("registry source and publisher sets must not change"))
	}
	for keyID, currentKey := range current.trustedKey {
		candidateKey, found := candidate.trustedKey[keyID]
		if !found || !bytes.Equal(currentKey, candidateKey) {
			return Transition{}, invalidTransition(errors.New("publisher trust must not change"))
		}
	}
	changed := 0
	for existingSource, existing := range current.bindings {
		next, found := candidate.bindings[existingSource]
		if !found {
			return Transition{}, invalidTransition(errors.New("source set must not change"))
		}
		if existing != next {
			changed++
			if existingSource != sourceID {
				return Transition{}, invalidTransition(errors.New("an unrelated source binding changed"))
			}
		}
	}
	if changed != 1 || from == to {
		return Transition{}, invalidTransition(errors.New("exactly the selected source must change"))
	}

	transition := Transition{Mode: mode, SourceID: sourceID, From: from, To: to}
	if mode == TransitionActivate {
		expected, err := current.Promote(sourceID, promotionFromBinding(to))
		if err != nil {
			return Transition{}, invalidTransition(err)
		}
		expectedRaw, err := expected.Encode()
		if err != nil {
			return Transition{}, invalidTransition(err)
		}
		candidateRaw, err := candidate.Encode()
		if err != nil || !bytes.Equal(expectedRaw, candidateRaw) {
			return Transition{}, invalidTransition(errors.New("candidate is not the exact deterministic promotion"))
		}
		return transition, nil
	}

	if filepath.Dir(to.InstalledPath) != filepath.Dir(from.InstalledPath) {
		return Transition{}, invalidTransition(errors.New("rollback object root must not change"))
	}
	// A rollback is accepted only when the current registry is the exact
	// deterministic forward promotion of the retained destination registry.
	// This preserves every forward trust and delta-parent invariant in reverse.
	replayed, err := candidate.Promote(sourceID, promotionFromBinding(from))
	if err != nil {
		return Transition{}, invalidTransition(errors.New("rollback is not the reverse of a valid promotion"))
	}
	replayedRaw, err := replayed.Encode()
	if err != nil {
		return Transition{}, invalidTransition(err)
	}
	currentRaw, err := current.Encode()
	if err != nil || !bytes.Equal(replayedRaw, currentRaw) {
		return Transition{}, invalidTransition(errors.New("rollback is not the exact reverse transition"))
	}
	return transition, nil
}

func promotionFromBinding(binding Binding) Promotion {
	return Promotion{
		PackID: binding.PackID, SigningKeyID: binding.SigningKeyID, Kind: binding.Kind,
		ManifestSHA256: binding.ManifestSHA256, ParentManifestSHA256: binding.ParentManifestSHA256,
		Revision: binding.Revision, ManifestRecordCount: binding.ManifestRecordCount,
		ProjectionRecordCount: binding.RecordCount, CreatedAt: binding.CreatedAt,
		ExpiresAt: binding.ExpiresAt, InstalledPath: binding.InstalledPath,
	}
}

func invalidTransition(cause error) error {
	return fmt.Errorf("%w: %v", ErrInvalidTransition, cause)
}
