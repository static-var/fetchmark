package tufrepository

import (
	"errors"
	"fmt"
	"time"

	"github.com/theupdateframework/go-tuf/v2/metadata"
)

const MaxRootRotations = 32

// validateSignedRootTransition proves one complete TUF root update under both
// the previous and successor root thresholds. It deliberately validates the
// fixed Fetchmark profile before invoking signature verification so malformed
// key bindings cannot reach library dereferences.
func validateSignedRootTransition(
	current *metadata.Metadata[metadata.RootType],
	successorRaw []byte,
	now time.Time,
) (*metadata.Metadata[metadata.RootType], error) {
	if current == nil {
		return nil, invalidRootRotation(errors.New("current root is required"))
	}
	successor, err := metadata.Root().FromBytes(successorRaw)
	if err != nil {
		return nil, invalidRootRotation(err)
	}
	if err := validateRootOnlyTransition(current, successor); err != nil {
		return nil, err
	}
	if len(successor.Signatures) == 0 {
		return nil, invalidRootRotation(errors.New("signed successor root is required"))
	}
	if !now.IsZero() {
		if !exactRotationTime(now) || successor.Signed.Expires.Sub(now) < MinimumPublicationHorizon {
			return nil, invalidRootRotation(errors.New("successor root must remain fresh through the minimum rotation horizon"))
		}
	}
	if err := current.VerifyDelegate(metadata.ROOT, successor); err != nil {
		return nil, fmt.Errorf("%w: old root threshold: %v", ErrRootRotationThreshold, err)
	}
	if err := successor.VerifyDelegate(metadata.ROOT, successor); err != nil {
		return nil, fmt.Errorf("%w: new root threshold: %v", ErrRootRotationThreshold, err)
	}
	return successor, nil
}

func validateRootOnlyTransition(
	current, successor *metadata.Metadata[metadata.RootType],
) error {
	if err := validateFixedRootProfile(current); err != nil {
		return err
	}
	if err := validateFixedRootProfile(successor); err != nil {
		return err
	}
	if successor.Signed.Version != current.Signed.Version+1 ||
		successor.Signed.ConsistentSnapshot != current.Signed.ConsistentSnapshot ||
		successor.Signed.Type != current.Signed.Type || successor.Signed.SpecVersion != current.Signed.SpecVersion {
		return invalidRootRotation(errors.New("successor root must be sequential and preserve the repository profile"))
	}
	oldRootKeys := make(map[string]struct{}, 2)
	for _, keyID := range current.Signed.Roles[metadata.ROOT].KeyIDs {
		oldRootKeys[keyID] = struct{}{}
	}
	for _, keyID := range successor.Signed.Roles[metadata.ROOT].KeyIDs {
		if _, reused := oldRootKeys[keyID]; reused {
			return invalidRootRotation(errors.New("successor must fully rotate both root keys"))
		}
	}
	for _, roleName := range []string{metadata.TARGETS, metadata.SNAPSHOT, metadata.TIMESTAMP} {
		currentRole, successorRole := current.Signed.Roles[roleName], successor.Signed.Roles[roleName]
		if successorRole.Threshold != currentRole.Threshold ||
			!equalStrings(sortedStrings(successorRole.KeyIDs), sortedStrings(currentRole.KeyIDs)) {
			return invalidRootRotation(errors.New("successor must preserve every non-root role binding"))
		}
		for _, keyID := range currentRole.KeyIDs {
			if !equalMetadataKey(successor.Signed.Keys[keyID], current.Signed.Keys[keyID]) {
				return invalidRootRotation(errors.New("successor non-root key bytes differ"))
			}
		}
	}
	return nil
}

func validateRootChain(bootstrapRaw []byte, updates [][]byte, now time.Time) (*metadata.Metadata[metadata.RootType], error) {
	if len(updates) > MaxRootRotations {
		return nil, invalidRootRotation(fmt.Errorf("root chain exceeds %d rotations", MaxRootRotations))
	}
	current, err := decodeCurrentRoot(bootstrapRaw)
	if err != nil {
		return nil, err
	}
	for _, updateRaw := range updates {
		current, err = validateSignedRootTransition(current, updateRaw, now)
		if err != nil {
			return nil, err
		}
	}
	return current, nil
}
