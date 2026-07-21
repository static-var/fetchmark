// Package openpackregistryfile loads an operator-owned open-pack trust
// registry through a fail-closed local-filesystem boundary.
package openpackregistryfile

import (
	"bytes"
	"fmt"

	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/openpackregistry"
)

var ErrUnsafePath = secureconfigfile.ErrUnsafePath

// Load opens, verifies, and parses a registry at a clean absolute path. The
// file must be owned by the current effective user, must not be writable by
// group or world, and may not be reached through arbitrary symlinks.
func Load(rawPath string) (openpackregistry.Registry, error) {
	registry, _, err := LoadWithDigest(rawPath)
	return registry, err
}

// LoadWithDigest returns the validated registry and the SHA-256 of the exact
// stable bytes read from disk. The digest is suitable for a later explicit
// compare-and-swap activation step.
func LoadWithDigest(rawPath string) (openpackregistry.Registry, string, error) {
	raw, err := secureconfigfile.Read(rawPath, secureconfigfile.Options{
		MaxBytes: openpackregistry.MaxRegistryBytes,
		Mode:     secureconfigfile.PublicConfig,
	})
	if err != nil {
		return openpackregistry.Registry{}, "", fmt.Errorf("open pack registry file: %w", err)
	}
	registry, err := openpackregistry.Load(bytes.NewReader(raw))
	if err != nil {
		return openpackregistry.Registry{}, "", err
	}
	return registry, indexpack.Digest(raw), nil
}
