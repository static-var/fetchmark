// Package federationconfigfile loads federation trust and private identity
// documents through a fail-closed local-filesystem boundary.
package federationconfigfile

import (
	"bytes"
	"fmt"

	"github.com/staticvar/fetchmark/internal/adapters/secureconfigfile"
	"github.com/staticvar/fetchmark/internal/core/federation"
)

var ErrUnsafePath = secureconfigfile.ErrUnsafePath

// LoadTrustRegistry loads public operator trust configuration. The file may be
// readable by group or world but must not be writable by either.
func LoadTrustRegistry(rawPath string) (federation.TrustRegistry, error) {
	raw, err := secureconfigfile.Read(rawPath, secureconfigfile.Options{
		MaxBytes: federation.MaxTrustRegistryBytes,
		Mode:     secureconfigfile.PublicConfig,
	})
	if err != nil {
		return federation.TrustRegistry{}, fmt.Errorf("federation trust registry file: %w", err)
	}
	registry, err := federation.LoadTrustRegistry(bytes.NewReader(raw))
	if err != nil {
		return federation.TrustRegistry{}, err
	}
	return registry, nil
}

// LoadIdentity loads a private local signing identity. The file must be owned
// by the effective user and have mode 0600 exactly.
func LoadIdentity(rawPath string) (federation.Identity, error) {
	raw, err := secureconfigfile.Read(rawPath, secureconfigfile.Options{
		MaxBytes: federation.MaxIdentityBytes,
		Mode:     secureconfigfile.PrivateIdentity,
	})
	if err != nil {
		return federation.Identity{}, fmt.Errorf("federation identity file: %w", err)
	}
	identity, err := federation.DecodeIdentity(raw)
	if err != nil {
		return federation.Identity{}, err
	}
	return identity, nil
}
