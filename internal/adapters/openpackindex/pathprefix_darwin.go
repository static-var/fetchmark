//go:build darwin

package openpackindex

import (
	"fmt"
	"path/filepath"
	"strings"
)

// macOS exposes a small set of root compatibility aliases managed by the OS
// itself. Resolve only those fixed aliases; arbitrary symlink components below
// the canonical prefix remain forbidden by inspectExistingComponents.
func canonicalizePlatformPrefix(path string) (string, error) {
	aliases := []struct {
		alias  string
		target string
	}{{"/var", "/private/var"}, {"/tmp", "/private/tmp"}}
	for _, candidate := range aliases {
		if path != candidate.alias && !strings.HasPrefix(path, candidate.alias+string(filepath.Separator)) {
			continue
		}
		resolved, err := filepath.EvalSymlinks(candidate.alias)
		if err != nil {
			return "", err
		}
		if resolved != candidate.target {
			return "", fmt.Errorf("unexpected %s target %s", candidate.alias, resolved)
		}
		return filepath.Clean(candidate.target + strings.TrimPrefix(path, candidate.alias)), nil
	}
	return path, nil
}
