// Package buildidentity fingerprints the Fetchmark executable file resolved
// for the process at startup. This is build-artifact evidence, not a mapped
// process-image, source-revision, or configuration digest.
package buildidentity

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
)

// HeaderName carries the SHA-256 digest of the resolved executable file.
const HeaderName = "X-Fetchmark-Build-SHA256"

// Parse validates a SHA-256 digest and returns its canonical lowercase form.
func Parse(raw string) (string, bool) {
	value := strings.TrimSpace(raw)
	if len(value) != sha256.Size*2 {
		return "", false
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return "", false
	}
	return strings.ToLower(value), true
}

// CurrentExecutableSHA256 hashes the executable file resolved for this process.
func CurrentExecutableSHA256() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve executable: %w", err)
	}
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open executable: %w", err)
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("hash executable: %w", err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
