// Package secureconfigfile reads bounded, operator-owned configuration files
// through a fail-closed local-filesystem boundary.
package secureconfigfile

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var ErrUnsafePath = errors.New("secure config file: unsafe path")

// ModePolicy defines the accepted permissions for the final file.
type ModePolicy uint8

const (
	// PublicConfig permits read bits for group or world, but never write bits.
	PublicConfig ModePolicy = iota + 1
	// PrivateIdentity requires mode 0600 exactly.
	PrivateIdentity
)

// Options constrains a secure file read. MaxBytes must be positive. Empty
// files are rejected because neither configuration nor identity files have a
// useful empty representation.
type Options struct {
	MaxBytes int64
	Mode     ModePolicy
}

// ValidateDirectory returns the canonical path of an existing directory after
// verifying that every component is a real directory with a trusted owner and
// safe write permissions. It is intended for descriptor-anchored writers that
// need the same local-filesystem boundary as secure configuration reads.
func ValidateDirectory(rawPath string) (string, error) {
	path, err := cleanAbsolutePath(rawPath)
	if err != nil {
		return "", err
	}
	if err := inspectDirectoryComponents(path); err != nil {
		return "", err
	}
	return path, nil
}

// CreatePrivate atomically claims a new clean absolute path and writes one
// owner-only private configuration file. It never overwrites an existing
// path. The parent directory must already exist and satisfy the same real,
// non-writable component policy used by Read.
func CreatePrivate(rawPath string, raw []byte, maxBytes int64) (err error) {
	if maxBytes < 1 || len(raw) == 0 || int64(len(raw)) > maxBytes {
		return fmt.Errorf("secure config file: size must be 1..%d bytes", maxBytes)
	}
	path, err := cleanAbsolutePath(rawPath)
	if err != nil {
		return err
	}
	if err := inspectDirectoryComponents(filepath.Dir(path)); err != nil {
		return err
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("secure config file: create without overwrite: %w", err)
	}
	created, statErr := file.Stat()
	cleanup := true
	defer func() {
		if !cleanup {
			return
		}
		_ = file.Close()
		current, currentErr := os.Lstat(path)
		if statErr == nil && currentErr == nil && os.SameFile(created, current) {
			_ = os.Remove(path)
		}
	}()

	if statErr != nil || !created.Mode().IsRegular() || !ownedByEffectiveUser(created) {
		return fmt.Errorf("%w: newly created identity file is not a regular owner-owned file", ErrUnsafePath)
	}
	if err := file.Chmod(0o600); err != nil {
		return fmt.Errorf("secure config file: set private mode: %w", err)
	}
	if _, err := io.Copy(file, bytes.NewReader(raw)); err != nil {
		return fmt.Errorf("secure config file: write private file: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("secure config file: sync private file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("secure config file: close private file: %w", err)
	}
	verification, err := Read(rawPath, Options{MaxBytes: maxBytes, Mode: PrivateIdentity})
	if err != nil {
		return fmt.Errorf("secure config file: verify private file: %w", err)
	}
	if !bytes.Equal(raw, verification) {
		return fmt.Errorf("%w: private file changed after creation", ErrUnsafePath)
	}
	cleanup = false
	return nil
}

// Read returns a stable, bounded snapshot of a file at a clean absolute path.
// Every directory component must be a real directory and must not be group- or
// world-writable unless it has the sticky bit (for operating-system temporary
// roots). The final file must be regular, owned by the effective user, and
// satisfy the requested mode policy. File contents are never included in an
// error.
func Read(rawPath string, options Options) ([]byte, error) {
	if options.MaxBytes < 1 {
		return nil, fmt.Errorf("secure config file: invalid maximum size")
	}
	if options.Mode != PublicConfig && options.Mode != PrivateIdentity {
		return nil, fmt.Errorf("secure config file: invalid mode policy")
	}

	path, err := cleanAbsolutePath(rawPath)
	if err != nil {
		return nil, err
	}
	if err := inspectDirectoryComponents(filepath.Dir(path)); err != nil {
		return nil, err
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("secure config file: inspect: %w", err)
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: file must be regular and not a symlink", ErrUnsafePath)
	}
	if err := validateMode(before.Mode().Perm(), options.Mode); err != nil {
		return nil, err
	}
	if !ownedByEffectiveUser(before) {
		return nil, fmt.Errorf("%w: file must be owned by the effective user", ErrUnsafePath)
	}
	if before.Size() < 1 || before.Size() > options.MaxBytes {
		return nil, fmt.Errorf("secure config file: size must be 1..%d bytes", options.MaxBytes)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("secure config file: open: %w", err)
	}
	defer file.Close()

	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return nil, fmt.Errorf("%w: file changed while opening", ErrUnsafePath)
	}
	raw, err := io.ReadAll(io.LimitReader(file, options.MaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("secure config file: read: %w", err)
	}
	if int64(len(raw)) > options.MaxBytes {
		return nil, fmt.Errorf("secure config file: size must be 1..%d bytes", options.MaxBytes)
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(opened, after) || opened.Size() != after.Size() || !opened.ModTime().Equal(after.ModTime()) || int64(len(raw)) != after.Size() {
		return nil, fmt.Errorf("%w: file changed while reading", ErrUnsafePath)
	}

	// Re-read through the same descriptor. This catches an in-place rewrite
	// that preserves size and timestamp during the first read.
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("secure config file: verify read: %w", err)
	}
	verification, err := io.ReadAll(io.LimitReader(file, options.MaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("secure config file: verify read: %w", err)
	}
	verified, err := file.Stat()
	if err != nil || !os.SameFile(after, verified) || after.Size() != verified.Size() || !after.ModTime().Equal(verified.ModTime()) || !bytes.Equal(raw, verification) {
		return nil, fmt.Errorf("%w: file changed while verifying", ErrUnsafePath)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("secure config file: close: %w", err)
	}
	return raw, nil
}

func validateMode(mode os.FileMode, policy ModePolicy) error {
	switch policy {
	case PublicConfig:
		if mode&0o022 != 0 {
			return fmt.Errorf("%w: file must not be writable by group or world", ErrUnsafePath)
		}
	case PrivateIdentity:
		if mode != 0o600 {
			return fmt.Errorf("%w: private identity file mode must be 0600", ErrUnsafePath)
		}
	}
	return nil
}

func cleanAbsolutePath(raw string) (string, error) {
	if raw == "" || strings.TrimSpace(raw) != raw || !filepath.IsAbs(raw) || filepath.Clean(raw) != raw {
		return "", fmt.Errorf("%w: clean absolute path is required", ErrUnsafePath)
	}
	path, err := canonicalizePlatformPrefix(raw)
	if err != nil {
		return "", fmt.Errorf("%w: canonicalize operating-system path prefix: %v", ErrUnsafePath, err)
	}
	return path, nil
}

func inspectDirectoryComponents(path string) error {
	path = filepath.Clean(path)
	volume := filepath.VolumeName(path)
	rest := strings.TrimPrefix(path, volume)
	current := volume + string(filepath.Separator)
	for _, component := range strings.Split(strings.TrimPrefix(rest, string(filepath.Separator)), string(filepath.Separator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("%w: inspect %s: %v", ErrUnsafePath, current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("%w: path component %s is not a real directory", ErrUnsafePath, current)
		}
		if !trustedDirectoryOwner(info) {
			return fmt.Errorf("%w: path component %s has an untrusted owner", ErrUnsafePath, current)
		}
		if info.Mode().Perm()&0o022 != 0 && !allowedStickyTemporaryRoot(current, info) {
			return fmt.Errorf("%w: path component %s is writable by group/world and is not an operating-system temporary root", ErrUnsafePath, current)
		}
	}
	return nil
}
