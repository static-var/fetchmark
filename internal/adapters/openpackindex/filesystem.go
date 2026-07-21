package openpackindex

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func prepareSecureRoot(raw string) (string, error) {
	root, err := cleanAbsolutePath(raw)
	if err != nil {
		return "", err
	}
	if err := inspectExistingComponents(filepath.Dir(root)); err != nil {
		return "", err
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", fmt.Errorf("create root: %w", err)
	}
	if err := inspectExistingComponents(root); err != nil {
		return "", err
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("%w: root must be a real directory", ErrUnsafePath)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("%w: root permissions must not grant group/world access", ErrUnsafePath)
	}
	if !trustedDirectoryOwner(info) {
		return "", fmt.Errorf("%w: root must be owned by the effective user or root", ErrUnsafePath)
	}
	return root, nil
}

func requireSecureExistingDirectory(raw string) (string, error) {
	path, err := cleanAbsolutePath(raw)
	if err != nil {
		return "", err
	}
	if err := inspectExistingComponents(path); err != nil {
		return "", err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("%w: expected a real directory", ErrUnsafePath)
	}
	if info.Mode().Perm()&0o022 != 0 {
		return "", fmt.Errorf("%w: directory is group/world writable", ErrUnsafePath)
	}
	if !trustedDirectoryOwner(info) {
		return "", fmt.Errorf("%w: directory must be owned by the effective user or root", ErrUnsafePath)
	}
	return path, nil
}

func cleanAbsolutePath(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || !filepath.IsAbs(trimmed) || filepath.Clean(trimmed) != trimmed {
		return "", fmt.Errorf("%w: clean absolute path is required", ErrUnsafePath)
	}
	canonical, err := canonicalizePlatformPrefix(trimmed)
	if err != nil {
		return "", fmt.Errorf("%w: canonicalize operating-system path prefix: %v", ErrUnsafePath, err)
	}
	return canonical, nil
}

func inspectExistingComponents(path string) error {
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
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
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

func ensurePrivateDirectory(path string) error {
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("open pack index: create %s: %w", path, err)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 || !trustedDirectoryOwner(info) {
		return fmt.Errorf("%w: %s must be a private real directory", ErrUnsafePath, path)
	}
	return nil
}

func readRegularFileAt(root, relative string, maximum uint64) ([]byte, error) {
	file, err := openRegularFileAt(root, relative, maximum)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, int64(maximum)+1))
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) > maximum {
		return nil, fmt.Errorf("%w: file exceeds %d bytes", ErrInstallLimit, maximum)
	}
	return data, nil
}

func validateBundleRoot(root *os.Root) error {
	if root == nil {
		return fmt.Errorf("%w: descriptor-anchored bundle root is required", ErrUnsafePath)
	}
	info, err := root.Lstat(".")
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 || !trustedDirectoryOwner(info) {
		return fmt.Errorf("%w: bundle root must be a trusted non-writable real directory", ErrUnsafePath)
	}
	return nil
}

func readRegularFileFromRoot(root *os.Root, relative string, maximum uint64) ([]byte, error) {
	file, err := openRegularFileFromRoot(root, relative, maximum)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, int64(maximum)+1))
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) > maximum {
		return nil, fmt.Errorf("%w: file exceeds %d bytes", ErrInstallLimit, maximum)
	}
	return data, nil
}

func openRegularFileFromRoot(root *os.Root, relative string, maximum uint64) (*os.File, error) {
	if root == nil || relative == "" || filepath.IsAbs(relative) || filepath.Clean(relative) != relative || strings.Contains(relative, "\\") {
		return nil, fmt.Errorf("%w: unsafe relative file path", ErrUnsafePath)
	}
	info, err := root.Lstat(filepath.FromSlash(relative))
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: %s must be a regular non-symlink file", ErrUnsafePath, relative)
	}
	if info.Size() < 0 || uint64(info.Size()) > maximum {
		return nil, fmt.Errorf("%w: file is %d bytes, maximum %d", ErrInstallLimit, info.Size(), maximum)
	}
	file, err := root.Open(filepath.FromSlash(relative))
	if err != nil {
		return nil, err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) || !opened.Mode().IsRegular() {
		file.Close()
		return nil, fmt.Errorf("%w: file changed while opening", ErrUnsafePath)
	}
	return file, nil
}

func openRegularFileAt(root, relative string, maximum uint64) (*os.File, error) {
	if relative == "" || filepath.IsAbs(relative) || filepath.Clean(relative) != relative || strings.Contains(relative, "\\") {
		return nil, fmt.Errorf("%w: unsafe relative file path", ErrUnsafePath)
	}
	path := filepath.Join(root, filepath.FromSlash(relative))
	if !pathWithin(root, path) {
		return nil, fmt.Errorf("%w: file escapes bundle", ErrUnsafePath)
	}
	if err := inspectExistingComponents(filepath.Dir(path)); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: %s must be a regular non-symlink file", ErrUnsafePath, path)
	}
	if info.Size() < 0 || uint64(info.Size()) > maximum {
		return nil, fmt.Errorf("%w: file is %d bytes, maximum %d", ErrInstallLimit, info.Size(), maximum)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) || !opened.Mode().IsRegular() {
		file.Close()
		return nil, fmt.Errorf("%w: file changed while opening", ErrUnsafePath)
	}
	return file, nil
}

func pathWithin(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
