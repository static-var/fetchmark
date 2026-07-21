//go:build !darwin && !linux

package openpackregistryfile

import (
	"errors"
	"os"
)

func renameCandidateNoReplaceAt(_ *os.File, _, _ string) error {
	return errors.New("open pack registry file: atomic no-replace activation is unsupported on this platform")
}

func replaceRegistryAt(_ *os.File, _, _ string) error {
	return errors.New("open pack registry file: atomic registry replacement is unsupported on this platform")
}

func tryLockRegistryFile(_ *os.File) (bool, error) {
	return false, errors.New("open pack registry file: advisory registry locking is unsupported on this platform")
}

func unlockRegistryFile(_ *os.File) error { return nil }
