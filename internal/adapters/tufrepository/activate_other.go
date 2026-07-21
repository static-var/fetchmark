//go:build !darwin && !linux

package tufrepository

import (
	"errors"
	"os"
)

func renameNoReplaceAt(_ *os.File, _, _ string) error {
	return errors.New("TUF repository: atomic descriptor-anchored no-replace activation is unsupported on this platform")
}

func replaceRepositoryAt(_ *os.File, _, _ string) error {
	return errors.New("TUF repository: atomic descriptor-anchored replacement is unsupported on this platform")
}

func tryLockRepositoryFile(_ *os.File) (bool, error) {
	return false, errors.New("TUF repository: advisory mirror locking is unsupported on this platform")
}

func unlockRepositoryFile(_ *os.File) error { return nil }
