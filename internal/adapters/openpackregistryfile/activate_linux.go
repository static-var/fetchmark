//go:build linux

package openpackregistryfile

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func renameCandidateNoReplaceAt(directory *os.File, source, destination string) error {
	fd := int(directory.Fd())
	return unix.Renameat2(fd, source, fd, destination, unix.RENAME_NOREPLACE)
}

func replaceRegistryAt(directory *os.File, source, destination string) error {
	fd := int(directory.Fd())
	return unix.Renameat(fd, source, fd, destination)
}

func tryLockRegistryFile(file *os.File) (bool, error) {
	err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) {
		return false, nil
	}
	return err == nil, err
}

func unlockRegistryFile(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}
