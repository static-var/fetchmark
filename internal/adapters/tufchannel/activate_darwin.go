//go:build darwin

package tufchannel

import (
	"os"

	"golang.org/x/sys/unix"
)

func renameNoReplaceAt(directory *os.File, source, destination string) error {
	fd := int(directory.Fd())
	return unix.RenameatxNp(fd, source, fd, destination, unix.RENAME_EXCL)
}
