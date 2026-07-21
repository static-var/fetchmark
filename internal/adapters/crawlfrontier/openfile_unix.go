//go:build darwin || linux

package crawlfrontier

import (
	"os"
	"syscall"
)

func openNoFollow(name string, flag int, mode os.FileMode) (*os.File, error) {
	fd, err := syscall.Open(name, flag|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, uint32(mode.Perm()))
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		_ = syscall.Close(fd)
		return nil, syscall.EINVAL
	}
	return file, nil
}
