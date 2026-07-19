//go:build darwin || linux

package localartifact

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

type storeOwnership struct {
	file *os.File
}

func acquireStoreOwnership(root *os.Root) (*storeOwnership, error) {
	const lockPath = ".fetchmark.lock"
	file, err := root.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("local artifact: open ownership lock: %w", err)
	}
	closeWithError := func(err error) (*storeOwnership, error) {
		_ = file.Close()
		return nil, err
	}
	info, err := root.Lstat(lockPath)
	if err != nil {
		return closeWithError(fmt.Errorf("local artifact: inspect ownership lock: %w", err))
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return closeWithError(errors.New("local artifact: ownership lock must be a regular file"))
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return closeWithError(ErrStoreInUse)
		}
		return closeWithError(fmt.Errorf("local artifact: acquire ownership lock: %w", err))
	}
	return &storeOwnership{file: file}, nil
}

func (ownership *storeOwnership) Close() error {
	if ownership == nil || ownership.file == nil {
		return nil
	}
	file := ownership.file
	ownership.file = nil
	return errors.Join(syscall.Flock(int(file.Fd()), syscall.LOCK_UN), file.Close())
}
