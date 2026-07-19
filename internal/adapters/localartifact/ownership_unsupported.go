//go:build !darwin && !linux

package localartifact

import (
	"errors"
	"os"
)

type storeOwnership struct{}

func acquireStoreOwnership(*os.Root) (*storeOwnership, error) {
	return nil, errors.New("local artifact: persistent store ownership is unsupported on this platform")
}

func (*storeOwnership) Close() error { return nil }
