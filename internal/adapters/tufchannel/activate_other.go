//go:build !darwin && !linux

package tufchannel

import (
	"errors"
	"os"
)

func renameNoReplaceAt(_ *os.File, _, _ string) error {
	return errors.New("TUF channel retrieval: atomic descriptor-anchored no-replace activation is unsupported on this platform")
}
