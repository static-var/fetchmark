//go:build !darwin && !linux

package openpackbuilder

import (
	"errors"
	"os"
)

func renameNoReplaceAt(_ *os.File, _, _ string) error {
	return errors.New("atomic descriptor-anchored no-replace activation is unsupported on this platform")
}
