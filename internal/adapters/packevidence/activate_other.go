//go:build !darwin && !linux

package packevidence

import (
	"errors"
	"os"
)

func renameNoReplaceAt(_ *os.File, _, _ string) error {
	return errors.New("pack evidence collector: atomic no-replace activation is unsupported on this platform")
}
