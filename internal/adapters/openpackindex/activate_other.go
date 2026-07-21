//go:build !darwin && !linux

package openpackindex

import "errors"

func renameNoReplace(_, _ string) error {
	return errors.New("atomic no-replace activation is unsupported on this platform")
}
