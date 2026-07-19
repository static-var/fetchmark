//go:build !darwin && !linux

package crawlfrontier

import "os"

func openNoFollow(name string, flag int, mode os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, mode)
}
