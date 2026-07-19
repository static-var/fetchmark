//go:build !darwin && !linux

package packevidencegate

import "os"

func openNoFollow(name string, flag int, mode os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, mode)
}

func ownedByEffectiveUser(info os.FileInfo) bool { return info != nil }
