//go:build !darwin && !linux && !freebsd && !openbsd && !netbsd && !dragonfly

package secureconfigfile

import "os"

func ownedByEffectiveUser(os.FileInfo) bool {
	return false
}

func trustedDirectoryOwner(os.FileInfo) bool { return false }

func allowedStickyTemporaryRoot(string, os.FileInfo) bool { return false }
