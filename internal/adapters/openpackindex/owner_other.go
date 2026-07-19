//go:build !darwin && !linux && !freebsd && !openbsd && !netbsd && !dragonfly

package openpackindex

import "os"

func trustedDirectoryOwner(os.FileInfo) bool { return false }

func allowedStickyTemporaryRoot(string, os.FileInfo) bool { return false }
