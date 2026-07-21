//go:build darwin || linux || freebsd || openbsd || netbsd || dragonfly

package secureconfigfile

import (
	"os"
	"path/filepath"
	"runtime"
	"syscall"
)

func ownedByEffectiveUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

func trustedDirectoryOwner(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && (stat.Uid == 0 || stat.Uid == uint32(os.Geteuid()))
}

func allowedStickyTemporaryRoot(path string, info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 || info.Mode()&os.ModeSticky == 0 {
		return false
	}
	path = filepath.Clean(path)
	if runtime.GOOS == "darwin" {
		return path == "/private/tmp" || path == "/private/var/tmp"
	}
	return path == "/tmp" || path == "/var/tmp"
}
