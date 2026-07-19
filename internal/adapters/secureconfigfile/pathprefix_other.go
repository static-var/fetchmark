//go:build !darwin

package secureconfigfile

func canonicalizePlatformPrefix(path string) (string, error) {
	return path, nil
}
