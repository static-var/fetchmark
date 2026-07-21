//go:build !darwin

package openpackindex

func canonicalizePlatformPrefix(path string) (string, error) {
	return path, nil
}
