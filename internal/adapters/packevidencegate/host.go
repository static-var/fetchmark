// Package packevidencegate owns durable, cross-process request admission for
// the separately operated open-pack evidence collector. It does not fetch,
// resolve, validate, or proxy URLs.
package packevidencegate

import (
	"errors"
	"net/netip"
	"strings"
)

var ErrInvalidHost = errors.New("pack evidence gate: invalid host")

// CanonicalHost returns the identity used to coordinate every actual HTTP
// request hop. Scheme and port are intentionally absent: operators must not be
// able to multiply a site's host budget by changing either one.
func CanonicalHost(raw string) (string, error) {
	if raw == "" || strings.TrimSpace(raw) != raw || strings.ContainsAny(raw, "[]/\\\x00\r\n\t ") {
		return "", ErrInvalidHost
	}
	if address, err := netip.ParseAddr(raw); err == nil {
		return address.Unmap().String(), nil
	}
	if strings.Contains(raw, ":") || onlyDigitsAndDots(raw) {
		return "", ErrInvalidHost
	}
	for _, char := range raw {
		if char > 127 {
			return "", ErrInvalidHost
		}
	}
	host := strings.ToLower(strings.TrimSuffix(raw, "."))
	if host == "" || len(host) > 253 {
		return "", ErrInvalidHost
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", ErrInvalidHost
		}
		for _, char := range label {
			if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' {
				return "", ErrInvalidHost
			}
		}
	}
	return host, nil
}

func onlyDigitsAndDots(raw string) bool {
	for _, char := range raw {
		if (char < '0' || char > '9') && char != '.' {
			return false
		}
	}
	return true
}
