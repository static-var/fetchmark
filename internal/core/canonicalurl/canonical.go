// Package canonicalurl owns versioned URL normalization contracts shared by
// cache keys, discovery deduplication, and signed index-pack validation.
package canonicalurl

import (
	"errors"
	"net"
	"net/url"
	"sort"
	"strings"
)

const VersionV1 = 1

var trackingParamsV1 = map[string]struct{}{
	"utm_source": {}, "utm_medium": {}, "utm_campaign": {}, "utm_term": {}, "utm_content": {},
	"fbclid": {}, "gclid": {}, "mc_cid": {}, "mc_eid": {}, "igshid": {},
}

// V1 is the frozen canonicalization policy used by signed index-pack schema
// v1. Changing these rules requires a new version rather than mutating V1.
func V1(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Scheme == "" || u.Host == "" {
		return "", errors.New("canonical URL v1: missing scheme or host")
	}
	u.Scheme = strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Hostname())
	if port := u.Port(); port != "" {
		if (u.Scheme == "http" && port == "80") || (u.Scheme == "https" && port == "443") {
			u.Host = hostV1(host)
		} else {
			u.Host = net.JoinHostPort(host, port)
		}
	} else {
		u.Host = hostV1(host)
	}
	u.Fragment = ""
	u.RawFragment = ""

	if u.RawQuery != "" {
		query := u.Query()
		for key := range query {
			if _, drop := trackingParamsV1[strings.ToLower(key)]; drop {
				query.Del(key)
			}
		}
		keys := make([]string, 0, len(query))
		for key := range query {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		var encoded strings.Builder
		for keyIndex, key := range keys {
			if keyIndex > 0 {
				encoded.WriteByte('&')
			}
			values := query[key]
			sort.Strings(values)
			for valueIndex, value := range values {
				if valueIndex > 0 {
					encoded.WriteByte('&')
				}
				encoded.WriteString(url.QueryEscape(key))
				encoded.WriteByte('=')
				encoded.WriteString(url.QueryEscape(value))
			}
		}
		u.RawQuery = encoded.String()
	}
	return u.String(), nil
}

func hostV1(host string) string {
	if strings.Contains(host, ":") {
		return "[" + host + "]"
	}
	return host
}
