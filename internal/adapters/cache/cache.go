// Package cache provides a versioned two-layer cache for fetch artifacts
// and rendered outputs, fronted by singleflight to suppress stampedes.
//
// Key schema:
//
//	fa:v<EXTRACTOR_VER>:<sha256(canonical_url)>         -> fetch artifact
//	fmt:v<EXTRACTOR_VER>:<sha256(canonical_url)>:<fmt>  -> rendered output
//
// A nil Redis client reduces the cache to an in-process map (useful for
// tests and single-binary deployments).
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/redis/go-redis/v9"
)

// ExtractorVersion is bumped whenever the extraction pipeline changes so
// that cached entries are invalidated automatically.
const ExtractorVersion = "1"

// Tracking query params stripped before canonicalisation.
var trackingParams = map[string]struct{}{
	"utm_source":   {},
	"utm_medium":   {},
	"utm_campaign": {},
	"utm_term":     {},
	"utm_content":  {},
	"fbclid":       {},
	"gclid":        {},
	"mc_cid":       {},
	"mc_eid":       {},
	"igshid":       {},
}

// Cache is the exported cache surface. All operations are safe for
// concurrent use.
type Cache struct {
	rdb *redis.Client
	ttl time.Duration

	mu  sync.RWMutex
	mem map[string]memEntry

	sf singleflight.Group
}

type memEntry struct {
	value   []byte
	expires time.Time
}

// New constructs a Cache. If rdb is nil, an in-memory fallback is used.
func New(rdb *redis.Client, ttl time.Duration) *Cache {
	if ttl <= 0 {
		ttl = time.Hour
	}
	return &Cache{rdb: rdb, ttl: ttl, mem: map[string]memEntry{}}
}

// CanonicalURL normalises a URL so two equivalent URLs produce the same
// cache key. It:
//   - lowercases scheme + host
//   - strips default ports
//   - strips fragments
//   - drops well-known tracking params
//   - sorts the remaining query params
func CanonicalURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Scheme == "" || u.Host == "" {
		return "", errors.New("cache: url missing scheme/host")
	}
	u.Scheme = strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Hostname())
	if port := u.Port(); port != "" {
		if (u.Scheme == "http" && port == "80") || (u.Scheme == "https" && port == "443") {
			u.Host = host
		} else {
			u.Host = host + ":" + port
		}
	} else {
		u.Host = host
	}
	u.Fragment = ""
	u.RawFragment = ""

	if u.RawQuery != "" {
		q := u.Query()
		for k := range q {
			if _, drop := trackingParams[strings.ToLower(k)]; drop {
				q.Del(k)
			}
		}
		keys := make([]string, 0, len(q))
		for k := range q {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		for i, k := range keys {
			if i > 0 {
				b.WriteByte('&')
			}
			vals := q[k]
			sort.Strings(vals)
			for j, v := range vals {
				if j > 0 {
					b.WriteByte('&')
				}
				b.WriteString(url.QueryEscape(k))
				b.WriteByte('=')
				b.WriteString(url.QueryEscape(v))
			}
		}
		u.RawQuery = b.String()
	}
	return u.String(), nil
}

// ArtifactKey returns the Redis key for a fetch artifact.
func ArtifactKey(canonicalURL string) string {
	return fmt.Sprintf("fa:v%s:%s", ExtractorVersion, sha(canonicalURL))
}

// FormatKey returns the Redis key for a rendered-format payload.
func FormatKey(canonicalURL, format string) string {
	return fmt.Sprintf("fmt:v%s:%s:%s", ExtractorVersion, sha(canonicalURL), strings.ToLower(format))
}

func sha(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// Get returns the cached value for key, or nil if absent.
func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	if c.rdb != nil {
		v, err := c.rdb.Get(ctx, key).Bytes()
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return v, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.mem[key]
	if !ok || time.Now().After(e.expires) {
		return nil, nil
	}
	return e.value, nil
}

// Set writes a value with the cache TTL.
func (c *Cache) Set(ctx context.Context, key string, val []byte) error {
	if c.rdb != nil {
		return c.rdb.Set(ctx, key, val, c.ttl).Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mem[key] = memEntry{value: val, expires: time.Now().Add(c.ttl)}
	return nil
}

// Do memoises concurrent callers of the same key onto a single fn call.
// Semantics match golang.org/x/sync/singleflight.Group.Do.
func (c *Cache) Do(key string, fn func() (any, error)) (any, error, bool) {
	v, err, shared := c.sf.Do(key, fn)
	return v, err, shared
}
