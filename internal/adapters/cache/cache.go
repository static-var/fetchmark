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
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/redis/go-redis/v9"

	"github.com/staticvar/fetchmark/internal/core/canonicalurl"
)

// ExtractorVersion is bumped whenever the extraction pipeline changes so
// that cached entries are invalidated automatically.
const ExtractorVersion = "2"

// Cache is the exported cache surface. All operations are safe for
// concurrent use.
type Cache struct {
	rdb *redis.Client
	ttl time.Duration

	mu           sync.RWMutex
	mem          map[string]memEntry
	memoryLimits MemoryLimits
	memoryBytes  int64
	nextSequence uint64

	stop      chan struct{}
	closeOnce sync.Once
	sf        singleflight.Group
}

type memEntry struct {
	value    []byte
	expires  time.Time
	sequence uint64
}

// MemoryLimits bound the in-process fallback cache. Zero values disable the
// corresponding limit. MaxValueBytes applies before either memory or Redis
// admission so one pathological artifact cannot dominate the cache.
type MemoryLimits struct {
	MaxEntries    int
	MaxBytes      int64
	MaxValueBytes int64
}

// New constructs a Cache without capacity limits. Production callers should
// use NewWithMemoryLimits so Redis fallback cannot grow without bound.
// The in-memory map is periodically swept of expired entries so that
// long-running processes without Redis do not leak memory.
func New(rdb *redis.Client, ttl time.Duration) *Cache {
	return NewWithMemoryLimits(rdb, ttl, MemoryLimits{})
}

// NewWithMemoryLimits constructs a Cache with bounded in-memory fallback.
func NewWithMemoryLimits(rdb *redis.Client, ttl time.Duration, limits MemoryLimits) *Cache {
	if ttl <= 0 {
		ttl = time.Hour
	}
	c := &Cache{rdb: rdb, ttl: ttl, mem: map[string]memEntry{}, memoryLimits: limits}
	if rdb == nil {
		c.stop = make(chan struct{})
		go c.sweeper()
	}
	return c
}

// Close releases the background sweeper (in-memory mode only). It is
// idempotent and safe to call from any goroutine.
func (c *Cache) Close() {
	c.closeOnce.Do(func() {
		if c.stop != nil {
			close(c.stop)
		}
	})
}

func (c *Cache) sweeper() {
	interval := c.ttl
	if interval > 5*time.Minute {
		interval = 5 * time.Minute
	}
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
			c.sweepExpired()
		}
	}
}

func (c *Cache) sweepExpired() {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.mem {
		if now.After(e.expires) {
			c.deleteMemoryEntry(k, e)
		}
	}
}

func (c *Cache) deleteMemoryEntry(key string, entry memEntry) {
	delete(c.mem, key)
	c.memoryBytes -= int64(len(entry.value))
	if c.memoryBytes < 0 {
		c.memoryBytes = 0
	}
}

// CanonicalURL normalises a URL so two equivalent URLs produce the same
// cache key. It:
//   - lowercases scheme + host
//   - strips default ports
//   - strips fragments
//   - drops well-known tracking params
//   - sorts the remaining query params
func CanonicalURL(raw string) (string, error) {
	return canonicalurl.V1(raw)
}

// ArtifactKey returns the Redis key for a fetch artifact (plain fetch).
func ArtifactKey(canonicalURL string) string {
	return fmt.Sprintf("fa:v%s:%s", ExtractorVersion, sha(canonicalURL))
}

// RenderedArtifactKey returns the Redis key for a fetch artifact
// produced via the headless renderer. A separate key space prevents a
// cached js_required placeholder from shadowing a later render=true
// call (and vice versa — a rendered blob never masks the cheap path).
func RenderedArtifactKey(canonicalURL string) string {
	return fmt.Sprintf("fa:v%s:%s:r", ExtractorVersion, sha(canonicalURL))
}

// FormatKey returns the Redis key for a rendered-format payload.
func FormatKey(canonicalURL, format string) string {
	return fmt.Sprintf("fmt:v%s:%s:%s", ExtractorVersion, sha(canonicalURL), strings.ToLower(format))
}

func sha(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// Get returns the cached value for key, or nil if absent. Expired
// in-memory entries are deleted lazily on read as a belt-and-braces
// safeguard beside the background sweeper.
func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	if c.rdb != nil {
		v, err := c.rdb.Get(ctx, key).Bytes()
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return v, err
	}
	c.mu.RLock()
	e, ok := c.mem[key]
	expired := ok && time.Now().After(e.expires)
	c.mu.RUnlock()
	if !ok {
		return nil, nil
	}
	if expired {
		c.mu.Lock()
		if cur, still := c.mem[key]; still && time.Now().After(cur.expires) {
			c.deleteMemoryEntry(key, cur)
		}
		c.mu.Unlock()
		return nil, nil
	}
	return e.value, nil
}

// Set writes a value with the cache TTL. Values over MaxValueBytes are not
// admitted; skipping cache admission is not a request failure.
func (c *Cache) Set(ctx context.Context, key string, val []byte) error {
	if max := c.memoryLimits.MaxValueBytes; max > 0 && int64(len(val)) > max {
		return nil
	}
	if c.rdb != nil {
		return c.rdb.Set(ctx, key, val, c.ttl).Err()
	}
	value := append([]byte(nil), val...)
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.mem[key]; ok {
		c.deleteMemoryEntry(key, old)
	}
	c.nextSequence++
	c.mem[key] = memEntry{value: value, expires: time.Now().Add(c.ttl), sequence: c.nextSequence}
	c.memoryBytes += int64(len(value))
	c.evictMemoryEntries()
	return nil
}

func (c *Cache) evictMemoryEntries() {
	for (c.memoryLimits.MaxEntries > 0 && len(c.mem) > c.memoryLimits.MaxEntries) ||
		(c.memoryLimits.MaxBytes > 0 && c.memoryBytes > c.memoryLimits.MaxBytes) {
		var oldestKey string
		var oldest memEntry
		first := true
		for key, entry := range c.mem {
			if first || entry.sequence < oldest.sequence {
				oldestKey, oldest, first = key, entry, false
			}
		}
		if first {
			return
		}
		c.deleteMemoryEntry(oldestKey, oldest)
	}
}

// MemoryUsage reports in-memory fallback occupancy. Redis-backed caches return
// zero because Redis capacity is managed externally.
func (c *Cache) MemoryUsage() (entries int, bytes int64) {
	if c.rdb != nil {
		return 0, 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.mem), c.memoryBytes
}

// Do memoises concurrent callers of the same key onto a single fn call.
// Semantics match golang.org/x/sync/singleflight.Group.Do.
func (c *Cache) Do(key string, fn func() (any, error)) (any, error, bool) {
	v, err, shared := c.sf.Do(key, fn)
	return v, err, shared
}

// LockOptions tunes WithLock behaviour. Each duration is independent so
// the lock TTL is never conflated with caller deadlines.
type LockOptions struct {
	// LockTTL is the expiry applied to the Redis lock key. Callers
	// should size it as an upper bound on the critical section.
	// Default: 10s.
	LockTTL time.Duration
	// WaitMax is the maximum time a caller waits to acquire the lock
	// before falling through to run fn without the lock (best-effort
	// stampede suppression rather than strict mutual exclusion).
	// A zero value means: wait until ctx is done.
	WaitMax time.Duration
	// PollInterval is how often acquisition is retried. Default: 100ms.
	PollInterval time.Duration
}

// releaseScript releases a Redis lock only if the token still matches
// ours, guarding against accidentally releasing a lock that expired and
// was re-acquired by another node.
//
//	KEYS[1] = lock key, ARGV[1] = token
const releaseScript = `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`

// WithLock acquires a cross-instance advisory lock on key, runs fn, and
// releases the lock. Inside fn callers should re-check the cache — by
// the time the lock is acquired another node may already have populated
// it. When Redis is not configured this degrades to a direct fn call;
// the local singleflight layer on top already coalesces concurrent
// callers inside a single process.
func (c *Cache) WithLock(ctx context.Context, key string, opts LockOptions, fn func(context.Context) ([]byte, error)) ([]byte, error) {
	if fn == nil {
		return nil, errors.New("cache: WithLock: fn is nil")
	}
	if c.rdb == nil {
		return fn(ctx)
	}
	if opts.LockTTL <= 0 {
		opts.LockTTL = 10 * time.Second
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 100 * time.Millisecond
	}

	// 128-bit token is comfortably beyond any realistic collision risk
	// and preserves the CAS invariant even under aggressive lock churn.
	var rawTok [16]byte
	if _, err := rand.Read(rawTok[:]); err != nil {
		return nil, fmt.Errorf("cache: lock token: %w", err)
	}
	token := hex.EncodeToString(rawTok[:])

	lockKey := "fm:lock:" + key

	var waitCtx context.Context = ctx
	var cancel context.CancelFunc
	if opts.WaitMax > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, opts.WaitMax)
		defer cancel()
	}

	acquired := false
	for {
		ok, err := c.rdb.SetNX(ctx, lockKey, token, opts.LockTTL).Result()
		if err == nil && ok {
			acquired = true
			break
		}
		if err != nil {
			// Lock service unreachable — run fn anyway. Correctness
			// still holds because Set uses its own cache TTL; the worst
			// case is a small amount of duplicated work.
			return fn(ctx)
		}
		select {
		case <-waitCtx.Done():
			// Timed out waiting: proceed without the lock. This matches
			// the "best-effort stampede suppression" contract — under
			// contention we'd rather serve with extra work than return
			// a 5xx.
			return fn(ctx)
		case <-time.After(opts.PollInterval):
		}
	}

	defer func() {
		if !acquired {
			return
		}
		// Best-effort release with a detached context so a canceled
		// caller still frees the lock.
		relCtx, relCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer relCancel()
		_, _ = c.rdb.Eval(relCtx, releaseScript, []string{lockKey}, token).Result()
	}()

	return fn(ctx)
}
