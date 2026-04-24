# cache

Two-layer cache (Redis + in-process LRU) for raw artifacts, rendered
artifacts, and per-format derived blobs. Also hosts the cross-instance
stampede lock.

## Entry points

- `cache.go` — `New(redisClient, ttl)` → `*Cache` with `Get`, `Set`,
  `WithLock(key, ttl, wait, fn)`.
- Key helpers: `CanonicalURL`, `ArtifactKey`, `RenderedArtifactKey`,
  `FormatKey`.
- `ExtractorVersion` — bump to invalidate all keys on shape changes.

## Invariants

- **All keys are canonicalised via `CanonicalURL`** before hashing.
  Skip this and you double-cache the same page under URL-variant
  keys.
- **Keys are versioned** with `ExtractorVersion`. Do not read
  unversioned legacy keys.
- `WithLock` is a Redis SETNX lock with a bounded-wait poller. TTL
  must cover the critical-section budget; `pipeline.lockTTL` sizes it
  from the renderer timeout when render is on.
- `RenderedArtifactKey` is a distinct key from `ArtifactKey` so the
  rendered stampede lock (Q-d) doesn't collide with the plain-fetch
  one.

## Tests

- `cache_test.go` — versioned keys, canonical-URL collapse, WithLock
  serialisation, TTL expiry under miniredis.
