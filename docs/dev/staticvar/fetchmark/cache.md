# cache

Two-layer cache for raw artifacts, rendered artifacts, and per-format
derived blobs. Redis is used when reachable; otherwise the process falls
back to its in-memory map. Redis also hosts the cross-instance stampede
lock, so fallback mode is local to one process.

This expiring response cache is not the personal corpus source store.
`internal/adapters/localartifact` owns opt-in durable, hash-verified source
representations and HTTP validators under `FM_LOCAL_ARTIFACT_PATH`; Bleve is a
third, rebuildable discovery projection. Do not merge their key spaces or
retention semantics. In personal mode, one cancelable coordinator sweeps Bleve
then artifacts at `FM_LOCAL_EXPIRY_SWEEP_INTERVAL`; expired entries are already
read-ineligible before their physical deletion.

## Entry points

- `cache.go` — `New(redisClient, ttl)` → `*Cache` with `Get`, `Set`,
  `WithLock(key, opts, fn)`.
- Key helpers: `CanonicalURL`, `ArtifactKey`, `RenderedArtifactKey`,
  `FormatKey`.
- `ExtractorVersion` — bump to invalidate all keys on shape changes.

## Invariants

- **All keys are canonicalised via `CanonicalURL`** before hashing.
  Skip this and you double-cache the same page under URL-variant
  keys.
- **Keys are versioned** with `ExtractorVersion`. Do not read
  unversioned legacy keys.
- `WithLock` is a Redis SETNX lock with a bounded-wait poller when Redis
  is active. In in-memory mode it runs `fn` directly; local
  `singleflight` still coalesces same-process callers, but there is no
  cross-instance lock.
- `RenderedArtifactKey` is a distinct key from `ArtifactKey` so the
  rendered stampede lock (Q-d) doesn't collide with the plain-fetch
  one.

## Tests

- `cache_test.go` — versioned keys, canonical-URL collapse, Redis
  WithLock serialisation, no-Redis fallback, TTL expiry under miniredis.
