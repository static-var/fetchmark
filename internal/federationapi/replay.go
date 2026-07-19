// Package federationapi serves the optional private federation listener. It
// intentionally has no dependency on Fetchmark's public API router.
package federationapi

import (
	"errors"
	"strings"
	"sync"
	"time"
)

var (
	ErrReplay         = errors.New("federation: request nonce was already used")
	ErrReplayCapacity = errors.New("federation: replay table is full")
)

type replayKey struct {
	peerID string
	nonce  string
}

// ReplayCache atomically reserves peer-scoped nonces for a bounded interval.
// It is process-local by design; multi-replica serving requires shared atomic
// replay state before it can be supported.
type ReplayCache struct {
	mu      sync.Mutex
	entries map[replayKey]time.Time
	maximum int
	ttl     time.Duration
}

func NewReplayCache(maximum int, ttl time.Duration) (*ReplayCache, error) {
	if maximum < 1 || ttl <= 0 {
		return nil, errors.New("federation: positive replay capacity and TTL are required")
	}
	return &ReplayCache{entries: make(map[replayKey]time.Time, maximum), maximum: maximum, ttl: ttl}, nil
}

// Reserve records one validated peer/nonce pair. Duplicate live reservations
// fail even if the caller's clock moved backwards. Expired entries are removed
// before capacity is evaluated.
func (cache *ReplayCache) Reserve(peerID, nonce string, now time.Time) error {
	peerID = strings.TrimSpace(peerID)
	nonce = strings.TrimSpace(nonce)
	if cache == nil || peerID == "" || nonce == "" || now.IsZero() {
		return errors.New("federation: peer, nonce, and current time are required")
	}
	now = now.UTC()
	key := replayKey{peerID: peerID, nonce: nonce}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if expiry, exists := cache.entries[key]; exists && expiry.After(now) {
		return ErrReplay
	}
	for candidate, expiry := range cache.entries {
		if !expiry.After(now) {
			delete(cache.entries, candidate)
		}
	}
	if len(cache.entries) >= cache.maximum {
		return ErrReplayCapacity
	}
	cache.entries[key] = now.Add(cache.ttl)
	return nil
}
