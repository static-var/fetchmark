package federationapi

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReplayCacheScopesNonceByPeerAndExpires(t *testing.T) {
	now := time.Date(2026, time.July, 18, 12, 0, 0, 0, time.UTC)
	cache, err := NewReplayCache(2, fiveMinutes)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Reserve("peer-a", "nonce", now); err != nil {
		t.Fatal(err)
	}
	if err := cache.Reserve("peer-a", "nonce", now.Add(time.Minute)); !errors.Is(err, ErrReplay) {
		t.Fatalf("duplicate Reserve = %v, want ErrReplay", err)
	}
	if err := cache.Reserve("peer-b", "nonce", now.Add(time.Minute)); err != nil {
		t.Fatalf("peer-scoped Reserve: %v", err)
	}
	if err := cache.Reserve("peer-c", "other", now.Add(time.Minute)); !errors.Is(err, ErrReplayCapacity) {
		t.Fatalf("full Reserve = %v, want ErrReplayCapacity", err)
	}
	if err := cache.Reserve("peer-c", "other", now.Add(fiveMinutes)); err != nil {
		t.Fatalf("expired Reserve: %v", err)
	}
}

func TestReplayCacheConcurrentReservationHasOneWinner(t *testing.T) {
	cache, err := NewReplayCache(16, fiveMinutes)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	var winners atomic.Int32
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if cache.Reserve("peer-a", "same-nonce", now) == nil {
				winners.Add(1)
			}
		}()
	}
	wait.Wait()
	if winners.Load() != 1 {
		t.Fatalf("reservation winners = %d, want 1", winners.Load())
	}
}

func TestReplayCacheRejectsInvalidConfigurationAndInput(t *testing.T) {
	if _, err := NewReplayCache(0, time.Minute); err == nil {
		t.Fatal("NewReplayCache accepted zero capacity")
	}
	cache, err := NewReplayCache(1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Reserve("", "nonce", time.Now()); err == nil {
		t.Fatal("Reserve accepted empty peer")
	}
}

const fiveMinutes = 5 * time.Minute
