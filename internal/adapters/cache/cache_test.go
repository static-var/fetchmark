package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCanonicalURL(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"https://Example.com:443/a/b?utm_source=x&z=1&a=2#frag", "https://example.com/a/b?a=2&z=1"},
		{"http://Example.COM:80/?b=2&a=1&fbclid=xyz", "http://example.com/?a=1&b=2"},
		{"https://host/path?k=v", "https://host/path?k=v"},
	}
	for _, c := range cases {
		got, err := CanonicalURL(c.in)
		if err != nil {
			t.Fatalf("CanonicalURL(%q): %v", c.in, err)
		}
		if got != c.want {
			t.Errorf("CanonicalURL(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestCanonicalURL_SameKeyDifferentParamOrder(t *testing.T) {
	a, _ := CanonicalURL("https://x/p?b=2&a=1")
	b, _ := CanonicalURL("https://x/p?a=1&b=2")
	if a != b {
		t.Fatalf("param-order canonicalization broken: %q vs %q", a, b)
	}
}

func TestKeys(t *testing.T) {
	k1 := ArtifactKey("u")
	if k1 == "" || k1[:5] != "fa:v"+ExtractorVersion {
		t.Fatalf("artifact key wrong: %q", k1)
	}
	k2 := FormatKey("u", "MD")
	if k2[len(k2)-3:] != ":md" {
		t.Fatalf("format key not lowercase: %q", k2)
	}
}

func TestMemoryCache_RoundTrip(t *testing.T) {
	c := New(nil, 50*time.Millisecond)
	ctx := context.Background()
	if err := c.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	got, _ := c.Get(ctx, "k")
	if string(got) != "v" {
		t.Fatalf("got %q", got)
	}
	requireEventually(t, time.Second, func() bool {
		got, _ = c.Get(ctx, "k")
		return got == nil
	})
}

func TestSingleflight_Suppression(t *testing.T) {
	c := New(nil, time.Minute)
	var calls int32
	const goroutines = 5
	entered := make(chan struct{})
	release := make(chan struct{})
	fn := func() (any, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			close(entered)
		}
		<-release
		return "ok", nil
	}

	type result struct {
		value  any
		err    error
		shared bool
	}
	results := make(chan result, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			v, err, shared := c.Do("k", fn)
			results <- result{value: v, err: err, shared: shared}
		}()
	}
	waitForReceive(t, entered, time.Second, "singleflight function to start")
	requireEventually(t, time.Second, func() bool { return atomic.LoadInt32(&calls) == 1 })
	assertNoReceive(t, results, 50*time.Millisecond, "waiter returned before release")

	close(release)
	waitForWaitGroup(t, &wg, time.Second, "singleflight callers")
	close(results)

	for res := range results {
		if res.err != nil || res.value != "ok" || !res.shared {
			t.Fatalf("Do result = (%v, %v, shared=%v), want (ok, nil, shared=true)", res.value, res.err, res.shared)
		}
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func waitForReceive[T any](t *testing.T, ch <-chan T, timeout time.Duration, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func assertNoReceive[T any](t *testing.T, ch <-chan T, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal(msg)
	case <-time.After(timeout):
	}
}

func waitForWaitGroup(t *testing.T, wg *sync.WaitGroup, timeout time.Duration, what string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	waitForReceive(t, done, timeout, what)
}

func TestWithLock_NoRedisRunsFnDirectly(t *testing.T) {
	c := New(nil, time.Second)
	defer c.Close()
	called := 0
	out, err := c.WithLock(context.Background(), "k", LockOptions{}, func(_ context.Context) ([]byte, error) {
		called++
		return []byte("ok"), nil
	})
	if err != nil {
		t.Fatalf("WithLock: %v", err)
	}
	if called != 1 || string(out) != "ok" {
		t.Fatalf("called=%d out=%q", called, out)
	}
}

func TestWithLock_NilFnReturnsError(t *testing.T) {
	c := New(nil, time.Second)
	defer c.Close()
	if _, err := c.WithLock(context.Background(), "k", LockOptions{}, nil); err == nil {
		t.Fatal("expected error on nil fn")
	}
}
