package cache

import (
	"context"
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
	c := New(nil, 500*time.Millisecond)
	ctx := context.Background()
	if err := c.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	got, _ := c.Get(ctx, "k")
	if string(got) != "v" {
		t.Fatalf("got %q", got)
	}
	time.Sleep(600 * time.Millisecond)
	got, _ = c.Get(ctx, "k")
	if got != nil {
		t.Fatalf("expected expiry, got %q", got)
	}
}

func TestSingleflight_Suppression(t *testing.T) {
	c := New(nil, time.Minute)
	var calls int32
	fn := func() (any, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(50 * time.Millisecond)
		return "ok", nil
	}
	done := make(chan struct{}, 5)
	for i := 0; i < 5; i++ {
		go func() {
			_, _, _ = c.Do("k", fn)
			done <- struct{}{}
		}()
	}
	for i := 0; i < 5; i++ {
		<-done
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
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
