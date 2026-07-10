package main

import "testing"

func TestParseRedisOptionsRejectsMalformedURL(t *testing.T) {
	if _, err := parseRedisOptions("://not-a-redis-url"); err == nil {
		t.Fatal("expected malformed Redis URL to fail")
	}
}

func TestParseRedisOptionsAcceptsRedisURL(t *testing.T) {
	opts, err := parseRedisOptions("redis://localhost:6379/2")
	if err != nil {
		t.Fatalf("parseRedisOptions: %v", err)
	}
	if opts.Addr != "localhost:6379" || opts.DB != 2 {
		t.Fatalf("options = Addr %q DB %d", opts.Addr, opts.DB)
	}
}
