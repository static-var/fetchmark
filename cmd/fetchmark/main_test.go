package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthcheckEndpoint(t *testing.T) {
	tests := map[string]string{
		"":                    "http://127.0.0.1:8080/healthz",
		":9090":               "http://127.0.0.1:9090/healthz",
		"0.0.0.0:9090":        "http://127.0.0.1:9090/healthz",
		"localhost:7070":      "http://localhost:7070/healthz",
		"http://example.test": "http://example.test/healthz",
	}

	for listenAddr, want := range tests {
		if got := healthcheckEndpoint(listenAddr); got != want {
			t.Fatalf("healthcheckEndpoint(%q) = %q, want %q", listenAddr, got, want)
		}
	}
}

func TestRunHealthcheck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			t.Fatalf("path = %q, want /healthz", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	if err := runHealthcheck(t.Context(), srv.Listener.Addr().String(), srv.Client()); err != nil {
		t.Fatalf("runHealthcheck returned error: %v", err)
	}
}

func TestRunHealthcheckFailsOnUnhealthyStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	if err := runHealthcheck(t.Context(), srv.Listener.Addr().String(), srv.Client()); err == nil {
		t.Fatal("runHealthcheck returned nil error for unhealthy status")
	}
}
