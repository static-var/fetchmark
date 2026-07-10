package egressproxy

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/staticvar/fetchmark/internal/adapters/egress"
)

func TestProxyRejectsDeniedHostBeforeDial(t *testing.T) {
	policy := egress.DefaultExternal()
	policy.HostDenylist = []string{"blocked.example"}
	proxyServer := httptest.NewServer(New(policy))
	t.Cleanup(proxyServer.Close)
	proxyURL, _ := url.Parse(proxyServer.URL)
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}

	resp, err := client.Get("http://blocked.example/path")
	if err != nil {
		t.Fatalf("proxy response: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

func TestProxyRejectsPrivateConnectDestinationBeforeDial(t *testing.T) {
	policy := egress.DefaultExternal()
	proxyServer := httptest.NewServer(New(policy))
	t.Cleanup(proxyServer.Close)
	conn, err := net.Dial("tcp", proxyServer.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = fmt.Fprint(conn, "CONNECT 127.0.0.1:443 HTTP/1.1\r\nHost: 127.0.0.1:443\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

func TestProxyRejectsPrivateHTTPDestinationBeforeDial(t *testing.T) {
	var hits atomic.Int64
	private := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, "private")
	}))
	t.Cleanup(private.Close)

	policy := egress.DefaultExternal()
	proxyServer := httptest.NewServer(New(policy))
	t.Cleanup(proxyServer.Close)
	proxyURL, err := url.Parse(proxyServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}

	resp, err := client.Get(private.URL)
	if err != nil {
		t.Fatalf("proxy response: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("private destination received %d requests", got)
	}
}
