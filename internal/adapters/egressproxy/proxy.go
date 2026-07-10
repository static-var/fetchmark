// Package egressproxy provides the connection-time policy boundary for
// headless-browser traffic. Chromium is configured to send HTTP and HTTPS
// through this proxy so every target dial is checked by egress.Policy.
package egressproxy

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/egress"
)

// Proxy is an HTTP forward proxy backed by a policy-enforcing transport.
type Proxy struct {
	policy    egress.Policy
	transport *http.Transport
}

// New constructs a proxy that validates host policy before every request and
// revalidates resolved IPs at dial time.
func New(policy egress.Policy) *Proxy {
	return &Proxy{policy: policy, transport: policy.Transport()}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if p == nil || p.transport == nil || p.transport.DialContext == nil {
		http.Error(w, "egress proxy unavailable", http.StatusServiceUnavailable)
		return
	}
	if r.Method == http.MethodConnect {
		p.connect(w, r)
		return
	}
	p.forward(w, r)
}

func (p *Proxy) forward(w http.ResponseWriter, r *http.Request) {
	if r.URL == nil || r.URL.Scheme == "" || r.URL.Host == "" {
		http.Error(w, "absolute target URL required", http.StatusBadRequest)
		return
	}
	if err := p.policy.Validate(r.Context(), r.URL.String()); err != nil {
		writeProxyError(w, err)
		return
	}
	out := r.Clone(r.Context())
	out.RequestURI = ""
	out.Header = r.Header.Clone()
	removeHopHeaders(out.Header)
	out.Header.Del("Proxy-Authorization")

	resp, err := p.transport.RoundTrip(out)
	if err != nil {
		writeProxyError(w, err)
		return
	}
	defer resp.Body.Close()
	copyHeaders(w.Header(), resp.Header)
	removeHopHeaders(w.Header())
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (p *Proxy) connect(w http.ResponseWriter, r *http.Request) {
	target := r.Host
	if target == "" {
		target = r.URL.Host
	}
	if _, _, err := net.SplitHostPort(target); err != nil {
		target = net.JoinHostPort(strings.Trim(target, "[]"), "443")
	}
	if err := p.policy.Validate(r.Context(), "https://"+target); err != nil {
		writeProxyError(w, err)
		return
	}
	upstream, err := p.transport.DialContext(r.Context(), "tcp", target)
	if err != nil {
		writeProxyError(w, err)
		return
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		_ = upstream.Close()
		http.Error(w, "hijacking unsupported", http.StatusInternalServerError)
		return
	}
	client, buffered, err := hijacker.Hijack()
	if err != nil {
		_ = upstream.Close()
		return
	}
	if _, err := buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		_ = client.Close()
		_ = upstream.Close()
		return
	}
	if err := buffered.Flush(); err != nil {
		_ = client.Close()
		_ = upstream.Close()
		return
	}

	go tunnel(client, upstream)
}

func tunnel(client, upstream net.Conn) {
	defer client.Close()
	defer upstream.Close()
	done := make(chan struct{}, 2)
	copyConn := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		if tcp, ok := dst.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
		done <- struct{}{}
	}
	go copyConn(upstream, client)
	go copyConn(client, upstream)
	<-done
	select {
	case <-done:
	case <-time.After(time.Second):
	}
}

func writeProxyError(w http.ResponseWriter, err error) {
	var policyErr *egress.Error
	if errors.As(err, &policyErr) {
		http.Error(w, "destination denied", http.StatusForbidden)
		return
	}
	http.Error(w, fmt.Sprintf("upstream unavailable: %v", err), http.StatusBadGateway)
}

func copyHeaders(dst, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func removeHopHeaders(header http.Header) {
	for _, key := range []string{
		"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate",
		"Proxy-Authorization", "TE", "Trailer", "Transfer-Encoding", "Upgrade",
	} {
		header.Del(key)
	}
}
