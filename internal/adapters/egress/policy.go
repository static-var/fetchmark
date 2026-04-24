// Package egress centralises outbound-network safety for Fetchmark.
//
// Every fetch of a user-influenced URL MUST pass through a Policy.
// The policy enforces:
//
//   - Scheme allow-list (http, https only)
//   - Host allow/deny lists (exact match or CIDR for IP literals)
//   - Resolved-IP rejection of private/loopback/link-local/CGNAT/ULA ranges
//   - Dial-time IP re-validation (defence against DNS rebinding)
//   - Redirect-hop re-validation with cross-scheme downgrade rejection
//
// The resulting *http.Client is safe to hand to SearXNG, the fetcher, and
// any other component that performs outbound HTTP.
package egress

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Policy captures the egress rules. A zero Policy blocks every request;
// callers should use NewPolicy to get sensible defaults.
type Policy struct {
	// AllowedSchemes lists URL schemes a fetch may use. Defaults to
	// {"http", "https"}.
	AllowedSchemes []string

	// AllowPrivate permits connections to private / loopback / link-local
	// addresses. This is used for internal backends like the bundled
	// SearXNG instance. Never enable for user-influenced URLs.
	AllowPrivate bool

	// HostAllowlist, when non-empty, restricts destinations to the listed
	// hostnames. Entries are matched case-insensitively against the
	// request URL host (no port) and the resolved IPs' PTR is NOT used.
	HostAllowlist []string

	// HostDenylist blocks specific hostnames even when they would
	// otherwise be allowed. Checked after HostAllowlist.
	HostDenylist []string

	// MaxRedirects caps redirect chains. Zero means "no redirects".
	MaxRedirects int

	// DialTimeout bounds the time a single connect may take.
	DialTimeout time.Duration

	// Resolver is used for pre-connect DNS lookups. nil means net.DefaultResolver.
	Resolver *net.Resolver
}

// DefaultExternal returns a Policy suitable for fetching arbitrary
// user-supplied URLs on the public internet.
func DefaultExternal() Policy {
	return Policy{
		AllowedSchemes: []string{"http", "https"},
		AllowPrivate:   false,
		MaxRedirects:   5,
		DialTimeout:    5 * time.Second,
	}
}

// DefaultInternal returns a Policy for traffic to trusted compose-internal
// services (SearXNG, Redis HTTP admin). It permits private-range IPs but
// still applies scheme and redirect limits.
func DefaultInternal() Policy {
	p := DefaultExternal()
	p.AllowPrivate = true
	return p
}

// Error is a structured egress rejection, suitable for metrics labels.
type Error struct {
	Reason string
	URL    string
	Detail string
}

func (e *Error) Error() string {
	if e.Detail == "" {
		return fmt.Sprintf("egress blocked: %s (%s)", e.Reason, e.URL)
	}
	return fmt.Sprintf("egress blocked: %s (%s): %s", e.Reason, e.URL, e.Detail)
}

// Reasons used in Error.Reason, exported so metrics can enumerate them.
const (
	ReasonScheme       = "scheme_not_allowed"
	ReasonHostDenied   = "host_denied"
	ReasonHostNotAllow = "host_not_allowlisted"
	ReasonPrivateIP    = "private_ip_blocked"
	ReasonResolve      = "resolve_failed"
	ReasonTooManyHops  = "too_many_redirects"
	ReasonDowngrade    = "scheme_downgrade"
)

// Validate checks a URL against the policy. Does NOT perform the HTTP
// request; returns nil if safe to dispatch.
func (p Policy) Validate(ctx context.Context, rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return &Error{Reason: "invalid_url", URL: rawURL, Detail: err.Error()}
	}
	return p.validate(ctx, u)
}

func (p Policy) validate(ctx context.Context, u *url.URL) error {
	if !containsFold(p.schemes(), u.Scheme) {
		return &Error{Reason: ReasonScheme, URL: u.String(), Detail: u.Scheme}
	}
	host := u.Hostname()
	if host == "" {
		return &Error{Reason: "empty_host", URL: u.String()}
	}
	if containsFold(p.HostDenylist, host) {
		return &Error{Reason: ReasonHostDenied, URL: u.String(), Detail: host}
	}
	if len(p.HostAllowlist) > 0 && !containsFold(p.HostAllowlist, host) {
		return &Error{Reason: ReasonHostNotAllow, URL: u.String(), Detail: host}
	}

	if p.AllowPrivate {
		return nil
	}

	// If the host is an IP literal, check it directly.
	if ip := net.ParseIP(host); ip != nil {
		if !isPublicIP(ip) {
			return &Error{Reason: ReasonPrivateIP, URL: u.String(), Detail: ip.String()}
		}
		return nil
	}

	// Otherwise resolve and require EVERY answer to be public. Rejecting
	// on any private answer (rather than majority-public) closes the
	// DNS-rebinding angle where a host returns 1.1.1.1 + 127.0.0.1.
	resolver := p.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	ips, err := resolver.LookupIPAddr(rctx, host)
	if err != nil {
		return &Error{Reason: ReasonResolve, URL: u.String(), Detail: err.Error()}
	}
	if len(ips) == 0 {
		return &Error{Reason: ReasonResolve, URL: u.String(), Detail: "no_records"}
	}
	for _, a := range ips {
		if !isPublicIP(a.IP) {
			return &Error{Reason: ReasonPrivateIP, URL: u.String(), Detail: a.IP.String()}
		}
	}
	return nil
}

// Transport returns an *http.Transport whose Dialer re-checks the
// resolved IP at connect time. Use HTTPClient to also install the
// redirect-revalidating CheckRedirect.
func (p Policy) Transport() *http.Transport {
	dialer := &net.Dialer{
		Timeout:   p.dialTimeout(),
		KeepAlive: 30 * time.Second,
		Control: func(network, address string, c syscall.RawConn) error {
			if p.AllowPrivate {
				return nil
			}
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			ip := net.ParseIP(host)
			if ip == nil {
				return fmt.Errorf("dialer: unresolved host %q", host)
			}
			if !isPublicIP(ip) {
				return &Error{Reason: ReasonPrivateIP, URL: address, Detail: ip.String()}
			}
			return nil
		},
	}
	tr := &http.Transport{
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return tr
}

// HTTPClient returns a fully wired *http.Client (Transport +
// CheckRedirect) ready to use with this policy.
func (p Policy) HTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:   timeout,
		Transport: p.Transport(),
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= p.MaxRedirects {
				return &Error{Reason: ReasonTooManyHops, URL: req.URL.String()}
			}
			if len(via) > 0 {
				prev := via[len(via)-1].URL
				if prev.Scheme == "https" && req.URL.Scheme == "http" {
					return &Error{Reason: ReasonDowngrade, URL: req.URL.String()}
				}
			}
			if err := p.validate(req.Context(), req.URL); err != nil {
				return err
			}
			return nil
		},
	}
}

func (p Policy) schemes() []string {
	if len(p.AllowedSchemes) == 0 {
		return []string{"http", "https"}
	}
	return p.AllowedSchemes
}

func (p Policy) dialTimeout() time.Duration {
	if p.DialTimeout <= 0 {
		return 5 * time.Second
	}
	return p.DialTimeout
}

var (
	// Cached IPNets, built once.
	privateNetsOnce sync.Once
	privateNets     []*net.IPNet
)

// isPublicIP reports whether ip is safe to connect to from a public
// fetcher (i.e. NOT private, loopback, link-local, CGNAT, multicast,
// ULA, or reserved).
func isPublicIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsUnspecified() || ip.IsLoopback() || ip.IsMulticast() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() {
		return false
	}
	// v4-mapped: check the embedded v4 address too.
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	privateNetsOnce.Do(initPrivateNets)
	for _, n := range privateNets {
		if n.Contains(ip) {
			return false
		}
	}
	return true
}

func initPrivateNets() {
	cidrs := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"127.0.0.0/8",
		"169.254.0.0/16",
		"100.64.0.0/10", // CGNAT
		"192.0.0.0/24",  // IETF protocol assignments
		"192.0.2.0/24",  // TEST-NET-1
		"198.18.0.0/15", // benchmarking
		"198.51.100.0/24",
		"203.0.113.0/24",
		"224.0.0.0/4",
		"240.0.0.0/4",
		// IPv6
		"::1/128",
		"fc00::/7",
		"fe80::/10",
		"ff00::/8",
		// Note: v4-mapped (::ffff:0:0/96) is intentionally omitted. Go's
		// net package normalizes that CIDR into a v4 /0, which would
		// reject every public IPv4 address. v4-mapped inputs are
		// normalized to 4-byte form via To4() above, which already
		// routes them through the v4 rules.
	}
	for _, c := range cidrs {
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			panic(err)
		}
		privateNets = append(privateNets, n)
	}
}

func containsFold(list []string, s string) bool {
	for _, e := range list {
		if strings.EqualFold(e, s) {
			return true
		}
	}
	return false
}

// Sentinel for callers that want to type-assert on rejection.
var ErrBlocked = errors.New("egress blocked")
