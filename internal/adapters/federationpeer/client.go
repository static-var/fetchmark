// Package federationpeer implements outbound, URL-only discovery against one
// explicitly trusted Fetchmark federation peer.
package federationpeer

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/tls"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/egress"
	"github.com/staticvar/fetchmark/internal/core/federation"
	"github.com/staticvar/fetchmark/internal/core/retryafter"
	"github.com/staticvar/fetchmark/internal/core/search"
	"github.com/staticvar/fetchmark/internal/obs"
)

const (
	defaultTimeout         = 10 * time.Second
	maximumTimeout         = 60 * time.Second
	maxRetryAfter          = 24 * time.Hour
	defaultResults         = 10
	maxResponseHeaderBytes = 32 << 10
)

// Options are copied by New. An injected HTTPClient must use an explicit,
// proxy-free standard transport; omit HTTPClient to use the production egress
// policy. This lets New reject insecure TLS and custom dial bypasses while
// copying test root CAs without sharing mutable client policy.
type Options struct {
	Identity   federation.Identity
	Peer       federation.TrustedPeer
	HTTPClient *http.Client
	Timeout    time.Duration
	Now        func() time.Time
	Random     io.Reader
}

// Client is safe for concurrent use. Each instance addresses exactly one
// operator-approved peer and performs exactly one request per search.
type Client struct {
	identity federation.Identity
	peer     federation.TrustedPeer
	endpoint *url.URL
	http     *http.Client
	policy   egress.Policy
	timeout  time.Duration
	now      func() time.Time
	random   io.Reader
	randMu   sync.Mutex
	timeMu   sync.Mutex
}

var _ search.Searcher = (*Client)(nil)
var _ search.BatchSearcher = (*Client)(nil)

func New(options Options) (*Client, error) {
	if err := validateOptions(options); err != nil {
		return nil, &Error{Kind: ErrInvalidConfiguration}
	}
	timeout := options.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	random := options.Random
	if random == nil {
		random = cryptorand.Reader
	}

	base, _ := url.Parse(options.Peer.BaseOrigin)
	endpoint := *base
	endpoint.Path = federation.SearchPath
	policy := egress.DefaultExternal()
	policy.AllowedSchemes = []string{"https"}
	policy.AllowPrivate = options.Peer.AllowPrivateNetwork
	policy.HostAllowlist = []string{base.Hostname()}
	policy.MaxRedirects = 0
	policy.DialTimeout = min(timeout, 5*time.Second)
	policy.ResponseHeaderTimeout = timeout

	var httpClient *http.Client
	var err error
	if options.HTTPClient == nil {
		httpClient = policy.HTTPClient(timeout)
	} else {
		httpClient, err = copyHTTPClient(options.HTTPClient, policy, timeout)
		if err != nil {
			return nil, &Error{Kind: ErrInvalidConfiguration}
		}
	}
	if transport, ok := httpClient.Transport.(*http.Transport); ok {
		transport.DisableCompression = true
		transport.MaxResponseHeaderBytes = maxResponseHeaderBytes
	}
	// Never follow a redirect, even if the caller's copied client allowed it.
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return ErrRedirect }

	return &Client{
		identity: options.Identity,
		peer:     clonePeer(options.Peer),
		endpoint: &endpoint,
		http:     httpClient,
		policy:   policy,
		timeout:  timeout,
		now:      now,
		random:   random,
	}, nil
}

func validateOptions(options Options) error {
	if options.Timeout < 0 || options.Timeout > maximumTimeout {
		return ErrInvalidConfiguration
	}
	identityPublic := options.Identity.PublicKey()
	identityPrivate := options.Identity.PrivateKey()
	if options.Identity.IdentityID() == "" || options.Identity.KeyID() == "" ||
		len(identityPublic) == 0 || len(identityPrivate) == 0 ||
		federation.KeyID(identityPublic) != options.Identity.KeyID() {
		return ErrInvalidConfiguration
	}
	peerPublic := options.Peer.PublicKey()
	if options.Peer.PeerID == "" || options.Peer.IdentityID == "" || options.Peer.KeyID == "" ||
		len(peerPublic) == 0 || federation.KeyID(peerPublic) != options.Peer.KeyID ||
		options.Peer.IdentityID == options.Identity.IdentityID() {
		return ErrInvalidConfiguration
	}
	base, err := url.Parse(options.Peer.BaseOrigin)
	if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil || base.Path != "" ||
		base.RawPath != "" || base.RawQuery != "" || base.Fragment != "" || base.ForceQuery {
		return ErrInvalidConfiguration
	}
	return nil
}

func copyHTTPClient(source *http.Client, policy egress.Policy, timeout time.Duration) (*http.Client, error) {
	copy := *source
	copy.Timeout = timeout
	// A caller-provided cookie jar can contain credentials for the trusted
	// origin and can be mutated by response cookies. Federation authentication
	// is exclusively the signed envelope, so never attach a shared jar.
	copy.Jar = nil
	switch transport := source.Transport.(type) {
	case nil:
		return nil, ErrInvalidConfiguration
	case *http.Transport:
		if transport.Proxy != nil || transport.DialContext != nil || transport.DialTLS != nil || transport.DialTLSContext != nil ||
			(transport.TLSClientConfig != nil && transport.TLSClientConfig.InsecureSkipVerify) {
			return nil, ErrInvalidConfiguration
		}
		if transport.TLSClientConfig != nil && transport.TLSClientConfig.ServerName != "" {
			return nil, ErrInvalidConfiguration
		}
		protected := policy.Transport()
		protected.DisableCompression = true
		protected.MaxResponseHeaderBytes = maxResponseHeaderBytes
		if transport.TLSClientConfig != nil && transport.TLSClientConfig.RootCAs != nil {
			protected.TLSClientConfig = &tls.Config{RootCAs: transport.TLSClientConfig.RootCAs.Clone()}
		}
		copy.Transport = protected
	default:
		return nil, ErrInvalidConfiguration
	}
	return &copy, nil
}

func clonePeer(peer federation.TrustedPeer) federation.TrustedPeer {
	// TrustedPeer's accessor already returns defensive key bytes. The remaining
	// fields are immutable values; retaining the value cannot share maps/slices.
	return peer
}

func (client *Client) Search(ctx context.Context, query search.Query) ([]search.Hit, error) {
	batch, err := client.SearchBatch(ctx, query)
	if err != nil {
		return nil, err
	}
	return batch.Hits, nil
}

func (client *Client) SearchBatch(ctx context.Context, query search.Query) (search.SearchBatch, error) {
	started := time.Now()
	metricOutcome := "failure"
	defer func() {
		obs.FederationRequestTotal.WithLabelValues("outbound", client.peer.PeerID, metricOutcome).Inc()
		obs.FederationRequestDuration.WithLabelValues("outbound", client.peer.PeerID).Observe(time.Since(started).Seconds())
	}()
	fail := func(kind error, retryable bool, retryAfter time.Duration, statusCode int) (search.SearchBatch, error) {
		metricOutcome = reason(kind)
		typed := &Error{Kind: kind, StatusCode: statusCode, Retryable: retryable, RetryAfter: retryAfter}
		return search.SearchBatch{
			Provider: "federation", Instance: client.peer.PeerID, Status: search.BatchFailed,
			Diagnostics: []search.ProviderDiagnostic{{
				Provider: "federation", Instance: client.peer.PeerID, Source: "trusted_peer",
				Reason: reason(kind), Retryable: retryable, RetryAfter: retryAfter,
			}},
			Duration: time.Since(started),
		}, typed
	}

	if ctx == nil {
		return fail(ErrCanceled, false, 0, 0)
	}
	if err := ctx.Err(); err != nil {
		return fail(ErrCanceled, false, 0, 0)
	}
	if unsupported(query) {
		return fail(ErrUnsupportedControls, false, 0, 0)
	}
	limit := query.MaxResults
	if limit <= 0 {
		limit = defaultResults
	}
	if limit > federation.MaxResults {
		limit = federation.MaxResults
	}
	wireRequest := federation.SearchRequest{Query: query.Q, Limit: limit}
	if _, err := federation.EncodeSearchRequest(wireRequest); err != nil {
		return fail(ErrInvalidQuery, false, 0, 0)
	}
	nonce := make([]byte, federation.NonceBytes)
	client.randMu.Lock()
	_, randomErr := io.ReadFull(client.random, nonce)
	client.randMu.Unlock()
	if randomErr != nil {
		return fail(ErrTransport, false, 0, 0)
	}
	signedAt := client.currentTime()
	requestBody, err := federation.SignRequest(
		wireRequest, client.identity.IdentityID(), client.peer.IdentityID, signedAt, nonce, client.identity.PrivateKey(),
	)
	if err != nil {
		return fail(ErrInvalidQuery, false, 0, 0)
	}

	requestContext, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	if err := client.policy.Validate(requestContext, client.endpoint.String()); err != nil {
		if ctx.Err() != nil {
			return fail(ErrCanceled, false, 0, 0)
		}
		if requestContext.Err() == context.DeadlineExceeded {
			return fail(ErrTimeout, true, 0, 0)
		}
		return fail(ErrEgressPolicy, false, 0, 0)
	}
	request, err := http.NewRequestWithContext(requestContext, federation.SearchMethod, client.endpoint.String(), bytes.NewReader(requestBody))
	if err != nil {
		return fail(ErrTransport, false, 0, 0)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept-Encoding", "identity")
	response, err := client.http.Do(request)
	if err != nil {
		if errors.Is(err, ErrRedirect) {
			return fail(ErrRedirect, false, 0, 0)
		}
		if ctx.Err() != nil {
			return fail(ErrCanceled, false, 0, 0)
		}
		var networkError net.Error
		if errors.Is(err, context.DeadlineExceeded) || requestContext.Err() == context.DeadlineExceeded ||
			(errors.As(err, &networkError) && networkError.Timeout()) {
			return fail(ErrTimeout, true, 0, 0)
		}
		return fail(ErrTransport, true, 0, 0)
	}
	defer response.Body.Close()
	if response.Request == nil || response.Request.URL.String() != client.endpoint.String() || response.TLS == nil {
		return fail(ErrTransport, false, 0, 0)
	}
	if response.StatusCode != http.StatusOK {
		retryAfter := parseRetryAfter(response.Header.Get("Retry-After"), client.currentTime())
		retryable := response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500
		return fail(ErrHTTPStatus, retryable, retryAfter, response.StatusCode)
	}
	if len(response.Header.Values("Content-Encoding")) != 0 {
		return fail(ErrCompressedResponse, false, 0, 0)
	}
	contentTypes := response.Header.Values("Content-Type")
	if len(contentTypes) != 1 {
		return fail(ErrMalformedResponse, false, 0, 0)
	}
	contentType, _, err := mime.ParseMediaType(contentTypes[0])
	if err != nil || contentType != "application/json" {
		return fail(ErrMalformedResponse, false, 0, 0)
	}
	if response.ContentLength > federation.MaxResponseBytes {
		return fail(ErrResponseTooLarge, false, 0, 0)
	}
	rawResponse, err := io.ReadAll(io.LimitReader(response.Body, federation.MaxResponseBytes+1))
	if err != nil {
		if ctx.Err() != nil {
			return fail(ErrCanceled, false, 0, 0)
		}
		if requestContext.Err() == context.DeadlineExceeded {
			return fail(ErrTimeout, true, 0, 0)
		}
		return fail(ErrTransport, true, 0, 0)
	}
	if len(rawResponse) > federation.MaxResponseBytes {
		return fail(ErrResponseTooLarge, false, 0, 0)
	}
	if len(rawResponse) == 0 {
		return fail(ErrMalformedResponse, false, 0, 0)
	}
	verified, err := federation.VerifyResponse(rawResponse, requestBody, client.peer.PublicKey(), federation.Verification{
		Sender: client.peer.IdentityID, Audience: client.identity.IdentityID(), Now: client.currentTime(),
		MaxClockSkew: federation.DefaultClockSkew,
	})
	if err != nil {
		return fail(ErrVerification, false, 0, 0)
	}
	if len(verified.Search.Results) > limit {
		return fail(ErrVerification, false, 0, 0)
	}
	hits := make([]search.Hit, 0, len(verified.Search.Results))
	for index, result := range verified.Search.Results {
		parsed, _ := url.Parse(result.URL)
		hostname := parsed.Hostname()
		zonedLiteral := false
		if zone := strings.LastIndexByte(hostname, '%'); zone >= 0 {
			zonedLiteral = net.ParseIP(hostname[:zone]) != nil
		}
		if zonedLiteral {
			return fail(ErrEgressPolicy, false, 0, 0)
		}
		if net.ParseIP(hostname) != nil {
			resultPolicy := egress.DefaultExternal()
			if err := resultPolicy.Validate(requestContext, result.URL); err != nil {
				return fail(ErrEgressPolicy, false, 0, 0)
			}
		}
		hits = append(hits, search.Hit{
			URL:     result.URL,
			Engines: []string{"federation"},
			Metadata: map[string]string{
				"source":          "federation",
				"federation_rank": strconv.Itoa(index + 1),
			},
		})
	}
	status := search.BatchAuthoritativeEmpty
	metricOutcome = "empty"
	if len(hits) > 0 {
		status = search.BatchHealthy
		metricOutcome = "ok"
	}
	return search.SearchBatch{
		Hits: hits, Provider: "federation", Instance: client.peer.PeerID,
		Status: status, Duration: time.Since(started),
	}, nil
}

func unsupported(query search.Query) bool {
	return len(query.Engines) != 0 || len(query.Categories) != 0 ||
		(query.Language != "" && !strings.EqualFold(query.Language, "auto")) ||
		query.TimeRange != "" || len(query.IncludeDomains) != 0 ||
		len(query.ExcludeDomains) != 0 || query.ExactMatch
}

func canonicalTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Second)
}

func (client *Client) currentTime() time.Time {
	client.timeMu.Lock()
	defer client.timeMu.Unlock()
	return canonicalTime(client.now())
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	return retryafter.Parse(value, now, maxRetryAfter)
}

func reason(kind error) string {
	switch kind {
	case ErrInvalidQuery:
		return "invalid_query"
	case ErrUnsupportedControls:
		return "unsupported_controls"
	case ErrCanceled:
		return "canceled"
	case ErrTimeout:
		return "timeout"
	case ErrEgressPolicy:
		return "egress_policy"
	case ErrTransport:
		return "transport"
	case ErrRedirect:
		return "redirect"
	case ErrHTTPStatus:
		return "http_status"
	case ErrCompressedResponse:
		return "compressed_response"
	case ErrResponseTooLarge:
		return "response_too_large"
	case ErrMalformedResponse:
		return "malformed_response"
	case ErrVerification:
		return "verification_failed"
	default:
		return "failure"
	}
}
