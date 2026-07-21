package federationpeer

import (
	"bytes"
	"context"
	"crypto/ed25519"
	cryptorand "crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/egress"
	"github.com/staticvar/fetchmark/internal/core/federation"
	"github.com/staticvar/fetchmark/internal/core/search"
)

var fixedTime = time.Date(2026, 7, 18, 12, 34, 56, 987654321, time.FixedZone("test", 5*60*60+30*60))

type peerFixture struct {
	local        federation.Identity
	remote       federation.Identity
	peer         federation.TrustedPeer
	server       *httptest.Server
	httpClient   *http.Client
	responseNow  time.Time
	responseBody federation.SearchResponse
}

func newPeerFixture(t *testing.T, handler func(*peerFixture, http.ResponseWriter, *http.Request, []byte)) *peerFixture {
	t.Helper()
	fixture := &peerFixture{
		local:        newIdentity(t, "local-node"),
		remote:       newIdentity(t, "remote-node"),
		responseNow:  canonicalTime(fixedTime),
		responseBody: federation.SearchResponse{Results: []federation.SearchResult{{URL: "https://example.org/result"}}},
	}
	fixture.server = httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		raw, err := io.ReadAll(io.LimitReader(request.Body, federation.MaxRequestBytes+1))
		if err != nil {
			t.Errorf("read request: %v", err)
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		if handler != nil {
			handler(fixture, writer, request, raw)
			return
		}
		writeSignedResponse(t, fixture, writer, raw, fixture.responseBody)
	}))
	fixture.server.StartTLS()
	fixture.peer = newTrustedPeer(t, fixture.remote, fixture.server.URL, true)
	pool := x509.NewCertPool()
	pool.AddCert(fixture.server.Certificate())
	fixture.httpClient = &http.Client{Transport: &http.Transport{TLSClientConfig: fixture.server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()}}
	fixture.httpClient.Transport.(*http.Transport).TLSClientConfig.InsecureSkipVerify = false
	fixture.httpClient.Transport.(*http.Transport).TLSClientConfig.RootCAs = pool
	t.Cleanup(fixture.server.Close)
	return fixture
}

func newIdentity(t *testing.T, id string) federation.Identity {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	document := map[string]any{
		"version": federation.IdentityVersion, "identity_id": id,
		"key_id": federation.KeyID(publicKey), "ed25519_private_key": base64.StdEncoding.EncodeToString(privateKey),
	}
	raw, _ := json.Marshal(document)
	identity, err := federation.DecodeIdentity(raw)
	if err != nil {
		t.Fatalf("decode identity: %v", err)
	}
	return identity
}

func newTrustedPeer(t *testing.T, identity federation.Identity, origin string, allowPrivate bool) federation.TrustedPeer {
	t.Helper()
	document := map[string]any{
		"version": federation.TrustRegistryVersion,
		"identities": []map[string]any{{
			"id": identity.IdentityID(), "key_id": identity.KeyID(),
			"ed25519_public_key": base64.StdEncoding.EncodeToString(identity.PublicKey()),
		}},
		"peers": []map[string]any{{
			"id": "peer-one", "identity_id": identity.IdentityID(), "base_origin": origin,
			"allow_private_network": allowPrivate, "allow_inbound": true,
		}},
	}
	raw, _ := json.Marshal(document)
	registry, err := federation.LoadTrustRegistry(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("load trust registry: %v", err)
	}
	peer, ok := registry.Lookup("peer-one")
	if !ok {
		t.Fatal("trusted peer missing")
	}
	return peer
}

func newClient(t *testing.T, fixture *peerFixture, mutate func(*Options)) *Client {
	t.Helper()
	options := Options{
		Identity: fixture.local, Peer: fixture.peer, HTTPClient: fixture.httpClient,
		Now: func() time.Time { return fixedTime },
	}
	if mutate != nil {
		mutate(&options)
	}
	client, err := New(options)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return client
}

func writeSignedResponse(t *testing.T, fixture *peerFixture, writer http.ResponseWriter, exactRequest []byte, response federation.SearchResponse) {
	t.Helper()
	raw, err := federation.SignResponse(
		response, exactRequest, fixture.responseNow, bytes.Repeat([]byte{0x73}, federation.NonceBytes), fixture.remote.PrivateKey(),
	)
	if err != nil {
		t.Fatalf("sign response: %v", err)
	}
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write(raw)
}

func TestSearchBatchSuccessUsesExactSignedRouteAndLocalMetadata(t *testing.T) {
	fixture := newPeerFixture(t, func(fixture *peerFixture, writer http.ResponseWriter, request *http.Request, raw []byte) {
		if request.Method != federation.SearchMethod || request.URL.Path != federation.SearchPath || request.URL.RawQuery != "" {
			t.Errorf("unexpected request target: %s %s", request.Method, request.URL.String())
		}
		if request.Header.Get("Accept") != "application/json" || request.Header.Get("Content-Type") != "application/json" || request.Header.Get("Accept-Encoding") != "identity" {
			t.Errorf("unexpected headers: %v", request.Header)
		}
		envelope, decoded, err := federation.DecodeRequest(raw)
		if err != nil {
			t.Errorf("decode request: %v", err)
		} else {
			if decoded.Query != "test query" || decoded.Limit != defaultResults {
				t.Errorf("request = %+v", decoded)
			}
			if envelope.Timestamp != "2026-07-18T07:04:56Z" {
				t.Errorf("timestamp = %q", envelope.Timestamp)
			}
		}
		writeSignedResponse(t, fixture, writer, raw, fixture.responseBody)
	})
	client := newClient(t, fixture, nil)
	if fixture.httpClient.CheckRedirect != nil {
		t.Fatal("fixture unexpectedly has redirect policy")
	}
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "test query", Language: "auto"})
	if err != nil {
		t.Fatalf("SearchBatch: %v", err)
	}
	if fixture.httpClient.CheckRedirect != nil {
		t.Fatal("caller HTTP client was mutated")
	}
	if batch.Provider != "federation" || batch.Instance != "peer-one" || batch.Status != search.BatchHealthy || len(batch.Hits) != 1 {
		t.Fatalf("batch = %+v", batch)
	}
	hit := batch.Hits[0]
	if hit.URL != "https://example.org/result" || hit.Title != "" || hit.Snippet != "" || len(hit.Engines) != 1 || hit.Engines[0] != "federation" {
		t.Fatalf("hit = %+v", hit)
	}
	if len(hit.Metadata) != 2 || hit.Metadata["source"] != "federation" || hit.Metadata["federation_rank"] != "1" {
		t.Fatalf("metadata = %#v", hit.Metadata)
	}
	if _, leaked := hit.Metadata["federation_peer"]; leaked {
		t.Fatalf("private peer binding leaked in public metadata: %#v", hit.Metadata)
	}
}

func TestSearchBatchCapsLimitAndRejectsTooManyResults(t *testing.T) {
	var observed int
	var calls int
	fixture := newPeerFixture(t, func(fixture *peerFixture, writer http.ResponseWriter, _ *http.Request, raw []byte) {
		_, request, err := federation.DecodeRequest(raw)
		if err != nil {
			t.Errorf("decode request: %v", err)
		}
		observed = request.Limit
		calls++
		results := []federation.SearchResult{}
		if calls == 1 {
			results = []federation.SearchResult{{URL: "https://example.org/one"}, {URL: "https://example.org/two"}}
		}
		writeSignedResponse(t, fixture, writer, raw, federation.SearchResponse{Results: results})
	})
	client := newClient(t, fixture, nil)
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "query", MaxResults: 1})
	if !errors.Is(err, ErrVerification) || observed != 1 || batch.Diagnostics[0].Reason != "verification_failed" {
		t.Fatalf("observed=%d batch=%+v err=%v", observed, batch, err)
	}

	// A caller cannot expand the wire contract beyond its protocol cap.
	if _, err := client.SearchBatch(context.Background(), search.Query{Q: "query", MaxResults: 999}); err != nil {
		t.Fatalf("capped request: %v", err)
	}
	if observed != federation.MaxResults {
		t.Fatalf("limit = %d, want %d", observed, federation.MaxResults)
	}
}

func TestSearchBatchRejectsUnsupportedControlsWithoutNetwork(t *testing.T) {
	var calls atomic.Int32
	fixture := newPeerFixture(t, func(_ *peerFixture, writer http.ResponseWriter, _ *http.Request, _ []byte) {
		calls.Add(1)
		writer.WriteHeader(http.StatusInternalServerError)
	})
	client := newClient(t, fixture, nil)
	tests := []search.Query{
		{Q: "secret", Engines: []string{"x"}},
		{Q: "secret", Categories: []string{"x"}},
		{Q: "secret", Language: "en"},
		{Q: "secret", TimeRange: "day"},
		{Q: "secret", IncludeDomains: []string{"example.org"}},
		{Q: "secret", ExcludeDomains: []string{"example.org"}},
		{Q: "secret", ExactMatch: true},
	}
	for _, query := range tests {
		batch, err := client.SearchBatch(context.Background(), query)
		if !errors.Is(err, ErrUnsupportedControls) || batch.Diagnostics[0].Reason != "unsupported_controls" {
			t.Fatalf("query=%+v batch=%+v err=%v", query, batch, err)
		}
		if strings.Contains(err.Error(), "secret") || strings.Contains(fmt.Sprint(batch.Diagnostics), "secret") {
			t.Fatal("error or diagnostic leaked query")
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("network calls = %d", calls.Load())
	}
}

func TestSearchBatchAcceptsAllSafeSearchLevels(t *testing.T) {
	fixture := newPeerFixture(t, nil)
	client := newClient(t, fixture, nil)
	for _, level := range []int{0, 1, 2} {
		level := level
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "query", SafeSearch: &level})
		if err != nil || len(batch.Hits) != 1 {
			t.Fatalf("safe level %d: batch=%+v err=%v", level, batch, err)
		}
	}
}

func TestSearchBatchRejectsInvalidQueryBeforeEntropyOrNetwork(t *testing.T) {
	var calls atomic.Int32
	fixture := newPeerFixture(t, func(_ *peerFixture, writer http.ResponseWriter, _ *http.Request, _ []byte) {
		calls.Add(1)
		writer.WriteHeader(http.StatusInternalServerError)
	})
	client := newClient(t, fixture, func(options *Options) { options.Random = bytes.NewReader(nil) })
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "   "})
	if !errors.Is(err, ErrInvalidQuery) || batch.Diagnostics[0].Reason != "invalid_query" || calls.Load() != 0 {
		t.Fatalf("calls=%d batch=%+v err=%v", calls.Load(), batch, err)
	}
}

func TestSearchBatchUsesFreshNoncePerRequest(t *testing.T) {
	var nonces []string
	fixture := newPeerFixture(t, func(fixture *peerFixture, writer http.ResponseWriter, _ *http.Request, raw []byte) {
		envelope, _, err := federation.DecodeRequest(raw)
		if err != nil {
			t.Errorf("decode request: %v", err)
		}
		nonces = append(nonces, envelope.Nonce)
		writeSignedResponse(t, fixture, writer, raw, federation.SearchResponse{Results: []federation.SearchResult{}})
	})
	random := append(bytes.Repeat([]byte{1}, federation.NonceBytes), bytes.Repeat([]byte{2}, federation.NonceBytes)...)
	client := newClient(t, fixture, func(options *Options) { options.Random = bytes.NewReader(random) })
	for range 2 {
		if _, err := client.SearchBatch(context.Background(), search.Query{Q: "query"}); err != nil {
			t.Fatal(err)
		}
	}
	if len(nonces) != 2 || nonces[0] == nonces[1] {
		t.Fatalf("nonces = %#v", nonces)
	}
}

func TestSearchBatchRejectsTamperingAndWrongRequestBinding(t *testing.T) {
	tests := []struct {
		name    string
		handler func(*peerFixture, http.ResponseWriter, []byte)
	}{
		{
			name: "body tampering",
			handler: func(fixture *peerFixture, writer http.ResponseWriter, raw []byte) {
				signed, err := federation.SignResponse(fixture.responseBody, raw, fixture.responseNow, bytes.Repeat([]byte{1}, federation.NonceBytes), fixture.remote.PrivateKey())
				if err != nil {
					t.Fatal(err)
				}
				signed = bytes.Replace(signed, []byte("/result"), []byte("/tamper"), 1)
				writer.Header().Set("Content-Type", "application/json")
				_, _ = writer.Write(signed)
			},
		},
		{
			name: "different exact request",
			handler: func(fixture *peerFixture, writer http.ResponseWriter, _ []byte) {
				other, err := federation.SignRequest(federation.SearchRequest{Query: "other", Limit: 10}, fixture.local.IdentityID(), fixture.remote.IdentityID(), fixture.responseNow, bytes.Repeat([]byte{2}, federation.NonceBytes), fixture.local.PrivateKey())
				if err != nil {
					t.Fatal(err)
				}
				writeSignedResponse(t, fixture, writer, other, fixture.responseBody)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newPeerFixture(t, func(fixture *peerFixture, writer http.ResponseWriter, _ *http.Request, raw []byte) {
				test.handler(fixture, writer, raw)
			})
			client := newClient(t, fixture, nil)
			batch, err := client.SearchBatch(context.Background(), search.Query{Q: "query"})
			if !errors.Is(err, ErrVerification) || batch.Diagnostics[0].Reason != "verification_failed" {
				t.Fatalf("batch=%+v err=%v", batch, err)
			}
		})
	}
}

func TestSearchBatchRejectsRedirectCompressionMalformedAndOversize(t *testing.T) {
	tests := []struct {
		name string
		kind error
		fn   func(http.ResponseWriter)
	}{
		{"redirect", ErrRedirect, func(writer http.ResponseWriter) {
			writer.Header().Set("Location", "/elsewhere")
			writer.WriteHeader(http.StatusFound)
		}},
		{"compression", ErrCompressedResponse, func(writer http.ResponseWriter) {
			writer.Header().Set("Content-Encoding", "gzip")
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte("{}"))
		}},
		{"empty encoding header", ErrCompressedResponse, func(writer http.ResponseWriter) {
			writer.Header()["Content-Encoding"] = []string{""}
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte("{}"))
		}},
		{"content type", ErrMalformedResponse, func(writer http.ResponseWriter) {
			writer.Header().Set("Content-Type", "text/plain")
			_, _ = writer.Write([]byte("{}"))
		}},
		{"duplicate content type", ErrMalformedResponse, func(writer http.ResponseWriter) {
			writer.Header().Add("Content-Type", "application/json")
			writer.Header().Add("Content-Type", "application/json")
			_, _ = writer.Write([]byte("{}"))
		}},
		{"oversize", ErrResponseTooLarge, func(writer http.ResponseWriter) {
			writer.Header().Set("Content-Type", "application/json")
			writer.Header().Set("Content-Length", fmt.Sprint(federation.MaxResponseBytes+1))
			writer.WriteHeader(http.StatusOK)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newPeerFixture(t, func(_ *peerFixture, writer http.ResponseWriter, _ *http.Request, _ []byte) { test.fn(writer) })
			client := newClient(t, fixture, nil)
			_, err := client.SearchBatch(context.Background(), search.Query{Q: "query"})
			if !errors.Is(err, test.kind) {
				t.Fatalf("err=%v, want %v", err, test.kind)
			}
		})
	}
}

func TestSearchBatchBoundsStreamedResponseBody(t *testing.T) {
	fixture := newPeerFixture(t, func(_ *peerFixture, writer http.ResponseWriter, _ *http.Request, _ []byte) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		writer.(http.Flusher).Flush()
		_, _ = writer.Write(bytes.Repeat([]byte{'x'}, federation.MaxResponseBytes+1))
	})
	client := newClient(t, fixture, nil)
	_, err := client.SearchBatch(context.Background(), search.Query{Q: "query"})
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("err=%v, want oversized response", err)
	}
}

func TestSearchBatchRejectsSignedNonPublicIPResult(t *testing.T) {
	for _, resultURL := range []string{"http://127.0.0.1/private", "http://[::1]/private"} {
		fixture := newPeerFixture(t, nil)
		fixture.responseBody = federation.SearchResponse{Results: []federation.SearchResult{{URL: resultURL}}}
		client := newClient(t, fixture, nil)
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "query"})
		if !errors.Is(err, ErrEgressPolicy) || batch.Diagnostics[0].Reason != "egress_policy" {
			t.Fatalf("URL=%s batch=%+v err=%v", resultURL, batch, err)
		}
		if strings.Contains(err.Error(), resultURL) || strings.Contains(fmt.Sprint(batch.Diagnostics), resultURL) {
			t.Fatal("policy failure leaked the result URL")
		}
	}
}

func TestSearchBatchClassifiesStatusRetryAfterAndDoesNotRetry(t *testing.T) {
	var calls atomic.Int32
	fixture := newPeerFixture(t, func(_ *peerFixture, writer http.ResponseWriter, _ *http.Request, _ []byte) {
		calls.Add(1)
		writer.Header().Set("Retry-After", "999999999999999999999999")
		writer.WriteHeader(http.StatusTooManyRequests)
	})
	client := newClient(t, fixture, nil)
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "query"})
	var typed *Error
	if !errors.As(err, &typed) || !errors.Is(err, ErrHTTPStatus) || typed.StatusCode != http.StatusTooManyRequests || !typed.Retryable || typed.RetryAfter != maxRetryAfter {
		t.Fatalf("typed=%+v err=%v", typed, err)
	}
	if calls.Load() != 1 || !batch.Diagnostics[0].Retryable || batch.Diagnostics[0].RetryAfter != maxRetryAfter {
		t.Fatalf("calls=%d batch=%+v", calls.Load(), batch)
	}
}

func TestSearchBatchClassifiesServerFailureAsRetryableWithoutRetrying(t *testing.T) {
	var calls atomic.Int32
	fixture := newPeerFixture(t, func(_ *peerFixture, writer http.ResponseWriter, _ *http.Request, _ []byte) {
		calls.Add(1)
		writer.WriteHeader(http.StatusServiceUnavailable)
	})
	client := newClient(t, fixture, nil)
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "query"})
	var typed *Error
	if !errors.As(err, &typed) || !typed.Retryable || typed.StatusCode != http.StatusServiceUnavailable || calls.Load() != 1 || !batch.Diagnostics[0].Retryable {
		t.Fatalf("calls=%d typed=%+v batch=%+v err=%v", calls.Load(), typed, batch, err)
	}
}

func TestSearchBatchCancellationAndTimeout(t *testing.T) {
	fixture := newPeerFixture(t, func(_ *peerFixture, writer http.ResponseWriter, request *http.Request, _ []byte) {
		<-request.Context().Done()
		writer.WriteHeader(http.StatusGatewayTimeout)
	})
	client := newClient(t, fixture, func(options *Options) { options.Timeout = 30 * time.Millisecond })
	_, err := client.SearchBatch(context.Background(), search.Query{Q: "query"})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("timeout err = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.SearchBatch(canceled, search.Query{Q: "query"})
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("cancel err = %v", err)
	}
}

func TestNewRejectsInsecureTLSAndBuildsExactSSRFPolicy(t *testing.T) {
	fixture := newPeerFixture(t, nil)
	if _, err := New(Options{Identity: fixture.local, Peer: fixture.peer, HTTPClient: &http.Client{}}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("nil injected transport err = %v", err)
	}
	tests := map[string]func(*http.Transport){
		"proxy": func(transport *http.Transport) {
			transport.Proxy = http.ProxyURL(&url.URL{Scheme: "http", Host: "proxy.example"})
		},
		"dial context": func(transport *http.Transport) {
			transport.DialContext = func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("unused") }
		},
		"dial TLS": func(transport *http.Transport) {
			transport.DialTLS = func(string, string) (net.Conn, error) { return nil, errors.New("unused") }
		},
		"dial TLS context": func(transport *http.Transport) {
			transport.DialTLSContext = func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("unused") }
		},
		"insecure TLS": func(transport *http.Transport) {
			transport.TLSClientConfig.InsecureSkipVerify = true
		},
		"TLS server name override": func(transport *http.Transport) {
			transport.TLSClientConfig.ServerName = "example.org"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			transport := fixture.httpClient.Transport.(*http.Transport).Clone()
			transport.TLSClientConfig = transport.TLSClientConfig.Clone()
			mutate(transport)
			_, err := New(Options{Identity: fixture.local, Peer: fixture.peer, HTTPClient: &http.Client{Transport: transport}})
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("New err = %v", err)
			}
		})
	}

	client := newClient(t, fixture, nil)
	if !client.policy.AllowPrivate || len(client.policy.AllowedSchemes) != 1 || client.policy.AllowedSchemes[0] != "https" ||
		len(client.policy.HostAllowlist) != 1 || client.policy.HostAllowlist[0] != "127.0.0.1" || client.policy.MaxRedirects != 0 {
		t.Fatalf("policy = %+v", client.policy)
	}
	if err := client.policy.Validate(context.Background(), "https://example.org"+federation.SearchPath); err == nil {
		t.Fatal("policy accepted a host outside the exact allowlist")
	} else {
		var blocked *egress.Error
		if !errors.As(err, &blocked) || blocked.Reason != egress.ReasonHostNotAllow {
			t.Fatalf("policy error = %v", err)
		}
	}
}

func TestInjectedHTTPClientRetainsDialTimePrivateAddressRejection(t *testing.T) {
	fixture := newPeerFixture(t, nil)
	publicPeer := newTrustedPeer(t, fixture.remote, "https://example.com", false)
	client, err := New(Options{Identity: fixture.local, Peer: publicPeer, HTTPClient: fixture.httpClient})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	transport := client.http.Transport.(*http.Transport)
	if transport.DialContext == nil {
		t.Fatal("injected client lost the policy dialer")
	}
	connection, err := transport.DialContext(context.Background(), "tcp", "127.0.0.1:443")
	if connection != nil {
		_ = connection.Close()
		t.Fatal("policy dialer connected to a private address")
	}
	var blocked *egress.Error
	if !errors.As(err, &blocked) || blocked.Reason != egress.ReasonPrivateIP {
		t.Fatalf("dial error = %v, want private-IP policy rejection", err)
	}
}

func TestSearchBatchRequiresNormalTLSVerification(t *testing.T) {
	fixture := newPeerFixture(t, nil)
	client := newClient(t, fixture, func(options *Options) { options.HTTPClient = nil })
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "query"})
	var typed *Error
	if !errors.As(err, &typed) || !errors.Is(err, ErrTransport) || !typed.Retryable || !batch.Diagnostics[0].Retryable {
		t.Fatalf("typed=%+v batch=%+v err=%v", typed, batch, err)
	}
}

func TestNewCopiesCallerHTTPClientAndDisablesCompression(t *testing.T) {
	fixture := newPeerFixture(t, nil)
	sourceTransport := fixture.httpClient.Transport.(*http.Transport)
	client := newClient(t, fixture, nil)
	if client.http == fixture.httpClient || client.http.Transport == sourceTransport {
		t.Fatal("HTTP client or transport was not copied")
	}
	if !client.http.Transport.(*http.Transport).DisableCompression || sourceTransport.DisableCompression {
		t.Fatal("compression policy leaked into or out of the copied transport")
	}
	if client.http.Transport.(*http.Transport).DialContext == nil || client.http.Transport.(*http.Transport).MaxResponseHeaderBytes != maxResponseHeaderBytes {
		t.Fatal("copied transport lost federation dial/header protections")
	}
	if client.http.Timeout != defaultTimeout || fixture.httpClient.Timeout != 0 {
		t.Fatalf("timeouts copied=%v source=%v", client.http.Timeout, fixture.httpClient.Timeout)
	}
}

func TestSearchBatchConcurrentUse(t *testing.T) {
	fixture := newPeerFixture(t, nil)
	client := newClient(t, fixture, nil)
	const count = 24
	var wait sync.WaitGroup
	errorsFound := make(chan error, count)
	for index := 0; index < count; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			batch, err := client.SearchBatch(context.Background(), search.Query{Q: "parallel"})
			if err != nil {
				errorsFound <- err
				return
			}
			if len(batch.Hits) != 1 {
				errorsFound <- fmt.Errorf("unexpected hit count %d", len(batch.Hits))
			}
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
}
