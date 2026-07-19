package federationapi

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/staticvar/fetchmark/internal/core/federation"
	"github.com/staticvar/fetchmark/internal/core/search"
	"github.com/staticvar/fetchmark/internal/obs"
	"golang.org/x/time/rate"
)

type batchSearcherFunc func(context.Context, search.Query) (search.SearchBatch, error)

func (function batchSearcherFunc) SearchBatch(ctx context.Context, query search.Query) (search.SearchBatch, error) {
	return function(ctx, query)
}

type handlerFixture struct {
	localIdentity federation.Identity
	localPublic   ed25519.PublicKey
	peerID        string
	peerPrivate   ed25519.PrivateKey
	peerPublic    ed25519.PublicKey
	now           time.Time
	registry      federation.TrustRegistry
}

func TestHandlerReturnsSignedURLOnlyResponseAndSafeLocalQuery(t *testing.T) {
	fixture := newHandlerFixture(t)
	var captured search.Query
	searcher := batchSearcherFunc(func(_ context.Context, query search.Query) (search.SearchBatch, error) {
		captured = query
		published := fixture.now
		return search.SearchBatch{Status: search.BatchHealthy, Hits: []search.Hit{
			{URL: "https://Example.com:443/a?utm_source=secret&b=2", Title: "private title", Snippet: "private snippet", PublishedAt: &published, Metadata: map[string]string{"private": "metadata"}},
			{URL: "https://example.com/a?b=2"},
			{URL: "ftp://example.com/not-exportable"},
			{URL: "https://person@example.com/not-exportable"},
			{URL: "http://example.net/second#fragment"},
		}}, nil
	})
	handler := fixture.handler(t, searcher, HandlerOptions{Random: bytes.NewReader(bytes.Repeat([]byte{7}, 64))})
	rawRequest := fixture.signRequest(t, "sensitive query", 3, nonceFor(1))
	recorder := perform(handler, http.MethodPost, federation.SearchPath, "application/json", rawRequest, nil)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if captured.Q != "sensitive query" || captured.MaxResults != 3 || captured.SafeSearch == nil || *captured.SafeSearch != 1 {
		t.Fatalf("local query = %#v", captured)
	}
	if len(captured.Engines) != 0 || len(captured.Categories) != 0 || len(captured.IncludeDomains) != 0 || len(captured.ExcludeDomains) != 0 {
		t.Fatalf("remote request controlled local source selection: %#v", captured)
	}
	verified, err := federation.VerifyResponse(recorder.Body.Bytes(), rawRequest, fixture.localPublic, federation.Verification{
		Sender: fixture.localIdentity.IdentityID(), Audience: fixture.peerID, Now: fixture.now,
	})
	if err != nil {
		t.Fatalf("VerifyResponse: %v", err)
	}
	want := []federation.SearchResult{{URL: "https://example.com/a?b=2"}, {URL: "http://example.net/second"}}
	if fmt.Sprint(verified.Search.Results) != fmt.Sprint(want) {
		t.Fatalf("results = %#v, want %#v", verified.Search.Results, want)
	}
	for _, secret := range []string{"sensitive query", "private title", "private snippet", "metadata", "PublishedAt"} {
		if strings.Contains(recorder.Body.String(), secret) {
			t.Fatalf("response leaked %q: %s", secret, recorder.Body.String())
		}
	}
	assertSecurityHeaders(t, recorder)
}

func TestHandlerAuthenticatesBeforeSearchAndRejectsReplay(t *testing.T) {
	fixture := newHandlerFixture(t)
	var calls atomic.Int32
	handler := fixture.handler(t, batchSearcherFunc(func(context.Context, search.Query) (search.SearchBatch, error) {
		calls.Add(1)
		return search.SearchBatch{Status: search.BatchAuthoritativeEmpty}, nil
	}), HandlerOptions{Random: bytes.NewReader(bytes.Repeat([]byte{8}, 64))})

	raw := fixture.signRequest(t, "auth test", 1, nonceFor(2))
	tampered := append([]byte(nil), raw...)
	tampered[len(tampered)-2] ^= 1
	response := perform(handler, http.MethodPost, federation.SearchPath, "application/json", tampered, nil)
	if response.Code != http.StatusUnauthorized || calls.Load() != 0 {
		t.Fatalf("tampered request status/calls = %d/%d", response.Code, calls.Load())
	}
	response = perform(handler, http.MethodPost, federation.SearchPath, "application/json", raw, nil)
	if response.Code != http.StatusOK || calls.Load() != 1 {
		t.Fatalf("valid request status/calls = %d/%d: %s", response.Code, calls.Load(), response.Body.String())
	}
	response = perform(handler, http.MethodPost, federation.SearchPath, "application/json", raw, nil)
	if response.Code != http.StatusConflict || calls.Load() != 1 {
		t.Fatalf("replay status/calls = %d/%d", response.Code, calls.Load())
	}
}

func TestHandlerAttributesPeerMetricsOnlyAfterAuthentication(t *testing.T) {
	fixture := newHandlerFixture(t)
	var calls atomic.Int32
	handler := fixture.handler(t, batchSearcherFunc(func(context.Context, search.Query) (search.SearchBatch, error) {
		calls.Add(1)
		return search.SearchBatch{Status: search.BatchAuthoritativeEmpty}, nil
	}), HandlerOptions{Random: bytes.NewReader(bytes.Repeat([]byte{8}, 64))})

	untrustedAuth := obs.FederationRequestTotal.WithLabelValues("inbound", "untrusted", "auth")
	trustedAuth := obs.FederationRequestTotal.WithLabelValues("inbound", "peer-binding", "auth")
	beforeUntrustedAuth := counterValue(t, untrustedAuth)
	beforeTrustedAuth := counterValue(t, trustedAuth)

	tampered := fixture.signRequest(t, "auth metrics", 1, nonceFor(6))
	envelope, _, err := federation.DecodeRequest(tampered)
	if err != nil {
		t.Fatal(err)
	}
	signature, err := base64.StdEncoding.DecodeString(envelope.Signature)
	if err != nil {
		t.Fatal(err)
	}
	signature[0] ^= 1
	envelope.Signature = base64.StdEncoding.EncodeToString(signature)
	tampered, err = json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}

	response := perform(handler, http.MethodPost, federation.SearchPath, "application/json", tampered, nil)
	if response.Code != http.StatusUnauthorized || calls.Load() != 0 {
		t.Fatalf("tampered request status/calls = %d/%d", response.Code, calls.Load())
	}
	if got := counterValue(t, untrustedAuth) - beforeUntrustedAuth; got != 1 {
		t.Fatalf("untrusted auth metric delta = %v, want 1", got)
	}
	if got := counterValue(t, trustedAuth) - beforeTrustedAuth; got != 0 {
		t.Fatalf("trusted auth metric delta = %v, want 0", got)
	}

	trustedEmpty := obs.FederationRequestTotal.WithLabelValues("inbound", "peer-binding", "empty")
	beforeTrustedEmpty := counterValue(t, trustedEmpty)
	response = perform(handler, http.MethodPost, federation.SearchPath, "application/json",
		fixture.signRequest(t, "authenticated metrics", 1, nonceFor(7)), nil)
	if response.Code != http.StatusOK || calls.Load() != 1 {
		t.Fatalf("authenticated request status/calls = %d/%d: %s", response.Code, calls.Load(), response.Body.String())
	}
	if got := counterValue(t, trustedEmpty) - beforeTrustedEmpty; got != 1 {
		t.Fatalf("trusted empty metric delta = %v, want 1", got)
	}
}

func counterValue(t *testing.T, counter prometheus.Counter) float64 {
	t.Helper()
	metric := new(dto.Metric)
	if err := counter.Write(metric); err != nil {
		t.Fatalf("write counter metric: %v", err)
	}
	return metric.GetCounter().GetValue()
}

func TestHandlerFailsClosedWhenReplayCapacityIsUnavailable(t *testing.T) {
	fixture := newHandlerFixture(t)
	replay, err := NewReplayCache(1, fiveMinutes)
	if err != nil {
		t.Fatal(err)
	}
	if err := replay.Reserve("another-peer", "live-nonce", fixture.now); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	handler := fixture.handler(t, batchSearcherFunc(func(context.Context, search.Query) (search.SearchBatch, error) {
		calls.Add(1)
		return search.SearchBatch{Status: search.BatchHealthy}, nil
	}), HandlerOptions{ReplayCache: replay, Random: bytes.NewReader(bytes.Repeat([]byte{8}, 32))})
	response := perform(handler, http.MethodPost, federation.SearchPath, "application/json",
		fixture.signRequest(t, "capacity", 1, nonceFor(3)), nil)
	if response.Code != http.StatusServiceUnavailable || calls.Load() != 0 {
		t.Fatalf("capacity response/calls = %d/%d: %s", response.Code, calls.Load(), response.Body.String())
	}
}

func TestHandlerFailsClosedOnNonAuthoritativeLocalBatch(t *testing.T) {
	for _, status := range []search.BatchStatus{search.BatchPartial, search.BatchDegradedEmpty, search.BatchFailed, ""} {
		t.Run(string(status), func(t *testing.T) {
			fixture := newHandlerFixture(t)
			handler := fixture.handler(t, batchSearcherFunc(func(context.Context, search.Query) (search.SearchBatch, error) {
				return search.SearchBatch{Status: status, Hits: []search.Hit{{URL: "https://example.com/should-not-export"}}}, nil
			}), HandlerOptions{Random: bytes.NewReader(bytes.Repeat([]byte{8}, 32))})
			response := perform(handler, http.MethodPost, federation.SearchPath, "application/json",
				fixture.signRequest(t, "status", 1, nonceFor(4)), nil)
			if response.Code != http.StatusBadGateway || strings.Contains(response.Body.String(), "should-not-export") {
				t.Fatalf("response = %d %q", response.Code, response.Body.String())
			}
		})
	}
}

func TestHandlerSignsResponseWithPostSearchClock(t *testing.T) {
	fixture := newHandlerFixture(t)
	later := fixture.now.Add(15 * time.Second)
	var calls atomic.Int32
	handler := fixture.handler(t, batchSearcherFunc(func(context.Context, search.Query) (search.SearchBatch, error) {
		return search.SearchBatch{Status: search.BatchAuthoritativeEmpty}, nil
	}), HandlerOptions{
		Clock: func() time.Time {
			if calls.Add(1) == 1 {
				return fixture.now
			}
			return later
		},
		Random: bytes.NewReader(bytes.Repeat([]byte{8}, 32)),
	})
	raw := fixture.signRequest(t, "clock", 1, nonceFor(5))
	response := perform(handler, http.MethodPost, federation.SearchPath, "application/json", raw, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	envelope, _, err := federation.DecodeResponse(response.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if envelope.Timestamp != later.Format(time.RFC3339) {
		t.Fatalf("response timestamp = %q, want %q", envelope.Timestamp, later.Format(time.RFC3339))
	}
}

func TestHandlerEnforcesPerPeerRateLimit(t *testing.T) {
	fixture := newHandlerFixture(t)
	var calls atomic.Int32
	handler := fixture.handler(t, batchSearcherFunc(func(context.Context, search.Query) (search.SearchBatch, error) {
		calls.Add(1)
		return search.SearchBatch{Status: search.BatchAuthoritativeEmpty}, nil
	}), HandlerOptions{
		PeerRate: rate.Limit(1), PeerBurst: 2,
		Random: bytes.NewReader(bytes.Repeat([]byte{9}, 64)),
	})
	for index := 0; index < 3; index++ {
		raw := fixture.signRequest(t, "rate test", 1, nonceFor(byte(10+index)))
		response := perform(handler, http.MethodPost, federation.SearchPath, "application/json", raw, nil)
		want := http.StatusOK
		if index == 2 {
			want = http.StatusTooManyRequests
		}
		if response.Code != want {
			t.Fatalf("request %d status = %d, want %d: %s", index, response.Code, want, response.Body.String())
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("search calls = %d, want 2", calls.Load())
	}
}

func TestHandlerRateLimitRunsBeforeReplayReservation(t *testing.T) {
	fixture := newHandlerFixture(t)
	replay, err := NewReplayCache(1, fiveMinutes)
	if err != nil {
		t.Fatal(err)
	}
	handler := fixture.handler(t, batchSearcherFunc(func(context.Context, search.Query) (search.SearchBatch, error) {
		return search.SearchBatch{Status: search.BatchAuthoritativeEmpty}, nil
	}), HandlerOptions{
		ReplayCache: replay, PeerRate: rate.Limit(1), PeerBurst: 1,
		Random: bytes.NewReader(bytes.Repeat([]byte{9}, 32)),
	})
	first := perform(handler, http.MethodPost, federation.SearchPath, "application/json",
		fixture.signRequest(t, "first", 1, nonceFor(16)), nil)
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d: %s", first.Code, first.Body.String())
	}
	second := perform(handler, http.MethodPost, federation.SearchPath, "application/json",
		fixture.signRequest(t, "rate limited", 1, nonceFor(17)), nil)
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second status = %d, want 429 before full replay table: %s", second.Code, second.Body.String())
	}
}

func TestHandlerBoundsConcurrentLocalSearch(t *testing.T) {
	fixture := newHandlerFixture(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	searcher := batchSearcherFunc(func(ctx context.Context, _ search.Query) (search.SearchBatch, error) {
		once.Do(func() { close(started) })
		select {
		case <-release:
			return search.SearchBatch{Status: search.BatchAuthoritativeEmpty}, nil
		case <-ctx.Done():
			return search.SearchBatch{}, ctx.Err()
		}
	})
	handler := fixture.handler(t, searcher, HandlerOptions{
		MaxConcurrent: 1, PeerMaxConcurrent: 2, PeerBurst: 4, Random: bytes.NewReader(bytes.Repeat([]byte{10}, 64)),
	})
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	firstRaw := fixture.signRequest(t, "first", 1, nonceFor(20))
	go func() {
		firstDone <- perform(handler, http.MethodPost, federation.SearchPath, "application/json", firstRaw, nil)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first search did not start")
	}
	second := perform(handler, http.MethodPost, federation.SearchPath, "application/json", fixture.signRequest(t, "second", 1, nonceFor(21)), nil)
	if second.Code != http.StatusServiceUnavailable {
		t.Fatalf("concurrent status = %d, want 503: %s", second.Code, second.Body.String())
	}
	close(release)
	if first := <-firstDone; first.Code != http.StatusOK {
		t.Fatalf("first status = %d: %s", first.Code, first.Body.String())
	}
}

func TestHandlerBoundsConcurrentSearchPerPeer(t *testing.T) {
	fixture := newHandlerFixture(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var calls atomic.Int32
	searcher := batchSearcherFunc(func(ctx context.Context, _ search.Query) (search.SearchBatch, error) {
		calls.Add(1)
		once.Do(func() { close(started) })
		select {
		case <-release:
			return search.SearchBatch{Status: search.BatchAuthoritativeEmpty}, nil
		case <-ctx.Done():
			return search.SearchBatch{}, ctx.Err()
		}
	})
	handler := fixture.handler(t, searcher, HandlerOptions{
		MaxConcurrent: 4, PeerMaxConcurrent: 1, PeerBurst: 4,
		Random: bytes.NewReader(bytes.Repeat([]byte{10}, 64)),
	})
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		firstDone <- perform(handler, http.MethodPost, federation.SearchPath, "application/json",
			fixture.signRequest(t, "first", 1, nonceFor(22)), nil)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first search did not start")
	}
	second := perform(handler, http.MethodPost, federation.SearchPath, "application/json",
		fixture.signRequest(t, "same peer", 1, nonceFor(23)), nil)
	if second.Code != http.StatusServiceUnavailable || calls.Load() != 1 {
		t.Fatalf("same-peer status/calls = %d/%d, want 503/1: %s", second.Code, calls.Load(), second.Body.String())
	}
	close(release)
	if first := <-firstDone; first.Code != http.StatusOK {
		t.Fatalf("first status = %d: %s", first.Code, first.Body.String())
	}
}

func TestHandlerTimesOutLocalSearch(t *testing.T) {
	fixture := newHandlerFixture(t)
	searcher := batchSearcherFunc(func(ctx context.Context, _ search.Query) (search.SearchBatch, error) {
		<-ctx.Done()
		return search.SearchBatch{}, ctx.Err()
	})
	handler := fixture.handler(t, searcher, HandlerOptions{
		SearchTimeout: 10 * time.Millisecond, Random: bytes.NewReader(bytes.Repeat([]byte{11}, 32)),
	})
	response := perform(handler, http.MethodPost, federation.SearchPath, "application/json", fixture.signRequest(t, "timeout", 1, nonceFor(30)), nil)
	if response.Code != http.StatusGatewayTimeout || response.Body.String() != internalErrorBody {
		t.Fatalf("timeout response = %d %q", response.Code, response.Body.String())
	}
}

func TestHandlerStrictHTTPAndBoundedGenericErrors(t *testing.T) {
	fixture := newHandlerFixture(t)
	var calls atomic.Int32
	handler := fixture.handler(t, batchSearcherFunc(func(context.Context, search.Query) (search.SearchBatch, error) {
		calls.Add(1)
		return search.SearchBatch{}, errorsForTest("must-not-leak-query-or-raw-error")
	}), HandlerOptions{Random: bytes.NewReader(bytes.Repeat([]byte{12}, 32))})
	valid := fixture.signRequest(t, "must-not-leak-query", 1, nonceFor(40))
	trailing := append(append([]byte(nil), valid...), []byte(` {}`)...)
	oversized := bytes.Repeat([]byte{'x'}, federation.MaxRequestBytes+1)
	tests := []struct {
		name        string
		method      string
		path        string
		contentType string
		body        []byte
		headers     http.Header
		want        int
	}{
		{name: "wrong method", method: http.MethodGet, path: federation.SearchPath, contentType: "application/json", body: valid, want: http.StatusMethodNotAllowed},
		{name: "wrong path", method: http.MethodPost, path: "/federation/v1/index/other", contentType: "application/json", body: valid, want: http.StatusNotFound},
		{name: "query parameters", method: http.MethodPost, path: federation.SearchPath + "?debug=true", contentType: "application/json", body: valid, want: http.StatusNotFound},
		{name: "wrong media type", method: http.MethodPost, path: federation.SearchPath, contentType: "text/json", body: valid, want: http.StatusUnsupportedMediaType},
		{name: "unsupported charset", method: http.MethodPost, path: federation.SearchPath, contentType: "application/json; charset=latin1", body: valid, want: http.StatusUnsupportedMediaType},
		{name: "duplicate content type", method: http.MethodPost, path: federation.SearchPath, contentType: "application/json", body: valid, headers: http.Header{"Content-Type": {"application/json"}}, want: http.StatusUnsupportedMediaType},
		{name: "content encoding", method: http.MethodPost, path: federation.SearchPath, contentType: "application/json", body: valid, headers: http.Header{"Content-Encoding": {"identity"}}, want: http.StatusUnsupportedMediaType},
		{name: "oversized", method: http.MethodPost, path: federation.SearchPath, contentType: "application/json", body: oversized, want: http.StatusBadRequest},
		{name: "trailing JSON", method: http.MethodPost, path: federation.SearchPath, contentType: "application/json", body: trailing, want: http.StatusUnauthorized},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := perform(handler, test.method, test.path, test.contentType, test.body, test.headers)
			if response.Code != test.want {
				t.Fatalf("status = %d, want %d: %s", response.Code, test.want, response.Body.String())
			}
			if response.Body.Len() > 64 || strings.Contains(response.Body.String(), "debug") || strings.Contains(response.Body.String(), "signature") {
				t.Fatalf("unbounded or revealing error: %q", response.Body.String())
			}
			assertSecurityHeaders(t, response)
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("strictly rejected requests reached search: %d", calls.Load())
	}

	searchFailure := perform(handler, http.MethodPost, federation.SearchPath, "application/json", valid, nil)
	if searchFailure.Code != http.StatusBadGateway || searchFailure.Body.String() != internalErrorBody ||
		strings.Contains(searchFailure.Body.String(), "must-not-leak") {
		t.Fatalf("search error response = %d %q", searchFailure.Code, searchFailure.Body.String())
	}
}

func TestNewHandlerRejectsMissingSecurityDependencies(t *testing.T) {
	fixture := newHandlerFixture(t)
	replay, err := NewReplayCache(4, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	searcher := batchSearcherFunc(func(context.Context, search.Query) (search.SearchBatch, error) {
		return search.SearchBatch{Status: search.BatchAuthoritativeEmpty}, nil
	})
	if _, err := NewHandler(HandlerOptions{TrustRegistry: fixture.registry, Searcher: searcher, ReplayCache: replay}); err == nil {
		t.Fatal("NewHandler accepted zero identity")
	}
	if _, err := NewHandler(HandlerOptions{Identity: fixture.localIdentity, TrustRegistry: fixture.registry, ReplayCache: replay}); err == nil {
		t.Fatal("NewHandler accepted nil searcher")
	}
	if _, err := NewHandler(HandlerOptions{Identity: fixture.localIdentity, TrustRegistry: fixture.registry, Searcher: searcher}); err == nil {
		t.Fatal("NewHandler accepted nil replay cache")
	}
	if _, err := NewHandler(HandlerOptions{
		Identity: fixture.localIdentity, TrustRegistry: fixture.registry, Searcher: searcher, ReplayCache: replay,
		MaxVerifications: -1,
	}); err == nil {
		t.Fatal("NewHandler accepted negative verification concurrency")
	}
}

func newHandlerFixture(t *testing.T) handlerFixture {
	t.Helper()
	localPrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{1}, ed25519.SeedSize))
	peerPrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{2}, ed25519.SeedSize))
	local := decodeIdentityForTest(t, "local-node", localPrivate)
	peerPublic := peerPrivate.Public().(ed25519.PublicKey)
	keyID := federation.KeyID(peerPublic)
	registryJSON := fmt.Sprintf(`{"version":1,"identities":[{"id":"peer-node","key_id":"%s","ed25519_public_key":"%s"}],"peers":[{"id":"peer-binding","identity_id":"peer-node","base_origin":"https://peer.example","allow_private_network":false,"allow_inbound":true}]}`,
		keyID, base64.StdEncoding.EncodeToString(peerPublic))
	registry, err := federation.LoadTrustRegistry(strings.NewReader(registryJSON))
	if err != nil {
		t.Fatalf("LoadTrustRegistry: %v", err)
	}
	return handlerFixture{
		localIdentity: local, localPublic: local.PublicKey(), peerID: "peer-node", peerPrivate: peerPrivate,
		peerPublic: peerPublic, now: time.Date(2026, time.July, 18, 12, 0, 0, 0, time.UTC), registry: registry,
	}
}

func decodeIdentityForTest(t *testing.T, identityID string, privateKey ed25519.PrivateKey) federation.Identity {
	t.Helper()
	publicKey := privateKey.Public().(ed25519.PublicKey)
	raw := fmt.Sprintf(`{"version":1,"identity_id":"%s","key_id":"%s","ed25519_private_key":"%s"}`,
		identityID, federation.KeyID(publicKey), base64.StdEncoding.EncodeToString(privateKey))
	identity, err := federation.DecodeIdentity([]byte(raw))
	if err != nil {
		t.Fatalf("DecodeIdentity: %v", err)
	}
	return identity
}

func (fixture handlerFixture) handler(t *testing.T, searcher search.BatchSearcher, overrides HandlerOptions) http.Handler {
	t.Helper()
	if overrides.ReplayCache == nil {
		replay, err := NewReplayCache(128, fiveMinutes)
		if err != nil {
			t.Fatal(err)
		}
		overrides.ReplayCache = replay
	}
	overrides.Identity = fixture.localIdentity
	overrides.TrustRegistry = fixture.registry
	overrides.Searcher = searcher
	if overrides.Clock == nil {
		overrides.Clock = func() time.Time { return fixture.now }
	}
	handler, err := NewHandler(overrides)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler
}

func (fixture handlerFixture) signRequest(t *testing.T, query string, limit int, nonce []byte) []byte {
	t.Helper()
	raw, err := federation.SignRequest(federation.SearchRequest{Query: query, Limit: limit}, fixture.peerID,
		fixture.localIdentity.IdentityID(), fixture.now, nonce, fixture.peerPrivate)
	if err != nil {
		t.Fatalf("SignRequest: %v", err)
	}
	return raw
}

func nonceFor(value byte) []byte { return bytes.Repeat([]byte{value}, federation.NonceBytes) }

func perform(handler http.Handler, method, path, contentType string, body []byte, headers http.Header) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewReader(body))
	request.Header.Set("Content-Type", contentType)
	for name, values := range headers {
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func assertSecurityHeaders(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("X-Content-Type-Options") != "nosniff" ||
		response.Header().Get("Content-Type") != "application/json" || response.Header().Get("Content-Encoding") != "" {
		t.Fatalf("security headers = %#v", response.Header())
	}
}

type errorsForTest string

func (message errorsForTest) Error() string { return string(message) }
